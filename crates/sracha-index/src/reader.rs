//! Vortex shard reader / point-lookup API.
//!
//! Uses Vortex's `scan().with_filter(eq(...))` predicate pushdown
//! so individual lookups read only the matching chunk(s) — open()
//! does not materialize the whole catalog. For a 5500-accession
//! shard, open() is ~ms and lookup() is ~ms.

use std::path::Path;

use futures::TryStreamExt;
use vortex::VortexSessionDefault;
use vortex::array::ArrayRef;
use vortex::array::expr::{eq, get_item, lit, root};
use vortex::file::{OpenOptionsSessionExt, VortexFile};
use vortex::io::session::RuntimeSessionExt;
use vortex::session::VortexSession;

use crate::record::{AccessionRecord, BlobLocator, ColumnMetaEntry, Layout, Platform, SchemaEntry};
use crate::{Error, Result};

/// One self-contained shard: three Vortex files
/// (accessions/col_extents/schemas) where all foreign keys
/// (`schema_id`, `accession_idx`) resolve within the shard.
pub struct ShardReader {
    #[allow(dead_code)]
    session: VortexSession,
    accessions_file: VortexFile,
    col_extents_file: VortexFile,
    schemas_file: VortexFile,
}

impl ShardReader {
    pub async fn open_local(path: &Path) -> Result<Self> {
        let session = VortexSession::default().with_tokio();
        let accessions_file = session
            .open_options()
            .open_path(path.join("accessions.vortex"))
            .await
            .map_err(|e| Error::Reader(format!("open accessions: {e}")))?;
        let col_extents_file = session
            .open_options()
            .open_path(path.join("col_extents.vortex"))
            .await
            .map_err(|e| Error::Reader(format!("open col_extents: {e}")))?;
        let schemas_file = session
            .open_options()
            .open_path(path.join("schemas.vortex"))
            .await
            .map_err(|e| Error::Reader(format!("open schemas: {e}")))?;
        Ok(Self {
            session,
            accessions_file,
            col_extents_file,
            schemas_file,
        })
    }

    /// Number of accessions in the catalog (file row count, no
    /// materialization).
    pub fn len(&self) -> usize {
        self.accessions_file.row_count() as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Point lookup. Pushes a `accession == X` filter into the
    /// Vortex scan; the engine prunes chunks via min/max stats and
    /// reads only the matching range.
    pub async fn lookup(&self, accession: &str) -> Result<Option<AccessionRecord>> {
        // 1. Filter accessions by string equality.
        let chunks = scan_with_filter(
            &self.accessions_file,
            eq(get_item("accession", root()), lit(accession)),
        )
        .await?;
        let Some(row) = first_row(&chunks)? else {
            return Ok(None);
        };
        let acc_struct = row.scalar_at(0).map_err(vortex_err)?;
        let acc_struct = acc_struct
            .as_struct_opt()
            .ok_or_else(|| Error::Reader("accessions row not a struct".into()))?;

        let accession_idx = field_u32(&acc_struct, "accession_idx")?;
        let file_size = field_u64(&acc_struct, "file_size")?;
        let kar_data_offset = field_u64(&acc_struct, "kar_data_offset")?;
        let schema_id = field_u32(&acc_struct, "schema_id")?;
        let spots_present = field_u8(&acc_struct, "spots_present")? != 0;
        let spots = if spots_present {
            Some(field_u64(&acc_struct, "spots")?)
        } else {
            None
        };
        let layout = match field_u8(&acc_struct, "layout")? {
            1 => Layout::Single,
            2 => Layout::Paired,
            _ => Layout::Unknown,
        };
        let platform = match field_u8(&acc_struct, "platform")? {
            1 => Platform::Illumina,
            2 => Platform::PacBio,
            3 => Platform::OxfordNanopore,
            4 => Platform::IonTorrent,
            _ => Platform::Other,
        };
        let read_lengths_json = field_string(&acc_struct, "read_lengths_json")?;
        let read_lengths: Vec<u32> = serde_json::from_str(&read_lengths_json).unwrap_or_default();
        // md5 not yet populated by the extractor — separate task.
        let md5 = None;

        // 2. Look up schema by schema_id.
        let schema = self.lookup_schema(schema_id).await?;

        // 3. Filter col_extents by accession_idx.
        let blobs = self.collect_blobs_for_accession(accession_idx).await?;

        Ok(Some(AccessionRecord {
            accession: accession.to_string(),
            file_size,
            md5,
            spots,
            layout,
            platform,
            read_lengths,
            schema_fingerprint: schema.fingerprint,
            kar_data_offset,
            blobs,
            schema,
            name_fmt: None,
            extract_secs: 0.0,
            bytes_fetched: 0,
        }))
    }

    async fn lookup_schema(&self, schema_id: u32) -> Result<SchemaEntry> {
        let chunks = scan_with_filter(
            &self.schemas_file,
            eq(get_item("schema_id", root()), lit(schema_id)),
        )
        .await?;
        let row = first_row(&chunks)?
            .ok_or_else(|| Error::Reader(format!("schema_id {schema_id} not found")))?;
        let s = row.scalar_at(0).map_err(vortex_err)?;
        let s = s
            .as_struct_opt()
            .ok_or_else(|| Error::Reader("schemas row not a struct".into()))?;
        let fp_bytes = field_binary(&s, "fingerprint")?;
        let fingerprint: [u8; 32] = fp_bytes
            .as_slice()
            .try_into()
            .map_err(|_| Error::Reader(format!("fingerprint length {} != 32", fp_bytes.len())))?;
        let is_csra = field_u8(&s, "is_csra")? != 0;
        let cols_json = field_string(&s, "columns_json")?;
        let columns: Vec<ColumnMetaEntry> = serde_json::from_str(&cols_json)?;
        Ok(SchemaEntry {
            fingerprint,
            columns,
            is_csra,
        })
    }

    async fn collect_blobs_for_accession(&self, accession_idx: u32) -> Result<Vec<BlobLocator>> {
        let chunks = scan_with_filter(
            &self.col_extents_file,
            eq(get_item("accession_idx", root()), lit(accession_idx)),
        )
        .await?;
        let mut out = Vec::new();
        for chunk in &chunks {
            let n = chunk.len();
            for row in 0..n {
                let r = chunk.scalar_at(row).map_err(vortex_err)?;
                let r = r
                    .as_struct_opt()
                    .ok_or_else(|| Error::Reader("col_extents row not a struct".into()))?;
                let column_id = field_u8(&r, "column_id")?;
                let n_blobs = field_u32(&r, "n_blobs")?;
                let data_slab_offset = field_u64(&r, "data_slab_offset")?;
                let data_slab_size = field_u64(&r, "data_slab_size")?;
                let first_start_id = field_i64(&r, "first_start_id")?;
                let uniform_id_range = field_u32(&r, "uniform_id_range")?;
                if n_blobs == 0 {
                    continue;
                }
                let approx_size =
                    u32::try_from(data_slab_size / u64::from(n_blobs)).unwrap_or(u32::MAX);
                for i in 0..n_blobs {
                    let blob_offset = data_slab_offset + u64::from(i) * u64::from(approx_size);
                    let start_id = first_start_id + i64::from(i) * i64::from(uniform_id_range);
                    out.push(BlobLocator {
                        column_id,
                        start_id,
                        id_range: uniform_id_range,
                        blob_offset,
                        blob_size: approx_size,
                    });
                }
            }
        }
        Ok(out)
    }
}

// --- helpers -------------------------------------------------------------

fn vortex_err(e: vortex::error::VortexError) -> Error {
    Error::Reader(format!("vortex: {e}"))
}

async fn scan_with_filter(
    file: &VortexFile,
    filter: vortex::array::expr::Expression,
) -> Result<Vec<ArrayRef>> {
    let chunks: Vec<ArrayRef> = file
        .scan()
        .map_err(vortex_err)?
        .with_filter(filter)
        .into_array_stream()
        .map_err(vortex_err)?
        .try_collect()
        .await
        .map_err(vortex_err)?;
    Ok(chunks)
}

fn first_row(chunks: &[ArrayRef]) -> Result<Option<ArrayRef>> {
    for chunk in chunks {
        if !chunk.is_empty() {
            return Ok(Some(chunk.slice(0..1).map_err(vortex_err)?));
        }
    }
    Ok(None)
}

// Field accessors take a StructScalar by value (it's a thin view).

fn field_string(s: &vortex::array::scalar::StructScalar, name: &str) -> Result<String> {
    let f = s
        .field(name)
        .ok_or_else(|| Error::Reader(format!("missing field {name}")))?;
    let v = f
        .as_utf8_opt()
        .ok_or_else(|| Error::Reader(format!("field {name} not utf8")))?;
    Ok(v.value()
        .ok_or_else(|| Error::Reader(format!("field {name} null")))?
        .as_str()
        .to_string())
}

fn field_binary(s: &vortex::array::scalar::StructScalar, name: &str) -> Result<Vec<u8>> {
    let f = s
        .field(name)
        .ok_or_else(|| Error::Reader(format!("missing field {name}")))?;
    let v = f
        .as_binary_opt()
        .ok_or_else(|| Error::Reader(format!("field {name} not binary")))?;
    Ok(v.value()
        .ok_or_else(|| Error::Reader(format!("field {name} null")))?
        .as_slice()
        .to_vec())
}

fn field_u8(s: &vortex::array::scalar::StructScalar, name: &str) -> Result<u8> {
    let f = s
        .field(name)
        .ok_or_else(|| Error::Reader(format!("missing field {name}")))?;
    let p = f
        .as_primitive_opt()
        .ok_or_else(|| Error::Reader(format!("field {name} not primitive")))?;
    p.as_::<u8>()
        .ok_or_else(|| Error::Reader(format!("field {name} not u8")))
}

fn field_u32(s: &vortex::array::scalar::StructScalar, name: &str) -> Result<u32> {
    let f = s
        .field(name)
        .ok_or_else(|| Error::Reader(format!("missing field {name}")))?;
    let p = f
        .as_primitive_opt()
        .ok_or_else(|| Error::Reader(format!("field {name} not primitive")))?;
    p.as_::<u32>()
        .ok_or_else(|| Error::Reader(format!("field {name} not u32")))
}

fn field_u64(s: &vortex::array::scalar::StructScalar, name: &str) -> Result<u64> {
    let f = s
        .field(name)
        .ok_or_else(|| Error::Reader(format!("missing field {name}")))?;
    let p = f
        .as_primitive_opt()
        .ok_or_else(|| Error::Reader(format!("field {name} not primitive")))?;
    p.as_::<u64>()
        .ok_or_else(|| Error::Reader(format!("field {name} not u64")))
}

fn field_i64(s: &vortex::array::scalar::StructScalar, name: &str) -> Result<i64> {
    let f = s
        .field(name)
        .ok_or_else(|| Error::Reader(format!("missing field {name}")))?;
    let p = f
        .as_primitive_opt()
        .ok_or_else(|| Error::Reader(format!("field {name} not primitive")))?;
    p.as_::<i64>()
        .ok_or_else(|| Error::Reader(format!("field {name} not i64")))
}

// ---------------------------------------------------------------------------
// Multi-shard catalog
// ---------------------------------------------------------------------------

/// Manifest format. Lives at `<catalog_dir>/manifest.json`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Manifest {
    pub version: u32,
    pub shards: Vec<ShardEntry>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ShardEntry {
    /// Display name (e.g. "base", "2026-04-26").
    pub name: String,
    /// Path relative to the catalog dir.
    pub path: String,
    /// Number of accessions in this shard. Diagnostic — not load
    /// bearing.
    pub n_accessions: u64,
    /// ISO 8601 timestamp of when this shard was built.
    pub built_at: String,
}

/// Multi-shard catalog reader. Lookups query each shard in order
/// (newest last) — last hit wins, so an `append` rebuild of an
/// existing accession overrides the original.
pub struct CatalogReader {
    /// Shards in manifest order. Oldest first; newest last.
    shards: Vec<ShardReader>,
}

impl CatalogReader {
    /// Open a catalog directory. Reads `manifest.json` and opens
    /// every shard listed.
    pub async fn open_local(catalog_dir: &Path) -> Result<Self> {
        let manifest_path = catalog_dir.join("manifest.json");
        // Backwards compat: if there's no manifest.json but the dir
        // contains accessions.vortex, treat the dir as a single
        // shard catalog. Lets old single-shard outputs keep working
        // through the multi-shard reader.
        if !manifest_path.exists() && catalog_dir.join("accessions.vortex").exists() {
            let shard = ShardReader::open_local(catalog_dir).await?;
            return Ok(Self {
                shards: vec![shard],
            });
        }
        let manifest_bytes = std::fs::read(&manifest_path)
            .map_err(|e| Error::Reader(format!("read manifest: {e}")))?;
        let manifest: Manifest = serde_json::from_slice(&manifest_bytes)?;
        if manifest.version != 1 {
            return Err(Error::Reader(format!(
                "unsupported manifest version {}",
                manifest.version
            )));
        }
        let mut shards = Vec::with_capacity(manifest.shards.len());
        for entry in &manifest.shards {
            let path = catalog_dir.join(&entry.path);
            shards.push(ShardReader::open_local(&path).await?);
        }
        Ok(Self { shards })
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Number of accessions across all shards. Note: if an
    /// accession appears in multiple shards (e.g. base + delta
    /// rebuild) the count is per-shard, not deduped.
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.shards.iter().all(|s| s.is_empty())
    }

    /// Point lookup. Walks shards newest-to-oldest; first hit wins.
    /// (manifest order is oldest-first; we iterate in reverse.)
    pub async fn lookup(&self, accession: &str) -> Result<Option<AccessionRecord>> {
        for shard in self.shards.iter().rev() {
            if let Some(rec) = shard.lookup(accession).await? {
                return Ok(Some(rec));
            }
        }
        Ok(None)
    }
}
