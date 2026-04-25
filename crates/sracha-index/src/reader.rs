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

use crate::record::{
    AccessionRecord, BlobLocator, ColumnMetaEntry, Layout, Platform, SchemaEntry,
};
use crate::{Error, Result};

pub struct CatalogReader {
    session: VortexSession,
    accessions_file: VortexFile,
    col_extents_file: VortexFile,
    schemas_file: VortexFile,
}

impl CatalogReader {
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
        // Fields not stored yet (extractor TODOs): layout, platform,
        // spots, md5. Fill placeholders.
        let layout = Layout::Unknown;
        let platform = Platform::Other;
        let spots = None;
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
            read_lengths: Vec::new(),
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
        let fingerprint: [u8; 32] = fp_bytes.as_slice().try_into().map_err(|_| {
            Error::Reader(format!("fingerprint length {} != 32", fp_bytes.len()))
        })?;
        let is_csra = field_u8(&s, "is_csra")? != 0;
        let cols_json = field_string(&s, "columns_json")?;
        let columns: Vec<ColumnMetaEntry> = serde_json::from_str(&cols_json)?;
        Ok(SchemaEntry {
            fingerprint,
            columns,
            is_csra,
        })
    }

    async fn collect_blobs_for_accession(
        &self,
        accession_idx: u32,
    ) -> Result<Vec<BlobLocator>> {
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
                let r = r.as_struct_opt().ok_or_else(|| {
                    Error::Reader("col_extents row not a struct".into())
                })?;
                let column_id = field_u8(&r, "column_id")?;
                let n_blobs = field_u32(&r, "n_blobs")?;
                let data_slab_offset = field_u64(&r, "data_slab_offset")?;
                let data_slab_size = field_u64(&r, "data_slab_size")?;
                let first_start_id = field_i64(&r, "first_start_id")?;
                let uniform_id_range = field_u32(&r, "uniform_id_range")?;
                if n_blobs == 0 {
                    continue;
                }
                let approx_size = u32::try_from(data_slab_size / u64::from(n_blobs))
                    .unwrap_or(u32::MAX);
                for i in 0..n_blobs {
                    let blob_offset =
                        data_slab_offset + u64::from(i) * u64::from(approx_size);
                    let start_id = first_start_id
                        + i64::from(i) * i64::from(uniform_id_range);
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
        if chunk.len() > 0 {
            return Ok(Some(chunk.slice(0..1).map_err(vortex_err)?));
        }
    }
    Ok(None)
}

// Field accessors take a StructScalar by value (it's a thin view).

fn field_string(
    s: &vortex::array::scalar::StructScalar,
    name: &str,
) -> Result<String> {
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

fn field_binary(
    s: &vortex::array::scalar::StructScalar,
    name: &str,
) -> Result<Vec<u8>> {
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
