//! Vortex shard writer.
//!
//! Take a stream of [`AccessionRecord`]s, dedupe schemas on the fly,
//! and write a single Vortex shard file with three top-level fields:
//! `accessions`, `blobs`, `schemas`. Queries on the resulting file
//! can scan one field without materializing the others.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};
use vortex::array::arrays::{PrimitiveArray, StructArray, VarBinArray};
use vortex::array::{ArrayRef, IntoArray};
use vortex::buffer::ByteBufferMut;
use vortex::dtype::{DType, Nullability};
use vortex::file::{WriteOptionsSessionExt, WriteStrategyBuilder};
use vortex::session::VortexSession;
use vortex_btrblocks::BtrBlocksCompressorBuilder;

use crate::record::{AccessionRecord, ColumnMetaEntry, SchemaEntry};
use crate::{Error, Result};

pub struct ShardWriter {
    path: PathBuf,
    records: Vec<AccessionRecord>,
    /// fingerprint → (schema_id, schema content). The first record
    /// to introduce a fingerprint owns that schema_id.
    schemas: HashMap<[u8; 32], (u32, SchemaEntry)>,
    next_schema_id: u32,
}

impl ShardWriter {
    pub fn create(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        Ok(Self {
            path: path.to_path_buf(),
            records: Vec::new(),
            schemas: HashMap::new(),
            next_schema_id: 0,
        })
    }

    /// Append one record to the in-memory buffer. Schema dedup
    /// happens here so subsequent calls share the same schema_id.
    pub fn append(&mut self, mut record: AccessionRecord) -> Result<()> {
        let fp = record.schema_fingerprint;
        let entry = self.schemas.entry(fp).or_insert_with(|| {
            let id = self.next_schema_id;
            // bump must happen here lazily; we can't borrow self
            // mutably twice, so the bump happens below.
            (id, record.schema.clone())
        });
        if entry.0 == self.next_schema_id {
            // newly inserted — bump.
            self.next_schema_id += 1;
        }
        // We don't store schema_id on the record itself in the in-memory
        // form; it's resolved at finish() time when we materialize the
        // accessions table.
        record.schema = SchemaEntry {
            fingerprint: fp,
            columns: Vec::new(), // dropped — present in schemas table.
            is_csra: record.schema.is_csra,
        };
        self.records.push(record);
        Ok(())
    }

    /// Materialize the in-memory records into Vortex arrays and
    /// write the shard as a directory of .vortex files.
    ///
    /// Schema (v4 ruthless: no per-blob storage):
    /// - `accessions.vortex` — one row per accession.
    /// - `col_extents.vortex` — one row per (accession, column).
    ///   Carries `n_blobs`, `data_slab_offset`, `data_slab_size`,
    ///   `first_start_id`, `uniform_id_range`. NO per-blob data
    ///   stored.
    /// - `schemas.vortex` — deduped column-layout templates.
    ///
    /// Reader can compute approximate blob boundaries assuming
    /// uniform per-blob size (`data_slab_size / n_blobs`) — good
    /// enough for streaming-download chunk-priority planning. For
    /// EXACT blob boundaries (decode), sracha re-fetches the (tiny,
    /// few-KB) idx files from S3 directly. The catalog tells it
    /// where to find them via `data_slab_offset`.
    /// Write the shard's three .vortex files into `self.path` and
    /// optionally update `manifest_path`'s shard list. Pass `None`
    /// for manifest_path on tests / one-off runs that don't need a
    /// catalog directory.
    pub async fn finish_with_manifest(
        self,
        session: &VortexSession,
        manifest_path: Option<&std::path::Path>,
        shard_name: &str,
        shard_relative_path: &str,
    ) -> Result<WriteSummary> {
        let summary = self.finish(session).await?;
        if let Some(mpath) = manifest_path {
            update_manifest(mpath, shard_name, shard_relative_path, summary.n_accessions)?;
        }
        Ok(summary)
    }

    pub async fn finish(self, session: &VortexSession) -> Result<WriteSummary> {
        std::fs::create_dir_all(&self.path)?;
        let accessions_array = build_accessions_array(&self.records, &self.schemas)?;
        let col_extents_array = build_col_extents(&self.records)?;
        let schemas_array = build_schemas_array(&self.schemas)?;

        // Compact-mode BtrBlocks compressor: enables aggressive
        // cascaded encodings (FoR + Delta + Dict + RLE + bit-pack).
        let compressor = BtrBlocksCompressorBuilder::default().with_compact().build();
        let mut total_bytes = 0;
        for (name, array) in [
            ("accessions.vortex", accessions_array),
            ("col_extents.vortex", col_extents_array),
            ("schemas.vortex", schemas_array),
        ] {
            // Strategy is one-shot — `build()` consumes the builder,
            // so re-build for each shard file.
            let strategy = WriteStrategyBuilder::default()
                .with_compressor(compressor.clone())
                .build();
            let mut buf = ByteBufferMut::empty();
            session
                .write_options()
                .with_strategy(strategy)
                .write(&mut buf, array.to_array_stream())
                .await
                .map_err(|e| Error::Writer(format!("vortex write {name}: {e}")))?;
            let bytes = buf.freeze();
            total_bytes += bytes.len();
            std::fs::write(self.path.join(name), bytes.as_slice())?;
        }

        Ok(WriteSummary {
            path: self.path,
            n_accessions: self.records.len(),
            n_schemas: self.schemas.len(),
            bytes: total_bytes,
        })
    }
}

#[derive(Debug, Clone)]
pub struct WriteSummary {
    pub path: PathBuf,
    pub n_accessions: usize,
    pub n_schemas: usize,
    pub bytes: usize,
}

// --- table builders ------------------------------------------------------

fn build_accessions_array(
    records: &[AccessionRecord],
    schemas: &HashMap<[u8; 32], (u32, SchemaEntry)>,
) -> Result<ArrayRef> {
    let n = records.len();
    let mut accession_idx: Vec<u32> = Vec::with_capacity(n);
    let mut accession_id_bytes: Vec<Vec<u8>> = Vec::with_capacity(n);
    let mut file_size: Vec<u64> = Vec::with_capacity(n);
    let mut kar_data_offset: Vec<u64> = Vec::with_capacity(n);
    let mut schema_id: Vec<u32> = Vec::with_capacity(n);
    // Newly populated by the extractor (md/cur parse).
    let mut spots: Vec<u64> = Vec::with_capacity(n);
    let mut spots_present: Vec<u8> = Vec::with_capacity(n);
    let mut layout: Vec<u8> = Vec::with_capacity(n);
    let mut platform: Vec<u8> = Vec::with_capacity(n);
    // read_lengths is a small list (often 1-2 u32 entries). Encode
    // as a JSON string per row — Vortex's BtrBlocks dictionary will
    // collapse the few common patterns ([150,150], [100,100], …)
    // to ~zero bytes. Avoids wiring a ListArray for what's typically
    // 4-8 bytes raw.
    let mut read_lengths_json: Vec<Vec<u8>> = Vec::with_capacity(n);

    for (i, r) in records.iter().enumerate() {
        accession_idx
            .push(u32::try_from(i).map_err(|_| Error::Writer("accession idx overflow".into()))?);
        accession_id_bytes.push(r.accession.as_bytes().to_vec());
        file_size.push(r.file_size);
        kar_data_offset.push(r.kar_data_offset);
        let sid = schemas
            .get(&r.schema_fingerprint)
            .map(|(id, _)| *id)
            .unwrap_or(u32::MAX);
        schema_id.push(sid);
        match r.spots {
            Some(v) => {
                spots.push(v);
                spots_present.push(1);
            }
            None => {
                spots.push(0);
                spots_present.push(0);
            }
        }
        layout.push(layout_to_u8(r.layout));
        platform.push(platform_to_u8(r.platform));
        read_lengths_json.push(serde_json::to_vec(&r.read_lengths)?);
    }

    let acc_idx_arr: PrimitiveArray = accession_idx.into_iter().collect();
    let acc_arr = VarBinArray::from_vec(accession_id_bytes, DType::Utf8(Nullability::NonNullable))
        .into_array();
    let fs_arr: PrimitiveArray = file_size.into_iter().collect();
    let kar_arr: PrimitiveArray = kar_data_offset.into_iter().collect();
    let sch_arr: PrimitiveArray = schema_id.into_iter().collect();
    let spots_arr: PrimitiveArray = spots.into_iter().collect();
    let spots_present_arr: PrimitiveArray = spots_present.into_iter().collect();
    let layout_arr: PrimitiveArray = layout.into_iter().collect();
    let platform_arr: PrimitiveArray = platform.into_iter().collect();
    let read_lens_arr =
        VarBinArray::from_vec(read_lengths_json, DType::Utf8(Nullability::NonNullable))
            .into_array();

    let fields: [(&str, ArrayRef); 10] = [
        ("accession_idx", acc_idx_arr.into_array()),
        ("accession", acc_arr),
        ("file_size", fs_arr.into_array()),
        ("kar_data_offset", kar_arr.into_array()),
        ("schema_id", sch_arr.into_array()),
        ("spots", spots_arr.into_array()),
        ("spots_present", spots_present_arr.into_array()),
        ("layout", layout_arr.into_array()),
        ("platform", platform_arr.into_array()),
        ("read_lengths_json", read_lens_arr),
    ];

    Ok(StructArray::from_fields(&fields)
        .map_err(|e| Error::Writer(format!("accessions struct: {e}")))?
        .into_array())
}

/// Build the col_extents table (one row per (accession, column)),
/// with no per-blob data — only summary metadata sufficient to
/// compute approximate blob boundaries.
///
/// For exact decode, sracha re-fetches the idx files from S3 using
/// `data_slab_offset` as the column data slab base.
fn build_col_extents(records: &[AccessionRecord]) -> Result<ArrayRef> {
    let total_extents: usize = records
        .iter()
        .map(|r| {
            let mut cols: std::collections::HashSet<u8> = std::collections::HashSet::new();
            for b in &r.blobs {
                cols.insert(b.column_id);
            }
            cols.len()
        })
        .sum();

    let mut ext_acc_idx: Vec<u32> = Vec::with_capacity(total_extents);
    let mut ext_col_id: Vec<u8> = Vec::with_capacity(total_extents);
    let mut ext_n_blobs: Vec<u32> = Vec::with_capacity(total_extents);
    let mut ext_data_slab_offset: Vec<u64> = Vec::with_capacity(total_extents);
    let mut ext_data_slab_size: Vec<u64> = Vec::with_capacity(total_extents);
    let mut ext_first_start_id: Vec<i64> = Vec::with_capacity(total_extents);
    let mut ext_uniform_id_range: Vec<u32> = Vec::with_capacity(total_extents);

    for (i, r) in records.iter().enumerate() {
        let idx = u32::try_from(i).map_err(|_| Error::Writer("accession idx overflow".into()))?;

        let mut by_col: std::collections::BTreeMap<u8, Vec<&crate::record::BlobLocator>> =
            std::collections::BTreeMap::new();
        for b in &r.blobs {
            by_col.entry(b.column_id).or_default().push(b);
        }

        for (col_id, mut blobs) in by_col {
            blobs.sort_by_key(|b| b.start_id);
            if blobs.is_empty() {
                continue;
            }
            let first = blobs[0];
            let last = blobs.last().unwrap();
            let uniform_ir = if blobs.iter().all(|b| b.id_range == first.id_range) {
                first.id_range
            } else {
                tracing::debug!(
                    "{}: column_id {col_id} has non-uniform id_range",
                    r.accession,
                );
                0
            };
            // Data slab spans from the first blob's offset to the
            // end of the last blob.
            let data_slab_offset = first.blob_offset;
            let data_slab_size =
                (last.blob_offset + u64::from(last.blob_size)).saturating_sub(first.blob_offset);
            ext_acc_idx.push(idx);
            ext_col_id.push(col_id);
            ext_n_blobs.push(
                u32::try_from(blobs.len())
                    .map_err(|_| Error::Writer("blob count overflow".into()))?,
            );
            ext_data_slab_offset.push(data_slab_offset);
            ext_data_slab_size.push(data_slab_size);
            ext_first_start_id.push(first.start_id);
            ext_uniform_id_range.push(uniform_ir);
        }
    }

    let ext_acc_arr: PrimitiveArray = ext_acc_idx.into_iter().collect();
    let ext_col_arr: PrimitiveArray = ext_col_id.into_iter().collect();
    let ext_n_arr: PrimitiveArray = ext_n_blobs.into_iter().collect();
    let ext_off_arr: PrimitiveArray = ext_data_slab_offset.into_iter().collect();
    let ext_size_arr: PrimitiveArray = ext_data_slab_size.into_iter().collect();
    let ext_sid_arr: PrimitiveArray = ext_first_start_id.into_iter().collect();
    let ext_ir_arr: PrimitiveArray = ext_uniform_id_range.into_iter().collect();

    let extents_fields: [(&str, ArrayRef); 7] = [
        ("accession_idx", ext_acc_arr.into_array()),
        ("column_id", ext_col_arr.into_array()),
        ("n_blobs", ext_n_arr.into_array()),
        ("data_slab_offset", ext_off_arr.into_array()),
        ("data_slab_size", ext_size_arr.into_array()),
        ("first_start_id", ext_sid_arr.into_array()),
        ("uniform_id_range", ext_ir_arr.into_array()),
    ];
    Ok(StructArray::from_fields(&extents_fields)
        .map_err(|e| Error::Writer(format!("col_extents struct: {e}")))?
        .into_array())
}

fn build_schemas_array(schemas: &HashMap<[u8; 32], (u32, SchemaEntry)>) -> Result<ArrayRef> {
    let mut entries: Vec<&(u32, SchemaEntry)> = schemas.values().collect();
    entries.sort_by_key(|(id, _)| *id);

    let mut schema_id: Vec<u32> = Vec::with_capacity(entries.len());
    let mut fingerprint_rows: Vec<Vec<u8>> = Vec::with_capacity(entries.len());
    let mut is_csra: Vec<u8> = Vec::with_capacity(entries.len());
    // For columns: stash a json blob per schema. Compressing JSON
    // through Vortex isn't great, but it's expedient for v0; later
    // we'll normalize columns into their own table with a `columns`
    // FK back to `schemas`.
    let mut columns_json: Vec<String> = Vec::with_capacity(entries.len());

    for (id, schema) in &entries {
        schema_id.push(*id);
        fingerprint_rows.push(schema.fingerprint.to_vec());
        is_csra.push(u8::from(schema.is_csra));
        let cols_json = serde_json::to_string(&schema.columns)?;
        columns_json.push(cols_json);
    }

    let id_arr: PrimitiveArray = schema_id.into_iter().collect();
    let fp_arr = VarBinArray::from_vec(fingerprint_rows, DType::Binary(Nullability::NonNullable))
        .into_array();
    let csra_arr: PrimitiveArray = is_csra.into_iter().collect();
    let cols_arr = VarBinArray::from_vec(
        columns_json.into_iter().map(String::into_bytes).collect(),
        DType::Utf8(Nullability::NonNullable),
    )
    .into_array();

    let fields: [(&str, ArrayRef); 4] = [
        ("schema_id", id_arr.into_array()),
        ("fingerprint", fp_arr),
        ("is_csra", csra_arr.into_array()),
        ("columns_json", cols_arr),
    ];

    Ok(StructArray::from_fields(&fields)
        .map_err(|e| Error::Writer(format!("schemas struct: {e}")))?
        .into_array())
}

// --- helpers -------------------------------------------------------------

/// Append (or create) a `manifest.json` entry pointing at the new
/// shard. Idempotent on shard_name — re-builds replace the entry.
fn update_manifest(
    manifest_path: &std::path::Path,
    shard_name: &str,
    shard_relative_path: &str,
    n_accessions: usize,
) -> Result<()> {
    use crate::reader::{Manifest, ShardEntry};
    let mut manifest = if manifest_path.exists() {
        let bytes = std::fs::read(manifest_path)?;
        serde_json::from_slice::<Manifest>(&bytes)?
    } else {
        Manifest {
            version: 1,
            shards: Vec::new(),
        }
    };

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let entry = ShardEntry {
        name: shard_name.to_string(),
        path: shard_relative_path.to_string(),
        n_accessions: n_accessions as u64,
        built_at: format!("epoch:{now}"),
    };

    // Replace existing entry with the same name, else append.
    if let Some(slot) = manifest.shards.iter_mut().find(|s| s.name == shard_name) {
        *slot = entry;
    } else {
        manifest.shards.push(entry);
    }

    let body = serde_json::to_vec_pretty(&manifest)?;
    std::fs::write(manifest_path, body)?;
    Ok(())
}

fn layout_to_u8(layout: crate::record::Layout) -> u8 {
    use crate::record::Layout::*;
    match layout {
        Single => 1,
        Paired => 2,
        Unknown => 0,
    }
}

fn platform_to_u8(platform: crate::record::Platform) -> u8 {
    use crate::record::Platform::*;
    match platform {
        Illumina => 1,
        PacBio => 2,
        OxfordNanopore => 3,
        IonTorrent => 4,
        Other => 0,
    }
}

/// SHA256 fingerprint of a list of [`ColumnMetaEntry`]s — used for
/// schema dedup. Public so tests can assert determinism.
pub fn schema_fingerprint(columns: &[ColumnMetaEntry]) -> [u8; 32] {
    let mut h = Sha256::new();
    for c in columns {
        h.update(c.name.as_bytes());
        h.update(c.version.to_le_bytes());
        h.update(c.codec.to_le_bytes());
        h.update(c.page_size.to_le_bytes());
    }
    h.finalize().into()
}
