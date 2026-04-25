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
use vortex::file::WriteOptionsSessionExt;
use vortex::session::VortexSession;

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
    /// write the shard as a directory with three files:
    /// `accessions.vortex`, `blobs.vortex`, `schemas.vortex`.
    /// Each file holds one StructArray; row counts differ across
    /// tables so they can't share a single top-level struct.
    pub async fn finish(self, session: &VortexSession) -> Result<WriteSummary> {
        std::fs::create_dir_all(&self.path)?;
        let accessions_array = build_accessions_array(&self.records, &self.schemas)?;
        let blobs_array = build_blobs_array(&self.records)?;
        let schemas_array = build_schemas_array(&self.schemas)?;

        let mut total_bytes = 0;
        for (name, array) in [
            ("accessions.vortex", accessions_array),
            ("blobs.vortex", blobs_array),
            ("schemas.vortex", schemas_array),
        ] {
            let mut buf = ByteBufferMut::empty();
            session
                .write_options()
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
    // Optional fields are flattened to (value + present-flag) columns
    // for v0 — simpler than threading Vortex Validity through each
    // builder. Reader unflattens via the *_present column.
    let n = records.len();
    let mut accession_id_bytes: Vec<Vec<u8>> = Vec::with_capacity(n);
    let mut file_size: Vec<u64> = Vec::with_capacity(n);
    let mut spots: Vec<u64> = Vec::with_capacity(n);
    let mut spots_present: Vec<u8> = Vec::with_capacity(n);
    let mut kar_data_offset: Vec<u64> = Vec::with_capacity(n);
    let mut schema_id: Vec<u32> = Vec::with_capacity(n);
    let mut layout_byte: Vec<u8> = Vec::with_capacity(n);
    let mut platform_byte: Vec<u8> = Vec::with_capacity(n);
    // md5: stored as a VarBin column with one entry per row. Each
    // entry is either exactly 16 bytes (present) or empty (missing).
    // Reader pairs this with `md5_present` to disambiguate.
    let mut md5_rows: Vec<Vec<u8>> = Vec::with_capacity(n);
    let mut md5_present: Vec<u8> = Vec::with_capacity(n);

    for r in records {
        accession_id_bytes.push(r.accession.as_bytes().to_vec());
        file_size.push(r.file_size);
        spots.push(r.spots.unwrap_or(0));
        spots_present.push(u8::from(r.spots.is_some()));
        kar_data_offset.push(r.kar_data_offset);
        let sid = schemas
            .get(&r.schema_fingerprint)
            .map(|(id, _)| *id)
            .unwrap_or(u32::MAX);
        schema_id.push(sid);
        layout_byte.push(layout_to_u8(r.layout));
        platform_byte.push(platform_to_u8(r.platform));
        match r.md5 {
            Some(m) => {
                md5_rows.push(m.to_vec());
                md5_present.push(1);
            }
            None => {
                md5_rows.push(Vec::new());
                md5_present.push(0);
            }
        }
    }

    let acc_arr =
        VarBinArray::from_vec(accession_id_bytes, DType::Utf8(Nullability::NonNullable))
            .into_array();
    let fs_arr: PrimitiveArray = file_size.into_iter().collect();
    let kar_arr: PrimitiveArray = kar_data_offset.into_iter().collect();
    let sch_arr: PrimitiveArray = schema_id.into_iter().collect();
    let lay_arr: PrimitiveArray = layout_byte.into_iter().collect();
    let plat_arr: PrimitiveArray = platform_byte.into_iter().collect();
    let spots_arr: PrimitiveArray = spots.into_iter().collect();
    let spots_present_arr: PrimitiveArray = spots_present.into_iter().collect();
    let md5_arr =
        VarBinArray::from_vec(md5_rows, DType::Binary(Nullability::NonNullable)).into_array();
    let md5_present_arr: PrimitiveArray = md5_present.into_iter().collect();

    let fields: [(&str, ArrayRef); 10] = [
        ("accession", acc_arr),
        ("file_size", fs_arr.into_array()),
        ("spots", spots_arr.into_array()),
        ("spots_present", spots_present_arr.into_array()),
        ("kar_data_offset", kar_arr.into_array()),
        ("schema_id", sch_arr.into_array()),
        ("layout", lay_arr.into_array()),
        ("platform", plat_arr.into_array()),
        ("md5_bytes", md5_arr),
        ("md5_present", md5_present_arr.into_array()),
    ];

    Ok(StructArray::from_fields(&fields)
        .map_err(|e| Error::Writer(format!("accessions struct: {e}")))?
        .into_array())
}

fn build_blobs_array(records: &[AccessionRecord]) -> Result<ArrayRef> {
    // Estimate capacity: typical accession has ~50-200 blob rows.
    let capacity = records.iter().map(|r| r.blobs.len()).sum::<usize>();
    let mut accession_idx: Vec<u32> = Vec::with_capacity(capacity);
    let mut column_id: Vec<u8> = Vec::with_capacity(capacity);
    let mut blob_idx: Vec<u32> = Vec::with_capacity(capacity);
    let mut start_id: Vec<i64> = Vec::with_capacity(capacity);
    let mut id_range: Vec<u32> = Vec::with_capacity(capacity);
    let mut blob_offset: Vec<u64> = Vec::with_capacity(capacity);
    let mut blob_size: Vec<u32> = Vec::with_capacity(capacity);
    let mut pg: Vec<u64> = Vec::with_capacity(capacity);

    for (i, r) in records.iter().enumerate() {
        let idx = u32::try_from(i).map_err(|_| Error::Writer("accession idx overflow".into()))?;
        for b in &r.blobs {
            accession_idx.push(idx);
            column_id.push(b.column_id);
            blob_idx.push(b.blob_idx);
            start_id.push(b.start_id);
            id_range.push(b.id_range);
            blob_offset.push(b.blob_offset);
            blob_size.push(b.blob_size);
            pg.push(b.pg);
        }
    }

    let acc_idx_arr: PrimitiveArray = accession_idx.into_iter().collect();
    let col_arr: PrimitiveArray = column_id.into_iter().collect();
    let bi_arr: PrimitiveArray = blob_idx.into_iter().collect();
    let sid_arr: PrimitiveArray = start_id.into_iter().collect();
    let ir_arr: PrimitiveArray = id_range.into_iter().collect();
    let bo_arr: PrimitiveArray = blob_offset.into_iter().collect();
    let bs_arr: PrimitiveArray = blob_size.into_iter().collect();
    let pg_arr: PrimitiveArray = pg.into_iter().collect();

    let fields: [(&str, ArrayRef); 8] = [
        ("accession_idx", acc_idx_arr.into_array()),
        ("column_id", col_arr.into_array()),
        ("blob_idx", bi_arr.into_array()),
        ("start_id", sid_arr.into_array()),
        ("id_range", ir_arr.into_array()),
        ("blob_offset", bo_arr.into_array()),
        ("blob_size", bs_arr.into_array()),
        ("pg", pg_arr.into_array()),
    ];

    Ok(StructArray::from_fields(&fields)
        .map_err(|e| Error::Writer(format!("blobs struct: {e}")))?
        .into_array())
}

fn build_schemas_array(
    schemas: &HashMap<[u8; 32], (u32, SchemaEntry)>,
) -> Result<ArrayRef> {
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
    let fp_arr =
        VarBinArray::from_vec(fingerprint_rows, DType::Binary(Nullability::NonNullable)).into_array();
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
