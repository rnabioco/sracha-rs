//! Convert an SRA file into a Vortex file.
//!
//! Same Arrow `RecordBatch` stream the Parquet writer produces; only the sink
//! differs. Vortex picks its own encoding cascade — there is no `compression`
//! knob here, by design (Issue #9).
//!
//! v1 scope: bulk columns only (READ, QUALITY, READ_LEN, NAME). Matches the
//! Parquet v1 schema so file sizes are directly comparable.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef as ArrowArrayRef, BinaryArray, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use tokio::runtime::Builder as RuntimeBuilder;

use vortex::VortexSessionDefault;
use vortex::array::ArrayRef;
use vortex::array::arrow::FromArrowArray;
use vortex::file::WriteOptionsSessionExt;
use vortex::session::VortexSession;

use crate::error::{Error, Result};
use crate::parquet::schema::{DnaPacking, LengthMode, build_per_read_schema};
use crate::parquet::writer::{
    BatchBuilder, LengthModeChoice, decode_one_blob, resolve_length_mode,
};
use crate::vdb::cursor::VdbCursor;
use crate::vdb::kar::KarArchive;

// ---------------------------------------------------------------------------
// Public configuration
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct VortexConvertConfig {
    pub pack_dna: DnaPacking,
    pub length_mode: LengthModeChoice,
    /// Number of blobs to decode per Arrow `RecordBatch`.
    pub blobs_per_batch: usize,
}

impl Default for VortexConvertConfig {
    fn default() -> Self {
        Self {
            pack_dna: DnaPacking::TwoNa,
            length_mode: LengthModeChoice::Auto,
            blobs_per_batch: 64,
        }
    }
}

#[derive(Debug, Clone)]
pub struct VortexConvertStats {
    pub spots: u64,
    pub reads: u64,
    pub input_bytes: u64,
    pub output_bytes: u64,
    pub output_path: PathBuf,
    pub length_mode: LengthMode,
    pub dna_packing: DnaPacking,
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Convert an SRA file at `sra_path` into a Vortex file at `output_path`.
pub fn convert_sra_to_vortex(
    sra_path: &Path,
    output_path: &Path,
    config: &VortexConvertConfig,
) -> Result<VortexConvertStats> {
    let input_bytes = std::fs::metadata(sra_path)?.len();

    let file = File::open(sra_path)?;
    let mut archive = KarArchive::open(std::io::BufReader::new(file))?;
    let cursor = VdbCursor::open(&mut archive, sra_path)?;

    let length_mode = resolve_length_mode(&cursor, config.length_mode)?;
    let pack_dna = config.pack_dna;
    let schema = build_per_read_schema(length_mode, pack_dna);

    tracing::debug!(
        "vortex: length_mode={:?}, pack_dna={:?}",
        length_mode,
        pack_dna
    );

    // ---- per-blob iteration into RecordBatches -----------------------------
    let batches = collect_record_batches(&cursor, schema.clone(), length_mode, pack_dna, config)?;
    let (spots, reads) = count_spots_and_reads(&batches);

    // ---- write out via Vortex ---------------------------------------------
    write_batches_to_vortex(output_path, schema, batches)?;

    let output_bytes = std::fs::metadata(output_path)?.len();
    Ok(VortexConvertStats {
        spots,
        reads,
        input_bytes,
        output_bytes,
        output_path: output_path.to_path_buf(),
        length_mode,
        dna_packing: pack_dna,
    })
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

fn collect_record_batches(
    cursor: &VdbCursor,
    schema: Arc<arrow::datatypes::Schema>,
    length_mode: LengthMode,
    pack_dna: DnaPacking,
    config: &VortexConvertConfig,
) -> Result<Vec<RecordBatch>> {
    let read_cs = cursor.read_col().meta().checksum_type;
    let blob_infos = cursor.read_col().blobs().to_vec();
    let quality_cs = cursor.quality_col().map_or(0, |c| c.meta().checksum_type);
    let read_len_cs = cursor.read_len_col().map_or(0, |c| c.meta().checksum_type);
    let name_cs = cursor.name_col().map_or(0, |c| c.meta().checksum_type);

    let mut spot_id_acc: u64 = cursor.first_row().max(1) as u64;
    let mut batch_builder = BatchBuilder::new(schema, length_mode, pack_dna);
    let mut out: Vec<RecordBatch> = Vec::new();

    for (blob_idx, blob_info) in blob_infos.iter().enumerate() {
        let start_row = blob_info.start_id;
        let id_range = blob_info.id_range as u64;

        let read_raw = cursor.read_col().read_raw_blob_slice(start_row)?;
        let quality_raw = cursor
            .quality_col()
            .filter(|c| blob_idx < c.blob_count())
            .map(|c| c.read_raw_blob_slice(start_row))
            .transpose()?
            .unwrap_or(&[]);
        let read_len_raw = cursor
            .read_len_col()
            .filter(|c| blob_idx < c.blob_count())
            .map(|c| c.read_raw_blob_slice(start_row))
            .transpose()?
            .unwrap_or(&[]);
        let name_raw = cursor
            .name_col()
            .filter(|c| blob_idx < c.blob_count())
            .map(|c| c.read_raw_blob_slice(start_row))
            .transpose()?
            .unwrap_or(&[]);

        let decoded = decode_one_blob(
            read_raw,
            read_cs,
            id_range,
            quality_raw,
            quality_cs,
            read_len_raw,
            read_len_cs,
            name_raw,
            name_cs,
        )?;

        let n_spots = decoded.spot_count();
        for (spot_offset, spot) in decoded.iter_spots().enumerate() {
            let spot_id = spot_id_acc + spot_offset as u64;
            for (read_num, read) in spot.iter_reads().enumerate() {
                batch_builder.push(
                    spot_id,
                    read_num as u8,
                    spot.name,
                    read.sequence,
                    read.quality,
                );
            }
        }
        spot_id_acc += n_spots as u64;

        if batch_builder.len() >= config.blobs_per_batch * 1024 {
            out.push(batch_builder.finish()?);
        }
    }

    if !batch_builder.is_empty() {
        out.push(batch_builder.finish()?);
    }

    Ok(out)
}

fn count_spots_and_reads(batches: &[RecordBatch]) -> (u64, u64) {
    let reads: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
    let mut spots: u64 = 0;
    for b in batches {
        if let Some(col) = b.column_by_name("spot_id")
            && let Some(arr) = col.as_any().downcast_ref::<arrow::array::UInt64Array>()
        {
            let mut prev: Option<u64> = None;
            for i in 0..arr.len() {
                let v = arr.value(i);
                if Some(v) != prev {
                    spots += 1;
                    prev = Some(v);
                }
            }
        }
    }
    (spots, reads)
}

fn write_batches_to_vortex(
    output_path: &Path,
    schema: Arc<arrow::datatypes::Schema>,
    batches: Vec<RecordBatch>,
) -> Result<()> {
    if batches.is_empty() {
        return Err(Error::Vdb("vortex: no batches to write".into()));
    }
    // Concatenate all RecordBatches into a single batch, then convert to a
    // single Vortex StructArray. Vortex's default BtrBlocks cascade only
    // applies FSST/dict to Utf8 columns, so we reinterpret Binary columns as
    // Utf8 where the bytes happen to be valid UTF-8 (true for ASCII sequence
    // and for all quality values). For 2na/4na-packed sequence the bytes
    // aren't valid UTF-8, so those stay Binary (and uncompressed — but
    // already dense).
    let batch_refs: Vec<&RecordBatch> = batches.iter().collect();
    let concat = arrow::compute::concat_batches(&schema, batch_refs)
        .map_err(|e| Error::Vdb(format!("arrow concat_batches: {e}")))?;
    let concat = reinterpret_binary_as_utf8_where_possible(&concat)?;

    let array: ArrayRef = ArrayRef::from_arrow(concat, /* nullable = */ false)
        .map_err(|e| Error::Vdb(format!("vortex from_arrow: {e}")))?;

    let output_path = output_path.to_path_buf();
    // Vortex's write API is async; we may be called from within a tokio
    // runtime already (sracha's CLI is #[tokio::main]), so `block_on` on an
    // ambient runtime panics. Spawn a dedicated std thread with its own
    // current-thread runtime to keep the writer sync from the caller's POV.
    std::thread::spawn(move || {
        let runtime = RuntimeBuilder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| Error::Vdb(format!("vortex tokio runtime: {e}")))?;
        runtime.block_on(async move {
            let session = VortexSession::default();
            let mut file = tokio::fs::File::create(&output_path)
                .await
                .map_err(|e| Error::Vdb(format!("vortex create {}: {e}", output_path.display())))?;
            session
                .write_options()
                .write(&mut file, array.to_array_stream())
                .await
                .map_err(|e| Error::Vdb(format!("vortex write: {e}")))?;
            Ok::<(), Error>(())
        })
    })
    .join()
    .map_err(|_| Error::Vdb("vortex writer thread panicked".into()))?
}

/// Rewrite a RecordBatch so every `Binary` column whose rows are all valid
/// UTF-8 becomes a `Utf8` column. This lets Vortex's BtrBlocks cascade apply
/// FSST/dict compression (it gates on `DType::Utf8`).
fn reinterpret_binary_as_utf8_where_possible(batch: &RecordBatch) -> Result<RecordBatch> {
    let old_schema = batch.schema();
    let n = batch.num_columns();
    let mut new_fields: Vec<Field> = Vec::with_capacity(n);
    let mut new_cols: Vec<ArrowArrayRef> = Vec::with_capacity(n);

    for (i, field) in old_schema.fields().iter().enumerate() {
        let col = batch.column(i);
        match (
            field.data_type(),
            col.as_any().downcast_ref::<BinaryArray>(),
        ) {
            (DataType::Binary, Some(bin)) if all_rows_valid_utf8(bin) => {
                let mut b =
                    arrow::array::StringBuilder::with_capacity(bin.len(), bin.value_data().len());
                for row in 0..bin.len() {
                    if bin.is_null(row) {
                        b.append_null();
                    } else {
                        // Safety check above guarantees valid UTF-8.
                        b.append_value(std::str::from_utf8(bin.value(row)).unwrap());
                    }
                }
                let s: StringArray = b.finish();
                new_cols.push(std::sync::Arc::new(s) as ArrowArrayRef);
                new_fields.push(Field::new(
                    field.name(),
                    DataType::Utf8,
                    field.is_nullable(),
                ));
            }
            _ => {
                new_cols.push(col.clone());
                new_fields.push(field.as_ref().clone());
            }
        }
    }

    let new_schema = std::sync::Arc::new(ArrowSchema::new(new_fields));
    RecordBatch::try_new(new_schema, new_cols)
        .map_err(|e| Error::Vdb(format!("vortex rewrite batch: {e}")))
}

fn all_rows_valid_utf8(arr: &BinaryArray) -> bool {
    for i in 0..arr.len() {
        if arr.is_null(i) {
            continue;
        }
        if std::str::from_utf8(arr.value(i)).is_err() {
            return false;
        }
    }
    true
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayRef as ArrowArrayRef, BinaryArray, StringArray, UInt8Array, UInt32Array, UInt64Array,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn build_sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("spot_id", DataType::UInt64, false),
            Field::new("read_num", DataType::UInt8, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("read_len", DataType::UInt32, false),
            Field::new("sequence", DataType::Binary, false),
            Field::new("quality", DataType::Binary, true),
        ]));
        let spot_ids = Arc::new(UInt64Array::from(vec![1u64, 2, 3])) as ArrowArrayRef;
        let read_nums = Arc::new(UInt8Array::from(vec![0u8, 0, 0])) as ArrowArrayRef;
        let names =
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])) as ArrowArrayRef;
        let read_lens = Arc::new(UInt32Array::from(vec![4u32, 4, 4])) as ArrowArrayRef;
        let seqs = Arc::new(BinaryArray::from(vec![
            b"ACGT".as_ref(),
            b"TGCA".as_ref(),
            b"AAAA".as_ref(),
        ])) as ArrowArrayRef;
        let quals = Arc::new(BinaryArray::from(vec![
            Some(b"IIII".as_ref()),
            Some(b"IIII".as_ref()),
            None,
        ])) as ArrowArrayRef;
        RecordBatch::try_new(
            schema,
            vec![spot_ids, read_nums, names, read_lens, seqs, quals],
        )
        .unwrap()
    }

    #[test]
    fn roundtrip_small_batch() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("sample.vortex");
        let batch = build_sample_batch();
        let schema = batch.schema();
        write_batches_to_vortex(&path, schema, vec![batch]).unwrap();

        let bytes = std::fs::metadata(&path).unwrap().len();
        assert!(bytes > 0, "vortex file should be non-empty");

        let runtime = RuntimeBuilder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let n_rows = runtime.block_on(async {
            use futures::StreamExt;
            use vortex::file::OpenOptionsSessionExt;

            let session = VortexSession::default();
            let file = session.open_options().open_path(&path).await.unwrap();
            let mut stream = file.scan().unwrap().into_array_stream().unwrap();
            let mut n = 0usize;
            while let Some(chunk) = stream.next().await {
                n += chunk.unwrap().len();
            }
            n
        });
        assert_eq!(n_rows, 3);
    }
}
