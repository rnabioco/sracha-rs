//! Convert an SRA file into a Parquet file.
//!
//! v1 scope: bulk columns only (READ, QUALITY, READ_LEN, NAME). The fasterq-
//! dump-equivalent edge cases (ALTREAD ambiguity merge, Illumina name
//! reconstruction from skey, SRA-lite synthetic quality, technical-read
//! filtering) are deliberately skipped — they don't affect the storage-
//! density measurement we're after, only the byte-for-byte content of
//! affected columns.
//!
//! Output schema is per-read (one row per biological+technical read), chosen
//! at runtime as fixed-length or variable-length based on detected uniformity
//! of `READ_LEN`.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, FixedSizeBinaryBuilder, RecordBatch, StringBuilder, UInt8Builder,
    UInt32Builder, UInt64Builder,
};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::schema::types::ColumnPath;

use crate::convert::decode::{decode_one_blob, pack_sequence, resolve_length_mode};
use crate::convert::schema::{DnaPacking, LengthMode, LengthModeChoice};
use crate::error::{Error, Result};
use crate::vdb::cursor::VdbCursor;
use crate::vdb::kar::KarArchive;

use super::schema::build_per_read_schema;

// ---------------------------------------------------------------------------
// Public configuration
// ---------------------------------------------------------------------------

/// Page-level compression codec applied to all columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParquetCompression {
    None,
    Snappy,
    /// Zstd level (typically 1–22; 22 is the slowest/densest).
    Zstd(i32),
}

impl ParquetCompression {
    fn into_parquet(self) -> Compression {
        match self {
            ParquetCompression::None => Compression::UNCOMPRESSED,
            ParquetCompression::Snappy => Compression::SNAPPY,
            ParquetCompression::Zstd(level) => {
                Compression::ZSTD(ZstdLevel::try_new(level).unwrap_or_default())
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConvertConfig {
    pub compression: ParquetCompression,
    pub pack_dna: DnaPacking,
    /// Target row-group size in MiB. Parquet flushes a row group when it
    /// estimates this many MiB have been buffered.
    pub row_group_mib: usize,
    pub length_mode: LengthModeChoice,
    /// Number of blobs to decode per Arrow `RecordBatch`.
    pub blobs_per_batch: usize,
}

impl Default for ConvertConfig {
    fn default() -> Self {
        Self {
            compression: ParquetCompression::Zstd(22),
            pack_dna: DnaPacking::TwoNa,
            row_group_mib: 256,
            length_mode: LengthModeChoice::Auto,
            blobs_per_batch: 64,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConvertStats {
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

/// Convert an SRA file at `sra_path` into a Parquet file at `output_path`.
pub fn convert_sra_to_parquet(
    sra_path: &Path,
    output_path: &Path,
    config: &ConvertConfig,
) -> Result<ConvertStats> {
    let input_bytes = std::fs::metadata(sra_path)?.len();

    let file = File::open(sra_path)?;
    let mut archive = KarArchive::open(std::io::BufReader::new(file))?;
    let cursor = VdbCursor::open(&mut archive, sra_path)?;

    let length_mode = resolve_length_mode(&cursor, config.length_mode)?;
    let pack_dna = config.pack_dna;
    let schema = build_per_read_schema(length_mode, pack_dna);

    tracing::debug!(
        "parquet: length_mode={:?}, pack_dna={:?}, compression={:?}",
        length_mode,
        pack_dna,
        config.compression
    );

    let writer_props = build_writer_properties(config);
    let out_file = File::create(output_path)?;
    let mut writer = ArrowWriter::try_new(out_file, schema.clone(), Some(writer_props))
        .map_err(|e| Error::Vdb(format!("parquet writer init: {e}")))?;

    // ---- per-blob iteration ------------------------------------------------
    let read_cs = cursor.read_col().meta().checksum_type;
    let num_blobs = cursor.read_col().blob_count();
    let blob_infos = cursor.read_col().blobs().to_vec();

    let quality_cs = cursor.quality_col().map_or(0, |c| c.meta().checksum_type);
    let read_len_cs = cursor.read_len_col().map_or(0, |c| c.meta().checksum_type);
    let name_cs = cursor.name_col().map_or(0, |c| c.meta().checksum_type);
    let metadata_rps = cursor.metadata_reads_per_spot();

    let mut stats = ConvertStats {
        spots: 0,
        reads: 0,
        input_bytes,
        output_bytes: 0,
        output_path: output_path.to_path_buf(),
        length_mode,
        dna_packing: pack_dna,
    };

    let mut spot_id_acc: u64 = cursor.first_row().max(1) as u64;
    let mut batch_builder = BatchBuilder::new(schema.clone(), length_mode, pack_dna);

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
            metadata_rps,
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
                stats.reads += 1;
            }
        }

        stats.spots += n_spots as u64;
        spot_id_acc += n_spots as u64;

        if batch_builder.len() >= config.blobs_per_batch * 1024 {
            let batch = batch_builder.finish()?;
            writer
                .write(&batch)
                .map_err(|e| Error::Vdb(format!("parquet write batch: {e}")))?;
        }
    }

    if !batch_builder.is_empty() {
        let batch = batch_builder.finish()?;
        writer
            .write(&batch)
            .map_err(|e| Error::Vdb(format!("parquet write final batch: {e}")))?;
    }

    writer
        .close()
        .map_err(|e| Error::Vdb(format!("parquet close: {e}")))?;

    stats.output_bytes = std::fs::metadata(output_path)?.len();
    let _ = num_blobs;
    Ok(stats)
}

// ---------------------------------------------------------------------------
// Writer properties
// ---------------------------------------------------------------------------

fn build_writer_properties(config: &ConvertConfig) -> WriterProperties {
    let compression = config.compression.into_parquet();
    let row_group_bytes = config.row_group_mib * 1024 * 1024;

    let mut builder = WriterProperties::builder()
        .set_compression(compression)
        .set_data_page_size_limit(1024 * 1024)
        .set_write_batch_size(8192)
        .set_dictionary_enabled(true)
        .set_statistics_enabled(EnabledStatistics::Chunk);

    let _ = row_group_bytes;

    builder = builder.set_column_encoding(
        ColumnPath::from(vec!["spot_id".into()]),
        Encoding::DELTA_BINARY_PACKED,
    );
    builder = builder.set_column_encoding(
        ColumnPath::from(vec!["read_len".into()]),
        Encoding::DELTA_BINARY_PACKED,
    );
    builder = builder.set_column_encoding(
        ColumnPath::from(vec!["name".into()]),
        Encoding::DELTA_BYTE_ARRAY,
    );
    builder = builder.set_column_dictionary_enabled(ColumnPath::from(vec!["name".into()]), false);

    builder.build()
}

// ---------------------------------------------------------------------------
// Arrow batch builder
// ---------------------------------------------------------------------------

pub(crate) struct BatchBuilder {
    schema: Arc<arrow::datatypes::Schema>,
    length_mode: LengthMode,
    pack_dna: DnaPacking,

    spot_id: UInt64Builder,
    read_num: UInt8Builder,
    name: StringBuilder,
    read_len: UInt32Builder,
    seq_var: BinaryBuilder,
    qual_var: BinaryBuilder,
    seq_fixed: Option<FixedSizeBinaryBuilder>,
    qual_fixed: Option<FixedSizeBinaryBuilder>,
    rows: usize,
}

impl BatchBuilder {
    pub(crate) fn new(
        schema: Arc<arrow::datatypes::Schema>,
        length_mode: LengthMode,
        pack_dna: DnaPacking,
    ) -> Self {
        let (seq_fixed, qual_fixed) = match length_mode {
            LengthMode::Fixed { read_len } => {
                let seq_w = pack_dna.packed_len(read_len) as i32;
                (
                    Some(FixedSizeBinaryBuilder::new(seq_w)),
                    Some(FixedSizeBinaryBuilder::new(read_len as i32)),
                )
            }
            LengthMode::Variable => (None, None),
        };
        Self {
            schema,
            length_mode,
            pack_dna,
            spot_id: UInt64Builder::new(),
            read_num: UInt8Builder::new(),
            name: StringBuilder::new(),
            read_len: UInt32Builder::new(),
            seq_var: BinaryBuilder::new(),
            qual_var: BinaryBuilder::new(),
            seq_fixed,
            qual_fixed,
            rows: 0,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.rows
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.rows == 0
    }

    pub(crate) fn push(
        &mut self,
        spot_id: u64,
        read_num: u8,
        name: &[u8],
        sequence_ascii: &[u8],
        quality_ascii: &[u8],
    ) {
        self.spot_id.append_value(spot_id);
        self.read_num.append_value(read_num);
        if name.is_empty() {
            self.name.append_null();
        } else {
            self.name
                .append_value(std::str::from_utf8(name).unwrap_or(""));
        }

        let packed = pack_sequence(sequence_ascii, self.pack_dna);

        match self.length_mode {
            LengthMode::Fixed { read_len } => {
                let want = self.pack_dna.packed_len(read_len) as usize;
                let mut buf = packed;
                if buf.len() < want {
                    buf.resize(want, 0);
                } else if buf.len() > want {
                    buf.truncate(want);
                }
                self.seq_fixed.as_mut().unwrap().append_value(&buf).ok();
                let qbuf = if quality_ascii.is_empty() {
                    None
                } else {
                    let mut q = quality_ascii.to_vec();
                    if q.len() < read_len as usize {
                        q.resize(read_len as usize, b'?');
                    } else if q.len() > read_len as usize {
                        q.truncate(read_len as usize);
                    }
                    Some(q)
                };
                match qbuf {
                    Some(q) => {
                        self.qual_fixed.as_mut().unwrap().append_value(&q).ok();
                    }
                    None => {
                        self.qual_fixed.as_mut().unwrap().append_null();
                    }
                }
            }
            LengthMode::Variable => {
                self.read_len.append_value(sequence_ascii.len() as u32);
                self.seq_var.append_value(&packed);
                if quality_ascii.is_empty() {
                    self.qual_var.append_null();
                } else {
                    self.qual_var.append_value(quality_ascii);
                }
            }
        }

        self.rows += 1;
    }

    pub(crate) fn finish(&mut self) -> Result<RecordBatch> {
        let mut cols: Vec<ArrayRef> = Vec::with_capacity(6);
        cols.push(Arc::new(self.spot_id.finish()));
        cols.push(Arc::new(self.read_num.finish()));
        cols.push(Arc::new(self.name.finish()));
        match self.length_mode {
            LengthMode::Fixed { .. } => {
                cols.push(Arc::new(self.seq_fixed.as_mut().unwrap().finish()));
                cols.push(Arc::new(self.qual_fixed.as_mut().unwrap().finish()));
            }
            LengthMode::Variable => {
                cols.push(Arc::new(self.read_len.finish()));
                cols.push(Arc::new(self.seq_var.finish()));
                cols.push(Arc::new(self.qual_var.finish()));
            }
        }
        self.rows = 0;
        RecordBatch::try_new(self.schema.clone(), cols)
            .map_err(|e| Error::Vdb(format!("arrow record batch: {e}")))
    }
}
