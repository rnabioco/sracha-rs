//! Convert an SRA file into a Vortex file.
//!
//! Vortex-first: builds native Vortex arrays directly from decoded VDB blobs
//! (see `crate::vortex::builder::VortexRowBuilder`), no Arrow `RecordBatch`
//! intermediate. The per-blob decode is reused from `crate::parquet::writer`.
//!
//! v1 scope: bulk columns only (READ, QUALITY, READ_LEN, NAME). The
//! fasterq-dump-equivalent edge cases (ALTREAD ambiguity merge, Illumina name
//! reconstruction from skey, SRA-lite synthetic quality, technical-read
//! filtering) are deliberately skipped.

use std::fs::File;
use std::path::{Path, PathBuf};

use tokio::runtime::Builder as RuntimeBuilder;

use vortex::VortexSessionDefault;
use vortex::array::ArrayRef;
use vortex::file::WriteOptionsSessionExt;
use vortex::session::VortexSession;

use crate::error::{Error, Result};
use crate::parquet::schema::{DnaPacking, LengthMode};
use crate::parquet::writer::{LengthModeChoice, decode_one_blob, resolve_length_mode};
use crate::vdb::cursor::VdbCursor;
use crate::vdb::kar::KarArchive;
use crate::vortex::builder::VortexRowBuilder;

// ---------------------------------------------------------------------------
// Public configuration
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct VortexConvertConfig {
    pub pack_dna: DnaPacking,
    pub length_mode: LengthModeChoice,
    /// Number of blobs to accumulate before starting a new row-builder.
    /// Unused in the current serial implementation but kept for API parity
    /// with the parquet config — reserved for future parallelism.
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

    tracing::debug!(
        "vortex: length_mode={:?}, pack_dna={:?}",
        length_mode,
        pack_dna
    );

    let (array, spots, reads) = build_struct_array(&cursor, pack_dna)?;

    write_struct_array(output_path, array)?;

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

/// Decode every blob and push rows into a single Vortex `StructArray`.
///
/// Returns the finished array plus `(spots, reads)` counts.
fn build_struct_array(cursor: &VdbCursor, pack_dna: DnaPacking) -> Result<(ArrayRef, u64, u64)> {
    let read_cs = cursor.read_col().meta().checksum_type;
    let blob_infos = cursor.read_col().blobs().to_vec();
    let quality_cs = cursor.quality_col().map_or(0, |c| c.meta().checksum_type);
    let read_len_cs = cursor.read_len_col().map_or(0, |c| c.meta().checksum_type);
    let name_cs = cursor.name_col().map_or(0, |c| c.meta().checksum_type);
    let metadata_rps = cursor.metadata_reads_per_spot();

    // Ballpark capacity so VarBinView buffers don't thrash. Sum blob id_range
    // × max reads-per-spot; metadata_rps fallback = 1 is fine if unknown.
    let capacity: usize = blob_infos
        .iter()
        .map(|b| b.id_range as usize)
        .sum::<usize>()
        .saturating_mul(metadata_rps.unwrap_or(1).max(1));

    let mut builder = VortexRowBuilder::with_capacity(pack_dna, capacity);
    let mut spot_id_acc: u64 = cursor.first_row().max(1) as u64;
    let mut total_spots: u64 = 0;
    let mut total_reads: u64 = 0;

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
                builder.push(
                    spot_id,
                    read_num as u8,
                    spot.name,
                    read.sequence,
                    read.quality,
                );
                total_reads += 1;
            }
        }
        total_spots += n_spots as u64;
        spot_id_acc += n_spots as u64;
    }

    if builder.is_empty() {
        return Err(Error::Vdb("vortex: no rows to write".into()));
    }

    let array = builder.finish()?;
    Ok((array, total_spots, total_reads))
}

/// Write a finalized Vortex `StructArray` to disk.
///
/// Vortex's write API is async; sracha's CLI is `#[tokio::main]`, so
/// `block_on` on the ambient runtime panics. Spawn a dedicated std thread
/// with its own current-thread runtime to keep the caller sync.
fn write_struct_array(output_path: &Path, array: ArrayRef) -> Result<()> {
    let output_path = output_path.to_path_buf();
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use vortex::array::arrays::StructArray;
    use vortex::array::builders::{ArrayBuilder, PrimitiveBuilder, VarBinViewBuilder};
    use vortex::array::{Canonical, IntoArray};
    use vortex::dtype::{DType, Nullability};

    /// Build a tiny 3-row StructArray natively, write it, read it back.
    /// Verifies the async write + sync wrap still round-trip.
    #[test]
    fn roundtrip_small_native_struct() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("sample.vortex");

        let mut spot_id = PrimitiveBuilder::<u64>::with_capacity(Nullability::NonNullable, 3);
        spot_id.append_value(1);
        spot_id.append_value(2);
        spot_id.append_value(3);

        let mut name = VarBinViewBuilder::with_capacity(DType::Utf8(Nullability::Nullable), 3);
        name.append_value("a");
        name.append_value("b");
        name.append_null();

        let mut seq = VarBinViewBuilder::with_capacity(DType::Utf8(Nullability::NonNullable), 3);
        seq.append_value("ACGT");
        seq.append_value("TGCA");
        seq.append_value("AAAA");

        let fields: Vec<(std::sync::Arc<str>, ArrayRef)> = vec![
            (std::sync::Arc::from("spot_id"), spot_id.finish()),
            (std::sync::Arc::from("name"), name.finish()),
            (std::sync::Arc::from("sequence"), seq.finish()),
        ];
        let struct_arr = StructArray::try_from_iter(fields).unwrap();
        let array: ArrayRef = struct_arr.into_array();

        write_struct_array(&path, array).unwrap();
        let bytes = std::fs::metadata(&path).unwrap().len();
        assert!(bytes > 0);

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
            // Silence unused-import lint if Canonical isn't referenced elsewhere.
            let _ = std::marker::PhantomData::<Canonical>;
            n
        });
        assert_eq!(n_rows, 3);
    }
}
