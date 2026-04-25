//! Pipeline orchestration for the `sracha get` command.
//!
//! Implements a full-download approach:
//!
//! 1. **Phase 1 -- Download**: Use the parallel chunked downloader to fetch the
//!    full SRA file to a temporary location.
//! 2. **Phase 2 -- Parse + Output**: Open the downloaded file as a KAR archive,
//!    create a VdbCursor to read SEQUENCE table columns, decode VDB blobs, and
//!    write FASTQ output.
//! 3. **Phase 3 -- Cleanup + Report**: Delete the temp file and print stats.

use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use rayon::prelude::*;

use crate::compress::{DEFAULT_BLOCK_SIZE, ParGzWriter};
use crate::download::{DownloadConfig, download_file};
use crate::error::{Error, Result};
use crate::fastq::{
    CompressionMode, FastqConfig, IntegrityDiag, OutputSlot, SplitMode, output_filename,
};
use crate::sdl::ResolvedAccession;
use crate::vdb::cursor::VdbCursor;
use crate::vdb::kar::KarArchive;

/// Output bytes for one (blob, slot) pair, plus the number of records the
/// buffer holds. Returned from `decode_blob_to_fastq` so the writer can do
/// one `write_all` per slot per blob instead of one per record.
pub(crate) struct BlobSlotOutput {
    pub(crate) slot: OutputSlot,
    pub(crate) bytes: Vec<u8>,
    pub(crate) records: u64,
}

mod config;
pub use config::{PipelineConfig, PipelineStats};

// ---------------------------------------------------------------------------
// Completion marker — skip re-decode when output files already exist.
// ---------------------------------------------------------------------------

mod blob_decode;
mod marker;
mod validate;
use blob_decode::{
    BlobDecodeCtx, RawBlobData, decode_blob_to_fastq, decode_raw, decode_zip_encoding,
};
use marker::{
    StatsEntry, check_completion_marker, marker_path, write_completion_marker, write_stats_file,
};
pub use validate::{ValidationResult, run_validate};

// ---------------------------------------------------------------------------
// Log-friendly progress target for non-TTY output (e.g. SLURM logs).
// ---------------------------------------------------------------------------

/// A [`TermLike`](indicatif::TermLike) adapter that prints each progress update
/// as a new line to stderr. Cursor movement and clearing are no-ops.
#[derive(Debug)]
pub(crate) struct LogTarget;

impl indicatif::TermLike for LogTarget {
    fn width(&self) -> u16 {
        80
    }

    fn move_cursor_up(&self, _n: usize) -> std::io::Result<()> {
        Ok(())
    }

    fn move_cursor_down(&self, _n: usize) -> std::io::Result<()> {
        Ok(())
    }

    fn move_cursor_right(&self, _n: usize) -> std::io::Result<()> {
        Ok(())
    }

    fn move_cursor_left(&self, _n: usize) -> std::io::Result<()> {
        Ok(())
    }

    fn write_line(&self, s: &str) -> std::io::Result<()> {
        eprintln!("{s}");
        Ok(())
    }

    fn write_str(&self, _s: &str) -> std::io::Result<()> {
        Ok(())
    }

    fn clear_line(&self) -> std::io::Result<()> {
        Ok(())
    }

    fn flush(&self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Create a progress bar with the project's standard style.
pub(crate) fn make_styled_pb(total: u64, template: &str) -> indicatif::ProgressBar {
    use std::io::IsTerminal;
    let pb = if std::io::stderr().is_terminal() {
        indicatif::ProgressBar::new(total)
    } else {
        let target = indicatif::ProgressDrawTarget::term_like_with_hz(Box::new(LogTarget), 1);
        indicatif::ProgressBar::with_draw_target(Some(total), target)
    };
    pb.set_style(
        indicatif::ProgressStyle::default_bar()
            .template(template)
            .expect("valid progress bar template")
            .progress_chars("━╸─"),
    );
    pb
}

// ---------------------------------------------------------------------------
// Output writer
// ---------------------------------------------------------------------------

/// An output writer that handles gzip, zstd, plain, or stdout output.
enum OutputWriter {
    Gz(ParGzWriter<std::io::BufWriter<std::fs::File>>),
    Zstd(zstd::stream::write::Encoder<'static, std::io::BufWriter<std::fs::File>>),
    Plain(std::io::BufWriter<std::fs::File>),
    Stdout(std::io::BufWriter<std::io::Stdout>),
}

impl OutputWriter {
    fn write_all(&mut self, data: &[u8]) -> std::io::Result<()> {
        match self {
            OutputWriter::Gz(w) => w.write_all(data),
            OutputWriter::Zstd(w) => w.write_all(data),
            OutputWriter::Plain(w) => w.write_all(data),
            OutputWriter::Stdout(w) => w.write_all(data),
        }
    }

    fn finish(self) -> std::io::Result<()> {
        match self {
            OutputWriter::Gz(w) => {
                w.finish()?;
                Ok(())
            }
            OutputWriter::Zstd(w) => {
                w.finish()?;
                Ok(())
            }
            OutputWriter::Plain(mut w) => {
                w.flush()?;
                Ok(())
            }
            OutputWriter::Stdout(mut w) => {
                w.flush()?;
                Ok(())
            }
        }
    }
}

/// Build the output writer for a given (accession, slot), returning the
/// writer along with the final and in-flight (`*.partial`) paths. Extracted
/// from `decode_and_write` so the writer thread body is readable at a
/// glance; behavior is unchanged. Callers are responsible for recording
/// `(final_path, tmp_path)` so the post-decode rename logic can promote
/// partial files into place.
fn create_output_writer_for_slot(
    accession: &str,
    slot: OutputSlot,
    config: &PipelineConfig,
    compress_pool: &Option<Arc<rayon::ThreadPool>>,
) -> (OutputWriter, PathBuf, PathBuf) {
    let filename = output_filename(accession, slot, config.fasta, &config.compression);
    let final_path = config.output_dir.join(&filename);
    let tmp_path = config.output_dir.join(format!("{filename}.partial"));

    let file = std::fs::File::create(&tmp_path).expect("failed to create output file");
    let buf = std::io::BufWriter::with_capacity(256 * 1024, file);

    let writer = match config.compression {
        CompressionMode::Gzip { level } => OutputWriter::Gz(ParGzWriter::new(
            buf,
            level,
            DEFAULT_BLOCK_SIZE,
            compress_pool.clone().expect("gzip pool must exist"),
        )),
        CompressionMode::Zstd { level, threads } => {
            let mut encoder = zstd::stream::write::Encoder::new(buf, level)
                .expect("failed to create zstd encoder");
            encoder
                .multithread(threads)
                .expect("failed to set zstd threads");
            OutputWriter::Zstd(encoder)
        }
        CompressionMode::None => OutputWriter::Plain(buf),
    };

    (writer, final_path, tmp_path)
}

// ---------------------------------------------------------------------------
// Mirror selection
// ---------------------------------------------------------------------------

/// Legacy sequencing platforms with complex read structures that sracha
/// does not support. Modern short-read (Illumina, BGISEQ, DNBSEQ, Element,
/// Ultima) and long-read (PacBio, Nanopore) platforms are allowed.
pub const UNSUPPORTED_PLATFORMS: &[&str] =
    &["LS454", "ABI_SOLID", "ION_TORRENT", "HELICOS", "CAPILLARY"];

pub fn is_unsupported_platform(platform: &str) -> bool {
    UNSUPPORTED_PLATFORMS.contains(&platform)
}

/// Map a raw `sracha_vdb::Error::BlobIntegrity` from `decode_blob` into the
/// user-facing [`Error::IntegrityFailure`], attaching the accession and the
/// shared [`crate::error::BLOB_INTEGRITY_GUIDANCE`] text. Passes other errors
/// through unchanged.
fn wrap_blob_integrity(accession: &str, err: Error) -> Error {
    match err {
        Error::Vdb(sracha_vdb::Error::BlobIntegrity {
            kind,
            stored,
            computed,
        }) => Error::IntegrityFailure {
            accession: accession.to_string(),
            summary: format!(
                "per-blob {kind} mismatch during decode (stored={stored}, computed={computed}). {}",
                crate::error::BLOB_INTEGRITY_GUIDANCE,
            ),
        },
        other => other,
    }
}

/// Select the best mirror URL for downloading.
///
/// Prefers cloud mirrors (s3, gs) over NCBI on-premises servers because
/// cloud CDNs are typically much faster for parallel chunked downloads.
fn select_mirror(resolved: &ResolvedAccession) -> Result<String> {
    let mirrors = &resolved.sra_file.mirrors;
    if mirrors.is_empty() {
        return Err(Error::Download {
            accession: resolved.accession.clone(),
            message: "no download mirrors available".into(),
        });
    }

    // Prefer cloud mirrors — much faster for parallel downloads.
    // Priority: s3 > gs > sra-ncbi > ncbi > any
    let priority = |s: &str| -> u8 {
        match s {
            "s3" | "s3-direct" => 0,
            "gs" => 1,
            s if s.contains("sra-ncbi") => 2,
            "ncbi" => 3,
            _ => 4,
        }
    };

    let best = mirrors
        .iter()
        .min_by_key(|m| priority(m.service.as_str()))
        .unwrap();

    tracing::debug!(
        "selected mirror: [{}] {}",
        best.service,
        &best.url[..best.url.len().min(80)],
    );

    Ok(best.url.clone())
}

/// Validate that the blob row-id ranges on a column form a contiguous,
/// monotonic cover of the expected spot range.
///
/// Catches corrupt KDB indexes that would otherwise silently skip or
/// duplicate spots. `expected_spots` is from RunInfo when available; if
/// absent we only check internal consistency (monotonic, non-overlapping,
/// non-empty).
fn validate_blob_ranges(
    accession: &str,
    blobs: &[crate::vdb::kdb::BlobLoc],
    expected_spots: Option<u64>,
) -> Result<()> {
    if blobs.is_empty() {
        return Ok(());
    }

    let first_id = blobs[0].start_id;
    let mut prev_end: i64 = first_id;
    for (i, blob) in blobs.iter().enumerate() {
        if blob.id_range == 0 {
            // Synthetic single-blob columns (id_range=0 means "covers all
            // rows") are legal — skip range bookkeeping for those.
            continue;
        }
        if blob.start_id < prev_end {
            return Err(Error::Pipeline(format!(
                "{accession}: blob {i} start_id {} overlaps previous end {prev_end}",
                blob.start_id,
            )));
        }
        if blob.start_id > prev_end {
            return Err(Error::Pipeline(format!(
                "{accession}: blob {i} start_id {} leaves a gap from {prev_end}",
                blob.start_id,
            )));
        }
        prev_end = blob.start_id + blob.id_range as i64;
    }

    let covered = (prev_end - first_id) as u64;
    if let Some(expected) = expected_spots
        && covered != expected
    {
        return Err(Error::Pipeline(format!(
            "{accession}: blob ranges cover {covered} rows, RunInfo expects {expected}",
        )));
    }
    Ok(())
}

/// Decode VDB columns from a local SRA file, format FASTQ, and write to
/// output files.
///
/// This opens the SRA file as a KAR archive, creates a VdbCursor for the
/// SEQUENCE table, bulk-decompresses each column, and iterates through spots
/// to produce FASTQ output.
///
/// Blobs are processed in batches: raw bytes are read sequentially (I/O),
/// then all blobs in the batch are decoded in parallel via rayon, and
/// finally FASTQ output is written sequentially to preserve order.
fn decode_and_write(
    sra_path: &std::path::Path,
    accession: &str,
    config: &PipelineConfig,
    is_lite: bool,
    diag: &IntegrityDiag,
    chunk_ready: Option<&Arc<crate::download::chunk_ready::ChunkReadyTracker>>,
) -> Result<(u64, u64, Vec<PathBuf>)> {
    // Phase 3c gate (entry, metadata): when a streaming tracker is
    // present, wait for the bytes that KarArchive::open + VdbCursor::open
    // will read before we open them. Otherwise sparse-mmap reads return
    // zeros and we'd silently get a corrupt TOC / column metadata.
    //
    // Two-stage:
    //   1. Wait for the 24-byte KAR header so we can extract file_offset
    //      (where the data section starts).
    //   2. Wait for [0, file_offset) — the rest of the TOC.
    //   3. Open KarArchive, walk its entries to find the maximum byte
    //      offset of any non-`/data` file (these are the small idx*
    //      sidecars). Wait for [0, max_meta_end).
    //   4. Open VdbCursor (its ColumnReader::open calls read those idx
    //      files; mmaps the data slabs but doesn't read them yet — the
    //      per-batch gate below handles those reads).
    if let Some(tracker) = chunk_ready {
        wait_metadata_ready(sra_path, tracker)?;
    }

    let file = std::fs::File::open(sra_path)?;
    let mut archive = KarArchive::open(std::io::BufReader::new(file))?;

    if let Some(tracker) = chunk_ready {
        // Collect all non-`/data` archive file ranges first so we can
        // log the summary + inject chunk-priority hints BEFORE waiting
        // on any of them.
        let total_size = tracker.file_size();
        let mut idx_ranges: Vec<(String, u64, u64)> = Vec::new();
        for path in archive.list_files() {
            if path.ends_with("/data") {
                // Data slabs are gated per-batch later; only idx-style
                // files (idx, idx0, idx1, idx2, skey) need pre-decode
                // wait coverage.
                continue;
            }
            if let Some((off, sz)) = archive.file_location(path) {
                idx_ranges.push((path.to_string(), off, sz));
            }
        }
        let idx_max_offset = idx_ranges.iter().map(|(_, o, s)| o + s).max().unwrap_or(0);
        let pct = (idx_max_offset as f64 / total_size.max(1) as f64) * 100.0;
        tracing::info!(
            "{accession}: streaming-decode metadata gate: {} idx files, furthest at byte \
             {idx_max_offset} ({pct:.1}% into {total_size}-byte file)",
            idx_ranges.len(),
        );

        // Phase 3g-2: compute which chunks contain idx file bytes and
        // ask the downloader to prioritize them. Workers will finish
        // their current chunk and pick up the priority indices next,
        // so decode can open the cursor (read these idx files) seconds
        // into the download instead of minutes (which is what happened
        // when chunks were dispatched in pure numerical order).
        let mut prio_chunks: Vec<usize> = Vec::new();
        for (_, off, sz) in &idx_ranges {
            let first = tracker.chunk_index_for_byte(*off);
            let last = tracker.chunk_index_for_byte((off + sz).saturating_sub(1));
            for c in first..=last {
                if !prio_chunks.contains(&c) {
                    prio_chunks.push(c);
                }
            }
        }
        if !prio_chunks.is_empty() {
            tracing::info!(
                "{accession}: prioritizing {} chunks containing idx-file bytes",
                prio_chunks.len(),
            );
            tracker.prioritize_pending(&prio_chunks);
        }

        // Per-file wait — now that we've nudged the dispatch order, the
        // chunks we need will arrive ASAP rather than at their
        // numerical-order turn.
        for (_, off, sz) in &idx_ranges {
            tracker.wait_range(*off, off + sz);
        }
    }

    let cursor = VdbCursor::open(&mut archive, sra_path)?;

    // Check platform — reject legacy platforms with complex read structures.
    if let Some(platform) = cursor.platform()
        && is_unsupported_platform(platform)
    {
        return Err(Error::UnsupportedPlatform {
            platform: platform.to_string(),
        });
    }

    // Column-priority chunk hint: prioritize the chunks containing the
    // first BATCH_SIZE blobs of every column's data slab. Without this,
    // sequential download lands READ's data slab first (low byte
    // offsets) and the decode loop's per-batch gate sits idle waiting
    // for the LAST column's data slab to be reached at the end of the
    // file (multi-GB into the download for big SEQUENCE tables — see
    // issue #19 analysis). With this, the chunks needed for batch 0 of
    // every column jump to the front of the dispatch queue and arrive
    // interleaved at the start of the download.
    //
    // Pairs with the per-batch `wait_chunks` switch below: the prior
    // contiguous `wait_range(min, max)` would still have blocked on the
    // sequentially-downloaded bytes between columns even after these
    // chunks landed, defeating the priority hint.
    if let Some(tracker) = chunk_ready {
        const PRIO_BLOBS_PER_COL: usize = 1024;
        let mut col_prio: Vec<usize> = Vec::new();
        let mut seen: std::collections::HashSet<usize> = std::collections::HashSet::new();
        let mut push_col =
            |blobs: &[crate::vdb::kdb::BlobLoc],
             range_for: &dyn Fn(&crate::vdb::kdb::BlobLoc) -> Option<(u64, u64)>| {
                let take = PRIO_BLOBS_PER_COL.min(blobs.len());
                for blob in &blobs[..take] {
                    let Some((s, e)) = range_for(blob) else {
                        continue;
                    };
                    if e <= s {
                        continue;
                    }
                    let first = tracker.chunk_index_for_byte(s);
                    let last = tracker.chunk_index_for_byte(e - 1);
                    for c in first..=last {
                        if seen.insert(c) {
                            col_prio.push(c);
                        }
                    }
                }
            };
        push_col(cursor.read_col().blobs(), &|b| {
            cursor.read_col().blob_absolute_range(b)
        });
        if let Some(c) = cursor.quality_col() {
            push_col(c.blobs(), &|b| c.blob_absolute_range(b));
        }
        if let Some(c) = cursor.read_len_col() {
            push_col(c.blobs(), &|b| c.blob_absolute_range(b));
        }
        if let Some(c) = cursor.name_col() {
            push_col(c.blobs(), &|b| c.blob_absolute_range(b));
        }
        if let Some(c) = cursor.read_type_col() {
            push_col(c.blobs(), &|b| c.blob_absolute_range(b));
        }
        if let Some(c) = cursor.altread_col() {
            push_col(c.blobs(), &|b| c.blob_absolute_range(b));
        }
        if let Some(c) = cursor.x_col() {
            push_col(c.blobs(), &|b| c.blob_absolute_range(b));
        }
        if let Some(c) = cursor.y_col() {
            push_col(c.blobs(), &|b| c.blob_absolute_range(b));
        }
        if let Some(c) = cursor.name_fmt_col() {
            push_col(c.blobs(), &|b| c.blob_absolute_range(b));
        }
        if !col_prio.is_empty() {
            tracing::info!(
                "{accession}: prioritizing {} chunks containing first-batch column data",
                col_prio.len(),
            );
            tracker.prioritize_pending(&col_prio);
        }
    }

    // Detect SRA-lite from actual file: the QUALITY column is absent
    // (classic variant) or the VDB metadata carries the `SOFTWARE/delite`
    // stamp (delite-processed variant — QUALITY column is present but
    // contains only synthesized Q3/Q30 placeholder bytes that don't match
    // fasterq-dump's output). Both cases mean we should synthesize quality
    // ourselves rather than echo the stored bytes.
    let is_lite = is_lite || !cursor.has_quality() || cursor.is_sra_lite_schema();

    // Validate blob locator ranges on the authoritative (READ) column before
    // decoding: row IDs must be monotonic, non-overlapping, and — when
    // RunInfo is available — cover exactly [first_id, first_id + spots).
    // A corrupt KDB index is the only way these can fail, and the failure
    // mode is silent duplication / skipped spots, so catch it up front.
    validate_blob_ranges(
        accession,
        cursor.read_col().blobs(),
        config.run_info.as_ref().and_then(|ri| ri.spots),
    )?;

    // Load Illumina name format templates from skey index.
    let (name_templates, name_spot_starts): (Vec<Vec<u8>>, Vec<i64>) =
        if cursor.has_illumina_name_parts() {
            VdbCursor::load_name_templates(&mut archive)
        } else {
            (Vec::new(), Vec::new())
        };

    // ------------------------------------------------------------------
    // Batch-parallel blob decode and FASTQ output.
    //
    // For each batch of blobs:
    //   1. Read raw bytes sequentially (disk I/O, ColumnReader is !Send).
    //   2. Decode all blobs in the batch in parallel (CPU-bound, rayon).
    //   3. Write FASTQ output sequentially (I/O, preserves order).
    // ------------------------------------------------------------------

    // Build a scoped rayon thread pool with the requested thread count.
    let num_threads = config.threads;
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build()
        .map_err(|e| Error::Pipeline(format!("failed to build rayon thread pool: {e}")))?;

    // Dedicated thread pool for parallel gzip compression (only needed for gzip).
    let compress_pool: Option<Arc<rayon::ThreadPool>> =
        if matches!(config.compression, CompressionMode::Gzip { .. }) {
            let max_hw = std::thread::available_parallelism().map_or(usize::MAX, |p| p.get());
            let compress_threads = (num_threads * 2).min(max_hw);
            Some(Arc::new(
                rayon::ThreadPoolBuilder::new()
                    .num_threads(compress_threads)
                    .thread_name(|i| format!("pargz-{i}"))
                    .build()
                    .map_err(|e| {
                        Error::Pipeline(format!("failed to build gzip thread pool: {e}"))
                    })?,
            ))
        } else {
            None
        };

    tracing::debug!("{accession}: using {num_threads} threads for decode");

    let fastq_config = FastqConfig {
        split_mode: config.split_mode,
        skip_technical: config.skip_technical,
        min_read_len: config.min_read_len,
        fasta: config.fasta,
    };

    // Create output directory (not needed for stdout mode).
    if !config.stdout {
        std::fs::create_dir_all(&config.output_dir)?;
    }

    // Lazily create output writers as we encounter different output slots.
    // Writers create `<name>.partial` files on disk; only after the full
    // decode + integrity checks pass do we atomically rename them to their
    // final `<name>` so consumers never see a half-written FASTQ file.
    let mut writers: HashMap<OutputSlot, OutputWriter> = HashMap::new();
    // (final_path, tmp_path). `tmp_path` is what's on disk during writing.
    let mut output_paths: Vec<(PathBuf, PathBuf)> = Vec::new();

    // For stdout mode, create a single writer up front. All output slots
    // write to this shared writer (interleaved, uncompressed).
    let mut stdout_writer: Option<OutputWriter> = if config.stdout {
        Some(OutputWriter::Stdout(std::io::BufWriter::with_capacity(
            256 * 1024,
            std::io::stdout(),
        )))
    } else {
        None
    };

    let spots_read = std::sync::atomic::AtomicU64::new(0);
    let mut reads_written: u64 = 0;
    let mut per_slot_counts: std::collections::HashMap<OutputSlot, u64> =
        std::collections::HashMap::new();

    // Capture column metadata before the batch loop. These are Copy/Clone
    // types that can be shared with rayon closures.
    let read_cs = cursor.read_col().meta().checksum_type;
    let num_blobs = cursor.read_col().blob_count();

    let has_quality = cursor.quality_col().is_some();
    let quality_blob_count = cursor.quality_col().map_or(0, |c| c.blob_count());
    let quality_cs = cursor.quality_col().map_or(0, |c| c.meta().checksum_type);

    let has_read_len = cursor.read_len_col().is_some();
    let read_len_blob_count = cursor.read_len_col().map_or(0, |c| c.blob_count());
    let read_len_cs = cursor.read_len_col().map_or(0, |c| c.meta().checksum_type);

    let has_name = cursor.name_col().is_some();
    let name_blob_count = cursor.name_col().map_or(0, |c| c.blob_count());
    let name_cs = cursor.name_col().map_or(0, |c| c.meta().checksum_type);

    let has_read_type = cursor.read_type_col().is_some();
    let read_type_blob_count = cursor.read_type_col().map_or(0, |c| c.blob_count());
    let read_type_cs = cursor.read_type_col().map_or(0, |c| c.meta().checksum_type);

    let has_altread = cursor.altread_col().is_some();
    let has_illumina_name_parts = cursor.has_illumina_name_parts();
    let _altread_blob_count = cursor.altread_col().map_or(0, |c| c.blob_count());
    let altread_cs = cursor.altread_col().map_or(0, |c| c.meta().checksum_type);
    let x_blob_count = cursor.x_col().map_or(0, |c| c.blob_count());
    let x_cs = cursor.x_col().map_or(0, |c| c.meta().checksum_type);
    let y_blob_count = cursor.y_col().map_or(0, |c| c.blob_count());
    let y_cs = cursor.y_col().map_or(0, |c| c.meta().checksum_type);
    let has_name_fmt = cursor.name_fmt_col().is_some();
    let name_fmt_cs = cursor.name_fmt_col().map_or(0, |c| c.meta().checksum_type);

    let metadata_reads_per_spot = cursor.metadata_reads_per_spot();

    // For files without READ_LEN, determine the fixed spot length from
    // blob 0's page map or, for v1 blobs (no page map), the row_length
    // encoded in the blob header.
    let fixed_spot_len: Option<u32> = if !has_read_len && num_blobs > 0 {
        let blob0_info = &cursor.read_col().blobs()[0];
        let blob0_raw = cursor.read_col().read_raw_blob_slice(blob0_info.start_id)?;
        let blob0_id_range = blob0_info.id_range as u64;
        let decoded = decode_raw(blob0_raw, read_cs, blob0_id_range)?;
        decoded
            .page_map
            .as_ref()
            .and_then(|pm| pm.lengths.iter().copied().find(|&l| l > 0 && l <= 100_000))
            .or(decoded
                .row_length
                .map(|rl| rl as u32)
                .filter(|&l| l > 0 && l <= 100_000))
    } else {
        None
    };
    if let Some(fsl) = fixed_spot_len {
        tracing::debug!("fixed_spot_len={fsl} (from blob 0)");
    }

    // Fallback per-read lengths (from NCBI EUtils API or VDB metadata).
    // Only used when READ_LEN column is absent. Cloned once here so rayon
    // closures can borrow it without moving the config.
    let fallback_read_lengths: Option<Vec<u32>> = if !has_read_len {
        config
            .run_info
            .as_ref()
            .map(|ri| ri.avg_read_len.clone())
            .or_else(|| cursor.metadata_read_lengths())
    } else {
        None
    };
    // When the physical READ_TYPE column is absent (common in old
    // DDBJ submissions), read types come from schema metadata. Without
    // this fallback, pipeline defaults every read to biological (0),
    // so technical reads in runs like DRR000065 leak into the output —
    // fasterq-dump filters them (driven by the same metadata), and the
    // outputs diverge at spot count and file count.
    let fallback_read_types: Option<Vec<u8>> = if !has_read_type {
        cursor.metadata_read_types()
    } else {
        None
    };

    tracing::debug!(
        "{accession}: has_read_len={has_read_len} (blobs={read_len_blob_count}), \
         has_read_type={has_read_type} (blobs={read_type_blob_count}), \
         has_name={has_name}, has_quality={has_quality}, \
         metadata_rps={metadata_reads_per_spot:?}, fallback_read_lengths={fallback_read_lengths:?}",
    );
    tracing::debug!("{accession}: streaming decode of {num_blobs} blobs (batch-parallel)",);

    // Decode progress: when a combined bar is wired (streaming mode),
    // we skip the standalone per-decode bar and tick the combined bar
    // instead — the standalone bar's per_sec/eta are misleading during
    // streaming per-batch waits (see Phase 4d plan). The combined bar
    // is ticked by blob share in the writer loop below.
    //
    // Non-streaming paths (`sracha fastq` and the legacy
    // `sracha get` multi-accession) set `progress_combined: None` and
    // keep the existing blob-count bar with its real per_sec/eta —
    // those paths never stall, so the rate/eta are accurate.
    let decode_pb = if config.progress && config.progress_combined.is_none() {
        let bar = make_styled_pb(
            num_blobs as u64,
            "  {elapsed_precise} [{bar:40.cyan}] {pos}/{len} blobs  {per_sec}  eta {eta}",
        );
        let bar = match &config.progress_parent {
            Some(mp) => mp.add(bar),
            None => bar,
        };
        Some(bar)
    } else {
        None
    };
    // Captured by the writer thread; when Some, each blob written ticks
    // the shared combined bar by `5000 / num_blobs` units (the decode
    // side's share of the 10000-unit total).
    let combined_decode_pb = config.progress_combined.clone();
    let combined_decode_units = if num_blobs > 0 {
        (5000u64 / num_blobs as u64).max(1)
    } else {
        0
    };

    /// Number of blobs per batch for parallel decode.
    const BATCH_SIZE: usize = 1024;

    // ------------------------------------------------------------------
    // Pipelined decode → write.
    //
    // A crossbeam channel decouples the decode loop (producer) from the
    // write loop (consumer).  While the writer drains batch N, the
    // decode pool is already working on batch N+1.
    // ------------------------------------------------------------------
    type FormattedBlob = (Vec<BlobSlotOutput>, u64);
    // Bounded channel gives the decode pool slack when writer batch time
    // varies. Capacity 4 costs at most 4×BATCH_SIZE formatted blobs of
    // memory — bounded, and measurably better than 2 on variable-sized
    // writer work (e.g. gzip vs plain).
    let (batch_tx, batch_rx) = crossbeam_channel::bounded::<Vec<Result<FormattedBlob>>>(4);

    // Rebind the shared state the writer thread touches as explicit
    // `&mut` / `&` references BEFORE the scope, then the writer's `move`
    // closure captures each reference (a copy) plus `batch_rx` by value.
    // Moving the receiver into the writer is what matters: if we captured
    // it by reference, the `Receiver` would stay owned by this stack frame
    // and dropping it on writer exit would be impossible — meaning a
    // writer that returns `Err` early could not disconnect the channel,
    // and the decode loop would deadlock on a full `batch_tx.send()`
    // (see #20).
    let writers_ref = &mut writers;
    let output_paths_ref = &mut output_paths;
    let reads_written_ref = &mut reads_written;
    let per_slot_counts_ref = &mut per_slot_counts;
    let stdout_writer_ref = &mut stdout_writer;
    let spots_read_ref = &spots_read;
    let decode_pb_ref = &decode_pb;
    let write_result: Result<()> = std::thread::scope(|scope| {
        // ---- Writer thread ----
        let writer_handle = scope.spawn(move || -> Result<()> {
            let mut blob_counter: usize = 0;
            while let Ok(formatted_batches) = batch_rx.recv() {
                for result in formatted_batches {
                    let (slot_outputs, num_spots) = result?;

                    // One write_all per (slot, blob) instead of per record —
                    // orders of magnitude fewer write calls, and the Vec<u8>
                    // allocation happens at most 4× per blob, not per record.
                    for slot_out in &slot_outputs {
                        let writer = if let Some(sw) = stdout_writer_ref {
                            sw
                        } else {
                            writers_ref.entry(slot_out.slot).or_insert_with(|| {
                                let (writer, final_path, tmp_path) = create_output_writer_for_slot(
                                    accession,
                                    slot_out.slot,
                                    config,
                                    &compress_pool,
                                );
                                output_paths_ref.push((final_path, tmp_path));
                                writer
                            })
                        };

                        writer.write_all(&slot_out.bytes).map_err(Error::Io)?;
                        *reads_written_ref += slot_out.records;
                        *per_slot_counts_ref.entry(slot_out.slot).or_insert(0) += slot_out.records;
                    }

                    spots_read_ref.fetch_add(num_spots, std::sync::atomic::Ordering::Relaxed);
                    blob_counter += 1;

                    if let Some(pb) = decode_pb_ref.as_ref() {
                        pb.inc(1);
                    }
                    // Phase 4d: when streaming mode is active, tick the
                    // shared combined bar by this blob's share of the
                    // decode-side budget (5000 units / num_blobs).
                    if let Some(ref combined_pb) = combined_decode_pb {
                        combined_pb.inc(combined_decode_units);
                    }

                    if blob_counter.is_multiple_of(50) || blob_counter == num_blobs {
                        tracing::debug!(
                            "{accession}: decoded {blob_counter}/{num_blobs} blobs, \
                             {} spots so far",
                            spots_read_ref.load(std::sync::atomic::Ordering::Relaxed),
                        );
                    }
                }
            }
            Ok(())
        });

        // Per-call immutable context reused across every blob decode.
        let decode_ctx = BlobDecodeCtx {
            run_name: accession,
            config: &fastq_config,
            diag,
            is_lite,
            read_cs,
        };

        // ---- Decode loop (main thread) ----
        //
        // `cumulative_spots` tracks the total number of spots in blobs 0..blob_idx
        // so each batch can compute `spots_before_per_blob` deterministically
        // from blob metadata alone. Reading `spots_read` here would race with
        // the writer thread on archives with more than `BATCH_SIZE` (1024)
        // blobs — the bounded channel of capacity 4 lets the decoder queue
        // four batches ahead of the writer, and the writer's fetch_add on
        // `spots_read` doesn't happen until it has processed a batch.
        // DRR045255 (3658 blobs) used to emit `spots_before=0` for batch 2
        // onwards, resetting the FASTQ defline spot number to 1.
        let mut cumulative_spots: u64 = 0;
        let mut blob_idx: usize = 0;
        while blob_idx < num_blobs {
            if let Some(ref flag) = config.cancelled
                && flag.load(Ordering::Relaxed)
            {
                break;
            }

            let batch_end = (blob_idx + BATCH_SIZE).min(num_blobs);

            let blob_id_ranges: Vec<u32> = (blob_idx..batch_end)
                .map(|bi| cursor.read_col().blobs()[bi].id_range)
                .collect();
            let (spots_before_per_blob, batch_spots) =
                spots_before_per_blob_in_batch(cumulative_spots, &blob_id_ranges);
            cumulative_spots += batch_spots;

            // Phase 3c gate (per-batch): if streaming, wait for the
            // bytes the upcoming rayon decode will read. We compute the
            // exact set of chunks each blob in the batch will touch
            // (READ + all auxiliary columns; ALTREAD blob boundaries
            // can differ from READ's so use find_blob) and wait on
            // exactly those chunks via `wait_chunks`.
            //
            // Multi-chunk (was: contiguous `wait_range(min, max)`)
            // matters because the column-priority hint above pulls
            // bytes for the FIRST batch of every column to the front
            // of the dispatch queue, so they arrive out of file order.
            // A contiguous wait would still block on the in-between
            // sequential bytes that aren't actually needed for this
            // batch, defeating the priority hint. With multi-chunk
            // wait the gate releases as soon as exactly the chunks
            // this batch needs are present.
            if let Some(tracker) = chunk_ready {
                let mut chunks: std::collections::HashSet<usize> = std::collections::HashSet::new();
                let mut accumulate = |range: Option<(u64, u64)>| {
                    let Some((s, e)) = range else { return };
                    if e <= s {
                        return;
                    }
                    let first = tracker.chunk_index_for_byte(s);
                    let last = tracker.chunk_index_for_byte(e - 1);
                    for c in first..=last {
                        chunks.insert(c);
                    }
                };
                for bi in blob_idx..batch_end {
                    let rb = &cursor.read_col().blobs()[bi];
                    accumulate(cursor.read_col().blob_absolute_range(rb));
                    if has_quality && bi < quality_blob_count {
                        let c = cursor.quality_col().unwrap();
                        accumulate(c.blob_absolute_range(&c.blobs()[bi]));
                    }
                    if has_read_len && bi < read_len_blob_count {
                        let c = cursor.read_len_col().unwrap();
                        accumulate(c.blob_absolute_range(&c.blobs()[bi]));
                    }
                    if has_name && bi < name_blob_count {
                        let c = cursor.name_col().unwrap();
                        accumulate(c.blob_absolute_range(&c.blobs()[bi]));
                    }
                    if has_read_type && bi < read_type_blob_count {
                        let c = cursor.read_type_col().unwrap();
                        accumulate(c.blob_absolute_range(&c.blobs()[bi]));
                    }
                    if has_altread {
                        let c = cursor.altread_col().unwrap();
                        if let Some(b) = c.find_blob(rb.start_id) {
                            accumulate(c.blob_absolute_range(b));
                        }
                    }
                    if has_illumina_name_parts && bi < x_blob_count {
                        let c = cursor.x_col().unwrap();
                        accumulate(c.blob_absolute_range(&c.blobs()[bi]));
                    }
                    if has_illumina_name_parts && bi < y_blob_count {
                        let c = cursor.y_col().unwrap();
                        accumulate(c.blob_absolute_range(&c.blobs()[bi]));
                    }
                }
                if !chunks.is_empty() {
                    let mut sorted: Vec<usize> = chunks.into_iter().collect();
                    sorted.sort_unstable();
                    tracker.wait_chunks(&sorted);
                }
            }

            let formatted_batches: Vec<Result<FormattedBlob>> = pool.install(|| {
                (blob_idx..batch_end)
                    .into_par_iter()
                    .enumerate()
                    .map(|(i, bi)| {
                        let read_blob = &cursor.read_col().blobs()[bi];
                        let read_raw = cursor.read_col().read_raw_blob_slice(read_blob.start_id)?;
                        let read_id_range = read_blob.id_range as u64;
                        let read_start_id = read_blob.start_id;

                        let (q_raw, q_id_range): (&[u8], u64) =
                            if has_quality && bi < quality_blob_count {
                                let qcol = cursor.quality_col().unwrap();
                                let qblob = &qcol.blobs()[bi];
                                (
                                    qcol.read_raw_blob_slice(qblob.start_id)?,
                                    qblob.id_range as u64,
                                )
                            } else {
                                (&[], 0)
                            };

                        // READ_LEN by row id, not blob index: READ_LEN's
                        // blobs may span more rows than READ's (DRR023226:
                        // 4,726 vs 4,908 blobs), so index pairing fetches
                        // the wrong lengths and overruns the READ buffer
                        // mid-blob, dropping every spot afterward. Mirrors
                        // the ALTREAD pattern below.
                        let (rl_raw, rl_id_range, rl_start_id): (&[u8], u64, i64) = if has_read_len
                        {
                            let rlcol = cursor.read_len_col().unwrap();
                            match rlcol.find_blob(read_start_id) {
                                Some(blob) => (
                                    rlcol.read_raw_blob_slice(blob.start_id)?,
                                    blob.id_range as u64,
                                    blob.start_id,
                                ),
                                None => (&[], 0, 0),
                            }
                        } else {
                            (&[], 0, 0)
                        };

                        let (n_raw, n_id_range): (&[u8], u64) = if has_name && bi < name_blob_count
                        {
                            let ncol = cursor.name_col().unwrap();
                            let nblob = &ncol.blobs()[bi];
                            (
                                ncol.read_raw_blob_slice(nblob.start_id)?,
                                nblob.id_range as u64,
                            )
                        } else {
                            (&[], 0)
                        };

                        let (rt_raw, rt_id_range): (&[u8], u64) =
                            if has_read_type && bi < read_type_blob_count {
                                let rtcol = cursor.read_type_col().unwrap();
                                let rtblob = &rtcol.blobs()[bi];
                                (
                                    rtcol.read_raw_blob_slice(rtblob.start_id)?,
                                    rtblob.id_range as u64,
                                )
                            } else {
                                (&[], 0)
                            };

                        // ALTREAD column: 4na ambiguity mask (also triggers
                        // Illumina name reconstruction when X + Y present).
                        //
                        // ALTREAD and READ can have *different blob
                        // boundaries*. E.g. DRR035866 has READ in 51 blobs
                        // of 4096 rows each, but ALTREAD in 50 blobs of
                        // 8192 rows each. Pairing by index (`col.blobs()[bi]`)
                        // fetches the wrong ALTREAD blob for most READ
                        // blobs and yields 4na overlays at totally unrelated
                        // row positions. Look up the ALTREAD blob that
                        // actually covers READ blob `bi`'s starting row id.
                        let (alt_raw, alt_id_range, alt_start_id): (&[u8], u64, i64) =
                            if has_altread {
                                let col = cursor.altread_col().unwrap();
                                match col.find_blob(read_start_id) {
                                    Some(blob) => (
                                        col.read_raw_blob_slice(blob.start_id)?,
                                        blob.id_range as u64,
                                        blob.start_id,
                                    ),
                                    None => (&[], 0, 0),
                                }
                            } else {
                                (&[], 0, 0)
                            };
                        let (xr, xi): (&[u8], u64) = if has_illumina_name_parts && bi < x_blob_count
                        {
                            let col = cursor.x_col().unwrap();
                            let blob = &col.blobs()[bi];
                            (
                                col.read_raw_blob_slice(blob.start_id)?,
                                blob.id_range as u64,
                            )
                        } else {
                            (&[], 0)
                        };
                        let (yr, yi): (&[u8], u64) = if has_illumina_name_parts && bi < y_blob_count
                        {
                            let col = cursor.y_col().unwrap();
                            let blob = &col.blobs()[bi];
                            (
                                col.read_raw_blob_slice(blob.start_id)?,
                                blob.id_range as u64,
                            )
                        } else {
                            (&[], 0)
                        };
                        // NAME_FMT by row id — its blob layout doesn't
                        // necessarily align 1:1 with READ's, similar to
                        // ALTREAD.
                        let (nfr, nfi): (&[u8], u64) = if has_name_fmt {
                            let col = cursor.name_fmt_col().unwrap();
                            match col.find_blob(read_start_id) {
                                Some(blob) => (
                                    col.read_raw_blob_slice(blob.start_id)?,
                                    blob.id_range as u64,
                                ),
                                None => (&[], 0),
                            }
                        } else {
                            (&[], 0)
                        };

                        let raw = RawBlobData {
                            read_raw,
                            read_id_range,
                            read_start_id,
                            quality_raw: q_raw,
                            quality_id_range: q_id_range,
                            quality_cs,
                            read_len_raw: rl_raw,
                            read_len_id_range: rl_id_range,
                            read_len_start_id: rl_start_id,
                            read_len_cs,
                            name_raw: n_raw,
                            name_id_range: n_id_range,
                            name_cs,
                            read_type_raw: rt_raw,
                            read_type_id_range: rt_id_range,
                            read_type_cs,
                            altread_raw: alt_raw,
                            altread_id_range: alt_id_range,
                            altread_start_id: alt_start_id,
                            altread_cs,
                            has_altread,
                            x_raw: xr,
                            x_id_range: xi,
                            x_cs,
                            y_raw: yr,
                            y_id_range: yi,
                            y_cs,
                            has_illumina_name_parts,
                            name_templates: &name_templates,
                            name_spot_starts: &name_spot_starts,
                            name_fmt_raw: nfr,
                            name_fmt_id_range: nfi,
                            name_fmt_cs,
                            has_name_fmt,
                            has_read_len,
                            has_name,
                            has_read_type,
                            metadata_reads_per_spot,
                            fixed_spot_len,
                            fallback_read_lengths: fallback_read_lengths.as_deref(),
                            fallback_read_types: fallback_read_types.as_deref(),
                        };

                        decode_blob_to_fastq(&raw, &decode_ctx, bi, spots_before_per_blob[i])
                    })
                    .collect()
            });

            // Send to writer thread (blocks if writer is behind by 2 batches).
            if batch_tx.send(formatted_batches).is_err() {
                break; // Writer thread exited (error) — rx is dropped
                // because the writer closure captured it by value (move).
            }

            blob_idx = batch_end;
        }

        // Signal writer we're done, then wait for it.
        drop(batch_tx);
        writer_handle.join().unwrap()
    });

    let tmp_paths: Vec<PathBuf> = output_paths.iter().map(|(_, t)| t.clone()).collect();
    let final_paths: Vec<PathBuf> = output_paths.iter().map(|(f, _)| f.clone()).collect();

    write_result?;

    // If cancelled, drop writers without finalizing and return Cancelled
    // with the list of partial output files so the caller can delete them.
    if let Some(ref flag) = config.cancelled
        && flag.load(Ordering::Relaxed)
    {
        if let Some(pb) = decode_pb {
            pb.finish_and_clear();
        }
        drop(writers);
        return Err(Error::Cancelled {
            output_files: tmp_paths,
        });
    }

    if let Some(pb) = decode_pb {
        pb.finish_and_clear();
    }

    let total_spots = spots_read.load(Ordering::Relaxed);
    tracing::debug!(
        "{accession}: streaming decode complete -- {total_spots} spots, {reads_written} reads written",
    );

    // Reconcile against RunInfo if available. Filters (skip_technical, min_read_len)
    // operate on reads within a spot, not on spots themselves, so every spot should
    // still be traversed.
    if let Some(expected) = config.run_info.as_ref().and_then(|ri| ri.spots)
        && expected != total_spots
    {
        // Don't rename partials into place if the spot count is wrong —
        // leave the `.partial` files so the user can inspect them but so
        // that no tool sees a superficially-complete FASTQ.
        return Err(Error::SpotCountMismatch {
            accession: accession.to_string(),
            expected,
            actual: total_spots,
        });
    }

    // Paired-split invariant: every record routed to Read1 should have a
    // mate in Read2. This cannot drift by construction (both are emitted
    // in the same iteration of decode_blob_to_fastq), but a mismatch would
    // indicate a filter/routing bug and must not ship silently.
    let r1 = per_slot_counts
        .get(&OutputSlot::Read1)
        .copied()
        .unwrap_or(0);
    let r2 = per_slot_counts
        .get(&OutputSlot::Read2)
        .copied()
        .unwrap_or(0);
    if r1 != r2 {
        diag.paired_spot_violations
            .fetch_add(r1.abs_diff(r2), std::sync::atomic::Ordering::Relaxed);
        tracing::warn!(
            "{accession}: Read1 count ({r1}) != Read2 count ({r2}) — paired-split invariant violated",
        );
    }

    // Finish all writers.
    if let Some(sw) = stdout_writer {
        sw.finish().map_err(Error::Io)?;
    }
    for (_, writer) in writers {
        writer.finish().map_err(Error::Io)?;
    }

    // Atomic promotion: rename each `.partial` to its final name. Do this
    // last so a crash mid-decode leaves `.partial` files rather than a
    // truncated FASTQ that looks valid to downstream tools.
    if !config.stdout {
        for (final_path, tmp_path) in &output_paths {
            std::fs::rename(tmp_path, final_path).map_err(Error::Io)?;
        }
    }

    Ok((total_spots, reads_written, final_paths))
}

/// Statistics from a completed fastq conversion (no download).
pub struct FastqStats {
    /// The accession/label used in FASTQ deflines.
    pub accession: String,
    /// Number of spots (rows) read from the SRA file.
    pub spots_read: u64,
    /// Number of FASTQ reads written (after filtering).
    pub reads_written: u64,
    /// Paths of all output files created.
    pub output_files: Vec<PathBuf>,
    /// Data-integrity counters captured during decode.
    pub integrity: Arc<IntegrityDiag>,
}

/// Convert a local SRA file to FASTQ without downloading.
///
/// Opens the SRA file as a KAR archive, creates a VdbCursor, decodes VDB
/// blobs, and writes FASTQ output. The `accession` is used for FASTQ
/// defline naming; if `None`, the filename stem is used.
pub fn run_fastq(
    sra_path: &std::path::Path,
    accession: Option<&str>,
    config: &PipelineConfig,
) -> Result<FastqStats> {
    let acc = accession.map(String::from).unwrap_or_else(|| {
        sra_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown")
            .to_string()
    });

    // Reference-compressed cSRA dispatch: archives with SEQUENCE.CMP_READ
    // plus sibling PRIMARY_ALIGNMENT + REFERENCE tables can't decode
    // through the regular VdbCursor (it reads physical READ only); route
    // them through CsraCursor's splice. v1 handles one uncompressed output
    // file and ignores split / compression / stdout flags — a follow-up
    // will port CsraCursor to the batched pipeline.
    let vdbcache_candidate = crate::vdb::csra::vdbcache_sidecar_path(sra_path);
    let vdbcache_for_probe = if vdbcache_candidate.exists() {
        Some(vdbcache_candidate.as_path())
    } else {
        None
    };
    if crate::vdb::csra::looks_like_decodable_csra(sra_path, vdbcache_for_probe).unwrap_or(false) {
        return run_fastq_csra(sra_path, &acc, config, vdbcache_for_probe);
    }

    // Detect SRA-lite by checking if the quality column is absent.
    // We pass `false` initially; decode_and_write will handle quality
    // absence gracefully via sra_lite_quality fallback.
    let is_lite = false;

    let diag = Arc::new(IntegrityDiag::default());
    let (spots_read, reads_written, output_files) =
        decode_and_write(sra_path, &acc, config, is_lite, &diag, None)
            .map_err(|e| wrap_blob_integrity(&acc, e))?;

    // Warn (but don't fail) when split-3 was requested on an effectively
    // single-end run: every spot produced exactly one read, so output
    // lands in `{accession}.fastq` with no `_1`/`_2` files. Users asking
    // for --split split-3 on single-end data usually didn't realize the
    // layout; surfacing this beats a silently-different filename.
    if matches!(config.split_mode, SplitMode::Split3)
        && spots_read > 0
        && reads_written == spots_read
    {
        eprintln!(
            "warning: {acc}: --split split-3 requested but run is single-end \
             ({spots_read} spots, 1 read each); output is {acc}.fastq"
        );
    }

    if !config.stdout
        && let Err(e) = write_stats_file(StatsEntry {
            output_dir: &config.output_dir,
            accession: &acc,
            spots_read,
            reads_written,
            sra_md5: None,
            sra_size: 0,
            output_files: &output_files,
            diag: &diag,
        })
    {
        tracing::warn!("{acc}: failed to append to sracha-stats.jsonl: {e}");
    }

    if diag.any() {
        let summary = diag.summary();
        if config.strict && diag.any_strict_fatal() {
            return Err(Error::IntegrityFailure {
                accession: acc,
                summary,
            });
        } else {
            tracing::warn!("{acc}: integrity counters non-zero — {summary}");
        }
    }

    Ok(FastqStats {
        accession: acc,
        spots_read,
        reads_written,
        output_files,
        integrity: diag,
    })
}

/// Compute the `spots_before` offset for each blob in a single decode
/// batch, given the cumulative spot count before the batch and the
/// batch's per-blob `id_range`s. Returns `(per_blob, total_spots_in_batch)`.
///
/// Extracted so the race-free accumulation pattern can be pinned with a
/// unit test without needing the 246 MiB DRR045255 fixture.
pub(crate) fn spots_before_per_blob_in_batch(
    cumulative_before_batch: u64,
    blob_id_ranges: &[u32],
) -> (Vec<u64>, u64) {
    let mut out = Vec::with_capacity(blob_id_ranges.len());
    let mut cum = cumulative_before_batch;
    for &id_range in blob_id_ranges {
        out.push(cum);
        cum += u64::from(id_range);
    }
    (out, cum - cumulative_before_batch)
}

#[cfg(test)]
mod batch_spots_tests {
    use super::spots_before_per_blob_in_batch;

    #[test]
    fn single_batch_starts_at_zero() {
        let (per_blob, total) = spots_before_per_blob_in_batch(0, &[100, 200, 50]);
        assert_eq!(per_blob, vec![0, 100, 300]);
        assert_eq!(total, 350);
    }

    #[test]
    fn second_batch_continues_from_cumulative() {
        // Regression for the BATCH_SIZE=1024 race: batch 2's per-blob
        // offsets must pick up from the running total at the end of
        // batch 1, not from a stale `spots_read` atomic that the writer
        // thread hadn't yet updated via `fetch_add`.
        let batch1_blobs: Vec<u32> = vec![1024; 1024];
        let (_, batch1_total) = spots_before_per_blob_in_batch(0, &batch1_blobs);
        assert_eq!(batch1_total, 1_048_576);

        let batch2_blobs: Vec<u32> = vec![1024, 1024, 1024];
        let (batch2, _) = spots_before_per_blob_in_batch(batch1_total, &batch2_blobs);
        // Pre-fix bug: batch2 would have started at 0 → [0, 1024, 2048].
        assert_eq!(batch2, vec![1_048_576, 1_049_600, 1_050_624]);
    }

    #[test]
    fn variable_blob_sizes_accumulate_precisely() {
        // DRR045255 had a 1032-row blob near spot 1 048 577. Mixing
        // blob sizes across batches still needs the right offset.
        let (_, b1) = spots_before_per_blob_in_batch(0, &vec![1024; 1023]);
        let (b2, _) = spots_before_per_blob_in_batch(b1, &[1032, 1024, 1024]);
        assert_eq!(b1, 1023 * 1024);
        assert_eq!(b2[0], 1_047_552);
        assert_eq!(b2[1], 1_048_584);
        assert_eq!(b2[2], 1_049_608);
    }

    #[test]
    fn empty_batch_returns_zero_total() {
        let (per_blob, total) = spots_before_per_blob_in_batch(1_000, &[]);
        assert!(per_blob.is_empty());
        assert_eq!(total, 0);
    }
}

/// cSRA decode path — drives `CsraCursor` through the same FASTQ writer
/// infrastructure (split modes, compression, stdout) as the regular
/// pipeline, so all of `PipelineConfig` honours through.
fn run_fastq_csra(
    sra_path: &std::path::Path,
    acc: &str,
    config: &PipelineConfig,
    vdbcache_path: Option<&std::path::Path>,
) -> Result<FastqStats> {
    use crate::fastq::{CompressionMode, FastqConfig, OutputSlot, SpotRecord, output_filename};
    use crate::vdb::csra::CsraCursor;
    use crate::vdb::kar::KarArchive;
    use crate::vdb::restore::fourna_to_ascii;

    let file = std::fs::File::open(sra_path)?;
    let mut archive = KarArchive::open(std::io::BufReader::new(file))?;
    // Open the vdbcache sidecar when present. Each decode worker opens
    // its own copy below (mmap/file handles are not Send across rayon
    // threads), so the top-level one is only used for the SEQUENCE
    // open/row_count probe.
    let mut vdbcache_archive = match vdbcache_path {
        Some(p) => Some(KarArchive::open(std::io::BufReader::new(
            std::fs::File::open(p)?,
        ))?),
        None => None,
    };
    let csra = {
        let vdbcache_for_open: Option<(&mut KarArchive<_>, &std::path::Path)> =
            match (&mut vdbcache_archive, vdbcache_path) {
                (Some(a), Some(p)) => Some((a, p)),
                _ => None,
            };
        CsraCursor::open_any(&mut archive, sra_path, vdbcache_for_open)?
    };

    let fastq_config = FastqConfig {
        split_mode: config.split_mode,
        skip_technical: config.skip_technical,
        min_read_len: config.min_read_len,
        fasta: config.fasta,
    };

    if !config.stdout {
        std::fs::create_dir_all(&config.output_dir)?;
    }

    let mut writers: HashMap<OutputSlot, OutputWriter> = HashMap::new();
    let mut output_paths: Vec<(PathBuf, PathBuf)> = Vec::new();
    let mut stdout_writer: Option<OutputWriter> = if config.stdout {
        Some(OutputWriter::Stdout(std::io::BufWriter::with_capacity(
            256 * 1024,
            std::io::stdout(),
        )))
    } else {
        None
    };

    // Gzip pool reused across slot writers when --compress gzip is set.
    let compress_pool: Option<Arc<rayon::ThreadPool>> = match config.compression {
        CompressionMode::Gzip { .. } => {
            let n = config.threads.max(1);
            Some(Arc::new(
                rayon::ThreadPoolBuilder::new()
                    .num_threads(n)
                    .build()
                    .map_err(|e| Error::Pipeline(format!("gzip threadpool: {e}")))?,
            ))
        }
        _ => None,
    };

    // Partition the spot range into chunks of CHUNK_SIZE for parallel
    // decode. Each rayon worker opens its own CsraCursor (mmap is cheap;
    // actual I/O happens on-demand), decodes its spot range, returns a
    // per-slot byte buffer. Main thread writes chunks in order to the
    // shared writers, preserving spot order across all outputs.
    //
    // Opening CsraCursor inside rayon workers means we don't need to
    // share the top-level one; drop it here to release its file handle
    // and avoid the small double-open cost on single-threaded paths.
    let total_spots = csra.row_count();
    let first_row = csra.first_row();
    drop(csra);
    drop(archive);

    // Chunk size picked to (a) amortise per-chunk cursor-open overhead
    // (each chunk re-mmaps the SEQUENCE / PRIMARY_ALIGNMENT / REFERENCE
    // columns), and (b) give rayon enough work items that all threads
    // stay busy — roughly 8 chunks per thread so stragglers in one
    // thread don't pile up.
    let num_threads = config.threads.max(1);
    let chunk_size: u64 = {
        let target_chunks = (num_threads as u64) * 8;
        let raw = total_spots.div_ceil(target_chunks.max(1));
        raw.clamp(32, 1024)
    };
    let mut spots_read = 0u64;
    let mut reads_written = 0u64;

    // When there's only one thread or the archive is tiny, skip the
    // parallel path — each worker carries ~20 mmap setups of overhead.
    let use_parallel = num_threads > 1 && total_spots >= chunk_size * 2;

    let chunks: Vec<(i64, i64)> = {
        let mut out = Vec::new();
        let mut start = first_row;
        let end = first_row + total_spots as i64;
        while start < end {
            let chunk_end = (start + chunk_size as i64).min(end);
            out.push((start, chunk_end));
            start = chunk_end;
        }
        out
    };

    // Decode each chunk. The worker returns an ordered Vec of
    // (slot, record-bytes, record-count) so the writer stays sequential.
    type SlotChunk = (OutputSlot, Vec<u8>, u64);
    let decode_chunk = |start: i64, end: i64| -> Result<Vec<SlotChunk>> {
        let file = std::fs::File::open(sra_path)?;
        let mut archive = KarArchive::open(std::io::BufReader::new(file))?;
        let mut cache_archive = match vdbcache_path {
            Some(p) => Some(KarArchive::open(std::io::BufReader::new(
                std::fs::File::open(p)?,
            ))?),
            None => None,
        };
        let csra = {
            let cache_opt: Option<(&mut KarArchive<_>, &std::path::Path)> =
                match (&mut cache_archive, vdbcache_path) {
                    (Some(a), Some(p)) => Some((a, p)),
                    _ => None,
                };
            CsraCursor::open_any(&mut archive, sra_path, cache_opt)?
        };
        let mut per_slot: HashMap<OutputSlot, (Vec<u8>, u64)> = HashMap::new();
        let mut itoa_buf = itoa::Buffer::new();
        for row_id in start..end {
            let s = csra.read_spot(row_id)?;
            let sequence = fourna_to_ascii(&s.bases);
            let quality: Vec<u8> = s.quality.iter().map(|q| q.wrapping_add(33)).collect();
            let read_types: Vec<u8> = s
                .read_types
                .iter()
                .map(|&rt| if rt & 0x01 != 0 { 0 } else { 1 })
                .collect();
            let spot_num_str = itoa_buf.format(row_id).to_string();
            let spot = SpotRecord {
                name: spot_num_str.as_bytes().to_vec(),
                sequence,
                quality,
                read_lengths: s.read_lens,
                read_types,
                read_filter: Vec::new(),
                spot_group: Vec::new(),
            };
            for (slot, rec) in crate::fastq::format_spot(&spot, acc, &fastq_config) {
                let entry = per_slot.entry(slot).or_insert_with(|| (Vec::new(), 0));
                entry.0.extend_from_slice(&rec.data);
                entry.1 += 1;
            }
        }
        Ok(per_slot
            .into_iter()
            .map(|(slot, (bytes, count))| (slot, bytes, count))
            .collect())
    };

    // Create a writer for this slot lazily (mirrors the plain path).
    let get_writer = |slot: OutputSlot,
                      writers: &mut HashMap<OutputSlot, OutputWriter>,
                      output_paths: &mut Vec<(PathBuf, PathBuf)>|
     -> Result<()> {
        if writers.contains_key(&slot) {
            return Ok(());
        }
        let filename = output_filename(acc, slot, config.fasta, &config.compression);
        let final_path = config.output_dir.join(&filename);
        let tmp_path = config.output_dir.join(format!("{filename}.partial"));
        output_paths.push((final_path, tmp_path.clone()));
        let file = std::fs::File::create(&tmp_path)?;
        let buf = std::io::BufWriter::with_capacity(256 * 1024, file);
        let w = match config.compression {
            CompressionMode::Gzip { level } => OutputWriter::Gz(ParGzWriter::new(
                buf,
                level,
                DEFAULT_BLOCK_SIZE,
                compress_pool.clone().expect("gzip pool must exist"),
            )),
            CompressionMode::Zstd { level, threads } => {
                let mut encoder = zstd::stream::write::Encoder::new(buf, level)
                    .map_err(|e| Error::Pipeline(format!("zstd encoder: {e}")))?;
                encoder
                    .multithread(threads)
                    .map_err(|e| Error::Pipeline(format!("zstd threads: {e}")))?;
                OutputWriter::Zstd(encoder)
            }
            CompressionMode::None => OutputWriter::Plain(buf),
        };
        writers.insert(slot, w);
        Ok(())
    };

    if use_parallel {
        use rayon::prelude::*;
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build()
            .map_err(|e| Error::Pipeline(format!("cSRA threadpool: {e}")))?;
        let results: Vec<Result<Vec<SlotChunk>>> = pool.install(|| {
            chunks
                .par_iter()
                .map(|(s, e)| decode_chunk(*s, *e))
                .collect()
        });
        for (chunk_res, (s, e)) in results.into_iter().zip(chunks.iter()) {
            let chunk = chunk_res?;
            spots_read += (e - s) as u64;
            for (slot, bytes, records) in chunk {
                if let Some(ref mut sw) = stdout_writer {
                    sw.write_all(&bytes).map_err(Error::Io)?;
                } else {
                    get_writer(slot, &mut writers, &mut output_paths)?;
                    writers
                        .get_mut(&slot)
                        .unwrap()
                        .write_all(&bytes)
                        .map_err(Error::Io)?;
                }
                reads_written += records;
            }
        }
    } else {
        for &(s, e) in &chunks {
            let chunk = decode_chunk(s, e)?;
            spots_read += (e - s) as u64;
            for (slot, bytes, records) in chunk {
                if let Some(ref mut sw) = stdout_writer {
                    sw.write_all(&bytes).map_err(Error::Io)?;
                } else {
                    get_writer(slot, &mut writers, &mut output_paths)?;
                    writers
                        .get_mut(&slot)
                        .unwrap()
                        .write_all(&bytes)
                        .map_err(Error::Io)?;
                }
                reads_written += records;
            }
        }
    }

    if let Some(sw) = stdout_writer {
        sw.finish().map_err(Error::Io)?;
    }
    for (_, writer) in writers {
        writer.finish().map_err(Error::Io)?;
    }

    let mut final_paths = Vec::with_capacity(output_paths.len());
    if !config.stdout {
        for (final_path, tmp_path) in &output_paths {
            std::fs::rename(tmp_path, final_path).map_err(Error::Io)?;
            final_paths.push(final_path.clone());
        }
    }

    Ok(FastqStats {
        accession: acc.to_string(),
        spots_read,
        reads_written,
        output_files: final_paths,
        integrity: Arc::new(IntegrityDiag::default()),
    })
}

/// Result of the download phase of `run_get`.
pub struct DownloadedSra {
    /// Path to the temporary SRA file on disk.
    pub temp_path: PathBuf,
    /// Bytes actually transferred over the network this session.
    pub bytes_transferred: u64,
    /// Total SRA file size on the server.
    pub total_sra_size: u64,
    /// Whether this is an SRA-lite file.
    pub is_lite: bool,
    /// The accession string.
    pub accession: String,
    /// MD5 of the SRA file (computed or verified during download).
    pub sra_md5: Option<String>,
    /// Per-chunk readiness tracker (see
    /// [`crate::download::chunk_ready::ChunkReadyTracker`]). `Some` only
    /// when the parallel-chunked download path was used. By the time
    /// `download_sra` returns successfully, every chunk is marked done
    /// — the tracker is Phase-2 plumbing for a future Phase 3b that
    /// runs decode concurrently with download. Phase 3a code paths
    /// just call `tracker.await_all()` at decode entry as a no-op
    /// gate that proves the wiring works without changing behavior.
    pub chunk_ready: Option<Arc<crate::download::chunk_ready::ChunkReadyTracker>>,
}

/// Download an SRA file to a temporary location.
///
/// This is the download-only phase of `run_get`, separated so that callers
/// can overlap the download of the next accession with the decode of the
/// current one.
pub async fn download_sra(
    resolved: &ResolvedAccession,
    config: &PipelineConfig,
) -> Result<DownloadedSra> {
    download_sra_inner(resolved, config, None).await
}

/// Streaming variant of [`download_sra`]: hands the freshly-constructed
/// `ChunkReadyTracker` out via `tracker_init` as soon as it's available
/// (typically <100 ms after function entry, on the parallel-chunked
/// path). Callers can then await byte-range readiness on the tracker
/// concurrently with the rest of the download.
///
/// On the single-stream fallback (file < `SMALL_FILE`) or an early
/// failure, the sender is dropped without sending — receivers should
/// treat `RecvError` as "no streaming available, await this future and
/// process the result normally."
pub async fn download_sra_streaming(
    resolved: &ResolvedAccession,
    config: &PipelineConfig,
    tracker_init: tokio::sync::oneshot::Sender<
        Arc<crate::download::chunk_ready::ChunkReadyTracker>,
    >,
) -> Result<DownloadedSra> {
    download_sra_inner(resolved, config, Some(tracker_init)).await
}

/// First-stage metadata gate for streaming decode. Waits for the bytes
/// `KarArchive::open` needs to read (24-byte header + the TOC, whose
/// extent is encoded as `header.file_offset`).
///
/// Returns once enough of the file is on disk that `KarArchive::open`
/// can be called without hitting sparse holes.
fn wait_metadata_ready(
    sra_path: &std::path::Path,
    tracker: &Arc<crate::download::chunk_ready::ChunkReadyTracker>,
) -> Result<()> {
    use std::io::Read;
    // KAR header is 24 bytes at offset 0; chunk 0 always covers it.
    tracker.wait_range(0, 24);
    let mut hdr = [0u8; 24];
    let mut f = std::fs::File::open(sra_path)?;
    f.read_exact(&mut hdr)?;
    // Bytes 16..24 hold the little-endian file_offset (start of the
    // data section). Anything before that is the TOC and must be on
    // disk before we can parse it.
    let file_offset = u64::from_le_bytes(hdr[16..24].try_into().unwrap());
    tracker.wait_range(0, file_offset);
    Ok(())
}

/// Wrap a download future with the standard cancellation handler:
/// honor `config.cancelled` if set, removing the temp file + progress
/// sidecar before bubbling `Error::Cancelled`. Extracted so the two
/// `match` arms in [`download_sra_inner`] (streaming vs legacy) can
/// share the cancellation logic.
async fn await_with_cancel<F>(
    fut: F,
    config: &PipelineConfig,
    temp_path: &Path,
    accession: &str,
) -> Result<crate::download::DownloadResult>
where
    F: std::future::Future<Output = Result<crate::download::DownloadResult>>,
{
    if let Some(ref flag) = config.cancelled {
        let flag = flag.clone();
        tokio::select! {
            result = fut => result,
            _ = poll_cancelled(flag) => {
                tracing::info!("{accession}: download cancelled");
                let _ = tokio::fs::remove_file(temp_path).await;
                let sidecar = crate::download::progress_path(temp_path);
                let _ = tokio::fs::remove_file(&sidecar).await;
                Err(Error::Cancelled { output_files: vec![] })
            }
        }
    } else {
        fut.await
    }
}

async fn download_sra_inner(
    resolved: &ResolvedAccession,
    config: &PipelineConfig,
    tracker_init: Option<
        tokio::sync::oneshot::Sender<Arc<crate::download::chunk_ready::ChunkReadyTracker>>,
    >,
) -> Result<DownloadedSra> {
    let accession = &resolved.accession;
    let total_sra_size = resolved.sra_file.size;

    // Delete completion marker when --force is used.
    if config.force {
        let _ = std::fs::remove_file(marker_path(&config.output_dir, accession));
    }

    // If outputs already exist (validated via completion marker), skip entirely.
    if !config.force
        && !config.stdout
        && check_completion_marker(&config.output_dir, accession, config, total_sra_size).is_some()
    {
        tracing::info!("{accession}: outputs already exist, skipping download");
        let temp_filename = format!(".sracha-tmp-{accession}.sra");
        let temp_path = config.output_dir.join(&temp_filename);
        return Ok(DownloadedSra {
            temp_path,
            bytes_transferred: 0,
            total_sra_size,
            is_lite: resolved.sra_file.is_lite,
            accession: accession.clone(),
            sra_md5: resolved.sra_file.md5.clone(),
            // Outputs already exist; no download happened, so no
            // streaming tracker is meaningful.
            chunk_ready: None,
        });
    }

    let url = select_mirror(resolved)?;
    let urls = vec![url.clone()];

    tracing::debug!("{accession}: starting full download from {url}");

    let temp_filename = format!(".sracha-tmp-{accession}.sra");
    let temp_path = config.output_dir.join(&temp_filename);

    tokio::fs::create_dir_all(&config.output_dir).await?;

    let dl_config = DownloadConfig {
        connections: config.connections,
        chunk_size: 0,
        force: config.force,
        validate: true,
        progress: config.progress,
        resume: config.resume,
        client: config.http_client.clone(),
        // KAR magic: "NCBI" + ".sra". Guards the size-match-skip path when
        // SDL didn't supply an MD5, so a corrupt temp SRA from a crashed
        // prior run can't feed garbage into the VDB decoder.
        expected_prefix: Some(b"NCBI.sra".to_vec()),
        progress_parent: config.progress_parent.clone(),
        progress_combined: config.progress_combined.clone(),
    };

    tracing::info!(
        "{accession}: downloading {} to {}",
        crate::util::format_size(total_sra_size),
        temp_path.display(),
    );

    // Pick the streaming variant when a tracker_init sender was passed
    // in; otherwise the legacy non-streaming path.
    let dl_result = match tracker_init {
        Some(tx) => {
            let dl_future = crate::download::download_file_streaming(
                &urls,
                total_sra_size,
                resolved.sra_file.md5.as_deref(),
                &temp_path,
                &dl_config,
                tx,
            );
            await_with_cancel(dl_future, config, &temp_path, accession).await?
        }
        None => {
            let dl_future = download_file(
                &urls,
                total_sra_size,
                resolved.sra_file.md5.as_deref(),
                &temp_path,
                &dl_config,
            );
            await_with_cancel(dl_future, config, &temp_path, accession).await?
        }
    };

    tracing::info!(
        "{accession}: download complete ({})",
        crate::util::format_size(dl_result.size),
    );

    Ok(DownloadedSra {
        temp_path,
        bytes_transferred: dl_result.bytes_transferred,
        total_sra_size,
        is_lite: resolved.sra_file.is_lite,
        accession: accession.clone(),
        sra_md5: dl_result.md5,
        chunk_ready: dl_result.chunk_ready,
    })
}

/// Download pre-computed ENA FASTQ.gz files directly — no VDB decode involved.
///
/// Each file listed in `ena` is fetched via [`download_file`] (parallel chunked
/// HTTP + resume + MD5 verify) to its final output path. The caller guarantees
/// the pipeline config is compatible (gzip + split-3/split-files + no fasta +
/// no stdout); incompatible configs should be routed to [`download_sra`] +
/// [`decode_sra`] instead.
///
/// On cancellation, any partially-written output files are left on disk so the
/// `download_file` resume machinery can finish them on the next run.
pub async fn download_ena_fastq(
    ena: &crate::ena::EnaResolved,
    config: &PipelineConfig,
) -> Result<PipelineStats> {
    let accession = &ena.accession;
    tokio::fs::create_dir_all(&config.output_dir).await?;

    let dl_config = DownloadConfig {
        connections: config.connections,
        chunk_size: 0,
        force: config.force,
        validate: true,
        progress: config.progress,
        resume: config.resume,
        client: config.http_client.clone(),
        // ENA paths always supply MD5s for resume verification, so the
        // prefix fallback isn't needed here.
        expected_prefix: None,
        progress_parent: config.progress_parent.clone(),
        progress_combined: config.progress_combined.clone(),
    };

    let mut output_files: Vec<PathBuf> = Vec::with_capacity(ena.fastq_files.len());
    let mut bytes_transferred: u64 = 0;

    for file in &ena.fastq_files {
        // Honor Ctrl-C between files; download_file itself doesn't poll
        // `config.cancelled`, so check here and surface partial outputs.
        if let Some(ref flag) = config.cancelled
            && flag.load(Ordering::Relaxed)
        {
            return Err(Error::Cancelled {
                output_files: output_files.clone(),
            });
        }

        let name = output_filename(accession, file.slot, config.fasta, &config.compression);
        let target = config.output_dir.join(&name);

        tracing::info!(
            "{accession}: downloading ENA FASTQ {} ({}) → {}",
            file.url,
            crate::util::format_size(file.size),
            target.display(),
        );

        let urls = vec![file.url.clone()];
        let dl_future = download_file(&urls, file.size, Some(&file.md5), &target, &dl_config);

        let dl_result = if let Some(ref flag) = config.cancelled {
            let flag = flag.clone();
            tokio::select! {
                result = dl_future => result?,
                _ = poll_cancelled(flag) => {
                    tracing::info!("{accession}: ENA download cancelled");
                    return Err(Error::Cancelled { output_files });
                }
            }
        } else {
            dl_future.await?
        };

        bytes_transferred += dl_result.bytes_transferred;
        output_files.push(dl_result.path);
    }

    Ok(PipelineStats {
        accession: accession.clone(),
        // Spots/reads are unknown without decoding the gzip stream. Counting
        // would defeat the point of the fast path, so we report 0 and the CLI
        // emits an ENA-specific summary line.
        spots_read: 0,
        reads_written: 0,
        bytes_transferred,
        total_sra_size: ena.total_size,
        output_files,
        integrity: Arc::new(IntegrityDiag::default()),
    })
}

/// Poll an `AtomicBool` flag until it becomes `true`.
async fn poll_cancelled(flag: Arc<AtomicBool>) {
    loop {
        if flag.load(Ordering::Relaxed) {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

/// Decode a previously downloaded SRA file into FASTQ and clean up the temp file.
///
/// This is the decode phase of `run_get`. Call from within
/// `tokio::task::block_in_place` or a blocking thread.
pub fn decode_sra(downloaded: &DownloadedSra, config: &PipelineConfig) -> Result<PipelineStats> {
    // Check if decode can be skipped (unless --force or stdout mode).
    if let Some(output_files) = (!config.force && !config.stdout)
        .then(|| {
            check_completion_marker(
                &config.output_dir,
                &downloaded.accession,
                config,
                downloaded.total_sra_size,
            )
        })
        .flatten()
    {
        tracing::info!(
            "{}: output files already exist and match, skipping decode",
            downloaded.accession,
        );
        // Clean up temp SRA if the download produced one (cache hit path).
        let _ = std::fs::remove_file(&downloaded.temp_path);
        let sidecar = crate::download::progress_path(&downloaded.temp_path);
        let _ = std::fs::remove_file(&sidecar);

        return Ok(PipelineStats {
            accession: downloaded.accession.clone(),
            spots_read: 0,
            reads_written: 0,
            bytes_transferred: downloaded.bytes_transferred,
            total_sra_size: downloaded.total_sra_size,
            output_files,
            integrity: Arc::new(IntegrityDiag::default()),
        });
    }

    // Phase 3c: granular gating now lives inside `decode_and_write`
    // (metadata waits at entry, per-batch waits in the decode loop).
    // The Phase 3a coarse `wait_all()` is removed so streaming decode
    // can actually overlap with download. When `chunk_ready` is None
    // (single-stream fallback or already-on-disk path) decode_and_write
    // skips the gates entirely.
    let diag = Arc::new(IntegrityDiag::default());
    let (spots_read, reads_written, output_files) = match decode_and_write(
        &downloaded.temp_path,
        &downloaded.accession,
        config,
        downloaded.is_lite,
        &diag,
        downloaded.chunk_ready.as_ref(),
    ) {
        Ok(result) => result,
        Err(Error::Cancelled { output_files }) => {
            // Delete completion marker (may not exist yet).
            let _ = std::fs::remove_file(marker_path(&config.output_dir, &downloaded.accession));
            // Delete partial FASTQ output files.
            for path in &output_files {
                if let Err(e) = std::fs::remove_file(path) {
                    tracing::warn!(
                        "{}: failed to remove partial file {}: {e}",
                        downloaded.accession,
                        path.display(),
                    );
                }
            }
            // In stdout mode, always delete the temp SRA (streaming should
            // leave no artifacts). Otherwise keep it so the next run can
            // skip the download.
            if config.stdout {
                let _ = std::fs::remove_file(&downloaded.temp_path);
                tracing::info!(
                    "{}: cancelled, cleaned up {} partial output file(s) and temp SRA",
                    downloaded.accession,
                    output_files.len(),
                );
            } else {
                tracing::info!(
                    "{}: cancelled, cleaned up {} partial output file(s) \
                     (temp SRA kept — next run will skip download)",
                    downloaded.accession,
                    output_files.len(),
                );
            }
            return Err(Error::Cancelled {
                output_files: vec![],
            });
        }
        Err(e) => return Err(wrap_blob_integrity(&downloaded.accession, e)),
    };

    // Clean up temp file (or preserve it in the output dir if requested).
    if config.keep_sra && !config.stdout {
        let kept = config
            .output_dir
            .join(format!("{}.sra", downloaded.accession));
        if let Err(e) = std::fs::rename(&downloaded.temp_path, &kept) {
            tracing::warn!(
                "{}: failed to move temp SRA {} -> {}: {e}",
                downloaded.accession,
                downloaded.temp_path.display(),
                kept.display(),
            );
        }
        // Drop the progress sidecar — download is fully verified at this point.
        let sidecar = crate::download::progress_path(&downloaded.temp_path);
        let _ = std::fs::remove_file(&sidecar);
    } else if let Err(e) = std::fs::remove_file(&downloaded.temp_path) {
        tracing::warn!(
            "{}: failed to remove temp file {}: {e}",
            downloaded.accession,
            downloaded.temp_path.display(),
        );
    }

    // Write completion marker so future runs can skip this accession.
    if !config.stdout
        && let Err(e) = write_completion_marker(
            &config.output_dir,
            &downloaded.accession,
            downloaded.sra_md5.as_deref(),
            downloaded.total_sra_size,
            config,
            &output_files,
        )
    {
        tracing::warn!(
            "{}: failed to write completion marker: {e}",
            downloaded.accession,
        );
    }

    // Append one JSONL line per accession (passing or failing) to the
    // shared `sracha-stats.jsonl`. For a BioProject-scale run this yields
    // a single grep-able audit log instead of one file per accession.
    if !config.stdout
        && let Err(e) = write_stats_file(StatsEntry {
            output_dir: &config.output_dir,
            accession: &downloaded.accession,
            spots_read,
            reads_written,
            sra_md5: downloaded.sra_md5.as_deref(),
            sra_size: downloaded.total_sra_size,
            output_files: &output_files,
            diag: &diag,
        })
    {
        tracing::warn!(
            "{}: failed to append to sracha-stats.jsonl: {e}",
            downloaded.accession,
        );
    }

    // `downloaded.bytes_transferred` may be 0 here even on a real network
    // transfer: when called from `run_get_streaming` the value is a
    // placeholder (the real number isn't known until the parallel download
    // task finishes; the orchestrator patches it into the returned
    // PipelineStats afterward). Suppress the misleading "0 B transferred"
    // line in that case — the CLI prints the correct totals at the end
    // anyway.
    let bytes_msg = if downloaded.bytes_transferred == 0 {
        "(bytes transferred reported by orchestrator)".to_string()
    } else {
        format!(
            "{} transferred",
            crate::util::format_size(downloaded.bytes_transferred)
        )
    };
    tracing::info!(
        "{}: done -- {spots_read} spots, {reads_written} reads written, {bytes_msg}",
        downloaded.accession,
    );

    if diag.any() {
        let summary = diag.summary();
        if config.strict && diag.any_strict_fatal() {
            return Err(Error::IntegrityFailure {
                accession: downloaded.accession.clone(),
                summary,
            });
        } else {
            tracing::warn!(
                "{}: integrity counters non-zero — {summary}",
                downloaded.accession,
            );
        }
    }

    Ok(PipelineStats {
        accession: downloaded.accession.clone(),
        spots_read,
        reads_written,
        bytes_transferred: downloaded.bytes_transferred,
        total_sra_size: downloaded.total_sra_size,
        output_files,
        integrity: diag,
    })
}

/// Run the full get pipeline for a single accession.
///
/// Convenience wrapper that calls [`download_sra`] then [`decode_sra`].
/// For multi-accession prefetch, use those functions directly.
pub async fn run_get(
    resolved: &ResolvedAccession,
    config: &PipelineConfig,
) -> Result<PipelineStats> {
    let downloaded = download_sra(resolved, config).await?;
    tokio::task::block_in_place(|| decode_sra(&downloaded, config))
}

/// Streaming variant of [`run_get`]: starts the FASTQ decode while
/// the SRA download is still in flight.
///
/// **How it works** (post-Phase 3c+3d):
/// 1. Download is spawned as a background Tokio task. As soon as the
///    parallel-chunked path constructs its `ChunkReadyTracker`, the
///    tracker is delivered to us via a oneshot.
/// 2. With the tracker in hand we synthesize a `DownloadedSra` for the
///    decoder using the fields we already know (temp path, accession,
///    `is_lite` from SDL metadata). `bytes_transferred` is unknown at
///    this point — we patch it in from the real `DownloadedSra` after
///    the download task finishes.
/// 3. Decode runs in `spawn_blocking`. Its first action is the
///    streaming-decode metadata gate (waits for KAR header + TOC + idx
///    files); each subsequent batch waits for the chunks covering its
///    blobs before the rayon decode block runs. Both threads make
///    progress in parallel.
/// 4. We `try_join!` download and decode; the decoder finishes shortly
///    after the last chunk lands.
///
/// On the single-stream fallback (file < `SMALL_FILE`) the tracker is
/// never sent — we transparently fall through to the same await-then-
/// decode flow [`run_get`] uses, with no streaming overlap.
pub async fn run_get_streaming(
    resolved: &ResolvedAccession,
    config: &PipelineConfig,
) -> Result<PipelineStats> {
    // Phase 4d UX: replace the old dual-bar MultiProgress wiring
    // (separate download + decode bars, the latter of which stalled
    // during per-batch waits) with a single combined "work" bar. The
    // download worker ticks this bar by each chunk's byte share; the
    // decode writer ticks it by each blob's share. Both sides
    // contribute to the same 10000-unit total, so the bar advances
    // monotonically whenever ANY work is happening — no freeze during
    // streaming decode stalls.
    //
    // MultiProgress still gets created when progress is enabled, so
    // any `tracing` output dumped from the pipeline doesn't overwrite
    // the bar — the bar is `.add`'d to the MultiProgress and logs
    // render above it automatically.
    let combined_pb = if config.progress && config.progress_combined.is_none() {
        let bar = make_styled_pb(
            10_000,
            "  {elapsed_precise} [{bar:40.cyan}] {percent}%  {msg}",
        );
        bar.set_message("streaming download + decode");
        Some(Arc::new(bar))
    } else {
        config.progress_combined.clone()
    };
    let mp = if config.progress && config.progress_parent.is_none() && combined_pb.is_some() {
        let new_mp = Arc::new(indicatif::MultiProgress::new());
        if let Some(ref bar) = combined_pb {
            // Registering lets `tracing` output interleave cleanly
            // above the bar rather than clobbering it.
            new_mp.add((**bar).clone());
        }
        Some(new_mp)
    } else {
        config.progress_parent.clone()
    };
    let mut config_with_mp: PipelineConfig = config.clone();
    config_with_mp.progress_parent = mp.clone();
    config_with_mp.progress_combined = combined_pb.clone();
    // Suppress the individual per-file download bar when the combined
    // bar is taking over — otherwise the download bar would render in
    // parallel with the combined bar, defeating the "single bar" UX.
    if combined_pb.is_some() {
        config_with_mp.progress = false;
    }

    let (tracker_tx, tracker_rx) = tokio::sync::oneshot::channel();
    let resolved_owned = resolved.clone();
    let config_for_dl = config_with_mp.clone();

    // Spawn the download as a Tokio task. Args must be 'static.
    let dl_handle: tokio::task::JoinHandle<Result<DownloadedSra>> = tokio::spawn(async move {
        download_sra_streaming(&resolved_owned, &config_for_dl, tracker_tx).await
    });

    // Receive the tracker. Err means the sender was dropped without
    // firing — either the download took the single-stream path or
    // failed before reaching the tracker-construction site.
    let early_tracker = tracker_rx.await.ok();

    let Some(tracker) = early_tracker else {
        // No streaming available — degrade gracefully to await-then-decode.
        let downloaded = dl_handle
            .await
            .map_err(|e| Error::Pipeline(format!("download task panicked: {e}")))??;
        return tokio::task::block_in_place(|| decode_sra(&downloaded, &config_with_mp));
    };

    // Synthesize the DownloadedSra fields the decoder needs upfront.
    // Everything except `bytes_transferred` is known from `resolved` +
    // the temp-path convention; we patch `bytes_transferred` in from
    // the real result after the download task finishes.
    let temp_path = config
        .output_dir
        .join(format!(".sracha-tmp-{}.sra", resolved.accession));
    let synthetic = DownloadedSra {
        temp_path,
        bytes_transferred: 0,
        total_sra_size: resolved.sra_file.size,
        is_lite: resolved.sra_file.is_lite,
        accession: resolved.accession.clone(),
        sra_md5: resolved.sra_file.md5.clone(),
        chunk_ready: Some(tracker),
    };

    // Spawn decode on a blocking thread (decode_sra is sync and uses
    // rayon internally; spawn_blocking gives it a dedicated OS thread
    // off the Tokio worker pool). It runs concurrently with the
    // download task — its metadata + per-batch gates inside
    // `decode_and_write` block on chunk readiness as needed.
    let config_for_decode = config_with_mp.clone();
    let decode_handle: tokio::task::JoinHandle<Result<PipelineStats>> =
        tokio::task::spawn_blocking(move || decode_sra(&synthetic, &config_for_decode));

    // Await both. The download finishes first (download's MD5 + sidecar
    // cleanup are part of `download_sra_streaming`); decode finishes
    // shortly after the last chunk's bytes are wait_range-released.
    let dl_result = dl_handle
        .await
        .map_err(|e| Error::Pipeline(format!("download task panicked: {e}")))?;
    let downloaded = dl_result?;
    let decode_result = decode_handle
        .await
        .map_err(|e| Error::Pipeline(format!("decode task panicked: {e}")))?;
    let mut stats = decode_result?;

    // Patch in the real bytes_transferred (synthetic was 0).
    stats.bytes_transferred = downloaded.bytes_transferred;
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::blob_decode::{encode_raw_quality_for_fastq, expand_via_page_map};
    use super::marker::{iso8601_now_utc, stats_path};
    use super::*;
    use crate::sdl::{ResolvedFile, ResolvedMirror};
    use crate::vdb::blob;

    fn make_resolved(mirrors: Vec<ResolvedMirror>) -> ResolvedAccession {
        ResolvedAccession {
            accession: "SRR000001".into(),
            sra_file: ResolvedFile {
                mirrors,
                size: 1000,
                md5: None,
                is_lite: false,
            },
            vdbcache_file: None,
            run_info: None,
        }
    }

    #[test]
    fn select_mirror_prefers_s3() {
        let resolved = make_resolved(vec![
            ResolvedMirror {
                url: "https://ncbi.example.com/f".into(),
                service: "ncbi".into(),
            },
            ResolvedMirror {
                url: "https://gs.example.com/f".into(),
                service: "gs".into(),
            },
            ResolvedMirror {
                url: "https://s3.example.com/f".into(),
                service: "s3".into(),
            },
        ]);
        let url = select_mirror(&resolved).unwrap();
        assert_eq!(url, "https://s3.example.com/f");
    }

    #[test]
    fn select_mirror_prefers_gs_over_ncbi() {
        let resolved = make_resolved(vec![
            ResolvedMirror {
                url: "https://ncbi.example.com/f".into(),
                service: "ncbi".into(),
            },
            ResolvedMirror {
                url: "https://gs.example.com/f".into(),
                service: "gs".into(),
            },
        ]);
        let url = select_mirror(&resolved).unwrap();
        assert_eq!(url, "https://gs.example.com/f");
    }

    #[test]
    fn select_mirror_empty_errors() {
        let resolved = make_resolved(vec![]);
        assert!(select_mirror(&resolved).is_err());
    }

    #[test]
    fn select_mirror_s3_direct_equivalent() {
        let resolved = make_resolved(vec![
            ResolvedMirror {
                url: "https://gs.example.com/f".into(),
                service: "gs".into(),
            },
            ResolvedMirror {
                url: "https://s3-direct.example.com/f".into(),
                service: "s3-direct".into(),
            },
        ]);
        let url = select_mirror(&resolved).unwrap();
        assert_eq!(url, "https://s3-direct.example.com/f");
    }

    #[test]
    fn select_mirror_sra_ncbi_over_ncbi() {
        let resolved = make_resolved(vec![
            ResolvedMirror {
                url: "https://ncbi.example.com/f".into(),
                service: "ncbi".into(),
            },
            ResolvedMirror {
                url: "https://sra-ncbi.example.com/f".into(),
                service: "sra-ncbi".into(),
            },
        ]);
        let url = select_mirror(&resolved).unwrap();
        assert_eq!(url, "https://sra-ncbi.example.com/f");
    }

    #[test]
    fn select_mirror_unknown_service_fallback() {
        let resolved = make_resolved(vec![ResolvedMirror {
            url: "https://other.example.com/f".into(),
            service: "unknown-cdn".into(),
        }]);
        let url = select_mirror(&resolved).unwrap();
        assert_eq!(url, "https://other.example.com/f");
    }

    // -----------------------------------------------------------------------
    // is_unsupported_platform
    // -----------------------------------------------------------------------

    #[test]
    fn unsupported_platforms_rejected() {
        for p in &["LS454", "ABI_SOLID", "ION_TORRENT", "HELICOS", "CAPILLARY"] {
            assert!(is_unsupported_platform(p), "{p} should be unsupported");
        }
    }

    #[test]
    fn supported_platforms_allowed() {
        for p in &[
            "ILLUMINA",
            "BGISEQ",
            "DNBSEQ",
            "PACBIO_SMRT",
            "OXFORD_NANOPORE",
            "ELEMENT",
            "ULTIMA",
        ] {
            assert!(!is_unsupported_platform(p), "{p} should be supported");
        }
    }

    // -----------------------------------------------------------------------
    // expand_via_page_map
    // -----------------------------------------------------------------------

    #[test]
    fn expand_no_page_map_returns_input() {
        let data = vec![1, 0, 0, 0, 2, 0, 0, 0]; // two u32s
        let result = expand_via_page_map(data.clone(), &None).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn expand_empty_data_runs_returns_input() {
        let data = vec![1, 0, 0, 0, 2, 0, 0, 0];
        let pm = blob::PageMap {
            data_recs: 2,
            lengths: vec![1],
            leng_runs: vec![2],
            data_runs: vec![],
        };
        let result = expand_via_page_map(data.clone(), &Some(pm)).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn expand_data_runs_direct_offset() {
        // 3 unique u32 values: [10, 20, 30]
        let data = vec![
            10, 0, 0, 0, // entry 0
            20, 0, 0, 0, // entry 1
            30, 0, 0, 0, // entry 2
        ];
        // 4 rows, each referencing an entry by offset index
        let pm = blob::PageMap {
            data_recs: 3,
            lengths: vec![1],
            leng_runs: vec![4],
            data_runs: vec![0, 2, 1, 0], // rows → entries: 0,2,1,0
        };
        let result = expand_via_page_map(data, &Some(pm)).unwrap();
        assert_eq!(
            result,
            vec![
                10, 0, 0, 0, // row 0 → entry 0
                30, 0, 0, 0, // row 1 → entry 2
                20, 0, 0, 0, // row 2 → entry 1
                10, 0, 0, 0, // row 3 → entry 0
            ]
        );
    }

    #[test]
    fn expand_data_runs_direct_offset_rejects_oob() {
        // Only 2 entries in data (8 bytes = 2 u32s), but offset 5 points past.
        let data = vec![10, 0, 0, 0, 20, 0, 0, 0];
        let pm = blob::PageMap {
            data_recs: 2,
            lengths: vec![1],
            leng_runs: vec![3],
            data_runs: vec![0, 5, 1],
        };
        assert!(expand_via_page_map(data, &Some(pm)).is_err());
    }

    // -----------------------------------------------------------------------
    // decode_raw
    // -----------------------------------------------------------------------

    #[test]
    fn decode_raw_rejects_bad_crc32() {
        // Fabricate a minimally-plausible v1 blob (high bit clear) plus an
        // intentionally wrong CRC32 trailer; decode_raw must surface this as
        // an error rather than silently returning bogus data.
        let blob_body: Vec<u8> = vec![0u8; 8];
        let mut raw = blob_body.clone();
        raw.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
        assert!(decode_raw(&raw, 1, 0).is_err());
    }

    #[test]
    fn decode_raw_rejects_bad_md5() {
        let blob_body: Vec<u8> = vec![0u8; 20];
        let mut raw = blob_body.clone();
        raw.extend_from_slice(&[0u8; 16]);
        assert!(decode_raw(&raw, 2, 0).is_err());
    }

    // -----------------------------------------------------------------------
    // validate_blob_ranges
    // -----------------------------------------------------------------------

    fn loc(start_id: i64, id_range: u32) -> crate::vdb::kdb::BlobLoc {
        crate::vdb::kdb::BlobLoc {
            pg: 0,
            size: 0,
            id_range,
            start_id,
        }
    }

    #[test]
    fn validate_blob_ranges_accepts_contiguous() {
        let blobs = vec![loc(1, 10), loc(11, 10), loc(21, 5)];
        assert!(validate_blob_ranges("ACC", &blobs, Some(25)).is_ok());
    }

    #[test]
    fn validate_blob_ranges_accepts_no_expected() {
        let blobs = vec![loc(1, 10), loc(11, 10)];
        assert!(validate_blob_ranges("ACC", &blobs, None).is_ok());
    }

    #[test]
    fn validate_blob_ranges_rejects_gap() {
        // 1..11 then 12..17 leaves row 11 uncovered.
        let blobs = vec![loc(1, 10), loc(12, 5)];
        let err = validate_blob_ranges("ACC", &blobs, None).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("gap"), "unexpected error: {msg}");
    }

    #[test]
    fn validate_blob_ranges_rejects_overlap() {
        let blobs = vec![loc(1, 10), loc(5, 10)];
        let err = validate_blob_ranges("ACC", &blobs, None).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("overlap"), "unexpected error: {msg}");
    }

    #[test]
    fn validate_blob_ranges_rejects_runinfo_mismatch() {
        let blobs = vec![loc(1, 10), loc(11, 10)];
        let err = validate_blob_ranges("ACC", &blobs, Some(100)).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("expects 100"), "unexpected error: {msg}");
    }

    #[test]
    fn validate_blob_ranges_skips_synthetic_single_blob() {
        // id_range == 0 signals "covers all rows" — treat as synthetic.
        let blobs = vec![loc(1, 0)];
        assert!(validate_blob_ranges("ACC", &blobs, None).is_ok());
    }

    // -----------------------------------------------------------------------
    // sracha-stats.jsonl plumbing
    // -----------------------------------------------------------------------

    #[test]
    fn write_stats_file_appends_one_jsonl_line_per_call() {
        let tmp = tempfile::tempdir().unwrap();
        let diag = IntegrityDiag::default();
        diag.quality_overruns
            .fetch_add(3, std::sync::atomic::Ordering::Relaxed);

        write_stats_file(StatsEntry {
            output_dir: tmp.path(),
            accession: "SRR1",
            spots_read: 42,
            reads_written: 84,
            sra_md5: Some("abc123"),
            sra_size: 999,
            output_files: &[],
            diag: &diag,
        })
        .unwrap();

        write_stats_file(StatsEntry {
            output_dir: tmp.path(),
            accession: "SRR2",
            spots_read: 0,
            reads_written: 0,
            sra_md5: None,
            sra_size: 0,
            output_files: &[],
            diag: &IntegrityDiag::default(),
        })
        .unwrap();

        let text = std::fs::read_to_string(stats_path(tmp.path())).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(
            lines.len(),
            2,
            "expected two JSONL lines, got {}",
            lines.len()
        );

        let v0: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(v0["accession"], "SRR1");
        assert_eq!(v0["integrity"]["ok"], false);
        assert_eq!(v0["integrity"]["quality_overruns"], 3);

        let v1: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(v1["accession"], "SRR2");
        assert_eq!(v1["integrity"]["ok"], true);
    }

    // -----------------------------------------------------------------------
    // encode_raw_quality_for_fastq
    //
    // Regression for the +33 offset bug: the physical QUALITY column is
    // always raw Phred, never ASCII-encoded, so every byte must pass
    // through phred_to_ascii. A prior heuristic passed input through
    // verbatim whenever every byte was in [33, 126], which broke files
    // like DRR040728/DRR040407 whose raw quality distribution was
    // entirely above 33.
    // -----------------------------------------------------------------------

    #[test]
    fn encode_quality_applies_phred33_offset_to_high_values() {
        // All bytes >= 33 — would have been pass-through under the old
        // heuristic. Output must be input + 33 (with Q2 floor, but all
        // inputs here are > 2 already).
        let raw = vec![33u8, 40, 50, 60, 80];
        let (encoded, is_empty) = encode_raw_quality_for_fastq(&raw);
        assert!(!is_empty);
        assert_eq!(encoded, vec![33 + 33, 40 + 33, 50 + 33, 60 + 33, 80 + 33]);
    }

    #[test]
    fn encode_quality_applies_phred33_offset_to_low_values() {
        let raw = vec![5u8, 10, 15, 20];
        let (encoded, is_empty) = encode_raw_quality_for_fastq(&raw);
        assert!(!is_empty);
        assert_eq!(encoded, vec![5 + 33, 10 + 33, 15 + 33, 20 + 33]);
    }

    #[test]
    fn encode_quality_preserves_q0_and_q1() {
        // Raw Q0 and Q1 must pass through as `!` and `"` respectively.
        // An earlier Q2 floor was reverted after iter-6 surfaced
        // DRR000918 (srf-load 1.0.0, 2010) which legitimately stores
        // Q0 and fasterq-dump emits `!` — flooring regressed that file
        // on every Q<2 byte.
        let raw = vec![0u8, 1, 2, 3];
        let (encoded, is_empty) = encode_raw_quality_for_fastq(&raw);
        assert!(!is_empty);
        assert_eq!(encoded, vec![b'!', b'"', b'#', b'$']);
    }

    #[test]
    fn encode_quality_empty_input_is_flagged_empty() {
        let (encoded, is_empty) = encode_raw_quality_for_fastq(&[]);
        assert!(is_empty);
        assert!(encoded.is_empty());
    }

    #[test]
    fn encode_quality_all_zero_input_is_flagged_empty() {
        // All-zero blobs are a legitimate "no data" signal used by some
        // producers; caller synthesizes a fallback when empty=true.
        let (encoded, is_empty) = encode_raw_quality_for_fastq(&[0, 0, 0, 0]);
        assert!(is_empty);
        assert!(encoded.is_empty());
    }

    #[test]
    fn encode_quality_never_passes_through_ascii_range_bytes() {
        // This is the explicit guard against re-introducing the
        // all_valid_ascii heuristic: bytes 33..=126 must still go
        // through phred_to_ascii (= +33), not be emitted verbatim.
        // Verify a representative ASCII-range input:
        //   input 33 -> output 66  (not 33)
        //   input 65 -> output 98  (not 65)
        //   input 93 -> output 126 (not 93)
        let raw = vec![33u8, 65, 93];
        let (encoded, _) = encode_raw_quality_for_fastq(&raw);
        assert_ne!(encoded, raw, "verbatim passthrough — heuristic regression");
        assert_eq!(encoded, vec![66, 98, 126]);
    }

    #[test]
    fn iso8601_now_utc_has_expected_shape() {
        let s = iso8601_now_utc();
        // YYYY-MM-DDTHH:MM:SSZ = 20 chars.
        assert_eq!(s.len(), 20, "got {s:?}");
        assert!(s.ends_with('Z'));
        assert_eq!(&s[4..5], "-");
        assert_eq!(&s[7..8], "-");
        assert_eq!(&s[10..11], "T");
        assert_eq!(&s[13..14], ":");
        assert_eq!(&s[16..17], ":");
        // Year should be plausible (>= 2025).
        let year: i64 = s[..4].parse().unwrap();
        assert!(year >= 2025, "year parsed as {year}");
    }
}
