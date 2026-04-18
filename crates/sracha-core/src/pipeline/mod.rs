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

use serde::{Deserialize, Serialize};

use crate::compress::{DEFAULT_BLOCK_SIZE, ParGzWriter};
use crate::download::{DownloadConfig, download_file};
use crate::error::{Error, Result};
use crate::fastq::{
    CompressionMode, FastqConfig, IntegrityDiag, OutputSlot, SplitMode, append_fasta_record,
    append_fastq_record, output_filename,
};

/// Output bytes for one (blob, slot) pair, plus the number of records the
/// buffer holds. Returned from `decode_blob_to_fastq` so the writer can do
/// one `write_all` per slot per blob instead of one per record.
pub(crate) struct BlobSlotOutput {
    pub(crate) slot: OutputSlot,
    pub(crate) bytes: Vec<u8>,
    pub(crate) records: u64,
}
use crate::sdl::ResolvedAccession;
use crate::vdb::blob;
use crate::vdb::cursor::VdbCursor;
use crate::vdb::kar::KarArchive;
use rayon::prelude::*;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the get pipeline.
#[derive(Clone)]
pub struct PipelineConfig {
    /// Directory for output files.
    pub output_dir: PathBuf,
    /// How to split reads across output files.
    pub split_mode: SplitMode,
    /// Output compression mode.
    pub compression: CompressionMode,
    /// Number of threads for decode/compression.
    pub threads: usize,
    /// Number of parallel HTTP connections for downloading.
    pub connections: usize,
    /// Skip technical reads.
    pub skip_technical: bool,
    /// Minimum read length filter.
    pub min_read_len: Option<u32>,
    /// Overwrite existing output files.
    pub force: bool,
    /// Show progress indicators.
    pub progress: bool,
    /// Read structure from NCBI EUtils (authoritative, when available).
    pub run_info: Option<crate::sdl::RunInfo>,
    /// Output FASTA instead of FASTQ (drops quality line).
    pub fasta: bool,
    /// Allow resuming partial downloads.
    pub resume: bool,
    /// Write output to stdout instead of files.
    pub stdout: bool,
    /// Flag for graceful cancellation (e.g. Ctrl-C).
    pub cancelled: Option<Arc<AtomicBool>>,
    /// Strict integrity mode: abort with [`Error::IntegrityFailure`] if any
    /// quality-length / mate-pair / blob-truncation counter is non-zero at
    /// the end of decode, instead of merely reporting the counts.
    pub strict: bool,
}

// ---------------------------------------------------------------------------
// Statistics
// ---------------------------------------------------------------------------

/// Statistics from a completed pipeline run.
pub struct PipelineStats {
    /// The accession that was processed.
    pub accession: String,
    /// Number of spots (rows) read from the SRA file.
    pub spots_read: u64,
    /// Number of FASTQ reads written (after filtering).
    pub reads_written: u64,
    /// Bytes actually transferred over the network this session.
    pub bytes_transferred: u64,
    /// Total size of the full SRA file on the server.
    pub total_sra_size: u64,
    /// Paths of all output files created.
    pub output_files: Vec<PathBuf>,
    /// Data-integrity counters captured during decode. Inspect these (or
    /// run with `--strict`) to detect silent corruption.
    pub integrity: Arc<IntegrityDiag>,
}

// ---------------------------------------------------------------------------
// Completion marker — skip re-decode when output files already exist.
// ---------------------------------------------------------------------------

/// Marker format version. Bump this when the marker schema changes to
/// invalidate all existing markers (forcing a clean re-decode).
const MARKER_VERSION: u32 = 1;

/// Completion marker written after a successful decode.
///
/// Stored as `.sracha-done-{accession}` in the output directory. On re-run,
/// the marker is loaded and validated: if all recorded output files still
/// exist at the expected sizes and the decode parameters match, the decode
/// (and the download) are skipped entirely.
#[derive(Debug, Serialize, Deserialize)]
struct CompletionMarker {
    version: u32,
    accession: String,
    sra_md5: Option<String>,
    sra_size: u64,
    split_mode: String,
    compression: String,
    fasta: bool,
    skip_technical: bool,
    min_read_len: Option<u32>,
    output_files: Vec<(String, u64)>,
}

fn marker_path(output_dir: &Path, accession: &str) -> PathBuf {
    output_dir.join(format!(".sracha-done-{accession}"))
}

/// Serialise `CompressionMode` to a stable string for the marker.
fn compression_key(c: &crate::fastq::CompressionMode) -> String {
    match c {
        crate::fastq::CompressionMode::None => "none".into(),
        crate::fastq::CompressionMode::Gzip { level } => format!("gzip:{level}"),
        crate::fastq::CompressionMode::Zstd { level, threads } => {
            format!("zstd:{level}:{threads}")
        }
    }
}

/// Path of the shared JSONL stats log (one line per accession).
///
/// Using a single append-only file scales to BioProject-sized runs: 200
/// accessions produce 200 lines, not 200 files. Users can `jq '.integrity
/// | select(.ok == false)'` to pull just the failures.
fn stats_path(output_dir: &Path) -> PathBuf {
    output_dir.join("sracha-stats.jsonl")
}

/// Inputs for a single `sracha-stats.jsonl` line.
struct StatsEntry<'a> {
    output_dir: &'a Path,
    accession: &'a str,
    spots_read: u64,
    reads_written: u64,
    sra_md5: Option<&'a str>,
    sra_size: u64,
    output_files: &'a [PathBuf],
    diag: &'a IntegrityDiag,
}

/// Append an integrity summary line to the shared stats JSONL. We record
/// every accession — passing and failing — so the file doubles as an audit
/// log that a run happened and which inputs produced which outputs.
fn write_stats_file(entry: StatsEntry<'_>) -> Result<()> {
    let StatsEntry {
        output_dir,
        accession,
        spots_read,
        reads_written,
        sra_md5,
        sra_size,
        output_files,
        diag,
    } = entry;
    use std::io::Write as _;
    use std::sync::atomic::Ordering;
    let files: Vec<serde_json::Value> = output_files
        .iter()
        .filter_map(|p| {
            let name = p.file_name()?.to_str()?.to_string();
            let size = std::fs::metadata(p).ok()?.len();
            Some(serde_json::json!({ "name": name, "bytes": size }))
        })
        .collect();
    let payload = serde_json::json!({
        "timestamp": iso8601_now_utc(),
        "accession": accession,
        "spots_read": spots_read,
        "reads_written": reads_written,
        "sra_md5": sra_md5,
        "sra_size": sra_size,
        "output_files": files,
        "integrity": {
            "ok": !diag.any(),
            "quality_length_mismatches": diag.quality_length_mismatches.load(Ordering::Relaxed),
            "quality_invalid_bytes": diag.quality_invalid_bytes.load(Ordering::Relaxed),
            "quality_overruns": diag.quality_overruns.load(Ordering::Relaxed),
            "all_zero_quality_blobs": diag.all_zero_quality_blobs.load(Ordering::Relaxed),
            "paired_spot_violations": diag.paired_spot_violations.load(Ordering::Relaxed),
            "truncated_spots": diag.truncated_spots.load(Ordering::Relaxed),
        },
    });
    let line = serde_json::to_string(&payload).map_err(|e| Error::Io(std::io::Error::other(e)))?;
    let path = stats_path(output_dir);
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)?;
    writeln!(file, "{line}")?;
    Ok(())
}

/// RFC3339 UTC timestamp for audit-log entries. Avoids pulling a dedicated
/// date-time crate just for this one line.
fn iso8601_now_utc() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = dur.as_secs() as i64;
    let days = secs.div_euclid(86_400);
    let time_of_day = secs.rem_euclid(86_400);
    let mut y = 1970i64;
    let mut d = days;
    loop {
        let leap = (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
        let yd = if leap { 366 } else { 365 };
        if d < yd {
            break;
        }
        d -= yd;
        y += 1;
    }
    let leap = (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
    let mdays: [i64; 12] = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let mut m = 0usize;
    while m < 12 && d >= mdays[m] {
        d -= mdays[m];
        m += 1;
    }
    let day = d + 1;
    let hour = time_of_day / 3600;
    let minute = (time_of_day % 3600) / 60;
    let second = time_of_day % 60;
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        y,
        m + 1,
        day,
        hour,
        minute,
        second,
    )
}

fn write_completion_marker(
    output_dir: &Path,
    accession: &str,
    sra_md5: Option<&str>,
    sra_size: u64,
    config: &PipelineConfig,
    output_files: &[PathBuf],
) -> Result<()> {
    let file_entries: Vec<(String, u64)> = output_files
        .iter()
        .filter_map(|p| {
            let name = p.file_name()?.to_str()?.to_string();
            let size = std::fs::metadata(p).ok()?.len();
            Some((name, size))
        })
        .collect();

    let marker = CompletionMarker {
        version: MARKER_VERSION,
        accession: accession.to_string(),
        sra_md5: sra_md5.map(String::from),
        sra_size,
        split_mode: config.split_mode.to_string(),
        compression: compression_key(&config.compression),
        fasta: config.fasta,
        skip_technical: config.skip_technical,
        min_read_len: config.min_read_len,
        output_files: file_entries,
    };

    let path = marker_path(output_dir, accession);
    let json =
        serde_json::to_string_pretty(&marker).map_err(|e| Error::Io(std::io::Error::other(e)))?;
    std::fs::write(&path, json)?;
    Ok(())
}

/// Check if decode can be skipped for this accession.
///
/// Returns `Some(output_files)` if the completion marker is valid and all
/// output files exist at the recorded sizes. Returns `None` otherwise.
fn check_completion_marker(
    output_dir: &Path,
    accession: &str,
    config: &PipelineConfig,
    sra_size: u64,
) -> Option<Vec<PathBuf>> {
    let path = marker_path(output_dir, accession);
    let content = std::fs::read_to_string(&path).ok()?;
    let marker: CompletionMarker = serde_json::from_str(&content).ok()?;

    if marker.version != MARKER_VERSION {
        return None;
    }
    if marker.accession != accession {
        return None;
    }
    if marker.sra_size != sra_size {
        return None;
    }
    if marker.split_mode != config.split_mode.to_string() {
        return None;
    }
    if marker.compression != compression_key(&config.compression) {
        return None;
    }
    if marker.fasta != config.fasta {
        return None;
    }
    if marker.skip_technical != config.skip_technical {
        return None;
    }
    if marker.min_read_len != config.min_read_len {
        return None;
    }

    // Verify all output files exist at recorded sizes.
    let mut output_paths = Vec::new();
    for (name, expected_size) in &marker.output_files {
        let file_path = output_dir.join(name);
        let meta = std::fs::metadata(&file_path).ok()?;
        if meta.len() != *expected_size {
            return None;
        }
        output_paths.push(file_path);
    }

    // Temp SRA file must be absent (confirms prior decode completed).
    let temp_filename = format!(".sracha-tmp-{accession}.sra");
    let temp_path = output_dir.join(temp_filename);
    if temp_path.exists() {
        return None;
    }

    Some(output_paths)
}

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

/// Map a raw `Error::BlobIntegrity` from `decode_blob` into the user-facing
/// [`Error::IntegrityFailure`], attaching the accession and the shared
/// [`crate::error::BLOB_INTEGRITY_GUIDANCE`] text. Passes other errors through
/// unchanged.
fn wrap_blob_integrity(accession: &str, err: Error) -> Error {
    match err {
        Error::BlobIntegrity {
            kind,
            stored,
            computed,
        } => Error::IntegrityFailure {
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

// ---------------------------------------------------------------------------
// Phase 2: Parse VDB + output FASTQ
// ---------------------------------------------------------------------------

/// Decode a raw blob, validating the trailing CRC32/MD5 checksum and
/// stripping envelope/headers/page_map.
///
/// The blob locator `size` field includes trailing checksum bytes, which
/// [`blob::decode_blob`] checks against the on-disk data before returning.
/// A mismatch indicates a corrupt download or truncated blob and surfaces as
/// [`Error::Vdb`] so callers can abort rather than produce wrong reads.
fn decode_raw<'a>(
    raw: &'a [u8],
    checksum_type: u8,
    row_count: u64,
) -> Result<blob::DecodedBlob<'a>> {
    blob::decode_blob(raw, checksum_type, row_count, 8)
}

/// Decode irzip-compressed integers from a blob, detecting single vs dual series.
fn decode_irzip_column(decoded: &blob::DecodedBlob<'_>) -> Result<Vec<u8>> {
    let hdr_version = decoded.headers.first().map(|h| h.version).unwrap_or(0);
    let decoded_ints = if hdr_version >= 1 {
        let hdr = &decoded.headers[0];
        let planes = hdr.ops.first().copied().unwrap_or(0xFF);
        let min = hdr.args.first().copied().unwrap_or(0);
        let slope = hdr.args.get(1).copied().unwrap_or(0);
        let num_elems = (hdr.osize as u32) / 4;
        // Detect dual-series (irzip v3): 4 args = min[0], slope[0], min[1], slope[1]
        let series2 = hdr
            .args
            .get(2)
            .and_then(|&min2| hdr.args.get(3).map(|&slope2| (min2, slope2)));
        blob::irzip_decode(&decoded.data, 32, num_elems, min, slope, planes, series2)?
    } else {
        let num_elems = decoded
            .row_length
            .unwrap_or_else(|| (decoded.data.len() as u64 * 8) / 32) as u32;
        blob::izip_decode(&decoded.data, 32, num_elems)?
    };
    expand_via_page_map(decoded_ints, &decoded.page_map)
}

/// Expand decoded integer data via a page map's data_runs, if present.
/// For columns like X, Y, and READ_LEN, the irzip/izip decoder produces
/// unique data entries, and the page map maps each row to its data entry.
fn expand_via_page_map(decoded_ints: Vec<u8>, page_map: &Option<blob::PageMap>) -> Result<Vec<u8>> {
    let Some(pm) = page_map else {
        return Ok(decoded_ints);
    };
    let elem_bytes = 4usize; // u32
    let row_length = pm.lengths.first().copied().unwrap_or(1) as usize;
    let entry_bytes = row_length * elem_bytes;

    if !pm.data_runs.is_empty() && pm.data_runs.len() as u64 >= pm.total_rows() {
        // Random-access variant: data_runs is repurposed as offsets into
        // the unique decoded entries. A missing/oversized offset means the
        // page map disagrees with the decoded column — refuse to silently
        // drop those rows.
        let mut expanded = Vec::with_capacity(pm.data_runs.len() * entry_bytes);
        for &offset in &pm.data_runs {
            let start = offset as usize * entry_bytes;
            let end = start + entry_bytes;
            if end > decoded_ints.len() {
                return Err(Error::Vdb(format!(
                    "page_map: offset {offset} × {entry_bytes} out of {} decoded bytes",
                    decoded_ints.len(),
                )));
            }
            expanded.extend_from_slice(&decoded_ints[start..end]);
        }
        Ok(expanded)
    } else if !pm.data_runs.is_empty() {
        pm.expand_data_runs_bytes(&decoded_ints, elem_bytes)
    } else {
        Ok(decoded_ints)
    }
}

/// Decode a zip_encoding data section: the blob header tells us the
/// version. Version 1 = raw deflate, byte-aligned output. Version 2 =
/// raw deflate with trailing-bits argument. No headers (v1 blob) = the
/// data is already the raw-deflate stream or uncompressed.
///
/// When a compression header is present (hdr_version >= 1), both deflate and
/// zlib failing is treated as an error — silently returning the still-
/// compressed bytes would produce corrupt downstream output. For v0 blobs
/// (no headers), the raw-bytes fallback remains, since those are often
/// already-uncompressed payloads.
fn decode_zip_encoding(decoded: &blob::DecodedBlob<'_>) -> Result<Vec<u8>> {
    let hdr_version = decoded.headers.first().map(|h| h.version).unwrap_or(0);

    if decoded.data.is_empty() {
        return Ok(Vec::new());
    }

    let osize = decoded
        .headers
        .first()
        .map(|h| h.osize as usize)
        .filter(|&s| s > 0);
    // Estimate output size from header osize, or 4x input as heuristic.
    let estimated = osize.unwrap_or(decoded.data.len() * 4);

    // Try raw deflate via libdeflate.
    if let Ok(mut out) = blob::deflate_decompress(&decoded.data, estimated)
        && !out.is_empty()
    {
        // Version 2: trim trailing bits if specified in header args.
        if hdr_version == 2
            && let Some(trailing_bits) = decoded.headers.first().and_then(|h| h.args.first())
        {
            let total_bits = out.len() as i64 * 8;
            let actual_bits = total_bits - (8 - trailing_bits);
            let actual_bytes = ((actual_bits + 7) / 8) as usize;
            out.truncate(actual_bytes);
        }
        if let Some(expected) = osize
            && out.len() != expected
        {
            tracing::debug!(
                "zip_encoding: decompressed {} bytes, header osize={}",
                out.len(),
                expected,
            );
        }
        return Ok(out);
    }

    // Fallback: try zlib (with header).
    if let Ok(out2) = blob::zlib_decompress(&decoded.data, estimated)
        && !out2.is_empty()
    {
        return Ok(out2);
    }

    if hdr_version >= 1 {
        return Err(Error::Vdb(format!(
            "zip_encoding v{hdr_version}: both deflate and zlib failed on {}-byte payload",
            decoded.data.len(),
        )));
    }

    // v0 (no transform header): payload is often raw bytes already.
    Ok(decoded.data.to_vec())
}

// ---------------------------------------------------------------------------
// Batch-parallel blob decode types and helpers
// ---------------------------------------------------------------------------

/// Raw bytes for a single blob across all columns.
/// Holds borrowed slices into the mmap (zero-copy) so this is Send
/// as long as the mmap outlives the parallel closure.
struct RawBlobData<'a> {
    /// READ column raw bytes.
    read_raw: &'a [u8],
    /// Row count (id_range) for the READ blob.
    read_id_range: u64,
    /// QUALITY column raw bytes (empty if column absent or blob out of range).
    quality_raw: &'a [u8],
    /// Row count for the QUALITY blob (0 if absent).
    quality_id_range: u64,
    /// Checksum type for the QUALITY column.
    quality_cs: u8,
    /// READ_LEN column raw bytes (empty if column absent).
    read_len_raw: &'a [u8],
    /// Row count for the READ_LEN blob (0 if absent).
    read_len_id_range: u64,
    /// Checksum type for the READ_LEN column.
    read_len_cs: u8,
    /// NAME column raw bytes (empty if column absent).
    name_raw: &'a [u8],
    /// Row count for the NAME blob (0 if absent).
    name_id_range: u64,
    /// Checksum type for the NAME column.
    name_cs: u8,
    /// READ_TYPE column raw bytes (empty if column absent).
    read_type_raw: &'a [u8],
    /// Row count for the READ_TYPE blob (0 if absent).
    read_type_id_range: u64,
    /// Checksum type for the READ_TYPE column.
    read_type_cs: u8,
    /// Whether there is a READ_LEN column at all.
    has_read_len: bool,
    /// Whether there is a NAME column at all.
    has_name: bool,
    /// Whether there is a READ_TYPE column at all.
    has_read_type: bool,
    /// ALTREAD column raw bytes (4na ambiguity mask; also used for name reconstruction detection).
    altread_raw: &'a [u8],
    /// Row count for the ALTREAD blob (0 if absent).
    altread_id_range: u64,
    /// Checksum type for the ALTREAD column.
    altread_cs: u8,
    /// Whether the ALTREAD column exists (independent of Illumina name parts).
    has_altread: bool,
    /// X column raw bytes.
    x_raw: &'a [u8],
    x_id_range: u64,
    x_cs: u8,
    /// Y column raw bytes.
    y_raw: &'a [u8],
    y_id_range: u64,
    y_cs: u8,
    /// Whether Illumina name parts (ALTREAD + X + Y) are available.
    has_illumina_name_parts: bool,
    /// Shared name format templates (from skey index), sorted by spot_start.
    name_templates: &'a [Vec<u8>],
    /// Starting spot_id for each name template (parallel to name_templates).
    name_spot_starts: &'a [i64],
    /// Reads-per-spot from table metadata (fallback when READ_LEN absent).
    metadata_reads_per_spot: Option<usize>,
    /// Fixed spot length in bases (from READ column page_size).
    fixed_spot_len: Option<u32>,
    /// Per-read lengths from NCBI EUtils API or VDB metadata (used as
    /// fallback when READ_LEN column is absent). Borrowed from the outer
    /// `decode_and_write` scope so we don't clone a Vec per blob.
    fallback_read_lengths: Option<&'a [u32]>,
}

/// Decode a single blob and produce FASTQ records directly.
///
/// This fused function replaces the former two-step decode_blob_to_spots +
/// format_spot, eliminating intermediate `SpotRecord` allocations. It operates
/// only on borrowed data (Send-safe for rayon).
///
/// Returns `(records, num_spots)`.
/// Per-call immutable context shared across every blob decode of a run.
/// Replaces the prior 8-argument `decode_blob_to_fastq` signature with a
/// single `ctx` reference plus per-blob position.
struct BlobDecodeCtx<'a> {
    run_name: &'a str,
    config: &'a FastqConfig,
    diag: &'a IntegrityDiag,
    is_lite: bool,
    read_cs: u8,
}

fn decode_blob_to_fastq(
    raw: &RawBlobData<'_>,
    ctx: &BlobDecodeCtx<'_>,
    blob_idx: usize,
    spots_before: u64,
) -> Result<(Vec<BlobSlotOutput>, u64)> {
    let BlobDecodeCtx {
        run_name,
        config,
        diag,
        is_lite,
        read_cs,
    } = *ctx;
    // ------------------------------------------------------------------
    // Decode READ blob -> 2na -> ASCII bases.
    // ------------------------------------------------------------------
    let read_decoded = decode_raw(raw.read_raw, read_cs, raw.read_id_range)?;
    let total_bits = read_decoded.data.len() * 8;
    let adjust = read_decoded.adjust as usize;
    let actual_bases = (total_bits.saturating_sub(adjust)) / 2;
    let mut read_data = crate::vdb::encoding::unpack_2na(&read_decoded.data, actual_bases);
    let read_page_map = read_decoded.page_map;

    // V2 blobs may deduplicate identical rows via the page map's
    // `data_runs` — e.g., two spots with identical base calls get
    // written once and replicated on read. For fixed-row-length
    // columns we replicate the 2na-decoded ASCII bytes accordingly so
    // downstream slicing sees the full logical row count; without this
    // trailing duplicate rows would drop out at blob boundaries and
    // every subsequent spot would drift.
    if let Some(ref pm) = read_page_map
        && !pm.data_runs.is_empty()
        && !pm.lengths.is_empty()
        && pm.lengths.iter().all(|&l| l == pm.lengths[0])
        && pm.lengths[0] > 0
    {
        let row_bytes = pm.lengths[0] as usize;
        if read_data.len().is_multiple_of(row_bytes) {
            match pm.expand_data_runs_bytes(&read_data, row_bytes) {
                Ok(expanded) => read_data = expanded,
                Err(e) => {
                    tracing::debug!("blob {blob_idx}: READ expand_data_runs_bytes skipped: {e}");
                }
            }
        }
    }
    let actual_bases = read_data.len();

    // ------------------------------------------------------------------
    // Decode QUALITY blob (skipped entirely in FASTA mode).
    // ------------------------------------------------------------------
    let (quality_all, quality_is_empty): (Vec<u8>, bool) = if config.fasta {
        (Vec::new(), true)
    } else {
        let quality_data: Vec<u8> = if !raw.quality_raw.is_empty() {
            let qdecoded = decode_raw(raw.quality_raw, raw.quality_cs, raw.quality_id_range)?;
            let qpage_map = qdecoded.page_map.clone();
            let mut qdata = decode_zip_encoding(&qdecoded)?;
            // Expand quality data via page map data_runs if present.
            // Some blobs (e.g., PacBio) store repeated rows once with
            // data_runs > 1; the decompressed data only contains unique
            // rows and must be expanded to match the total base count.
            if let Some(ref pm) = qpage_map
                && !pm.data_runs.is_empty()
            {
                qdata = pm.expand_variable_data_runs(&qdata)?;
            }
            qdata
        } else {
            Vec::new()
        };

        let is_empty = quality_data.is_empty() || quality_data.iter().all(|&b| b == 0);
        if is_empty && !quality_data.is_empty() {
            diag.all_zero_quality_blobs
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tracing::warn!(
                "quality blob {} is all-zero ({} bytes) — synthesizing Phred fallback",
                blob_idx,
                quality_data.len(),
            );
        }
        let all: Vec<u8> = if !is_empty {
            // Check ALL bytes to decide if data is already Phred+33 ASCII.
            let all_valid_ascii = quality_data.len() == read_data.len()
                && quality_data.iter().all(|&b| (33..=126).contains(&b));
            if all_valid_ascii {
                quality_data
            } else {
                crate::vdb::encoding::phred_to_ascii(&quality_data)
            }
        } else {
            Vec::new()
        };

        (all, is_empty)
    };

    // ------------------------------------------------------------------
    // N-masking: ALTREAD ambiguity merge (preferred) or quality fallback.
    //
    // When ALTREAD + X + Y columns are all present (Illumina), ALTREAD
    // stores ASCII name templates, not 4na ambiguity data — skip merge.
    // When ALTREAD is present WITHOUT Illumina name parts, it contains
    // the 4na ambiguity mask used by the VDB schema's bit_or operation.
    // ------------------------------------------------------------------
    if raw.has_altread && !raw.has_illumina_name_parts && !raw.altread_raw.is_empty() {
        let alt_decoded = decode_raw(raw.altread_raw, raw.altread_cs, raw.altread_id_range)?;
        let alt_page_map = alt_decoded.page_map.clone();
        let altread_data = decode_zip_encoding(&alt_decoded)?;
        // Physical ALTREAD is `<INSDC:4na:bin>zip_encoding#1 .ALTREAD =
        // trim<0,0>(…)` — one byte per base in the low nibble, stored
        // as variable-length rows with leading zeros stripped. Pad each
        // row back to the full per-row length (right-aligning the
        // stored bytes because `trim<0, 0>` trims from the left) using
        // the page_map, then merge byte-per-base.
        let row_bases = actual_bases / raw.read_id_range.max(1) as usize;
        if let Some(pm) = alt_page_map.as_ref()
            && row_bases > 0
            && actual_bases == row_bases * raw.read_id_range as usize
            && let Ok(padded) = pm.pad_trimmed_rows_fixed(
                &altread_data,
                row_bases,
                crate::vdb::blob::TrimSide::Leading,
            )
        {
            crate::vdb::encoding::merge_altread_bin(&mut read_data, &padded, actual_bases);
        } else {
            // Fallback for variable-length rows or missing page_map:
            // best-effort byte-per-base merge over the flat payload.
            crate::vdb::encoding::merge_altread_bin(&mut read_data, &altread_data, actual_bases);
        }
    } else if !quality_is_empty && quality_all.len() == read_data.len() {
        // Fallback: quality-based N-masking (Phred <= 2 → N).
        const NOCALL_QUAL_BYTE: u8 = 35; // Phred 2 + 33 offset = ASCII '#'
        for (base, &qual) in read_data.iter_mut().zip(quality_all.iter()) {
            if qual <= NOCALL_QUAL_BYTE {
                *base = b'N';
            }
        }
    }

    // Quality fallback buffer for SRA-lite / empty quality.  Only allocated
    // when quality is actually missing; the quality-overrun fallback path
    // allocates on demand (rare).
    let lite_qual_char = if is_lite {
        crate::vdb::encoding::SRA_LITE_REJECT_QUAL + crate::vdb::encoding::QUAL_PHRED_OFFSET
    } else {
        crate::vdb::encoding::SRA_LITE_PASS_QUAL + crate::vdb::encoding::QUAL_PHRED_OFFSET
    };
    let mut lite_qual_buf: Option<Vec<u8>> = if quality_is_empty {
        Some(vec![lite_qual_char; read_data.len()])
    } else {
        None
    };

    // ------------------------------------------------------------------
    // Decode READ_LEN blob -> irzip/izip -> u32 lengths.
    // ------------------------------------------------------------------
    let (read_lengths, reads_per_spot): (Vec<u32>, usize) = if raw.has_read_len
        && !raw.read_len_raw.is_empty()
    {
        let rldecoded = decode_raw(raw.read_len_raw, raw.read_len_cs, raw.read_len_id_range)?;

        let rps = rldecoded
            .page_map
            .as_ref()
            .and_then(|pm| pm.lengths.first().copied())
            .unwrap_or(1) as usize;

        let rl_bytes = decode_irzip_column(&rldecoded)?;

        let lengths: Vec<u32> = rl_bytes
            .chunks_exact(4)
            .map(|chunk| u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
            .collect();

        if blob_idx == 0 {
            tracing::debug!(
                "READ_LEN: {} values, reads_per_spot={}, first_10={:?}",
                lengths.len(),
                rps,
                &lengths[..lengths.len().min(10)],
            );
        }

        (lengths, rps)
    } else if !raw.has_read_len {
        // No READ_LEN column. Use fallback read structure from API or
        // VDB metadata.
        if let Some(ref api_lens) = raw.fallback_read_lengths {
            // Fallback: we know the exact per-read lengths.
            let rps = api_lens.len();
            let spot_len: u32 = api_lens.iter().sum();
            let total_bases = read_data.len() as u32;
            let n_spots = total_bases / spot_len.max(1);

            if blob_idx == 0 {
                tracing::debug!(
                    "using fallback read lengths: {:?}, spot_len={}, n_spots={}",
                    api_lens,
                    spot_len,
                    n_spots,
                );
            }

            let mut row_lengths = Vec::with_capacity(n_spots as usize * rps);
            for _ in 0..n_spots {
                row_lengths.extend_from_slice(api_lens);
            }
            // Handle remainder (partial last spot).
            let used = n_spots * spot_len;
            if used < total_bases {
                let remainder = total_bases - used;
                row_lengths.push(remainder);
            }
            (row_lengths, rps)
        } else {
            // Fallback: use page map / metadata heuristics.
            let meta_rps = raw.metadata_reads_per_spot.unwrap_or(1);
            if let Some(ref pm) = read_page_map {
                if blob_idx == 0 {
                    tracing::debug!(
                        "READ page_map (no READ_LEN): lengths={:?}, leng_runs={:?}, data_recs={}",
                        &pm.lengths[..pm.lengths.len().min(10)],
                        &pm.leng_runs[..pm.leng_runs.len().min(10)],
                        pm.data_recs,
                    );
                }
                let mut row_lengths = Vec::new();
                // Determine the expected per-spot length from the most
                // common (smallest reasonable) page-map entry.
                let typical_spot_len = pm
                    .lengths
                    .iter()
                    .copied()
                    .filter(|&l| l > 0 && l <= 100_000)
                    .min()
                    .unwrap_or(0);

                for (len, run) in pm.lengths.iter().zip(pm.leng_runs.iter()) {
                    for _ in 0..*run {
                        if *len <= 100_000 || typical_spot_len == 0 {
                            // Normal per-spot entry: split into reads.
                            let spot_len = *len;
                            if meta_rps > 1 && spot_len > 0 {
                                let per_read = spot_len / meta_rps as u32;
                                for r in 0..meta_rps as u32 {
                                    if r < meta_rps as u32 - 1 {
                                        row_lengths.push(per_read);
                                    } else {
                                        row_lengths.push(spot_len - per_read * r);
                                    }
                                }
                            } else {
                                row_lengths.push(spot_len);
                            }
                        } else {
                            // Blob-aggregate entry: split into individual
                            // spots using the typical spot length.
                            let n_spots = *len / typical_spot_len;
                            let remainder = *len - n_spots * typical_spot_len;
                            for s in 0..n_spots {
                                let spot_len = if s == n_spots - 1 {
                                    typical_spot_len + remainder
                                } else {
                                    typical_spot_len
                                };
                                if meta_rps > 1 {
                                    let per_read = spot_len / meta_rps as u32;
                                    for r in 0..meta_rps as u32 {
                                        if r < meta_rps as u32 - 1 {
                                            row_lengths.push(per_read);
                                        } else {
                                            row_lengths.push(spot_len - per_read * r);
                                        }
                                    }
                                } else {
                                    row_lengths.push(spot_len);
                                }
                            }
                        }
                    }
                }
                (row_lengths, meta_rps)
            } else if let Some(spot_len) = raw.fixed_spot_len.filter(|_| meta_rps > 1) {
                // No page map but we know the fixed spot length from the
                // READ column metadata. Split blob into uniform spots.
                let total = read_data.len() as u32;
                let n_spots = total / spot_len.max(1);
                let mut row_lengths = Vec::with_capacity(n_spots as usize * meta_rps);
                for s in 0..n_spots {
                    let sl = if s < n_spots - 1 {
                        spot_len
                    } else {
                        total - spot_len * s
                    };
                    let per_read = sl / meta_rps as u32;
                    for r in 0..meta_rps as u32 {
                        if r < meta_rps as u32 - 1 {
                            row_lengths.push(per_read);
                        } else {
                            row_lengths.push(sl - per_read * r);
                        }
                    }
                }
                (row_lengths, meta_rps)
            } else {
                (vec![read_data.len() as u32], 1)
            }
        }
    } else {
        // has_read_len but raw is empty (blob out of range).
        (vec![read_data.len() as u32], 1)
    };

    // ------------------------------------------------------------------
    // Decode NAME blob.
    // ------------------------------------------------------------------
    let spot_names: Option<Vec<Vec<u8>>> = if raw.has_name && !raw.name_raw.is_empty() {
        let ndecoded = decode_raw(raw.name_raw, raw.name_cs, raw.name_id_range)?;
        let name_bytes = decode_zip_encoding(&ndecoded)?;

        let num_spots = read_lengths
            .len()
            .checked_div(reads_per_spot)
            .unwrap_or(read_lengths.len());

        if let Some(ref pm) = ndecoded.page_map {
            let mut names = Vec::with_capacity(num_spots);
            let mut offset = 0usize;
            for (len, run) in pm.lengths.iter().zip(pm.leng_runs.iter()) {
                let name_len = *len as usize;
                for _ in 0..*run {
                    if offset + name_len <= name_bytes.len() {
                        names.push(name_bytes[offset..offset + name_len].to_vec());
                        offset += name_len;
                    }
                }
            }
            if blob_idx == 0 {
                tracing::debug!(
                    "NAME: {} names decoded, first={:?}",
                    names.len(),
                    names
                        .first()
                        .map(|n| String::from_utf8_lossy(n).to_string()),
                );
            }
            Some(names)
        } else if let Some(row_len) = ndecoded.row_length {
            let rl = row_len as usize;
            if rl > 0 {
                let names: Vec<Vec<u8>> = name_bytes.chunks(rl).map(|c| c.to_vec()).collect();
                Some(names)
            } else {
                None
            }
        } else {
            let delimiter = if name_bytes.contains(&0) { 0u8 } else { b'\n' };
            let names: Vec<Vec<u8>> = name_bytes
                .split(|&b| b == delimiter)
                .filter(|s| !s.is_empty())
                .map(|s| s.to_vec())
                .collect();
            if !names.is_empty() { Some(names) } else { None }
        }
    } else {
        None
    };

    // ------------------------------------------------------------------
    // Illumina name reconstruction from ALTREAD + X + Y columns.
    // If the NAME column is absent but ALTREAD/X/Y are present, reconstruct
    // the original Illumina read name by substituting $X and $Y placeholders
    // in the ALTREAD template string with per-spot X/Y coordinates.
    // ------------------------------------------------------------------
    let spot_names: Option<Vec<Vec<u8>>> = if spot_names.is_none()
        && raw.has_illumina_name_parts
        && !raw.altread_raw.is_empty()
        && !raw.x_raw.is_empty()
        && !raw.y_raw.is_empty()
    {
        // Decode ALTREAD blob → name format template(s).
        // ALTREAD stores ASCII name templates — use raw decoded data directly
        // (not zip_encoding, as these are plain text strings).
        // Use name templates preloaded from the skey index.

        // Decode X column → u32 coordinates (irzip/izip encoded integers).
        let x_decoded = decode_raw(raw.x_raw, raw.x_cs, raw.x_id_range)?;
        let x_bytes = decode_irzip_column(&x_decoded)?;

        // Decode Y column → u32 coordinates (irzip/izip encoded integers).
        let y_decoded = decode_raw(raw.y_raw, raw.y_cs, raw.y_id_range)?;
        let y_bytes = decode_irzip_column(&y_decoded)?;

        // ALTREAD is a per-spot ASCII template (may vary per tile).
        // Parse templates: each spot's template is stored as a fixed-length or
        // page-map-delineated string.
        let num_spots = read_lengths
            .len()
            .checked_div(reads_per_spot)
            .unwrap_or(read_lengths.len());

        // The skey index maps spot ranges to name templates. Each blob covers
        // a contiguous spot range corresponding to one tile. Use the ALTREAD
        // blob index to determine which template this blob uses.
        // For now, use blob_idx to index into the templates list. If the
        // blob count exceeds the template count, wrap around.
        let all_templates: &[Vec<u8>] = raw.name_templates;

        // X/Y values are already decoded as u32 by irzip/izip (stored as
        // little-endian 4-byte groups in the output Vec<u8>).
        let x_vals: Vec<u32> = x_bytes
            .chunks_exact(4)
            .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
            .collect();

        let y_vals: Vec<u32> = y_bytes
            .chunks_exact(4)
            .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
            .collect();

        // Pick the template per spot using the skey spot_start mapping.
        // A single blob can span multiple tile ranges, so we look up the
        // correct template for each spot_id via binary search.
        let has_templates = !all_templates.is_empty() && !raw.name_spot_starts.is_empty();

        if blob_idx == 0 && has_templates {
            tracing::debug!(
                "name reconstruction: {} templates, x_vals={}, y_vals={}",
                all_templates.len(),
                x_vals.len(),
                y_vals.len(),
            );
        }

        if has_templates && !x_vals.is_empty() && !y_vals.is_empty() {
            let mut names = Vec::with_capacity(num_spots);
            let mut itoa_x = itoa::Buffer::new();
            let mut itoa_y = itoa::Buffer::new();
            for spot_i in 0..num_spots {
                let spot_id = spots_before as i64 + spot_i as i64 + 1;
                let tmpl_idx = match raw.name_spot_starts.binary_search(&spot_id) {
                    Ok(i) => i,
                    Err(i) => i.saturating_sub(1),
                };
                let tmpl = &all_templates[tmpl_idx.min(all_templates.len() - 1)];
                let x = x_vals.get(spot_i).copied().unwrap_or(0);
                let y = y_vals.get(spot_i).copied().unwrap_or(0);

                // Substitute $X and $Y in the template.
                let x_str = itoa_x.format(x);
                let y_str = itoa_y.format(y);
                let mut name = Vec::with_capacity(tmpl.len() + 10);
                let mut ti = 0;
                while ti < tmpl.len() {
                    if tmpl[ti] == b'$' && ti + 1 < tmpl.len() {
                        if tmpl[ti + 1] == b'X' {
                            name.extend_from_slice(x_str.as_bytes());
                            ti += 2;
                            continue;
                        } else if tmpl[ti + 1] == b'Y' {
                            name.extend_from_slice(y_str.as_bytes());
                            ti += 2;
                            continue;
                        }
                    }
                    name.push(tmpl[ti]);
                    ti += 1;
                }
                names.push(name);
            }

            if blob_idx == 0 && !names.is_empty() {
                tracing::debug!(
                    "Illumina name reconstructed: first={:?}",
                    String::from_utf8_lossy(&names[0]),
                );
            }
            Some(names)
        } else {
            None
        }
    } else {
        spot_names
    };

    // ------------------------------------------------------------------
    // Decode READ_TYPE blob -> byte array of per-read type codes.
    // 0 = biological, 1 = technical (SRA_READ_TYPE values).
    // ------------------------------------------------------------------
    let read_type_data: Vec<u8> = if raw.has_read_type && !raw.read_type_raw.is_empty() {
        let rtdecoded = decode_raw(raw.read_type_raw, raw.read_type_cs, raw.read_type_id_range)?;
        let raw_bytes = decode_zip_encoding(&rtdecoded)?;
        if !raw_bytes.is_empty() {
            raw_bytes
        } else {
            rtdecoded.data.into_owned()
        }
    } else {
        Vec::new()
    };

    // ------------------------------------------------------------------
    // Iterate spots and produce FASTQ records directly (fused path).
    //
    // Instead of building intermediate SpotRecord structs, we slice
    // directly into the decoded column data and call format_read.
    // ------------------------------------------------------------------
    let rps = reads_per_spot.max(1);
    // Per-slot output accumulators. At most 2-4 slots in realistic configs
    // (Split3 -> Read1/Read2/Unpaired, SplitSpot/Interleaved -> Single,
    // SplitFiles -> ReadN(0..rps)). A linear scan over this small Vec beats
    // a HashMap lookup in the hot path.
    let mut records: Vec<BlobSlotOutput> = Vec::with_capacity(4);
    let mut seq_offset: usize = 0;
    let mut qual_offset: usize = 0;
    let mut rt_offset: usize = 0;
    let mut spot_idx_in_blob: usize = 0;
    let mut rl_cursor = 0usize;

    // Reusable buffer for itoa spot-name formatting.
    let mut itoa_buf = itoa::Buffer::new();

    while rl_cursor + rps <= read_lengths.len() {
        let spot_read_lengths = &read_lengths[rl_cursor..rl_cursor + rps];
        let spot_total_bases: usize = spot_read_lengths.iter().map(|&l| l as usize).sum();

        let seq_end = seq_offset + spot_total_bases;
        if seq_end > read_data.len() {
            diag.truncated_spots
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tracing::debug!(
                "blob {blob_idx}, spot {spot_idx_in_blob}: sequence overrun at offset \
                 {seq_offset} + {spot_total_bases} > {}; stopping blob",
                read_data.len(),
            );
            break;
        }

        // Borrow slices directly -- no .to_vec().
        let sequence = &read_data[seq_offset..seq_end];
        seq_offset = seq_end;

        let quality: &[u8] = if quality_is_empty {
            &lite_qual_buf.as_ref().unwrap()[seq_offset - spot_total_bases..seq_offset]
        } else {
            let qual_end = qual_offset + spot_total_bases;
            if qual_end > quality_all.len() {
                diag.quality_overruns
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::debug!(
                    "blob {blob_idx}, spot {spot_idx_in_blob}: quality overrun at offset \
                     {qual_offset} + {spot_total_bases} > {}; using fallback quality",
                    quality_all.len(),
                );
                // Advance qual_offset so subsequent spots don't re-read stale data.
                qual_offset = qual_end;
                // Lazily allocate fallback buffer on first overrun (rare path).
                if lite_qual_buf.is_none() {
                    lite_qual_buf = Some(vec![lite_qual_char; read_data.len()]);
                }
                &lite_qual_buf.as_ref().unwrap()[seq_offset - spot_total_bases..seq_offset]
            } else {
                let q = &quality_all[qual_offset..qual_end];
                qual_offset = qual_end;
                q
            }
        };

        // Invariant: quality must be exactly as long as sequence.
        debug_assert_eq!(
            quality.len(),
            sequence.len(),
            "blob {blob_idx}, spot {spot_idx_in_blob}: quality length {} != sequence length {}",
            quality.len(),
            sequence.len(),
        );

        // Spot number: always numeric 1-based index.
        let spot_number_str = itoa_buf.format(spots_before as usize + spot_idx_in_blob + 1);
        let spot_number = spot_number_str.as_bytes();

        // Original read name from the NAME column (if present).
        let original_name: Option<&[u8]> = if let Some(ref names) = spot_names {
            if spot_idx_in_blob < names.len() {
                Some(&names[spot_idx_in_blob])
            } else {
                None
            }
        } else {
            None
        };

        // Read types for this spot: borrow from decoded data or default to biological.
        let spot_read_types: &[u8] =
            if !read_type_data.is_empty() && rt_offset + rps <= read_type_data.len() {
                let rt = &read_type_data[rt_offset..rt_offset + rps];
                rt_offset += rps;
                rt
            } else {
                rt_offset += rps;
                &[] // empty = all biological (checked below)
            };

        // ------------------------------------------------------------------
        // Inline format_spot logic: split reads, filter, route, format.
        // ------------------------------------------------------------------
        struct ReadSeg {
            start: usize,
            len: usize,
        }

        let mut segments: Vec<ReadSeg> = Vec::with_capacity(rps);
        let mut read_offset: usize = 0;
        for (i, &rlen) in spot_read_lengths.iter().enumerate() {
            let rlen_usize = rlen as usize;
            let end = read_offset + rlen_usize;
            if end > spot_total_bases {
                break;
            }

            // Filter: skip technical reads if configured.
            if config.skip_technical {
                let rtype = spot_read_types.get(i).copied().unwrap_or(0);
                if rtype != 0 {
                    read_offset = end;
                    continue;
                }
            }

            // Filter: skip reads shorter than the minimum length.
            if let Some(min_len) = config.min_read_len
                && rlen < min_len
            {
                read_offset = end;
                continue;
            }

            segments.push(ReadSeg {
                start: read_offset,
                len: rlen_usize,
            });
            read_offset = end;
        }

        if !segments.is_empty() {
            // Append a segment's formatted record to the slot's accumulated
            // buffer, inserting a new slot on first use.
            let mut emit = |slot: OutputSlot, seg: &ReadSeg| {
                let buf = match records.iter_mut().find(|s| s.slot == slot) {
                    Some(s) => s,
                    None => {
                        records.push(BlobSlotOutput {
                            slot,
                            bytes: Vec::new(),
                            records: 0,
                        });
                        records.last_mut().unwrap()
                    }
                };
                let seq = &sequence[seg.start..seg.start + seg.len];
                if config.fasta {
                    append_fasta_record(&mut buf.bytes, run_name, spot_number, original_name, seq);
                } else {
                    let qual = &quality[seg.start..seg.start + seg.len];
                    append_fastq_record(
                        &mut buf.bytes,
                        run_name,
                        spot_number,
                        original_name,
                        seq,
                        qual,
                        Some(diag),
                    );
                }
                buf.records += 1;
            };

            match config.split_mode {
                SplitMode::Split3 => {
                    if segments.len() == 2 {
                        emit(OutputSlot::Read1, &segments[0]);
                        emit(OutputSlot::Read2, &segments[1]);
                    } else {
                        for seg in &segments {
                            emit(OutputSlot::Unpaired, seg);
                        }
                    }
                }
                SplitMode::Interleaved | SplitMode::SplitSpot => {
                    for seg in &segments {
                        emit(OutputSlot::Single, seg);
                    }
                }
                SplitMode::SplitFiles => {
                    for (file_idx, seg) in segments.iter().enumerate() {
                        emit(OutputSlot::ReadN(file_idx as u32), seg);
                    }
                }
            }
        }

        rl_cursor += rps;
        spot_idx_in_blob += 1;
    }

    Ok((records, spot_idx_in_blob as u64))
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
            return Err(Error::Vdb(format!(
                "{accession}: blob {i} start_id {} overlaps previous end {prev_end}",
                blob.start_id,
            )));
        }
        if blob.start_id > prev_end {
            return Err(Error::Vdb(format!(
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
        return Err(Error::Vdb(format!(
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
) -> Result<(u64, u64, Vec<PathBuf>)> {
    let file = std::fs::File::open(sra_path)?;
    let mut archive = KarArchive::open(std::io::BufReader::new(file))?;
    let cursor = VdbCursor::open(&mut archive, sra_path)?;

    // Check platform — reject legacy platforms with complex read structures.
    if let Some(platform) = cursor.platform()
        && is_unsupported_platform(platform)
    {
        return Err(Error::UnsupportedPlatform {
            platform: platform.to_string(),
        });
    }

    // Detect SRA-lite from actual file: if QUALITY column is absent,
    // treat as lite regardless of what the SDL API reported.
    let is_lite = is_lite || !cursor.has_quality();

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
        .map_err(|e| Error::Vdb(format!("failed to build rayon thread pool: {e}")))?;

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
                    .map_err(|e| Error::Vdb(format!("failed to build gzip thread pool: {e}")))?,
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
    let altread_blob_count = cursor.altread_col().map_or(0, |c| c.blob_count());
    let altread_cs = cursor.altread_col().map_or(0, |c| c.meta().checksum_type);
    let x_blob_count = cursor.x_col().map_or(0, |c| c.blob_count());
    let x_cs = cursor.x_col().map_or(0, |c| c.meta().checksum_type);
    let y_blob_count = cursor.y_col().map_or(0, |c| c.blob_count());
    let y_cs = cursor.y_col().map_or(0, |c| c.meta().checksum_type);

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

    tracing::debug!(
        "{accession}: has_read_len={has_read_len} (blobs={read_len_blob_count}), \
         has_read_type={has_read_type} (blobs={read_type_blob_count}), \
         has_name={has_name}, has_quality={has_quality}, \
         metadata_rps={metadata_reads_per_spot:?}, fallback_read_lengths={fallback_read_lengths:?}",
    );
    tracing::debug!("{accession}: streaming decode of {num_blobs} blobs (batch-parallel)",);

    let decode_pb = if config.progress {
        Some(make_styled_pb(
            num_blobs as u64,
            "  {elapsed_precise} [{bar:40.cyan}] {pos}/{len} blobs  {per_sec}  eta {eta}",
        ))
    } else {
        None
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

    let write_result: Result<()> = std::thread::scope(|scope| {
        // ---- Writer thread ----
        let writer_handle = scope.spawn(|| -> Result<()> {
            let mut blob_counter: usize = 0;
            while let Ok(formatted_batches) = batch_rx.recv() {
                for result in formatted_batches {
                    let (slot_outputs, num_spots) = result?;

                    // One write_all per (slot, blob) instead of per record —
                    // orders of magnitude fewer write calls, and the Vec<u8>
                    // allocation happens at most 4× per blob, not per record.
                    for slot_out in &slot_outputs {
                        let writer = if let Some(ref mut sw) = stdout_writer {
                            sw
                        } else {
                            writers.entry(slot_out.slot).or_insert_with(|| {
                                let filename = output_filename(
                                    accession,
                                    slot_out.slot,
                                    config.fasta,
                                    &config.compression,
                                );
                                let final_path = config.output_dir.join(&filename);
                                let tmp_path =
                                    config.output_dir.join(format!("{filename}.partial"));
                                output_paths.push((final_path, tmp_path.clone()));

                                let file = std::fs::File::create(&tmp_path)
                                    .expect("failed to create output file");
                                let buf = std::io::BufWriter::with_capacity(256 * 1024, file);

                                match config.compression {
                                    CompressionMode::Gzip { level } => {
                                        OutputWriter::Gz(ParGzWriter::new(
                                            buf,
                                            level,
                                            DEFAULT_BLOCK_SIZE,
                                            compress_pool.clone().expect("gzip pool must exist"),
                                        ))
                                    }
                                    CompressionMode::Zstd { level, threads } => {
                                        let mut encoder =
                                            zstd::stream::write::Encoder::new(buf, level)
                                                .expect("failed to create zstd encoder");
                                        encoder
                                            .multithread(threads)
                                            .expect("failed to set zstd threads");
                                        OutputWriter::Zstd(encoder)
                                    }
                                    CompressionMode::None => OutputWriter::Plain(buf),
                                }
                            })
                        };

                        writer.write_all(&slot_out.bytes).map_err(Error::Io)?;
                        reads_written += slot_out.records;
                        *per_slot_counts.entry(slot_out.slot).or_insert(0) += slot_out.records;
                    }

                    spots_read.fetch_add(num_spots, std::sync::atomic::Ordering::Relaxed);
                    blob_counter += 1;

                    if let Some(ref pb) = decode_pb {
                        pb.inc(1);
                    }

                    if blob_counter.is_multiple_of(50) || blob_counter == num_blobs {
                        tracing::debug!(
                            "{accession}: decoded {blob_counter}/{num_blobs} blobs, \
                             {} spots so far",
                            spots_read.load(std::sync::atomic::Ordering::Relaxed),
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
        let mut blob_idx: usize = 0;
        while blob_idx < num_blobs {
            if let Some(ref flag) = config.cancelled
                && flag.load(Ordering::Relaxed)
            {
                break;
            }

            let batch_end = (blob_idx + BATCH_SIZE).min(num_blobs);
            let batch_len = batch_end - blob_idx;

            let mut spots_before_per_blob: Vec<u64> = Vec::with_capacity(batch_len);
            {
                let mut cumulative = spots_read.load(std::sync::atomic::Ordering::Relaxed);
                for bi in blob_idx..batch_end {
                    spots_before_per_blob.push(cumulative);
                    cumulative += cursor.read_col().blobs()[bi].id_range as u64;
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

                        let (rl_raw, rl_id_range): (&[u8], u64) =
                            if has_read_len && bi < read_len_blob_count {
                                let rlcol = cursor.read_len_col().unwrap();
                                let rlblob = &rlcol.blobs()[bi];
                                (
                                    rlcol.read_raw_blob_slice(rlblob.start_id)?,
                                    rlblob.id_range as u64,
                                )
                            } else {
                                (&[], 0)
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
                        let (alt_raw, alt_id_range): (&[u8], u64) =
                            if has_altread && bi < altread_blob_count {
                                let col = cursor.altread_col().unwrap();
                                let blob = &col.blobs()[bi];
                                (
                                    col.read_raw_blob_slice(blob.start_id)?,
                                    blob.id_range as u64,
                                )
                            } else {
                                (&[], 0)
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

                        let raw = RawBlobData {
                            read_raw,
                            read_id_range,
                            quality_raw: q_raw,
                            quality_id_range: q_id_range,
                            quality_cs,
                            read_len_raw: rl_raw,
                            read_len_id_range: rl_id_range,
                            read_len_cs,
                            name_raw: n_raw,
                            name_id_range: n_id_range,
                            name_cs,
                            read_type_raw: rt_raw,
                            read_type_id_range: rt_id_range,
                            read_type_cs,
                            altread_raw: alt_raw,
                            altread_id_range: alt_id_range,
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
                            has_read_len,
                            has_name,
                            has_read_type,
                            metadata_reads_per_spot,
                            fixed_spot_len,
                            fallback_read_lengths: fallback_read_lengths.as_deref(),
                        };

                        decode_blob_to_fastq(&raw, &decode_ctx, bi, spots_before_per_blob[i])
                    })
                    .collect()
            });

            // Send to writer thread (blocks if writer is behind by 2 batches).
            if batch_tx.send(formatted_batches).is_err() {
                break; // Writer thread exited (error).
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

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Result of validating an SRA file.
pub struct ValidationResult {
    /// Label for the file that was validated.
    pub label: String,
    /// Whether the file is valid (no errors).
    pub valid: bool,
    /// Number of spots decoded during validation.
    pub spots_validated: u64,
    /// Number of blobs validated.
    pub blobs_validated: usize,
    /// Columns found in the SEQUENCE table.
    pub columns_found: Vec<String>,
    /// Errors encountered during validation.
    pub errors: Vec<String>,
    /// MD5 hex digest of the entire SRA file.
    pub md5: Option<String>,
    /// True if any error originated from [`Error::BlobIntegrity`] (per-blob
    /// CRC32/MD5 failure during decode). Callers can use this to show the
    /// shared [`crate::error::BLOB_INTEGRITY_GUIDANCE`] text once.
    pub any_blob_integrity_error: bool,
}

/// Validate an SRA file by opening as KAR archive, parsing the SEQUENCE
/// table, and decoding all blobs. No output files are produced.
pub fn run_validate(
    sra_path: &std::path::Path,
    threads: usize,
    progress: bool,
) -> ValidationResult {
    let label = sra_path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown")
        .to_string();

    let mut errors = Vec::new();
    let mut columns_found = Vec::new();

    // Step 1: Open KAR archive.
    let file = match std::fs::File::open(sra_path) {
        Ok(f) => f,
        Err(e) => {
            errors.push(format!("cannot open file: {e}"));
            return ValidationResult {
                label,
                valid: false,
                spots_validated: 0,
                blobs_validated: 0,
                columns_found,
                errors,
                md5: None,
                any_blob_integrity_error: false,
            };
        }
    };

    let mut archive = match KarArchive::open(std::io::BufReader::new(file)) {
        Ok(a) => a,
        Err(e) => {
            errors.push(format!("invalid KAR archive: {e}"));
            return ValidationResult {
                label,
                valid: false,
                spots_validated: 0,
                blobs_validated: 0,
                columns_found,
                errors,
                md5: None,
                any_blob_integrity_error: false,
            };
        }
    };

    // Step 2: Open VDB cursor.
    let cursor = match VdbCursor::open(&mut archive, sra_path) {
        Ok(c) => c,
        Err(e) => {
            errors.push(format!("cannot open VDB cursor: {e}"));
            return ValidationResult {
                label,
                valid: false,
                spots_validated: 0,
                blobs_validated: 0,
                columns_found,
                errors,
                md5: None,
                any_blob_integrity_error: false,
            };
        }
    };

    // Record found columns.
    columns_found.push("READ".into());
    if cursor.has_quality() {
        columns_found.push("QUALITY".into());
    }
    if cursor.read_len_col().is_some() {
        columns_found.push("READ_LEN".into());
    }
    if cursor.read_type_col().is_some() {
        columns_found.push("READ_TYPE".into());
    }
    if cursor.name_col().is_some() {
        columns_found.push("NAME".into());
    }

    let expected_spots = cursor.spot_count();
    let num_blobs = cursor.read_col().blob_count();
    let read_cs = cursor.read_col().meta().checksum_type;
    let has_quality = cursor.quality_col().is_some();
    let quality_blob_count = cursor.quality_col().map_or(0, |c| c.blob_count());
    let quality_cs = cursor.quality_col().map_or(0, |c| c.meta().checksum_type);
    let has_read_len = cursor.read_len_col().is_some();
    let read_len_blob_count = cursor.read_len_col().map_or(0, |c| c.blob_count());
    let read_len_cs = cursor.read_len_col().map_or(0, |c| c.meta().checksum_type);

    // Step 3: Decode all blobs in parallel.
    let pool = match rayon::ThreadPoolBuilder::new().num_threads(threads).build() {
        Ok(p) => p,
        Err(e) => {
            errors.push(format!("failed to build thread pool: {e}"));
            return ValidationResult {
                label,
                valid: false,
                spots_validated: 0,
                blobs_validated: 0,
                columns_found,
                errors,
                md5: None,
                any_blob_integrity_error: false,
            };
        }
    };

    let decode_pb = if progress {
        Some(make_styled_pb(
            num_blobs as u64,
            "  {elapsed_precise} [{bar:40.cyan}] {pos}/{len} blobs  {per_sec}  eta {eta}",
        ))
    } else {
        None
    };

    let total_spots = std::sync::atomic::AtomicU64::new(0);
    let mut blobs_validated: usize = 0;
    let mut any_blob_integrity_error = false;

    const BATCH_SIZE: usize = 1024;
    let mut blob_idx: usize = 0;

    // Helper: capture (msg, is_blob_integrity) from a decode Error.
    fn fmt_err(tag: &str, bi: usize, e: Error) -> (String, bool) {
        let is_integrity = matches!(e, Error::BlobIntegrity { .. });
        (format!("{tag} blob {bi} decode: {e}"), is_integrity)
    }

    while blob_idx < num_blobs {
        let batch_end = (blob_idx + BATCH_SIZE).min(num_blobs);

        let batch_errors: Vec<(usize, String, bool)> = pool.install(|| {
            (blob_idx..batch_end)
                .into_par_iter()
                .filter_map(|bi| {
                    // Decode READ blob.
                    let read_blob = &cursor.read_col().blobs()[bi];
                    let read_raw = match cursor.read_col().read_raw_blob_slice(read_blob.start_id) {
                        Ok(r) => r,
                        Err(e) => return Some((bi, format!("READ blob {bi}: {e}"), false)),
                    };
                    let id_range = read_blob.id_range as u64;
                    if let Err(e) = decode_raw(read_raw, read_cs, id_range) {
                        let (msg, is_integ) = fmt_err("READ", bi, e);
                        return Some((bi, msg, is_integ));
                    }

                    // Count spots from this blob.
                    total_spots.fetch_add(id_range, std::sync::atomic::Ordering::Relaxed);

                    // Decode QUALITY blob.
                    if has_quality && bi < quality_blob_count {
                        let qcol = cursor.quality_col().unwrap();
                        let qblob = &qcol.blobs()[bi];
                        match qcol.read_raw_blob_slice(qblob.start_id) {
                            Ok(q_raw) => {
                                let q_id = qblob.id_range as u64;
                                match decode_raw(q_raw, quality_cs, q_id) {
                                    Ok(qd) => {
                                        if let Err(e) = decode_zip_encoding(&qd) {
                                            return Some((
                                                bi,
                                                format!("QUALITY blob {bi} unzip: {e}"),
                                                false,
                                            ));
                                        }
                                    }
                                    Err(e) => {
                                        let (msg, is_integ) = fmt_err("QUALITY", bi, e);
                                        return Some((bi, msg, is_integ));
                                    }
                                }
                            }
                            Err(e) => {
                                return Some((bi, format!("QUALITY blob {bi}: {e}"), false));
                            }
                        }
                    }

                    // Decode READ_LEN blob.
                    if has_read_len && bi < read_len_blob_count {
                        let rlcol = cursor.read_len_col().unwrap();
                        let rlblob = &rlcol.blobs()[bi];
                        match rlcol.read_raw_blob_slice(rlblob.start_id) {
                            Ok(rl_raw) => {
                                let rl_id = rlblob.id_range as u64;
                                if let Err(e) = decode_raw(rl_raw, read_len_cs, rl_id) {
                                    let (msg, is_integ) = fmt_err("READ_LEN", bi, e);
                                    return Some((bi, msg, is_integ));
                                }
                            }
                            Err(e) => {
                                return Some((bi, format!("READ_LEN blob {bi}: {e}"), false));
                            }
                        }
                    }

                    None
                })
                .collect()
        });

        for (bi, msg, is_integ) in &batch_errors {
            errors.push(msg.clone());
            if *is_integ {
                any_blob_integrity_error = true;
            }
            tracing::error!("validation error at blob {bi}: {msg}");
        }

        blobs_validated += batch_end - blob_idx;

        if let Some(ref pb) = decode_pb {
            pb.inc((batch_end - blob_idx) as u64);
        }

        blob_idx = batch_end;
    }

    if let Some(pb) = decode_pb {
        pb.finish_and_clear();
    }

    let decoded_spots = total_spots.load(std::sync::atomic::Ordering::Relaxed);
    if expected_spots > 0 && decoded_spots != expected_spots {
        errors.push(format!(
            "spot count mismatch: metadata says {expected_spots}, decoded {decoded_spots}",
        ));
    }

    let md5 = compute_file_md5(sra_path)
        .map_err(|e| errors.push(format!("MD5 compute: {e}")))
        .ok();

    ValidationResult {
        valid: errors.is_empty(),
        label,
        spots_validated: decoded_spots,
        blobs_validated,
        columns_found,
        errors,
        md5,
        any_blob_integrity_error,
    }
}

fn compute_file_md5(path: &std::path::Path) -> std::io::Result<String> {
    use md5::{Digest, Md5};
    use std::io::Read;
    let mut file = std::fs::File::open(path)?;
    let mut hasher = Md5::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    let digest = hasher.finalize();
    Ok(digest.iter().map(|b| format!("{b:02x}")).collect())
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

    // Detect SRA-lite by checking if the quality column is absent.
    // We pass `false` initially; decode_and_write will handle quality
    // absence gracefully via sra_lite_quality fallback.
    let is_lite = false;

    let diag = Arc::new(IntegrityDiag::default());
    let (spots_read, reads_written, output_files) =
        decode_and_write(sra_path, &acc, config, is_lite, &diag)
            .map_err(|e| wrap_blob_integrity(&acc, e))?;

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
        if config.strict {
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
    };

    tracing::info!(
        "{accession}: downloading {} to {}",
        crate::util::format_size(total_sra_size),
        temp_path.display(),
    );

    let dl_future = download_file(
        &urls,
        total_sra_size,
        resolved.sra_file.md5.as_deref(),
        &temp_path,
        &dl_config,
    );

    let dl_result = if let Some(ref flag) = config.cancelled {
        let flag = flag.clone();
        tokio::select! {
            result = dl_future => result?,
            _ = poll_cancelled(flag) => {
                tracing::info!("{accession}: download cancelled");
                let _ = tokio::fs::remove_file(&temp_path).await;
                let sidecar = crate::download::progress_path(&temp_path);
                let _ = tokio::fs::remove_file(&sidecar).await;
                return Err(Error::Cancelled { output_files: vec![] });
            }
        }
    } else {
        dl_future.await?
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

    let diag = Arc::new(IntegrityDiag::default());
    let (spots_read, reads_written, output_files) = match decode_and_write(
        &downloaded.temp_path,
        &downloaded.accession,
        config,
        downloaded.is_lite,
        &diag,
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

    // Clean up temp file.
    if let Err(e) = std::fs::remove_file(&downloaded.temp_path) {
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

    tracing::info!(
        "{}: done -- {spots_read} spots, {reads_written} reads written, \
         {} transferred",
        downloaded.accession,
        crate::util::format_size(downloaded.bytes_transferred),
    );

    if diag.any() {
        let summary = diag.summary();
        if config.strict {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sdl::{ResolvedFile, ResolvedMirror};

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
