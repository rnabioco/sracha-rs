//! Completion markers + stats JSONL — the on-disk "did we finish decoding
//! this accession" bookkeeping that lets re-runs skip already-finished
//! work, plus the append-only audit log of every decode attempt.
//!
//! Extracted from `pipeline/mod.rs` as part of the pipeline refactor
//! (no behavior change).

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::fastq::IntegrityDiag;

use super::PipelineConfig;

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

pub(crate) fn marker_path(output_dir: &Path, accession: &str) -> PathBuf {
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
pub(super) fn stats_path(output_dir: &Path) -> PathBuf {
    output_dir.join("sracha-stats.jsonl")
}

/// Inputs for a single `sracha-stats.jsonl` line.
pub(crate) struct StatsEntry<'a> {
    pub output_dir: &'a Path,
    pub accession: &'a str,
    pub spots_read: u64,
    pub reads_written: u64,
    pub sra_md5: Option<&'a str>,
    pub sra_size: u64,
    pub output_files: &'a [PathBuf],
    pub diag: &'a IntegrityDiag,
}

/// Append an integrity summary line to the shared stats JSONL. We record
/// every accession — passing and failing — so the file doubles as an audit
/// log that a run happened and which inputs produced which outputs.
pub(crate) fn write_stats_file(entry: StatsEntry<'_>) -> Result<()> {
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
pub(crate) fn iso8601_now_utc() -> String {
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

pub(crate) fn write_completion_marker(
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
pub(crate) fn check_completion_marker(
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
