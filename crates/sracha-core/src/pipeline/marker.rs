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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    /// A minimal `PipelineConfig` for marker round-trips. Only the fields the
    /// marker actually compares (`split_mode`, `compression`, `fasta`,
    /// `skip_technical`, `min_read_len`) matter here; the rest are defaults.
    fn test_config(output_dir: &Path) -> PipelineConfig {
        PipelineConfig {
            output_dir: output_dir.to_path_buf(),
            split_mode: crate::fastq::SplitMode::Split3,
            compression: crate::fastq::CompressionMode::None,
            threads: 1,
            connections: 1,
            skip_technical: true,
            min_read_len: None,
            force: false,
            progress: false,
            run_info: None,
            fasta: false,
            resume: true,
            stdout: false,
            cancelled: None,
            strict: false,
            http_client: None,
            keep_sra: false,
            paired_suffix: crate::fastq::PairedSuffix::Numeric,
            seq_defline: None,
            folder_per_accession: false,
            metadata: None,
            metadata_url: None,
            metadata_md5: None,
            metadata_size: None,
            metadata_service: None,
        }
    }

    /// Create `name` in `dir` with `len` bytes and return its path.
    fn make_output(dir: &Path, name: &str, len: usize) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, vec![b'A'; len]).unwrap();
        path
    }

    #[test]
    fn marker_path_format() {
        let p = marker_path(Path::new("/out"), "SRR123");
        assert_eq!(p, PathBuf::from("/out/.sracha-done-SRR123"));
    }

    #[test]
    fn compression_key_is_stable() {
        use crate::fastq::CompressionMode;
        assert_eq!(compression_key(&CompressionMode::None), "none");
        assert_eq!(
            compression_key(&CompressionMode::Gzip { level: 6 }),
            "gzip:6"
        );
        assert_eq!(
            compression_key(&CompressionMode::Zstd {
                level: 3,
                threads: 4
            }),
            "zstd:3:4"
        );
    }

    #[test]
    fn iso8601_now_utc_has_rfc3339_shape() {
        let s = iso8601_now_utc();
        // YYYY-MM-DDTHH:MM:SSZ
        assert_eq!(s.len(), 20, "got {s:?}");
        assert!(s.ends_with('Z'));
        assert_eq!(&s[4..5], "-");
        assert_eq!(&s[10..11], "T");
        assert!(s[0..4].parse::<u32>().unwrap() >= 2026);
    }

    #[test]
    fn write_then_check_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = test_config(dir.path());
        let f1 = make_output(dir.path(), "SRR1_1.fastq", 10);
        let f2 = make_output(dir.path(), "SRR1_2.fastq", 20);

        write_completion_marker(
            dir.path(),
            "SRR1",
            Some("abc"),
            1234,
            &cfg,
            &[f1.clone(), f2.clone()],
        )
        .unwrap();

        let got = check_completion_marker(dir.path(), "SRR1", &cfg, 1234);
        assert_eq!(got, Some(vec![f1, f2]));
    }

    #[test]
    fn check_returns_none_when_marker_absent() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = test_config(dir.path());
        assert!(check_completion_marker(dir.path(), "SRR1", &cfg, 1234).is_none());
    }

    #[test]
    fn check_rejects_sra_size_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = test_config(dir.path());
        let f1 = make_output(dir.path(), "SRR1_1.fastq", 10);
        write_completion_marker(dir.path(), "SRR1", None, 1234, &cfg, &[f1]).unwrap();

        // Same accession + config, but a different upstream SRA size means the
        // input changed → must re-decode.
        assert!(check_completion_marker(dir.path(), "SRR1", &cfg, 9999).is_none());
    }

    #[test]
    fn check_rejects_config_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = test_config(dir.path());
        let f1 = make_output(dir.path(), "SRR1_1.fastq", 10);
        write_completion_marker(dir.path(), "SRR1", None, 1234, &cfg, &[f1]).unwrap();

        let mut other = test_config(dir.path());
        other.compression = crate::fastq::CompressionMode::Gzip { level: 6 };
        assert!(check_completion_marker(dir.path(), "SRR1", &other, 1234).is_none());

        let mut other = test_config(dir.path());
        other.fasta = true;
        assert!(check_completion_marker(dir.path(), "SRR1", &other, 1234).is_none());

        let mut other = test_config(dir.path());
        other.min_read_len = Some(25);
        assert!(check_completion_marker(dir.path(), "SRR1", &other, 1234).is_none());
    }

    #[test]
    fn check_rejects_resized_output_file() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = test_config(dir.path());
        let f1 = make_output(dir.path(), "SRR1_1.fastq", 10);
        write_completion_marker(
            dir.path(),
            "SRR1",
            None,
            1234,
            &cfg,
            std::slice::from_ref(&f1),
        )
        .unwrap();

        // Truncate the output: recorded size no longer matches → re-decode.
        std::fs::write(&f1, b"short").unwrap();
        assert!(check_completion_marker(dir.path(), "SRR1", &cfg, 1234).is_none());
    }

    #[test]
    fn check_rejects_missing_output_file() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = test_config(dir.path());
        let f1 = make_output(dir.path(), "SRR1_1.fastq", 10);
        write_completion_marker(
            dir.path(),
            "SRR1",
            None,
            1234,
            &cfg,
            std::slice::from_ref(&f1),
        )
        .unwrap();

        std::fs::remove_file(&f1).unwrap();
        assert!(check_completion_marker(dir.path(), "SRR1", &cfg, 1234).is_none());
    }

    #[test]
    fn check_rejects_when_temp_sra_present() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = test_config(dir.path());
        let f1 = make_output(dir.path(), "SRR1_1.fastq", 10);
        write_completion_marker(dir.path(), "SRR1", None, 1234, &cfg, &[f1]).unwrap();

        // A leftover temp SRA means the prior decode did not finish cleanly.
        std::fs::write(dir.path().join(".sracha-tmp-SRR1.sra"), b"partial").unwrap();
        assert!(check_completion_marker(dir.path(), "SRR1", &cfg, 1234).is_none());
    }

    #[test]
    fn check_rejects_stale_marker_version() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = test_config(dir.path());
        let f1 = make_output(dir.path(), "SRR1_1.fastq", 10);
        write_completion_marker(dir.path(), "SRR1", None, 1234, &cfg, &[f1]).unwrap();

        // Rewrite the on-disk marker with a bumped version to simulate a
        // schema change; the loader must invalidate it.
        let path = marker_path(dir.path(), "SRR1");
        let content = std::fs::read_to_string(&path).unwrap();
        let bumped = content.replace(
            &format!("\"version\": {MARKER_VERSION}"),
            &format!("\"version\": {}", MARKER_VERSION + 1),
        );
        assert_ne!(bumped, content, "version field not found in marker JSON");
        std::fs::write(&path, bumped).unwrap();

        assert!(check_completion_marker(dir.path(), "SRR1", &cfg, 1234).is_none());
    }

    #[test]
    fn write_stats_file_appends_ndjson() {
        let dir = tempfile::tempdir().unwrap();
        let out = make_output(dir.path(), "SRR1_1.fastq", 42);
        let diag = IntegrityDiag::default();

        for acc in ["SRR1", "SRR2"] {
            write_stats_file(StatsEntry {
                output_dir: dir.path(),
                accession: acc,
                spots_read: 100,
                reads_written: 200,
                sra_md5: Some("deadbeef"),
                sra_size: 4096,
                output_files: std::slice::from_ref(&out),
                diag: &diag,
            })
            .unwrap();
        }

        let content = std::fs::read_to_string(stats_path(dir.path())).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2, "append should produce one line per call");

        let first: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first["accession"], "SRR1");
        assert_eq!(first["spots_read"], 100);
        assert_eq!(first["integrity"]["ok"], true);
        assert_eq!(first["output_files"][0]["name"], "SRR1_1.fastq");
        assert_eq!(first["output_files"][0]["bytes"], 42);

        let second: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(second["accession"], "SRR2");
    }
}
