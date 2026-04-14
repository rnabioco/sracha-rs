//! Integration tests for the sracha pipeline.
//!
//! Tests marked `#[ignore]` require network access to download a small SRA
//! fixture file.  Run them with:
//!
//! ```sh
//! cargo test -p sracha-core -- --ignored
//! ```

use std::io::Read;
use std::path::PathBuf;
use std::sync::Once;

use sracha_core::fastq::{CompressionMode, SplitMode};
use sracha_core::pipeline::PipelineConfig;

// ---------------------------------------------------------------------------
// Fixture helpers
// ---------------------------------------------------------------------------

/// Directory where cached SRA fixture files live.
fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// Ensure the SRR000001 fixture exists, downloading it if necessary.
///
/// Uses a `Once` guard so concurrent tests don't race.  The fixture is
/// ~5.5 MiB and hosted on a public S3 bucket (no auth required).
fn ensure_srr000001() -> PathBuf {
    static DOWNLOAD: Once = Once::new();
    let path = fixtures_dir().join("SRR000001.sra");

    DOWNLOAD.call_once(|| {
        if path.exists() {
            return;
        }
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();

        let url = "https://sra-pub-run-odp.s3.amazonaws.com/sra/SRR000001/SRR000001";
        eprintln!("downloading SRR000001 fixture from {url} ...");

        let resp = reqwest::blocking::get(url)
            .unwrap_or_else(|e| panic!("failed to download SRR000001: {e}"));
        assert!(
            resp.status().is_success(),
            "HTTP {} downloading fixture",
            resp.status()
        );
        let bytes = resp.bytes().unwrap();
        std::fs::write(&path, &bytes).unwrap();
        eprintln!(
            "fixture saved to {} ({} bytes)",
            path.display(),
            bytes.len()
        );
    });

    assert!(path.exists(), "fixture not found at {}", path.display());
    path
}

/// Ensure the SRR28588231 fixture (Illumina paired-end, 23 MiB).
///
/// This accession exercises: tile boundary detection (skey id2ord),
/// per-spot template selection, fixed spot length from v1 blob headers,
/// and irzip v3 dual-series X/Y coordinate decoding.
fn ensure_srr28588231() -> PathBuf {
    static DOWNLOAD: Once = Once::new();
    let path = fixtures_dir().join("SRR28588231.sra");

    DOWNLOAD.call_once(|| {
        if path.exists() {
            return;
        }
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();

        let url = "https://sra-pub-run-odp.s3.amazonaws.com/sra/SRR28588231/SRR28588231";
        eprintln!("downloading SRR28588231 fixture from {url} ...");

        let resp = reqwest::blocking::get(url)
            .unwrap_or_else(|e| panic!("failed to download SRR28588231: {e}"));
        assert!(
            resp.status().is_success(),
            "HTTP {} downloading fixture",
            resp.status()
        );
        let bytes = resp.bytes().unwrap();
        std::fs::write(&path, &bytes).unwrap();
        eprintln!(
            "fixture saved to {} ({} bytes)",
            path.display(),
            bytes.len()
        );
    });

    assert!(path.exists(), "fixture not found at {}", path.display());
    path
}

/// Build a `PipelineConfig` suitable for testing.
fn test_config(
    output_dir: &std::path::Path,
    split_mode: SplitMode,
    compression: CompressionMode,
) -> PipelineConfig {
    PipelineConfig {
        output_dir: output_dir.to_path_buf(),
        split_mode,
        compression,
        threads: 2,
        connections: 1,
        skip_technical: true,
        min_read_len: None,
        force: true,
        progress: false,
        run_info: None,
        fasta: false,
    }
}

/// Validate that the data looks like a FASTQ file: 4-line records starting
/// with '@' header, sequence, '+', quality.
fn assert_valid_fastq(data: &[u8]) {
    let text = std::str::from_utf8(data).expect("FASTQ should be valid UTF-8");
    let lines: Vec<&str> = text.lines().collect();
    assert!(
        lines.len() >= 4,
        "FASTQ must have at least 4 lines, got {}",
        lines.len()
    );
    assert_eq!(
        lines.len() % 4,
        0,
        "FASTQ line count must be a multiple of 4, got {}",
        lines.len()
    );
    // Check first record structure.
    assert!(
        lines[0].starts_with('@'),
        "first line should start with '@', got {:?}",
        &lines[0][..lines[0].len().min(40)]
    );
    assert!(!lines[1].is_empty(), "sequence line should not be empty");
    assert!(
        lines[2].starts_with('+'),
        "third line should start with '+'"
    );
    assert_eq!(
        lines[1].len(),
        lines[3].len(),
        "sequence and quality lengths must match"
    );
}

// ---------------------------------------------------------------------------
// Tests that require the SRA fixture (network gated)
// ---------------------------------------------------------------------------

#[ignore] // requires network on first run; cached thereafter
#[test]
fn run_fastq_split3() {
    let sra_path = ensure_srr000001();
    let tmp = tempfile::tempdir().unwrap();
    let config = test_config(tmp.path(), SplitMode::Split3, CompressionMode::None);

    let stats = sracha_core::pipeline::run_fastq(&sra_path, Some("SRR000001"), &config).unwrap();

    assert!(stats.spots_read > 0, "should read at least one spot");
    assert!(stats.reads_written > 0, "should write at least one read");
    assert!(
        !stats.output_files.is_empty(),
        "should produce output files"
    );

    // SRR000001 has single-end reads, so split3 produces an unpaired _0 file.
    // Paired-end accessions would produce _1 and _2 files instead.
    let names: Vec<String> = stats
        .output_files
        .iter()
        .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
        .collect();
    assert!(
        names.iter().any(|n| n.ends_with(".fastq")),
        "should produce .fastq files, got: {names:?}"
    );

    // Validate FASTQ content of every output file.
    for path in &stats.output_files {
        let data = std::fs::read(path).unwrap();
        assert!(!data.is_empty(), "{} should not be empty", path.display());
        assert_valid_fastq(&data);
    }
}

#[ignore]
#[test]
fn run_fastq_split_spot() {
    let sra_path = ensure_srr000001();
    let tmp = tempfile::tempdir().unwrap();
    let config = test_config(tmp.path(), SplitMode::SplitSpot, CompressionMode::None);

    let stats = sracha_core::pipeline::run_fastq(&sra_path, Some("SRR000001"), &config).unwrap();

    assert!(stats.spots_read > 0);
    assert_eq!(
        stats.output_files.len(),
        1,
        "split-spot should produce exactly one file"
    );

    let data = std::fs::read(&stats.output_files[0]).unwrap();
    assert_valid_fastq(&data);
}

#[ignore]
#[test]
fn run_fastq_gzip() {
    let sra_path = ensure_srr000001();
    let tmp = tempfile::tempdir().unwrap();
    let config = test_config(
        tmp.path(),
        SplitMode::SplitSpot,
        CompressionMode::Gzip { level: 1 },
    );

    let stats = sracha_core::pipeline::run_fastq(&sra_path, Some("SRR000001"), &config).unwrap();

    assert!(stats.spots_read > 0);
    for path in &stats.output_files {
        let name = path.file_name().unwrap().to_string_lossy();
        assert!(
            name.ends_with(".fastq.gz"),
            "gzip output should end with .fastq.gz, got {name}"
        );

        // Decompress and verify FASTQ content.
        let file = std::fs::File::open(path).unwrap();
        let mut decoder = flate2::read::MultiGzDecoder::new(file);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed).unwrap();
        assert_valid_fastq(&decompressed);
    }
}

#[ignore]
#[test]
fn run_fastq_deterministic() {
    let sra_path = ensure_srr000001();

    let tmp1 = tempfile::tempdir().unwrap();
    let config1 = test_config(tmp1.path(), SplitMode::SplitSpot, CompressionMode::None);
    let stats1 = sracha_core::pipeline::run_fastq(&sra_path, Some("SRR000001"), &config1).unwrap();

    let tmp2 = tempfile::tempdir().unwrap();
    let config2 = test_config(tmp2.path(), SplitMode::SplitSpot, CompressionMode::None);
    let stats2 = sracha_core::pipeline::run_fastq(&sra_path, Some("SRR000001"), &config2).unwrap();

    assert_eq!(stats1.spots_read, stats2.spots_read);
    assert_eq!(stats1.reads_written, stats2.reads_written);
    assert_eq!(stats1.output_files.len(), stats2.output_files.len());

    for (f1, f2) in stats1.output_files.iter().zip(stats2.output_files.iter()) {
        let data1 = std::fs::read(f1).unwrap();
        let data2 = std::fs::read(f2).unwrap();
        assert_eq!(data1, data2, "output should be byte-identical across runs");
    }
}

// ---------------------------------------------------------------------------
// Tests that do NOT require network
// ---------------------------------------------------------------------------

#[test]
fn run_fastq_nonexistent_file() {
    let tmp = tempfile::tempdir().unwrap();
    let config = test_config(tmp.path(), SplitMode::SplitSpot, CompressionMode::None);
    let result =
        sracha_core::pipeline::run_fastq(std::path::Path::new("/nonexistent.sra"), None, &config);
    assert!(result.is_err());
}

#[test]
fn run_fastq_corrupt_file() {
    let tmp = tempfile::tempdir().unwrap();
    let corrupt_path = tmp.path().join("corrupt.sra");
    std::fs::write(&corrupt_path, b"this is not a valid SRA file").unwrap();

    let config = test_config(tmp.path(), SplitMode::SplitSpot, CompressionMode::None);
    let result = sracha_core::pipeline::run_fastq(&corrupt_path, None, &config);
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// SRR28588231 regression tests (Illumina paired-end, #3)
//
// Guards against:
//   - Writing entire blobs as single records (v1 fixed_spot_len detection)
//   - Wrong tile assignments at spot boundaries (skey id2ord big-endian)
//   - Wrong X/Y coordinates (irzip v3 dual-series decoding)
// ---------------------------------------------------------------------------

/// Compute MD5 hex digest of a file.
fn md5_file(path: &std::path::Path) -> String {
    use md5::{Digest, Md5};
    let data = std::fs::read(path).unwrap();
    let hash = Md5::digest(&data);
    hash.iter().map(|b| format!("{b:02x}")).collect()
}

#[ignore] // requires network on first run
#[test]
fn illumina_paired_spot_count() {
    // SRR28588231 has 66,220 spots × 2 reads = 132,440 reads.
    // Without the v1 row_length fix this produced 16 spots (one per blob).
    let sra_path = ensure_srr28588231();
    let tmp = tempfile::tempdir().unwrap();
    let config = test_config(tmp.path(), SplitMode::Split3, CompressionMode::None);

    let stats = sracha_core::pipeline::run_fastq(&sra_path, Some("SRR28588231"), &config).unwrap();

    assert_eq!(stats.spots_read, 66_220, "expected 66,220 spots");
    assert_eq!(stats.reads_written, 132_440, "expected 132,440 reads");
    assert_eq!(
        stats.output_files.len(),
        2,
        "paired-end should produce _1 and _2 files"
    );

    // Each file should have 66,220 records × 4 lines = 264,880 lines.
    for path in &stats.output_files {
        let data = std::fs::read(path).unwrap();
        assert_valid_fastq(&data);
        let line_count = data.iter().filter(|&&b| b == b'\n').count();
        assert_eq!(
            line_count, 264_880,
            "each paired file should have 264,880 lines, got {line_count}"
        );
    }
}

#[ignore] // requires network on first run
#[test]
fn illumina_paired_byte_identity() {
    // Byte-for-byte identity with fasterq-dump 3.2.0 output.
    // These md5s cover tile boundaries, X/Y coordinates, and sequence/quality.
    let sra_path = ensure_srr28588231();
    let tmp = tempfile::tempdir().unwrap();
    let config = test_config(tmp.path(), SplitMode::Split3, CompressionMode::None);

    let stats = sracha_core::pipeline::run_fastq(&sra_path, Some("SRR28588231"), &config).unwrap();

    let md5_1 = md5_file(&stats.output_files[0]);
    let md5_2 = md5_file(&stats.output_files[1]);

    assert_eq!(
        md5_1, "f889b1aec36ef6ba73f71d1e1c1a805a",
        "SRR28588231_1.fastq md5 mismatch — deflines or data differ from fasterq-dump"
    );
    assert_eq!(
        md5_2, "d307ea894444f2ede7a1ee51d65c6e04",
        "SRR28588231_2.fastq md5 mismatch — deflines or data differ from fasterq-dump"
    );
}
