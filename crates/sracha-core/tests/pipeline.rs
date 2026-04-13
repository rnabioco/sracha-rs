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

use sracha_core::fastq::SplitMode;
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

/// Build a `PipelineConfig` suitable for testing.
fn test_config(output_dir: &std::path::Path, split_mode: SplitMode, gzip: bool) -> PipelineConfig {
    PipelineConfig {
        output_dir: output_dir.to_path_buf(),
        split_mode,
        gzip,
        gzip_level: 1, // fast compression for tests
        threads: 2,
        connections: 1,
        skip_technical: true,
        min_read_len: None,
        force: true,
        progress: false,
        run_info: None,
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

/// Validate every record in a FASTQ file: structure, seq/qual length match,
/// and that all quality bytes are valid Phred+33 ASCII (33–126).
///
/// This is the key regression check for the quality encoding bug: if raw
/// Phred integers are written without the +33 offset, bytes < 33 (including
/// 0x0a = newline) appear in the quality string and break downstream tools.
fn assert_quality_bytes_valid(data: &[u8], label: &str) {
    let text = std::str::from_utf8(data).expect("FASTQ should be valid UTF-8");
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(
        lines.len() % 4,
        0,
        "{label}: line count must be a multiple of 4, got {}",
        lines.len()
    );
    for (record_idx, chunk) in lines.chunks(4).enumerate() {
        let seq = chunk[1].as_bytes();
        let qual = chunk[3].as_bytes();

        assert_eq!(
            seq.len(),
            qual.len(),
            "{label}: record {record_idx}: sequence length {} != quality length {}",
            seq.len(),
            qual.len(),
        );

        for (pos, &byte) in qual.iter().enumerate() {
            assert!(
                byte >= 33 && byte <= 126,
                "{label}: record {record_idx}, position {pos}: quality byte {byte} (0x{byte:02x}) \
                 is outside valid Phred+33 ASCII range [33, 126]. \
                 This indicates raw Phred integers were written without the +33 offset.",
            );
        }
    }
}

/// Parse a FASTQ byte slice into (spot_id, sequence, quality) tuples.
///
/// `spot_id` is extracted from the defline as the integer after the last `.`
/// (e.g. `@SRR10971381.42 length=150` -> `42`). Used to align records from
/// two tools that may format deflines differently.
fn parse_fastq_records(data: &[u8]) -> Vec<(u64, Vec<u8>, Vec<u8>)> {
    let text = std::str::from_utf8(data).expect("FASTQ should be valid UTF-8");
    let mut records = Vec::new();
    let lines: Vec<&str> = text.lines().collect();
    for chunk in lines.chunks(4) {
        if chunk.len() < 4 {
            break;
        }
        // Extract spot id: digits after the last '.' before any whitespace.
        let defline = chunk[0].trim_start_matches('@');
        let id_str = defline
            .split_whitespace()
            .next()
            .unwrap_or(defline)
            .rsplit('.')
            .next()
            .unwrap_or("0");
        let spot_id: u64 = id_str.parse().unwrap_or(0);
        records.push((
            spot_id,
            chunk[1].as_bytes().to_vec(),
            chunk[3].as_bytes().to_vec(),
        ));
    }
    records.sort_by_key(|(id, _, _)| *id);
    records
}

/// Ensure a small paired-end Illumina fixture exists, downloading if needed.
///
/// SRR10971381 is a SARS-CoV-2 amplicon library sequenced on an Illumina
/// instrument (paired-end, 2 × 150 bp). The .sra file is ~2 MiB.
/// It exercises the quality encoding path that was previously broken by the
/// "looks like ASCII" heuristic: Illumina data with many high-quality bases
/// at the start of a blob caused raw Phred integers to be written without the
/// mandatory +33 offset.
fn ensure_srr10971381() -> PathBuf {
    static DOWNLOAD: Once = Once::new();
    let path = fixtures_dir().join("SRR10971381.sra");

    DOWNLOAD.call_once(|| {
        if path.exists() {
            return;
        }
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();

        let url = "https://sra-pub-run-odp.s3.amazonaws.com/sra/SRR10971381/SRR10971381";
        eprintln!("downloading SRR10971381 fixture from {url} ...");

        let resp = reqwest::blocking::get(url)
            .unwrap_or_else(|e| panic!("failed to download SRR10971381: {e}"));
        assert!(
            resp.status().is_success(),
            "HTTP {} downloading SRR10971381 fixture",
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

// ---------------------------------------------------------------------------
// Tests that require the SRA fixture (network gated)
// ---------------------------------------------------------------------------

#[ignore] // requires network on first run; cached thereafter
#[test]
fn run_fastq_split3() {
    let sra_path = ensure_srr000001();
    let tmp = tempfile::tempdir().unwrap();
    let config = test_config(tmp.path(), SplitMode::Split3, false);

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
    let config = test_config(tmp.path(), SplitMode::SplitSpot, false);

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
    let config = test_config(tmp.path(), SplitMode::SplitSpot, true);

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
    let config1 = test_config(tmp1.path(), SplitMode::SplitSpot, false);
    let stats1 = sracha_core::pipeline::run_fastq(&sra_path, Some("SRR000001"), &config1).unwrap();

    let tmp2 = tempfile::tempdir().unwrap();
    let config2 = test_config(tmp2.path(), SplitMode::SplitSpot, false);
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
// Quality encoding regression tests (require network on first run)
// ---------------------------------------------------------------------------

/// Regression test for the quality encoding bug: verify that every quality
/// byte in sracha output for a paired-end Illumina file is valid Phred+33
/// ASCII (33–126).
///
/// The original bug: `pipeline::decode_and_write` sampled only the first 100
/// bytes of a quality blob to decide whether to apply the Phred+33 offset.
/// For high-quality Illumina data, the first 100 raw Phred values are often
/// all ≥ 33, causing the heuristic to pass them through unmodified. Later in
/// the blob, lower-quality bases produced raw bytes < 33 — including 0x0a
/// (Phred 10 = ASCII newline) — which were written directly into the FASTQ
/// quality string and caused "quality string length ≠ sequence length" errors
/// in downstream aligners (STAR, BWA, etc.).
#[ignore] // requires network on first run; cached thereafter
#[test]
fn run_fastq_illumina_quality_bytes_valid() {
    let sra_path = ensure_srr10971381();
    let tmp = tempfile::tempdir().unwrap();
    let config = test_config(tmp.path(), SplitMode::Split3, false);

    let stats = sracha_core::pipeline::run_fastq(&sra_path, Some("SRR10971381"), &config).unwrap();

    assert!(stats.spots_read > 0, "should read at least one spot");
    assert!(stats.reads_written > 0, "should write at least one read");

    // Verify every quality byte in every output file is valid Phred+33 ASCII.
    for path in &stats.output_files {
        let data = std::fs::read(path).unwrap();
        assert!(!data.is_empty(), "{} should not be empty", path.display());
        let label = path.file_name().unwrap().to_string_lossy();
        assert_quality_bytes_valid(&data, &label);
    }
}

/// Cross-validate sracha quality output against fasterq-dump for the same
/// SRA file.
///
/// For each paired read, the decoded sequence and per-base quality values must
/// be identical between the two tools. This test is skipped automatically if
/// `fasterq-dump` is not on PATH (e.g. in CI without sra-tools installed).
#[ignore] // requires network + fasterq-dump
#[test]
fn run_fastq_cross_validate_fasterq_dump() {
    // Skip if fasterq-dump is not available.
    let fasterq = match which_fasterq_dump() {
        Some(p) => p,
        None => {
            eprintln!("skipping cross-validation: fasterq-dump not found on PATH");
            return;
        }
    };

    let sra_path = ensure_srr10971381();
    let ref_dir = tempfile::tempdir().unwrap();
    let sracha_dir = tempfile::tempdir().unwrap();

    // --- Reference: fasterq-dump ---
    let status = std::process::Command::new(&fasterq)
        .args([
            "--split-3",
            "--outdir",
            ref_dir.path().to_str().unwrap(),
            sra_path.to_str().unwrap(),
        ])
        .status()
        .expect("failed to run fasterq-dump");
    assert!(status.success(), "fasterq-dump exited with {status}");

    // --- sracha ---
    let config = test_config(sracha_dir.path(), SplitMode::Split3, false);
    sracha_core::pipeline::run_fastq(&sra_path, Some("SRR10971381"), &config).unwrap();

    // --- Compare _1 files ---
    for suffix in &["_1.fastq", "_2.fastq"] {
        let ref_path = ref_dir.path().join(format!("SRR10971381{suffix}"));
        let sracha_path = sracha_dir.path().join(format!("SRR10971381{suffix}"));

        if !ref_path.exists() || !sracha_path.exists() {
            // One or both files absent (e.g. single-end data) -- skip.
            continue;
        }

        let ref_data = std::fs::read(&ref_path).unwrap();
        let sracha_data = std::fs::read(&sracha_path).unwrap();

        let ref_records = parse_fastq_records(&ref_data);
        let sracha_records = parse_fastq_records(&sracha_data);

        assert_eq!(
            ref_records.len(),
            sracha_records.len(),
            "{suffix}: record count mismatch: fasterq-dump={}, sracha={}",
            ref_records.len(),
            sracha_records.len(),
        );

        for (i, ((ref_id, ref_seq, ref_qual), (sra_id, sra_seq, sra_qual))) in
            ref_records.iter().zip(sracha_records.iter()).enumerate()
        {
            assert_eq!(
                ref_id, sra_id,
                "{suffix}: record {i}: spot ID mismatch ({ref_id} vs {sra_id})"
            );
            assert_eq!(
                ref_seq, sra_seq,
                "{suffix}: record {i} (spot {ref_id}): sequence mismatch"
            );
            assert_eq!(
                ref_qual.len(),
                sra_qual.len(),
                "{suffix}: record {i} (spot {ref_id}): quality length mismatch \
                 (fasterq-dump={}, sracha={})",
                ref_qual.len(),
                sra_qual.len(),
            );
            assert_eq!(
                ref_qual, sra_qual,
                "{suffix}: record {i} (spot {ref_id}): quality bytes mismatch"
            );
        }
    }
}

/// Find the `fasterq-dump` binary on PATH, returning its path if found.
fn which_fasterq_dump() -> Option<std::path::PathBuf> {
    std::env::var_os("PATH").and_then(|path_var| {
        std::env::split_paths(&path_var).find_map(|dir| {
            let candidate = dir.join("fasterq-dump");
            if candidate.is_file() {
                Some(candidate)
            } else {
                None
            }
        })
    })
}

// ---------------------------------------------------------------------------
// Tests that do NOT require network
// ---------------------------------------------------------------------------

#[test]
fn run_fastq_nonexistent_file() {
    let tmp = tempfile::tempdir().unwrap();
    let config = test_config(tmp.path(), SplitMode::SplitSpot, false);
    let result =
        sracha_core::pipeline::run_fastq(std::path::Path::new("/nonexistent.sra"), None, &config);
    assert!(result.is_err());
}

#[test]
fn run_fastq_corrupt_file() {
    let tmp = tempfile::tempdir().unwrap();
    let corrupt_path = tmp.path().join("corrupt.sra");
    std::fs::write(&corrupt_path, b"this is not a valid SRA file").unwrap();

    let config = test_config(tmp.path(), SplitMode::SplitSpot, false);
    let result = sracha_core::pipeline::run_fastq(&corrupt_path, None, &config);
    assert!(result.is_err());
}
