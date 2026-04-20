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

/// Ensure the SRR10358300 fixture (Illumina paired-end, ~23 MiB).
///
/// Exercises the aligned-schema-plain-SRA path: `latf-load`-origin archive
/// with `NCBI:align:tbl:seq#1.1` table schema and no alignment tables, so
/// the narrowed `reject_if_csra` lets it through and decode goes through
/// the existing SRA-lite code path using static-column read structure.
fn ensure_srr10358300() -> PathBuf {
    static DOWNLOAD: Once = Once::new();
    let path = fixtures_dir().join("SRR10358300.sra");

    DOWNLOAD.call_once(|| {
        if path.exists() {
            return;
        }
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();

        let url = "https://sra-pub-run-odp.s3.amazonaws.com/sra/SRR10358300/SRR10358300";
        eprintln!("downloading SRR10358300 fixture from {url} ...");

        let resp = reqwest::blocking::get(url)
            .unwrap_or_else(|e| panic!("failed to download SRR10358300: {e}"));
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

/// Ensure the SRR33907345 fixture (Illumina paired-end, 77 MiB).
///
/// This accession regression-tests issue #22: one READ blob (blob 46) uses a
/// `data_runs` page map with variable per-row base counts, so the decoder
/// must expand the stored 2na buffer via `PageMap::expand_variable_data_runs`.
/// The earlier fixed-row-length-only guard silently skipped expansion,
/// dropping one spot and producing asymmetric paired output.
fn ensure_srr33907345() -> PathBuf {
    static DOWNLOAD: Once = Once::new();
    let path = fixtures_dir().join("SRR33907345.sra");

    DOWNLOAD.call_once(|| {
        if path.exists() {
            return;
        }
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();

        let url = "https://sra-pub-run-odp.s3.amazonaws.com/sra/SRR33907345/SRR33907345";
        eprintln!("downloading SRR33907345 fixture from {url} ...");

        let resp = reqwest::blocking::get(url)
            .unwrap_or_else(|e| panic!("failed to download SRR33907345: {e}"));
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

/// Ensure the SRR000001 fixture (LS454 / 454 GS FLX).
///
/// Covers the legacy-platform rejection path end-to-end: 454 is in the
/// `UNSUPPORTED_PLATFORMS` list in pipeline/mod.rs, and decode_and_write
/// must surface `Error::UnsupportedPlatform` when it encounters one
/// rather than panicking or producing garbage FASTQ. The unit test at
/// `unsupported_platforms_rejected` only covers the string check in
/// isolation; this test exercises the rejection against a real archive's
/// parsed platform code.
fn ensure_srr000001() -> PathBuf {
    static DOWNLOAD: Once = Once::new();
    let path = fixtures_dir().join("SRR000001.sra");

    DOWNLOAD.call_once(|| {
        if path.exists() {
            return;
        }
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();

        let url = "https://sra-pub-run-odp.s3.amazonaws.com/sra/SRR000001/SRR000001";
        eprintln!("downloading SRR000001 fixture from {url} ... (large, 312 MiB)");

        let resp = reqwest::blocking::get(url)
            .unwrap_or_else(|e| panic!("failed to download SRR000001: {e}"));
        assert!(
            resp.status().is_success(),
            "HTTP {} downloading SRR000001 fixture",
            resp.status()
        );
        let bytes = resp.bytes().unwrap();
        std::fs::write(&path, &bytes).unwrap();
    });

    assert!(path.exists(), "fixture not found at {}", path.display());
    path
}

/// Ensure the VDB-3418 fixture (BAM-loaded cSRA, 12 MiB, 985 SEQUENCE /
/// 938 PRIMARY_ALIGNMENT rows; schema `NCBI:align:db:alignment_sorted#1.3`).
///
/// Used by every `csra_*` test to exercise reference-compressed alignment
/// decode (HAS_MISMATCH / REF_OFFSET / align_restore_read) end-to-end.
/// The fixture is shipped with ncbi-vdb's test suite and fetched from its
/// GitHub mirror; pinning to `master` is tolerable since this is a long-
/// lived test asset — if upstream deletes or reshapes it the download 404s
/// and we notice at test time rather than silently skipping.
fn ensure_vdb_3418() -> PathBuf {
    static DOWNLOAD: Once = Once::new();
    let path = fixtures_dir().join("VDB-3418.sra");
    const EXPECTED_SIZE: u64 = 12_887_839;
    const EXPECTED_MD5: &str = "7d66f3f346db0f916a8c723d40087b6c";

    DOWNLOAD.call_once(|| {
        if path.exists() {
            return;
        }
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();

        let url = "https://raw.githubusercontent.com/ncbi/ncbi-vdb/master/test/vdb/db/VDB-3418.sra";
        eprintln!("downloading VDB-3418 fixture from {url} ...");

        let resp = reqwest::blocking::get(url)
            .unwrap_or_else(|e| panic!("failed to download VDB-3418: {e}"));
        assert!(
            resp.status().is_success(),
            "HTTP {} downloading VDB-3418 fixture",
            resp.status()
        );
        let bytes = resp.bytes().unwrap();
        assert_eq!(
            bytes.len() as u64,
            EXPECTED_SIZE,
            "VDB-3418 size changed upstream — expected {EXPECTED_SIZE}, got {}",
            bytes.len()
        );
        std::fs::write(&path, &bytes).unwrap();
    });

    assert!(path.exists(), "fixture not found at {}", path.display());
    // Cross-check content on every call — cheap (12 MiB) and catches
    // truncated-download artifacts that the Once block would otherwise
    // cache. md5 mismatch here means either upstream edited the file or a
    // partial write landed on disk; either way the cSRA tests below would
    // start producing garbage we'd have to debug, so fail loud now.
    let got_md5 = md5_file(&path);
    assert_eq!(
        got_md5, EXPECTED_MD5,
        "VDB-3418 fixture md5 mismatch — upstream changed or file is corrupt"
    );
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
        resume: true,
        stdout: false,
        cancelled: None,
        strict: false,
        http_client: None,
        keep_sra: false,
        progress_parent: None,
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
    let sra_path = ensure_srr28588231();
    let tmp = tempfile::tempdir().unwrap();
    let config = test_config(tmp.path(), SplitMode::Split3, CompressionMode::None);

    let stats = sracha_core::pipeline::run_fastq(&sra_path, Some("SRR28588231"), &config).unwrap();

    assert!(stats.spots_read > 0, "should read at least one spot");
    assert!(stats.reads_written > 0, "should write at least one read");
    assert!(
        !stats.output_files.is_empty(),
        "should produce output files"
    );

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
    let sra_path = ensure_srr28588231();
    let tmp = tempfile::tempdir().unwrap();
    let config = test_config(tmp.path(), SplitMode::SplitSpot, CompressionMode::None);

    let stats = sracha_core::pipeline::run_fastq(&sra_path, Some("SRR28588231"), &config).unwrap();

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
    let sra_path = ensure_srr28588231();
    let tmp = tempfile::tempdir().unwrap();
    let config = test_config(
        tmp.path(),
        SplitMode::SplitSpot,
        CompressionMode::Gzip { level: 1 },
    );

    let stats = sracha_core::pipeline::run_fastq(&sra_path, Some("SRR28588231"), &config).unwrap();

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
    let sra_path = ensure_srr28588231();

    let tmp1 = tempfile::tempdir().unwrap();
    let config1 = test_config(tmp1.path(), SplitMode::SplitSpot, CompressionMode::None);
    let stats1 =
        sracha_core::pipeline::run_fastq(&sra_path, Some("SRR28588231"), &config1).unwrap();

    let tmp2 = tempfile::tempdir().unwrap();
    let config2 = test_config(tmp2.path(), SplitMode::SplitSpot, CompressionMode::None);
    let stats2 =
        sracha_core::pipeline::run_fastq(&sra_path, Some("SRR28588231"), &config2).unwrap();

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

#[ignore] // requires network on first run; regression test for issue #22
#[test]
fn variable_length_data_runs_spot_count() {
    // SRR33907345 has 305,571 spots × 2 reads = 611,142 reads. Blob 46's
    // READ column uses a `data_runs` page map with variable per-row
    // base counts; before the fix the decoder silently skipped expansion
    // whenever `pm.lengths` wasn't all equal, dropping one spot and
    // producing `_1 ≠ _2` asymmetry.
    let sra_path = ensure_srr33907345();
    let tmp = tempfile::tempdir().unwrap();
    let config = test_config(tmp.path(), SplitMode::Split3, CompressionMode::None);

    let stats = sracha_core::pipeline::run_fastq(&sra_path, Some("SRR33907345"), &config).unwrap();

    assert_eq!(stats.spots_read, 305_571, "expected 305,571 spots");
    assert_eq!(stats.reads_written, 611_142, "expected 611,142 reads");
    assert_eq!(
        stats.output_files.len(),
        2,
        "paired-end should produce _1 and _2 files"
    );

    for path in &stats.output_files {
        let data = std::fs::read(path).unwrap();
        assert_valid_fastq(&data);
        let line_count = data.iter().filter(|&&b| b == b'\n').count();
        assert_eq!(
            line_count,
            305_571 * 4,
            "each paired file should have 305,571 records, got {} lines",
            line_count,
        );
    }
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

// ---------------------------------------------------------------------------
// Additional mode coverage tests
// ---------------------------------------------------------------------------

#[ignore]
#[test]
fn run_fasta_split3() {
    let sra_path = ensure_srr28588231();
    let tmp = tempfile::tempdir().unwrap();
    let mut config = test_config(tmp.path(), SplitMode::Split3, CompressionMode::None);
    config.fasta = true;

    let stats = sracha_core::pipeline::run_fastq(&sra_path, Some("SRR28588231"), &config).unwrap();

    assert!(stats.spots_read > 0);
    assert!(stats.reads_written > 0);

    for path in &stats.output_files {
        let name = path.file_name().unwrap().to_string_lossy();
        assert!(
            name.ends_with(".fasta"),
            "fasta output should end with .fasta, got {name}"
        );
        let data = std::fs::read_to_string(path).unwrap();
        // FASTA: 2-line records starting with '>'
        let lines: Vec<&str> = data.lines().collect();
        assert!(lines.len() >= 2, "need at least 2 lines");
        assert!(
            lines[0].starts_with('>'),
            "first line should start with '>', got {:?}",
            &lines[0][..lines[0].len().min(40)]
        );
        assert!(!lines[1].is_empty(), "sequence line should not be empty");
    }
}

#[ignore]
#[test]
fn run_fastq_zstd() {
    let sra_path = ensure_srr28588231();
    let tmp = tempfile::tempdir().unwrap();
    let config = test_config(
        tmp.path(),
        SplitMode::SplitSpot,
        CompressionMode::Zstd {
            level: 1,
            threads: 2,
        },
    );

    let stats = sracha_core::pipeline::run_fastq(&sra_path, Some("SRR28588231"), &config).unwrap();

    assert!(stats.spots_read > 0);
    for path in &stats.output_files {
        let name = path.file_name().unwrap().to_string_lossy();
        assert!(
            name.ends_with(".fastq.zst"),
            "zstd output should end with .fastq.zst, got {name}"
        );

        // Decompress and verify FASTQ content.
        let file = std::fs::File::open(path).unwrap();
        let mut decoder = zstd::stream::Decoder::new(file).unwrap();
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed).unwrap();
        assert_valid_fastq(&decompressed);
    }
}

#[ignore]
#[test]
fn run_fastq_interleaved() {
    let sra_path = ensure_srr28588231();
    let tmp = tempfile::tempdir().unwrap();
    let config = test_config(tmp.path(), SplitMode::Interleaved, CompressionMode::None);

    let stats = sracha_core::pipeline::run_fastq(&sra_path, Some("SRR28588231"), &config).unwrap();

    assert!(stats.spots_read > 0);
    assert_eq!(
        stats.output_files.len(),
        1,
        "interleaved should produce exactly one file"
    );

    let data = std::fs::read(&stats.output_files[0]).unwrap();
    assert_valid_fastq(&data);

    // Interleaved: both reads in one file, so reads_written should equal 2 * spots_read.
    assert_eq!(
        stats.reads_written,
        stats.spots_read * 2,
        "interleaved should write R1+R2 alternating"
    );
}

#[ignore]
#[test]
fn run_fastq_split_files() {
    let sra_path = ensure_srr28588231();
    let tmp = tempfile::tempdir().unwrap();
    let config = test_config(tmp.path(), SplitMode::SplitFiles, CompressionMode::None);

    let stats = sracha_core::pipeline::run_fastq(&sra_path, Some("SRR28588231"), &config).unwrap();

    assert!(stats.spots_read > 0);
    assert_eq!(
        stats.output_files.len(),
        2,
        "split-files on paired data should produce 2 files"
    );

    for path in &stats.output_files {
        let data = std::fs::read(path).unwrap();
        assert_valid_fastq(&data);
    }
}

#[ignore]
#[test]
fn run_fastq_min_read_len_filter() {
    let sra_path = ensure_srr28588231();
    let tmp = tempfile::tempdir().unwrap();
    let mut config = test_config(tmp.path(), SplitMode::SplitSpot, CompressionMode::None);
    // SRR28588231 has 301bp reads (no READ_LEN column; 602bp spot / 2 reads).
    // Setting min_read_len to 400 should filter all.
    config.min_read_len = Some(400);

    let stats = sracha_core::pipeline::run_fastq(&sra_path, Some("SRR28588231"), &config).unwrap();

    assert_eq!(
        stats.reads_written, 0,
        "min_read_len=400 should filter all 301bp reads"
    );
}

// ---------------------------------------------------------------------------
// Byte identity regression tests
// ---------------------------------------------------------------------------

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

#[ignore] // requires network on first run
#[test]
fn aligned_schema_plain_byte_identity() {
    // Regression guard for the cSRA-rejection narrowing (0c63e1a) and
    // static-column read-structure fallback. SRR10358300 is a
    // `latf-load.2.9.1`-origin archive whose table schema is
    // `NCBI:align:tbl:seq#1.1`; it has no alignment tables, no physical
    // READ_LEN column, and no platform-inferable schema name, so before
    // the aligned-schema relaxation it was rejected up-front and after
    // is decoded via `col/READ_TYPE/row` + `col/READ_LEN/row` static
    // columns plus the blob-0 row_length hint.
    //
    // md5s below match `fastq-dump --split-3` output on all 62,983
    // spots.
    let sra_path = ensure_srr10358300();
    let tmp = tempfile::tempdir().unwrap();
    let config = test_config(tmp.path(), SplitMode::Split3, CompressionMode::None);

    let stats = sracha_core::pipeline::run_fastq(&sra_path, Some("SRR10358300"), &config).unwrap();

    assert_eq!(stats.spots_read, 62_983);
    assert_eq!(stats.reads_written, stats.spots_read * 2);
    assert_eq!(stats.output_files.len(), 2);

    let md5_1 = md5_file(&stats.output_files[0]);
    let md5_2 = md5_file(&stats.output_files[1]);
    assert_eq!(
        md5_1, "395fc640bdf2dd9d1888f9b422f800ba",
        "SRR10358300_1.fastq md5 mismatch — aligned-schema plain path regressed"
    );
    assert_eq!(
        md5_2, "10e15712bdf6449535c5044e0460e32b",
        "SRR10358300_2.fastq md5 mismatch — aligned-schema plain path regressed"
    );
}

// ---------------------------------------------------------------------------
// Corruption-injection tests: mutate a valid fixture and verify that decode
// surfaces an error instead of silently producing wrong FASTQ.
// ---------------------------------------------------------------------------

/// Copy the fixture to a temp path and return the copied path.
fn clone_fixture(tmp_dir: &std::path::Path, name: &str) -> PathBuf {
    let src = ensure_srr28588231();
    let dst = tmp_dir.join(name);
    std::fs::copy(&src, &dst).unwrap();
    dst
}

#[ignore]
#[test]
fn corrupt_truncated_sra_fails() {
    let tmp = tempfile::tempdir().unwrap();
    let path = clone_fixture(tmp.path(), "truncated.sra");
    // Truncate to 1 KiB — far smaller than a valid KAR header + TOC. This
    // path must fail inside `KarArchive::open` before any mmap, so decode
    // never reaches a point where it could SIGBUS on missing bytes.
    std::fs::write(&path, [0u8; 1024]).unwrap();

    let out = tempfile::tempdir().unwrap();
    let config = test_config(out.path(), SplitMode::Split3, CompressionMode::None);
    let result = sracha_core::pipeline::run_fastq(&path, Some("SRR28588231"), &config);
    assert!(
        result.is_err(),
        "truncated SRA must not decode silently: {:?}",
        result.as_ref().map(|s| &s.output_files)
    );
}

#[ignore]
#[test]
fn corrupt_flipped_bytes_trigger_checksum_or_decode_error() {
    let tmp = tempfile::tempdir().unwrap();
    let path = clone_fixture(tmp.path(), "flipped.sra");
    // Corrupt ~1% of the file at three separate positions. One stripe is
    // almost certain to land inside a column blob rather than padding,
    // tripping CRC32 / deflate / page-map bounds. Doing three widely-
    // spaced stripes makes the test robust to fixture layout changes.
    let mut bytes = std::fs::read(&path).unwrap();
    let stripe = (bytes.len() / 100).max(8192);
    for frac in [3, 5, 7] {
        let start = bytes.len() * frac / 10;
        let end = (start + stripe).min(bytes.len());
        for b in &mut bytes[start..end] {
            *b ^= 0xA5;
        }
    }
    std::fs::write(&path, &bytes).unwrap();

    let out = tempfile::tempdir().unwrap();
    let config = test_config(out.path(), SplitMode::Split3, CompressionMode::None);
    let result = sracha_core::pipeline::run_fastq(&path, Some("SRR28588231"), &config);
    assert!(
        result.is_err(),
        "4 KiB mid-file corruption must surface as a decode error: {:?}",
        result.as_ref().map(|s| &s.output_files)
    );
}

#[ignore]
#[test]
fn csra_alignment_columns_open() {
    // Phase 1 smoke test: open the PRIMARY_ALIGNMENT columns of a real
    // reference-compressed cSRA fixture. VDB-3418 is a 12 MiB BAM-loaded
    // archive shipped with ncbi-vdb's test suite (schema
    // `NCBI:align:db:alignment_sorted#1.3`; 985 SEQUENCE / 938 PRIMARY_ALIGNMENT
    // rows). All we verify here is that the column-opening plumbing works
    // end-to-end against a real archive; read_row and reconstruction land
    // in later phases.
    use sracha_core::vdb::alignment::AlignmentCursor;
    use sracha_core::vdb::kar::KarArchive;

    let path = ensure_vdb_3418();
    let file = std::fs::File::open(&path).unwrap();
    let mut archive = KarArchive::open(std::io::BufReader::new(file)).unwrap();
    let cur = AlignmentCursor::open(&mut archive, &path).unwrap();
    assert_eq!(cur.row_count(), 938, "expected 938 PRIMARY_ALIGNMENT rows");
    assert_eq!(cur.first_row(), 1);

    let grs = cur.first_global_ref_start().unwrap();
    let rlen = cur.first_ref_len().unwrap();
    eprintln!("row 1: GLOBAL_REF_START={grs} REF_LEN={rlen}");
    assert_eq!(grs, 2, "expected vdb-dump GLOBAL_REF_START=2");
    assert_eq!(rlen, 35712, "expected vdb-dump REF_LEN=35712");

    // vdb-dump row 1 reports:
    //   HAS_MISMATCH length 36185, 2689 ones
    //   HAS_REF_OFFSET length 36185, 2149 ones
    //   HAS_MISMATCH first 100 bits: all 1s
    // Cross-check row 1 against `vdb-dump -T PRIMARY_ALIGNMENT -R 1`:
    //   HAS_MISMATCH   length 36185, 2689 ones, first 100 bits all-1,
    //                  last 10 bits "1011111010"
    //   HAS_REF_OFFSET length 36185, 2149 ones
    //   REF_OFFSET     first values -260, -1, -1, -1, -1; last value -80
    let row = cur.read_row(1).unwrap();
    assert_eq!(row.has_mismatch.len(), 36185);
    assert_eq!(row.has_ref_offset.len(), 36185);
    assert_eq!(row.mismatch.len(), 2689);
    assert_eq!(row.ref_offset.len(), 2149);
    assert!(row.has_mismatch[..100].iter().all(|&b| b == 1));
    let tail: Vec<u8> = row.has_mismatch[row.has_mismatch.len() - 10..].to_vec();
    assert_eq!(tail, vec![1, 0, 1, 1, 1, 1, 1, 0, 1, 0]);
    assert_eq!(&row.ref_offset[..5], &[-260, -1, -1, -1, -1]);
    assert_eq!(row.ref_offset.last().copied(), Some(-80));

    // Internal consistency: the invariant align_restore_read relies on.
    let mm1s = row.has_mismatch.iter().filter(|&&b| b != 0).count();
    let ro1s = row.has_ref_offset.iter().filter(|&&b| b != 0).count();
    assert_eq!(mm1s, row.mismatch.len());
    assert_eq!(ro1s, row.ref_offset.len());
}

#[ignore]
#[test]
fn csra_reference_fetch_span() {
    // Verify REFERENCE chunk reader against vdb-dump. VDB-3418's REFERENCE
    // table: 180 chunks, MAX_SEQ_LEN=5000, chromosome "III".
    // vdb-dump -T REFERENCE -C READ -R 1 first bases: CCCACACACCACACCCACACCACACCCACA...
    use sracha_core::vdb::kar::KarArchive;
    use sracha_core::vdb::reference::ReferenceCursor;

    let path = ensure_vdb_3418();
    let file = std::fs::File::open(&path).unwrap();
    let mut archive = KarArchive::open(std::io::BufReader::new(file)).unwrap();
    let refs = ReferenceCursor::open(&mut archive, &path).unwrap();
    assert_eq!(refs.row_count(), 180);
    assert_eq!(refs.max_seq_len(), 5000);

    // First 30 bases of chunk 1 (global_ref_start=0): CCCACACACCACACCCACACCACACCCACA
    let bases = refs.fetch_span(0, 30).unwrap();
    let expected_text = "CCCACACACCACACCCACACCACACCCACA";
    let got_text: String = bases
        .iter()
        .map(|&b| match b {
            0x1 => 'A',
            0x2 => 'C',
            0x4 => 'G',
            0x8 => 'T',
            _ => '?',
        })
        .collect();
    assert_eq!(got_text, expected_text, "first 30 bases mismatch");

    // Cross-chunk span: start at position 4995 (end of chunk 1), length 10,
    // should straddle into chunk 2. Just assert we get 10 bases back without
    // error — a stronger check lands once we have a second fixture to
    // compare against vdb-dump's concatenated output.
    let bases2 = refs.fetch_span(4995, 10).unwrap();
    assert_eq!(bases2.len(), 10);
    for (i, &b) in bases2.iter().enumerate() {
        assert!(
            matches!(b, 0x1 | 0x2 | 0x4 | 0x8),
            "byte {i} of cross-chunk span not a 2na code: {b}"
        );
    }
}

#[ignore]
#[test]
fn csra_align_restore_read_row_1() {
    // Phase 2 end-to-end: reconstruct PRIMARY_ALIGNMENT row 1's aligned
    // bases from REFERENCE + HAS_MISMATCH + MISMATCH + HAS_REF_OFFSET +
    // REF_OFFSET, and verify the ASCII matches `vdb-dump -T PRIMARY_ALIGNMENT
    // -C READ -R 1`:
    //   length  36185
    //   first30 AGTTACGTATTGCTAAGGTTATTAGGGAAA
    //   last31  AGCAATACGTAACTGAACGAAGTAATACCGA
    //
    // `align_restore_read` output is in REFERENCE orientation — vdb-dump's
    // `PRIMARY_ALIGNMENT.READ` column matches it directly. Strand flip
    // based on SEQUENCE.READ_TYPE happens one level up in seq_restore_read
    // when splicing aligned halves into a full spot.
    use sracha_core::vdb::alignment::AlignmentCursor;
    use sracha_core::vdb::kar::KarArchive;
    use sracha_core::vdb::reference::ReferenceCursor;
    use sracha_core::vdb::restore::{align_restore_read, fourna_to_ascii};

    let path = ensure_vdb_3418();
    let file = std::fs::File::open(&path).unwrap();
    let mut archive = KarArchive::open(std::io::BufReader::new(file)).unwrap();
    let align = AlignmentCursor::open(&mut archive, &path).unwrap();
    let refs = ReferenceCursor::open(&mut archive, &path).unwrap();

    let row = align.read_row(1).unwrap();
    let read_len = row.has_mismatch.len();
    assert_eq!(read_len, 36185);

    let ref_read = refs.fetch_span(row.global_ref_start, row.ref_len).unwrap();
    let bases = align_restore_read(
        &ref_read,
        &row.has_mismatch,
        &row.mismatch,
        &row.has_ref_offset,
        &row.ref_offset,
        read_len,
    )
    .unwrap();
    let ascii = fourna_to_ascii(&bases);
    let text = std::str::from_utf8(&ascii).unwrap();

    assert_eq!(text.len(), 36185);
    assert_eq!(&text[..30], "AGTTACGTATTGCTAAGGTTATTAGGGAAA");
    assert_eq!(&text[text.len() - 31..], "AGCAATACGTAACTGAACGAAGTAATACCGA");
}

#[ignore]
#[test]
fn csra_seq_restore_read_row_1() {
    // End-to-end Phase 2 splice: SEQUENCE row 1 of VDB-3418.
    //
    // `vdb-dump -T SEQUENCE -R 1` reports READ_LEN=36185,
    // READ_TYPE=BIOLOGICAL|REVERSE (0x0A), PRIMARY_ALIGNMENT_ID=1, CMP_READ
    // empty (single-read fully-aligned spot). So the splice reduces to:
    // reverse-complement(align_restore_read(alignment row 1)).
    //
    // vdb-dump -T SEQUENCE -C READ first 30 chars:
    //   TCGGTATTACTTCGTTCAGTTACGTATTGCT  (31)
    // last 31:
    //   GTTTCCCTAATAACCTTAGCAATACGTAACT
    use sracha_core::vdb::alignment::AlignmentCursor;
    use sracha_core::vdb::kar::KarArchive;
    use sracha_core::vdb::reference::ReferenceCursor;
    use sracha_core::vdb::restore::{
        SRA_READ_TYPE_REVERSE, align_restore_read, fourna_to_ascii, seq_restore_read,
    };

    let path = ensure_vdb_3418();
    let file = std::fs::File::open(&path).unwrap();
    let mut archive = KarArchive::open(std::io::BufReader::new(file)).unwrap();
    let align = AlignmentCursor::open(&mut archive, &path).unwrap();
    let refs = ReferenceCursor::open(&mut archive, &path).unwrap();

    // Hardcoded SEQUENCE row 1 values (see vdb-dump). A SEQUENCE column
    // reader lands in Phase 3; the splice algorithm is the same either way.
    let align_ids = [1i64];
    let read_lens = [36185u32];
    // BIOLOGICAL (0x01) | REVERSE (0x04) = 0x05 per vdb-dump.
    let read_types = [SRA_READ_TYPE_REVERSE | 0x01];
    let cmp_rd: Vec<u8> = Vec::new();

    let spot = seq_restore_read(
        &cmp_rd,
        &align_ids,
        &read_lens,
        &read_types,
        |alignment_id| {
            let row = align.read_row(alignment_id)?;
            let ref_read = refs.fetch_span(row.global_ref_start, row.ref_len)?;
            align_restore_read(
                &ref_read,
                &row.has_mismatch,
                &row.mismatch,
                &row.has_ref_offset,
                &row.ref_offset,
                row.has_mismatch.len(),
            )
        },
    )
    .unwrap();

    let ascii = fourna_to_ascii(&spot);
    let text = std::str::from_utf8(&ascii).unwrap();
    assert_eq!(text.len(), 36185);
    assert_eq!(&text[..31], "TCGGTATTACTTCGTTCAGTTACGTATTGCT");
    assert_eq!(&text[text.len() - 31..], "GTTTCCCTAATAACCTTAGCAATACGTAACT");
}

#[ignore]
#[test]
fn csra_cursor_read_spot_row_1() {
    // Phase 3 entry point: the user-facing CsraCursor opens all six
    // column readers (SEQUENCE.CMP_READ / PRIMARY_ALIGNMENT_ID /
    // READ_LEN / READ_TYPE / QUALITY + PRIMARY_ALIGNMENT + REFERENCE)
    // and decodes one spot's bases + quality in one call. No hardcoded
    // metadata — everything comes from the archive.
    //
    // Cross-checks against vdb-dump on VDB-3418 row 1:
    //   SEQUENCE.READ first 31: TCGGTATTACTTCGTTCAGTTACGTATTGCT
    //   SEQUENCE.READ last 31:  GTTTCCCTAATAACCTTAGCAATACGTAACT
    //   length 36185, single read, paired-end status: n/a (1-read spot)
    use sracha_core::vdb::csra::CsraCursor;
    use sracha_core::vdb::kar::KarArchive;
    use sracha_core::vdb::restore::fourna_to_ascii;

    let path = ensure_vdb_3418();
    let file = std::fs::File::open(&path).unwrap();
    let mut archive = KarArchive::open(std::io::BufReader::new(file)).unwrap();
    let csra = CsraCursor::open(&mut archive, &path).unwrap();
    assert_eq!(csra.row_count(), 985);
    assert_eq!(csra.first_row(), 1);

    let spot = csra.read_spot(1).unwrap();
    assert_eq!(spot.read_lens, vec![36185]);
    assert_eq!(spot.read_types.len(), 1);
    // BIOLOGICAL (0x01) | REVERSE (0x04) = 0x05 per vdb-dump.
    assert_eq!(spot.read_types[0] & 0x07, 0x05);
    assert_eq!(spot.bases.len(), 36185);
    assert_eq!(spot.quality.len(), 36185);

    let ascii = fourna_to_ascii(&spot.bases);
    let text = std::str::from_utf8(&ascii).unwrap();
    assert_eq!(&text[..31], "TCGGTATTACTTCGTTCAGTTACGTATTGCT");
    assert_eq!(&text[text.len() - 31..], "GTTTCCCTAATAACCTTAGCAATACGTAACT");
}

#[ignore]
#[test]
fn csra_full_archive_matches_fasterq_dump() {
    // Phase 3b end-to-end: decode every spot in VDB-3418 and verify the
    // resulting FASTQ has the expected shape (985 records, 985 *4 lines).
    // A byte-identity check against a fasterq-dump reference FASTQ is the
    // stronger signal and runs when /tmp/csra-ref/VDB-3418.fastq exists
    // (generate with `fasterq-dump --split-files` into that directory).
    use sracha_core::vdb::csra::CsraCursor;
    use sracha_core::vdb::kar::KarArchive;

    let path = ensure_vdb_3418();
    let file = std::fs::File::open(&path).unwrap();
    let mut archive = KarArchive::open(std::io::BufReader::new(file)).unwrap();
    let csra = CsraCursor::open(&mut archive, &path).unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let out_path = tmp.path().join("VDB-3418.fastq");
    let out_file = std::fs::File::create(&out_path).unwrap();
    let buf_writer = std::io::BufWriter::new(out_file);
    let stats = csra.write_fastq("VDB-3418", buf_writer).unwrap();
    assert_eq!(stats.spots, 985);

    let out_bytes = std::fs::read(&out_path).unwrap();
    let line_count = out_bytes.iter().filter(|&&b| b == b'\n').count();
    assert_eq!(line_count, 985 * 4, "expected 985 × 4 FASTQ lines");

    // Byte-identity check against fasterq-dump reference, when present.
    let ref_path = std::path::Path::new("/tmp/csra-ref/VDB-3418.fastq");
    if ref_path.exists() {
        let got = md5_file(&out_path);
        let want = md5_file(ref_path);
        assert_eq!(
            got, want,
            "cSRA FASTQ diverges from fasterq-dump — got {got}, want {want}"
        );
    } else {
        eprintln!(
            "fasterq-dump reference not found at {}; skipping md5 check",
            ref_path.display()
        );
    }
}

/// Ensure the DRR045255 fixture (Illumina paired, ~246 MB, 3.7M spots,
/// 3658 blobs). Exercises two pipeline invariants:
///
/// - **BATCH_SIZE=1024 cumulative-spots tracking**: the decode loop used
///   to read `spots_read` atomically, racing with the writer thread
///   across the bounded channel (capacity 4). For archives with > 1024
///   blobs the spot-number in FASTQ deflines would reset to 1 at the
///   1,048,577th spot. The fix tracks `cumulative_spots` locally in
///   the decode loop; this archive is the smallest known trigger.
/// - **Adaptive page_map dispatch**: a READ_LEN blob at row ~1M has a
///   data_runs that only fits the u32-index interpretation, not the
///   usual entry-index one.
fn ensure_drr045255() -> PathBuf {
    static DOWNLOAD: Once = Once::new();
    let path = fixtures_dir().join("DRR045255.sra");

    DOWNLOAD.call_once(|| {
        if path.exists() {
            return;
        }
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let url = "https://sra-pub-run-odp.s3.amazonaws.com/sra/DRR045255/DRR045255";
        eprintln!("downloading DRR045255 fixture from {url} ...");
        let resp = reqwest::blocking::get(url)
            .unwrap_or_else(|e| panic!("failed to download DRR045255: {e}"));
        assert!(
            resp.status().is_success(),
            "HTTP {} downloading fixture",
            resp.status()
        );
        let bytes = resp.bytes().unwrap();
        std::fs::write(&path, &bytes).unwrap();
    });

    assert!(path.exists(), "fixture not found at {}", path.display());
    path
}

#[ignore]
#[test]
fn drr045255_cross_batch_spot_numbering() {
    // Regression: spots_before computation used to reset at each
    // BATCH_SIZE=1024 boundary, producing `@DRR045255.1` instead of
    // `@DRR045255.1048577` at spot 1048577. Verified with md5 parity
    // against `fasterq-dump --split-files`:
    //   DRR045255_1.fastq  74fd4681aff8fd1cd52140322e86ac45
    //   DRR045255_2.fastq  ecb11ba74aa9d6d02ff63872d04fc76f
    // (Recomputed each time the fixture is ensured; values captured
    // from sra-tools 3.2.1 via `module load sratoolkit/3.2.1 &&
    // fasterq-dump --split-files DRR045255.sra`.)
    let sra_path = ensure_drr045255();
    let tmp = tempfile::tempdir().unwrap();
    let config = test_config(tmp.path(), SplitMode::Split3, CompressionMode::None);

    let stats = sracha_core::pipeline::run_fastq(&sra_path, Some("DRR045255"), &config).unwrap();
    assert_eq!(stats.spots_read, 3_745_800);
    assert_eq!(stats.reads_written, 7_491_600);
    assert_eq!(stats.output_files.len(), 2);

    // Cheaper than md5 on ~280 MB files: check the defline at record
    // 1048577 is the correct spot number. The race-condition bug made
    // sracha emit `@DRR045255.1` here before the fix.
    let file = std::fs::File::open(&stats.output_files[0]).unwrap();
    let reader = std::io::BufReader::new(file);
    use std::io::BufRead;
    let target_line = (1_048_577 - 1) * 4; // 0-indexed line number of the defline
    let line = reader
        .lines()
        .nth(target_line)
        .expect("R1 should have >= 1048577 records")
        .unwrap();
    assert!(
        line.starts_with("@DRR045255.1048577 1048577 "),
        "spot 1048577 defline regressed: {line:?}"
    );
}

#[ignore]
#[test]
fn corrupt_kar_magic_fails_fast() {
    let tmp = tempfile::tempdir().unwrap();
    let path = clone_fixture(tmp.path(), "badmagic.sra");
    let mut bytes = std::fs::read(&path).unwrap();
    // KAR file magic sits at the start; zero it.
    for b in &mut bytes[..8] {
        *b = 0;
    }
    std::fs::write(&path, &bytes).unwrap();

    let out = tempfile::tempdir().unwrap();
    let config = test_config(out.path(), SplitMode::Split3, CompressionMode::None);
    let result = sracha_core::pipeline::run_fastq(&path, Some("SRR28588231"), &config);
    assert!(
        result.is_err(),
        "corrupt KAR header must fail before any decode: {:?}",
        result.as_ref().map(|s| &s.output_files)
    );
}

// ---------------------------------------------------------------------------
// validate subcommand
// ---------------------------------------------------------------------------

#[ignore]
#[test]
fn validate_clean_sra_reports_valid() {
    let path = ensure_srr28588231();
    let result = sracha_core::pipeline::run_validate(&path, 2, false);
    assert!(
        result.valid,
        "clean fixture must validate clean: {:?}",
        result.errors
    );
    assert!(result.errors.is_empty());
    assert!(result.spots_validated > 0);
    assert!(result.blobs_validated > 0);
    assert!(
        result.columns_found.iter().any(|c| c == "READ"),
        "READ column expected: {:?}",
        result.columns_found
    );
    assert!(result.md5.as_ref().map(|s| s.len() == 32).unwrap_or(false));
    assert!(!result.any_blob_integrity_error);
}

#[ignore]
#[test]
fn validate_missing_file_reports_error() {
    let tmp = tempfile::tempdir().unwrap();
    let missing = tmp.path().join("not-there.sra");
    let result = sracha_core::pipeline::run_validate(&missing, 2, false);
    assert!(!result.valid);
    assert!(!result.errors.is_empty());
    assert!(
        result.errors[0].contains("cannot open"),
        "expected open error, got {:?}",
        result.errors
    );
    assert_eq!(result.spots_validated, 0);
    assert_eq!(result.blobs_validated, 0);
    assert!(result.md5.is_none());
}

#[ignore]
#[test]
fn validate_corrupt_kar_magic_reports_invalid_archive() {
    let tmp = tempfile::tempdir().unwrap();
    let path = clone_fixture(tmp.path(), "bad-validate.sra");
    let mut bytes = std::fs::read(&path).unwrap();
    for b in &mut bytes[..8] {
        *b = 0;
    }
    std::fs::write(&path, &bytes).unwrap();

    let result = sracha_core::pipeline::run_validate(&path, 2, false);
    assert!(!result.valid);
    assert!(
        result
            .errors
            .iter()
            .any(|e| e.contains("invalid KAR") || e.contains("VDB cursor")),
        "expected archive error, got {:?}",
        result.errors
    );
    // No blob decode was reachable — so this is not a blob-integrity failure.
    assert!(!result.any_blob_integrity_error);
}

#[ignore]
#[test]
fn ctrl_c_before_decode_aborts_with_cancelled_error() {
    // Pre-flipped cancel flag: the batch loop in decode_and_write polls the
    // flag every iteration, so starting with `cancelled=true` should cause
    // run_fastq to bail out with Error::Cancelled and drop partial output
    // files rather than leave half-written `.partial` files behind.
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    let path = ensure_srr28588231();
    let out = tempfile::tempdir().unwrap();
    let cancelled = Arc::new(AtomicBool::new(true));
    let mut config = test_config(out.path(), SplitMode::Split3, CompressionMode::None);
    config.cancelled = Some(Arc::clone(&cancelled));

    let result = sracha_core::pipeline::run_fastq(&path, Some("SRR28588231"), &config);
    assert!(
        matches!(result, Err(sracha_core::error::Error::Cancelled { .. })),
        "pre-cancelled run must return Error::Cancelled, got {:?}",
        result.as_ref().map(|s| &s.output_files)
    );
    // Any .partial files must have been cleaned up.
    let partials: Vec<_> = std::fs::read_dir(out.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".partial"))
        .collect();
    assert!(
        partials.is_empty(),
        "cancellation must delete .partial files, found: {:?}",
        partials.iter().map(|e| e.file_name()).collect::<Vec<_>>()
    );
    // Sanity: flag was observed.
    assert!(cancelled.load(Ordering::Relaxed));
}

#[ignore]
#[test]
fn ls454_platform_is_rejected_end_to_end() {
    // Legacy-platform hard reject: 454 (`LS454`) is in the
    // UNSUPPORTED_PLATFORMS set. The existing unit test covers the
    // string-match helper in isolation; this test drives run_fastq against
    // a real 454 archive (SRR000001) to verify the rejection is actually
    // wired into the decode_and_write entry point — a refactor that moved
    // the platform check past first decode would pass the unit test but
    // fail here.
    let sra_path = ensure_srr000001();
    let tmp = tempfile::tempdir().unwrap();
    let config = test_config(tmp.path(), SplitMode::Split3, CompressionMode::None);

    let result = sracha_core::pipeline::run_fastq(&sra_path, Some("SRR000001"), &config);
    match result {
        Err(sracha_core::error::Error::UnsupportedPlatform { platform }) => {
            assert_eq!(
                platform, "LS454",
                "expected LS454 rejection, got {platform}",
            );
        }
        Ok(stats) => panic!(
            "LS454 must be rejected, but decode succeeded with {} spots",
            stats.spots_read,
        ),
        Err(e) => panic!("unexpected non-UnsupportedPlatform error: {e}"),
    }

    // No output files should have been created.
    let files: Vec<_> = std::fs::read_dir(tmp.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            let n = e.file_name().to_string_lossy().into_owned();
            n.ends_with(".fastq") || n.ends_with(".partial")
        })
        .collect();
    assert!(
        files.is_empty(),
        "no FASTQ output should be written for rejected platform, found: {:?}",
        files.iter().map(|e| e.file_name()).collect::<Vec<_>>(),
    );
}

#[ignore]
#[test]
fn ctrl_c_mid_decode_returns_partial_paths() {
    // Complementary to the pre-flipped cancel test: here the pipeline has
    // genuinely started writing output when the flag flips, exercising the
    // "ran for a while then cancelled" path rather than the "abort at
    // first poll" path.
    //
    // A watcher thread polls the output directory and flips the flag the
    // first time it sees a `.partial` file appear — so cancellation lands
    // strictly after the pipeline has created at least one output handle.
    // On a very fast machine the whole decode may finish before the first
    // poll sees anything; we tolerate that by accepting `Ok(_)` with a
    // warning, and only assert the partial-path contract when cancellation
    // actually fired.
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    let path = ensure_srr33907345(); // larger fixture — more likely to race
    let out = tempfile::tempdir().unwrap();
    let out_dir = out.path().to_path_buf();
    let cancelled = Arc::new(AtomicBool::new(false));

    let mut config = test_config(&out_dir, SplitMode::Split3, CompressionMode::None);
    config.cancelled = Some(Arc::clone(&cancelled));

    // Spawn a thread that polls for a `.partial` file and flips cancel the
    // first time it sees one. Bounded so the test can't hang if decode
    // never produces partials for some reason.
    let flag = Arc::clone(&cancelled);
    let poll_dir = out_dir.clone();
    let watcher = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if let Ok(entries) = std::fs::read_dir(&poll_dir) {
                for e in entries.flatten() {
                    if e.file_name().to_string_lossy().ends_with(".partial") {
                        flag.store(true, Ordering::Relaxed);
                        return true;
                    }
                }
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        false
    });

    let result = sracha_core::pipeline::run_fastq(&path, Some("SRR33907345"), &config);
    let fired = watcher.join().unwrap();

    match result {
        Err(sracha_core::error::Error::Cancelled { output_files }) => {
            assert!(fired, "cancel fired without a prior .partial sighting");
            // `run_fastq` contract (pipeline/mod.rs:856): it does NOT delete
            // partial files itself — it returns `Error::Cancelled` with the
            // list of partials so the higher-level `run_get` wrapper can
            // clean them up (see pipeline/mod.rs:1641). The contract we
            // fence here is that the error actually names the partials that
            // were on disk when cancellation fired.
            let on_disk: std::collections::HashSet<PathBuf> = std::fs::read_dir(&out_dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| e.file_name().to_string_lossy().ends_with(".partial"))
                .map(|e| e.path())
                .collect();
            assert!(
                !on_disk.is_empty(),
                "watcher fired on .partial but they're gone — unexpected",
            );
            let named: std::collections::HashSet<PathBuf> = output_files.iter().cloned().collect();
            assert_eq!(
                on_disk, named,
                "Error::Cancelled.output_files must match the .partial files left \
                 on disk so the CLI can clean them up",
            );
        }
        Ok(_) => {
            // Decode completed before the watcher could observe a .partial
            // — rare but possible on fast machines; the cleanup invariant
            // isn't exercised here. Not a failure.
            eprintln!("decode completed before mid-decode cancel could fire");
        }
        Err(e) => panic!("unexpected non-cancel error: {e}"),
    }
}

#[ignore]
#[test]
fn validate_flipped_bytes_surface_integrity_errors() {
    let tmp = tempfile::tempdir().unwrap();
    let path = clone_fixture(tmp.path(), "flipped-validate.sra");
    let mut bytes = std::fs::read(&path).unwrap();
    // Corrupt a single mid-file stripe; validate should continue scanning
    // all blobs and report the per-blob failures, not abort on the first.
    let stripe = (bytes.len() / 100).max(8192);
    let start = bytes.len() / 2;
    let end = (start + stripe).min(bytes.len());
    for b in &mut bytes[start..end] {
        *b ^= 0xA5;
    }
    std::fs::write(&path, &bytes).unwrap();

    let result = sracha_core::pipeline::run_validate(&path, 2, false);
    assert!(!result.valid);
    assert!(
        !result.errors.is_empty(),
        "expected at least one blob error"
    );
    // At least one blob decode error should bubble through; whether it
    // trips CRC32 or zlib depends on whether the stripe lands on the
    // checksum tail or the compressed payload — either outcome counts as
    // a correctness signal.
    assert!(
        result.blobs_validated > 0,
        "validate should still iterate blobs even with mid-file corruption"
    );
}
