//! CLI black-box tests for the `sracha` binary.
//!
//! Fills the audit gap: the unit tests in `main.rs` only cover helper
//! functions (argument collection, confirmation, disk-space preflight).
//! Here we actually exec the built binary to verify exit codes, argument
//! parsing, flag conflicts, and end-to-end FASTQ output.
//!
//! Tests that need a real SRA file are marked `#[ignore]` and rely on the
//! cached `SRR28588231.sra` fixture from `sracha-core`'s integration tests.
//! Run with `cargo test -p sracha -- --ignored` once the fixture exists.

use std::path::PathBuf;

use assert_cmd::Command;
use predicates::prelude::*;

fn bin() -> Command {
    Command::cargo_bin("sracha").expect("sracha binary must build")
}

/// Path to the shared fixtures directory under sracha-core (where
/// `pipeline.rs` integration tests download SRR28588231.sra on first run).
fn shared_fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("sracha-core")
        .join("tests")
        .join("fixtures")
        .join(name)
}

// ---------------------------------------------------------------------------
// Parsing / help
// ---------------------------------------------------------------------------

#[test]
fn prints_top_level_help() {
    bin()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("fastq"))
        .stdout(predicate::str::contains("fetch"))
        .stdout(predicate::str::contains("get"))
        .stdout(predicate::str::contains("validate"));
}

#[test]
fn prints_subcommand_help() {
    bin()
        .args(["fastq", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("--split"))
        .stdout(predicate::str::contains("--stdout"));
}

#[test]
fn errors_on_missing_subcommand() {
    bin().assert().failure();
}

// ---------------------------------------------------------------------------
// Flag conflicts — clap declares these, so breaking the #[arg(conflicts_with_all)]
// annotations would silently let them through. These tests fence the conflict
// surface so the next person to touch FastqArgs doesn't regress it.
// ---------------------------------------------------------------------------

#[test]
fn stdout_conflicts_with_zstd() {
    bin()
        .args(["fastq", "--stdout", "--zstd", "dummy.sra"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("cannot be used with"));
}

#[test]
fn stdout_conflicts_with_gzip_level() {
    bin()
        .args(["fastq", "--stdout", "--gzip-level", "6", "dummy.sra"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("cannot be used with"));
}

#[test]
fn no_gzip_conflicts_with_zstd() {
    bin()
        .args(["fastq", "--no-gzip", "--zstd", "dummy.sra"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("cannot be used with"));
}

#[test]
fn zstd_level_out_of_range_rejected() {
    // value_parser range(1..=22) — 99 must fail at parse time.
    bin()
        .args(["fastq", "--zstd", "--zstd-level", "99", "dummy.sra"])
        .assert()
        .failure();
}

// ---------------------------------------------------------------------------
// Error handling on missing input
// ---------------------------------------------------------------------------

#[test]
fn fastq_missing_file_reports_error() {
    // Current behaviour: `sracha fastq <missing>` prints an error to
    // stderr but exits 0 (multi-file input continues on per-file
    // failures). This test fences the error-reporting contract — a future
    // refactor that silently swallowed the missing input would break it.
    // Exit-code semantics are left deliberately unasserted; if that policy
    // changes, add the `.failure()` expectation alongside.
    let tmp = tempfile::tempdir().unwrap();
    bin()
        .args(["fastq", "/definitely-does-not-exist-sracha-test.sra", "-O"])
        .arg(tmp.path())
        .args(["--no-gzip", "--no-progress"])
        .assert()
        .stderr(predicate::str::contains("file not found"));
}

// ---------------------------------------------------------------------------
// End-to-end: convert a cached fixture (requires the sracha-core fixture
// to be present; skip when absent so CI without the fixture passes).
// ---------------------------------------------------------------------------

#[test]
#[ignore] // requires SRR28588231.sra fixture from sracha-core integration tests
fn fastq_end_to_end_produces_paired_files() {
    let fixture = shared_fixture("SRR28588231.sra");
    if !fixture.exists() {
        eprintln!(
            "skipping: fixture not present at {}; run sracha-core integration tests first",
            fixture.display()
        );
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    bin()
        .args(["fastq"])
        .arg(&fixture)
        .args(["-O"])
        .arg(tmp.path())
        .args(["--no-gzip", "--no-progress", "--force"])
        .assert()
        .success();

    // Split-3 default on paired-end data produces SRR28588231_1.fastq and _2.fastq.
    let r1 = tmp.path().join("SRR28588231_1.fastq");
    let r2 = tmp.path().join("SRR28588231_2.fastq");
    assert!(r1.exists(), "R1 missing at {}", r1.display());
    assert!(r2.exists(), "R2 missing at {}", r2.display());

    let r1_bytes = std::fs::read(&r1).unwrap();
    assert!(!r1_bytes.is_empty(), "R1 empty");
    assert!(
        r1_bytes.starts_with(b"@SRR28588231"),
        "R1 defline unexpected"
    );
}

#[test]
#[ignore] // requires SRR28588231.sra fixture
fn fastq_stdout_mode_streams_fastq() {
    // Covers the stdout-mode code path end-to-end (PipelineConfig.stdout =
    // true). No cleaner way to test this in-process: the pipeline calls
    // `std::io::stdout()` directly, so a subprocess + captured stdout is
    // the only faithful harness.
    let fixture = shared_fixture("SRR28588231.sra");
    if !fixture.exists() {
        eprintln!("skipping: fixture not present at {}", fixture.display());
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let assert = bin()
        .args(["fastq", "--stdout"])
        .arg(&fixture)
        .args(["-O"])
        .arg(tmp.path())
        .args(["--no-progress"])
        .assert()
        .success();

    // Stdout mode produces a single interleaved stream of FASTQ records.
    // We check only the first few bytes to avoid pulling the full 66k-spot
    // output into the test harness.
    let out = assert.get_output().stdout.clone();
    assert!(!out.is_empty(), "stdout mode produced no output");
    assert!(
        out.starts_with(b"@SRR28588231"),
        "stdout FASTQ should start with '@SRR28588231' defline, got: {:?}",
        &out[..out.len().min(80)],
    );
    // Quick structural check: at least one newline-separated record exists.
    let lines: Vec<&[u8]> = out.split(|&b| b == b'\n').take(4).collect();
    assert!(lines.len() >= 4, "expected ≥4 lines in stdout stream");
    assert!(
        lines[2].starts_with(b"+"),
        "line 3 of first record should be '+', got {:?}",
        std::str::from_utf8(lines[2]).unwrap_or("<non-utf8>"),
    );

    // In stdout mode no output files should land in -O.
    let files: Vec<_> = std::fs::read_dir(tmp.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name())
        .collect();
    let fastqs: Vec<_> = files
        .iter()
        .filter(|f| f.to_string_lossy().ends_with(".fastq"))
        .collect();
    assert!(
        fastqs.is_empty(),
        "stdout mode should not create files in -O, found: {fastqs:?}",
    );
}

#[test]
fn fastq_corrupt_input_reports_error() {
    // Garbage content at a valid path — the CLI must at least surface the
    // decode error on stderr. Exit-code policy (see
    // `fastq_missing_file_reports_error`) is intentionally not asserted:
    // whether garbage input should terminate the process or just log an
    // error and move on to the next input is a policy decision, but a
    // silent success here would always be wrong.
    let tmp = tempfile::tempdir().unwrap();
    let corrupt = tmp.path().join("corrupt.sra");
    std::fs::write(&corrupt, b"not a valid VDB archive").unwrap();

    bin()
        .args(["fastq"])
        .arg(&corrupt)
        .args(["-O"])
        .arg(tmp.path())
        .args(["--no-gzip", "--no-progress"])
        .assert()
        .stderr(predicate::str::contains("error").or(predicate::str::contains("Error")));
}
