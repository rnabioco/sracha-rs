//! Integration tests for the generic VDB inspector against a cached fixture.
//!
//! Marked `#[ignore]` because they need network on first run (to download
//! the fixture) and a ~23 MiB file on disk. Run with:
//!
//! ```bash
//! cargo test -p sracha-vdb -- --ignored
//! ```

use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::Once;

use sracha_vdb::inspect::{self, VdbKind};
use sracha_vdb::kar::KarArchive;
use sracha_vdb::metadata;

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// Ensure the SRR28588231 fixture (Illumina paired-end, 23 MiB).
///
/// Mirrors `pipeline.rs::ensure_srr28588231` so this test binary is
/// self-sufficient — cargo does not order separate test binaries, so we
/// cannot rely on another binary having downloaded the fixture first.
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

#[test]
#[ignore]
fn srr28588231_info_smoke() {
    let path = ensure_srr28588231();
    let f = File::open(&path).unwrap();
    let mut kar = KarArchive::open(BufReader::new(f)).unwrap();

    let kind = inspect::detect_kind(&kar).unwrap();
    assert!(matches!(kind, VdbKind::Database | VdbKind::Table));

    let cols = inspect::list_columns(&kar, None).unwrap();
    assert!(cols.iter().any(|c| c == "READ"), "READ column missing");

    let report = inspect::gather_info(&mut kar, &path).unwrap();
    assert!(report.primary_row_count().unwrap() > 0);

    if let Some(nodes) = inspect::read_table_metadata(&mut kar, None) {
        let _ = metadata::schema_text(&nodes);
        let _ = metadata::load_timestamp(&nodes);
    }
}

#[test]
#[ignore]
fn srr28588231_id_range_matches_row_count() {
    let path = ensure_srr28588231();
    let f = File::open(&path).unwrap();
    let mut kar = KarArchive::open(BufReader::new(f)).unwrap();
    let (first, count) = inspect::id_range(&mut kar, &path, None, None).unwrap();
    assert!(count > 0);
    assert_eq!(first, 1, "row IDs are 1-indexed");
}
