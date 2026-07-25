//! End-to-end checks for remote `vdb` inspection over HTTP range requests.
//!
//! The point of the feature is that inspecting an archive should cost
//! kilobytes, not the archive's size, and should give the same answers as
//! inspecting a local copy. Both are asserted here against a real SRA
//! fixture served over a loopback HTTP server.
//!
//! Marked `#[ignore]` because the fixture is downloaded from NCBI on first
//! run. Run with:
//!
//! ```bash
//! cargo nextest run -p sracha-core --run-ignored=ignored-only -E 'test(remote_vdb)'
//! ```

use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Once;

use sracha_core::remote::{HttpRangeReader, TransferStats};
use sracha_core::vdb::dump::{self, DumpFormat, DumpSpec};
use sracha_core::vdb::inspect;
use sracha_core::vdb::kar::KarArchive;
use sracha_core::vdb::kdb::ColumnData;
use sracha_core::vdb::row_range::RowRanges;

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// Ensure the SRR28588231 fixture (Illumina paired-end SRA-Lite, ~23 MiB).
/// Mirrors `pipeline.rs::ensure_srr28588231` — cargo does not order test
/// binaries, so each one fetches for itself.
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
        assert!(resp.status().is_success(), "HTTP {}", resp.status());
        std::fs::write(&path, resp.bytes().unwrap()).unwrap();
    });

    assert!(path.exists(), "fixture not found at {}", path.display());
    path
}

// ---------------------------------------------------------------------------
// A loopback HTTP server that serves one file with byte-range support
// ---------------------------------------------------------------------------

struct Server {
    url: String,
    addr: std::net::SocketAddr,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl Drop for Server {
    fn drop(&mut self) {
        // The accept loop is single-threaded and serves one request per
        // connection, so the stop signal has to be its own late connection
        // rather than a socket held open for the server's lifetime.
        if let Ok(mut s) = TcpStream::connect(self.addr) {
            let _ = s.write_all(b"SHUTDOWN / HTTP/1.1\r\n\r\n");
        }
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

fn serve(path: &Path) -> Server {
    let file = Arc::new(File::open(path).unwrap());
    let len = file.metadata().unwrap().len();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let handle = std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { break };
            if !handle_one(stream, &file, len) {
                break;
            }
        }
    });

    Server {
        url: format!("http://{addr}/SRR28588231"),
        addr,
        handle: Some(handle),
    }
}

/// Returns false once the client asks the server to stop.
fn handle_one(mut stream: TcpStream, file: &File, len: u64) -> bool {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut request_line = String::new();
    if reader.read_line(&mut request_line).unwrap_or(0) == 0 {
        return true;
    }

    let mut range = None;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
            break;
        }
        if let Some(v) = line.to_ascii_lowercase().strip_prefix("range: bytes=") {
            let v = v.trim().to_string();
            let (a, b) = v.split_once('-').unwrap();
            range = Some((
                a.parse::<u64>().unwrap(),
                b.parse::<u64>().unwrap_or(len - 1),
            ));
        }
    }

    if request_line.starts_with("SHUTDOWN") {
        return false;
    }

    if request_line.starts_with("HEAD") {
        let head = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {len}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n"
        );
        let _ = stream.write_all(head.as_bytes());
        return true;
    }

    let (start, end) = range.unwrap_or((0, len - 1));
    let end = end.min(len - 1);
    let mut buf = vec![0u8; (end - start + 1) as usize];
    file.read_exact_at(&mut buf, start).unwrap();
    let head = format!(
        "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\n\
         Content-Range: bytes {start}-{end}/{len}\r\nConnection: close\r\n\r\n",
        buf.len()
    );
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(&buf);
    true
}

fn open_remote(url: &str) -> (KarArchive<HttpRangeReader>, TransferStats) {
    let stats = TransferStats::default();
    let reader = HttpRangeReader::with_stats(url, stats.clone()).unwrap();
    (KarArchive::open(reader).unwrap(), stats)
}

fn open_local(path: &Path) -> KarArchive<BufReader<File>> {
    KarArchive::open(BufReader::new(File::open(path).unwrap())).unwrap()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
#[ignore] // requires network on first run; cached thereafter
fn remote_vdb_info_matches_local_and_stays_small() {
    let path = ensure_srr28588231();
    let archive_size = std::fs::metadata(&path).unwrap().len();
    let server = serve(&path);

    let mut local = open_local(&path);
    let expected = inspect::gather_info(&mut local, ColumnData::Local(&path)).unwrap();

    let (mut remote, stats) = open_remote(&server.url);
    let actual = inspect::gather_info(&mut remote, ColumnData::Ranged).unwrap();

    assert_eq!(actual.kind, expected.kind);
    assert_eq!(actual.schema_name, expected.schema_name);
    assert_eq!(actual.platform, expected.platform);
    assert_eq!(actual.tables, expected.tables);
    assert!(
        expected.primary_row_count().unwrap() > 0,
        "fixture should have rows"
    );

    // The regression this guards: any change that reintroduces whole-file
    // reads turns a ~100 KiB inspection back into a 23 MiB download.
    let fetched = stats.bytes_fetched();
    assert!(
        fetched < archive_size / 20,
        "vdb info transferred {fetched} bytes of a {archive_size}-byte archive; \
         metadata-only commands must stay a small fraction of the file"
    );
    eprintln!("vdb info: {fetched} bytes in {} requests", stats.requests());
}

#[test]
#[ignore] // requires network on first run; cached thereafter
fn remote_vdb_id_range_matches_local() {
    let path = ensure_srr28588231();
    let server = serve(&path);

    let mut local = open_local(&path);
    let expected = inspect::id_range(&mut local, ColumnData::Local(&path), None, None).unwrap();

    let (mut remote, stats) = open_remote(&server.url);
    let actual = inspect::id_range(&mut remote, ColumnData::Ranged, None, None).unwrap();

    assert_eq!(actual, expected);
    eprintln!(
        "vdb id-range: {} bytes in {} requests",
        stats.bytes_fetched(),
        stats.requests()
    );
}

#[test]
#[ignore] // requires network on first run; cached thereafter
fn remote_vdb_dump_matches_local_for_a_row_range() {
    let path = ensure_srr28588231();
    let archive_size = std::fs::metadata(&path).unwrap().len();
    let server = serve(&path);

    // SRR28588231 is SRA-Lite (no physical QUALITY column), so dump the
    // columns that are actually on disk.
    let spec = || DumpSpec {
        columns: vec!["READ".into(), "X".into(), "Y".into()],
        exclude: Vec::new(),
        rows: RowRanges::parse("1-5").unwrap(),
        format: DumpFormat::Json,
        raw: false,
    };

    let mut local = open_local(&path);
    let expected = dump::dump_to_vec(&mut local, ColumnData::Local(&path), None, spec()).unwrap();

    let (mut remote, stats) = open_remote(&server.url);
    let actual = dump::dump_to_vec(&mut remote, ColumnData::Ranged, None, spec()).unwrap();

    assert_eq!(
        std::str::from_utf8(&actual).unwrap(),
        std::str::from_utf8(&expected).unwrap(),
        "remote dump output must be identical to the local one"
    );

    let fetched = stats.bytes_fetched();
    assert!(
        fetched < archive_size / 4,
        "dumping 5 rows transferred {fetched} bytes of a {archive_size}-byte archive"
    );
    eprintln!(
        "vdb dump -R 1-5: {fetched} bytes in {} requests",
        stats.requests()
    );
}
