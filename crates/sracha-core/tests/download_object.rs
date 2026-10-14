//! `download_sra_object` and `download_sra` against an in-process wiremock
//! server: cache hit, resume, corrupt-cache rejection, and the `.vdbcache`
//! companion that split-archive cSRA needs.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use sracha_core::download::DownloadConfig;
use sracha_core::fastq::CompressionMode;
use sracha_core::pipeline::{PipelineConfig, download_sra, download_sra_object, vdbcache_path_for};
use sracha_core::sdl::{ResolvedAccession, ResolvedFile, ResolvedMirror};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

const CHUNK: u64 = 8 * 1024 * 1024;

fn md5_hex(bytes: &[u8]) -> String {
    use md5::{Digest, Md5};
    Md5::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// A payload that starts with the KAR magic, as a real archive does.
fn kar_payload(len: usize, seed: usize) -> Vec<u8> {
    let mut v: Vec<u8> = (0..len).map(|i| (i * 31 + seed) as u8).collect();
    v[..8].copy_from_slice(b"NCBI.sra");
    v
}

fn dl_config() -> DownloadConfig {
    DownloadConfig {
        connections: 2,
        chunk_size: CHUNK,
        force: false,
        validate: true,
        progress: false,
        resume: true,
        auto_scale_connections: false,
        client: None,
        expected_prefix: None,
    }
}

fn file(url: String, body: &[u8], with_md5: bool) -> ResolvedFile {
    ResolvedFile {
        mirrors: vec![ResolvedMirror {
            url,
            service: "s3".into(),
        }],
        size: body.len() as u64,
        md5: with_md5.then(|| md5_hex(body)),
        is_lite: false,
    }
}

fn resolved(sra: ResolvedFile, vdbcache: Option<ResolvedFile>) -> ResolvedAccession {
    ResolvedAccession {
        accession: "SRR0000001".into(),
        sra_file: sra,
        vdbcache_file: vdbcache,
        run_info: None,
    }
}

/// Serves `Range` requests from `body`, counting them and optionally failing
/// the chunk that starts at `fail_start` (`u64::MAX` = never fail).
struct Ranged {
    body: Vec<u8>,
    fail_start: Arc<AtomicU64>,
    starts: Arc<Mutex<Vec<u64>>>,
}

impl Respond for Ranged {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let raw = request
            .headers
            .get("range")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("bytes="))
            .unwrap_or("");
        let last = self.body.len() as u64 - 1;
        let (start, end) = raw
            .split_once('-')
            .map(|(a, b)| {
                (
                    a.parse::<u64>().unwrap_or(0),
                    b.parse::<u64>().unwrap_or(last),
                )
            })
            .unwrap_or((0, last));
        let end = end.min(last);
        self.starts.lock().unwrap().push(start);
        if self.fail_start.load(Ordering::SeqCst) == start {
            return ResponseTemplate::new(503);
        }
        ResponseTemplate::new(206).set_body_bytes(self.body[start as usize..=end as usize].to_vec())
    }
}

async fn serve(server: &MockServer, route: &str, body: &[u8]) -> Arc<Mutex<Vec<u64>>> {
    Mock::given(method("HEAD"))
        .and(path(route))
        .respond_with(
            ResponseTemplate::new(200)
                .append_header("Accept-Ranges", "bytes")
                .append_header("Content-Length", body.len().to_string()),
        )
        .mount(server)
        .await;
    let starts = Arc::new(Mutex::new(Vec::new()));
    Mock::given(method("GET"))
        .and(path(route))
        .respond_with(Ranged {
            body: body.to_vec(),
            fail_start: Arc::new(AtomicU64::new(u64::MAX)),
            starts: starts.clone(),
        })
        .mount(server)
        .await;
    starts
}

#[tokio::test]
async fn cache_hit_transfers_nothing() {
    let server = MockServer::start().await;
    let body = kar_payload(1024, 3);
    let starts = serve(&server, "/a", &body).await;

    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("SRR0000001.sra");
    std::fs::write(&dest, &body).unwrap();

    let r = resolved(file(format!("{}/a", server.uri()), &body, true), None);
    let out = download_sra_object(&r, &dest, &dl_config()).await.unwrap();
    assert_eq!(out.archive.bytes_transferred, 0);
    assert!(out.vdbcache.is_none());
    assert!(starts.lock().unwrap().is_empty(), "no request on cache hit");
}

#[tokio::test]
async fn interrupted_download_resumes_and_reports_partial_bytes() {
    let size = (5 * CHUNK) as usize;
    let body = kar_payload(size, 7);
    let fail_offset = 2 * CHUNK;

    let server = MockServer::start().await;
    let fail_start = Arc::new(AtomicU64::new(fail_offset));
    let starts = Arc::new(Mutex::new(Vec::new()));
    Mock::given(method("GET"))
        .and(path("/big"))
        .respond_with(Ranged {
            body: body.clone(),
            fail_start: fail_start.clone(),
            starts: starts.clone(),
        })
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("SRR0000001.sra");
    let r = resolved(file(format!("{}/big", server.uri()), &body, true), None);

    download_sra_object(&r, &dest, &dl_config())
        .await
        .expect_err("first run hits the injected failure");

    fail_start.store(u64::MAX, Ordering::SeqCst);
    let out = download_sra_object(&r, &dest, &dl_config())
        .await
        .expect("second run resumes");
    let moved = out.archive.bytes_transferred;
    assert!(
        moved > 0 && moved < size as u64,
        "resumed run moves only the missing chunk, moved {moved} of {size}"
    );
    assert_eq!(std::fs::read(&dest).unwrap(), body);
}

#[tokio::test]
async fn corrupt_cached_file_without_md5_is_rejected() {
    let server = MockServer::start().await;
    let body = kar_payload(2048, 11);
    serve(&server, "/c", &body).await;

    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("SRR0000001.sra");
    // Right size, wrong magic: what a crashed earlier run could leave behind.
    std::fs::write(&dest, vec![0u8; body.len()]).unwrap();

    let r = resolved(file(format!("{}/c", server.uri()), &body, false), None);
    let out = download_sra_object(&r, &dest, &dl_config()).await.unwrap();
    assert!(
        out.archive.bytes_transferred > 0,
        "corrupt cache must be re-downloaded, not trusted"
    );
    assert_eq!(std::fs::read(&dest).unwrap(), body);
}

#[tokio::test]
async fn vdbcache_is_downloaded_beside_the_archive() {
    let server = MockServer::start().await;
    let sra = kar_payload(4096, 1);
    let vdb = kar_payload(2048, 2);
    serve(&server, "/sra", &sra).await;
    serve(&server, "/vdb", &vdb).await;

    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("SRR0000001.sra");
    let r = resolved(
        file(format!("{}/sra", server.uri()), &sra, true),
        Some(file(format!("{}/vdb", server.uri()), &vdb, true)),
    );
    let out = download_sra_object(&r, &dest, &dl_config()).await.unwrap();
    let cache = out.vdbcache.expect("vdbcache downloaded");
    assert_eq!(cache.path, vdbcache_path_for(&dest));
    assert_eq!(std::fs::read(&cache.path).unwrap(), vdb);
    assert_eq!(std::fs::read(&dest).unwrap(), sra);
}

fn pipeline_config(output_dir: &Path) -> PipelineConfig {
    PipelineConfig {
        output_dir: output_dir.to_path_buf(),
        compression: CompressionMode::None,
        threads: 2,
        strict: false,
        ..Default::default()
    }
}

#[tokio::test]
async fn get_downloads_the_vdbcache_where_decode_looks_for_it() {
    let server = MockServer::start().await;
    let sra = kar_payload(4096, 5);
    let vdb = kar_payload(2048, 6);
    serve(&server, "/sra", &sra).await;
    serve(&server, "/vdb", &vdb).await;

    let dir = tempfile::tempdir().unwrap();
    let r = resolved(
        file(format!("{}/sra", server.uri()), &sra, true),
        Some(file(format!("{}/vdb", server.uri()), &vdb, true)),
    );
    let downloaded = download_sra(&r, &pipeline_config(dir.path()))
        .await
        .unwrap();
    let cache = vdbcache_path_for(&downloaded.temp_path);
    assert_eq!(
        std::fs::read(&cache).expect("get must fetch the vdbcache"),
        vdb
    );
}
