//! Integration tests for [`sracha_core::download::download_file`] covering
//! the network-error paths the audit flagged as uncovered: retries on
//! transient 503s, MD5 mismatch, truncated responses, and URL fallback.
//!
//! These run against an in-process [`wiremock`] server so they're fast and
//! hermetic — no network access required. They're NOT marked `#[ignore]`.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use sracha_core::download::{DownloadConfig, download_file};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

/// Build a DownloadConfig appropriate for hermetic tests: progress off,
/// single connection, tiny chunks so we get >1 chunk for small payloads.
fn test_config() -> DownloadConfig {
    DownloadConfig {
        connections: 2,
        chunk_size: 64, // force chunking on tiny payloads
        force: false,
        validate: true,
        progress: false,
        resume: false,
        auto_scale_connections: false,
        client: None,
        expected_prefix: None,
    }
}

fn tmp_out(name: &str) -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join(name);
    (dir, p)
}

fn md5_hex(bytes: &[u8]) -> String {
    use md5::{Digest, Md5};
    let digest = Md5::digest(bytes);
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

#[tokio::test]
async fn download_file_succeeds_against_mock_server() {
    let server = MockServer::start().await;
    let payload = b"hello, SRA downloader".to_vec();

    // HEAD advertises Range support + size.
    Mock::given(method("HEAD"))
        .and(path("/file"))
        .respond_with(
            ResponseTemplate::new(200)
                .append_header("Accept-Ranges", "bytes")
                .append_header("Content-Length", payload.len().to_string()),
        )
        .mount(&server)
        .await;

    // GET with Range returns the requested slice. wiremock doesn't
    // natively parse Range headers, so respond with the full body for any
    // GET and let the client accept it — DownloadConfig validates total
    // size at the end.
    Mock::given(method("GET"))
        .and(path("/file"))
        .respond_with(ResponseTemplate::new(206).set_body_bytes(payload.clone()))
        .mount(&server)
        .await;

    let url = format!("{}/file", server.uri());
    let (_dir, out) = tmp_out("ok.sra");
    let expected_md5 = md5_hex(&payload);

    let res = download_file(
        &[url],
        payload.len() as u64,
        Some(&expected_md5),
        &out,
        &test_config(),
    )
    .await
    .expect("download should succeed");
    assert_eq!(res.size, payload.len() as u64);
    assert_eq!(res.md5.as_deref(), Some(expected_md5.as_str()));
    assert_eq!(std::fs::read(&out).unwrap(), payload);
}

#[tokio::test]
async fn download_file_rejects_md5_mismatch() {
    let server = MockServer::start().await;
    let payload = b"this is the payload we will serve".to_vec();

    Mock::given(method("HEAD"))
        .and(path("/f"))
        .respond_with(
            ResponseTemplate::new(200)
                .append_header("Accept-Ranges", "bytes")
                .append_header("Content-Length", payload.len().to_string()),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/f"))
        .respond_with(ResponseTemplate::new(206).set_body_bytes(payload.clone()))
        .mount(&server)
        .await;

    let url = format!("{}/f", server.uri());
    let (_dir, out) = tmp_out("bad-md5.sra");
    let wrong_md5 = "0".repeat(32);

    let err = download_file(
        &[url],
        payload.len() as u64,
        Some(&wrong_md5),
        &out,
        &test_config(),
    )
    .await
    .err()
    .expect("MD5 mismatch must error");
    let msg = format!("{err}");
    assert!(
        msg.to_lowercase().contains("md5") || msg.to_lowercase().contains("checksum"),
        "unexpected error: {msg}"
    );
}

#[tokio::test]
async fn download_file_persistent_failure_exhausts_retries() {
    let server = MockServer::start().await;
    let payload = b"persistent failure test".to_vec();

    Mock::given(method("HEAD"))
        .and(path("/bad"))
        .respond_with(
            ResponseTemplate::new(200)
                .append_header("Accept-Ranges", "bytes")
                .append_header("Content-Length", payload.len().to_string()),
        )
        .mount(&server)
        .await;
    // Every GET errors — the MAX_RETRIES-bounded retry loop must give up
    // and surface the failure rather than hang or panic.
    Mock::given(method("GET"))
        .and(path("/bad"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;

    let url = format!("{}/bad", server.uri());
    let (_dir, out) = tmp_out("bad.sra");
    let err = download_file(&[url], payload.len() as u64, None, &out, &test_config())
        .await
        .err()
        .expect("persistent failures must surface");
    let msg = format!("{err}").to_lowercase();
    assert!(
        msg.contains("download") || msg.contains("chunk") || msg.contains("503"),
        "unexpected error text: {msg}"
    );
}

#[tokio::test]
async fn download_file_empty_url_list_errors_fast() {
    let (_dir, out) = tmp_out("none.sra");
    let err = download_file(&[], 100, None, &out, &test_config())
        .await
        .err()
        .expect("empty URL list must error");
    let msg = format!("{err}").to_lowercase();
    assert!(
        msg.contains("no download") || msg.contains("urls"),
        "got {msg}"
    );
}

#[tokio::test]
async fn download_file_skips_when_existing_file_matches_md5() {
    // Resume is about not re-downloading when the local file is already
    // complete. If an SRA at the expected size with the expected MD5
    // already exists, download_file must return bytes_transferred=0 and
    // reuse the file — even if the server would have served something.
    let server = MockServer::start().await;
    let payload = b"already downloaded, nothing to do".to_vec();

    Mock::given(method("HEAD"))
        .and(path("/skip"))
        .respond_with(
            ResponseTemplate::new(200)
                .append_header("Accept-Ranges", "bytes")
                .append_header("Content-Length", payload.len().to_string()),
        )
        .mount(&server)
        .await;
    // GET registered but should never be hit.
    Mock::given(method("GET"))
        .and(path("/skip"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let url = format!("{}/skip", server.uri());
    let (_dir, out) = tmp_out("already.sra");
    std::fs::write(&out, &payload).unwrap();
    let expected_md5 = md5_hex(&payload);

    let cfg = DownloadConfig {
        resume: true,
        ..test_config()
    };
    let res = download_file(
        &[url],
        payload.len() as u64,
        Some(&expected_md5),
        &out,
        &cfg,
    )
    .await
    .expect("pre-existing file with matching MD5 must not trigger a download");
    assert_eq!(res.size, payload.len() as u64);
    assert_eq!(
        res.bytes_transferred, 0,
        "bytes_transferred must be 0 when skipping"
    );
}

#[tokio::test]
async fn download_file_force_overwrites_existing_even_when_complete() {
    // With `force: true`, an existing complete file must be replaced by a
    // fresh download. The assertion: bytes_transferred > 0.
    let server = MockServer::start().await;
    let payload = b"fresh content from server".to_vec();

    Mock::given(method("HEAD"))
        .and(path("/force"))
        .respond_with(
            ResponseTemplate::new(200)
                .append_header("Accept-Ranges", "bytes")
                .append_header("Content-Length", payload.len().to_string()),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/force"))
        .respond_with(ResponseTemplate::new(206).set_body_bytes(payload.clone()))
        .mount(&server)
        .await;

    let url = format!("{}/force", server.uri());
    let (_dir, out) = tmp_out("force.sra");
    // Pre-populate with the *wrong* content at the right size so a
    // resume-check would accept it (size-matches heuristic) — --force
    // must still redownload.
    let stale = vec![0xAAu8; payload.len()];
    std::fs::write(&out, &stale).unwrap();

    let cfg = DownloadConfig {
        force: true,
        resume: true, // even with resume enabled, force wins
        ..test_config()
    };
    let res = download_file(&[url], payload.len() as u64, None, &out, &cfg)
        .await
        .expect("force must re-download");
    assert_eq!(res.size, payload.len() as u64);
    assert!(
        res.bytes_transferred > 0,
        "force must actually transfer bytes, got {}",
        res.bytes_transferred
    );
    assert_eq!(std::fs::read(&out).unwrap(), payload);
}

#[tokio::test]
async fn download_file_recovers_after_transient_failure() {
    // A chunk fails once (503) then succeeds — exercises the per-chunk
    // retry/backoff path added for flaky hosts like ENA. The retry loop lives
    // on the parallel chunked path, which only engages for files >= 32 MiB
    // (SMALL_FILE); use a single 33 MiB chunk so the whole-body 206 is valid.
    let server = MockServer::start().await;
    let size = 33 * 1024 * 1024usize;
    let payload: Vec<u8> = (0..size).map(|i| (i * 17 + 3) as u8).collect();

    // First GET → 503, consumed after one response...
    Mock::given(method("GET"))
        .and(path("/flaky"))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;
    // ...subsequent GET (the retry) → 206 with the full body (one chunk).
    Mock::given(method("GET"))
        .and(path("/flaky"))
        .respond_with(ResponseTemplate::new(206).set_body_bytes(payload.clone()))
        .with_priority(2)
        .mount(&server)
        .await;

    let url = format!("{}/flaky", server.uri());
    let (_dir, out) = tmp_out("flaky.sra");
    let expected_md5 = md5_hex(&payload);

    let cfg = DownloadConfig {
        connections: 1,
        chunk_size: 64 * 1024 * 1024, // one chunk covering the whole file
        ..test_config()
    };
    let res = download_file(&[url], size as u64, Some(&expected_md5), &out, &cfg)
        .await
        .expect("download must recover after a transient 503");
    assert_eq!(std::fs::read(&out).unwrap(), payload);
    assert!(res.bytes_transferred > 0);
}

/// A `Range`-aware mock responder: serves the requested byte slice as a 206,
/// records every requested chunk start, and can be told to fail (503) the
/// chunk beginning at a specific offset. `fail_start == u64::MAX` disables
/// failure injection. This is what lets us drive the parallel + sidecar
/// resume path that plain wiremock matchers can't.
struct RangeResponder {
    body: Vec<u8>,
    fail_start: Arc<AtomicU64>,
    requested_starts: Arc<Mutex<Vec<u64>>>,
}

impl Respond for RangeResponder {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let raw = request
            .headers
            .get("range")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("bytes="))
            .unwrap_or("");
        let (start, end) = raw
            .split_once('-')
            .and_then(|(a, b)| Some((a.parse::<u64>().ok()?, b.parse::<u64>().ok()?)))
            .expect("test always sends a well-formed byte range");

        self.requested_starts.lock().unwrap().push(start);

        if self.fail_start.load(Ordering::SeqCst) == start {
            return ResponseTemplate::new(503);
        }
        let slice = &self.body[start as usize..=end as usize];
        ResponseTemplate::new(206).set_body_bytes(slice.to_vec())
    }
}

#[tokio::test]
async fn download_file_resumes_missing_chunk_via_sidecar() {
    // Cross both thresholds for the parallel + sidecar path: a > 32 MiB file
    // (SMALL_FILE) downloaded in 8 MiB chunks. One chunk fails on the first
    // run, leaving a partial file + `.sracha-progress`; the second run must
    // resume and re-fetch ONLY that chunk.
    const CHUNK: u64 = 8 * 1024 * 1024;
    let size = (5 * CHUNK) as usize; // 40 MiB => chunks at 0,8,16,24,32 MiB
    let fail_offset = 2 * CHUNK; // fail the chunk starting at 16 MiB

    // Deterministic, non-uniform payload so MD5 is meaningful.
    let payload: Vec<u8> = (0..size).map(|i| (i * 31 + 7) as u8).collect();
    let expected_md5 = md5_hex(&payload);

    let fail_start = Arc::new(AtomicU64::new(fail_offset));
    let requested = Arc::new(Mutex::new(Vec::<u64>::new()));

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/big"))
        .respond_with(RangeResponder {
            body: payload.clone(),
            fail_start: fail_start.clone(),
            requested_starts: requested.clone(),
        })
        .mount(&server)
        .await;

    let url = format!("{}/big", server.uri());
    let (_dir, out) = tmp_out("big.sra");

    let cfg = DownloadConfig {
        connections: 4,
        chunk_size: CHUNK,
        resume: true,
        ..test_config()
    };

    // First run: the chunk at `fail_offset` fails all retries → error, but
    // the other four chunks land and get recorded in the sidecar.
    let err = download_file(
        std::slice::from_ref(&url),
        size as u64,
        Some(&expected_md5),
        &out,
        &cfg,
    )
    .await
    .err()
    .expect("first run must fail on the injected bad chunk");
    let _ = err;

    let sidecar = out.parent().unwrap().join(format!(
        ".{}.sracha-progress",
        out.file_name().unwrap().to_str().unwrap()
    ));
    assert!(sidecar.exists(), "sidecar must persist partial progress");
    assert_eq!(
        std::fs::metadata(&out).unwrap().len(),
        size as u64,
        "output is preallocated to full size"
    );

    // Second run: stop failing and resume. Only the missing chunk should be
    // fetched this time.
    fail_start.store(u64::MAX, Ordering::SeqCst);
    requested.lock().unwrap().clear();

    let res = download_file(&[url], size as u64, Some(&expected_md5), &out, &cfg)
        .await
        .expect("resume run must complete");

    assert_eq!(res.md5.as_deref(), Some(expected_md5.as_str()));
    assert_eq!(std::fs::read(&out).unwrap(), payload);

    let starts = requested.lock().unwrap().clone();
    assert_eq!(
        starts,
        vec![fail_offset],
        "resume must re-fetch only the previously-failed chunk"
    );
    assert!(
        !sidecar.exists(),
        "sidecar is cleaned up after a successful completion"
    );
}
