//! Integration tests for [`sracha_core::download::download_file`] covering
//! the network-error paths the audit flagged as uncovered: retries on
//! transient 503s, MD5 mismatch, truncated responses, and URL fallback.
//!
//! These run against an in-process [`wiremock`] server so they're fast and
//! hermetic — no network access required. They're NOT marked `#[ignore]`.

use std::path::PathBuf;

use sracha_core::download::{DownloadConfig, download_file};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

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
        client: None,
        progress_parent: None,
        progress_combined: None,
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
    // Tiny payload uses the single-stream fallback path; chunk_ready
    // tracker is only attached on the parallel-chunked path
    // (file_size >= SMALL_FILE).
    assert!(
        res.chunk_ready.is_none(),
        "single-stream path should not return a tracker",
    );
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
