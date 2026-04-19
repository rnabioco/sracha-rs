//! Shared `reqwest::Client` construction for all outbound HTTP.
//!
//! The CLI orchestrator builds one client at startup and threads it through
//! SDL, S3, and the chunked downloader so TLS sessions + connection pools
//! are reused across every accession in a batch. A fresh client per call
//! paid a ~50–200 ms TLS handshake per host per accession, which added up
//! on multi-accession runs.

use std::time::Duration;

/// Default HTTP client: HTTP/2 adaptive window, generous per-host pool,
/// TCP keepalive, and a per-read timeout so a single stalled chunk
/// can't hold up a parallel download.
///
/// The read timeout bounds the gap between successive body reads, not
/// the total request duration — slow-but-steady connections are fine,
/// only true stalls are cancelled. A cancelled chunk returns an error,
/// which the retry loop in `download::download_chunk` handles by
/// reissuing the GET on a fresh connection. Without this, one chunk
/// sitting on a dead TCP connection can block total download time on
/// the slowest element, which profiling on NCBI's S3 mirror showed as
/// a real (~6 s) tail every few runs.
pub fn default_client() -> reqwest::Client {
    reqwest::Client::builder()
        .user_agent(format!("sracha/{}", env!("CARGO_PKG_VERSION")))
        .http2_adaptive_window(true)
        .pool_max_idle_per_host(16)
        .tcp_keepalive(Some(Duration::from_secs(60)))
        .connect_timeout(Duration::from_secs(10))
        .read_timeout(Duration::from_secs(15))
        .build()
        .expect("failed to build HTTP client")
}
