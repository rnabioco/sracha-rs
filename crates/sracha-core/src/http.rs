//! Shared `reqwest::Client` construction for all outbound HTTP.
//!
//! The CLI orchestrator builds one client at startup and threads it through
//! SDL, S3, and the chunked downloader so TLS sessions + connection pools
//! are reused across every accession in a batch. A fresh client per call
//! paid a ~50–200 ms TLS handshake per host per accession, which added up
//! on multi-accession runs.

use std::time::Duration;

/// Default HTTP client: HTTP/2 adaptive window, generous per-host pool,
/// TCP keepalive. Safe defaults for the SRA mirrors sracha talks to.
pub fn default_client() -> reqwest::Client {
    reqwest::Client::builder()
        .user_agent(format!("sracha/{}", env!("CARGO_PKG_VERSION")))
        .http2_adaptive_window(true)
        .pool_max_idle_per_host(16)
        .tcp_keepalive(Some(Duration::from_secs(60)))
        .build()
        .expect("failed to build HTTP client")
}
