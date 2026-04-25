//! Shared `reqwest::Client` construction for all outbound HTTP.
//!
//! The CLI orchestrator builds one client at startup and threads it through
//! SDL, S3, and the chunked downloader so TLS sessions + connection pools
//! are reused across every accession in a batch. A fresh client per call
//! paid a ~50–200 ms TLS handshake per host per accession, which added up
//! on multi-accession runs.

use std::time::Duration;

/// Default per-host idle connection pool size. Sized to match
/// `s3::DEFAULT_PROBE_CONCURRENCY` so the resolve-phase HEAD storm
/// can run wide without queueing on the pool.
pub const DEFAULT_POOL_MAX_IDLE_PER_HOST: usize = 64;

/// Default HTTP client at the default pool size.
pub fn default_client() -> reqwest::Client {
    client_with_pool(DEFAULT_POOL_MAX_IDLE_PER_HOST)
}

/// HTTP/2 adaptive window, configurable per-host pool, TCP keepalive,
/// and a per-read timeout so a single stalled chunk can't hold up a
/// parallel download. `pool_max_idle_per_host` is the only knob —
/// the rest match `default_client()`.
///
/// The read timeout bounds the gap between successive body reads, not
/// the total request duration — slow-but-steady connections are fine,
/// only true stalls are cancelled. A cancelled chunk returns an error,
/// which the retry loop in `download::download_chunk` handles by
/// reissuing the GET on a fresh connection. Without this, one chunk
/// sitting on a dead TCP connection can block total download time on
/// the slowest element, which profiling on NCBI's S3 mirror showed as
/// a real (~6 s) tail every few runs.
pub fn client_with_pool(pool_max_idle_per_host: usize) -> reqwest::Client {
    reqwest::Client::builder()
        .user_agent(format!("sracha/{}", env!("CARGO_PKG_VERSION")))
        .http2_adaptive_window(true)
        .pool_max_idle_per_host(pool_max_idle_per_host)
        .tcp_keepalive(Some(Duration::from_secs(60)))
        .connect_timeout(Duration::from_secs(10))
        .read_timeout(Duration::from_secs(15))
        .build()
        .expect("failed to build HTTP client")
}
