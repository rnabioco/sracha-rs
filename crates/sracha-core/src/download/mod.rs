use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio::sync::Semaphore;

use crate::error::{Error, Result};

pub mod chunk_ready;

use chunk_ready::ChunkReadyTracker;

/// Size thresholds for adaptive chunk sizing.
const SMALL_FILE: u64 = 32 * 1024 * 1024; // 32 MiB
const MEDIUM_FILE: u64 = 256 * 1024 * 1024; // 256 MiB
const LARGE_FILE: u64 = 2 * 1024 * 1024 * 1024; // 2 GiB

/// Maximum retry attempts per chunk.
const MAX_RETRIES: u32 = 3;

/// Auto-scale floor for parallel TCP connections on medium+ files.
/// 24 is the safe sweet spot: benched against 16 and 32 on a head-node
/// link to S3 us-east-1, results were within noise (cluster-bound), but
/// on a network with more headroom the extra parallel streams give better
/// aggregate throughput without triggering S3-side TCP fairness pushback
/// (a small regression appeared at 64).
const MEDIUM_FILE_CONNECTIONS: usize = 24;

/// Configuration for parallel chunked downloads.
pub struct DownloadConfig {
    /// Number of parallel connections (default 8).
    pub connections: usize,
    /// Bytes per chunk (0 = adaptive sizing based on file size).
    pub chunk_size: u64,
    /// Overwrite existing files.
    pub force: bool,
    /// Verify MD5 checksum after download.
    pub validate: bool,
    /// Show progress bar.
    pub progress: bool,
    /// Attempt to resume interrupted downloads (default true).
    pub resume: bool,
    /// Shared HTTP client. When `None`, a fresh client is built per call.
    /// The orchestrator should pass the same client it uses for SDL/S3 so
    /// TLS sessions and connection pools are reused.
    pub client: Option<reqwest::Client>,
    /// Optional byte prefix to verify on cached files whose MD5 is unknown.
    /// When set, the size-match-skip path reads this many bytes from the
    /// existing file and rejects it on mismatch — catches stale/truncated
    /// temp files from prior interrupted runs that happen to match the
    /// expected size. Currently wired up by `download_sra` with the KAR
    /// archive magic (`NCBI.sra`) so a corrupt `.sracha-tmp-*.sra` cannot
    /// silently feed garbage to the VDB decoder.
    pub expected_prefix: Option<Vec<u8>>,
}

impl Default for DownloadConfig {
    fn default() -> Self {
        Self {
            connections: 8,
            chunk_size: 0, // adaptive
            force: false,
            validate: true,
            progress: true,
            resume: true,
            client: None,
            expected_prefix: None,
        }
    }
}

/// Result of a successful download.
pub struct DownloadResult {
    /// Path to the downloaded file.
    pub path: PathBuf,
    /// Size in bytes.
    pub size: u64,
    /// MD5 hex digest (if validation was performed).
    pub md5: Option<String>,
    /// Bytes actually transferred over the network this session.
    /// Zero when the file was already complete (skipped).
    pub bytes_transferred: u64,
    /// Per-chunk completion tracker. `Some` only on the parallel-chunked
    /// path (file size >= [`SMALL_FILE`] and server supports Range);
    /// `None` for the single-stream fallback and early-return paths
    /// (file already complete on disk). All chunks are marked done by
    /// the time `download_file` returns successfully.
    ///
    /// Streaming consumers (decoder running concurrently with download)
    /// will, in a later phase, take this tracker as an input parameter
    /// rather than receiving it on completion. For Phase 2 it's
    /// returned post-hoc as plumbing/observability.
    pub chunk_ready: Option<Arc<ChunkReadyTracker>>,
}

/// Information gathered from probing the server.
struct ProbeResult {
    /// Whether the server supports HTTP Range requests.
    supports_range: bool,
    /// Content-Length reported by the server.
    content_length: Option<u64>,
}

/// A byte range for one chunk of the download.
#[derive(Debug, Clone, Copy)]
struct ChunkRange {
    start: u64,
    end: u64, // inclusive
}

impl ChunkRange {
    fn len(&self) -> u64 {
        self.end - self.start + 1
    }
}

/// Choose the chunk size based on file size (adaptive) or use the configured value.
fn effective_chunk_size(file_size: u64, config: &DownloadConfig) -> u64 {
    if config.chunk_size > 0 {
        return config.chunk_size;
    }
    // Adaptive sizing.
    if file_size < MEDIUM_FILE {
        8 * 1024 * 1024 // 8 MiB
    } else if file_size < LARGE_FILE {
        16 * 1024 * 1024 // 16 MiB
    } else {
        64 * 1024 * 1024 // 64 MiB
    }
}

/// Divide the file into byte-range chunks.
fn plan_chunks(file_size: u64, chunk_size: u64) -> Vec<ChunkRange> {
    let mut chunks = Vec::new();
    let mut offset = 0u64;
    while offset < file_size {
        let end = std::cmp::min(offset + chunk_size - 1, file_size - 1);
        chunks.push(ChunkRange { start: offset, end });
        offset = end + 1;
    }
    chunks
}

/// Probe the first URL with a HEAD request to check Range support and size.
async fn probe_url(client: &reqwest::Client, url: &str) -> Result<ProbeResult> {
    let resp = client.head(url).send().await?;

    if !resp.status().is_success() {
        return Err(Error::Download {
            accession: String::new(),
            message: format!("HEAD request failed with HTTP {}", resp.status()),
        });
    }

    let supports_range = resp
        .headers()
        .get(reqwest::header::ACCEPT_RANGES)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.contains("bytes"));

    // reqwest's content_length() may return 0 for HEAD responses when the
    // body is empty. Read the raw header instead.
    let content_length = resp
        .headers()
        .get(reqwest::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());

    Ok(ProbeResult {
        supports_range,
        content_length,
    })
}

/// Download a single chunk with retries and exponential backoff.
async fn download_chunk(
    client: &reqwest::Client,
    url: &str,
    chunk: ChunkRange,
    file: &std::sync::Arc<std::fs::File>,
    progress: Option<&indicatif::ProgressBar>,
) -> Result<()> {
    let mut last_error = None;

    for attempt in 0..MAX_RETRIES {
        if attempt > 0 {
            // Short first retry (a read-timeout stall is usually a dead TCP
            // connection; a fresh one wins fast), then back off in case the
            // real problem is server-side rate limiting.
            let delay = std::time::Duration::from_millis(250u64 << (attempt - 1));
            tracing::warn!(
                "Retrying chunk {}-{} (attempt {}/{}), backoff {:?}",
                chunk.start,
                chunk.end,
                attempt + 1,
                MAX_RETRIES,
                delay,
            );
            tokio::time::sleep(delay).await;
        }

        match try_download_chunk(client, url, chunk, file, progress).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                tracing::warn!(
                    "Chunk {}-{} attempt {} failed: {e}",
                    chunk.start,
                    chunk.end,
                    attempt + 1,
                );
                last_error = Some(e);
            }
        }
    }

    Err(last_error.unwrap_or_else(|| Error::Download {
        accession: String::new(),
        message: "chunk download failed with no error captured".into(),
    }))
}

/// Single attempt to download a chunk.
///
/// Streams hyper's response pieces through a bounded mpsc into a
/// single `spawn_blocking` writer that uses `pwrite` on a shared
/// `std::fs::File` (opened once in `download_file`). Pieces are
/// coalesced into a 1 MiB buffer before each `write_all_at`, which
/// keeps syscall count near the chunk count instead of the piece
/// count (hyper emits ~16 KiB pieces, so at 70 MiB/s that's ~4500
/// syscalls/s without batching). The channel is bounded at 4 pieces
/// so backpressure throttles hyper when the disk writer falls
/// behind. Progress ticks are coalesced per 256 KiB to cut atomic
/// contention on the indicatif counter.
async fn try_download_chunk(
    client: &reqwest::Client,
    url: &str,
    chunk: ChunkRange,
    file: &std::sync::Arc<std::fs::File>,
    progress: Option<&indicatif::ProgressBar>,
) -> Result<()> {
    let range_header = format!("bytes={}-{}", chunk.start, chunk.end);
    let mut response = client
        .get(url)
        .header(reqwest::header::RANGE, &range_header)
        .send()
        .await?;

    // Require 206. A server that ignores Range and returns 200 with the
    // full body would be pwritten at `chunk.start` and silently corrupt
    // the output; the tail-length check only catches it afterward.
    let status = response.status();
    if status != reqwest::StatusCode::PARTIAL_CONTENT {
        return Err(Error::Download {
            accession: String::new(),
            message: format!("Range request returned HTTP {status} (expected 206)"),
        });
    }

    let (tx, mut rx) = tokio::sync::mpsc::channel::<bytes::Bytes>(4);

    let writer_file = file.clone();
    let writer_start = chunk.start;
    let writer = tokio::task::spawn_blocking(move || -> std::io::Result<u64> {
        use std::os::unix::fs::FileExt;
        const WRITE_THRESHOLD: usize = 1 << 20; // 1 MiB
        let mut offset = writer_start;
        let mut total = 0u64;
        let mut buf: Vec<u8> = Vec::with_capacity(WRITE_THRESHOLD + 64 * 1024);
        while let Some(piece) = rx.blocking_recv() {
            total += piece.len() as u64;
            buf.extend_from_slice(&piece);
            if buf.len() >= WRITE_THRESHOLD {
                writer_file.write_all_at(&buf, offset)?;
                offset += buf.len() as u64;
                buf.clear();
            }
        }
        if !buf.is_empty() {
            writer_file.write_all_at(&buf, offset)?;
        }
        Ok(total)
    });

    // Feed the writer. Abort if it returned early (disk error) — dropping
    // `tx` closes the channel so the writer's `blocking_recv` returns None.
    const PB_TICK_THRESHOLD: u64 = 256 * 1024;
    let mut feed_error: Option<Error> = None;
    let mut pending_ticks = 0u64;
    while let Some(piece) = response.chunk().await? {
        let n = piece.len() as u64;
        if tx.send(piece).await.is_err() {
            feed_error = Some(Error::Download {
                accession: String::new(),
                message: format!(
                    "chunk {}-{}: writer task exited while receiving data",
                    chunk.start, chunk.end,
                ),
            });
            break;
        }
        if let Some(pb) = progress {
            pending_ticks += n;
            if pending_ticks >= PB_TICK_THRESHOLD {
                pb.inc(pending_ticks);
                pending_ticks = 0;
            }
        }
    }
    drop(tx);
    if let Some(pb) = progress
        && pending_ticks > 0
    {
        pb.inc(pending_ticks);
    }

    let bytes_written = writer
        .await
        .map_err(|e| Error::Download {
            accession: String::new(),
            message: format!("writer task panicked: {e}"),
        })?
        .map_err(Error::Io)?;

    if let Some(err) = feed_error {
        return Err(err);
    }

    if bytes_written != chunk.len() {
        return Err(Error::Download {
            accession: String::new(),
            message: format!(
                "chunk {}-{}: expected {} bytes, got {bytes_written}",
                chunk.start,
                chunk.end,
                chunk.len(),
            ),
        });
    }

    Ok(())
}

/// Download a file as a single stream (fallback when server lacks Range support).
async fn download_single_stream(
    client: &reqwest::Client,
    url: &str,
    output_path: &Path,
    expected_size: u64,
    progress: Option<&indicatif::ProgressBar>,
) -> Result<()> {
    let mut response = client.get(url).send().await?;

    if !response.status().is_success() {
        return Err(Error::Download {
            accession: String::new(),
            message: format!("GET request failed with HTTP {}", response.status()),
        });
    }

    let mut file = tokio::fs::File::create(output_path).await?;
    let mut bytes_written = 0u64;

    while let Some(piece) = response.chunk().await? {
        file.write_all(&piece).await?;
        let n = piece.len() as u64;
        bytes_written += n;
        if let Some(pb) = progress {
            pb.inc(n);
        }
    }
    file.flush().await?;

    if expected_size > 0 && bytes_written != expected_size {
        return Err(Error::Download {
            accession: String::new(),
            message: format!("size mismatch: expected {expected_size} bytes, got {bytes_written}"),
        });
    }

    Ok(())
}

/// Compute the MD5 hex digest of a file.
///
/// Uses a single mmap pass on a blocking worker. MD5 is inherently
/// sequential, so we can't hash parallel chunks as they arrive — but we
/// can avoid the thousands of `tokio::fs` async reads that the previous
/// 64 KiB-buffer loop required. The downloaded bytes are still in the OS
/// page cache immediately after writing, so the mmap pass is effectively
/// a single CPU-bound MD5 over in-memory data.
async fn compute_md5(path: &Path) -> Result<String> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || -> Result<String> {
        let file = std::fs::File::open(&path).map_err(Error::Io)?;
        let mut hasher = Md5::new();
        // mmap::map fails on empty files; hash empty input directly.
        if file.metadata().map_err(Error::Io)?.len() > 0 {
            // SAFETY: the temp SRA file is owned by this process; nothing
            // else writes to it while we're hashing. UB on mutation
            // (concurrent truncate) is acceptable — we already treat such a
            // case as a fatal I/O error.
            let mmap = unsafe { memmap2::Mmap::map(&file).map_err(Error::Io)? };
            hasher.update(&mmap[..]);
        }
        let digest = hasher.finalize();
        Ok(digest.iter().map(|b| format!("{b:02x}")).collect())
    })
    .await
    .map_err(|e| Error::Download {
        accession: String::new(),
        message: format!("md5 task panicked: {e}"),
    })?
}

/// Read the first `prefix.len()` bytes of `path` and compare to `prefix`.
///
/// Returns `false` when the file is shorter than `prefix` or the bytes
/// differ. Used to cheaply reject a size-matching cached file whose content
/// is clearly wrong (e.g. a truncated SRA written before the KAR header).
async fn prefix_matches(path: &Path, prefix: &[u8]) -> Result<bool> {
    use tokio::io::AsyncReadExt;
    let mut file = tokio::fs::File::open(path).await.map_err(Error::Io)?;
    let mut buf = vec![0u8; prefix.len()];
    match file.read_exact(&mut buf).await {
        Ok(_) => Ok(buf == prefix),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
        Err(e) => Err(Error::Io(e)),
    }
}

// ---------------------------------------------------------------------------
// Download resume: progress sidecar
// ---------------------------------------------------------------------------

/// Tracks which chunks have been downloaded for resume support.
///
/// Persisted as a JSON sidecar file next to the download target.
#[derive(Serialize, Deserialize)]
struct DownloadProgress {
    /// URL used for the download (detect URL changes).
    url: String,
    /// Expected total file size.
    expected_size: u64,
    /// Chunk size used for planning.
    chunk_size: u64,
    /// Total number of planned chunks.
    total_chunks: usize,
    /// Indices of chunks that completed successfully.
    completed_chunks: HashSet<usize>,
    /// Expected MD5 digest of the completed file, when known. Refusing to
    /// resume when this changes prevents concatenating bytes from two
    /// different server-side objects (re-uploaded SRA, etc.).
    #[serde(default)]
    expected_md5: Option<String>,
}

/// Compute the path of the progress sidecar file.
pub(crate) fn progress_path(output_path: &Path) -> PathBuf {
    let filename = output_path
        .file_name()
        .map(|f| f.to_string_lossy().to_string())
        .unwrap_or_default();
    output_path.with_file_name(format!(".{filename}.sracha-progress"))
}

/// Load an existing progress sidecar, if present and parseable.
fn load_progress(path: &Path) -> Option<DownloadProgress> {
    let data = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&data).ok()
}

/// Save progress to the sidecar file on the tokio runtime.
async fn save_progress(path: &Path, progress: &DownloadProgress) -> std::io::Result<()> {
    let data = serde_json::to_string(progress)?;
    tokio::fs::write(path, data).await
}

/// Delete the progress sidecar file.
fn delete_progress(path: &Path) {
    let _ = std::fs::remove_file(path);
}

/// Create a progress bar for the download.
fn make_progress_bar(total_size: u64) -> indicatif::ProgressBar {
    crate::pipeline::make_styled_pb(
        total_size,
        "  {elapsed_precise} [{bar:40.cyan}] {bytes}/{total_bytes}  {bytes_per_sec}  eta {eta}",
    )
}

/// Download a file from one or more mirror URLs using parallel HTTP Range requests.
///
/// The downloader probes the first URL for Range support, plans byte-range chunks,
/// and downloads them in parallel with a semaphore limiting concurrency. Falls back
/// to a single-stream download when the server does not support Range requests.
///
/// ## Resume behavior
///
/// When `config.resume` is `true` (the default):
///
/// - If the output file already exists at the expected size with a matching MD5
///   (when available), the download is skipped entirely.
/// - If a `.sracha-progress` sidecar file exists from a previous interrupted
///   parallel download, only the remaining chunks are re-downloaded.
/// - For single-stream downloads (< 32 MiB), the existing file size is used
///   to resume via an HTTP Range request.
/// - On failure, partial files and progress sidecars are preserved for the
///   next attempt.
pub async fn download_file(
    urls: &[String],
    expected_size: u64,
    expected_md5: Option<&str>,
    output_path: &Path,
    config: &DownloadConfig,
) -> Result<DownloadResult> {
    download_file_inner(urls, expected_size, expected_md5, output_path, config, None).await
}

/// Streaming variant of [`download_file`]: hands the freshly-constructed
/// per-chunk readiness tracker out via `tracker_init` as soon as it's
/// created (well before the download completes), so a streaming
/// consumer (e.g. a concurrent decoder) can start awaiting byte ranges
/// immediately.
///
/// `tracker_init` is fired only on the parallel-chunked path; on the
/// single-stream fallback (file < `SMALL_FILE`) the sender is dropped
/// without sending — receivers should treat `RecvError` as "no
/// streaming available, fall back to await-then-decode".
///
/// All other behavior is identical to [`download_file`].
pub async fn download_file_streaming(
    urls: &[String],
    expected_size: u64,
    expected_md5: Option<&str>,
    output_path: &Path,
    config: &DownloadConfig,
    tracker_init: tokio::sync::oneshot::Sender<Arc<ChunkReadyTracker>>,
) -> Result<DownloadResult> {
    download_file_inner(
        urls,
        expected_size,
        expected_md5,
        output_path,
        config,
        Some(tracker_init),
    )
    .await
}

async fn download_file_inner(
    urls: &[String],
    expected_size: u64,
    expected_md5: Option<&str>,
    output_path: &Path,
    config: &DownloadConfig,
    mut tracker_init: Option<tokio::sync::oneshot::Sender<Arc<ChunkReadyTracker>>>,
) -> Result<DownloadResult> {
    if urls.is_empty() {
        return Err(Error::Download {
            accession: String::new(),
            message: "no download URLs provided".into(),
        });
    }

    // Ensure parent directory exists.
    if let Some(parent) = output_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    // ------------------------------------------------------------------
    // Resume / existing file checks.
    // ------------------------------------------------------------------
    if output_path.exists() && !config.force {
        if config.resume {
            let existing_meta = tokio::fs::metadata(output_path).await?;
            let existing_size = existing_meta.len();

            // File at expected size — verify MD5 if available, then skip.
            if expected_size > 0 && existing_size == expected_size {
                if let Some(expected) = expected_md5 {
                    let computed = compute_md5(output_path).await?;
                    if computed == expected {
                        tracing::info!(
                            "file already exists with correct MD5, skipping: {}",
                            output_path.display(),
                        );
                        return Ok(DownloadResult {
                            path: output_path.to_path_buf(),
                            size: existing_size,
                            md5: Some(computed),
                            bytes_transferred: 0,
                            chunk_ready: None,
                        });
                    }
                    tracing::warn!(
                        "file exists at expected size but MD5 mismatch — re-downloading",
                    );
                } else if let Some(prefix) = config.expected_prefix.as_deref() {
                    // No MD5 to cross-check — fall back to a cheap prefix probe.
                    // Catches the common case of a truncated/garbage
                    // `.sracha-tmp-*.sra` left behind by a crashed prior run.
                    if prefix_matches(output_path, prefix).await? {
                        tracing::info!(
                            "file already exists at expected size with matching header, skipping: {}",
                            output_path.display(),
                        );
                        return Ok(DownloadResult {
                            path: output_path.to_path_buf(),
                            size: existing_size,
                            md5: None,
                            bytes_transferred: 0,
                        });
                    }
                    tracing::warn!(
                        "file exists at expected size but header prefix mismatch — re-downloading: {}",
                        output_path.display(),
                    );
                } else {
                    tracing::info!(
                        "file already exists at expected size, skipping: {}",
                        output_path.display(),
                    );
                    return Ok(DownloadResult {
                        path: output_path.to_path_buf(),
                        size: existing_size,
                        md5: None,
                        bytes_transferred: 0,
                        chunk_ready: None,
                    });
                }
            }
            // Else: partial file or wrong size — fall through to resume logic below.
        } else {
            return Err(Error::Download {
                accession: String::new(),
                message: format!(
                    "output file already exists: {} (use --force to overwrite)",
                    output_path.display()
                ),
            });
        }
    }

    // Prefer a caller-supplied client so multi-accession runs reuse TLS
    // sessions and connection pools. Fall back to a fresh client only when
    // no shared one was provided (e.g. tests, ad-hoc callers).
    let client = match &config.client {
        Some(c) => c.clone(),
        None => crate::http::default_client(),
    };

    // Probe the first URL. When the caller already knows the file size
    // (always true via SDL's RunInfo), skip the HEAD and assume Range
    // support — `try_download_chunk` verifies per-chunk by requiring a
    // 206 response, and a 200 surfaces as a retryable error. NCBI's S3
    // and HTTPS mirrors reliably honor Range, so on the normal path this
    // saves one RTT per accession (material for multi-accession batches).
    let url = &urls[0];
    let probe = if expected_size > 0 {
        tracing::debug!("skipping HEAD; using caller-supplied size {expected_size}");
        ProbeResult {
            supports_range: true,
            content_length: Some(expected_size),
        }
    } else {
        tracing::debug!("probing URL: {url}");
        let p = probe_url(&client, url).await?;
        tracing::debug!(
            "probe result: range={}, content_length={:?}",
            p.supports_range,
            p.content_length,
        );
        p
    };

    let file_size = probe.content_length.unwrap_or(expected_size);
    if file_size == 0 {
        return Err(Error::Download {
            accession: String::new(),
            message: "cannot determine file size from server or caller".into(),
        });
    }

    let use_parallel = probe.supports_range && file_size >= SMALL_FILE;

    // Scale up connections for medium+ files (see MEDIUM_FILE_CONNECTIONS).
    let connections = if file_size >= MEDIUM_FILE {
        config.connections.max(MEDIUM_FILE_CONNECTIONS)
    } else {
        config.connections
    };

    let bytes_transferred: u64;
    // Phase 2 plumbing: track per-chunk completion so a future streaming
    // decoder can begin work as bytes land. `Some` only on the parallel
    // path; the single-stream fallback stays None (streaming decode
    // doesn't apply to <SMALL_FILE files anyway).
    let mut chunk_ready_tracker: Option<Arc<ChunkReadyTracker>> = None;

    if use_parallel {
        // --- Parallel chunked download (with resume support) ---
        let chunk_size = effective_chunk_size(file_size, config);
        let all_chunks = plan_chunks(file_size, chunk_size);
        let total_chunks = all_chunks.len();

        // Check for existing progress sidecar. `--force` means "start
        // fresh", so ignore any stale sidecar even when resume is on;
        // otherwise a prior run's completed-chunks record would
        // short-circuit the download and skew the progress bar total.
        let prog_path = progress_path(output_path);
        let prev_progress = if config.resume && !config.force {
            load_progress(&prog_path)
        } else {
            None
        };
        if config.force {
            delete_progress(&prog_path);
        }

        // Track whether we have a prior-MD5 mismatch so we also wipe the
        // partial file, not just the sidecar.
        let mut force_wipe_partial = false;
        let (chunks_to_download, mut progress): (Vec<(usize, ChunkRange)>, DownloadProgress) =
            if let Some(prev) = prev_progress {
                let md5_changed = match (prev.expected_md5.as_deref(), expected_md5) {
                    (Some(a), Some(b)) => a != b,
                    _ => false,
                };
                if md5_changed {
                    tracing::warn!(
                        "progress sidecar MD5 {} != remote MD5 {} — restarting download",
                        prev.expected_md5.as_deref().unwrap_or("<none>"),
                        expected_md5.unwrap_or("<none>"),
                    );
                    force_wipe_partial = true;
                }

                // expected_size + chunk_size matching implies total_chunks
                // matches — no need to check it separately.
                if !md5_changed
                    && prev.expected_size == file_size
                    && prev.chunk_size == chunk_size
                    && prev.url == *url
                {
                    // Valid progress file — resume from where we left off.
                    let remaining: Vec<(usize, ChunkRange)> = all_chunks
                        .into_iter()
                        .enumerate()
                        .filter(|(i, _)| !prev.completed_chunks.contains(i))
                        .collect();
                    tracing::info!(
                        "resuming download: {}/{} chunks remaining",
                        remaining.len(),
                        total_chunks,
                    );
                    (remaining, prev)
                } else {
                    tracing::debug!(
                        "progress sidecar parameters changed — starting fresh download",
                    );
                    let indexed: Vec<(usize, ChunkRange)> =
                        all_chunks.into_iter().enumerate().collect();
                    let progress = DownloadProgress {
                        url: url.clone(),
                        expected_size: file_size,
                        chunk_size,
                        total_chunks,
                        completed_chunks: HashSet::new(),
                        expected_md5: expected_md5.map(String::from),
                    };
                    (indexed, progress)
                }
            } else {
                let indexed: Vec<(usize, ChunkRange)> =
                    all_chunks.into_iter().enumerate().collect();
                let progress = DownloadProgress {
                    url: url.clone(),
                    expected_size: file_size,
                    chunk_size,
                    total_chunks,
                    completed_chunks: HashSet::new(),
                    expected_md5: expected_md5.map(String::from),
                };
                (indexed, progress)
            };

        if force_wipe_partial {
            let _ = tokio::fs::remove_file(output_path).await;
            delete_progress(&prog_path);
        }

        // Build the per-chunk readiness tracker now that chunk_size,
        // total_chunks, and the resume state are all known. Pre-mark
        // chunks already complete from a prior partial download so a
        // streaming consumer doesn't wait on already-on-disk bytes.
        // (force_wipe_partial reset progress.completed_chunks above.)
        let tracker = Arc::new(ChunkReadyTracker::new(total_chunks, chunk_size, file_size));
        for &done_idx in &progress.completed_chunks {
            tracker.mark_done(done_idx);
        }
        chunk_ready_tracker = Some(tracker.clone());
        // Hand the tracker out to a streaming consumer (if any) BEFORE
        // we spawn chunk workers. Receiver may have already given up if
        // we took too long getting here — that's fine, send returns
        // Err which we ignore.
        if let Some(tx) = tracker_init.take() {
            let _ = tx.send(tracker.clone());
        }

        if chunks_to_download.is_empty() {
            tracing::info!("all chunks already downloaded — verifying");
            bytes_transferred = 0;
        } else {
            let already_done = progress.completed_chunks.len();
            tracing::debug!(
                "downloading {} of {} chunks ({} each) with {} connections",
                chunks_to_download.len(),
                total_chunks,
                crate::util::format_size(chunk_size),
                connections,
            );

            // Calculate bytes remaining for progress bar.
            let bytes_remaining: u64 = chunks_to_download.iter().map(|(_, c)| c.len()).sum();
            bytes_transferred = bytes_remaining;

            let pb: Option<std::sync::Arc<indicatif::ProgressBar>> = if config.progress {
                let pb = std::sync::Arc::new(make_progress_bar(bytes_remaining));
                if already_done > 0 {
                    // Show that we're resuming.
                    pb.set_message(format!("resuming ({already_done}/{total_chunks} cached)"));
                }
                Some(pb)
            } else {
                None
            };

            // Ensure the output file exists at exactly `file_size`. Fresh
            // downloads get a sparse zero-filled file; resumes verify the
            // partial file and extend/truncate if it drifted (user copied
            // something in, previous run died mid-preallocate, etc.).
            {
                let f = tokio::fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(output_path)
                    .await?;
                if f.metadata().await?.len() != file_size {
                    f.set_len(file_size).await?;
                }
            }

            // Open one shared sync file handle for pwrite across all chunks.
            // Pays one `open` + one `spawn_blocking` for the whole download
            // instead of one per chunk — N opens saved where N = chunk count.
            let shared_file = {
                let path = output_path.to_path_buf();
                tokio::task::spawn_blocking(move || {
                    std::fs::OpenOptions::new().write(true).open(&path)
                })
                .await
                .map_err(|e| Error::Download {
                    accession: String::new(),
                    message: format!("file open task panicked: {e}"),
                })?
                .map_err(Error::Io)?
            };
            let shared_file = std::sync::Arc::new(shared_file);

            let semaphore = std::sync::Arc::new(Semaphore::new(connections));
            let client = std::sync::Arc::new(client);
            let url = std::sync::Arc::new(url.clone());

            // Channel for completed chunk indices (for progress sidecar updates).
            let (done_tx, mut done_rx) = tokio::sync::mpsc::unbounded_channel::<usize>();

            let mut handles = Vec::with_capacity(chunks_to_download.len());

            for (chunk_idx, chunk) in chunks_to_download {
                let sem = semaphore.clone();
                let cli = client.clone();
                let u = url.clone();
                let f = shared_file.clone();
                let pb_clone = pb.clone();
                let tx = done_tx.clone();
                let chunk_tracker = tracker.clone();

                let handle = tokio::spawn(async move {
                    let _permit = sem.acquire().await.map_err(|e| Error::Download {
                        accession: String::new(),
                        message: format!("semaphore acquire failed: {e}"),
                    })?;

                    download_chunk(&cli, &u, chunk, &f, pb_clone.as_deref()).await?;

                    // Mark this chunk's bytes as readable. Order matters:
                    // do this AFTER download_chunk's pwrite returns
                    // (bytes are durably on disk via the shared fd) so
                    // any streaming consumer awaiting this byte range
                    // sees a consistent view.
                    chunk_tracker.mark_done(chunk_idx);

                    // Notify the progress sidecar saver (separate from the
                    // streaming tracker — this drives the .sracha-progress
                    // JSON checkpoint, not decoder readiness).
                    let _ = tx.send(chunk_idx);
                    Ok::<(), Error>(())
                });

                handles.push(handle);
            }
            // Drop our copy so the channel closes when all tasks finish.
            drop(done_tx);

            // Collect completed chunk indices and save progress on a timer.
            // A 2 s interval is plenty for resume granularity on a file that
            // takes minutes to download, and avoids blocking the runtime on
            // per-chunk JSON serialization + sync write.
            let prog_path_clone = prog_path.clone();
            let progress_saver = tokio::spawn(async move {
                let mut flush = tokio::time::interval(std::time::Duration::from_secs(2));
                flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                // Skip the immediate first tick — nothing to save yet.
                flush.tick().await;
                let mut dirty = false;

                loop {
                    tokio::select! {
                        biased;
                        msg = done_rx.recv() => match msg {
                            Some(idx) => {
                                progress.completed_chunks.insert(idx);
                                dirty = true;
                            }
                            None => break,
                        },
                        _ = flush.tick() => {
                            if dirty {
                                let _ = save_progress(&prog_path_clone, &progress).await;
                                dirty = false;
                            }
                        }
                    }
                }
                // Final save on channel close.
                if dirty {
                    let _ = save_progress(&prog_path_clone, &progress).await;
                }
            });

            // Await all download tasks; collect errors.
            let mut first_error: Option<Error> = None;
            for handle in handles {
                match handle.await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        if first_error.is_none() {
                            first_error = Some(e);
                        }
                    }
                    Err(join_err) => {
                        if first_error.is_none() {
                            first_error = Some(Error::Download {
                                accession: String::new(),
                                message: format!("task panicked: {join_err}"),
                            });
                        }
                    }
                }
            }

            // Wait for progress saver to finish.
            let _ = progress_saver.await;

            if let Some(pb) = &pb {
                if already_done > 0 {
                    pb.finish_with_message(format!(
                        "download complete (resumed, {already_done}/{total_chunks} cached)"
                    ));
                } else {
                    pb.finish_with_message("download complete");
                }
            }

            if let Some(e) = first_error {
                // Do NOT clean up partial file — preserve for resume.
                tracing::warn!(
                    "download failed; partial file and progress preserved for resume: {}",
                    output_path.display(),
                );
                return Err(e);
            }
        }

        // Download succeeded — clean up progress sidecar.
        delete_progress(&prog_path);
    } else {
        // --- Single-stream fallback (with resume support) ---
        // `--force` short-circuits resume: otherwise an already-complete
        // file on disk leaves `bytes_remaining = 0`, creating a
        // progress bar with `total = 0` that ticks to the real file
        // size (displays "X MiB/0 B") while the download re-runs.
        let existing_size = if config.resume && !config.force && output_path.exists() {
            tokio::fs::metadata(output_path).await?.len()
        } else {
            0
        };

        let bytes_remaining = file_size.saturating_sub(existing_size);
        bytes_transferred = bytes_remaining;

        let pb: Option<std::sync::Arc<indicatif::ProgressBar>> = if config.progress {
            Some(std::sync::Arc::new(make_progress_bar(bytes_remaining)))
        } else {
            None
        };

        if existing_size > 0 && existing_size < file_size && probe.supports_range {
            tracing::info!(
                "resuming single-stream download from byte {} ({} remaining)",
                existing_size,
                crate::util::format_size(bytes_remaining),
            );

            let range_header = format!("bytes={existing_size}-");
            let mut response = client
                .get(url)
                .header(reqwest::header::RANGE, &range_header)
                .send()
                .await?;

            let status = response.status();
            if !status.is_success() && status != reqwest::StatusCode::PARTIAL_CONTENT {
                return Err(Error::Download {
                    accession: String::new(),
                    message: format!("Range resume request failed with HTTP {status}"),
                });
            }

            // Defend against a server that silently returns the entire file
            // (status 200) when asked for a byte range: verify Content-Range
            // names exactly the bytes we asked for. If the header is missing
            // and the status isn't 206, refuse to append — otherwise the
            // tail would be concatenated onto the already-downloaded head
            // and the final MD5 check would flag it only at the very end.
            let content_range = response
                .headers()
                .get(reqwest::header::CONTENT_RANGE)
                .and_then(|v| v.to_str().ok())
                .map(String::from);
            let expected_end = file_size.saturating_sub(1);
            let expected_prefix = format!("bytes {existing_size}-{expected_end}/");
            match (status, content_range.as_deref()) {
                (reqwest::StatusCode::PARTIAL_CONTENT, Some(cr)) => {
                    if !cr.starts_with(&expected_prefix) {
                        return Err(Error::Download {
                            accession: String::new(),
                            message: format!(
                                "resume server returned Content-Range {cr:?}, expected prefix {expected_prefix:?}",
                            ),
                        });
                    }
                }
                (reqwest::StatusCode::PARTIAL_CONTENT, None) => {
                    return Err(Error::Download {
                        accession: String::new(),
                        message: "resume response is 206 but missing Content-Range header".into(),
                    });
                }
                _ => {
                    return Err(Error::Download {
                        accession: String::new(),
                        message: format!(
                            "resume server ignored Range header (status {status}); refusing to append full file to partial",
                        ),
                    });
                }
            }

            let mut file = tokio::fs::OpenOptions::new()
                .append(true)
                .open(output_path)
                .await?;

            let mut bytes_written = existing_size;
            while let Some(piece) = response.chunk().await? {
                file.write_all(&piece).await?;
                let n = piece.len() as u64;
                bytes_written += n;
                if let Some(ref pb) = pb {
                    pb.inc(n);
                }
            }
            file.flush().await?;

            if bytes_written != file_size {
                return Err(Error::Download {
                    accession: String::new(),
                    message: format!(
                        "size mismatch: expected {file_size} bytes, got {bytes_written}",
                    ),
                });
            }
        } else {
            tracing::debug!(
                "downloading {} in single stream (range support: {}, size < 32 MiB: {})",
                crate::util::format_size(file_size),
                probe.supports_range,
                file_size < SMALL_FILE,
            );

            download_single_stream(&client, url, output_path, file_size, pb.as_deref()).await?;
        }

        if let Some(pb) = &pb {
            if existing_size > 0 {
                pb.finish_with_message(format!(
                    "download complete (resumed from {})",
                    crate::util::format_size(existing_size),
                ));
            } else {
                pb.finish_with_message("download complete");
            }
        }
    }

    // Verify MD5 if requested.
    let md5_hex = if let (true, Some(expected)) = (config.validate, expected_md5) {
        let computed = compute_md5(output_path).await?;
        if computed != expected {
            // Clean up the bad file.
            let _ = tokio::fs::remove_file(output_path).await;
            return Err(Error::ChecksumMismatch {
                expected: expected.to_string(),
                actual: computed,
            });
        }
        Some(computed)
    } else if config.validate {
        // Validate requested but no expected MD5 — compute anyway for the result.
        Some(compute_md5(output_path).await?)
    } else {
        None
    };

    let metadata = tokio::fs::metadata(output_path).await?;

    // By this point all chunks are on disk and MD5 verified. If a
    // streaming consumer somehow ran past mark_done (it shouldn't) or
    // raced with the tail of the loop, mark_all_done as a belt-and-
    // suspenders safety net. Idempotent.
    if let Some(t) = &chunk_ready_tracker {
        t.mark_all_done();
    }

    Ok(DownloadResult {
        path: output_path.to_path_buf(),
        size: metadata.len(),
        md5: md5_hex,
        bytes_transferred,
        chunk_ready: chunk_ready_tracker,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plan_chunks_exact() {
        let chunks = plan_chunks(32, 8);
        assert_eq!(chunks.len(), 4);
        assert_eq!(chunks[0].start, 0);
        assert_eq!(chunks[0].end, 7);
        assert_eq!(chunks[1].start, 8);
        assert_eq!(chunks[1].end, 15);
        assert_eq!(chunks[3].start, 24);
        assert_eq!(chunks[3].end, 31);
    }

    #[test]
    fn test_plan_chunks_remainder() {
        let chunks = plan_chunks(30, 8);
        assert_eq!(chunks.len(), 4);
        assert_eq!(chunks[3].start, 24);
        assert_eq!(chunks[3].end, 29);
        assert_eq!(chunks[3].len(), 6);
    }

    #[test]
    fn test_plan_chunks_single() {
        let chunks = plan_chunks(5, 1024);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].start, 0);
        assert_eq!(chunks[0].end, 4);
    }

    #[test]
    fn test_effective_chunk_size_adaptive() {
        let config = DownloadConfig::default();
        // < 256 MiB => 8 MiB chunks
        assert_eq!(
            effective_chunk_size(100 * 1024 * 1024, &config),
            8 * 1024 * 1024
        );
        // < 2 GiB => 16 MiB chunks
        assert_eq!(
            effective_chunk_size(512 * 1024 * 1024, &config),
            16 * 1024 * 1024
        );
        // >= 2 GiB => 64 MiB chunks
        assert_eq!(
            effective_chunk_size(3 * 1024 * 1024 * 1024, &config),
            64 * 1024 * 1024
        );
    }

    #[test]
    fn test_effective_chunk_size_explicit() {
        let config = DownloadConfig {
            chunk_size: 4 * 1024 * 1024,
            ..Default::default()
        };
        assert_eq!(
            effective_chunk_size(1024 * 1024 * 1024, &config),
            4 * 1024 * 1024
        );
    }

    #[test]
    fn test_default_config() {
        let config = DownloadConfig::default();
        assert_eq!(config.connections, 8);
        assert_eq!(config.chunk_size, 0);
        assert!(!config.force);
        assert!(config.validate);
        assert!(config.progress);
        assert!(config.resume);
    }

    #[test]
    fn test_chunk_len() {
        let chunk = ChunkRange { start: 0, end: 99 };
        assert_eq!(chunk.len(), 100);

        let chunk = ChunkRange {
            start: 100,
            end: 199,
        };
        assert_eq!(chunk.len(), 100);
    }

    #[test]
    fn test_progress_path() {
        let path = std::path::Path::new("/tmp/SRR000001.sra");
        let prog = progress_path(path);
        assert_eq!(
            prog,
            std::path::PathBuf::from("/tmp/.SRR000001.sra.sracha-progress")
        );
    }

    #[tokio::test]
    async fn prefix_matches_accepts_exact_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("ok.sra");
        std::fs::write(&p, b"NCBI.sra<body>").unwrap();
        assert!(prefix_matches(&p, b"NCBI.sra").await.unwrap());
    }

    #[tokio::test]
    async fn prefix_matches_rejects_wrong_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("bad.sra");
        std::fs::write(&p, b"GARBAGE_<body>").unwrap();
        assert!(!prefix_matches(&p, b"NCBI.sra").await.unwrap());
    }

    #[tokio::test]
    async fn prefix_matches_rejects_too_short_file() {
        // Truncated temp file: shorter than the probe length.
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("short.sra");
        std::fs::write(&p, b"NCB").unwrap();
        assert!(!prefix_matches(&p, b"NCBI.sra").await.unwrap());
    }

    #[test]
    fn test_progress_serde_roundtrip() {
        let mut completed = HashSet::new();
        completed.insert(0);
        completed.insert(2);
        completed.insert(5);

        let progress = DownloadProgress {
            url: "https://example.com/file".into(),
            expected_size: 1024 * 1024,
            chunk_size: 8192,
            total_chunks: 128,
            completed_chunks: completed,
            expected_md5: Some("d41d8cd98f00b204e9800998ecf8427e".into()),
        };

        let json = serde_json::to_string(&progress).unwrap();
        let restored: DownloadProgress = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.url, progress.url);
        assert_eq!(restored.expected_size, progress.expected_size);
        assert_eq!(restored.chunk_size, progress.chunk_size);
        assert_eq!(restored.total_chunks, progress.total_chunks);
        assert_eq!(restored.completed_chunks, progress.completed_chunks);
    }

    #[tokio::test]
    async fn test_progress_save_load() {
        let tmp = tempfile::tempdir().unwrap();
        let prog_file = tmp.path().join(".test.sracha-progress");

        let mut completed = HashSet::new();
        completed.insert(1);
        completed.insert(3);

        let progress = DownloadProgress {
            url: "https://example.com/f".into(),
            expected_size: 999,
            chunk_size: 100,
            total_chunks: 10,
            completed_chunks: completed,
            expected_md5: None,
        };

        save_progress(&prog_file, &progress).await.unwrap();
        let loaded = load_progress(&prog_file).unwrap();
        assert_eq!(loaded.total_chunks, 10);
        assert!(loaded.completed_chunks.contains(&1));
        assert!(loaded.completed_chunks.contains(&3));
        assert!(!loaded.completed_chunks.contains(&0));

        delete_progress(&prog_file);
        assert!(load_progress(&prog_file).is_none());
    }
}
