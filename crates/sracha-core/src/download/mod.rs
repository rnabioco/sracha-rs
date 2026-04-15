use std::collections::HashSet;
use std::path::{Path, PathBuf};

use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Semaphore;

use crate::error::{Error, Result};

/// Size thresholds for adaptive chunk sizing.
const SMALL_FILE: u64 = 32 * 1024 * 1024; // 32 MiB
const MEDIUM_FILE: u64 = 256 * 1024 * 1024; // 256 MiB
const LARGE_FILE: u64 = 2 * 1024 * 1024 * 1024; // 2 GiB

/// Maximum retry attempts per chunk.
const MAX_RETRIES: u32 = 3;

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
    output_path: &Path,
    progress: Option<&indicatif::ProgressBar>,
) -> Result<()> {
    let mut last_error = None;

    for attempt in 0..MAX_RETRIES {
        if attempt > 0 {
            // Exponential backoff: 1s, 2s, 4s
            let delay = std::time::Duration::from_secs(1 << attempt);
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

        match try_download_chunk(client, url, chunk, output_path, progress).await {
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
async fn try_download_chunk(
    client: &reqwest::Client,
    url: &str,
    chunk: ChunkRange,
    output_path: &Path,
    progress: Option<&indicatif::ProgressBar>,
) -> Result<()> {
    let range_header = format!("bytes={}-{}", chunk.start, chunk.end);
    let resp = client
        .get(url)
        .header(reqwest::header::RANGE, &range_header)
        .send()
        .await?;

    let status = resp.status();
    if !status.is_success() && status != reqwest::StatusCode::PARTIAL_CONTENT {
        return Err(Error::Download {
            accession: String::new(),
            message: format!("Range request failed with HTTP {status}"),
        });
    }

    // Open the file for writing at the correct offset (pwrite-style).
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .open(output_path)
        .await?;
    file.seek(std::io::SeekFrom::Start(chunk.start)).await?;

    // Stream the response body in pieces.
    let mut bytes_written = 0u64;
    // Use reqwest's chunk() API which doesn't require futures::StreamExt.
    let mut response = resp;
    while let Some(piece) = response.chunk().await? {
        file.write_all(&piece).await?;
        let n = piece.len() as u64;
        bytes_written += n;
        if let Some(pb) = progress {
            pb.inc(n);
        }
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
async fn compute_md5(path: &Path) -> Result<String> {
    let mut file = tokio::fs::File::open(path).await?;
    let mut hasher = Md5::new();
    let mut buf = vec![0u8; 64 * 1024]; // 64 KiB read buffer

    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }

    let digest = hasher.finalize();
    Ok(digest.iter().map(|b| format!("{b:02x}")).collect())
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
}

/// Compute the path of the progress sidecar file.
fn progress_path(output_path: &Path) -> PathBuf {
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

/// Save progress to the sidecar file.
fn save_progress(path: &Path, progress: &DownloadProgress) -> std::io::Result<()> {
    let data = serde_json::to_string(progress)?;
    std::fs::write(path, data)
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
                        });
                    }
                    tracing::warn!(
                        "file exists at expected size but MD5 mismatch — re-downloading",
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

    let client = reqwest::Client::builder()
        .user_agent(format!("sracha/{}", env!("CARGO_PKG_VERSION")))
        .http2_adaptive_window(true)
        .build()?;

    // Probe the first URL.
    let url = &urls[0];
    tracing::debug!("probing URL: {url}");
    let probe = probe_url(&client, url).await?;
    tracing::debug!(
        "probe result: range={}, content_length={:?}",
        probe.supports_range,
        probe.content_length
    );

    let file_size = probe.content_length.unwrap_or(expected_size);
    if file_size == 0 {
        return Err(Error::Download {
            accession: String::new(),
            message: "cannot determine file size from server or caller".into(),
        });
    }

    let use_parallel = probe.supports_range && file_size >= SMALL_FILE;

    // Scale up connections for large files (16 for >256 MiB, user value otherwise).
    let connections = if file_size >= MEDIUM_FILE {
        config.connections.max(16)
    } else {
        config.connections
    };

    if use_parallel {
        // --- Parallel chunked download (with resume support) ---
        let chunk_size = effective_chunk_size(file_size, config);
        let all_chunks = plan_chunks(file_size, chunk_size);
        let total_chunks = all_chunks.len();

        // Check for existing progress sidecar.
        let prog_path = progress_path(output_path);
        let prev_progress = if config.resume {
            load_progress(&prog_path)
        } else {
            None
        };

        let (chunks_to_download, mut progress): (Vec<(usize, ChunkRange)>, DownloadProgress) =
            if let Some(prev) = prev_progress {
                if prev.expected_size == file_size
                    && prev.chunk_size == chunk_size
                    && prev.url == *url
                    && prev.total_chunks == total_chunks
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
                };
                (indexed, progress)
            };

        if chunks_to_download.is_empty() {
            tracing::info!("all chunks already downloaded — verifying");
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

            // Pre-allocate the output file if this is a fresh download.
            if progress.completed_chunks.is_empty() {
                let file = tokio::fs::File::create(output_path).await?;
                file.set_len(file_size).await?;
            }
            // If resuming, file already exists at full pre-allocated size.

            let semaphore = std::sync::Arc::new(Semaphore::new(connections));
            let client = std::sync::Arc::new(client);
            let url = std::sync::Arc::new(url.clone());
            let path = std::sync::Arc::new(output_path.to_path_buf());

            // Channel for completed chunk indices (for progress sidecar updates).
            let (done_tx, mut done_rx) = tokio::sync::mpsc::unbounded_channel::<usize>();

            let mut handles = Vec::with_capacity(chunks_to_download.len());

            for (chunk_idx, chunk) in chunks_to_download {
                let sem = semaphore.clone();
                let cli = client.clone();
                let u = url.clone();
                let p = path.clone();
                let pb_clone = pb.clone();
                let tx = done_tx.clone();

                let handle = tokio::spawn(async move {
                    let _permit = sem.acquire().await.map_err(|e| Error::Download {
                        accession: String::new(),
                        message: format!("semaphore acquire failed: {e}"),
                    })?;

                    download_chunk(&cli, &u, chunk, &p, pb_clone.as_deref()).await?;

                    // Notify progress tracker.
                    let _ = tx.send(chunk_idx);
                    Ok::<(), Error>(())
                });

                handles.push(handle);
            }
            // Drop our copy so the channel closes when all tasks finish.
            drop(done_tx);

            // Collect completed chunk indices and periodically save progress.
            let prog_path_clone = prog_path.clone();
            let progress_saver = tokio::spawn(async move {
                let mut count = 0u64;
                while let Some(idx) = done_rx.recv().await {
                    progress.completed_chunks.insert(idx);
                    count += 1;
                    // Save every 10 chunks.
                    if count.is_multiple_of(10) {
                        let _ = save_progress(&prog_path_clone, &progress);
                    }
                }
                // Final save.
                let _ = save_progress(&prog_path_clone, &progress);
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
                pb.finish_with_message("download complete");
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
        let existing_size = if config.resume && output_path.exists() {
            tokio::fs::metadata(output_path).await?.len()
        } else {
            0
        };

        let bytes_remaining = file_size.saturating_sub(existing_size);

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
            pb.finish_with_message("download complete");
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

    Ok(DownloadResult {
        path: output_path.to_path_buf(),
        size: metadata.len(),
        md5: md5_hex,
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
        };

        let json = serde_json::to_string(&progress).unwrap();
        let restored: DownloadProgress = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.url, progress.url);
        assert_eq!(restored.expected_size, progress.expected_size);
        assert_eq!(restored.chunk_size, progress.chunk_size);
        assert_eq!(restored.total_chunks, progress.total_chunks);
        assert_eq!(restored.completed_chunks, progress.completed_chunks);
    }

    #[test]
    fn test_progress_save_load() {
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
        };

        save_progress(&prog_file, &progress).unwrap();
        let loaded = load_progress(&prog_file).unwrap();
        assert_eq!(loaded.total_chunks, 10);
        assert!(loaded.completed_chunks.contains(&1));
        assert!(loaded.completed_chunks.contains(&3));
        assert!(!loaded.completed_chunks.contains(&0));

        delete_progress(&prog_file);
        assert!(load_progress(&prog_file).is_none());
    }
}
