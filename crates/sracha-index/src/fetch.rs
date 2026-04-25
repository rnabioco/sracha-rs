//! HTTP fetch of a hosted catalog (manifest + per-shard Vortex files)
//! into a local cache directory.
//!
//! Layout mirrored on both ends:
//! ```text
//! <base>/manifest.json
//! <base>/shards/<name>.vortex/accessions.vortex
//! <base>/shards/<name>.vortex/col_extents.vortex
//! <base>/shards/<name>.vortex/schemas.vortex
//! ```
//!
//! Atomic install: each shard is downloaded into `<name>.vortex.partial/`
//! and renamed once all three files are on disk. The manifest is the
//! last thing renamed, so an interrupted update leaves the previous
//! manifest pointing at the previous (still-complete) shards.

use std::path::{Path, PathBuf};

use crate::reader::Manifest;
use crate::{Error, Result};

/// Files comprising one Vortex shard. Kept in lock-step with
/// [`crate::reader::ShardReader::open_local`].
const SHARD_FILES: [&str; 3] = ["accessions.vortex", "col_extents.vortex", "schemas.vortex"];

/// One file's worth of fetch progress, emitted via the
/// `on_progress` callback so callers can drive a progress bar.
#[derive(Debug, Clone)]
pub struct FetchProgress {
    /// Shard name (e.g. "base", "2026-04-26"). Empty for the manifest itself.
    pub shard: String,
    /// File within the shard (e.g. "accessions.vortex"), or "manifest.json".
    pub file: String,
    /// Bytes pulled so far for this file.
    pub bytes_done: u64,
    /// Total bytes for this file (from Content-Length), if known.
    pub bytes_total: Option<u64>,
}

/// Summary of one update run.
#[derive(Debug, Clone)]
pub struct FetchSummary {
    pub manifest_path: PathBuf,
    pub shards_fetched: Vec<String>,
    pub shards_skipped: Vec<String>,
    pub bytes_fetched: u64,
}

/// Fetch the hosted catalog into `cache_dir`.
///
/// `base_url` is the parent prefix (no trailing slash, e.g.
/// `https://sracha-catalog.s3.amazonaws.com/v1`). `force` re-downloads
/// shards that already exist locally; otherwise only missing shards are
/// pulled.
pub async fn update_catalog(
    client: &reqwest::Client,
    base_url: &str,
    cache_dir: &Path,
    force: bool,
    mut on_progress: impl FnMut(FetchProgress),
) -> Result<FetchSummary> {
    std::fs::create_dir_all(cache_dir)?;
    std::fs::create_dir_all(cache_dir.join("shards"))?;

    let base = base_url.trim_end_matches('/');

    // 1. Pull the remote manifest into a partial file. Keep the
    //    existing manifest in place until every new shard is on disk
    //    so that an interrupted update is still queryable.
    let manifest_partial = cache_dir.join("manifest.json.partial");
    let manifest_final = cache_dir.join("manifest.json");
    let manifest_url = format!("{base}/manifest.json");
    let manifest_bytes = download_to_file(
        client,
        &manifest_url,
        &manifest_partial,
        |bytes_done, bytes_total| {
            on_progress(FetchProgress {
                shard: String::new(),
                file: "manifest.json".to_string(),
                bytes_done,
                bytes_total,
            });
        },
    )
    .await?;

    let manifest: Manifest = serde_json::from_slice(&manifest_bytes)?;
    if manifest.version != 1 {
        let _ = std::fs::remove_file(&manifest_partial);
        return Err(Error::Reader(format!(
            "remote manifest version {} not supported by this sracha build",
            manifest.version
        )));
    }

    let mut shards_fetched = Vec::new();
    let mut shards_skipped = Vec::new();
    let mut bytes_fetched = manifest_bytes.len() as u64;

    // 2. Pull each shard not already present.
    for entry in &manifest.shards {
        let shard_final = cache_dir.join(&entry.path);
        if shard_final.is_dir() && !force && shard_files_complete(&shard_final) {
            shards_skipped.push(entry.name.clone());
            continue;
        }
        let shard_partial_name = format!("{}.partial", entry.path);
        let shard_partial = cache_dir.join(&shard_partial_name);
        if shard_partial.exists() {
            std::fs::remove_dir_all(&shard_partial)?;
        }
        std::fs::create_dir_all(&shard_partial)?;

        for file in SHARD_FILES {
            let url = format!("{base}/{}/{}", entry.path, file);
            let dest = shard_partial.join(file);
            let n = download_to_file(client, &url, &dest, |bytes_done, bytes_total| {
                on_progress(FetchProgress {
                    shard: entry.name.clone(),
                    file: file.to_string(),
                    bytes_done,
                    bytes_total,
                });
            })
            .await?;
            bytes_fetched += n.len() as u64;
        }

        // Rename into place; if a previous copy is there (force
        // refresh), remove it first.
        if shard_final.exists() {
            std::fs::remove_dir_all(&shard_final)?;
        }
        if let Some(parent) = shard_final.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::rename(&shard_partial, &shard_final)?;
        shards_fetched.push(entry.name.clone());
    }

    // 3. Atomically swap the manifest in last so partial states
    //    can't strand readers on a manifest that names missing shards.
    std::fs::rename(&manifest_partial, &manifest_final)?;

    Ok(FetchSummary {
        manifest_path: manifest_final,
        shards_fetched,
        shards_skipped,
        bytes_fetched,
    })
}

/// True when every required *.vortex file is present in `shard_dir`.
fn shard_files_complete(shard_dir: &Path) -> bool {
    SHARD_FILES.iter().all(|f| shard_dir.join(f).is_file())
}

/// Stream a URL to a file, returning the body bytes on success. The
/// progress callback fires after each chunk with running totals; on
/// network or I/O error the partial file is removed.
async fn download_to_file(
    client: &reqwest::Client,
    url: &str,
    dest: &Path,
    mut on_chunk: impl FnMut(u64, Option<u64>),
) -> Result<Vec<u8>> {
    use futures::StreamExt;
    use std::io::Write;

    let resp = client
        .get(url)
        .send()
        .await?
        .error_for_status()
        .map_err(|e| Error::Reader(format!("GET {url}: {e}")))?;
    let total = resp.content_length();

    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp_path = dest.with_extension("tmp");
    let mut file = std::fs::File::create(&tmp_path)?;

    let mut stream = resp.bytes_stream();
    let mut acc: Vec<u8> = Vec::with_capacity(total.unwrap_or(0) as usize);
    let mut bytes_done: u64 = 0;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| {
            let _ = std::fs::remove_file(&tmp_path);
            Error::Reader(format!("read {url}: {e}"))
        })?;
        file.write_all(&chunk)?;
        acc.extend_from_slice(&chunk);
        bytes_done += chunk.len() as u64;
        on_chunk(bytes_done, total);
    }
    file.flush()?;
    drop(file);
    std::fs::rename(&tmp_path, dest)?;
    Ok(acc)
}
