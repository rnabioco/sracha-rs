//! Fetching external reference sequences for cSRA decode.
//!
//! Runs aligned to a public assembly keep only the chunk layout in their
//! REFERENCE table; the bases live in separate NCBI refseq objects named by
//! `REFERENCE.SEQ_ID` (e.g. `CM000663.1` = GRCh37 chr1). This module
//! resolves those accessions through SDL, downloads them into a shared
//! on-disk cache, and hands `sracha-vdb` local paths — the vdb crate never
//! does network I/O itself.
//!
//! The cache is deliberately *outside* the output directory: one GRCh37 set
//! (~780 MB across 24 objects) serves every human cSRA the user decodes.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::Semaphore;

use crate::download::{DownloadConfig, TransferRetryPolicy, download_file_with_retries};
use crate::error::{Error, Result};
use crate::pipeline::PipelineConfig;
use crate::sdl::{FormatPreference, ResolvedFile, SdlClient, resolved_file_from_sdl};
use crate::vdb::kar::KarArchive;
use crate::vdb::refseq::{ExternalRefNeed, external_refs_needed, is_safe_seq_id};

/// KAR magic; refseq objects are KAR archives like any other SRA object.
const KAR_MAGIC: &[u8] = b"NCBI.sra";

/// How many refseq objects to download at once. Each download already fans
/// out over `config.connections` range requests, so a low number here keeps
/// NCBI happy without leaving bandwidth on the table.
const REFSEQ_DOWNLOAD_CONCURRENCY: usize = 2;

/// Resolve the directory external refseq objects are cached in.
///
/// `--refseq-cache` wins, then `$SRACHA_REFSEQ_DIR`, then the platform
/// cache dir. Never inside the output directory — the whole point is reuse
/// across accessions and runs.
pub fn refseq_cache_dir(override_dir: Option<&Path>) -> Result<PathBuf> {
    if let Some(dir) = override_dir {
        return Ok(dir.to_path_buf());
    }
    if let Some(dir) = std::env::var_os("SRACHA_REFSEQ_DIR") {
        return Ok(PathBuf::from(dir));
    }
    let base = if let Some(xdg) = std::env::var_os("XDG_CACHE_HOME") {
        PathBuf::from(xdg)
    } else if let Some(home) = std::env::var_os("HOME") {
        if cfg!(target_os = "macos") {
            PathBuf::from(home).join("Library/Caches")
        } else {
            PathBuf::from(home).join(".cache")
        }
    } else {
        return Err(Error::Pipeline(
            "cannot locate a cache directory for external references — set \
             --refseq-cache or SRACHA_REFSEQ_DIR"
                .into(),
        ));
    };
    Ok(base.join("sracha").join("refseq"))
}

/// Discover, fetch, and return the local paths of every external reference
/// object `sra_path` needs.
///
/// Returns an empty vec when the archive embeds its reference bases (or
/// isn't a cSRA at all), so callers can invoke this unconditionally before
/// decode. Runs entirely in the async layer: rayon decode workers only ever
/// see local files.
pub async fn prepare_external_refseqs(
    sra_path: &Path,
    vdbcache_path: Option<&Path>,
    config: &PipelineConfig,
) -> Result<Vec<(String, PathBuf)>> {
    let needs = {
        let sra_path = sra_path.to_path_buf();
        let vdbcache_path = vdbcache_path.map(Path::to_path_buf);
        tokio::task::spawn_blocking(move || scan_needs(&sra_path, vdbcache_path.as_deref()))
            .await
            .map_err(|e| Error::Pipeline(format!("refseq scan panicked: {e}")))??
    };

    // Chunks that are entirely Ns address nothing external.
    let needs: Vec<ExternalRefNeed> = needs.into_iter().filter(|n| !n.all_n).collect();
    if needs.is_empty() {
        return Ok(Vec::new());
    }

    for need in &needs {
        if !is_safe_seq_id(&need.seq_id) {
            return Err(Error::Pipeline(format!(
                "refusing to fetch reference with unsafe SEQ_ID {:?}",
                need.seq_id
            )));
        }
    }

    let cache = refseq_cache_dir(config.refseq_cache_dir.as_deref())?;
    std::fs::create_dir_all(&cache).map_err(Error::Io)?;

    let client = SdlClient::with_client(
        config
            .http_client
            .clone()
            .unwrap_or_else(crate::http::default_client),
    );
    let accessions: Vec<String> = needs.iter().map(|n| n.seq_id.clone()).collect();
    // Use the raw batched call, not `resolve_many`: refseq accessions have
    // no EUtils RunInfo, and the completeness-aware retry loop would burn
    // its whole backoff schedule waiting for metadata that never arrives.
    let response = client.resolve(&accessions, FormatPreference::Sra).await?;

    let mut resolved: Vec<(String, ResolvedFile)> = Vec::new();
    let mut unresolved: Vec<String> = Vec::new();
    for acc in &accessions {
        let file = response
            .results
            .iter()
            .find(|r| r.bundle.as_deref() == Some(acc.as_str()))
            .and_then(|r| r.find_sra_file())
            .and_then(|f| resolved_file_from_sdl(f).ok())
            .filter(|f| !f.mirrors.is_empty());
        match file {
            Some(f) => resolved.push((acc.clone(), f)),
            None => unresolved.push(acc.clone()),
        }
    }

    // A cached copy is as good as a resolvable one — stay usable offline.
    let unresolved: Vec<String> = unresolved
        .into_iter()
        .filter(|acc| !cache.join(acc).exists())
        .collect();
    if !unresolved.is_empty() {
        return Err(Error::Pipeline(format!(
            "cannot locate {} external reference object(s) ({}) — they are \
             not in {} and SDL has no record of them",
            unresolved.len(),
            unresolved.join(", "),
            cache.display(),
        )));
    }

    let to_fetch: Vec<(String, ResolvedFile)> = resolved
        .iter()
        .filter(|(acc, f)| !is_cached(&cache.join(acc), f.size))
        .cloned()
        .collect();

    if !to_fetch.is_empty() {
        let total: u64 = to_fetch.iter().map(|(_, f)| f.size).sum();
        eprintln!(
            "  fetching {} external reference object(s) ({}) into {}",
            to_fetch.len(),
            crate::util::format_size(total),
            cache.display(),
        );
        fetch_all(&to_fetch, &cache, config).await?;
    }

    Ok(resolved
        .into_iter()
        .map(|(acc, _)| {
            let path = cache.join(&acc);
            (acc, path)
        })
        .collect())
}

/// Scan the archive (or its vdbcache, when REFERENCE lives there) for the
/// external references it needs.
fn scan_needs(sra_path: &Path, vdbcache_path: Option<&Path>) -> Result<Vec<ExternalRefNeed>> {
    let open = |p: &Path| -> Result<Vec<ExternalRefNeed>> {
        let file = std::fs::File::open(p)?;
        let mut archive = KarArchive::open(std::io::BufReader::new(file))?;
        Ok(external_refs_needed(&mut archive, p)?)
    };
    let from_main = open(sra_path)?;
    if !from_main.is_empty() {
        return Ok(from_main);
    }
    match vdbcache_path {
        Some(p) => open(p),
        None => Ok(from_main),
    }
}

/// Is this cache entry usable without re-downloading? Size match plus KAR
/// magic — a full md5 of ~780 MB on every run costs more than it catches.
/// `--verify` forces the download path, which does check md5.
fn is_cached(path: &Path, expected_size: u64) -> bool {
    let Ok(meta) = std::fs::metadata(path) else {
        return false;
    };
    if expected_size > 0 && meta.len() != expected_size {
        return false;
    }
    let mut buf = [0u8; 8];
    use std::io::Read;
    let Ok(mut f) = std::fs::File::open(path) else {
        return false;
    };
    f.read_exact(&mut buf).is_ok() && buf == KAR_MAGIC
}

async fn fetch_all(
    files: &[(String, ResolvedFile)],
    cache: &Path,
    config: &PipelineConfig,
) -> Result<()> {
    let sem = Arc::new(Semaphore::new(REFSEQ_DOWNLOAD_CONCURRENCY));
    let mut tasks = Vec::with_capacity(files.len());

    for (acc, file) in files {
        let permit = sem
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| Error::Pipeline(format!("refseq download semaphore closed: {e}")))?;
        let acc = acc.clone();
        let file = file.clone();
        let cache = cache.to_path_buf();
        let dl_config = DownloadConfig {
            connections: config.connections,
            chunk_size: 0,
            force: false,
            validate: file.md5.is_some(),
            // Per-object bars would interleave unreadably; the one-line
            // summary above already tells the user what is happening.
            progress: false,
            resume: config.resume,
            auto_scale_connections: true,
            client: config.http_client.clone(),
            expected_prefix: Some(KAR_MAGIC.to_vec()),
        };
        tasks.push(tokio::spawn(async move {
            let _permit = permit;
            let urls = mirror_urls(&file);
            // Download to a sibling `.partial` and rename on success, so a
            // reader never sees a truncated object and an interrupted run
            // leaves a resumable file rather than a poisoned cache entry.
            let partial = cache.join(format!("{acc}.partial"));
            let final_path = cache.join(&acc);
            download_file_with_retries(
                &urls,
                file.size,
                file.md5.as_deref(),
                &partial,
                &dl_config,
                &TransferRetryPolicy::default(),
            )
            .await
            .map_err(|e| Error::Pipeline(format!("external reference {acc}: {e}")))?;
            std::fs::rename(&partial, &final_path).map_err(Error::Io)?;
            let sidecar = crate::download::progress_path(&partial);
            let _ = std::fs::remove_file(&sidecar);
            Ok::<(), Error>(())
        }));
    }

    for task in tasks {
        task.await
            .map_err(|e| Error::Pipeline(format!("refseq download panicked: {e}")))??;
    }
    Ok(())
}

/// Mirror URLs for a resolved file, cloud first — same preference order
/// `select_mirror` applies to the main SRA object.
fn mirror_urls(file: &ResolvedFile) -> Vec<String> {
    let mut mirrors: Vec<_> = file.mirrors.iter().collect();
    mirrors.sort_by_key(|m| crate::pipeline::mirror_priority(&m.service));
    mirrors.into_iter().map(|m| m.url.clone()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_dir_prefers_explicit_override() {
        let explicit = PathBuf::from("/tmp/explicit");
        assert_eq!(refseq_cache_dir(Some(&explicit)).unwrap(), explicit);
    }

    #[test]
    fn missing_cache_entry_is_not_reused() {
        assert!(!is_cached(Path::new("/nonexistent/CM000663.1"), 100));
    }
}
