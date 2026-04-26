//! High-level `build_shard` helper used by the bundled `sracha index`
//! CLI. Hides the Vortex session plumbing so the top-level binary
//! doesn't need vortex as a direct dependency.

use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::Semaphore;
use vortex::VortexSessionDefault;
use vortex::io::session::RuntimeSessionExt;
use vortex::session::VortexSession;

use crate::record::Platform;
use crate::{Error, Result, extractor, writer};

/// Build (or append to) a catalog shard from an accession list.
///
/// Layout written:
/// - `<catalog_dir>/shards/<shard_name>.vortex/{accessions,col_extents,schemas}.vortex`
/// - `<catalog_dir>/manifest.json` (created or updated)
///
/// `is_append=false` removes any pre-existing shard with the same
/// name and the catalog's manifest before writing (full overwrite).
/// `is_append=true` keeps both and lets the manifest list union over
/// shards.
pub async fn build_shard(
    accession_list: &Path,
    catalog_dir: &Path,
    shard_name: &str,
    workers: usize,
    is_append: bool,
    skip_unsupported_platforms: bool,
) -> Result<()> {
    let raw = std::fs::read_to_string(accession_list)?;
    let accessions: Vec<String> = raw
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(String::from)
        .collect();
    let total = accessions.len();
    if total == 0 {
        return Err(Error::Extractor("empty accession list".into()));
    }
    tracing::info!("building shard for {total} accessions with {workers} parallel workers");

    let started = Instant::now();
    let sem = Arc::new(Semaphore::new(workers));
    let mut handles = Vec::with_capacity(total);
    for acc in accessions {
        let permit = sem.clone();
        handles.push(tokio::spawn(async move {
            let _p = permit.acquire().await.unwrap();
            (acc.clone(), extractor::extract(&acc).await)
        }));
    }

    let shard_relative = format!("shards/{shard_name}.vortex");
    let shard_path = catalog_dir.join(&shard_relative);
    let manifest_path = catalog_dir.join("manifest.json");

    if !is_append && shard_path.exists() {
        std::fs::remove_dir_all(&shard_path)?;
    }
    if !is_append && manifest_path.exists() {
        std::fs::remove_file(&manifest_path)?;
    }
    std::fs::create_dir_all(catalog_dir.join("shards"))?;

    let mut writer_obj = writer::ShardWriter::create(&shard_path)?;
    let mut total_bytes_fetched: u64 = 0;
    let mut total_extract_secs: f32 = 0.0;
    let mut n_ok = 0usize;
    let mut n_err = 0usize;
    let mut n_skipped_platform = 0usize;
    const PROGRESS_EVERY: usize = 100;

    for (i, h) in handles.into_iter().enumerate() {
        let (acc, res) = h
            .await
            .map_err(|e| Error::Extractor(format!("join: {e}")))?;
        match res {
            Ok(rec) => {
                total_bytes_fetched += rec.bytes_fetched;
                total_extract_secs += rec.extract_secs;
                if skip_unsupported_platforms && rec.platform == Platform::Other {
                    tracing::debug!("{acc}: skipped (unsupported platform)");
                    n_skipped_platform += 1;
                } else {
                    writer_obj.append(rec)?;
                    n_ok += 1;
                }
            }
            Err(e) => {
                tracing::warn!("{acc}: extract failed: {e}");
                n_err += 1;
            }
        }
        let done = i + 1;
        if done % PROGRESS_EVERY == 0 || done == total {
            let elapsed = started.elapsed().as_secs_f32();
            let rate = done as f32 / elapsed.max(0.001);
            let eta = if rate > 0.0 {
                (total - done) as f32 / rate
            } else {
                0.0
            };
            tracing::info!(
                "progress: done={done}/{total} ok={n_ok} err={n_err} \
                 skipped_platform={n_skipped_platform} \
                 rate={rate:.1}/s elapsed={elapsed:.0}s eta={eta:.0}s \
                 fetched={}MB",
                total_bytes_fetched / (1024 * 1024),
            );
        }
    }

    let session = VortexSession::default().with_tokio();
    let summary = writer_obj
        .finish_with_manifest(&session, Some(&manifest_path), shard_name, &shard_relative)
        .await?;

    let wall = started.elapsed().as_secs_f32();
    tracing::info!(
        "built {} ({} accessions, {} schemas, {} bytes shard) in {:.1}s wall",
        summary.path.display(),
        summary.n_accessions,
        summary.n_schemas,
        summary.bytes,
        wall,
    );
    tracing::info!(
        "extracted {n_ok} ok / {n_err} err / {n_skipped_platform} skipped (unsupported platform) — \
         {}MB pulled from S3 across all extractors, \
         {:.1}s aggregate extractor wall ({:.1}x parallel speedup)",
        total_bytes_fetched / (1024 * 1024),
        total_extract_secs,
        total_extract_secs / wall.max(0.001),
    );
    Ok(())
}

/// `YYYY-MM-DD` for the current UTC date — default delta shard name.
pub fn today_yyyy_mm_dd() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let days = secs / 86400;
    let (y, m, d) = days_to_ymd(days as i64);
    format!("{y:04}-{m:02}-{d:02}")
}

fn days_to_ymd(mut days: i64) -> (i32, u32, u32) {
    days += 719468;
    let era = days.div_euclid(146097);
    let doe = (days - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = (yoe as i64 + era * 400) as i32;
    let doy = (doe - (365 * yoe + yoe / 4 - yoe / 100)) as u32;
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}
