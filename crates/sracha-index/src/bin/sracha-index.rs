//! `sracha-index` CLI: extract, build, query, and compact catalog
//! shards.

use clap::{Parser, Subcommand};
use sracha_index::record::Platform;
use sracha_index::{Error, Result, extractor, reader, writer};
use vortex::VortexSessionDefault;
use vortex::io::session::RuntimeSessionExt;
use vortex::session::VortexSession;

#[derive(Parser)]
#[command(
    name = "sracha-index",
    about = "Build/query the sracha SRA metadata catalog"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,

    /// Verbose logging (-v for INFO, -vv for DEBUG).
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
}

#[derive(Subcommand)]
enum Cmd {
    /// Extract metadata for a single accession and print as JSON.
    Extract { accession: String },
    /// Build a fresh catalog (initial base shard). Replaces any
    /// existing manifest at the catalog dir.
    Build {
        /// File with one accession per line.
        #[arg(long)]
        accession_list: std::path::PathBuf,
        /// Output catalog directory.
        #[arg(short, long)]
        output: std::path::PathBuf,
        /// Shard name within the catalog (default "base").
        #[arg(long, default_value = "base")]
        shard_name: String,
        /// Parallel extractor workers.
        #[arg(short = 'j', long, default_value = "32")]
        workers: usize,
        /// Keep records for platforms sracha can't decode (default
        /// drops them — the extractor will still pull metadata for
        /// every accession, but only Illumina/PacBio/ONT/IonTorrent
        /// rows land in the catalog).
        #[arg(long)]
        include_unsupported_platforms: bool,
    },
    /// Append a new delta shard to an existing catalog. Re-uses the
    /// same shard format; readers union over all shards in the
    /// manifest, newest-wins on lookup collisions.
    Append {
        /// File with one accession per line.
        #[arg(long)]
        accession_list: std::path::PathBuf,
        /// Existing catalog directory (must contain manifest.json).
        #[arg(short, long)]
        catalog: std::path::PathBuf,
        /// Shard name (default = today's date `YYYY-MM-DD`).
        #[arg(long)]
        shard_name: Option<String>,
        /// Parallel extractor workers.
        #[arg(short = 'j', long, default_value = "32")]
        workers: usize,
        /// Keep records for platforms sracha can't decode (default
        /// drops them).
        #[arg(long)]
        include_unsupported_platforms: bool,
    },
    /// Query a catalog (single shard or multi-shard manifest) by
    /// accession id.
    Query {
        catalog: std::path::PathBuf,
        accession: String,
    },
}

fn init_logging(v: u8) {
    let level = match v {
        0 => "warn",
        1 => "info",
        _ => "debug",
    };
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| level.into()),
        )
        .init();
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_logging(cli.verbose);

    match cli.cmd {
        Cmd::Extract { accession } => {
            let rec = extractor::extract(&accession).await?;
            println!("{}", serde_json::to_string_pretty(&rec)?);
        }
        Cmd::Build {
            accession_list,
            output,
            shard_name,
            workers,
            include_unsupported_platforms,
        } => {
            run_build(
                &accession_list,
                &output,
                &shard_name,
                workers,
                false,
                !include_unsupported_platforms,
            )
            .await?;
        }
        Cmd::Append {
            accession_list,
            catalog,
            shard_name,
            workers,
            include_unsupported_platforms,
        } => {
            let name = shard_name.unwrap_or_else(today_yyyy_mm_dd);
            run_build(
                &accession_list,
                &catalog,
                &name,
                workers,
                true,
                !include_unsupported_platforms,
            )
            .await?;
        }
        Cmd::Query { catalog, accession } => {
            let started = std::time::Instant::now();
            let cat = reader::CatalogReader::open_local(&catalog).await?;
            let opened = started.elapsed();
            let lookup_start = std::time::Instant::now();
            let rec = cat.lookup(&accession).await?;
            let lookup = lookup_start.elapsed();
            tracing::info!(
                "opened catalog ({} shards, {} accessions) in {:.1}ms; \
                 point lookup in {:.3}ms",
                cat.shard_count(),
                cat.len(),
                opened.as_secs_f64() * 1000.0,
                lookup.as_secs_f64() * 1000.0,
            );
            match rec {
                Some(r) => println!("{}", serde_json::to_string_pretty(&r)?),
                None => {
                    eprintln!("not found: {accession}");
                    std::process::exit(1);
                }
            }
        }
    }
    Ok(())
}

/// Build a shard inside `<catalog_dir>/shards/<shard_name>.vortex/`
/// and add an entry to `<catalog_dir>/manifest.json`. When
/// `is_append` is false, removes any pre-existing shard with the
/// same name (overwrite); when true, appends/replaces in-place.
async fn run_build(
    accession_list: &std::path::Path,
    catalog_dir: &std::path::Path,
    shard_name: &str,
    workers: usize,
    is_append: bool,
    skip_unsupported_platforms: bool,
) -> Result<()> {
    use std::sync::Arc;
    use std::time::Instant;
    use tokio::sync::Semaphore;

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

    // Resolve paths.
    let shard_relative = format!("shards/{shard_name}.vortex");
    let shard_path = catalog_dir.join(&shard_relative);
    let manifest_path = catalog_dir.join("manifest.json");

    if !is_append && shard_path.exists() {
        std::fs::remove_dir_all(&shard_path)?;
    }
    if !is_append && manifest_path.exists() {
        // Fresh build: remove the old manifest. Subsequent appends
        // will recreate it.
        std::fs::remove_file(&manifest_path)?;
    }
    std::fs::create_dir_all(catalog_dir.join("shards"))?;

    let mut writer_obj = writer::ShardWriter::create(&shard_path)?;
    let mut total_bytes_fetched: u64 = 0;
    let mut total_extract_secs: f32 = 0.0;
    let mut n_ok = 0usize;
    let mut n_err = 0usize;
    let mut n_skipped_platform = 0usize;
    // Log progress every PROGRESS_EVERY accessions so the user can
    // see the build moving without enabling -vv. Roughly aligned
    // with cluster job watching cadence (~once per few seconds at
    // 16 workers × 1 sec/accession).
    const PROGRESS_EVERY: usize = 100;
    let mut last_log = Instant::now();

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
            last_log = Instant::now();
        }
        let _ = last_log;
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
fn today_yyyy_mm_dd() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let days = secs / 86400;
    // Days since 1970-01-01 → calendar date via the simple
    // proleptic-Gregorian algorithm. Avoids pulling in chrono just
    // for a filename.
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
