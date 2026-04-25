//! `sracha-index` CLI: extract, build, query, and compact catalog
//! shards.

use clap::{Parser, Subcommand};
use sracha_index::{Error, Result, extractor, writer};
use vortex::VortexSessionDefault;
use vortex::io::session::RuntimeSessionExt;
use vortex::session::VortexSession;

#[derive(Parser)]
#[command(name = "sracha-index", about = "Build/query the sracha SRA metadata catalog")]
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
    /// Build a catalog shard from a list of accessions.
    Build {
        /// File with one accession per line.
        #[arg(long)]
        accession_list: std::path::PathBuf,
        /// Output shard path.
        #[arg(short, long)]
        output: std::path::PathBuf,
        /// Parallel extractor workers.
        #[arg(short = 'j', long, default_value = "32")]
        workers: usize,
    },
    /// Query a shard by accession id.
    Query {
        shard: std::path::PathBuf,
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
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| level.into()),
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
            workers,
        } => {
            run_build(&accession_list, &output, workers).await?;
        }
        Cmd::Query { shard, accession } => {
            tracing::warn!(
                "query is not yet implemented (would lookup {accession} in {})",
                shard.display(),
            );
        }
    }
    Ok(())
}

async fn run_build(
    accession_list: &std::path::Path,
    output: &std::path::Path,
    workers: usize,
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

    let mut writer_obj = writer::ShardWriter::create(output)?;
    let mut total_bytes_fetched: u64 = 0;
    let mut total_extract_secs: f32 = 0.0;
    let mut n_ok = 0usize;
    let mut n_err = 0usize;
    // Log progress every PROGRESS_EVERY accessions so the user can
    // see the build moving without enabling -vv. Roughly aligned
    // with cluster job watching cadence (~once per few seconds at
    // 16 workers × 1 sec/accession).
    const PROGRESS_EVERY: usize = 100;
    let mut last_log = Instant::now();

    for (i, h) in handles.into_iter().enumerate() {
        let (acc, res) = h.await.map_err(|e| Error::Extractor(format!("join: {e}")))?;
        match res {
            Ok(rec) => {
                total_bytes_fetched += rec.bytes_fetched;
                total_extract_secs += rec.extract_secs;
                writer_obj.append(rec)?;
                n_ok += 1;
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
                 rate={rate:.1}/s elapsed={elapsed:.0}s eta={eta:.0}s \
                 fetched={}MB",
                total_bytes_fetched / (1024 * 1024),
            );
            last_log = Instant::now();
        }
        let _ = last_log;
    }

    let session = VortexSession::default().with_tokio();
    let summary = writer_obj.finish(&session).await?;

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
        "extracted {n_ok} ok / {n_err} err — {}MB pulled from S3 across all extractors, \
         {:.1}s aggregate extractor wall ({:.1}x parallel speedup)",
        total_bytes_fetched / (1024 * 1024),
        total_extract_secs,
        total_extract_secs / wall.max(0.001),
    );
    Ok(())
}
