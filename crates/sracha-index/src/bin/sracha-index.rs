//! `sracha-index` CLI: extract, build, query, and compact catalog
//! shards.

use clap::{Parser, Subcommand};
use sracha_index::{Result, extractor};

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
            tracing::warn!(
                "build is not yet implemented (would read {}, write {}, with {workers} workers)",
                accession_list.display(),
                output.display(),
            );
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
