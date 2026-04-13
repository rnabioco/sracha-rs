use std::path::PathBuf;

use clap::builder::styling::{AnsiColor, Effects, Styles};
use clap::{Args, Parser, Subcommand, ValueEnum};

const STYLES: Styles = Styles::styled()
    .header(AnsiColor::Yellow.on_default().effects(Effects::BOLD))
    .usage(AnsiColor::Yellow.on_default().effects(Effects::BOLD))
    .literal(AnsiColor::Green.on_default().effects(Effects::BOLD))
    .placeholder(AnsiColor::Cyan.on_default());

#[derive(Parser)]
#[command(
    name = "sracha",
    version,
    about = "Fast SRA downloader and FASTQ converter",
    styles = STYLES,
    after_help = "\
Examples:
  Download and convert to FASTQ in one shot:
    sracha get SRR1234567

  Download multiple accessions with progress:
    sracha get SRR1234567 SRR7654321 -p

  Fetch SRA file, then convert separately:
    sracha fetch SRR1234567
    sracha fastq SRR1234567.sra

  Interleaved paired-end output to stdout:
    sracha fastq SRR1234567.sra --split interleaved -Z

  Show accession metadata:
    sracha info SRR1234567

Documentation:
  https://rnabioco.github.io/sracha-rs/"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,

    /// Log verbosity (-v, -vv, -vvv)
    #[arg(short, long, action = clap::ArgAction::Count, global = true)]
    pub verbose: u8,

    /// Suppress all output except errors
    #[arg(short, long, global = true)]
    pub quiet: bool,
}

#[derive(Subcommand)]
pub enum Command {
    /// Download SRA/SRA-lite files
    Fetch(FetchArgs),

    /// Convert SRA file(s) to FASTQ
    Fastq(FastqArgs),

    /// Download, convert, and compress in one shot
    Get(GetArgs),

    /// Show accession metadata
    Info(InfoArgs),
}

#[derive(Args)]
pub struct FetchArgs {
    /// SRA accession(s) to download
    #[arg(required = true)]
    pub accessions: Vec<String>,

    /// Output directory
    #[arg(short = 'O', long, default_value = ".")]
    pub output_dir: PathBuf,

    /// Preferred format
    #[arg(long, default_value = "sra")]
    pub format: SraFormat,

    /// HTTP connections per file
    #[arg(long, default_value_t = 8)]
    pub connections: usize,

    /// Overwrite existing files
    #[arg(short, long)]
    pub force: bool,

    /// Show progress bar
    #[arg(short, long)]
    pub progress: bool,

    /// Verify MD5 after download
    #[arg(long)]
    pub validate: bool,
}

#[derive(Args)]
pub struct FastqArgs {
    /// SRA accession(s) or file path(s)
    #[arg(required = true)]
    pub inputs: Vec<String>,

    /// Split mode
    #[arg(long, default_value = "split-3")]
    pub split: SplitMode,

    /// Disable gzip compression (compressed by default)
    #[arg(long)]
    pub no_gzip: bool,

    /// Gzip compression level
    #[arg(long, default_value_t = 6, value_parser = clap::value_parser!(u32).range(1..=9))]
    pub gzip_level: u32,

    /// Number of threads for decode [default: 8]
    #[arg(short, long, default_value_t = 8)]
    pub threads: usize,

    /// Minimum read length
    #[arg(long)]
    pub min_read_len: Option<u32>,

    /// Include technical reads (skipped by default)
    #[arg(long)]
    pub include_technical: bool,

    /// Write to stdout
    #[arg(short = 'Z', long)]
    pub stdout: bool,

    /// Output directory
    #[arg(short = 'O', long, default_value = ".")]
    pub output_dir: PathBuf,

    /// Overwrite existing files
    #[arg(short, long)]
    pub force: bool,

    /// Show progress bar
    #[arg(short, long)]
    pub progress: bool,
}

#[derive(Args)]
pub struct GetArgs {
    /// SRA accession(s) to download and convert
    #[arg(required = true)]
    pub accessions: Vec<String>,

    /// Output directory
    #[arg(short = 'O', long, default_value = ".")]
    pub output_dir: PathBuf,

    /// Preferred SRA format
    #[arg(long, default_value = "sra")]
    pub format: SraFormat,

    /// Split mode
    #[arg(long, default_value = "split-3")]
    pub split: SplitMode,

    /// Disable gzip compression (compressed by default)
    #[arg(long)]
    pub no_gzip: bool,

    /// Gzip compression level
    #[arg(long, default_value_t = 6, value_parser = clap::value_parser!(u32).range(1..=9))]
    pub gzip_level: u32,

    /// Number of threads for decode [default: 8]
    #[arg(short, long, default_value_t = 8)]
    pub threads: usize,

    /// HTTP connections per file
    #[arg(long, default_value_t = 8)]
    pub connections: usize,

    /// Minimum read length
    #[arg(long)]
    pub min_read_len: Option<u32>,

    /// Include technical reads (skipped by default)
    #[arg(long)]
    pub include_technical: bool,

    /// Overwrite existing files
    #[arg(short, long)]
    pub force: bool,

    /// Show progress bar
    #[arg(short, long)]
    pub progress: bool,
}

#[derive(Args)]
pub struct InfoArgs {
    /// SRA accession(s) to query
    #[arg(required = true)]
    pub accessions: Vec<String>,
}

#[derive(Clone, Copy, ValueEnum)]
pub enum SraFormat {
    Sra,
    Sralite,
}

#[derive(Clone, Copy, ValueEnum)]
pub enum SplitMode {
    /// Paired reads to _1/_2, unpaired to _0
    #[value(name = "split-3")]
    Split3,
    /// Nth read to Nth file
    #[value(name = "split-files")]
    SplitFiles,
    /// All reads to one file
    #[value(name = "split-spot")]
    SplitSpot,
    /// R1/R2 interleaved in one file
    Interleaved,
}
