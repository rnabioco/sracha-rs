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
    version = env!("SRACHA_VERSION"),
    about = "Fast SRA downloader and FASTQ converter",
    styles = STYLES,
    after_help = "\
Examples:
  Download and convert to FASTQ in one shot:
    sracha get SRR2584863

  Download all runs from a BioProject or study:
    sracha get PRJNA675068
    sracha get SRP123456

  Download from an accession list file:
    sracha get --accession-list SRR_Acc_List.txt

  Fetch SRA file, then convert separately:
    sracha fetch SRR2584863
    sracha fastq SRR2584863.sra

  Interleaved paired-end output to stdout:
    sracha fastq SRR2584863.sra --split interleaved -Z

  Show accession metadata:
    sracha info SRR2584863

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

    /// Validate SRA file integrity
    Validate(ValidateArgs),
}

#[derive(Args)]
pub struct FetchArgs {
    /// SRA accession(s) to download (run, study, or BioProject)
    pub accessions: Vec<String>,

    /// Read accessions from a file (one per line)
    #[arg(long)]
    pub accession_list: Option<PathBuf>,

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

    /// Disable progress bar
    #[arg(long)]
    pub no_progress: bool,

    /// Verify MD5 after download
    #[arg(long)]
    pub validate: bool,

    /// Disable download resume (re-download from scratch)
    #[arg(long)]
    pub no_resume: bool,

    /// Skip confirmation prompt for large downloads (>500 GiB)
    #[arg(short, long)]
    pub yes: bool,

    /// Skip direct S3 and resolve via the SDL API
    #[arg(long)]
    pub prefer_sdl: bool,
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
    #[arg(long, conflicts_with = "zstd")]
    pub no_gzip: bool,

    /// Gzip compression level
    #[arg(long, default_value_t = 6, value_parser = clap::value_parser!(u32).range(1..=9))]
    pub gzip_level: u32,

    /// Use zstd compression instead of gzip
    #[arg(long, conflicts_with = "no_gzip")]
    pub zstd: bool,

    /// Zstd compression level (1-22)
    #[arg(long, default_value_t = 3, value_parser = clap::value_parser!(i32).range(1..=22))]
    pub zstd_level: i32,

    /// Output FASTA instead of FASTQ (drops quality scores)
    #[arg(long)]
    pub fasta: bool,

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

    /// Disable progress bar
    #[arg(long)]
    pub no_progress: bool,
}

#[derive(Args)]
pub struct GetArgs {
    /// SRA accession(s) to download and convert (run, study, or BioProject)
    pub accessions: Vec<String>,

    /// Read accessions from a file (one per line)
    #[arg(long)]
    pub accession_list: Option<PathBuf>,

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
    #[arg(long, conflicts_with = "zstd")]
    pub no_gzip: bool,

    /// Gzip compression level
    #[arg(long, default_value_t = 6, value_parser = clap::value_parser!(u32).range(1..=9))]
    pub gzip_level: u32,

    /// Use zstd compression instead of gzip
    #[arg(long, conflicts_with = "no_gzip")]
    pub zstd: bool,

    /// Zstd compression level (1-22)
    #[arg(long, default_value_t = 3, value_parser = clap::value_parser!(i32).range(1..=22))]
    pub zstd_level: i32,

    /// Output FASTA instead of FASTQ (drops quality scores)
    #[arg(long)]
    pub fasta: bool,

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

    /// Disable progress bar
    #[arg(long)]
    pub no_progress: bool,

    /// Disable download resume (re-download from scratch)
    #[arg(long)]
    pub no_resume: bool,

    /// Skip confirmation prompt for large downloads (>500 GiB)
    #[arg(short, long)]
    pub yes: bool,

    /// Skip direct S3 and resolve via the SDL API
    #[arg(long)]
    pub prefer_sdl: bool,
}

#[derive(Args)]
pub struct InfoArgs {
    /// SRA accession(s) to query (run, study, or BioProject)
    pub accessions: Vec<String>,

    /// Read accessions from a file (one per line)
    #[arg(long)]
    pub accession_list: Option<PathBuf>,
}

#[derive(Args)]
pub struct ValidateArgs {
    /// SRA file(s) to validate
    #[arg(required = true)]
    pub inputs: Vec<String>,

    /// Number of threads for decode
    #[arg(short, long, default_value_t = 8)]
    pub threads: usize,

    /// Disable progress bar
    #[arg(long)]
    pub no_progress: bool,
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
