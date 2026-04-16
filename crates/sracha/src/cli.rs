use std::path::PathBuf;

use clap::builder::styling::{AnsiColor, Effects, Styles};
use clap::{Args, Parser, Subcommand, ValueEnum};
use sracha_core::fastq::{self, CompressionMode};
use sracha_core::sdl::FormatPreference;

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
    /// Download, convert, and compress in one shot
    Get(GetArgs),

    /// Download SRA/SRA-lite files
    Fetch(FetchArgs),

    /// Convert SRA file(s) to FASTQ
    Fastq(FastqArgs),

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
    #[arg(short = 'O', long, default_value = ".", help_heading = "Output")]
    pub output_dir: PathBuf,

    /// Download format
    #[arg(long, default_value = "sra", help_heading = "Output")]
    pub format: SraFormat,

    /// Overwrite existing files
    #[arg(short, long, help_heading = "Output")]
    pub force: bool,

    /// HTTP connections per file
    #[arg(long, default_value_t = 8)]
    pub connections: usize,

    /// Confirm project downloads and large downloads (>100 GiB)
    #[arg(short, long)]
    pub yes: bool,

    /// Disable progress bar
    #[arg(long)]
    pub no_progress: bool,

    /// Verify MD5 after download
    #[arg(long, help_heading = "Advanced")]
    pub validate: bool,

    /// Disable download resume (re-download from scratch)
    #[arg(long, help_heading = "Advanced")]
    pub no_resume: bool,

    /// Skip direct S3 and resolve via the SDL API
    #[arg(long, help_heading = "Advanced")]
    pub prefer_sdl: bool,
}

#[derive(Args)]
pub struct FastqArgs {
    /// SRA file path(s) (.sra files from `sracha fetch`)
    #[arg(required = true)]
    pub inputs: Vec<String>,

    /// Output directory
    #[arg(short = 'O', long, default_value = ".", help_heading = "Output")]
    pub output_dir: PathBuf,

    /// Write to stdout
    #[arg(short = 'Z', long, help_heading = "Output")]
    pub stdout: bool,

    /// Split mode
    #[arg(long, default_value = "split-3", help_heading = "Output")]
    pub split: SplitMode,

    /// Output FASTA instead of FASTQ (drops quality scores)
    #[arg(long, help_heading = "Output")]
    pub fasta: bool,

    /// Overwrite existing files
    #[arg(short, long, help_heading = "Output")]
    pub force: bool,

    /// Disable gzip compression (compressed by default)
    #[arg(long, conflicts_with = "zstd", help_heading = "Compression")]
    pub no_gzip: bool,

    /// Gzip compression level
    #[arg(long, default_value_t = 6, value_parser = clap::value_parser!(u32).range(1..=9), help_heading = "Compression")]
    pub gzip_level: u32,

    /// Use zstd compression instead of gzip
    #[arg(long, conflicts_with = "no_gzip", help_heading = "Compression")]
    pub zstd: bool,

    /// Zstd compression level (1-22)
    #[arg(long, default_value_t = 3, value_parser = clap::value_parser!(i32).range(1..=22), help_heading = "Compression")]
    pub zstd_level: i32,

    /// Minimum read length
    #[arg(long, help_heading = "Filtering")]
    pub min_read_len: Option<u32>,

    /// Include technical reads (skipped by default)
    #[arg(long, help_heading = "Filtering")]
    pub include_technical: bool,

    /// Number of threads for decode [default: 8]
    #[arg(short, long, default_value_t = 8)]
    pub threads: usize,

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
    #[arg(short = 'O', long, default_value = ".", help_heading = "Output")]
    pub output_dir: PathBuf,

    /// Write to stdout (stream interleaved FASTQ, auto-delete temp SRA)
    #[arg(short = 'Z', long, help_heading = "Output")]
    pub stdout: bool,

    /// Split mode
    #[arg(long, default_value = "split-3", help_heading = "Output")]
    pub split: SplitMode,

    /// Output FASTA instead of FASTQ (drops quality scores)
    #[arg(long, help_heading = "Output")]
    pub fasta: bool,

    /// Overwrite existing files
    #[arg(short, long, help_heading = "Output")]
    pub force: bool,

    /// Disable gzip compression (compressed by default)
    #[arg(long, conflicts_with = "zstd", help_heading = "Compression")]
    pub no_gzip: bool,

    /// Gzip compression level
    #[arg(long, default_value_t = 6, value_parser = clap::value_parser!(u32).range(1..=9), help_heading = "Compression")]
    pub gzip_level: u32,

    /// Use zstd compression instead of gzip
    #[arg(long, conflicts_with = "no_gzip", help_heading = "Compression")]
    pub zstd: bool,

    /// Zstd compression level (1-22)
    #[arg(long, default_value_t = 3, value_parser = clap::value_parser!(i32).range(1..=22), help_heading = "Compression")]
    pub zstd_level: i32,

    /// Minimum read length
    #[arg(long, help_heading = "Filtering")]
    pub min_read_len: Option<u32>,

    /// Include technical reads (skipped by default)
    #[arg(long, help_heading = "Filtering")]
    pub include_technical: bool,

    /// Number of threads for decode [default: 8]
    #[arg(short, long, default_value_t = 8)]
    pub threads: usize,

    /// HTTP connections per file
    #[arg(long, default_value_t = 8)]
    pub connections: usize,

    /// Confirm project downloads and large downloads (>100 GiB)
    #[arg(short, long)]
    pub yes: bool,

    /// Disable progress bar
    #[arg(long)]
    pub no_progress: bool,

    /// Download format
    #[arg(long, default_value = "sra", help_heading = "Advanced")]
    pub format: SraFormat,

    /// Disable download resume (re-download from scratch)
    #[arg(long, help_heading = "Advanced")]
    pub no_resume: bool,

    /// Skip the EUtils RunInfo API call (read structure will be derived
    /// from VDB file metadata instead).
    #[arg(long, help_heading = "Advanced")]
    pub no_runinfo: bool,

    /// Skip direct S3 and resolve via the SDL API
    #[arg(long, help_heading = "Advanced")]
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
    /// Full quality scores
    Sra,
    /// Simplified quality scores (smaller files)
    Sralite,
}

impl From<SraFormat> for FormatPreference {
    fn from(f: SraFormat) -> Self {
        match f {
            SraFormat::Sra => FormatPreference::Sra,
            SraFormat::Sralite => FormatPreference::Sralite,
        }
    }
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

impl From<SplitMode> for fastq::SplitMode {
    fn from(m: SplitMode) -> Self {
        match m {
            SplitMode::Split3 => fastq::SplitMode::Split3,
            SplitMode::SplitFiles => fastq::SplitMode::SplitFiles,
            SplitMode::SplitSpot => fastq::SplitMode::SplitSpot,
            SplitMode::Interleaved => fastq::SplitMode::Interleaved,
        }
    }
}

/// Resolve the effective split mode, forcing interleaved when writing to stdout.
pub fn resolve_split_mode(split: SplitMode, stdout: bool) -> fastq::SplitMode {
    if stdout {
        fastq::SplitMode::Interleaved
    } else {
        split.into()
    }
}

/// Resolve the effective compression mode from CLI flags.
pub fn resolve_compression(
    stdout: bool,
    zstd: bool,
    zstd_level: i32,
    threads: usize,
    no_gzip: bool,
    gzip_level: u32,
) -> CompressionMode {
    if stdout || no_gzip {
        CompressionMode::None
    } else if zstd {
        CompressionMode::Zstd {
            level: zstd_level,
            threads: threads as u32,
        }
    } else {
        CompressionMode::Gzip { level: gzip_level }
    }
}
