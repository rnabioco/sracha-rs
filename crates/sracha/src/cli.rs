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

    /// Convert SRA file(s) to Parquet or Vortex (experimental,
    /// storage benchmarking)
    #[cfg(any(feature = "parquet", feature = "vortex"))]
    Convert(ConvertArgs),

    /// Show accession metadata
    Info(InfoArgs),

    /// Validate SRA file integrity
    Validate(ValidateArgs),
}

#[cfg(any(feature = "parquet", feature = "vortex"))]
#[derive(Args)]
pub struct ConvertArgs {
    /// SRA file path(s) (.sra files from `sracha fetch`)
    #[arg(required = true)]
    pub inputs: Vec<String>,

    /// Output directory (one file per input; extension reflects `--format`)
    #[arg(short = 'O', long, default_value = ".", help_heading = "Output")]
    pub output_dir: PathBuf,

    /// Force-overwrite existing output files
    #[arg(short, long, help_heading = "Output")]
    pub force: bool,

    /// Output format. Parquet writes `.parquet` with user-selectable compression;
    /// Vortex writes `.vortex` with its own encoding cascade (Vortex picks).
    #[arg(long, default_value_t = ConvertFormat::default(), help_heading = "Output")]
    pub format: ConvertFormat,

    /// DNA encoding for the `sequence` column.
    ///
    /// Default depends on `--format`: `two-na` for parquet, `ascii` for vortex.
    /// Vortex defaults to `ascii` because the Vortex 0.68 compressor skips
    /// every scheme for `Binary`-typed columns, so 2na/4na-packed sequence is
    /// written uncompressed; `ascii` lets BtrBlocks' FSST cascade fire on the
    /// 4-letter alphabet.
    #[arg(long, help_heading = "Encoding")]
    pub pack_dna: Option<PackDna>,

    /// Read-length mode for the schema
    #[arg(long, default_value = "auto", help_heading = "Encoding")]
    pub length_mode: LengthMode,

    /// Page-level compression codec (parquet only; ignored for vortex)
    #[cfg(feature = "parquet")]
    #[arg(long, default_value = "zstd", help_heading = "Compression")]
    pub compression: ParquetCodec,

    /// Zstd compression level (1-22), only when `--compression zstd` (parquet only)
    #[cfg(feature = "parquet")]
    #[arg(
        long,
        value_parser = clap::value_parser!(i32).range(1..=22),
        default_value_t = 22,
        help_heading = "Compression",
    )]
    pub zstd_level: i32,

    /// Target row-group size in MiB (parquet only)
    #[cfg(feature = "parquet")]
    #[arg(long, default_value_t = 256, help_heading = "Advanced")]
    pub row_group_mib: usize,
}

#[cfg(any(feature = "parquet", feature = "vortex"))]
#[derive(Clone, Copy, ValueEnum)]
pub enum ConvertFormat {
    /// Apache Parquet (`.parquet`) — user-selectable compression.
    #[cfg(feature = "parquet")]
    Parquet,
    /// Vortex (`.vortex`) — SpiralDB columnar, encoding cascade picked automatically.
    #[cfg(feature = "vortex")]
    Vortex,
}

#[cfg(any(feature = "parquet", feature = "vortex"))]
impl ConvertFormat {
    /// Default-feature-aware default: prefer parquet when both are enabled
    /// (matches the historical `default_value = "parquet"`), else fall
    /// back to whichever format is compiled in.
    pub fn default() -> Self {
        #[cfg(feature = "parquet")]
        {
            Self::Parquet
        }
        #[cfg(all(not(feature = "parquet"), feature = "vortex"))]
        {
            Self::Vortex
        }
    }
}

#[cfg(any(feature = "parquet", feature = "vortex"))]
impl std::fmt::Display for ConvertFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            #[cfg(feature = "parquet")]
            Self::Parquet => "parquet",
            #[cfg(feature = "vortex")]
            Self::Vortex => "vortex",
        })
    }
}

#[cfg(any(feature = "parquet", feature = "vortex"))]
#[derive(Clone, Copy, ValueEnum)]
pub enum PackDna {
    /// One byte per base (A/C/G/T/N/IUPAC)
    Ascii,
    /// 2 bits/base, 4× density (auto-falls back to 4na on ambiguity)
    #[value(name = "two-na")]
    TwoNa,
    /// 4 bits/base, preserves IUPAC ambiguity
    #[value(name = "four-na")]
    FourNa,
}

#[cfg(any(feature = "parquet", feature = "vortex"))]
impl From<PackDna> for sracha_core::convert::schema::DnaPacking {
    fn from(p: PackDna) -> Self {
        match p {
            PackDna::Ascii => Self::Ascii,
            PackDna::TwoNa => Self::TwoNa,
            PackDna::FourNa => Self::FourNa,
        }
    }
}

#[cfg(any(feature = "parquet", feature = "vortex"))]
#[derive(Clone, Copy, ValueEnum)]
pub enum LengthMode {
    /// Detect from data: fixed if all reads share a length, else variable
    Auto,
    /// Force FIXED_LEN_BYTE_ARRAY (errors on length mismatch)
    Fixed,
    /// Force variable-length BYTE_ARRAY
    Variable,
}

#[cfg(any(feature = "parquet", feature = "vortex"))]
impl From<LengthMode> for sracha_core::convert::schema::LengthModeChoice {
    fn from(m: LengthMode) -> Self {
        match m {
            LengthMode::Auto => Self::Auto,
            LengthMode::Fixed => Self::Fixed,
            LengthMode::Variable => Self::Variable,
        }
    }
}

#[cfg(feature = "parquet")]
#[derive(Clone, Copy, ValueEnum)]
pub enum ParquetCodec {
    None,
    Snappy,
    Zstd,
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
    #[arg(
        long,
        conflicts_with_all = ["stdout", "zstd", "zstd_level", "gzip_level"],
        help_heading = "Compression",
    )]
    pub no_gzip: bool,

    /// Gzip compression level [default: 6]
    #[arg(
        long,
        value_parser = clap::value_parser!(u32).range(1..=9),
        conflicts_with_all = ["stdout", "no_gzip", "zstd", "zstd_level"],
        help_heading = "Compression",
    )]
    pub gzip_level: Option<u32>,

    /// Use zstd compression instead of gzip
    #[arg(
        long,
        conflicts_with_all = ["stdout", "no_gzip", "gzip_level"],
        help_heading = "Compression",
    )]
    pub zstd: bool,

    /// Zstd compression level (1-22) [default: 3]
    #[arg(
        long,
        value_parser = clap::value_parser!(i32).range(1..=22),
        conflicts_with_all = ["stdout", "no_gzip", "gzip_level"],
        help_heading = "Compression",
    )]
    pub zstd_level: Option<i32>,

    /// Minimum read length
    #[arg(long, help_heading = "Filtering")]
    pub min_read_len: Option<u32>,

    /// Include technical reads (skipped by default)
    #[arg(long, help_heading = "Filtering")]
    pub include_technical: bool,

    /// Number of threads for decode
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
    #[arg(
        long,
        conflicts_with_all = ["stdout", "zstd", "zstd_level", "gzip_level"],
        help_heading = "Compression",
    )]
    pub no_gzip: bool,

    /// Gzip compression level [default: 6]
    #[arg(
        long,
        value_parser = clap::value_parser!(u32).range(1..=9),
        conflicts_with_all = ["stdout", "no_gzip", "zstd", "zstd_level"],
        help_heading = "Compression",
    )]
    pub gzip_level: Option<u32>,

    /// Use zstd compression instead of gzip
    #[arg(
        long,
        conflicts_with_all = ["stdout", "no_gzip", "gzip_level"],
        help_heading = "Compression",
    )]
    pub zstd: bool,

    /// Zstd compression level (1-22) [default: 3]
    #[arg(
        long,
        value_parser = clap::value_parser!(i32).range(1..=22),
        conflicts_with_all = ["stdout", "no_gzip", "gzip_level"],
        help_heading = "Compression",
    )]
    pub zstd_level: Option<i32>,

    /// Minimum read length
    #[arg(long, help_heading = "Filtering")]
    pub min_read_len: Option<u32>,

    /// Include technical reads (skipped by default)
    #[arg(long, help_heading = "Filtering")]
    pub include_technical: bool,

    /// Number of threads for decode
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
    /// SRA accession(s) to query, or local `.sra` file path(s)
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

/// Resolve the effective split mode. `--stdout/-Z` requires `--split interleaved`.
pub fn resolve_split_mode(split: SplitMode, stdout: bool) -> Result<fastq::SplitMode, String> {
    if stdout && !matches!(split, SplitMode::Interleaved) {
        return Err(
            "--stdout/-Z only supports --split interleaved (stdout streams a single FASTQ stream)"
                .into(),
        );
    }
    Ok(split.into())
}

/// Resolve the effective compression mode from CLI flags.
///
/// Clap's `conflicts_with_all` rejects incompatible combinations at parse time, so this
/// function only handles the remaining implication: `--zstd-level N` alone implies `--zstd`.
pub fn resolve_compression(
    stdout: bool,
    zstd: bool,
    zstd_level: Option<i32>,
    threads: usize,
    no_gzip: bool,
    gzip_level: Option<u32>,
) -> CompressionMode {
    if stdout || no_gzip {
        CompressionMode::None
    } else if zstd || zstd_level.is_some() {
        CompressionMode::Zstd {
            level: zstd_level.unwrap_or(3),
            threads: threads as u32,
        }
    } else {
        CompressionMode::Gzip {
            level: gzip_level.unwrap_or(6),
        }
    }
}
