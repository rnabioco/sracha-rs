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

    /// Inspect VDB structure of a local or remote .sra archive (replacement for vdb-dump)
    Vdb(VdbArgs),
}

#[derive(Args)]
pub struct VdbArgs {
    #[command(subcommand)]
    pub cmd: VdbCmd,
}

#[derive(Subcommand)]
pub enum VdbCmd {
    /// Print summary metadata (schema, platform, row counts, dates)
    Info {
        /// Local .sra path, https:// URL, or accession (fetched via HTTP range requests)
        source: String,
        /// Emit a single JSON object instead of human-readable text
        #[arg(long)]
        json: bool,
    },
    /// List tables in the archive (Database only)
    Tables {
        /// Local .sra path, https:// URL, or accession (fetched via HTTP range requests)
        source: String,
    },
    /// List columns in the chosen table
    Columns {
        /// Local .sra path, https:// URL, or accession (fetched via HTTP range requests)
        source: String,
        /// Table to inspect (defaults to SEQUENCE / first table)
        #[arg(short = 'T', long)]
        table: Option<String>,
        /// Show row counts, blob counts, and first-blob header stats per column
        #[arg(short = 's', long)]
        stats: bool,
    },
    /// Dump the metadata tree (schema/stats/LOAD/SOFTWARE nodes)
    Meta {
        /// Local .sra path, https:// URL, or accession (fetched via HTTP range requests)
        source: String,
        /// Table whose metadata tree to walk (defaults to SEQUENCE)
        #[arg(short = 'T', long)]
        table: Option<String>,
        /// Restrict to a sub-path like `STATS/TABLE` or `LOAD`
        #[arg(short = 'P', long)]
        path: Option<String>,
        /// Limit recursion depth below the chosen sub-path
        #[arg(short = 'd', long)]
        depth: Option<usize>,
        /// Walk the database-level tree (root `md/cur`) instead of a table tree
        #[arg(long)]
        db: bool,
    },
    /// Print the embedded schema text
    Schema {
        /// Local .sra path, https:// URL, or accession (fetched via HTTP range requests)
        source: String,
    },
    /// Print first row id and row count for the chosen table/column
    #[command(name = "id-range")]
    IdRange {
        /// Local .sra path, https:// URL, or accession (fetched via HTTP range requests)
        source: String,
        /// Table to inspect (defaults to SEQUENCE / first table)
        #[arg(short = 'T', long)]
        table: Option<String>,
        /// Column to read (defaults to the first column alphabetically)
        #[arg(short = 'C', long)]
        column: Option<String>,
    },
    /// Dump row-level data for the chosen columns
    Dump {
        /// Local .sra path, https:// URL, or accession (fetched via HTTP range requests)
        source: String,
        /// Table to dump from (defaults to SEQUENCE / first table)
        #[arg(short = 'T', long)]
        table: Option<String>,
        /// Comma-separated list of columns (default: all known SEQUENCE columns)
        #[arg(short = 'C', long, value_delimiter = ',')]
        columns: Vec<String>,
        /// Columns to exclude (applied after --columns)
        #[arg(short = 'x', long = "exclude", value_delimiter = ',')]
        exclude: Vec<String>,
        /// Row range, comma-separated. Examples: `5`, `5-20`, `100-`, `-50`, `1-10,200,500-`
        #[arg(short = 'R', long)]
        rows: Option<String>,
        /// Output format
        #[arg(short = 'f', long, default_value = "default")]
        format: DumpFormat,
        /// Advanced: skip type inference and render every column as hex
        /// bytes. Useful for debugging columns the heuristic doesn't
        /// recognize. Hidden from `--help` to keep the common surface small.
        #[arg(long, hide = true)]
        raw: bool,
    },
}

#[derive(Clone, Copy, ValueEnum)]
pub enum DumpFormat {
    /// vdb-dump-style row dump with quoted strings and bracketed arrays
    Default,
    /// Comma-separated values with standard CSV quoting
    Csv,
    /// Tab-separated values
    Tab,
    /// Newline-delimited JSON, one object per row
    Json,
}

impl From<DumpFormat> for sracha_core::vdb::dump::DumpFormat {
    fn from(f: DumpFormat) -> Self {
        match f {
            DumpFormat::Default => Self::Default,
            DumpFormat::Csv => Self::Csv,
            DumpFormat::Tab => Self::Tab,
            DumpFormat::Json => Self::Json,
        }
    }
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

    /// Skip the free-disk-space preflight check entirely. Useful when the
    /// output filesystem autoscales, or when `statvfs` reports a quota that
    /// doesn't reflect what the job can actually use.
    #[arg(long, help_heading = "Advanced")]
    pub no_disk_check: bool,

    /// Skip MD5 verification after download (verification is on by default)
    #[arg(long, help_heading = "Advanced")]
    pub no_validate: bool,

    /// Disable download resume (re-download from scratch)
    #[arg(long, help_heading = "Advanced")]
    pub no_resume: bool,

    /// Skip direct S3 and resolve via the SDL API
    #[arg(long, help_heading = "Advanced")]
    pub prefer_sdl: bool,

    /// Fetch pre-computed FASTQ.gz from ENA instead of the SRA binary.
    /// Falls back to the NCBI path when ENA has no FASTQ for an accession.
    #[arg(long, help_heading = "Advanced")]
    pub prefer_ena: bool,
}

#[derive(Args)]
pub struct DecodeArgs {
    /// Output directory
    #[arg(short = 'O', long, default_value = ".", help_heading = "Output")]
    pub output_dir: PathBuf,

    /// Write to stdout
    #[arg(short = 'Z', long, help_heading = "Output")]
    pub stdout: bool,

    /// Split mode
    #[arg(long, default_value = "split-3", help_heading = "Output")]
    pub split: SplitMode,

    /// Suffix style for paired/split FASTQ outputs.
    /// `numeric` -> `_1.fastq`/`_2.fastq` (default, matches fasterq-dump and ENA).
    /// `r` -> `_R1.fastq`/`_R2.fastq` (matches Illumina BCL output convention).
    #[arg(long, value_enum, default_value = "numeric", help_heading = "Output")]
    pub paired_suffix: PairedSuffix,

    /// Output FASTA instead of FASTQ (drops quality scores)
    #[arg(long, help_heading = "Output")]
    pub fasta: bool,

    /// Custom defline template (fasterq-dump `--seq-defline` syntax).
    /// Variables: $ac (accession) $si (spot id) $ri (read id) $sn (spot/read
    /// name) $rl (read length) $lg (LINKAGE_GROUP, e.g. 10x 'CB:..|UB:..';
    /// cSRA runs only); $$ is a literal '$'. Example: '@$ac.$si.$ri'.
    /// For PacBio/ONT, $sn is the native read name (e.g. PacBio
    /// 'm64012_.../ccs', ONT '<uuid> ch=.. start_time=..'); it is also the
    /// default header. The '+' line mirrors this. ($sg/spot-group unsupported.)
    #[arg(
        long,
        value_name = "TEMPLATE",
        help_heading = "Output",
        value_parser = fastq::DeflineTemplate::parse,
    )]
    pub seq_defline: Option<fastq::DeflineTemplate>,

    /// Custom template for the '+' line (same variables as --seq-defline).
    /// A leading '+' is optional; '+' alone writes a bare '+' line.
    /// Default: mirror --seq-defline. Ignored with --fasta.
    #[arg(
        long,
        value_name = "TEMPLATE",
        help_heading = "Output",
        value_parser = fastq::DeflineTemplate::parse_qual,
    )]
    pub qual_defline: Option<fastq::DeflineTemplate>,

    /// Overwrite existing files
    #[arg(short, long, help_heading = "Output")]
    pub force: bool,

    /// Place each accession's outputs (FASTQ + sidecars) inside its own
    /// subdirectory of the output directory, named after the accession.
    /// Default: all files flat in the output directory.
    #[arg(long, help_heading = "Output")]
    pub folder_per_accession: bool,

    /// Disable gzip compression (compressed by default)
    #[arg(
        long,
        conflicts_with_all = ["stdout", "zstd", "zstd_level", "gzip_level"],
        help_heading = "Compression",
    )]
    pub no_gzip: bool,

    /// Gzip compression level [default: 1]
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
    #[arg(short, long, default_value_t = 8, display_order = 102)]
    pub threads: usize,

    /// Disable progress bar
    #[arg(long, display_order = 105)]
    pub no_progress: bool,

    /// Disable strict integrity checking. By default, sracha fails on any
    /// data-integrity anomaly it cannot silently fall back on: quality or
    /// sequence streams that do not correspond to each other, decoded bases
    /// or spots that disagree with the archive's own recorded totals,
    /// invalid quality bytes, quality overruns, and paired-spot violations.
    /// Pass this flag to downgrade those failures to warnings. Benign-fallback
    /// counters (SRA-lite all-zero quality blobs and truncated-spot recovery)
    /// stay informational either way.
    #[arg(long, help_heading = "Advanced", display_order = 7)]
    pub no_strict: bool,

    /// Verify decoded quality values against the archive's STATS/QUALITY
    /// histogram. Every other quality check compares lengths, so this is the
    /// only one that notices a decode emitting the right number of plausible
    /// quality bytes at the wrong values. Costs one increment per base and
    /// is skipped for archives that synthesize quality (SRA-Lite, --fasta).
    #[arg(long, help_heading = "Advanced", display_order = 8)]
    pub verify: bool,

    /// Directory for cached external reference sequences, used by aligned
    /// (cSRA) runs whose reference bases live in NCBI refseq objects rather
    /// than in the archive itself. Shared across accessions and runs.
    /// Defaults to $SRACHA_REFSEQ_DIR, else the platform cache dir
    /// (~/.cache/sracha/refseq, or %LOCALAPPDATA%\sracha\refseq on Windows).
    #[arg(
        long,
        value_name = "DIR",
        help_heading = "Advanced",
        display_order = 14
    )]
    pub refseq_cache: Option<PathBuf>,
}

#[derive(Args)]
pub struct FastqArgs {
    /// SRA file path(s) (.sra files from `sracha fetch`)
    #[arg(required = true)]
    pub inputs: Vec<String>,

    #[command(flatten)]
    pub decode: DecodeArgs,
}

#[derive(Args)]
#[command(mut_arg(
    "stdout",
    |a| a.help("Write to stdout (stream interleaved FASTQ, auto-delete temp SRA)")
))]
pub struct GetArgs {
    /// SRA accession(s) to download and convert (run, study, or BioProject)
    pub accessions: Vec<String>,

    /// Read accessions from a file (one per line)
    // Sorts ahead of the global -v/-q (small implicit orders); the rest of
    // the Options section follows them, hence the 100+ numbers below.
    #[arg(long, display_order = 1)]
    pub accession_list: Option<PathBuf>,

    #[command(flatten)]
    pub decode: DecodeArgs,

    /// HTTP connections per file
    #[arg(long, default_value_t = 8, display_order = 103)]
    pub connections: usize,

    /// Write run metadata (BioSample, library, instrument, etc.) sidecar
    /// file(s) alongside each FASTQ output.
    #[arg(long, value_enum, help_heading = "Output")]
    pub metadata: Option<MetadataFormat>,

    /// Number of accessions to download ahead of the decoder. A larger
    /// value hides slow networks behind decode, at the cost of one extra
    /// temp SRA file per depth step. Only applies to multi-accession
    /// `get` runs.
    #[arg(
        long,
        default_value_t = 2,
        help_heading = "Advanced",
        display_order = 1
    )]
    pub prefetch_depth: usize,

    /// Confirm project downloads and large downloads (>100 GiB)
    #[arg(short, long, display_order = 104)]
    pub yes: bool,

    /// Download format
    #[arg(
        long,
        default_value = "sra",
        help_heading = "Advanced",
        display_order = 2
    )]
    pub format: SraFormat,

    /// Disable download resume (re-download from scratch)
    #[arg(long, help_heading = "Advanced", display_order = 3)]
    pub no_resume: bool,

    /// Skip the free-disk-space preflight check entirely. Useful when the
    /// output filesystem autoscales, or when `statvfs` reports a quota that
    /// doesn't reflect what the job can actually use.
    #[arg(long, help_heading = "Advanced", display_order = 4)]
    pub no_disk_check: bool,

    /// Skip the EUtils RunInfo API call (read structure will be derived
    /// from VDB file metadata instead).
    #[arg(long, help_heading = "Advanced", display_order = 5)]
    pub no_runinfo: bool,

    /// Skip direct S3 and resolve via the SDL API
    #[arg(long, help_heading = "Advanced", display_order = 6)]
    pub prefer_sdl: bool,

    /// Keep the downloaded SRA file in the output directory instead of
    /// deleting it after decode. Useful for validation runs that want
    /// to re-run another tool (e.g. `fasterq-dump`) on the same input.
    #[arg(long, help_heading = "Advanced", display_order = 9)]
    pub keep_sra: bool,

    /// Try ENA FASTQ mirrors first; fall back to NCBI if ENA has no FASTQ
    /// or the output config is incompatible. Requires gzip compression and
    /// split-3 or split-files (Phase 1 scope). Skips VDB decode entirely.
    #[arg(long, help_heading = "Advanced", display_order = 10)]
    pub prefer_ena: bool,

    /// Max concurrent S3 HEAD probes during accession resolution.
    /// Default 64; clamped to 1–256. The HTTP client's per-host
    /// connection pool is sized to match this value at startup, so
    /// raising it actually opens more sockets to the SRA bucket
    /// (rather than queueing on the pool). Values above ~128 rarely
    /// help and risk hitting bucket-side rate limits.
    #[arg(
        long,
        help_heading = "Advanced",
        value_name = "N",
        default_value_t = sracha_core::s3::DEFAULT_PROBE_CONCURRENCY,
        value_parser = parse_head_concurrency,
        display_order = 11,
    )]
    pub head_concurrency: usize,

    /// Resolve accessions and print what would be downloaded as TSV (default)
    /// or JSON. Does not download or decode anything.
    #[arg(long, help_heading = "Advanced", display_order = 12)]
    pub dry_run: bool,

    /// Format for `--dry-run` output.
    #[arg(
        long,
        value_enum,
        default_value = "tsv",
        help_heading = "Advanced",
        display_order = 13
    )]
    pub dry_run_format: DryRunFormat,
}

impl DecodeArgs {
    /// The pipeline settings every decoding command shares. Callers layer on
    /// what is specific to them (`..` struct update), so a new shared flag is
    /// declared once above and mapped once here.
    pub fn pipeline_config(
        &self,
        split_mode: fastq::SplitMode,
        compression: CompressionMode,
    ) -> sracha_core::pipeline::PipelineConfig {
        sracha_core::pipeline::PipelineConfig {
            output_dir: self.output_dir.clone(),
            split_mode,
            compression,
            threads: self.threads,
            skip_technical: !self.include_technical,
            min_read_len: self.min_read_len,
            force: self.force,
            progress: !self.no_progress && !self.stdout,
            fasta: self.fasta,
            stdout: self.stdout,
            strict: !self.no_strict,
            verify: self.verify,
            paired_suffix: self.paired_suffix.into(),
            seq_defline: self.seq_defline.clone(),
            qual_defline: self.qual_defline.clone(),
            folder_per_accession: self.folder_per_accession,
            refseq_cache_dir: self.refseq_cache.clone(),
            ..Default::default()
        }
    }
}

// `fastq` and `get` both flatten `DecodeArgs`; deref to it so command code can
// keep reading `args.threads`, `args.fasta`, ... without a `.decode` hop.
impl std::ops::Deref for FastqArgs {
    type Target = DecodeArgs;
    fn deref(&self) -> &DecodeArgs {
        &self.decode
    }
}

impl std::ops::Deref for GetArgs {
    type Target = DecodeArgs;
    fn deref(&self) -> &DecodeArgs {
        &self.decode
    }
}

#[derive(Clone, Copy, ValueEnum, Default)]
pub enum DryRunFormat {
    #[default]
    Tsv,
    Json,
}

const HEAD_CONCURRENCY_MAX: usize = 256;

fn parse_head_concurrency(s: &str) -> Result<usize, String> {
    let n: usize = s
        .parse()
        .map_err(|_| format!("`{s}` is not a non-negative integer"))?;
    if !(1..=HEAD_CONCURRENCY_MAX).contains(&n) {
        return Err(format!(
            "head-concurrency must be between 1 and {HEAD_CONCURRENCY_MAX} (got {n})",
        ));
    }
    Ok(n)
}

#[derive(Args)]
pub struct InfoArgs {
    /// SRA accession(s) to query, or local `.sra` file path(s)
    pub accessions: Vec<String>,

    /// Read accessions from a file (one per line)
    #[arg(long)]
    pub accession_list: Option<PathBuf>,

    /// Query ENA's filereport API and show the FASTQ file listing
    /// (URLs, sizes, MD5s) alongside NCBI info.
    #[arg(long)]
    pub prefer_ena: bool,

    /// Output format. `table` is human-readable; `tsv`/`csv` emit a single
    /// header row plus one record per accession for pipeline use; `json`
    /// emits an array with every field, including title, study and design.
    #[arg(long, value_enum, default_value_t = InfoFormat::Table)]
    pub format: InfoFormat,
}

#[derive(Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum InfoFormat {
    /// Human-readable table (default)
    Table,
    /// Tab-separated values, one record per accession
    Tsv,
    /// Comma-separated values, one record per accession
    Csv,
    /// JSON array, one object per accession, with every field (including
    /// title, study and design)
    Json,
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

    /// Expected MD5 hash (hex). When set, validate fails on mismatch.
    /// Apply to a single input; with multiple inputs every file must match.
    #[arg(long)]
    pub md5: Option<String>,

    /// Skip the SDL lookup for the expected MD5 (offline only)
    #[arg(long)]
    pub offline: bool,
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
pub enum MetadataFormat {
    /// Write a single TSV sidecar (`<acc>.metadata.tsv`).
    Tsv,
    /// Write a single JSON sidecar (`<acc>.metadata.json`).
    Json,
    /// Write both TSV and JSON sidecars.
    Both,
}

impl From<MetadataFormat> for sracha_core::metadata::MetadataFormat {
    fn from(f: MetadataFormat) -> Self {
        match f {
            MetadataFormat::Tsv => sracha_core::metadata::MetadataFormat::Tsv,
            MetadataFormat::Json => sracha_core::metadata::MetadataFormat::Json,
            MetadataFormat::Both => sracha_core::metadata::MetadataFormat::Both,
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

#[derive(Clone, Copy, ValueEnum, Default)]
pub enum PairedSuffix {
    /// `_1.fastq` / `_2.fastq` (default, matches fasterq-dump and ENA)
    #[default]
    Numeric,
    /// `_R1.fastq` / `_R2.fastq` (matches Illumina BCL output convention)
    R,
}

impl From<PairedSuffix> for fastq::PairedSuffix {
    fn from(s: PairedSuffix) -> Self {
        match s {
            PairedSuffix::Numeric => fastq::PairedSuffix::Numeric,
            PairedSuffix::R => fastq::PairedSuffix::R,
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
            level: gzip_level.unwrap_or(1),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    /// The rendered `<sub> --help`, unwrapped so it does not depend on the
    /// terminal the tests run in.
    fn help(sub: &str) -> String {
        Cli::command()
            .term_width(0)
            .try_get_matches_from(["sracha", sub, "--help"])
            .unwrap_err()
            .to_string()
    }

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(std::iter::once("sracha").chain(args.iter().copied())).unwrap()
    }

    fn fastq_args(args: &[&str]) -> FastqArgs {
        match parse(args).command {
            Command::Fastq(a) => a,
            _ => panic!("not a fastq command"),
        }
    }

    fn get_args(args: &[&str]) -> GetArgs {
        match parse(args).command {
            Command::Get(a) => a,
            _ => panic!("not a get command"),
        }
    }

    // `fastq` and `get` flatten one `DecodeArgs`, so their `--help` must not
    // move when a shared flag is added. These snapshots were captured before
    // the flags were shared and pin heading, order and wording.

    #[test]
    fn fastq_help_is_unchanged() {
        assert_eq!(
            help("fastq"),
            include_str!("../tests/snapshots/fastq-help.txt")
        );
    }

    #[test]
    fn get_help_is_unchanged() {
        assert_eq!(help("get"), include_str!("../tests/snapshots/get-help.txt"));
    }

    #[test]
    fn get_and_fastq_share_decode_flags() {
        let shared = [
            "--split",
            "split-spot",
            "--fasta",
            "--force",
            "--folder-per-accession",
            "--paired-suffix",
            "r",
            "--min-read-len",
            "30",
            "--include-technical",
            "--threads",
            "3",
            "--no-gzip",
            "--no-strict",
            "--verify",
            "--seq-defline",
            "@$ac.$si",
            "--qual-defline",
            "+",
            "-O",
            "out",
            "--refseq-cache",
            "/tmp/refseq",
        ];
        let mut fq = vec!["fastq", "x.sra"];
        fq.extend(shared);
        let mut get = vec!["get", "SRR1"];
        get.extend(shared);

        let (f, g) = (fastq_args(&fq), get_args(&get));
        let split = cli_split(&f);
        let a = f.pipeline_config(split, CompressionMode::None);
        let b = g.pipeline_config(split, CompressionMode::None);

        assert_eq!(a.output_dir, b.output_dir);
        assert_eq!(a.output_dir, PathBuf::from("out"));
        assert_eq!(a.threads, 3);
        assert_eq!(b.threads, 3);
        assert!(a.fasta && b.fasta);
        assert!(a.force && b.force);
        assert!(a.folder_per_accession && b.folder_per_accession);
        assert!(!a.skip_technical && !b.skip_technical);
        assert!(!a.strict && !b.strict);
        assert!(a.verify && b.verify);
        assert_eq!(a.min_read_len, Some(30));
        assert_eq!(b.min_read_len, Some(30));
        assert!(a.seq_defline.is_some() && b.seq_defline.is_some());
        assert!(a.qual_defline.is_some() && b.qual_defline.is_some());
        assert_eq!(a.refseq_cache_dir, Some(PathBuf::from("/tmp/refseq")));
        assert_eq!(b.refseq_cache_dir, Some(PathBuf::from("/tmp/refseq")));
        assert!(matches!(a.paired_suffix, fastq::PairedSuffix::R));
        assert!(matches!(b.paired_suffix, fastq::PairedSuffix::R));
        assert!(matches!(a.split_mode, fastq::SplitMode::SplitSpot));
    }

    fn cli_split(a: &FastqArgs) -> fastq::SplitMode {
        resolve_split_mode(a.split, a.stdout).unwrap()
    }

    #[test]
    fn fastq_overrides_connections_and_resume() {
        // `fastq` works on a local file: one connection, resumable, no
        // sidecar or run-info. That is exactly the shared base, so it adds
        // no overrides; `get` layers its own download settings on top.
        let f = fastq_args(&["fastq", "x.sra"]);
        let cfg = f.pipeline_config(fastq::SplitMode::Split3, CompressionMode::None);
        assert_eq!(cfg.connections, 1);
        assert!(cfg.resume);
        assert!(cfg.metadata.is_none() && cfg.metadata_row.is_none());
        assert!(cfg.run_info.is_none() && cfg.http_client.is_none() && cfg.cancelled.is_none());
        assert!(!cfg.keep_sra);

        let g = get_args(&[
            "get",
            "SRR1",
            "--connections",
            "4",
            "--no-resume",
            "--keep-sra",
        ]);
        assert_eq!(g.connections, 4);
        assert!(g.no_resume && g.keep_sra);
        // get's own defaults are unchanged by the refactor.
        assert_eq!(get_args(&["get", "SRR1"]).connections, 8);
    }

    #[test]
    fn progress_follows_no_progress_and_stdout() {
        let on = fastq_args(&["fastq", "x.sra"]);
        assert!(
            on.pipeline_config(fastq::SplitMode::Split3, CompressionMode::None)
                .progress
        );
        let off = fastq_args(&["fastq", "x.sra", "--no-progress"]);
        assert!(
            !off.pipeline_config(fastq::SplitMode::Split3, CompressionMode::None)
                .progress
        );
        let stdout = fastq_args(&["fastq", "x.sra", "-Z", "--split", "interleaved"]);
        assert!(
            !stdout
                .pipeline_config(fastq::SplitMode::Interleaved, CompressionMode::None)
                .progress
        );
    }
}
