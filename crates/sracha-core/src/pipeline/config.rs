//! `PipelineConfig` and `PipelineStats` — the public configuration and
//! result types shared across every pipeline entry point (`run_fastq`,
//! `run_get`, `decode_sra`).
//!
//! Extracted from the monolithic `pipeline/mod.rs` as part of the
//! pipeline refactor (no behavior change).

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::fastq::{CompressionMode, IntegrityDiag, SplitMode};

/// Configuration for the get pipeline.
#[derive(Clone)]
pub struct PipelineConfig {
    /// Directory for output files.
    pub output_dir: PathBuf,
    /// How to split reads across output files.
    pub split_mode: SplitMode,
    /// Output compression mode.
    pub compression: CompressionMode,
    /// Number of threads for decode/compression.
    pub threads: usize,
    /// Number of parallel HTTP connections for downloading.
    pub connections: usize,
    /// Skip technical reads.
    pub skip_technical: bool,
    /// Minimum read length filter.
    pub min_read_len: Option<u32>,
    /// Overwrite existing output files.
    pub force: bool,
    /// Show progress indicators.
    pub progress: bool,
    /// Read structure from NCBI EUtils (authoritative, when available).
    pub run_info: Option<crate::sdl::RunInfo>,
    /// Output FASTA instead of FASTQ (drops quality line).
    pub fasta: bool,
    /// Allow resuming partial downloads.
    pub resume: bool,
    /// Write output to stdout instead of files.
    pub stdout: bool,
    /// Flag for graceful cancellation (e.g. Ctrl-C).
    pub cancelled: Option<Arc<AtomicBool>>,
    /// Strict integrity mode: abort with [`crate::error::Error::IntegrityFailure`]
    /// if any quality-length / mate-pair / blob-truncation counter is non-zero
    /// at the end of decode, instead of merely reporting the counts.
    pub strict: bool,
    /// Shared HTTP client. When `Some`, `download_sra` threads it into
    /// [`crate::download::DownloadConfig`] so TLS sessions and connection
    /// pools are reused across accessions.
    pub http_client: Option<reqwest::Client>,
    /// Preserve the downloaded SRA file in the output directory after
    /// decode instead of deleting it. Useful for validation runs that
    /// want to compare against another tool on the same input file.
    pub keep_sra: bool,
}

/// Statistics from a completed pipeline run.
pub struct PipelineStats {
    /// The accession that was processed.
    pub accession: String,
    /// Number of spots (rows) read from the SRA file.
    pub spots_read: u64,
    /// Number of FASTQ reads written (after filtering).
    pub reads_written: u64,
    /// Bytes actually transferred over the network this session.
    pub bytes_transferred: u64,
    /// Total size of the full SRA file on the server.
    pub total_sra_size: u64,
    /// Paths of all output files created.
    pub output_files: Vec<PathBuf>,
    /// Data-integrity counters captured during decode. Strict mode is the
    /// default; pass `--no-strict` to downgrade any non-zero counter from a
    /// hard failure to a warning and inspect these values instead.
    pub integrity: Arc<IntegrityDiag>,
}
