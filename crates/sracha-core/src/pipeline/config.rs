//! `PipelineConfig` and `PipelineStats` — the public configuration and
//! result types shared across every pipeline entry point (`run_fastq`,
//! `run_get`, `decode_sra`).
//!
//! Extracted from the monolithic `pipeline/mod.rs` as part of the
//! pipeline refactor (no behavior change).

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::fastq::{CompressionMode, IntegrityDiag, PairedSuffix, SplitMode};

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
    /// Suffix style for paired/split FASTQ outputs (`_1`/`_2` vs `_R1`/`_R2`).
    pub paired_suffix: PairedSuffix,
    /// Place each accession's outputs (FASTQ + sidecars + temp SRA) inside
    /// its own subdirectory of `output_dir`, named after the accession.
    /// When `false`, all files land flat in `output_dir`.
    pub folder_per_accession: bool,
    /// Optional run-metadata sidecar format. When `Some`, [`decode_sra`]
    /// writes a `<accession>.metadata.{tsv,json}` file next to the FASTQ
    /// outputs after a successful decode.
    ///
    /// [`decode_sra`]: crate::pipeline::decode_sra
    pub metadata: Option<crate::metadata::MetadataFormat>,
    /// Primary download URL recorded into the metadata sidecar. Optional
    /// because the `fastq` subcommand (local file decode) has no URL.
    pub metadata_url: Option<String>,
    /// MD5 of the SRA payload recorded into the metadata sidecar.
    pub metadata_md5: Option<String>,
    /// SDL-reported SRA size in bytes recorded into the metadata sidecar.
    pub metadata_size: Option<u64>,
    /// Mirror service label (e.g. `s3`, `gs`, `ncbi`) recorded into the
    /// metadata sidecar.
    pub metadata_service: Option<String>,
}

impl PipelineConfig {
    /// Directory where outputs for `accession` should be written.
    /// With `folder_per_accession`, this is `output_dir / accession`.
    /// Otherwise it's `output_dir`.
    pub fn accession_output_dir(&self, accession: &str) -> std::path::PathBuf {
        if self.folder_per_accession {
            self.output_dir.join(accession)
        } else {
            self.output_dir.clone()
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(folder_per_accession: bool) -> PipelineConfig {
        PipelineConfig {
            output_dir: PathBuf::from("/tmp/x"),
            split_mode: crate::fastq::SplitMode::Split3,
            compression: crate::fastq::CompressionMode::None,
            threads: 1,
            connections: 1,
            skip_technical: true,
            min_read_len: None,
            force: false,
            progress: false,
            run_info: None,
            fasta: false,
            resume: true,
            stdout: false,
            cancelled: None,
            strict: false,
            http_client: None,
            keep_sra: false,
            paired_suffix: crate::fastq::PairedSuffix::Numeric,
            folder_per_accession,
            metadata: None,
            metadata_url: None,
            metadata_md5: None,
            metadata_size: None,
            metadata_service: None,
        }
    }

    #[test]
    fn accession_output_dir_flat() {
        let cfg = test_config(false);
        assert_eq!(cfg.accession_output_dir("SRR1"), PathBuf::from("/tmp/x"));
    }

    #[test]
    fn accession_output_dir_nested() {
        let cfg = test_config(true);
        assert_eq!(
            cfg.accession_output_dir("SRR1"),
            PathBuf::from("/tmp/x/SRR1"),
        );
    }
}
