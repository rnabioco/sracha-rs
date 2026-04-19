use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid accession format: {0}")]
    InvalidAccession(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("SDL API error: {message}")]
    Sdl { message: String },

    #[error("ENA API error: {message}")]
    Ena { message: String },

    #[error("download failed for {accession}: {message}")]
    Download { accession: String, message: String },

    #[error("checksum mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch { expected: String, actual: String },

    #[error("spot count mismatch for {accession}: expected {expected}, got {actual}")]
    SpotCountMismatch {
        accession: String,
        expected: u64,
        actual: u64,
    },

    #[error(transparent)]
    Vdb(#[from] sracha_vdb::Error),

    #[error("pipeline error: {0}")]
    Pipeline(String),

    #[error(
        "unsupported platform: {platform} — sracha does not support legacy sequencing platforms"
    )]
    UnsupportedPlatform { platform: String },

    #[error("file not found: {0}")]
    FileNotFound(PathBuf),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("operation cancelled")]
    Cancelled {
        /// Partial output files created before cancellation (for cleanup).
        output_files: Vec<PathBuf>,
    },

    #[error("integrity check failed for {accession}: {summary}")]
    IntegrityFailure { accession: String, summary: String },
}

pub type Result<T> = std::result::Result<T, Error>;

/// Guidance text shown whenever a decode-time per-blob integrity check fails.
/// Explains to the user which of the two action paths applies to them based
/// on how they invoked the download.
pub const BLOB_INTEGRITY_GUIDANCE: &str = "\
If `sracha fetch` ran with MD5 verification (default), the downloaded bytes \
match NCBI's source — this is likely a decoder bug in sracha, please report \
at https://github.com/rnabioco/sracha-rs/issues. If you used `--no-validate`, \
re-run without it to rule out a bad transfer.";
