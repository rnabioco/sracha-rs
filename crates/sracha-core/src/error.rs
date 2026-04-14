use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid accession format: {0}")]
    InvalidAccession(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("SDL API error: {message}")]
    Sdl { message: String },

    #[error("download failed for {accession}: {message}")]
    Download { accession: String, message: String },

    #[error("checksum mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch { expected: String, actual: String },

    #[error("invalid KAR archive: {0}")]
    InvalidKar(String),

    #[error("VDB format error: {0}")]
    Vdb(String),

    #[error("column not found: {table}/{column}")]
    ColumnNotFound { table: String, column: String },

    #[error("unsupported encoding: {0}")]
    UnsupportedEncoding(String),

    #[error("file not found: {0}")]
    FileNotFound(PathBuf),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
