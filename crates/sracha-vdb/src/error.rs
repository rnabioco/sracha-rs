#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid KAR archive: {0}")]
    InvalidKar(String),

    #[error("VDB format error: {0}")]
    Format(String),

    #[error("{kind} mismatch: stored={stored}, computed={computed}")]
    BlobIntegrity {
        kind: &'static str,
        stored: String,
        computed: String,
    },

    #[error("column not found: {table}/{column}")]
    ColumnNotFound { table: String, column: String },

    #[error("unsupported encoding: {0}")]
    UnsupportedEncoding(String),

    #[error("unsupported format: {format} — {hint}")]
    UnsupportedFormat { format: String, hint: String },

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
