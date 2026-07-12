#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid KAR archive: {0}")]
    InvalidKar(String),

    #[error("VDB format error: {0}")]
    Format(String),

    /// A reference-compressed cSRA whose shape sracha's `CsraCursor` cannot
    /// reconstruct (external refseq, static-metadata READ_LEN, …). Distinct
    /// from [`Format`] so callers can offer an ENA fallback instead of
    /// treating it as a decoder bug. The string is a human-readable diagnosis
    /// of *why* it can't be decoded (no trailing recommendation — the CLI owns
    /// that).
    #[error("{0}")]
    CsraUnsupported(String),

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
