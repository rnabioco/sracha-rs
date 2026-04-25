use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("network error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("sra-core error: {0}")]
    Core(#[from] sracha_core::error::Error),

    #[error("vdb parse error: {0}")]
    Vdb(#[from] sracha_vdb::error::Error),

    #[error("extractor: {0}")]
    Extractor(String),

    #[error("schema: {0}")]
    Schema(String),

    #[error("writer: {0}")]
    Writer(String),

    #[error("reader: {0}")]
    Reader(String),
}

pub type Result<T> = std::result::Result<T, Error>;
