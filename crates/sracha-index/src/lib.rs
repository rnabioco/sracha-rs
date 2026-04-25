//! Hosted Vortex catalog of SRA accession metadata.
//!
//! Builds and queries a remote-friendly columnar index over SRA accession
//! metadata extracted directly from the KAR archive header + idx files,
//! without downloading the full .sra. Designed to be queried via HTTP
//! Range against a single hosted file (Vortex's chunk-statistic
//! pushdown skips irrelevant data).
//!
//! See module docs:
//! - [`record`] — the per-accession record produced by extraction.
//! - [`extractor`] — async fetch-only-the-metadata-bytes from S3 and
//!   parse with `sracha-vdb`.
//! - [`writer`] / [`reader`] — Vortex shard I/O.
//! - [`schema`] — column-by-column encoding strategy notes.

pub mod error;
pub mod extractor;
pub mod reader;
pub mod record;
pub mod schema;
pub mod writer;

pub use error::{Error, Result};
pub use record::AccessionRecord;
