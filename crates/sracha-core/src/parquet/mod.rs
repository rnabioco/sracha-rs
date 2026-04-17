//! SRA → Parquet converter.
//!
//! Reads the SEQUENCE table of an SRA file via [`crate::vdb::cursor::VdbCursor`]
//! and writes per-read rows into a Parquet file. Designed to test whether
//! Apache Arrow / Parquet can match VDB's storage density when given access to
//! the same domain tricks (2na DNA packing, fixed-width detection,
//! dictionary-encoded quality).

pub mod encoding;
pub mod schema;
pub mod writer;

pub use schema::{DnaPacking, LengthMode};
pub use writer::{ConvertConfig, ConvertStats, ParquetCompression, convert_sra_to_parquet};
