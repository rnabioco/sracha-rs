//! SRA → Parquet converter.
//!
//! Reads the SEQUENCE table of an SRA file via [`crate::vdb::cursor::VdbCursor`]
//! and writes per-read rows into a Parquet file. Designed to test whether
//! Apache Arrow / Parquet can match VDB's storage density when given access to
//! the same domain tricks (2na DNA packing, fixed-width detection,
//! dictionary-encoded quality).
//!
//! Format-agnostic pieces (DNA packing, per-blob decode, length-mode
//! detection) live in [`crate::convert`]; this module only owns the
//! Parquet-specific Arrow schema and writer.

pub mod schema;
pub mod writer;

pub use crate::convert::schema::{DnaPacking, LengthMode};
pub use writer::{ConvertConfig, ConvertStats, ParquetCompression, convert_sra_to_parquet};
