//! Pure-Rust parser for the NCBI VDB / KAR binary format.
//!
//! No C FFI, no `ncbi-vdb` dependency — reads SRA archive files by
//! parsing the KAR container, resolving columns via KDB index files,
//! and decoding column blobs natively.

pub mod alignment;
pub mod blob;
pub mod blob_codecs;
pub(crate) mod cache;
pub mod csra;
pub mod cursor;
pub mod dump;
pub mod encoding;
pub mod error;
pub mod inspect;
pub mod kar;
pub mod kdb;
pub mod metadata;
pub(crate) mod ptrie;
pub mod reference;
pub mod restore;
pub mod row_range;

pub use cursor::VdbCursor;
pub use error::{Error, Result};
pub use inspect::VdbKind;
pub use kar::KarArchive;
