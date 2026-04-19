//! Pure-Rust parser for the NCBI VDB / KAR binary format.
//!
//! No C FFI, no `ncbi-vdb` dependency — reads SRA archive files by
//! parsing the KAR container, resolving columns via KDB index files,
//! and decoding column blobs natively.

pub mod blob;
pub mod cursor;
pub mod encoding;
pub mod error;
pub mod inspect;
pub mod kar;
pub mod kdb;
pub mod metadata;

pub use cursor::VdbCursor;
pub use error::{Error, Result};
pub use inspect::VdbKind;
pub use kar::KarArchive;
