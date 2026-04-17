//! Format-agnostic conversion pipeline: VDB blob decode + DNA packing +
//! length-mode detection.
//!
//! Shared by the optional `parquet` and `vortex` output modules. Nothing
//! here depends on Apache Arrow, Parquet, or Vortex, so it compiles with
//! all conversion features disabled.

pub mod decode;
pub mod encoding;
pub mod schema;
