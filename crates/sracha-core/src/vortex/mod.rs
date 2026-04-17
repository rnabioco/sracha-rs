//! SRA → Vortex converter.
//!
//! Vortex-first: rows go straight from VDB decode into native Vortex per-column
//! builders (`vortex/builder.rs`), then into a `StructArray`. No Arrow
//! `RecordBatch` intermediate. This lets us pick `DType::Utf8` vs
//! `DType::Binary` explicitly per column so BtrBlocks' FSST/dict cascade fires
//! where appropriate.
//!
//! Reuses the per-blob decode from `crate::parquet::writer` (`decode_one_blob`,
//! `resolve_length_mode`, DNA packing) — everything before the batch assembler
//! is format-agnostic.

mod builder;
pub mod writer;

pub use writer::{VortexConvertConfig, VortexConvertStats, convert_sra_to_vortex};
