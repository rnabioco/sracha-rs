//! SRA → Vortex converter.
//!
//! Sibling of [`crate::parquet`]: same Arrow `RecordBatch` stream, different
//! on-disk format. Vortex (SpiralDB, pre-1.0) picks its own encoding cascade,
//! so there are no compression knobs here — that's the whole point of the
//! comparison benchmark (Issue #9).
//!
//! Shares the schema (`crate::parquet::schema`), DNA-packing helpers
//! (`crate::parquet::encoding`), and the `BatchBuilder` rows→`RecordBatch`
//! assembler from `crate::parquet::writer` — only the sink differs.

pub mod writer;

pub use writer::{VortexConvertConfig, VortexConvertStats, convert_sra_to_vortex};
