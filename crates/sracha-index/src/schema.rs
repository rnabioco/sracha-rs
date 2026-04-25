//! Vortex schema definitions for the catalog tables.
//!
//! Three logical tables, each backed by a `vortex::array::arrays::StructArray`:
//!
//! 1. **`accessions`** — one row per SRA accession (~30M at full scale).
//! 2. **`blobs`** — one row per (accession, column, blob_idx) (~3-5B).
//! 3. **`schemas`** — distinct column-layout templates, deduped by
//!    sha256 fingerprint (~10K).
//! 4. **`name_fmts`** — distinct Illumina skey templates (~thousands).
//!
//! Compression strategy notes per column live next to each column in
//! the builders below; Vortex's [`BtrBlocksCompressor`] picks per-column
//! encodings (FoR/Delta/Dict/RLE) when the writer flushes batches.
//!
//! Sort order (matters more than codec for compression):
//! - `accessions`: sort by accession_id (already monotonic-ish).
//! - `blobs`: sort by (accession_id, column_id, blob_idx) so
//!   `blob_offset` becomes intra-column delta.
//!
//! TODO: actual Vortex StructArray builders go here once we wire up
//! the writer module. For now this is documentation for the design.
