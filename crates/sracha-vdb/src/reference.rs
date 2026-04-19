//! REFERENCE table reader for reference-compressed cSRA.
//!
//! Provides `fetch_span(global_ref_start, ref_len)` — returns `ref_len`
//! bases of the reference in 4na-bin form (one nibble per byte, low
//! nibble populated), spanning chunk boundaries as needed. This is the
//! input `ref_read` that `align_restore_read` overlays with
//! `HAS_MISMATCH` / `MISMATCH` to reconstruct aligned reads.
//!
//! See `docs/internal/csra-format-notes.md` for how `GLOBAL_REF_START`
//! maps to (REFERENCE row, offset) via `MAX_SEQ_LEN`.

use std::io::{Read, Seek};
use std::path::Path;

use crate::cache::{CachedColumn, ColumnKind};
use crate::error::{Error, Result};
use crate::inspect;
use crate::kar::KarArchive;
use crate::kdb::ColumnReader;

/// BAM-load's standard chunk size. REFERENCE rows each hold up to this many
/// bases (last chunk of a reference may be shorter, recorded in SEQ_LEN).
/// MAX_SEQ_LEN is declared as a static column in the align schema; we
/// hardcode 5000 for v1 and will read it from metadata once we have a
/// fixture that uses a different value.
const DEFAULT_MAX_SEQ_LEN: u32 = 5000;

/// Handle to the REFERENCE table's CMP_READ + SEQ_LEN columns.
pub struct ReferenceCursor {
    /// 2na-packed base bytes per chunk (unpacked to 4na-bin on read).
    cmp_read: CachedColumn,
    /// Real chunk length in bases (≤ `max_seq_len`). Stored as u32 irzip.
    seq_len: CachedColumn,
    max_seq_len: u32,
    first_row: i64,
    row_count: u64,
}

impl ReferenceCursor {
    pub fn open<R: Read + Seek>(archive: &mut KarArchive<R>, sra_path: &Path) -> Result<Self> {
        let col_base = inspect::column_base_path_public(archive, Some("REFERENCE"))?;
        let open = |archive: &mut KarArchive<R>, name: &str| -> Result<ColumnReader> {
            ColumnReader::open(archive, &format!("{col_base}/{name}"), sra_path)
                .map_err(|e| Error::Format(format!("REFERENCE/{name}: {e}")))
        };
        let cmp_read = open(archive, "CMP_READ")?;
        let seq_len = open(archive, "SEQ_LEN")?;
        let first_row = cmp_read.first_row_id().unwrap_or(1);
        let row_count = cmp_read.row_count();

        Ok(Self {
            cmp_read: CachedColumn::new(cmp_read, ColumnKind::TwoNa),
            seq_len: CachedColumn::new(seq_len, ColumnKind::Irzip { elem_bits: 32 }),
            max_seq_len: DEFAULT_MAX_SEQ_LEN,
            first_row,
            row_count,
        })
    }

    pub fn max_seq_len(&self) -> u32 {
        self.max_seq_len
    }

    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    pub fn first_row(&self) -> i64 {
        self.first_row
    }

    /// Return `ref_len` reference bases starting at absolute (concatenated)
    /// position `global_ref_start`, as 4na-bin bytes (one nibble per byte,
    /// low nibble populated). Spans chunk boundaries as needed. Adjacent
    /// spans touching the same REFERENCE blob skip re-decode entirely via
    /// `CachedColumn`.
    pub fn fetch_span(&self, global_ref_start: u64, ref_len: u32) -> Result<Vec<u8>> {
        let (first_chunk_row, mut offset_in_chunk) =
            plan_span_start(global_ref_start, self.max_seq_len);
        let mut remaining = ref_len as usize;
        let mut chunk_row = first_chunk_row;

        let mut out = Vec::with_capacity(ref_len as usize);
        while remaining > 0 {
            let chunk_len = self.seq_len.read_scalar_u32(chunk_row)? as usize;
            if offset_in_chunk > chunk_len {
                return Err(Error::Format(format!(
                    "reference: offset {offset_in_chunk} past chunk {chunk_row} len {chunk_len}"
                )));
            }
            let chunk_bases = self.cmp_read.read_2na_row(chunk_row)?;
            if chunk_bases.len() != chunk_len {
                return Err(Error::Format(format!(
                    "REFERENCE.CMP_READ row {chunk_row}: page_map says {} bases, \
                     SEQ_LEN says {chunk_len}",
                    chunk_bases.len()
                )));
            }
            let available = chunk_len - offset_in_chunk;
            let take = remaining.min(available);
            out.extend_from_slice(&chunk_bases[offset_in_chunk..offset_in_chunk + take]);
            remaining -= take;
            chunk_row += 1;
            offset_in_chunk = 0;
        }
        Ok(out)
    }
}

/// Translate a global reference position to `(chunk_row, offset_in_chunk)`.
///
/// REFERENCE rows are 1-based and each holds up to `max_seq_len` bases, laid
/// out end-to-end. Extracted from [`ReferenceCursor::fetch_span`] so the
/// arithmetic can be covered by unit tests without touching a KAR archive.
fn plan_span_start(global_ref_start: u64, max_seq_len: u32) -> (i64, usize) {
    let msl = u64::from(max_seq_len);
    let chunk_row = (global_ref_start / msl) as i64 + 1;
    let offset = (global_ref_start % msl) as usize;
    (chunk_row, offset)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_span_start_at_chunk_origin() {
        // Row 1 starts at global 0; row 2 at global 5000 when max_seq_len=5000.
        assert_eq!(plan_span_start(0, 5000), (1, 0));
        assert_eq!(plan_span_start(5000, 5000), (2, 0));
    }

    #[test]
    fn plan_span_start_mid_chunk() {
        assert_eq!(plan_span_start(123, 5000), (1, 123));
        // 5000 + 1620 lands mid row 2.
        assert_eq!(plan_span_start(6620, 5000), (2, 1620));
    }

    #[test]
    fn plan_span_start_chunk_boundary_minus_one() {
        assert_eq!(plan_span_start(4999, 5000), (1, 4999));
    }

    #[test]
    fn plan_span_start_large_offset_gives_large_row() {
        // 1000 chunks into the reference.
        let (row, off) = plan_span_start(1000 * 5000 + 42, 5000);
        assert_eq!(row, 1001);
        assert_eq!(off, 42);
    }

    #[test]
    fn plan_span_start_small_max_seq_len() {
        // Uncommon but valid: if a fixture uses MAX_SEQ_LEN=100, the math
        // must keep working.
        assert_eq!(plan_span_start(0, 100), (1, 0));
        assert_eq!(plan_span_start(99, 100), (1, 99));
        assert_eq!(plan_span_start(100, 100), (2, 0));
        assert_eq!(plan_span_start(199, 100), (2, 99));
        assert_eq!(plan_span_start(200, 100), (3, 0));
    }
}
