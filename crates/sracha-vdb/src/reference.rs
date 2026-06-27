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
/// MAX_SEQ_LEN is declared as a static column in the align schema. We resolve
/// the real value from the archive at open time (see [`resolve_max_seq_len`])
/// and only fall back to this historical default when neither the column nor
/// the SEQ_LEN-derived estimate is available — a wrong value silently corrupts
/// cSRA reads via [`plan_span_start`].
const DEFAULT_MAX_SEQ_LEN: u32 = 5000;

/// Upper bound on REFERENCE rows sampled when deriving MAX_SEQ_LEN from
/// SEQ_LEN, keeping `open()` cheap on references with millions of chunks.
const MAX_SEQ_LEN_SAMPLE_ROWS: u64 = 256;

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

        let cmp_read = CachedColumn::new(cmp_read, ColumnKind::TwoNa);
        let seq_len = CachedColumn::new(seq_len, ColumnKind::Irzip { elem_bits: 32 });

        // Resolve the real chunk size instead of assuming 5000. Prefer the
        // static MAX_SEQ_LEN column; if it is absent/unreadable, derive it from
        // the widest SEQ_LEN we observe (every non-terminal chunk equals
        // MAX_SEQ_LEN). Both can fail on degenerate archives, hence the default.
        let col_value =
            match ColumnReader::open(archive, &format!("{col_base}/MAX_SEQ_LEN"), sra_path) {
                Ok(col) => {
                    let fr = col.first_row_id().unwrap_or(1);
                    CachedColumn::new(col, ColumnKind::Irzip { elem_bits: 32 })
                        .read_scalar_u32(fr)
                        .ok()
                        .filter(|&v| v > 0)
                }
                Err(_) => None,
            };
        let derived = if col_value.is_none() {
            derive_max_seq_len_from_seq_len(&seq_len, first_row, row_count)
        } else {
            None
        };
        let (max_seq_len, source) = resolve_max_seq_len(col_value, derived);
        tracing::debug!("REFERENCE MAX_SEQ_LEN = {max_seq_len} (source: {source})");

        Ok(Self {
            cmp_read,
            seq_len,
            max_seq_len,
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

/// Estimate MAX_SEQ_LEN from SEQ_LEN when the static column is unavailable.
///
/// Every non-terminal chunk of a reference is exactly MAX_SEQ_LEN bases, so the
/// widest SEQ_LEN over a bounded prefix of the table recovers the value as long
/// as any reference in that prefix spans more than one chunk. Returns `None`
/// when nothing decodes (caller falls back to [`DEFAULT_MAX_SEQ_LEN`]).
fn derive_max_seq_len_from_seq_len(
    seq_len: &CachedColumn,
    first_row: i64,
    row_count: u64,
) -> Option<u32> {
    if row_count == 0 {
        return None;
    }
    let sampled = row_count.min(MAX_SEQ_LEN_SAMPLE_ROWS);
    let end = first_row + sampled as i64;
    let mut max = 0u32;
    for row in first_row..end {
        if let Ok(v) = seq_len.read_scalar_u32(row) {
            max = max.max(v);
        }
    }
    (max > 0).then_some(max)
}

/// Pick the MAX_SEQ_LEN value and a label for diagnostics, in priority order:
/// the static column, then the SEQ_LEN-derived estimate, then the default.
fn resolve_max_seq_len(col_value: Option<u32>, derived: Option<u32>) -> (u32, &'static str) {
    if let Some(v) = col_value {
        (v, "MAX_SEQ_LEN column")
    } else if let Some(v) = derived {
        (v, "derived from SEQ_LEN")
    } else {
        (DEFAULT_MAX_SEQ_LEN, "default")
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

    #[test]
    fn resolve_max_seq_len_prefers_column() {
        assert_eq!(
            resolve_max_seq_len(Some(10_000), Some(5000)),
            (10_000, "MAX_SEQ_LEN column")
        );
    }

    #[test]
    fn resolve_max_seq_len_falls_back_to_derived() {
        assert_eq!(
            resolve_max_seq_len(None, Some(4096)),
            (4096, "derived from SEQ_LEN")
        );
    }

    #[test]
    fn resolve_max_seq_len_falls_back_to_default() {
        assert_eq!(
            resolve_max_seq_len(None, None),
            (DEFAULT_MAX_SEQ_LEN, "default")
        );
    }
}
