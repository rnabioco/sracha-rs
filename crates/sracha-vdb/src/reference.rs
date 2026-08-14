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

use std::sync::Arc;

use crate::cache::{CachedColumn, ColumnKind};
use crate::error::{Error, Result};
use crate::inspect;
use crate::kar::KarArchive;
use crate::kdb::ColumnReader;
use crate::refseq::{BASE_N, RefSeqReaders, RefSeqStore};

/// BAM-load's standard chunk size. REFERENCE rows each hold up to this many
/// bases (last chunk of a reference may be shorter, recorded in SEQ_LEN).
/// MAX_SEQ_LEN is declared as a static column in the align schema. We resolve
/// the real value from the archive at open time (see [`resolve_max_seq_len`])
/// and only fall back to this historical default when neither the column nor
/// the SEQ_LEN-derived estimate is available — a wrong value silently corrupts
/// cSRA reads via [`plan_span_start`].
pub(crate) const DEFAULT_MAX_SEQ_LEN: u32 = 5000;

/// Upper bound on REFERENCE rows sampled when deriving MAX_SEQ_LEN from
/// SEQ_LEN, keeping `open()` cheap on references with millions of chunks.
const MAX_SEQ_LEN_SAMPLE_ROWS: u64 = 256;

/// Handle to the REFERENCE table's chunk layout and, where present, its
/// embedded bases.
///
/// Bases come from one of two places, decided per chunk row exactly as
/// ncbi-vdb's `ref_restore_read` does: the embedded `CMP_READ` column, or
/// an external refseq object named by `SEQ_ID`. An archive may need both
/// (`CMP_READ` present but short for some rows), so this is not an
/// either/or split at the table level.
pub struct ReferenceCursor {
    /// 2na-packed base bytes per chunk (unpacked to 4na-bin on read).
    /// Absent when the run stores no reference bases at all.
    cmp_read: Option<CachedColumn>,
    /// Real chunk length in bases (≤ `max_seq_len`). Stored as u32 irzip.
    seq_len: CachedColumn,
    /// External reference accession per chunk. Absent on archives that
    /// embed everything (e.g. VDB-3418).
    seq_id: Option<CachedColumn>,
    /// 1-based start of the chunk within its reference; 0 means "this
    /// chunk is `SEQ_LEN` Ns" and addresses nothing externally.
    seq_start: Option<CachedColumn>,
    /// Locally-materialised refseq objects, when the caller supplied them.
    external: Option<RefSeqReaders>,
    max_seq_len: u32,
    first_row: i64,
    row_count: u64,
}

impl ReferenceCursor {
    pub fn open<R: Read + Seek>(archive: &mut KarArchive<R>, sra_path: &Path) -> Result<Self> {
        Self::open_with_external(archive, sra_path, None)
    }

    /// Open the REFERENCE table, optionally backed by external refseq
    /// objects for chunks whose bases are not embedded.
    pub fn open_with_external<R: Read + Seek>(
        archive: &mut KarArchive<R>,
        sra_path: &Path,
        refseqs: Option<&Arc<RefSeqStore>>,
    ) -> Result<Self> {
        let col_base = inspect::column_base_path_public(archive, Some("REFERENCE"))?;
        let open = |archive: &mut KarArchive<R>, name: &str| -> Result<ColumnReader> {
            ColumnReader::open(archive, &format!("{col_base}/{name}"), sra_path)
                .map_err(|e| Error::Format(format!("REFERENCE/{name}: {e}")))
        };
        let cmp_read = open(archive, "CMP_READ").ok();
        let seq_len = open(archive, "SEQ_LEN")?;
        let seq_id = open(archive, "SEQ_ID").ok();
        let seq_start = open(archive, "SEQ_START").ok();
        // Row extent tracks SEQ_LEN: it is the one column present on every
        // REFERENCE shape, embedded or external.
        let first_row = seq_len.first_row_id().unwrap_or(1);
        let row_count = seq_len.row_count();

        let cmp_read = cmp_read.map(|c| CachedColumn::new(c, ColumnKind::TwoNa));
        let seq_id = seq_id.map(|c| CachedColumn::new(c, ColumnKind::Zip));
        let seq_start =
            seq_start.map(|c| CachedColumn::new(c, ColumnKind::Irzip { elem_bits: 32 }));
        let external = refseqs.map(RefSeqReaders::new);
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
            seq_id,
            seq_start,
            external,
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
            let chunk_bases = self.chunk_bases(chunk_row, chunk_len)?;
            let available = chunk_len - offset_in_chunk;
            let take = remaining.min(available);
            out.extend_from_slice(&chunk_bases[offset_in_chunk..offset_in_chunk + take]);
            remaining -= take;
            offset_in_chunk = 0;
            if remaining > 0 {
                // Two sequences never share a chunk, and no alignment runs
                // past the end of its reference — so continuing into a
                // chunk with a different SEQ_ID means the span (or the
                // archive) is wrong. Walking on silently would splice
                // bases from the next chromosome into the read.
                self.check_same_reference(chunk_row, chunk_row + 1)?;
                chunk_row += 1;
            }
        }
        Ok(out)
    }

    /// Resolve one chunk row's bases, mirroring ncbi-vdb's
    /// `ref_restore_read`: prefer embedded `CMP_READ`, pad a short row with
    /// Ns, treat `SEQ_START == 0` as an all-N chunk, and otherwise fetch
    /// from the external refseq object named by `SEQ_ID`.
    fn chunk_bases(&self, chunk_row: i64, chunk_len: usize) -> Result<Vec<u8>> {
        if let Some(cmp) = &self.cmp_read {
            let bases = cmp.read_2na_row(chunk_row)?;
            if bases.len() >= chunk_len {
                let mut bases = bases;
                bases.truncate(chunk_len);
                return Ok(bases);
            }
            if !bases.is_empty() {
                let mut bases = bases;
                bases.resize(chunk_len, BASE_N);
                return Ok(bases);
            }
        }

        let Some(seq_start_col) = &self.seq_start else {
            return Err(Error::Format(format!(
                "REFERENCE row {chunk_row}: no embedded bases and no SEQ_START \
                 column to locate external ones"
            )));
        };
        let seq_start = seq_start_col.read_scalar_u32(chunk_row)? as u64;
        if seq_start == 0 {
            return Ok(vec![BASE_N; chunk_len]);
        }

        let external = self.external.as_ref().ok_or_else(|| {
            Error::CsraUnsupported(
                "cSRA: reference bases are stored externally but no refseq \
                 objects were provided"
                    .into(),
            )
        })?;
        let seq_id = self.seq_id_at(chunk_row)?;
        external.bases_at(&seq_id, seq_start - 1, chunk_len)
    }

    /// `SEQ_ID` for a chunk row, as a string.
    fn seq_id_at(&self, chunk_row: i64) -> Result<String> {
        let col = self.seq_id.as_ref().ok_or_else(|| {
            Error::Format(format!(
                "REFERENCE row {chunk_row}: external bases needed but no SEQ_ID column"
            ))
        })?;
        let bytes = col.read_byte_row(chunk_row)?;
        Ok(String::from_utf8_lossy(&bytes).trim().to_string())
    }

    /// Error unless two chunk rows belong to the same reference sequence.
    /// A no-op on archives without a `SEQ_ID` column (nothing to compare).
    fn check_same_reference(&self, a: i64, b: i64) -> Result<()> {
        if self.seq_id.is_none() {
            return Ok(());
        }
        let (ida, idb) = (self.seq_id_at(a)?, self.seq_id_at(b)?);
        if ida != idb {
            return Err(Error::Format(format!(
                "reference span crosses the boundary between {ida} (chunk {a}) \
                 and {idb} (chunk {b})"
            )));
        }
        Ok(())
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
