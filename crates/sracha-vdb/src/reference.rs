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
    /// The chunk layout, held in full (see [`ReferenceLayout`]). Shared:
    /// decode builds a cursor per chunk, and re-reading this each time
    /// costs far more than the lookups it saves.
    layout: Arc<ReferenceLayout>,
    /// Locally-materialised refseq objects, when the caller supplied them.
    external: Option<RefSeqReaders>,
    max_seq_len: u32,
    first_row: i64,
    row_count: u64,
}

/// The REFERENCE table's chunk layout, read once at open and kept resident.
///
/// Alignments arrive in spot order but land at random genome positions, so
/// these columns are read in random row order — the worst case for a
/// single-blob cache, whose blobs here hold ~131k rows each. Expanding a
/// blob's page map to read one row was 44% of decode time. The layout is
/// small enough to just hold: rows are bounded by genome size divided by
/// `MAX_SEQ_LEN`, so a whole human genome is ~620k rows, about 7 MB across
/// the three arrays. The bases themselves stay lazy.
pub struct ReferenceLayout {
    /// Real chunk length in bases (≤ `max_seq_len`), one per row.
    seq_len: Vec<u32>,
    /// 1-based start of the chunk within its reference; 0 means "this
    /// chunk is `SEQ_LEN` Ns" and addresses nothing externally. Empty when
    /// the archive has no SEQ_START column.
    seq_start: Vec<u32>,
    /// Index into `seq_id_names` per row. Empty when the archive has no
    /// SEQ_ID column (it embeds everything, e.g. VDB-3418).
    seq_id: Vec<u32>,
    /// Distinct external reference accessions, in first-seen order.
    seq_id_names: Vec<String>,
}

impl ReferenceCursor {
    pub fn open<R: Read + Seek>(archive: &mut KarArchive<R>, sra_path: &Path) -> Result<Self> {
        Self::open_with_external(archive, sra_path, None, None)
    }

    /// Open the REFERENCE table, optionally backed by external refseq
    /// objects for chunks whose bases are not embedded.
    ///
    /// Pass `layout` to reuse a [`ReferenceLayout`] built once for the
    /// accession; without it each cursor reads the layout itself, which is
    /// only acceptable for one-shot callers like `sracha info`.
    pub fn open_with_external<R: Read + Seek>(
        archive: &mut KarArchive<R>,
        sra_path: &Path,
        refseqs: Option<&Arc<RefSeqStore>>,
        layout: Option<&Arc<ReferenceLayout>>,
    ) -> Result<Self> {
        let col_base = inspect::column_base_path_public(archive, Some("REFERENCE"))?;
        let open = |archive: &mut KarArchive<R>, name: &str| -> Result<ColumnReader> {
            ColumnReader::open(archive, &format!("{col_base}/{name}"), sra_path)
                .map_err(|e| Error::Format(format!("REFERENCE/{name}: {e}")))
        };
        let cmp_read = open(archive, "CMP_READ").ok();
        let seq_len = open(archive, "SEQ_LEN")?;
        // Row extent tracks SEQ_LEN: it is the one column present on every
        // REFERENCE shape, embedded or external.
        let first_row = seq_len.first_row_id().unwrap_or(1);
        let row_count = seq_len.row_count();

        let cmp_read = cmp_read.map(|c| CachedColumn::new(c, ColumnKind::TwoNa));
        let external = refseqs.map(RefSeqReaders::new);
        let layout = match layout {
            Some(l) => l.clone(),
            None => Arc::new(ReferenceLayout::open(archive, sra_path)?.ok_or_else(|| {
                Error::Format("REFERENCE: no SEQ_LEN column to read the layout from".into())
            })?),
        };

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
            derive_max_seq_len_from_seq_len(&layout.seq_len)
        } else {
            None
        };
        let (max_seq_len, source) = resolve_max_seq_len(col_value, derived);
        tracing::debug!("REFERENCE MAX_SEQ_LEN = {max_seq_len} (source: {source})");

        Ok(Self {
            cmp_read,
            layout,
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
            let chunk_len = self.layout.seq_len(chunk_row, self.first_row)? as usize;
            if offset_in_chunk > chunk_len {
                return Err(Error::Format(format!(
                    "reference: offset {offset_in_chunk} past chunk {chunk_row} len {chunk_len}"
                )));
            }
            let available = chunk_len - offset_in_chunk;
            let take = remaining.min(available);
            self.chunk_bases_into(&mut out, chunk_row, chunk_len, offset_in_chunk, take)?;
            remaining -= take;
            offset_in_chunk = 0;
            if remaining > 0 {
                // Stop at the end of this reference and return a short
                // span, exactly as ncbi-vdb's `ref_sub_select` does — its
                // read loop is bounded by the reference's `stop_id`
                // (libs/axf/ref-tbl-sub-select.c:204). A span can overrun
                // the end when soft clipping widens it; those positions are
                // mismatches and never read the reference, so the caller
                // never notices. Splicing in the next chromosome's bases
                // would silently corrupt the read.
                if !self.same_reference(chunk_row, chunk_row + 1)? {
                    break;
                }
                chunk_row += 1;
            }
        }
        Ok(out)
    }

    /// Resolve one chunk row's bases, mirroring ncbi-vdb's
    /// `ref_restore_read`: prefer embedded `CMP_READ`, pad a short row with
    /// Ns, treat `SEQ_START == 0` as an all-N chunk, and otherwise fetch
    /// from the external refseq object named by `SEQ_ID`.
    /// Append `take` bases of chunk `chunk_row`, starting `offset` into it,
    /// mirroring ncbi-vdb's `ref_restore_read`: prefer embedded `CMP_READ`,
    /// pad a short row with Ns, treat `SEQ_START == 0` as an all-N chunk,
    /// and otherwise fetch from the external refseq object named by
    /// `SEQ_ID`.
    fn chunk_bases_into(
        &self,
        out: &mut Vec<u8>,
        chunk_row: i64,
        chunk_len: usize,
        offset: usize,
        take: usize,
    ) -> Result<()> {
        if let Some(cmp) = &self.cmp_read {
            let stored = cmp.two_na_row_len(chunk_row)?;
            if stored >= chunk_len {
                out.extend_from_slice(&cmp.read_2na_range(chunk_row, offset, take)?);
                return Ok(());
            }
            if stored > 0 {
                // Short row: real bases then N padding out to SEQ_LEN.
                let real = stored.saturating_sub(offset).min(take);
                if real > 0 {
                    out.extend_from_slice(&cmp.read_2na_range(chunk_row, offset, real)?);
                }
                out.resize(out.len() + (take - real), BASE_N);
                return Ok(());
            }
        }

        if self.layout.seq_start.is_empty() {
            return Err(Error::Format(format!(
                "REFERENCE row {chunk_row}: no embedded bases and no SEQ_START \
                 column to locate external ones"
            )));
        }
        let seq_start = u64::from(self.layout.seq_start(chunk_row, self.first_row)?);
        if seq_start == 0 {
            out.resize(out.len() + take, BASE_N);
            return Ok(());
        }

        let external = self.external.as_ref().ok_or_else(|| {
            Error::CsraUnsupported(
                "cSRA: reference bases are stored externally but no refseq \
                 objects were provided"
                    .into(),
            )
        })?;
        let seq_id = self.layout.seq_id(chunk_row, self.first_row)?;
        out.extend_from_slice(&external.bases_at(seq_id, seq_start - 1 + offset as u64, take)?);
        Ok(())
    }

    /// Do two chunk rows belong to the same reference sequence?
    ///
    /// Always true on archives with no `SEQ_ID` column (one reference, so
    /// nothing to cross); false once `b` runs off the end of the table.
    fn same_reference(&self, a: i64, b: i64) -> Result<bool> {
        if self.layout.seq_id.is_empty() {
            return Ok(b < self.first_row + self.row_count as i64);
        }
        if b >= self.first_row + self.row_count as i64 {
            return Ok(false);
        }
        Ok(self.layout.seq_id(a, self.first_row)? == self.layout.seq_id(b, self.first_row)?)
    }
}

/// Estimate MAX_SEQ_LEN from SEQ_LEN when the static column is unavailable.
///
/// Every non-terminal chunk of a reference is exactly MAX_SEQ_LEN bases, so the
/// widest SEQ_LEN over a bounded prefix of the table recovers the value as long
/// as any reference in that prefix spans more than one chunk. Returns `None`
/// when nothing decodes (caller falls back to [`DEFAULT_MAX_SEQ_LEN`]).
impl ReferenceLayout {
    /// Open the REFERENCE layout columns from an archive and read them in.
    ///
    /// Build this **once per accession** and share the `Arc`: decode
    /// workers construct a `ReferenceCursor` per chunk, and re-reading the
    /// layout each time costs more than the random lookups it replaces.
    pub fn open<R: Read + Seek>(
        archive: &mut KarArchive<R>,
        sra_path: &Path,
    ) -> Result<Option<Self>> {
        let Ok(col_base) = inspect::column_base_path_public(archive, Some("REFERENCE")) else {
            return Ok(None);
        };
        let open = |archive: &mut KarArchive<R>, name: &str| -> Option<ColumnReader> {
            ColumnReader::open(archive, &format!("{col_base}/{name}"), sra_path).ok()
        };
        let Some(seq_len) = open(archive, "SEQ_LEN") else {
            return Ok(None);
        };
        let seq_start = open(archive, "SEQ_START");
        let seq_id = open(archive, "SEQ_ID");

        let first_row = seq_len.first_row_id().unwrap_or(1);
        let row_count = seq_len.row_count();
        let seq_len = CachedColumn::new(seq_len, ColumnKind::Irzip { elem_bits: 32 });
        let seq_start =
            seq_start.map(|c| CachedColumn::new(c, ColumnKind::Irzip { elem_bits: 32 }));
        let seq_id = seq_id.map(|c| CachedColumn::new(c, ColumnKind::Zip));

        Ok(Some(Self::load(
            &seq_len,
            seq_start.as_ref(),
            seq_id.as_ref(),
            first_row,
            row_count,
        )?))
    }

    /// Read the layout columns front to back, so each blob is decoded once.
    fn load(
        seq_len_col: &CachedColumn,
        seq_start_col: Option<&CachedColumn>,
        seq_id_col: Option<&CachedColumn>,
        first_row: i64,
        row_count: u64,
    ) -> Result<Self> {
        let n = row_count as usize;
        let mut seq_len = Vec::with_capacity(n);
        let mut seq_start = Vec::with_capacity(if seq_start_col.is_some() { n } else { 0 });
        let mut seq_id = Vec::with_capacity(if seq_id_col.is_some() { n } else { 0 });
        let mut seq_id_names: Vec<String> = Vec::new();
        // REFERENCE rows are grouped by SEQ_ID, so remembering the last one
        // turns the intern lookup into a pointer compare for all but the
        // handful of rows that start a new reference.
        let mut last: Option<(String, u32)> = None;

        for row in first_row..first_row + row_count as i64 {
            seq_len.push(seq_len_col.read_scalar_u32(row)?);
            if let Some(col) = seq_start_col {
                seq_start.push(col.read_scalar_u32(row)?);
            }
            if let Some(col) = seq_id_col {
                let bytes = col.read_byte_row(row)?;
                let name = std::str::from_utf8(&bytes)
                    .map_err(|e| Error::Format(format!("REFERENCE/SEQ_ID row {row}: {e}")))?
                    .trim();
                let idx = match &last {
                    Some((prev, idx)) if prev == name => *idx,
                    _ => {
                        let idx = seq_id_names
                            .iter()
                            .position(|n| n == name)
                            .unwrap_or_else(|| {
                                seq_id_names.push(name.to_string());
                                seq_id_names.len() - 1
                            }) as u32;
                        last = Some((name.to_string(), idx));
                        idx
                    }
                };
                seq_id.push(idx);
            }
        }

        Ok(Self {
            seq_len,
            seq_start,
            seq_id,
            seq_id_names,
        })
    }

    fn index(row: i64, first_row: i64, len: usize, what: &str) -> Result<usize> {
        let idx = row - first_row;
        if idx < 0 || idx as usize >= len {
            return Err(Error::Format(format!(
                "REFERENCE {what}: row {row} outside [{first_row}, {})",
                first_row + len as i64
            )));
        }
        Ok(idx as usize)
    }

    fn seq_len(&self, row: i64, first_row: i64) -> Result<u32> {
        Ok(self.seq_len[Self::index(row, first_row, self.seq_len.len(), "SEQ_LEN")?])
    }

    fn seq_start(&self, row: i64, first_row: i64) -> Result<u32> {
        Ok(self.seq_start[Self::index(row, first_row, self.seq_start.len(), "SEQ_START")?])
    }

    fn seq_id(&self, row: i64, first_row: i64) -> Result<&str> {
        let i = Self::index(row, first_row, self.seq_id.len(), "SEQ_ID")?;
        Ok(&self.seq_id_names[self.seq_id[i] as usize])
    }
}

fn derive_max_seq_len_from_seq_len(seq_len: &[u32]) -> Option<u32> {
    let sampled = seq_len.len().min(MAX_SEQ_LEN_SAMPLE_ROWS as usize);
    let max = seq_len[..sampled].iter().copied().max().unwrap_or(0);
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
