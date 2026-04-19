//! PRIMARY_ALIGNMENT table reader for reference-compressed cSRA.
//!
//! Exposes the per-row columns required by `align_restore_read` to
//! reconstruct aligned bases from REFERENCE + mismatch overlay.
//! See `docs/internal/csra-format-notes.md` for the algorithm.

use std::io::{Read, Seek};
use std::path::Path;

use crate::cache::{CachedColumn, ColumnKind};
use crate::error::{Error, Result};
use crate::inspect;
use crate::kar::KarArchive;
use crate::kdb::ColumnReader;

/// All per-row fields sracha needs to reconstruct an aligned read.
///
/// Byte slices are in the row's own encoding — conversion to the
/// `align_restore_read` inputs (4na-bin MISMATCH bytes, i32 REF_OFFSET
/// values) happens at the call site.
///
/// REF_ORIENTATION is intentionally absent: `seq_restore_read` takes
/// strand information from `SEQUENCE.READ_TYPE` (see
/// `ncbi-vdb/libs/axf/seq-restore-read.c:531`), which bam-load keeps
/// consistent with the alignment's orientation at load time.
#[derive(Debug, Clone)]
pub struct AlignmentRow {
    pub global_ref_start: u64,
    pub ref_len: u32,
    /// One byte per base of the final reconstructed read; `1` at positions
    /// that differ from the reference, `0` otherwise.
    pub has_mismatch: Vec<u8>,
    /// One byte per base of the final reconstructed read; `1` at positions
    /// where `ref_offset` advances or rewinds the reference cursor (indels).
    pub has_ref_offset: Vec<u8>,
    /// 4na-bin bases, length = number of `1`s in `has_mismatch`.
    pub mismatch: Vec<u8>,
    /// Signed offsets, length = number of `1`s in `has_ref_offset`.
    pub ref_offset: Vec<i32>,
}

/// Handle to the PRIMARY_ALIGNMENT table columns we consume during cSRA decode.
pub struct AlignmentCursor {
    global_ref_start: CachedColumn,
    ref_len: CachedColumn,
    has_mismatch: CachedColumn,
    has_ref_offset: CachedColumn,
    mismatch: CachedColumn,
    ref_offset: CachedColumn,
    row_count: u64,
    first_row: i64,
}

impl AlignmentCursor {
    /// Open the PRIMARY_ALIGNMENT columns in the archive.
    pub fn open<R: Read + Seek>(archive: &mut KarArchive<R>, sra_path: &Path) -> Result<Self> {
        let col_base = inspect::column_base_path_public(archive, Some("PRIMARY_ALIGNMENT"))?;
        let open = |archive: &mut KarArchive<R>, name: &str| -> Result<ColumnReader> {
            ColumnReader::open(archive, &format!("{col_base}/{name}"), sra_path)
                .map_err(|e| Error::Format(format!("PRIMARY_ALIGNMENT/{name}: {e}")))
        };
        let global_ref_start = open(archive, "GLOBAL_REF_START")?;
        let ref_len = open(archive, "REF_LEN")?;
        let has_mismatch = open(archive, "HAS_MISMATCH")?;
        let has_ref_offset = open(archive, "HAS_REF_OFFSET")?;
        let mismatch = open(archive, "MISMATCH")?;
        let ref_offset = open(archive, "REF_OFFSET")?;

        let first_row = global_ref_start.first_row_id().unwrap_or(1);
        let row_count = global_ref_start.row_count();

        Ok(Self {
            global_ref_start: CachedColumn::new(
                global_ref_start,
                ColumnKind::Irzip { elem_bits: 64 },
            ),
            ref_len: CachedColumn::new(ref_len, ColumnKind::Irzip { elem_bits: 32 }),
            has_mismatch: CachedColumn::new(has_mismatch, ColumnKind::Zip),
            has_ref_offset: CachedColumn::new(has_ref_offset, ColumnKind::Zip),
            mismatch: CachedColumn::new(mismatch, ColumnKind::Zip),
            ref_offset: CachedColumn::new(ref_offset, ColumnKind::Irzip { elem_bits: 32 }),
            row_count,
            first_row,
        })
    }

    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    pub fn first_row(&self) -> i64 {
        self.first_row
    }

    /// Return the alignment row 1's GLOBAL_REF_START — smoke-test hook for
    /// wiring verification during early Phase 1 development.
    pub fn first_global_ref_start(&self) -> Result<u64> {
        self.global_ref_start
            .read_scalar_i64(self.first_row)
            .map(|v| v as u64)
    }

    pub fn first_ref_len(&self) -> Result<u32> {
        self.ref_len.read_scalar_u32(self.first_row)
    }

    /// Read one alignment row end-to-end: all six columns' values for
    /// `row_id`. Blobs stay cached per-column across calls, so sequential
    /// rows within the same blob amortise the decode over thousands of
    /// reads rather than re-running deflate / irzip per row.
    pub fn read_row(&self, row_id: i64) -> Result<AlignmentRow> {
        let global_ref_start = self.global_ref_start.read_scalar_i64(row_id)? as u64;
        let ref_len = self.ref_len.read_scalar_u32(row_id)?;
        let has_mismatch = self.has_mismatch.read_bool_row(row_id)?;
        let has_ref_offset = self.has_ref_offset.read_bool_row(row_id)?;
        let mismatch = self.mismatch.read_byte_row(row_id)?;
        let ref_offset = self.ref_offset.read_i32_row(row_id)?;

        Ok(AlignmentRow {
            global_ref_start,
            ref_len,
            has_mismatch,
            has_ref_offset,
            mismatch,
            ref_offset,
        })
    }
}
