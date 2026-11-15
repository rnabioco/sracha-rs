//! Per-column decoded-blob cache.
//!
//! `ColumnReader::read_raw_blob_slice` is zero-copy, but
//! `blob::decode_blob` + the per-encoding decompression step
//! (deflate / irzip / izip) is not — and the cSRA decode path previously
//! re-ran it on every row even though a single blob covers hundreds to
//! thousands of consecutive rows. `CachedColumn` holds the last decoded
//! blob plus precomputed prefix sums and logical→rec-idx mapping, so
//! hot-path row lookups are O(1) once the blob is warm.

use std::cell::RefCell;
use std::sync::Arc;
#[cfg(feature = "colstats")]
use std::sync::atomic::{AtomicU64, Ordering};

use crate::blob::{self, DecodedBlob, PageMap, RowExtent};
use crate::error::{Error, Result};
use crate::kdb::ColumnReader;

/// Above this many rows in a single blob, don't expand every row's extent
/// on decode — look rows up one at a time instead.
///
/// A constant-valued column is stored as one run covering the whole table:
/// `SEQUENCE.READ_LEN` on a fixed-length run is a single blob spanning every
/// spot. Expanding that to read one row allocated ~740 MB per decode worker
/// and accounted for 99.6% of all extent expansion on SRR622461. Blobs below
/// the threshold keep the eager path, which is cheaper once several rows of
/// a blob get read — measured, not assumed.
const EAGER_EXTENT_MAX_ROWS: u64 = 1 << 20;

/// Diagnostic label so per-column blob-cache behaviour can be attributed.
/// Profilers lose the caller of `row_extents_range` to inlining, so the
/// counters below are the only way to tell which column is thrashing.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ColLabel {
    Other,
    SeqCmpRead,
    SeqCmpAltread,
    SeqQuality,
    SeqAlignId,
    SeqReadLen,
    SeqReadType,
    PaGlobalRefStart,
    PaHasMismatch,
    PaHasRefOffset,
    PaMismatch,
    PaRefOffset,
    RefCmpRead,
    RefSeqRead,
    RefSeqAltread,
}

impl ColLabel {
    #[allow(dead_code)]
    pub(crate) const COUNT: usize = 15;

    #[cfg(feature = "colstats")]
    fn idx(self) -> usize {
        self as usize
    }

    #[cfg(feature = "colstats")]
    fn name(i: usize) -> &'static str {
        [
            "other",
            "SEQ.CMP_READ",
            "SEQ.CMP_ALTREAD",
            "SEQ.QUALITY",
            "SEQ.PRIMARY_ALIGNMENT_ID",
            "SEQ.READ_LEN",
            "SEQ.READ_TYPE",
            "PA.GLOBAL_REF_START",
            "PA.HAS_MISMATCH",
            "PA.HAS_REF_OFFSET",
            "PA.MISMATCH",
            "PA.REF_OFFSET",
            "REFERENCE.CMP_READ",
            "refseq.READ",
            "refseq.ALTREAD",
        ][i]
    }
}

// One counter per label. `AtomicU64` has interior mutability, so this is
// spelled out rather than built from a `const` initialiser.
#[cfg(feature = "colstats")]
#[allow(clippy::declare_interior_mutable_const)]
const ZERO: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "colstats")]
static HITS: [AtomicU64; ColLabel::COUNT] = [ZERO; ColLabel::COUNT];
#[cfg(feature = "colstats")]
static MISSES: [AtomicU64; ColLabel::COUNT] = [ZERO; ColLabel::COUNT];
#[cfg(feature = "colstats")]
static EXTENTS: [AtomicU64; ColLabel::COUNT] = [ZERO; ColLabel::COUNT];

#[cfg(feature = "colstats")]
#[inline]
fn bump(counters: &[AtomicU64; ColLabel::COUNT], label: ColLabel, n: u64) {
    counters[label.idx()].fetch_add(n, Ordering::Relaxed);
}

/// Print per-column blob-cache stats to stderr. Diagnostic only.
#[cfg(not(feature = "colstats"))]
pub fn dump_column_stats() {
    eprintln!("column stats unavailable: rebuild with --features sracha-vdb/colstats");
}

/// Print per-column blob-cache stats to stderr. Diagnostic only.
#[cfg(feature = "colstats")]
pub fn dump_column_stats() {
    eprintln!(
        "{:<26} {:>14} {:>14} {:>7} {:>16}",
        "column", "hits", "misses", "miss%", "extents expanded"
    );
    let mut total_extents = 0u64;
    for i in 0..ColLabel::COUNT {
        let h = HITS[i].load(Ordering::Relaxed);
        let m = MISSES[i].load(Ordering::Relaxed);
        let e = EXTENTS[i].load(Ordering::Relaxed);
        total_extents += e;
        if h + m == 0 {
            continue;
        }
        eprintln!(
            "{:<26} {:>14} {:>14} {:>6.1}% {:>16}",
            ColLabel::name(i),
            h,
            m,
            100.0 * m as f64 / (h + m) as f64,
            e
        );
    }
    eprintln!("{:<26} {:>54}", "TOTAL extents expanded", total_extents);
}

/// How a column's blob payload is decoded once `blob::decode_blob` has
/// stripped the envelope / headers / page map. Caller selects the kind
/// at cursor-open time; it never changes for the life of the column.
#[derive(Clone, Copy, Debug)]
pub(crate) enum ColumnKind {
    /// zip-encoded payload (raw bytes after optional deflate). Used for
    /// byte records (MISMATCH, READ_TYPE, QUALITY) and bit-packed bool
    /// records (HAS_MISMATCH, HAS_REF_OFFSET) — the caller picks units.
    Zip,
    /// irzip / zip / legacy-izip integer payload at `elem_bits` width.
    Irzip { elem_bits: u32 },
    /// 2na-packed bases (2 bits per base, MSB-first). The blob's `data`
    /// field already carries the 2na stream; no further decompression.
    TwoNa,
}

/// One blob decoded and ready for O(1) row extraction.
pub(crate) struct DecodedColumnBlob {
    /// Row id of the blob's first row (matches `BlobLoc::start_id`).
    pub start_id: i64,
    /// Post-decompression payload. Unit depends on `ColumnKind`:
    ///   - `Zip` byte rows: raw bytes.
    ///   - `Zip` bit rows: raw bit-packed bytes (MSB-first).
    ///   - `Irzip`: decompressed little-endian integers.
    ///   - `TwoNa`: the 2na byte stream straight from the blob envelope.
    pub bytes: Vec<u8>,
    /// Present when the blob has a page map.
    pub page_map: Option<PageMap>,
    /// `page_map.row_extents()` cached once (empty if no page map): one
    /// `(offset, len)` per logical row, resolved from whichever mapping the
    /// page map carries. Units match what the column's encoding stores: bits
    /// for bit-packed rows, bytes for byte rows, elements for integer rows,
    /// bases for 2na rows.
    pub extents: Vec<RowExtent>,
    /// Fixed elements-per-row declared by a v1 blob header. v1 blobs carry
    /// no page map because every row is the same width — refseq `READ`
    /// columns are stored this way (one `MAX_SEQ_LEN` chunk per row).
    pub row_length: Option<u64>,
}

impl DecodedColumnBlob {
    /// Where a blob-relative row's data sits in the decoded payload.
    ///
    /// With no page map every row is one element wide at its own index.
    pub fn extent(&self, logical_offset: usize) -> Result<RowExtent> {
        if self.extents.is_empty() {
            // Either the blob was too wide to expand (see
            // `EAGER_EXTENT_MAX_ROWS`) or it has no page map at all, in
            // which case every row is one element wide at its own index.
            return match &self.page_map {
                Some(pm) => pm.extent_at(logical_offset),
                None => Ok(RowExtent {
                    offset: logical_offset as u32,
                    len: 1,
                }),
            };
        }
        self.extents.get(logical_offset).copied().ok_or_else(|| {
            Error::Format(format!(
                "logical offset {logical_offset} out of bounds ({} rows)",
                self.extents.len()
            ))
        })
    }
}

/// Each byte's bits, MSB first, as eight 0/1 bytes.
const UNPACK_BITS: [[u8; 8]; 256] = {
    let mut t = [[0u8; 8]; 256];
    let mut b = 0;
    while b < 256 {
        let mut i = 0;
        while i < 8 {
            t[b][i] = ((b >> (7 - i)) & 1) as u8;
            i += 1;
        }
        b += 1;
    }
    t
};

/// Unpack `len_bits` bits starting at bit `start_bits` of `bytes` (MSB-first
/// within each byte) into one 0/1 byte per bit. `None` when the range runs
/// past `bytes`. A byte at a time through [`UNPACK_BITS`] rather than a
/// bounds-checked shift per bit.
fn unpack_bit_row(bytes: &[u8], start_bits: usize, len_bits: usize) -> Option<Vec<u8>> {
    if len_bits == 0 {
        return Some(Vec::new());
    }
    let first = start_bits / 8;
    let head = start_bits % 8;
    let last = (start_bits + len_bits).div_ceil(8);
    let src = bytes.get(first..last)?;
    let mut out = Vec::with_capacity(src.len() * 8);
    for &b in src {
        out.extend_from_slice(&UNPACK_BITS[b as usize]);
    }
    if head != 0 {
        out.copy_within(head..head + len_bits, 0);
    }
    out.truncate(len_bits);
    Some(out)
}

/// A `ColumnReader` plus a small most-recently-used cache of decoded blobs
/// (one slot unless [`CachedColumn::slots`] raises it).
///
/// Not thread-safe — expects to be owned by a single-threaded consumer
/// (cSRA decode spawns one `CsraCursor` per rayon worker).
pub(crate) struct CachedColumn {
    col: Arc<ColumnReader>,
    kind: ColumnKind,
    label: ColLabel,
    /// Most-recently-used first; never longer than `max_slots`.
    cache: RefCell<Vec<DecodedColumnBlob>>,
    max_slots: usize,
}

impl CachedColumn {
    pub fn new(col: ColumnReader, kind: ColumnKind) -> Self {
        Self::from_shared(Arc::new(col), kind)
    }

    /// Wrap a `ColumnReader` that is already shared. External refseq
    /// objects are opened once per accession and read by every decode
    /// worker, so the mmap is shared while each worker keeps its own
    /// single-slot blob cache (which is `RefCell`, hence not `Sync`).
    pub fn from_shared(col: Arc<ColumnReader>, kind: ColumnKind) -> Self {
        Self {
            col,
            kind,
            label: ColLabel::Other,
            cache: RefCell::new(Vec::new()),
            max_slots: 1,
        }
    }

    /// Keep up to `n` decoded blobs (LRU). Random-access columns such as
    /// PRIMARY_ALIGNMENT, where a spot's mates land in different blobs,
    /// thrash a single slot.
    pub fn slots(mut self, n: usize) -> Self {
        self.max_slots = n.max(1);
        self
    }

    /// Tag this column for the diagnostic counters.
    pub fn labelled(mut self, label: ColLabel) -> Self {
        self.label = label;
        self
    }

    /// Ensure the blob containing `row_id` is decoded (reusing the cached
    /// one when it matches), then hand `f` a reference + the row's
    /// blob-relative logical offset.
    pub fn with_blob<R>(
        &self,
        row_id: i64,
        f: impl FnOnce(&DecodedColumnBlob, usize) -> Result<R>,
    ) -> Result<R> {
        let blob = self
            .col
            .find_blob(row_id)
            .ok_or_else(|| Error::Format(format!("column: no blob for row {row_id}")))?;
        let start_id = blob.start_id;
        let id_range = blob.id_range;
        let logical_offset = (row_id - start_id) as usize;

        {
            let mut cache = self.cache.borrow_mut();
            if let Some(pos) = cache.iter().position(|c| c.start_id == start_id) {
                if pos != 0 {
                    cache[..=pos].rotate_right(1);
                }
                drop(cache);
                #[cfg(feature = "colstats")]
                bump(&HITS, self.label, 1);
                let borrow = self.cache.borrow();
                return f(&borrow[0], logical_offset);
            }
        }
        #[cfg(feature = "colstats")]
        bump(&MISSES, self.label, 1);

        let raw = self.col.read_raw_blob_slice(row_id)?;
        let decoded = blob::decode_blob(
            raw,
            self.col.meta().checksum_type,
            u64::from(id_range),
            self.kind.elem_bits_hint(),
        )?;
        let bytes = match self.kind {
            ColumnKind::Zip => decode_bytes_payload(&decoded)?,
            ColumnKind::Irzip { elem_bits } => decode_integer_bytes(&decoded, elem_bits)?,
            ColumnKind::TwoNa => decoded.data.to_vec(),
        };
        let page_map = decoded.page_map;
        let row_length = decoded.row_length;
        let extents = match page_map.as_ref() {
            Some(pm) if pm.total_rows() <= EAGER_EXTENT_MAX_ROWS => pm.row_extents()?,
            _ => Vec::new(),
        };
        #[cfg(feature = "colstats")]
        bump(&EXTENTS, self.label, extents.len() as u64);

        {
            let mut cache = self.cache.borrow_mut();
            if cache.len() >= self.max_slots {
                cache.pop();
            }
            cache.insert(
                0,
                DecodedColumnBlob {
                    start_id,
                    bytes,
                    page_map,
                    extents,
                    row_length,
                },
            );
        }

        let borrow = self.cache.borrow();
        f(&borrow[0], logical_offset)
    }

    // ----------------------------------------------------------------
    // Convenience readers — encapsulate the record-slicing logic that
    // used to be spread across alignment.rs / reference.rs / csra.rs.
    // ----------------------------------------------------------------

    /// Variable-length bit-packed record (length in bits). The payload
    /// is MSB-first within each byte, matching `HAS_MISMATCH` /
    /// `HAS_REF_OFFSET`.
    pub fn read_bool_row(&self, row_id: i64) -> Result<Vec<u8>> {
        debug_assert!(matches!(self.kind, ColumnKind::Zip));
        self.with_blob(row_id, |blob, logical_offset| {
            let extent = blob.extent(logical_offset)?;
            let start_bits = extent.offset as usize;
            let len_bits = extent.len as usize;
            unpack_bit_row(&blob.bytes, start_bits, len_bits).ok_or_else(|| {
                let bit_idx = (blob.bytes.len() * 8).max(start_bits);
                Error::Format(format!("bool row {row_id}: bit {bit_idx} past payload"))
            })
        })
    }

    /// Variable-length byte record (MISMATCH, READ_TYPE, QUALITY).
    pub fn read_byte_row(&self, row_id: i64) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        self.read_byte_row_into(row_id, &mut out)?;
        Ok(out)
    }

    /// [`read_byte_row`](Self::read_byte_row) into a caller-owned buffer
    /// (cleared first), so a per-spot loop reuses one allocation.
    pub fn read_byte_row_into(&self, row_id: i64, out: &mut Vec<u8>) -> Result<()> {
        debug_assert!(matches!(self.kind, ColumnKind::Zip));
        out.clear();
        self.with_blob(row_id, |blob, logical_offset| {
            let extent = blob.extent(logical_offset)?;
            let start = extent.offset as usize;
            let len = extent.len as usize;
            let end = start + len;
            if end > blob.bytes.len() {
                return Err(Error::Format(format!(
                    "byte row {row_id}: slice [{start}..{end}] past payload {}",
                    blob.bytes.len()
                )));
            }
            out.extend_from_slice(&blob.bytes[start..end]);
            Ok(())
        })
    }

    /// Variable-length i32 record (REF_OFFSET).
    pub fn read_i32_row(&self, row_id: i64) -> Result<Vec<i32>> {
        debug_assert!(matches!(self.kind, ColumnKind::Irzip { elem_bits: 32 }));
        self.with_blob(row_id, |blob, logical_offset| {
            let extent = blob.extent(logical_offset)?;
            let start = (extent.offset as usize) * 4;
            let len_elems = extent.len as usize;
            let end = start + len_elems * 4;
            if end > blob.bytes.len() {
                return Err(Error::Format(format!(
                    "i32 row {row_id}: slice [{start}..{end}] past payload {}",
                    blob.bytes.len()
                )));
            }
            Ok(blob.bytes[start..end]
                .as_chunks::<4>()
                .0
                .iter()
                .map(|&c| i32::from_le_bytes(c))
                .collect())
        })
    }

    /// Variable-length u32 record (SEQUENCE.READ_LEN), into a caller-owned
    /// buffer (cleared first).
    pub fn read_u32_row_into(&self, row_id: i64, out: &mut Vec<u32>) -> Result<()> {
        debug_assert!(matches!(self.kind, ColumnKind::Irzip { elem_bits: 32 }));
        out.clear();
        self.with_blob(row_id, |blob, logical_offset| {
            let extent = blob.extent(logical_offset)?;
            let start = (extent.offset as usize) * 4;
            let len_elems = extent.len as usize;
            let end = start + len_elems * 4;
            if end > blob.bytes.len() {
                return Err(Error::Format(format!(
                    "u32 row {row_id}: slice [{start}..{end}] past payload {}",
                    blob.bytes.len()
                )));
            }
            out.extend(
                blob.bytes[start..end]
                    .as_chunks::<4>()
                    .0
                    .iter()
                    .map(|&c| u32::from_le_bytes(c)),
            );
            Ok(())
        })
    }

    /// Variable-length i64 record (SEQUENCE.PRIMARY_ALIGNMENT_ID).
    pub fn read_i64_row(&self, row_id: i64) -> Result<Vec<i64>> {
        let mut out = Vec::new();
        self.read_i64_row_into(row_id, &mut out)?;
        Ok(out)
    }

    /// [`read_i64_row`](Self::read_i64_row) into a caller-owned buffer
    /// (cleared first).
    pub fn read_i64_row_into(&self, row_id: i64, out: &mut Vec<i64>) -> Result<()> {
        debug_assert!(matches!(self.kind, ColumnKind::Irzip { elem_bits: 64 }));
        out.clear();
        self.with_blob(row_id, |blob, logical_offset| {
            let extent = blob.extent(logical_offset)?;
            let start = (extent.offset as usize) * 8;
            let len_elems = extent.len as usize;
            let end = start + len_elems * 8;
            if end > blob.bytes.len() {
                return Err(Error::Format(format!(
                    "i64 row {row_id}: slice [{start}..{end}] past payload {}",
                    blob.bytes.len()
                )));
            }
            out.extend(
                blob.bytes[start..end]
                    .as_chunks::<8>()
                    .0
                    .iter()
                    .map(|&c| i64::from_le_bytes(c)),
            );
            Ok(())
        })
    }

    /// Scalar fixed-width integer with optional `data_runs` RLE (used by
    /// `GLOBAL_REF_START`, `REF_LEN`, `REFERENCE.SEQ_LEN`). The blob
    /// payload holds only the unique values; the row's extent resolves
    /// which one this row maps to.
    pub fn read_scalar_i64(&self, row_id: i64) -> Result<i64> {
        self.read_scalar_int(row_id, 64)
    }

    pub fn read_scalar_u32(&self, row_id: i64) -> Result<u32> {
        self.read_scalar_int(row_id, 32).map(|v| v as u32)
    }

    fn read_scalar_int(&self, row_id: i64, elem_bits: u32) -> Result<i64> {
        debug_assert!(matches!(
            self.kind,
            ColumnKind::Irzip { elem_bits: eb } if eb == elem_bits
        ));
        self.with_blob(row_id, |blob, logical_offset| {
            let extent = blob.extent(logical_offset)?;
            let bytes_per = (elem_bits / 8) as usize;
            let byte_off = extent.offset as usize * bytes_per;
            if byte_off + bytes_per > blob.bytes.len() {
                return Err(Error::Format(format!(
                    "scalar row {row_id}: byte offset {byte_off} past decoded {}",
                    blob.bytes.len()
                )));
            }
            let val = match elem_bits {
                32 => i64::from(i32::from_le_bytes([
                    blob.bytes[byte_off],
                    blob.bytes[byte_off + 1],
                    blob.bytes[byte_off + 2],
                    blob.bytes[byte_off + 3],
                ])),
                64 => i64::from_le_bytes([
                    blob.bytes[byte_off],
                    blob.bytes[byte_off + 1],
                    blob.bytes[byte_off + 2],
                    blob.bytes[byte_off + 3],
                    blob.bytes[byte_off + 4],
                    blob.bytes[byte_off + 5],
                    blob.bytes[byte_off + 6],
                    blob.bytes[byte_off + 7],
                ]),
                _ => return Err(Error::Format(format!("unsupported elem_bits {elem_bits}"))),
            };
            Ok(val)
        })
    }

    /// `take` bases of a 2na row starting `skip` bases into it.
    ///
    /// Callers that want a short span out of a wide row should use this
    /// rather than slicing the whole row: a refseq
    /// chunk is 5,000 bases and an alignment typically needs ~100, so
    /// unpacking the whole row throws away 98% of the work. `take` is
    /// clamped to what the row actually holds.
    pub fn read_2na_range(&self, row_id: i64, skip: usize, take: usize) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        self.append_2na_range(row_id, skip, take, &mut out)?;
        Ok(out)
    }

    /// [`read_2na_range`](Self::read_2na_range), appending to `out` rather
    /// than allocating.
    pub fn append_2na_range(
        &self,
        row_id: i64,
        skip: usize,
        take: usize,
        out: &mut Vec<u8>,
    ) -> Result<()> {
        debug_assert!(matches!(self.kind, ColumnKind::TwoNa));
        self.with_blob(row_id, |blob, logical_offset| {
            let (start_bases, len_bases) = two_na_extent(blob, logical_offset)?;
            // Narrow to the requested window before doing any unpacking.
            let skipped = skip.min(len_bases);
            let start_bases = start_bases + skipped;
            let len_bases = (len_bases - skipped).min(take);
            if len_bases == 0 {
                return Ok(());
            }
            // One bounds check up front; the unpack below then never fails.
            let last_byte = (start_bases + len_bases - 1) / 4;
            if last_byte >= blob.bytes.len() {
                return Err(Error::Format(format!(
                    "2na row {row_id}: bit {} past payload",
                    (start_bases + len_bases - 1) * 2
                )));
            }
            unpack_2na_to_4na(&blob.bytes, start_bases, len_bases, out);
            Ok(())
        })
    }

    /// Bases in a 2na row, without unpacking any of them. Callers need
    /// this to place a right-aligned ALTREAD mask when they are only
    /// reading part of the row.
    pub fn two_na_row_len(&self, row_id: i64) -> Result<usize> {
        debug_assert!(matches!(self.kind, ColumnKind::TwoNa));
        self.with_blob(row_id, |blob, logical_offset| {
            Ok(two_na_extent(blob, logical_offset)?.1)
        })
    }
}

/// 2na byte → four 4na nibbles (A C G T → 0x1 0x2 0x4 0x8), first base in
/// the most significant bit pair (verified against vdb-dump on HAS_MISMATCH
/// row 1 of VDB-3418).
static LUT_2NA_BYTE: [[u8; 4]; 256] = {
    let map = [0x1u8, 0x2, 0x4, 0x8];
    let mut t = [[0u8; 4]; 256];
    let mut b = 0;
    while b < 256 {
        t[b] = [
            map[(b >> 6) & 3],
            map[(b >> 4) & 3],
            map[(b >> 2) & 3],
            map[b & 3],
        ];
        b += 1;
    }
    t
};

/// Append `len` bases starting at base index `start` of the 2na-packed
/// `bytes` to `out` as 4na nibbles. The caller guarantees the range lies
/// inside `bytes`.
fn unpack_2na_to_4na(bytes: &[u8], start: usize, len: usize, out: &mut Vec<u8>) {
    out.reserve(len);
    let mut remaining = len;
    let mut byte_idx = start / 4;
    let head = start % 4;
    if head != 0 {
        let n = (4 - head).min(remaining);
        out.extend_from_slice(&LUT_2NA_BYTE[bytes[byte_idx] as usize][head..head + n]);
        remaining -= n;
        byte_idx += 1;
    }
    let full = remaining / 4;
    for &b in &bytes[byte_idx..byte_idx + full] {
        out.extend_from_slice(&LUT_2NA_BYTE[b as usize]);
    }
    byte_idx += full;
    remaining -= full * 4;
    if remaining > 0 {
        out.extend_from_slice(&LUT_2NA_BYTE[bytes[byte_idx] as usize][..remaining]);
    }
}

/// Where a 2na row sits in its decoded blob, as `(first base, base count)`.
///
/// Two storage shapes. Variable-length rows (SEQUENCE/REFERENCE CMP_READ)
/// carry a page map with per-row extents. Fixed-width rows (a refseq
/// object's READ) are v1 blobs with no page map at all — rows sit back to
/// back at `row_length` bases each. A column's final row is short, so
/// clamp to what the payload actually holds.
fn two_na_extent(blob: &DecodedColumnBlob, logical_offset: usize) -> Result<(usize, usize)> {
    match (&blob.page_map, blob.row_length) {
        (Some(_), _) => {
            let extent = blob.extent(logical_offset)?;
            Ok((extent.offset as usize, extent.len as usize))
        }
        (None, Some(row_length)) => {
            let width = row_length as usize;
            let start = logical_offset * width;
            let available = (blob.bytes.len() * 8 / 2).saturating_sub(start);
            Ok((start, width.min(available)))
        }
        (None, None) => Err(Error::Format(
            "2na column: neither a page map nor a fixed row length".into(),
        )),
    }
}

impl ColumnKind {
    /// `blob::decode_blob` currently ignores its elem_bits parameter, but
    /// we still pass the per-kind hint for forward compatibility and so
    /// logging is self-documenting.
    fn elem_bits_hint(self) -> u32 {
        match self {
            ColumnKind::Zip => 8,
            ColumnKind::Irzip { elem_bits } => elem_bits,
            ColumnKind::TwoNa => 2,
        }
    }
}

fn decode_bytes_payload(decoded: &DecodedBlob<'_>) -> Result<Vec<u8>> {
    let hdr = decoded.headers.first();
    let osize = hdr.map(|h| h.osize as usize).unwrap_or(decoded.data.len());
    if decoded.data.len() == osize {
        return Ok(decoded.data.to_vec());
    }
    if let Ok(out) = blob::deflate_decompress(&decoded.data, osize)
        && out.len() == osize
    {
        return Ok(out);
    }
    Err(Error::Format(format!(
        "byte column: no decoder succeeded (data.len={}, osize={osize})",
        decoded.data.len()
    )))
}

fn decode_integer_bytes(decoded: &DecodedBlob<'_>, elem_bits: u32) -> Result<Vec<u8>> {
    let hdr = decoded.headers.first();
    let osize = hdr.map(|h| h.osize as usize).unwrap_or(decoded.data.len());

    if let Some(h) = hdr
        && !h.ops.is_empty()
    {
        let planes = h.ops[0];
        let min = h.args.first().copied().unwrap_or(0);
        let slope = h.args.get(1).copied().unwrap_or(0);
        let num_elems = (osize as u32) / (elem_bits / 8);
        let series2 = h
            .args
            .get(2)
            .and_then(|&m2| h.args.get(3).map(|&s2| (m2, s2)));
        return blob::irzip_decode(
            &decoded.data,
            elem_bits,
            num_elems,
            min,
            slope,
            planes,
            series2,
        );
    }

    if decoded.data.len() == osize {
        return Ok(decoded.data.to_vec());
    }
    if let Ok(out) = blob::deflate_decompress(&decoded.data, osize)
        && out.len() == osize
    {
        return Ok(out);
    }

    if hdr.is_none() && !decoded.data.is_empty() {
        return blob::izip_decode(&decoded.data, elem_bits);
    }

    Err(Error::Format(format!(
        "integer column: no decoder succeeded (elem_bits={elem_bits}, data.len={}, osize={osize})",
        decoded.data.len()
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::{PageMap, RowMapping};

    #[test]
    fn unpack_2na_matches_scalar_for_every_window() {
        let bytes: Vec<u8> = (0..23u32).map(|i| (i * 37 + 11) as u8).collect();
        let scalar = |i: usize| -> u8 {
            let bit = i * 2;
            [0x1, 0x2, 0x4, 0x8][((bytes[bit / 8] >> (6 - bit % 8)) & 3) as usize]
        };
        let total = bytes.len() * 4;
        for start in 0..total {
            for len in 0..=(total - start) {
                let mut out = vec![0xAA];
                unpack_2na_to_4na(&bytes, start, len, &mut out);
                let want: Vec<u8> = std::iter::once(0xAA)
                    .chain((start..start + len).map(scalar))
                    .collect();
                assert_eq!(out, want, "start={start} len={len}");
            }
        }
    }

    fn pm(mapping: RowMapping, lengths: Vec<u32>, leng_runs: Vec<u32>) -> PageMap {
        let data_recs = match &mapping {
            RowMapping::RepeatCounts(v) => v.len() as u64,
            _ => leng_runs.iter().map(|&r| u64::from(r)).sum(),
        };
        PageMap {
            data_recs,
            lengths,
            leng_runs,
            mapping,
        }
    }

    /// Identity: rows sit back to back, each one element wide.
    #[test]
    fn extents_identity_are_contiguous() {
        let extents = pm(RowMapping::Identity, vec![1], vec![4])
            .row_extents()
            .unwrap();
        let offsets: Vec<u32> = extents.iter().map(|e| e.offset).collect();
        assert_eq!(offsets, vec![0, 1, 2, 3]);
        assert!(extents.iter().all(|e| e.len == 1));
    }

    /// Repeat counts: [3, 2, 4] means rows 0..3 share record 0, rows 3..5
    /// share record 1, rows 5..9 share record 2 — so the data cursor advances
    /// once per record, not once per row.
    #[test]
    fn extents_repeat_counts_share_offsets() {
        let extents = pm(RowMapping::RepeatCounts(vec![3, 2, 4]), vec![1], vec![9])
            .row_extents()
            .unwrap();
        let offsets: Vec<u32> = extents.iter().map(|e| e.offset).collect();
        assert_eq!(offsets, vec![0, 0, 0, 1, 1, 2, 2, 2, 2]);
    }

    /// Random access: the stored value IS the row's element offset. These are
    /// not record indices — they may repeat and may decrease.
    #[test]
    fn extents_random_access_use_offsets_verbatim() {
        let offs = vec![2, 0, 1, 3];
        let extents = pm(
            RowMapping::RandomAccessOffsets(offs.clone()),
            vec![1],
            vec![4],
        )
        .row_extents()
        .unwrap();
        assert_eq!(
            extents.iter().map(|e| e.offset).collect::<Vec<_>>(),
            offs,
            "random-access offsets must pass through untouched"
        );
    }

    /// A row wider than one element scales the offset by the row length only
    /// through `lengths`, never by multiplying the stored offset.
    #[test]
    fn extents_random_access_multi_element_rows() {
        let extents = pm(
            RowMapping::RandomAccessOffsets(vec![0, 4, 0]),
            vec![2],
            vec![3],
        )
        .row_extents()
        .unwrap();
        assert_eq!(
            extents
                .iter()
                .map(|e| (e.offset, e.len))
                .collect::<Vec<_>>(),
            vec![(0, 2), (4, 2), (0, 2)]
        );
    }

    #[test]
    fn decoded_extent_without_page_map_is_identity() {
        let blob = DecodedColumnBlob {
            start_id: 0,
            bytes: Vec::new(),
            page_map: None,
            extents: Vec::new(),
            row_length: None,
        };
        assert_eq!(blob.extent(0).unwrap(), RowExtent { offset: 0, len: 1 });
        assert_eq!(blob.extent(42).unwrap(), RowExtent { offset: 42, len: 1 });
    }

    #[test]
    fn decoded_extent_lookup_and_bounds() {
        let blob = DecodedColumnBlob {
            start_id: 0,
            bytes: Vec::new(),
            page_map: None,
            extents: vec![
                RowExtent { offset: 0, len: 3 },
                RowExtent { offset: 0, len: 3 },
                RowExtent { offset: 3, len: 3 },
            ],
            row_length: None,
        };
        assert_eq!(blob.extent(0).unwrap(), RowExtent { offset: 0, len: 3 });
        assert_eq!(blob.extent(2).unwrap(), RowExtent { offset: 3, len: 3 });
        assert!(blob.extent(3).is_err());
    }
}

#[cfg(test)]
mod unpack_tests {
    #[test]
    fn unpack_bit_row_matches_per_bit_loop() {
        let bytes: Vec<u8> = (0..64u32)
            .map(|i| (i.wrapping_mul(2654435761) >> 7) as u8)
            .collect();
        for start in 0..40usize {
            for len in 0..120usize {
                let want: Option<Vec<u8>> = (0..len)
                    .map(|i| {
                        let bit = start + i;
                        bytes.get(bit / 8).map(|b| (b >> (7 - bit % 8)) & 1)
                    })
                    .collect();
                assert_eq!(
                    super::unpack_bit_row(&bytes, start, len),
                    want,
                    "{start}+{len}"
                );
            }
        }
        // Past the end.
        assert_eq!(super::unpack_bit_row(&bytes, 63 * 8, 9), None);
    }
}
