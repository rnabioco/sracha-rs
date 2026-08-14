//! VDB column blob decoding.
//!
//! A VDB column blob stored on disk has this structure:
//!
//! ```text
//!   [ blob_header | transform_headers | page_map | column_data ]  [ checksum ]
//! ```
//!
//! The blob header byte encodes (for v2 blobs, where bit 7 is set):
//!
//! - bits 0-2: adjust (unused trailing bits in last data byte)
//! - bit 3: byte order (0 = little-endian, 1 = big-endian)
//! - bits 4-5: variant (determines sizes of hdr_size/map_size fields)
//! - bits 6-7: version (must be 2)
//!
//! For v1 blobs (bit 7 clear), the header encodes row length and byte order
//! directly. The data follows immediately.
//!
//! After the blob data, the checksum is stored (4 bytes CRC32, 16 bytes MD5,
//! or none), depending on the column's checksum_type.
//!
//! This module also provides:
//! - [`vlen_decode_u64`]: Variable-length unsigned integer decoding (used in page maps).
//! - [`vlen_decode_i64`]: Variable-length signed integer decoding (used in blob headers).
//! - [`izip_decode`]: Integer decompression for READ_LEN, READ_START, etc.
//! - [`unpack`]: Bit-unpacking from packed to unpacked element sizes.
//! - [`page_map_deserialize`]: Page map deserialization.
//! - [`blob_headers_deserialize`]: Blob header stack deserialization.

use std::borrow::Cow;
use std::fmt::Write as _;

use md5::{Digest, Md5};

use crate::error::{Error, Result};

fn hex16(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

// CRC32 as used by ncbi-vdb column blobs: MSB-first, polynomial 0x04C11DB7,
// init=0, no reflection, no final XOR. This does NOT match CRC-32/ISO-HDLC
// (the `crc32fast` crate) — they share a polynomial but differ on reflection
// and seed. Source: ncbi-vdb/libs/klib/crc32.c.
const fn build_crc32_table() -> [u32; 256] {
    let mut t = [0u32; 256];
    let poly: u32 = 0x04C11DB7;
    let mut i = 0;
    while i < 256 {
        let mut c: u32 = (i as u32) << 24;
        let mut j = 0;
        while j < 8 {
            c = if c & 0x8000_0000 != 0 {
                (c << 1) ^ poly
            } else {
                c << 1
            };
            j += 1;
        }
        t[i] = c;
        i += 1;
    }
    t
}

static NCBI_CRC32_TABLE: [u32; 256] = build_crc32_table();

pub(crate) fn ncbi_crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0;
    for &b in data {
        let idx = ((crc >> 24) as u8) ^ b;
        crc = (crc << 8) ^ NCBI_CRC32_TABLE[idx as usize];
    }
    crc
}

// ---------------------------------------------------------------------------
// Variable-length integer encoding (vlen)
// ---------------------------------------------------------------------------

/// Decode a variable-length encoded unsigned integer.
///
/// The encoding uses 7 data bits per byte with the high bit as a continuation
/// flag: if bit 7 is set, more bytes follow.
///
/// Returns `(value, bytes_consumed)`.
#[inline]
pub fn vlen_decode_u64(data: &[u8]) -> Result<(u64, usize)> {
    if data.is_empty() {
        return Err(Error::Format("vlen_decode_u64: empty input".into()));
    }

    let limit = data.len().min(10);
    let mut value: u64 = 0;
    let mut i = 0;

    loop {
        if i >= limit {
            return Err(Error::Format(
                "vlen_decode_u64: too many continuation bytes".into(),
            ));
        }
        let byte = data[i];
        value = (value << 7) | u64::from(byte & 0x7F);
        i += 1;
        if byte & 0x80 == 0 {
            return Ok((value, i));
        }
    }
}

/// Decode a variable-length encoded signed integer.
///
/// The first byte uses bit 6 as a sign flag and bits 0-5 as data.
/// Subsequent bytes use 7 data bits with bit 7 as continuation.
///
/// Returns `(value, bytes_consumed)`.
#[inline]
pub fn vlen_decode_i64(data: &[u8]) -> Result<(i64, usize)> {
    if data.is_empty() {
        return Err(Error::Format("vlen_decode_i64: empty input".into()));
    }

    let limit = data.len().min(10);
    let first = data[0];
    let negative = first & 0x40 != 0;
    let mut value: i64 = i64::from(first & 0x3F);
    let mut i = 1;

    if first & 0x80 != 0 {
        loop {
            if i >= limit {
                return Err(Error::Format(
                    "vlen_decode_i64: too many continuation bytes".into(),
                ));
            }
            let byte = data[i];
            value = (value << 7) | i64::from(byte & 0x7F);
            i += 1;
            if byte & 0x80 == 0 {
                break;
            }
        }
    }

    if negative {
        value = -value;
    }

    Ok((value, i))
}

/// Decode a sequence of `count` variable-length encoded unsigned integers.
///
/// Returns `(values, total_bytes_consumed)`.
pub fn vlen_decode_u64_array(data: &[u8], count: usize) -> Result<(Vec<u64>, usize)> {
    let mut result = Vec::with_capacity(count);
    let mut offset = 0;
    for _ in 0..count {
        let (val, consumed) = vlen_decode_u64(&data[offset..])?;
        result.push(val);
        offset += consumed;
    }
    Ok((result, offset))
}

// ---------------------------------------------------------------------------
// Page map deserialization
// ---------------------------------------------------------------------------

/// Side that `vdb:trim` removed from each row when the column was
/// written. Mirrors the transform's first template argument
/// (`0 = leading`, `1 = trailing`). On restore, leading-trimmed rows
/// right-align their stored bytes inside the padded row; trailing-
/// trimmed rows left-align.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrimSide {
    Leading,
    Trailing,
}

/// How a page map maps logical rows onto the blob's element stream.
///
/// ncbi-vdb's `PageMap` carries a `random_access` flag plus two arrays that
/// share one allocation — `data_run` (repeat counts) and `data_offset`
/// (per-row offsets) — with the flag deciding which one is live
/// (`libs/kdb/page-map.c`, `PageMapDeserialize_v0`). Modelling that as one
/// untyped `Vec<u32>` is what produced issue #101: a version-2 blob's
/// `data_offset[]` was walked as though the entries were repeat counts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowMapping {
    /// Every data record covers exactly one logical row — the `data_run[..]
    /// == 1` case (variants 0 and 2 without random access). Row data is laid
    /// out contiguously in row order.
    Identity,
    /// `repeats[i]` consecutive logical rows all share data record `i`
    /// (variants 1 and 3). The read cursor advances by one record's element
    /// count per *record*, not per row, so a record covering many rows is
    /// stored once.
    RepeatCounts(Vec<u32>),
    /// `offsets[r]` is the element offset of logical row `r`'s data within
    /// the blob's element stream — ncbi-vdb's `data_offset[row_count]`,
    /// written only by version 2 (`random_access`) and only for variants 0
    /// and 2.
    ///
    /// The values are element offsets (`elem_count_t`), not byte offsets and
    /// not record indices. They may repeat and may decrease: the writer runs
    /// a vocabulary encoder (`libs/vdb/blob.c`, `VBlobPageMapOptimize`), so
    /// distinct rows with identical content point at the same offset and the
    /// data buffer is usually far smaller than `sum(row lengths)`.
    RandomAccessOffsets(Vec<u32>),
}

impl RowMapping {
    /// Repeat counts, when this map stores them.
    pub fn repeat_counts(&self) -> Option<&[u32]> {
        match self {
            RowMapping::RepeatCounts(v) => Some(v),
            _ => None,
        }
    }

    /// Per-row element offsets, when this map stores them.
    pub fn row_offsets(&self) -> Option<&[u32]> {
        match self {
            RowMapping::RandomAccessOffsets(v) => Some(v),
            _ => None,
        }
    }

    pub fn is_identity(&self) -> bool {
        matches!(self, RowMapping::Identity)
    }
}

/// Maximum logical rows per blob — reject page maps whose `leng_runs` sum to
/// more than this. Real SRA blobs hold well under it (a few million rows is
/// typical, hundreds of millions is the high end).
pub const MAX_LOGICAL_ROWS_PER_BLOB: u64 = 1_000_000_000;

/// Yields one element count per logical row by walking `lengths` / `leng_runs`
/// without expanding them.
struct LengthRunCursor<'a> {
    lengths: &'a [u32],
    leng_runs: &'a [u32],
    idx: usize,
    left_in_run: u32,
}

impl<'a> LengthRunCursor<'a> {
    fn new(pm: &'a PageMap) -> Self {
        Self {
            lengths: &pm.lengths,
            leng_runs: &pm.leng_runs,
            idx: 0,
            left_in_run: pm.leng_runs.first().copied().unwrap_or(0),
        }
    }

    /// Element count of the row at the cursor, without advancing.
    fn peek(&mut self) -> Option<u32> {
        while self.left_in_run == 0 {
            self.idx += 1;
            self.left_in_run = *self.leng_runs.get(self.idx)?;
        }
        self.lengths.get(self.idx).copied()
    }

    /// Advance past `n` rows and return the elements they span.
    ///
    /// Whole runs are consumed at a time, so skipping a record that covers
    /// millions of rows costs one iteration per run crossed, not one per row.
    fn skip_rows(&mut self, n: u64) -> u64 {
        let mut remaining = n;
        let mut span = 0u64;
        while remaining > 0 {
            let Some(len) = self.peek() else { break };
            let take = remaining.min(u64::from(self.left_in_run));
            span += take * u64::from(len);
            self.left_in_run -= take as u32;
            remaining -= take;
        }
        span
    }
}

/// Where one logical row's data sits in the blob's element stream.
///
/// Units are *elements*, matching ncbi-vdb's `PageMapIteratorDataOffset` /
/// `PageMapIteratorDataLength`: multiply by the column's element size to get
/// bytes. Two rows may share an offset when the writer deduplicated them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowExtent {
    pub offset: u32,
    pub len: u32,
}

/// Deserialized page map describing row boundaries within a blob.
#[derive(Debug, Clone)]
pub struct PageMap {
    /// Number of data records in the blob. Equals the row count for the
    /// identity and random-access mappings.
    pub data_recs: u64,
    /// Row lengths (one per unique length run).
    pub lengths: Vec<u32>,
    /// Length runs (how many consecutive rows share the same length).
    pub leng_runs: Vec<u32>,
    /// How logical rows map onto stored data.
    pub mapping: RowMapping,
}

impl PageMap {
    /// Total number of logical rows described by this page map.
    ///
    /// This is the sum of `leng_runs` (each entry tells how many consecutive
    /// rows share the same length).
    pub fn total_rows(&self) -> u64 {
        self.leng_runs.iter().map(|&r| u64::from(r)).sum()
    }

    /// Repeat counts, when the mapping stores them.
    pub fn repeat_counts(&self) -> Option<&[u32]> {
        self.mapping.repeat_counts()
    }

    /// One element count per logical row, expanded from `lengths` /
    /// `leng_runs`.
    ///
    /// Independent of the mapping: ncbi-vdb keeps the length runs unchanged
    /// when it converts a page map to random access
    /// (`PageMapToRandomAccess`), so row lengths always come from here.
    pub fn logical_row_lengths(&self) -> Vec<u32> {
        let mut out = Vec::with_capacity(self.total_rows() as usize);
        for (&len, &run) in self.lengths.iter().zip(self.leng_runs.iter()) {
            for _ in 0..run {
                out.push(len);
            }
        }
        out
    }

    /// Resolve every logical row to its `(offset, len)` in the blob's element
    /// stream.
    ///
    /// This is the single place the three mappings are interpreted; it mirrors
    /// the reference walk in ncbi-vdb's `PageMapFindRow` (page-map.c:431-461)
    /// and the `data_offset[row]` lookup in `PageMapIteratorDataOffset`.
    ///
    /// Returns fewer than `total_rows()` entries when the map is internally
    /// inconsistent (runs that don't cover every row); callers that care check
    /// the length.
    ///
    /// Errors when `leng_runs` sums past [`MAX_LOGICAL_ROWS_PER_BLOB`], which
    /// a crafted page map could otherwise use to force a huge allocation.
    pub fn row_extents(&self) -> Result<Vec<RowExtent>> {
        self.row_extents_range(0, self.total_rows() as usize)
    }

    /// [`row_extents`](Self::row_extents) for the window `[skip, skip + take)`.
    pub fn row_extents_range(&self, skip: usize, take: usize) -> Result<Vec<RowExtent>> {
        let mut out = Vec::with_capacity(take.min(self.total_rows() as usize));
        self.for_each_row_extent(skip, take, |_, e| {
            out.push(e);
            Ok(())
        })?;
        Ok(out)
    }

    /// Total elements across every logical row, without allocating.
    fn total_elems(&self) -> u64 {
        self.lengths
            .iter()
            .zip(self.leng_runs.iter())
            .map(|(&l, &r)| u64::from(l) * u64::from(r))
            .sum()
    }

    /// Visit each logical row's extent in `[skip, skip + take)`, in row order.
    ///
    /// Costs one step per length run and data record crossed — proportional to
    /// the *encoded* size, not the row count — plus one call per row visited.
    /// The decode paths use this rather than [`row_extents`] so a blob costs no
    /// per-row allocation; materializing one `RowExtent` per row for every blob
    /// of every column was worth about 2x the decode CPU on archives whose page
    /// maps are not identity. Blobs where a single record covers millions of
    /// rows (SRR18959644's first READ_TYPE record spans 22,227,968 spots) are
    /// read once per READ blob, so a per-row walk here would be quadratic.
    fn for_each_row_extent(
        &self,
        skip: usize,
        take: usize,
        mut f: impl FnMut(usize, RowExtent) -> Result<()>,
    ) -> Result<()> {
        let total_rows = self.total_rows();
        if total_rows > MAX_LOGICAL_ROWS_PER_BLOB {
            return Err(Error::Format(format!(
                "page_map: logical row count {total_rows} exceeds {MAX_LOGICAL_ROWS_PER_BLOB} cap"
            )));
        }
        let total = total_rows as usize;
        let end = skip.saturating_add(take).min(total);
        if skip >= end {
            return Ok(());
        }

        let mut lens = LengthRunCursor::new(self);

        match &self.mapping {
            RowMapping::Identity => {
                // Rows sit back to back, so the first requested row starts at
                // however many elements every earlier row spans.
                let mut offset = lens.skip_rows(skip as u64).min(u64::from(u32::MAX)) as u32;
                for row in skip..end {
                    let Some(len) = lens.peek() else { break };
                    lens.skip_rows(1);
                    f(row, RowExtent { offset, len })?;
                    offset = offset.saturating_add(len);
                }
            }
            RowMapping::RepeatCounts(repeats) => {
                // Each record covers `repeat` rows sharing one stored copy; the
                // cursor advances one row length per *record*.
                let mut offset = 0u32;
                let mut row = 0usize;
                for &repeat in repeats {
                    if row >= end {
                        break;
                    }
                    let Some(len) = lens.peek() else { break };
                    let rows_here = repeat as usize;
                    lens.skip_rows(u64::from(repeat));
                    // Only the part of this record's span inside the window is
                    // visited; a record covering millions of rows outside it
                    // costs nothing.
                    let lo = row.max(skip);
                    let hi = (row + rows_here).min(end);
                    for r in lo..hi {
                        f(r, RowExtent { offset, len })?;
                    }
                    offset = offset.saturating_add(len);
                    row += rows_here;
                }
            }
            RowMapping::RandomAccessOffsets(offsets) => {
                lens.skip_rows(skip as u64);
                for row in skip..end {
                    let Some(len) = lens.peek() else { break };
                    lens.skip_rows(1);
                    // A zero-length row can point anywhere; the writer emits 0
                    // for those (libs/vdb/blob.c:800-806).
                    let offset = offsets.get(row).copied().unwrap_or(0);
                    f(row, RowExtent { offset, len })?;
                }
            }
        }

        Ok(())
    }

    /// Gather every logical row's data into one flat, row-ordered buffer.
    ///
    /// `data` is the decompressed payload; `elem_bytes` is the column's true
    /// element size (1 for byte columns, 4 for u32). Under
    /// [`RowMapping::RepeatCounts`] a record stored once is emitted once per
    /// row it covers; under [`RowMapping::RandomAccessOffsets`] each row is
    /// pulled from its own offset, so deduplicated rows are materialized
    /// again. The result always holds `sum(logical_row_lengths) * elem_bytes`
    /// bytes.
    ///
    /// Errors when a row's slice would run past the end of `data`.
    pub fn expand_rows(&self, data: &[u8], elem_bytes: usize) -> Result<Vec<u8>> {
        if elem_bytes == 0 {
            return Err(Error::Format(
                "page_map: elem_bytes must be non-zero".into(),
            ));
        }

        let total_rows = self.total_rows() as usize;
        if total_rows > MAX_LOGICAL_ROWS_PER_BLOB as usize {
            return Err(Error::Format(format!(
                "page_map: logical row count {total_rows} exceeds {MAX_LOGICAL_ROWS_PER_BLOB} cap"
            )));
        }
        let out_bytes = (self.total_elems() as usize)
            .checked_mul(elem_bytes)
            .ok_or_else(|| Error::Format("page_map: expanded size overflows".into()))?;

        match &self.mapping {
            // Rows are already contiguous and in row order.
            RowMapping::Identity => Ok(data.to_vec()),

            // The hot path: one stored copy backs `repeat` consecutive rows.
            // Per-record rather than per-row so the slice arithmetic is hoisted
            // out of the repeat loop — this function is ~two thirds of decode
            // CPU on archives that use it, and doing the bounds math once per
            // row instead of once per record costs about 2x.
            RowMapping::RepeatCounts(repeats) => {
                let mut out = Vec::with_capacity(out_bytes);
                let mut lens = LengthRunCursor::new(self);
                let mut cursor = 0usize;
                let mut rows_seen = 0usize;
                for &repeat in repeats {
                    let Some(len) = lens.peek() else { break };
                    lens.skip_rows(u64::from(repeat));
                    let nbytes = (len as usize)
                        .checked_mul(elem_bytes)
                        .ok_or_else(|| Error::Format("page_map: row length overflows".into()))?;
                    let end = cursor
                        .checked_add(nbytes)
                        .ok_or_else(|| Error::Format("page_map: record extent overflows".into()))?;
                    if end > data.len() {
                        return Err(Error::Format(format!(
                            "page_map: record wants data[{cursor}..{end}] but data has {} bytes",
                            data.len(),
                        )));
                    }
                    let chunk = &data[cursor..end];
                    for _ in 0..repeat {
                        out.extend_from_slice(chunk);
                    }
                    cursor = end;
                    rows_seen += repeat as usize;
                }
                if rows_seen != total_rows {
                    return Err(Error::Format(format!(
                        "page_map: mapping covers {rows_seen} of {total_rows} rows \
                         (inconsistent runs)",
                    )));
                }
                Ok(out)
            }

            // Every row indexes its own slice out of a deduplicated pool, so
            // there is nothing to hoist.
            RowMapping::RandomAccessOffsets(offsets) => {
                let mut out = Vec::with_capacity(out_bytes);
                let mut lens = LengthRunCursor::new(self);
                for row in 0..total_rows {
                    let Some(len) = lens.peek() else { break };
                    lens.skip_rows(1);
                    let start = (offsets.get(row).copied().unwrap_or(0) as usize)
                        .checked_mul(elem_bytes)
                        .ok_or_else(|| Error::Format("page_map: row offset overflows".into()))?;
                    let nbytes = (len as usize)
                        .checked_mul(elem_bytes)
                        .ok_or_else(|| Error::Format("page_map: row length overflows".into()))?;
                    let end = start
                        .checked_add(nbytes)
                        .ok_or_else(|| Error::Format("page_map: row extent overflows".into()))?;
                    if end > data.len() {
                        return Err(Error::Format(format!(
                            "page_map: row {row} wants data[{start}..{end}] but data has {} bytes",
                            data.len(),
                        )));
                    }
                    out.extend_from_slice(&data[start..end]);
                }
                if out.len() != out_bytes {
                    return Err(Error::Format(format!(
                        "page_map: expanded {} bytes, expected {out_bytes} (inconsistent runs)",
                        out.len(),
                    )));
                }
                Ok(out)
            }
        }
    }

    /// Expand a per-row-trimmed column (e.g. ALTREAD `trim<0,0>`) to a flat
    /// `total_rows * row_bytes` buffer, zero-padding the positions the trim
    /// removed at write time.
    ///
    /// Each row's stored bytes are copied into the appropriate end of its
    /// slot — right-aligned for [`TrimSide::Leading`], left-aligned for
    /// [`TrimSide::Trailing`] — and the rest is left zero.
    ///
    /// Fails if a stored row is wider than `row_bytes` (which would silently
    /// drop data) or if a row's bytes run past the end of `data`.
    pub fn pad_trimmed_rows_fixed(
        &self,
        data: &[u8],
        row_bytes: usize,
        side: TrimSide,
    ) -> Result<Vec<u8>> {
        self.pad_trimmed_rows(data, |_| row_bytes, side)
    }

    /// Variable-target version of [`pad_trimmed_rows_fixed`].
    ///
    /// Used for columns whose logical rows have non-uniform true widths — e.g.
    /// ALTREAD on Illumina runs after adapter trimming, where each spot's base
    /// count matches that spot's `READ_LEN` sum. `row_lens` must have
    /// `self.total_rows()` entries and gives each row's full pre-trim width;
    /// the page map's own `lengths` give the trimmed width actually stored.
    ///
    /// Returns `sum(row_lens)` bytes — every logical row's padded bytes
    /// concatenated — so callers merging against another variable-row column
    /// can iterate byte for byte.
    pub fn pad_trimmed_rows_variable(
        &self,
        data: &[u8],
        row_lens: &[u32],
        side: TrimSide,
    ) -> Result<Vec<u8>> {
        let total_rows = self.total_rows() as usize;
        if row_lens.len() != total_rows {
            return Err(Error::Format(format!(
                "page_map: row_lens has {} entries, expected {total_rows} (total_rows)",
                row_lens.len(),
            )));
        }
        self.pad_trimmed_rows(data, |row| row_lens[row] as usize, side)
    }

    /// Shared streaming implementation behind both `pad_trimmed_rows_*` entry
    /// points. `target` gives each logical row's full pre-trim width; the
    /// stored bytes are right-aligned inside it for [`TrimSide::Leading`] and
    /// left-aligned for [`TrimSide::Trailing`], leaving the trimmed end zero.
    ///
    /// Handles all three mappings, including the random-access one where
    /// several rows share one stored copy.
    fn pad_trimmed_rows(
        &self,
        data: &[u8],
        target: impl Fn(usize) -> usize,
        side: TrimSide,
    ) -> Result<Vec<u8>> {
        let total_rows = self.total_rows() as usize;
        let total_bytes: usize = (0..total_rows).map(&target).sum();
        let mut out = vec![0u8; total_bytes];

        let mut out_off = 0usize;
        let mut rows_seen = 0usize;
        self.for_each_row_extent(0, total_rows, |row, extent| {
            let target = target(row);
            let stored = extent.len as usize;
            if stored > target {
                return Err(Error::Format(format!(
                    "page_map: row {row} stored {stored} bytes exceeds target {target}"
                )));
            }
            if stored > 0 {
                let start = extent.offset as usize;
                let end = start.checked_add(stored).ok_or_else(|| {
                    Error::Format(format!("page_map: row {row} extent overflows"))
                })?;
                if end > data.len() {
                    return Err(Error::Format(format!(
                        "page_map: row {row} wants data[{start}..{end}] but data has {} bytes",
                        data.len(),
                    )));
                }
                let bytes = &data[start..end];
                match side {
                    TrimSide::Leading => {
                        let pad = target - stored;
                        out[out_off + pad..out_off + target].copy_from_slice(bytes);
                    }
                    TrimSide::Trailing => {
                        out[out_off..out_off + stored].copy_from_slice(bytes);
                    }
                }
            }
            out_off += target;
            rows_seen += 1;
            Ok(())
        })?;

        if rows_seen != total_rows {
            return Err(Error::Format(format!(
                "page_map: mapping covers {rows_seen} of {total_rows} rows (inconsistent runs)",
            )));
        }

        Ok(out)
    }
}

/// Deserialize a page map from its serialized form.
///
/// The first byte encodes `variant` (bits 0-1) and `version` (bits 2+).
/// Version 0 uses the v0 deserializer directly. Versions 1-2 use v1 which
/// may delegate to v0 after decompression.
pub fn page_map_deserialize(data: &[u8], row_count: u64) -> Result<PageMap> {
    if data.is_empty() {
        return Err(Error::Format("page_map_deserialize: empty input".into()));
    }

    let version = data[0] >> 2;

    match version {
        0 => page_map_deserialize_v0(data, row_count),
        1 | 2 => page_map_deserialize_v1(data, row_count),
        _ => Err(Error::Format(format!(
            "page_map_deserialize: unsupported version {version}"
        ))),
    }
}

/// Deserialize a sequence of vlen-encoded u32 values from raw bytes.
fn deserialize_lengths(data: &[u8], count: usize) -> Result<(Vec<u32>, usize)> {
    let mut result = Vec::with_capacity(count);
    let mut offset = 0;
    for _ in 0..count {
        let (val, consumed) = vlen_decode_u64(&data[offset..])?;
        result.push(val as u32);
        offset += consumed;
    }
    Ok((result, offset))
}

fn page_map_deserialize_v0(data: &[u8], row_count: u64) -> Result<PageMap> {
    if data.is_empty() {
        return Err(Error::Format("page_map_v0: empty input".into()));
    }

    let variant = data[0] & 3;
    let mut cur = 1;

    let random_access = (data[0] >> 2) == 2;

    // Random access is only ever written for variants 0 and 2: the encoder
    // derives variant bit 0 from `data_recs != row_count`, and
    // `PageMapToRandomAccess` always sets `data_recs = row_count`
    // (ncbi-vdb page-map.c:729, :1098). Variants 1 and 3 reserve no space for
    // `data_offset[]`, so a version-2 header on one of them is malformed —
    // ncbi-vdb would leave `data_offset` NULL and dereference it later.
    if random_access && (variant == 1 || variant == 3) {
        return Err(Error::Format(format!(
            "page_map: version 2 (random access) is not valid with variant {variant} — \
             only variants 0 and 2 carry data_offset[]"
        )));
    }

    match variant {
        0 => {
            // Fixed row length.
            let (row_len, sz) = vlen_decode_u64(&data[cur..])?;
            cur += sz;

            let mapping = if random_access {
                // `data_offset[row_count]`: one element offset per logical row.
                let (data_offsets, _) = deserialize_lengths(&data[cur..], row_count as usize)?;
                RowMapping::RandomAccessOffsets(data_offsets)
            } else {
                RowMapping::Identity
            };

            Ok(PageMap {
                // ncbi-vdb sets data_recs = row_count for this variant whether
                // or not random access is on (page-map.c:1288).
                data_recs: row_count,
                lengths: vec![row_len as u32],
                leng_runs: vec![row_count as u32],
                mapping,
            })
        }
        1 => {
            // Fixed row length, variable data_run.
            let (row_len, sz) = vlen_decode_u64(&data[cur..])?;
            cur += sz;

            let (data_recs, sz) = vlen_decode_u64(&data[cur..])?;
            cur += sz;

            let (data_runs, _) = deserialize_lengths(&data[cur..], data_recs as usize)?;

            Ok(PageMap {
                data_recs,
                lengths: vec![row_len as u32],
                leng_runs: vec![row_count as u32],
                mapping: RowMapping::RepeatCounts(data_runs),
            })
        }
        2 => {
            // Variable row length, data_run = 1.
            //
            // When random_access is set (page_map version 2), this variant
            // additionally stores `data_offset[row_count]` after the
            // lengths/leng_runs pair — one element offset per logical row.
            // NAME_FMT blobs on Illumina HiSeq archives (DRR040793 blob 2 et
            // al) rely on it for their per-row template overrides; without it
            // sracha falls back to the skey range mapping, which can't
            // reproduce the fine-grained tile interleave.
            let (leng_recs, sz) = vlen_decode_u64(&data[cur..])?;
            cur += sz;

            // Both lengths and leng_runs are serialized sequentially.
            let total = 2 * leng_recs as usize;
            let (combined, sz) = deserialize_lengths(&data[cur..], total)?;
            cur += sz;

            let lengths = combined[..leng_recs as usize].to_vec();
            let leng_runs = combined[leng_recs as usize..].to_vec();

            let mapping = if random_access {
                let (offsets, _) = deserialize_lengths(&data[cur..], row_count as usize)?;
                RowMapping::RandomAccessOffsets(offsets)
            } else {
                RowMapping::Identity
            };

            Ok(PageMap {
                data_recs: row_count,
                lengths,
                leng_runs,
                mapping,
            })
        }
        3 => {
            // Variable row length, variable data_run.
            let (leng_recs, sz) = vlen_decode_u64(&data[cur..])?;
            cur += sz;

            let (data_recs, sz) = vlen_decode_u64(&data[cur..])?;
            cur += sz;

            let total = 2 * leng_recs as usize + data_recs as usize;
            let (combined, _) = deserialize_lengths(&data[cur..], total)?;

            let lengths = combined[..leng_recs as usize].to_vec();
            let leng_runs = combined[leng_recs as usize..2 * leng_recs as usize].to_vec();
            let data_runs = combined[2 * leng_recs as usize..].to_vec();

            Ok(PageMap {
                data_recs,
                lengths,
                leng_runs,
                mapping: RowMapping::RepeatCounts(data_runs),
            })
        }
        _ => Err(Error::Format(format!(
            "page_map_v0: unsupported variant {variant}"
        ))),
    }
}

fn page_map_deserialize_v1(data: &[u8], row_count: u64) -> Result<PageMap> {
    if data.is_empty() {
        return Err(Error::Format("page_map_v1: empty input".into()));
    }

    let variant = data[0] & 3;
    let random_access = (data[0] >> 2) == 2;

    // For variant 0 without random access, delegate directly to v0.
    if variant == 0 && !random_access {
        return page_map_deserialize_v0(data, row_count);
    }

    // Parse the header to determine hsize and bsize.
    let src = &data[1..];
    let endp = src.len();

    let (hsize, bsize) = match variant {
        0 => {
            // random_access variant 0
            let (val, sz) = vlen_decode_u64(src)?;
            let _ = val; // row_len
            let hdr_bytes = 1 + sz;
            (hdr_bytes, 5 * row_count as usize)
        }
        1 => {
            let (_, sz1) = vlen_decode_u64(src)?;
            let (data_recs, sz2) = vlen_decode_u64(&src[sz1..])?;
            let hdr_bytes = 1 + sz1 + sz2;
            (hdr_bytes, 5 * data_recs as usize)
        }
        2 => {
            let (leng_recs, sz) = vlen_decode_u64(src)?;
            let mut bs = 10 * leng_recs as usize;
            if random_access {
                bs += 5 * row_count as usize;
            }
            (1 + sz, bs)
        }
        3 => {
            let (leng_recs, sz1) = vlen_decode_u64(src)?;
            let (data_recs, sz2) = vlen_decode_u64(&src[sz1..])?;
            let bs = 10 * leng_recs as usize + 5 * data_recs as usize;
            (1 + sz1 + sz2, bs)
        }
        _ => {
            return Err(Error::Format(format!(
                "page_map_v1: unsupported variant {variant}"
            )));
        }
    };

    // Decompress the body (zlib after the header portion).
    let compressed = &data[hsize..];
    if compressed.is_empty() {
        return Err(Error::Format("page_map_v1: no compressed data".into()));
    }

    // Build decompressed buffer: copy header + decompress body.
    let mut decompressed = Vec::with_capacity(hsize + bsize);
    decompressed.extend_from_slice(&data[..hsize]);

    if endp > hsize {
        // VDB uses raw deflate (inflateInit2 with -15), not zlib format.
        let body = deflate_decompress(compressed, bsize)?;
        decompressed.extend_from_slice(&body);
    }

    // Deserialize as v0 with the full decompressed data.
    page_map_deserialize_v0(&decompressed, row_count)
}

// ---------------------------------------------------------------------------
// Blob header deserialization
// ---------------------------------------------------------------------------

/// A single frame in the blob header stack.
#[derive(Debug, Clone, Default)]
pub struct BlobHeaderFrame {
    /// Flags byte.
    pub flags: u8,
    /// Version byte.
    pub version: u8,
    /// Format ID.
    pub fmt: u32,
    /// Original (source) size.
    pub osize: u64,
    /// Opcode bytes.
    pub ops: Vec<u8>,
    /// Arguments (signed vlen-encoded integers).
    pub args: Vec<i64>,
}

/// Deserialize a blob header stack from its serialized form.
///
/// The first byte must be 0 (the only supported serialization version).
/// Returns the stack of header frames (outermost first).
pub fn blob_headers_deserialize(data: &[u8]) -> Result<Vec<BlobHeaderFrame>> {
    if data.is_empty() {
        return Err(Error::Format("blob_headers: empty input".into()));
    }
    if data[0] != 0 {
        return Err(Error::Format(format!(
            "blob_headers: unsupported serialization version {}",
            data[0]
        )));
    }
    deserialize_header_frames(&data[1..])
}

fn deserialize_header_frames(data: &[u8]) -> Result<Vec<BlobHeaderFrame>> {
    let mut frames = Vec::new();
    let mut pos = 0;

    while pos < data.len() {
        if data.len() - pos < 2 {
            return Err(Error::Format(
                "blob_headers: insufficient data for frame".into(),
            ));
        }

        let flags = data[pos];
        pos += 1;
        let version = data[pos];
        pos += 1;

        let (fmt_raw, sz) = vlen_decode_i64(&data[pos..])?;
        pos += sz;
        let fmt = fmt_raw as u32;

        let (osize_raw, sz) = vlen_decode_i64(&data[pos..])?;
        pos += sz;
        let osize = osize_raw as u64;

        let (op_count_raw, sz) = vlen_decode_i64(&data[pos..])?;
        pos += sz;
        let op_count = op_count_raw as usize;

        let (arg_count_raw, sz) = vlen_decode_i64(&data[pos..])?;
        pos += sz;
        let arg_count = arg_count_raw as usize;

        let mut ops = Vec::new();
        if op_count > 0 {
            if data.len() - pos < op_count {
                return Err(Error::Format("blob_headers: insufficient ops data".into()));
            }
            ops.extend_from_slice(&data[pos..pos + op_count]);
            pos += op_count;
        }

        let mut args = Vec::new();
        for _ in 0..arg_count {
            let (val, sz) = vlen_decode_i64(&data[pos..])?;
            args.push(val);
            pos += sz;
        }

        frames.push(BlobHeaderFrame {
            flags,
            version,
            fmt,
            osize,
            ops,
            args,
        });
    }

    Ok(frames)
}

// ---------------------------------------------------------------------------
// VDB blob v2 header decoding
// ---------------------------------------------------------------------------

/// Parsed blob envelope header (v2 format).
#[derive(Debug, Clone)]
pub struct BlobEnvelope {
    /// Number of trailing bits to discard from the last data byte.
    pub adjust: u8,
    /// Byte order: `false` = little-endian, `true` = big-endian.
    pub big_endian: bool,
    /// Size of the blob header section (transform headers).
    pub hdr_size: u32,
    /// Size of the page map section.
    pub map_size: u32,
    /// Total size of the envelope header (before headers + page map + data).
    pub envelope_size: u32,
}

/// Decode the v1 blob envelope (bit 7 of first byte is clear).
///
/// Returns `(byte_order_big_endian, adjust, row_length, offset_to_data)`.
fn decode_blob_v1(data: &[u8]) -> Result<(bool, u8, u64, usize)> {
    if data.is_empty() {
        return Err(Error::Format("blob v1: empty".into()));
    }

    let header = data[0];
    let byte_order = (header & 0x03) == 2; // 2 = big-endian
    let adjust = (header >> 2) & 7;
    let rls_code = (header >> 5) & 3;

    // Convert row-length-size code to actual byte count.
    let rls: usize = match rls_code {
        0 => 1,
        1 => 2,
        2 => 4,
        3 => 0, // implicit row_length = 1
        _ => unreachable!(),
    };

    let offset = rls + 1;
    let row_len: u64 = if rls == 0 {
        1
    } else {
        if data.len() < offset {
            return Err(Error::Format("blob v1: header too short".into()));
        }
        let mut val: u64 = 0;
        for i in 0..rls {
            val |= u64::from(data[1 + i]) << (8 * i);
        }
        val
    };

    Ok((byte_order, adjust, row_len, offset))
}

/// Decode the v2 blob envelope.
fn decode_blob_v2(data: &[u8]) -> Result<BlobEnvelope> {
    if data.is_empty() {
        return Err(Error::Format("blob v2: empty".into()));
    }

    let hdr_byte = data[0];
    let adjust = (8u8.wrapping_sub(hdr_byte & 7)) & 7;
    let big_endian = ((hdr_byte >> 3) & 1) != 0;
    let variant = (hdr_byte >> 4) & 3;
    let version = hdr_byte >> 6;

    if version != 2 {
        return Err(Error::Format(format!(
            "blob v2: bad version {version}, expected 2"
        )));
    }

    let (hdr_size, map_size, envelope_size) = match variant {
        0 => {
            if data.len() < 3 {
                return Err(Error::Format("blob v2.0: too short".into()));
            }
            (u32::from(data[1]), u32::from(data[2]), 3u32)
        }
        1 => {
            if data.len() < 4 {
                return Err(Error::Format("blob v2.1: too short".into()));
            }
            let ms = u32::from(data[2]) | (u32::from(data[3]) << 8);
            (u32::from(data[1]), ms, 4)
        }
        2 => {
            if data.len() < 6 {
                return Err(Error::Format("blob v2.2: too short".into()));
            }
            let ms = u32::from_le_bytes(data[2..6].try_into().unwrap());
            (u32::from(data[1]), ms, 6)
        }
        3 => {
            if data.len() < 9 {
                return Err(Error::Format("blob v2.3: too short".into()));
            }
            let hs = u32::from_le_bytes(data[1..5].try_into().unwrap());
            let ms = u32::from_le_bytes(data[5..9].try_into().unwrap());
            (hs, ms, 9)
        }
        _ => {
            return Err(Error::Format(format!(
                "blob v2: unsupported variant {variant}"
            )));
        }
    };

    Ok(BlobEnvelope {
        adjust,
        big_endian,
        hdr_size,
        map_size,
        envelope_size,
    })
}

// ---------------------------------------------------------------------------
// Blob decoding (main entry point)
// ---------------------------------------------------------------------------

/// Result of decoding a VDB blob.
#[derive(Debug, Clone)]
pub struct DecodedBlob<'a> {
    /// The raw column data (after stripping envelope, headers, page map).
    /// Borrows directly from the mmap'd blob slice when possible, avoiding
    /// a copy. Falls back to owned data only for the empty-blob case.
    pub data: Cow<'a, [u8]>,
    /// Number of trailing adjustment bits in the last data byte.
    pub adjust: u8,
    /// Whether the data is big-endian.
    pub big_endian: bool,
    /// Blob header frames (transform metadata).
    pub headers: Vec<BlobHeaderFrame>,
    /// Page map (row boundary info), if present.
    pub page_map: Option<PageMap>,
    /// Number of elements = (data_bits - adjust) / elem_bits.
    pub row_length: Option<u64>,
}

/// Decode a VDB column blob from raw bytes.
///
/// `raw` is the blob data as read from the data file (at the offset and size
/// indicated by the blob locator). `checksum_type`: 0 = none, 1 = CRC32,
/// 2 = MD5. `row_count` is the number of rows in this blob (from id_range).
/// `elem_bits` is the element bit-width of the physical column.
///
/// Returns the decoded blob structure with separated envelope, headers,
/// page map, and raw column data.
pub fn decode_blob<'a>(
    raw: &'a [u8],
    checksum_type: u8,
    row_count: u64,
    _elem_bits: u32,
) -> Result<DecodedBlob<'a>> {
    if raw.is_empty() {
        return Ok(DecodedBlob {
            data: Cow::Borrowed(b""),
            adjust: 0,
            big_endian: false,
            headers: vec![],
            page_map: None,
            row_length: None,
        });
    }

    // Strip checksum from the end.
    let cs_size: usize = match checksum_type {
        0 => 0,
        1 => 4,  // CRC32
        2 => 16, // MD5
        _ => {
            return Err(Error::Format(format!(
                "unknown checksum type {checksum_type}"
            )));
        }
    };

    if raw.len() < cs_size {
        return Err(Error::Format("blob too short for checksum".into()));
    }

    let blob_data = &raw[..raw.len() - cs_size];

    // Validate checksum if present.
    if checksum_type == 1 && cs_size == 4 {
        let stored_crc = u32::from_le_bytes([
            raw[raw.len() - 4],
            raw[raw.len() - 3],
            raw[raw.len() - 2],
            raw[raw.len() - 1],
        ]);
        let computed_crc = ncbi_crc32(blob_data);
        if stored_crc != computed_crc {
            return Err(Error::BlobIntegrity {
                kind: "CRC32",
                stored: format!("{stored_crc:#010x}"),
                computed: format!("{computed_crc:#010x}"),
            });
        }
    } else if checksum_type == 2 && cs_size == 16 {
        let stored: [u8; 16] = raw[raw.len() - 16..]
            .try_into()
            .expect("slice length is 16");
        let computed = Md5::digest(blob_data);
        if stored != computed.as_slice() {
            return Err(Error::BlobIntegrity {
                kind: "MD5",
                stored: hex16(&stored),
                computed: hex16(computed.as_slice()),
            });
        }
    }

    // Determine v1 vs v2 format.
    if blob_data[0] & 0x80 == 0 {
        // v1 format
        let (big_endian, adjust, row_length, offset) = decode_blob_v1(blob_data)?;

        Ok(DecodedBlob {
            data: Cow::Borrowed(&blob_data[offset..]),
            adjust,
            big_endian,
            headers: vec![],
            page_map: None,
            row_length: Some(row_length),
        })
    } else {
        // v2 format
        let envelope = decode_blob_v2(blob_data)?;

        let es = envelope.envelope_size as usize;
        let hs = envelope.hdr_size as usize;
        let ms = envelope.map_size as usize;

        if blob_data.len() < es + hs + ms {
            return Err(Error::Format(
                "blob v2: data too short for headers + page map".into(),
            ));
        }

        // Parse blob headers.
        let headers = if hs > 0 {
            blob_headers_deserialize(&blob_data[es..es + hs])?
        } else {
            vec![]
        };

        // Parse page map.
        let page_map = if ms > 0 {
            Some(page_map_deserialize(
                &blob_data[es + hs..es + hs + ms],
                row_count,
            )?)
        } else {
            None
        };

        let data_start = es + hs + ms;

        Ok(DecodedBlob {
            data: Cow::Borrowed(&blob_data[data_start..]),
            adjust: envelope.adjust,
            big_endian: envelope.big_endian,
            headers,
            page_map,
            row_length: None,
        })
    }
}

// ---------------------------------------------------------------------------
// Bit unpacking
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// izip (integer compression) decoder
// ---------------------------------------------------------------------------

/// Flags for how sub-arrays are stored in izip.
const DATA_CONSTANT: u32 = 1;
const DATA_ZIPPED: u32 = 2;
const DATA_ABSENT: u32 = 3;

/// 4 bits per field in data_flags.
const FLAG_BITS: u32 = 4;
const FLAG_MASK: u32 = (1 << FLAG_BITS) - 1;

fn flag_extract(data_flags: u32, shift: u32) -> u32 {
    (data_flags >> shift) & FLAG_MASK
}

/// Deserialized izip encoded header.
struct IzipEncoded<'a> {
    flags: u8,
    data_count: u32,
    /// For flags & 3 in {1, 2, 3}: simple zipped or packed data.
    simple_min: i64,
    simple_data: &'a [u8],
    /// For flags & 3 == 0: full izip fields.
    izipped: Option<IzipFields<'a>>,
}

#[allow(dead_code)]
struct IzipFields<'a> {
    data_flags: u32,
    segments: u32,
    outliers: u32,

    type_size: u32,
    diff_size: u32,
    length_size: u32,
    dy_size: u32,
    dx_size: u32,
    a_size: u32,
    outlier_size: u32,

    min_diff: i64,
    min_length: i64,
    min_dy: i64,
    min_dx: i64,
    min_a: i64,
    min_outlier: i64,

    type_data: &'a [u8],
    diff_data: &'a [u8],
    length_data: &'a [u8],
    dy_data: &'a [u8],
    dx_data: &'a [u8],
    a_data: &'a [u8],
    outlier_data: &'a [u8],
}

fn read_u32_le(data: &[u8], offset: usize) -> Result<u32> {
    if data.len() < offset + 4 {
        return Err(Error::Format("izip: read_u32_le out of bounds".into()));
    }
    Ok(u32::from_le_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
    ]))
}

fn read_i64_le(data: &[u8], offset: usize) -> Result<i64> {
    if data.len() < offset + 8 {
        return Err(Error::Format("izip: read_i64_le out of bounds".into()));
    }
    Ok(i64::from_le_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
        data[offset + 4],
        data[offset + 5],
        data[offset + 6],
        data[offset + 7],
    ]))
}

fn deserialize_izip_encoded(src: &[u8]) -> Result<IzipEncoded<'_>> {
    if src.len() < 5 {
        return Err(Error::Format("izip: data too short".into()));
    }

    let flags = src[0];
    let data_count = read_u32_le(src, 1)?;
    let mut i: usize = 5;
    let enc_type = flags & 0x03;

    match enc_type {
        // Type 2 or 3: packed (optionally zipped)
        2 | 3 => {
            if src.len() < i + 8 {
                return Err(Error::Format("izip: packed data too short for min".into()));
            }
            let min = read_i64_le(src, i)?;
            i += 8;
            Ok(IzipEncoded {
                flags,
                data_count,
                simple_min: min,
                simple_data: &src[i..],
                izipped: None,
            })
        }
        // Type 1: zipped only
        1 => Ok(IzipEncoded {
            flags,
            data_count,
            simple_min: 0,
            simple_data: &src[i..],
            izipped: None,
        }),
        // Type 0: full izip
        0 => {
            let data_flags = read_u32_le(src, i)?;
            i += 4;
            let segments = read_u32_le(src, i)?;
            i += 4;
            let outliers_count = read_u32_le(src, i)?;
            i += 4;

            let type_size = read_u32_le(src, i)?;
            i += 4;
            let diff_size = read_u32_le(src, i)?;
            i += 4;
            let length_size = read_u32_le(src, i)?;
            i += 4;
            let dy_size = read_u32_le(src, i)?;
            i += 4;
            let dx_size = read_u32_le(src, i)?;
            i += 4;
            let a_size = read_u32_le(src, i)?;
            i += 4;
            let outlier_size = read_u32_le(src, i)?;
            i += 4;

            let min_diff = read_i64_le(src, i)?;
            i += 8;
            let min_length = read_i64_le(src, i)?;
            i += 8;
            let min_dy = read_i64_le(src, i)?;
            i += 8;
            let min_dx = read_i64_le(src, i)?;
            i += 8;
            let min_a = read_i64_le(src, i)?;
            i += 8;
            let min_outlier = read_i64_le(src, i)?;
            i += 8;

            // Read sub-arrays.
            let flag_type = flag_extract(data_flags, 0);
            let type_data = if flag_type != DATA_ABSENT && flag_type != DATA_CONSTANT {
                if src.len() < i + type_size as usize {
                    return Err(Error::Format("izip: type_data too short".into()));
                }
                let d = &src[i..i + type_size as usize];
                i += type_size as usize;
                d
            } else {
                &[]
            };

            let flag_diff = flag_extract(data_flags, FLAG_BITS);
            let diff_data = if flag_diff != DATA_ABSENT && flag_diff != DATA_CONSTANT {
                if src.len() < i + diff_size as usize {
                    return Err(Error::Format("izip: diff_data too short".into()));
                }
                let d = &src[i..i + diff_size as usize];
                i += diff_size as usize;
                d
            } else {
                &[]
            };

            let flag_length = flag_extract(data_flags, 2 * FLAG_BITS);
            let length_data = if flag_length != DATA_ABSENT && flag_length != DATA_CONSTANT {
                if src.len() < i + length_size as usize {
                    return Err(Error::Format("izip: length_data too short".into()));
                }
                let d = &src[i..i + length_size as usize];
                i += length_size as usize;
                d
            } else {
                &[]
            };

            let flag_dy = flag_extract(data_flags, 3 * FLAG_BITS);
            let dy_data = if flag_dy != DATA_ABSENT && flag_dy != DATA_CONSTANT {
                if src.len() < i + dy_size as usize {
                    return Err(Error::Format("izip: dy_data too short".into()));
                }
                let d = &src[i..i + dy_size as usize];
                i += dy_size as usize;
                d
            } else {
                &[]
            };

            let flag_dx = flag_extract(data_flags, 4 * FLAG_BITS);
            let dx_data = if flag_dx != DATA_ABSENT && flag_dx != DATA_CONSTANT {
                if src.len() < i + dx_size as usize {
                    return Err(Error::Format("izip: dx_data too short".into()));
                }
                let d = &src[i..i + dx_size as usize];
                i += dx_size as usize;
                d
            } else {
                &[]
            };

            let flag_a = flag_extract(data_flags, 5 * FLAG_BITS);
            let a_data = if flag_a != DATA_ABSENT && flag_a != DATA_CONSTANT {
                if src.len() < i + a_size as usize {
                    return Err(Error::Format("izip: a_data too short".into()));
                }
                let d = &src[i..i + a_size as usize];
                i += a_size as usize;
                d
            } else {
                &[]
            };

            let flag_outlier = flag_extract(data_flags, 6 * FLAG_BITS);
            let outlier_data = if flag_outlier != DATA_ABSENT && flag_outlier != DATA_CONSTANT {
                if src.len() < i + outlier_size as usize {
                    return Err(Error::Format("izip: outlier_data too short".into()));
                }

                // i += outlier_size as usize; (last field)
                &src[i..i + outlier_size as usize]
            } else {
                &[]
            };

            Ok(IzipEncoded {
                flags,
                data_count,
                simple_min: 0,
                simple_data: &[],
                izipped: Some(IzipFields {
                    data_flags,
                    segments,
                    outliers: outliers_count,
                    type_size,
                    diff_size,
                    length_size,
                    dy_size,
                    dx_size,
                    a_size,
                    outlier_size,
                    min_diff,
                    min_length,
                    min_dy,
                    min_dx,
                    min_a,
                    min_outlier,
                    type_data,
                    diff_data,
                    length_data,
                    dy_data,
                    dx_data,
                    a_data,
                    outlier_data,
                }),
            })
        }
        _ => Err(Error::Format(format!(
            "izip: unknown encoding type {enc_type}"
        ))),
    }
}

/// Helper to decompress or copy a sub-array buffer.
///
/// `max_out` is the pre-allocated output size; bound it against the input so
/// a misinterpreted/oversized iZip header cannot drive an arbitrary deflate
/// destination buffer.
fn izip_decompress_buf(data: &[u8], flag: u32, max_out: usize) -> Result<Vec<u8>> {
    check_alloc_bytes(max_out, data.len(), "izip_decompress_buf")?;
    if flag == DATA_ZIPPED {
        zlib_raw_decompress(data, max_out)
    } else {
        Ok(data.to_vec())
    }
}

/// Decompress raw deflate data (no header, windowBits = -15) using libdeflate.
/// `max_out` is the expected decompressed size.
fn zlib_raw_decompress(data: &[u8], max_out: usize) -> Result<Vec<u8>> {
    deflate_decompress(data, max_out)
}

/// Fast raw-deflate decompression via libdeflate.
///
/// `expected_size` is the expected output size. If the actual decompressed
/// data is larger, falls back to flate2 streaming decoder.
pub fn deflate_decompress(data: &[u8], expected_size: usize) -> Result<Vec<u8>> {
    use libdeflater::Decompressor;

    if data.is_empty() {
        return Ok(Vec::new());
    }

    let mut decompressor = Decompressor::new();
    let mut out = vec![0u8; expected_size];
    match decompressor.deflate_decompress(data, &mut out) {
        Ok(actual) => {
            out.truncate(actual);
            Ok(out)
        }
        Err(_) => {
            // Fallback: size estimate was wrong, use streaming flate2.
            deflate_decompress_fallback(data)
        }
    }
}

/// Fast zlib (with header) decompression via libdeflate.
pub fn zlib_decompress(data: &[u8], expected_size: usize) -> Result<Vec<u8>> {
    use libdeflater::Decompressor;

    if data.is_empty() {
        return Ok(Vec::new());
    }

    let mut decompressor = Decompressor::new();
    let mut out = vec![0u8; expected_size];
    match decompressor.zlib_decompress(data, &mut out) {
        Ok(actual) => {
            out.truncate(actual);
            Ok(out)
        }
        Err(_) => {
            // Fallback: try streaming.
            zlib_decompress_fallback(data)
        }
    }
}

/// Raw-deflate decompression via libdeflate, also returning the number of
/// compressed input bytes consumed. Needed for irzip where multiple
/// compressed streams are concatenated.
pub(crate) fn deflate_decompress_ex(data: &[u8], expected_size: usize) -> Result<(Vec<u8>, usize)> {
    if data.is_empty() {
        return Ok((Vec::new(), 0));
    }

    let decompressor = unsafe { libdeflate_sys::libdeflate_alloc_decompressor() };
    if decompressor.is_null() {
        return Err(Error::Format(
            "failed to allocate libdeflate decompressor".into(),
        ));
    }

    let mut out = vec![0u8; expected_size];
    let mut actual_in: usize = 0;
    let mut actual_out: usize = 0;

    let ret = unsafe {
        libdeflate_sys::libdeflate_deflate_decompress_ex(
            decompressor,
            data.as_ptr() as *const std::ffi::c_void,
            data.len(),
            out.as_mut_ptr() as *mut std::ffi::c_void,
            out.len(),
            &mut actual_in,
            &mut actual_out,
        )
    };

    unsafe { libdeflate_sys::libdeflate_free_decompressor(decompressor) };

    if ret == 0 {
        // LIBDEFLATE_SUCCESS
        out.truncate(actual_out);
        Ok((out, actual_in))
    } else {
        // Fallback to flate2 streaming.
        use flate2::read::DeflateDecoder;
        use std::io::Read as _;
        let mut decoder = DeflateDecoder::new(data);
        let mut fallback_out = vec![0u8; expected_size];
        let mut total = 0;
        loop {
            let n = decoder
                .read(&mut fallback_out[total..])
                .map_err(|e| Error::Format(format!("deflate_ex fallback failed: {e}")))?;
            if n == 0 {
                break;
            }
            total += n;
        }
        fallback_out.truncate(total);
        let consumed = decoder.total_in() as usize;
        Ok((fallback_out, consumed))
    }
}

/// Streaming fallback for raw deflate when size is unknown.
fn deflate_decompress_fallback(data: &[u8]) -> Result<Vec<u8>> {
    use flate2::read::DeflateDecoder;
    use std::io::Read as _;

    let mut decoder = DeflateDecoder::new(data);
    let mut out = Vec::new();
    decoder
        .read_to_end(&mut out)
        .map_err(|e| Error::Format(format!("deflate decompression failed: {e}")))?;
    Ok(out)
}

/// Streaming fallback for zlib when size is unknown.
fn zlib_decompress_fallback(data: &[u8]) -> Result<Vec<u8>> {
    use flate2::read::ZlibDecoder;
    use std::io::Read as _;

    let mut decoder = ZlibDecoder::new(data);
    let mut out = Vec::new();
    decoder
        .read_to_end(&mut out)
        .map_err(|e| Error::Format(format!("zlib decompression failed: {e}")))?;
    Ok(out)
}

/// Determine the nbuf variant from elem_bits.
fn variant_from_elem_bits(elem_bits: u32) -> Result<u32> {
    match elem_bits {
        8 => Ok(4),
        16 => Ok(3),
        32 => Ok(2),
        64 => Ok(1),
        _ => Err(Error::Format(format!(
            "izip: invalid elem_bits {elem_bits}"
        ))),
    }
}

/// Read a raw value from a buffer using the nbuf variant encoding.
///
/// Returns `Error::Format` on out-of-bounds access — a truncated / corrupted
/// izip buffer (e.g. from a stale `.sracha-tmp` file matched only by size)
/// surfaces as a clean error rather than a thread panic.
///
/// `name` is included in the error message so the caller knows which
/// nbuf buffer (length, outlier, dx, dy, a, diff, simple) was truncated.
fn nbuf_read(data: &[u8], idx: usize, variant: u32, name: &str) -> Result<i64> {
    let (off, width) = match variant {
        4 => (idx, 1),
        3 => (idx * 2, 2),
        2 => (idx * 4, 4),
        _ => (idx * 8, 8),
    };
    let end = off.checked_add(width).ok_or_else(|| {
        Error::Format(format!(
            "izip: {name} nbuf offset overflow (idx={idx}, variant={variant})"
        ))
    })?;
    if end > data.len() {
        return Err(Error::Format(format!(
            "izip: {name} nbuf read out of bounds (idx={idx}, variant={variant}, need {end} bytes, have {})",
            data.len()
        )));
    }
    Ok(match variant {
        4 => i64::from(data[off]),
        3 => i64::from(u16::from_le_bytes([data[off], data[off + 1]])),
        2 => i64::from(u32::from_le_bytes([
            data[off],
            data[off + 1],
            data[off + 2],
            data[off + 3],
        ])),
        _ => i64::from_le_bytes([
            data[off],
            data[off + 1],
            data[off + 2],
            data[off + 3],
            data[off + 4],
            data[off + 5],
            data[off + 6],
            data[off + 7],
        ]),
    })
}

/// Bundle of (data, variant, min, name) for a single izip nbuf buffer.
///
/// The izip type-0 (line-segment) reconstruction reads from six distinct
/// nbuf buffers (length, outlier, dx, dy, a, diff), each with its own
/// encoding and min-offset. Carrying those four values as positional
/// arguments through every `nbuf_read_min` call was noisy and made error
/// messages ambiguous. `NbufStream` bundles them so each read becomes
/// `stream.read(idx)?`, and the `name` field flows into error messages.
struct NbufStream<'a> {
    data: &'a [u8],
    variant: u32,
    min: i64,
    name: &'static str,
}

impl<'a> NbufStream<'a> {
    fn new(data: &'a [u8], variant: u32, min: i64, name: &'static str) -> Self {
        Self {
            data,
            variant,
            min,
            name,
        }
    }

    #[inline(always)]
    fn read(&self, idx: usize) -> Result<i64> {
        Ok(nbuf_read(self.data, idx, self.variant, self.name)?.wrapping_add(self.min))
    }
}

/// Decode types bitmap: each bit in `src` maps to one segment type (0=line, 1=outlier).
///
/// Returns `Error::Format` when `src` is shorter than `ceil(n / 8)` bytes.
fn decode_types(n: usize, src: &[u8]) -> Result<Vec<u8>> {
    let needed = n.div_ceil(8);
    if src.len() < needed {
        return Err(Error::Format(format!(
            "izip: type bitmap truncated (need {needed} bytes for {n} segments, have {})",
            src.len()
        )));
    }
    let mut dst = vec![0u8; n];
    let mut j: u32 = 1;
    let mut k: u8 = 0;
    for i in 0..n {
        if j == 1 {
            k = src[i / 8];
        }
        dst[i] = if (k & j as u8) == 0 { 0 } else { 1 };
        j <<= 1;
        if j == 0x100 {
            j = 1;
        }
    }
    Ok(dst)
}

/// Maximum allowed output-to-input byte ratio for a single decode step.
///
/// iZip and deflate compress at most a few hundred to one for highly
/// repetitive payloads; 1024 is a defense-in-depth cap that rejects
/// header-driven allocation requests derived from misinterpreted bytes
/// (e.g. issue #30) without ever rejecting a real blob.
pub(crate) const MAX_DECODE_RATIO: usize = 1024;

/// Reject a header-driven allocation that exceeds `src.len() * MAX_DECODE_RATIO`.
///
/// `src_len` is clamped up to a 4 KiB floor so legitimate "no-payload,
/// small element count" cases still pass — cSRA paths can invoke
/// `irzip_decode` with `data.len() == 0` when the encoder elided the
/// deflate stream and the plane loop zero-fills the output.
pub(crate) fn check_alloc_bytes(n_bytes: usize, src_len: usize, ctx: &str) -> Result<()> {
    const MIN_INPUT_FLOOR: usize = 4096;
    let limit = src_len
        .max(MIN_INPUT_FLOOR)
        .saturating_mul(MAX_DECODE_RATIO);
    if n_bytes > limit {
        return Err(Error::Format(format!(
            "{ctx}: requested {n_bytes}-byte allocation exceeds {limit}-byte cap \
             for {src_len}-byte input"
        )));
    }
    Ok(())
}

/// Allocate a zero-filled byte buffer with `check_alloc_bytes` already applied.
fn bounded_zeros(n: usize, src_len: usize, ctx: &str) -> Result<Vec<u8>> {
    check_alloc_bytes(n, src_len, ctx)?;
    Ok(vec![0u8; n])
}

/// Decode izip-compressed integers.
///
/// `data` is the raw izip-encoded byte stream (as found in the blob's column
/// data after envelope/header stripping). `elem_bits` is the output element
/// size in bits (8, 16, 32, or 64). The output element count comes from the
/// iZip header's `data_count` field; allocations are bounded against
/// `data.len() * MAX_DECODE_RATIO` so that misinterpreted bytes (e.g. a
/// deflate-compressed quality blob fed to the iZip probe) cannot drive an
/// unbounded heap allocation.
///
/// Returns the decoded integers as a byte vector in native (little-endian)
/// format, with `data_count * (elem_bits / 8)` bytes.
pub fn izip_decode(data: &[u8], elem_bits: u32) -> Result<Vec<u8>> {
    let encoded = deserialize_izip_encoded(data)?;
    let n = encoded.data_count as usize;

    let enc_type = encoded.flags & 0x03;
    let _size_type = ((encoded.flags >> 2) & 3) as u32;

    let out_bytes = (elem_bits / 8) as usize;
    let total = n.checked_mul(out_bytes).ok_or_else(|| {
        Error::Format(format!(
            "izip_decode: n={n} * out_bytes={out_bytes} overflows usize"
        ))
    })?;
    check_alloc_bytes(total, data.len(), "izip_decode output")?;
    let mut output = vec![0u8; total];

    match enc_type {
        // Type 1: zlib-compressed, no min offset.
        // Type 3: zlib-compressed with min offset.
        1 | 3 => {
            let decompressed = zlib_raw_decompress(encoded.simple_data, n * 8)?;
            let elem_size_bits = (decompressed.len() * 8) / n;
            let var = variant_from_elem_bits(elem_size_bits as u32)?;

            let min = if enc_type == 3 { encoded.simple_min } else { 0 };

            for i in 0..n {
                let raw = nbuf_read(&decompressed, i, var, "simple")?;
                let val = (raw as i64).wrapping_add(min);
                write_element(&mut output, i, val, elem_bits);
            }
        }
        // Type 2: packed (no zlib), with min offset.
        2 => {
            let elem_size_bits = (encoded.simple_data.len() * 8) / n;
            let var = variant_from_elem_bits(elem_size_bits as u32)?;

            for i in 0..n {
                let raw = nbuf_read(encoded.simple_data, i, var, "simple")?;
                let val = (raw as i64).wrapping_add(encoded.simple_min);
                write_element(&mut output, i, val, elem_bits);
            }
        }
        // Type 0: full izip with line segments.
        0 => {
            let iz = encoded
                .izipped
                .as_ref()
                .ok_or_else(|| Error::Format("izip type 0: missing izip fields".into()))?;

            // Decode diff buffer.
            //
            // The DATA_CONSTANT branches below allocate from header-supplied
            // size fields directly — without `bounded_zeros`, a misinterpreted
            // header could request gigabytes here. The non-constant branches
            // route through `izip_decompress_buf`, which carries the same
            // bound internally.
            let flag_diff = flag_extract(iz.data_flags, FLAG_BITS);
            let diff_raw = if flag_diff == DATA_CONSTANT {
                bounded_zeros(iz.diff_size as usize, data.len(), "izip type 0 diff_const")?
            } else {
                izip_decompress_buf(iz.diff_data, flag_diff, n * 8)?
            };

            let diff_elem_bits = if diff_raw.is_empty() {
                8
            } else {
                (diff_raw.len() * 8 / n) as u32
            };
            let diff_var = variant_from_elem_bits(diff_elem_bits)?;

            // Determine lines and outlier counts.
            let segments = iz.segments as usize;
            check_alloc_bytes(segments, data.len(), "izip type 0 segments")?;
            let segment_types = if iz.outliers > 0 {
                let flag_type = flag_extract(iz.data_flags, 0);
                let type_raw = if flag_type == DATA_ZIPPED {
                    zlib_raw_decompress(iz.type_data, segments)?
                } else {
                    iz.type_data.to_vec()
                };
                decode_types(segments, &type_raw)?
            } else {
                vec![0u8; segments]
            };

            let lines = segment_types.iter().filter(|&&t| t == 0).count();
            let outlier_count = segment_types.iter().filter(|&&t| t != 0).count();

            // Decode raw byte buffers for each component. The packed values
            // are read inline during reconstruction via NbufStream::read(),
            // avoiding intermediate Vec<i64> allocations.
            let flag_length = flag_extract(iz.data_flags, 2 * FLAG_BITS);
            let total_segs = lines + outlier_count;
            let length_raw = if flag_length == DATA_CONSTANT {
                let n_bytes = (iz.length_size as usize)
                    .checked_mul(total_segs)
                    .ok_or_else(|| Error::Format(
                        format!("izip type 0 length_const: length_size={} * total_segs={total_segs} overflows usize",
                                iz.length_size)))?;
                bounded_zeros(n_bytes, data.len(), "izip type 0 length_const")?
            } else {
                izip_decompress_buf(iz.length_data, flag_length, total_segs * 4)?
            };
            let length_elem_bits = if length_raw.is_empty() || total_segs == 0 {
                8
            } else {
                (length_raw.len() * 8 / total_segs) as u32
            };
            let length_var = variant_from_elem_bits(length_elem_bits)?;

            let flag_dy = flag_extract(iz.data_flags, 3 * FLAG_BITS);
            let dy_raw = if flag_dy == DATA_CONSTANT {
                bounded_zeros(lines.saturating_mul(8), data.len(), "izip type 0 dy_const")?
            } else {
                izip_decompress_buf(iz.dy_data, flag_dy, lines * 8)?
            };
            let dy_elem_bits = if dy_raw.is_empty() || lines == 0 {
                8
            } else {
                (dy_raw.len() * 8 / lines) as u32
            };
            let dy_var = variant_from_elem_bits(dy_elem_bits)?;

            let flag_dx = flag_extract(iz.data_flags, 4 * FLAG_BITS);
            let dx_raw = if flag_dx == DATA_CONSTANT {
                bounded_zeros(lines.saturating_mul(8), data.len(), "izip type 0 dx_const")?
            } else {
                izip_decompress_buf(iz.dx_data, flag_dx, lines * 8)?
            };
            let dx_elem_bits = if dx_raw.is_empty() || lines == 0 {
                8
            } else {
                (dx_raw.len() * 8 / lines) as u32
            };
            let dx_var = variant_from_elem_bits(dx_elem_bits)?;

            let flag_a = flag_extract(iz.data_flags, 5 * FLAG_BITS);
            let a_raw = if flag_a == DATA_CONSTANT {
                bounded_zeros(lines.saturating_mul(8), data.len(), "izip type 0 a_const")?
            } else {
                izip_decompress_buf(iz.a_data, flag_a, lines * 8)?
            };
            let a_elem_bits = if a_raw.is_empty() || lines == 0 {
                8
            } else {
                (a_raw.len() * 8 / lines) as u32
            };
            let a_var = variant_from_elem_bits(a_elem_bits)?;

            let (outlier_raw, outlier_var) = if outlier_count > 0 {
                let flag_outlier = flag_extract(iz.data_flags, 6 * FLAG_BITS);
                let raw = if flag_outlier == DATA_CONSTANT {
                    bounded_zeros(
                        outlier_count.saturating_mul(8),
                        data.len(),
                        "izip type 0 outlier_const",
                    )?
                } else {
                    izip_decompress_buf(iz.outlier_data, flag_outlier, outlier_count * 8)?
                };
                let bits = if raw.is_empty() {
                    8
                } else {
                    (raw.len() * 8 / outlier_count) as u32
                };
                (raw, variant_from_elem_bits(bits)?)
            } else {
                (vec![], 4)
            };

            // Bundle each nbuf buffer with its variant, min offset, and a
            // stable name so call sites read naturally and out-of-bounds
            // errors identify which buffer was truncated.
            let length = NbufStream::new(&length_raw, length_var, iz.min_length, "length");
            let outlier = NbufStream::new(&outlier_raw, outlier_var, iz.min_outlier, "outlier");
            let dx = NbufStream::new(&dx_raw, dx_var, iz.min_dx, "dx");
            let dy = NbufStream::new(&dy_raw, dy_var, iz.min_dy, "dy");
            let a = NbufStream::new(&a_raw, a_var, iz.min_a, "a");
            let diff = NbufStream::new(&diff_raw, diff_var, iz.min_diff, "diff");

            // Reconstruct output, reading packed values inline to avoid
            // materializing intermediate Vec<i64> buffers.
            let mut k = 0usize; // output element index
            let mut u = 0usize; // line segment index
            let mut v = 0usize; // outlier value index

            for (seg_idx, &seg_type) in segment_types.iter().enumerate() {
                let seg_len = length.read(seg_idx)? as usize;

                if seg_type != 0 {
                    // Outlier segment: copy values directly.
                    for j in 0..seg_len {
                        if k + j >= n {
                            break;
                        }
                        let val = outlier.read(v + j)?;
                        write_element(&mut output, k + j, val, elem_bits);
                    }
                    k += seg_len;
                    v += seg_len;
                } else {
                    // Line segment: reconstruct using diff + linear model.
                    let dx_val = dx.read(u)?;
                    let dy_val = dy.read(u)?;
                    let a_val = a.read(u)?;

                    let m = if dx_val != 0 {
                        dy_val as f64 / dx_val as f64
                    } else {
                        0.0
                    };

                    for j in 0..seg_len {
                        if k + j >= n {
                            break;
                        }
                        let predicted = a_val as f64 + j as f64 * m;
                        let diff_val = diff.read(k + j)?;
                        let val = diff_val.wrapping_add(predicted as i64);
                        write_element(&mut output, k + j, val, elem_bits);
                    }
                    k += seg_len;
                    u += 1;
                }
            }
        }
        _ => {
            return Err(Error::Format(format!(
                "izip: unsupported encoding type {enc_type}"
            )));
        }
    }

    Ok(output)
}

/// Write a single element value to the output buffer.
///
/// `elem_bits` is loop-invariant at every call site. Marked `#[inline(always)]`
/// so LLVM can collapse the runtime match into the surrounding loop's
/// monomorphic form after the caller hoists `elem_bits` out.
#[inline(always)]
fn write_element(output: &mut [u8], idx: usize, val: i64, elem_bits: u32) {
    match elem_bits {
        8 => {
            output[idx] = val as u8;
        }
        16 => {
            let off = idx * 2;
            let bytes = (val as i16).to_le_bytes();
            output[off..off + 2].copy_from_slice(&bytes);
        }
        32 => {
            let off = idx * 4;
            let bytes = (val as i32).to_le_bytes();
            output[off..off + 4].copy_from_slice(&bytes);
        }
        64 => {
            let off = idx * 8;
            let bytes = val.to_le_bytes();
            output[off..off + 8].copy_from_slice(&bytes);
        }
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// irzip decode (v2 integer compression — plane-based zlib)
// ---------------------------------------------------------------------------

/// Apply delta decoding for a single value given a slope type.
fn apply_delta(last_val: i64, raw: u64, slope: i64) -> i64 {
    const DELTA_POS: i64 = 0x7ffffffffffffff0_u64 as i64;
    const DELTA_NEG: i64 = 0x7ffffffffffffff1_u64 as i64;

    if slope == DELTA_POS {
        last_val.wrapping_add(raw as i64)
    } else if slope == DELTA_NEG {
        last_val.wrapping_sub(raw as i64)
    } else {
        // DELTA_BOTH: low bit indicates direction
        if raw & 1 == 0 {
            last_val.wrapping_add((raw >> 1) as i64)
        } else {
            last_val.wrapping_sub((raw >> 1) as i64)
        }
    }
}

/// Decode irzip-compressed integers (v2 format used for READ_LEN, READ_START, etc.).
///
/// The irzip format compresses integer arrays by splitting each value into
/// byte-planes, zlib-compressing each plane independently, then reconstructing
/// values by OR-ing the planes back together with min/slope adjustment.
///
/// Parameters from the blob header:
/// - `min`: minimum value offset (added to each decoded value)
/// - `slope`: linear prediction slope or delta-type enum
/// - `planes`: bitmask indicating which byte-planes are present
/// - `num_elements`: number of output elements
/// - `series2`: optional `(min2, slope2)` for dual-series irzip v3 encoding
pub fn irzip_decode(
    data: &[u8],
    elem_bits: u32,
    num_elements: u32,
    min: i64,
    slope: i64,
    planes: u8,
    series2: Option<(i64, i64)>,
) -> Result<Vec<u8>> {
    let n = num_elements as usize;
    let out_bytes = (elem_bits / 8) as usize;

    // Bound both internal buffers against the input — `num_elements` is
    // caller-supplied and ultimately derived from blob-header bytes, so a
    // crafted header could otherwise drive an arbitrary allocation.
    let values_bytes = n
        .checked_mul(8)
        .ok_or_else(|| Error::Format(format!("irzip_decode: n={n} * 8 overflows usize")))?;
    check_alloc_bytes(values_bytes, data.len(), "irzip_decode values")?;
    let output_bytes = n.checked_mul(out_bytes).ok_or_else(|| {
        Error::Format(format!(
            "irzip_decode: n={n} * out_bytes={out_bytes} overflows usize"
        ))
    })?;
    check_alloc_bytes(output_bytes, data.len(), "irzip_decode output")?;

    // Decompress each byte-plane from concatenated zlib streams.
    let mut values = vec![0i64; n];
    let mut offset = 0usize;
    let mut first_plane = true;

    for bit in 0..8u32 {
        let mask = 1u8 << bit;
        if planes & mask == 0 {
            continue;
        }

        // Each plane is a separate raw-deflate stream expected to produce N
        // bytes. Some archives (observed on ENA-origin runs like
        // ERR15141550) legitimately produce a short stream — the reference
        // ncbi-vdb decoder reads `scratch[N]` from a malloc'd buffer, which
        // happens to be zero on a fresh allocation, so trailing indices are
        // effectively zero-filled. Mirror that: pad short planes with zero
        // bytes up to `n` before OR'ing into `values`. Avoids the panic
        // that used to fire on these archives (issue #20).
        let remaining = data.get(offset..).ok_or_else(|| {
            Error::Format(format!(
                "irzip: plane {bit} offset {offset} past data end ({})",
                data.len()
            ))
        })?;
        let (mut plane_bytes, consumed) = deflate_decompress_ex(remaining, n).map_err(|e| {
            let dump_len = remaining.len().min(64);
            let hex: String = remaining[..dump_len]
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<Vec<_>>()
                .join(" ");
            Error::Format(format!(
                "irzip plane {bit} at offset {offset} (remaining {} of {}, n={n}, planes={planes:#x}): {e}\n  bytes: {hex}",
                remaining.len(),
                data.len()
            ))
        })?;
        if plane_bytes.len() < n {
            tracing::debug!(
                "irzip plane {bit}: decompressed {} of {n} expected bytes — zero-filling trailing bytes",
                plane_bytes.len()
            );
            plane_bytes.resize(n, 0);
        }
        offset += consumed;

        // OR this plane's bytes into the values.
        let shift = bit * 8;
        if first_plane {
            for i in 0..n {
                values[i] = (plane_bytes[i] as i64) << shift;
            }
            first_plane = false;
        } else {
            for i in 0..n {
                values[i] |= (plane_bytes[i] as i64) << shift;
            }
        }
    }

    const DELTA_POS: i64 = 0x7ffffffffffffff0_u64 as i64;
    const DELTA_NEG: i64 = 0x7ffffffffffffff1_u64 as i64;
    const DELTA_BOTH: i64 = 0x7ffffffffffffff2_u64 as i64;

    let mut output = vec![0u8; output_bytes];

    if let Some((min2, slope2)) = series2 {
        // Dual-series (irzip v3): low bit of each value selects series.
        // Each series has independent delta accumulation.
        let mins = [min, min2];
        let slopes = [slope, slope2];
        let mut last_idx: [usize; 2] = [0, 0];
        let mut first_seen = [false, false];

        #[allow(clippy::needless_range_loop)]
        for i in 0..n {
            let raw = values[i] as u64;
            let series = (raw & 1) as usize;
            let val = raw >> 1; // remove series selector bit

            if !first_seen[series] {
                // First element of this series = min[series]
                values[i] = mins[series];
                first_seen[series] = true;
                last_idx[series] = i;
            } else {
                let prev = values[last_idx[series]];
                values[i] = apply_delta(prev, val, slopes[series]);
                last_idx[series] = i;
            }
            write_element(&mut output, i, values[i], elem_bits);
        }
    } else if slope == DELTA_POS || slope == DELTA_NEG || slope == DELTA_BOTH {
        // Single-series delta accumulation.
        let mut last_val: i64 = min;
        #[allow(clippy::needless_range_loop)]
        for i in 0..n {
            let raw = values[i] as u64;
            if i == 0 {
                write_element(&mut output, i, min, elem_bits);
                last_val = min;
            } else {
                let decoded = apply_delta(last_val, raw, slope);
                write_element(&mut output, i, decoded, elem_bits);
                last_val = decoded;
            }
        }
    } else {
        // Simple offset: val + min + i * slope (for non-delta slopes)
        for (i, &v) in values.iter().enumerate().take(n) {
            let val = v
                .wrapping_add(min)
                .wrapping_add((i as i64).wrapping_mul(slope));
            write_element(&mut output, i, val, elem_bits);
        }
    }

    Ok(output)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Test-only helpers (not used in production pipeline)
// ---------------------------------------------------------------------------

#[cfg(test)]
fn read_bits_be(src: &[u8], bit_offset: u64, n_bits: u32) -> Result<u64> {
    let mut value: u64 = 0;
    for bit in 0..n_bits {
        let abs_bit = bit_offset + bit as u64;
        let byte_idx = (abs_bit / 8) as usize;
        let bit_in_byte = 7 - (abs_bit % 8);

        if byte_idx >= src.len() {
            return Err(Error::Format("read_bits_be: out of bounds".into()));
        }

        let bit_val = (src[byte_idx] >> bit_in_byte) & 1;
        value = (value << 1) | u64::from(bit_val);
    }
    Ok(value)
}

#[cfg(test)]
fn unpack(packed_bits: u32, unpacked_bits: u32, src: &[u8], num_elements: u32) -> Result<Vec<u8>> {
    if packed_bits == 0 || unpacked_bits == 0 {
        return Err(Error::Format("unpack: zero bit width".into()));
    }
    if packed_bits > unpacked_bits {
        return Err(Error::Format(format!(
            "unpack: packed_bits ({packed_bits}) > unpacked_bits ({unpacked_bits})"
        )));
    }
    if !matches!(unpacked_bits, 8 | 16 | 32 | 64) {
        return Err(Error::Format(format!(
            "unpack: unpacked_bits must be 8/16/32/64, got {unpacked_bits}"
        )));
    }
    if num_elements == 0 {
        return Ok(vec![]);
    }
    if packed_bits == unpacked_bits && unpacked_bits == 8 {
        let count = num_elements as usize;
        if src.len() < count {
            return Err(Error::Format("unpack: source too short".into()));
        }
        return Ok(src[..count].to_vec());
    }
    let out_bytes = (unpacked_bits / 8) as usize;
    let mut result = vec![0u8; num_elements as usize * out_bytes];
    let mut bit_offset: u64 = 0;
    for i in 0..num_elements as usize {
        let value = read_bits_be(src, bit_offset, packed_bits)?;
        bit_offset += packed_bits as u64;
        let dst_offset = i * out_bytes;
        match unpacked_bits {
            8 => result[dst_offset] = value as u8,
            16 => result[dst_offset..dst_offset + 2].copy_from_slice(&(value as u16).to_le_bytes()),
            32 => result[dst_offset..dst_offset + 4].copy_from_slice(&(value as u32).to_le_bytes()),
            64 => result[dst_offset..dst_offset + 8].copy_from_slice(&value.to_le_bytes()),
            _ => unreachable!(),
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // vlen decode tests
    // -----------------------------------------------------------------------

    #[test]
    fn vlen_decode_u64_single_byte() {
        // Values < 0x80 are single-byte.
        let (val, consumed) = vlen_decode_u64(&[0x00]).unwrap();
        assert_eq!(val, 0);
        assert_eq!(consumed, 1);

        let (val, consumed) = vlen_decode_u64(&[0x7F]).unwrap();
        assert_eq!(val, 127);
        assert_eq!(consumed, 1);

        let (val, consumed) = vlen_decode_u64(&[42]).unwrap();
        assert_eq!(val, 42);
        assert_eq!(consumed, 1);
    }

    #[test]
    fn vlen_decode_u64_two_bytes() {
        // 0x80 | high7, low7 => (high7 << 7) | low7
        let (val, consumed) = vlen_decode_u64(&[0x81, 0x00]).unwrap();
        assert_eq!(val, 0x80); // (1 << 7) | 0
        assert_eq!(consumed, 2);

        // 128 = 0x80 => encoded as [0x81, 0x00]
        let (val, consumed) = vlen_decode_u64(&[0x81, 0x00]).unwrap();
        assert_eq!(val, 128);
        assert_eq!(consumed, 2);

        // 0x3FFF = 16383 => [0xFF, 0x7F]: (0x7F << 7) | 0x7F = 16383
        let (val, consumed) = vlen_decode_u64(&[0xFF, 0x7F]).unwrap();
        assert_eq!(val, 16383);
        assert_eq!(consumed, 2);
    }

    #[test]
    fn vlen_decode_u64_three_bytes() {
        // 16384 = 0x4000 => [0x81, 0x80, 0x00]
        // (1 << 14) | (0 << 7) | 0 = 16384
        let (val, consumed) = vlen_decode_u64(&[0x81, 0x80, 0x00]).unwrap();
        assert_eq!(val, 16384);
        assert_eq!(consumed, 3);
    }

    #[test]
    fn vlen_decode_u64_empty_error() {
        assert!(vlen_decode_u64(&[]).is_err());
    }

    #[test]
    fn vlen_decode_u64_all_continuation_error() {
        // 11 bytes all with continuation bit set = error.
        let data = [0x80u8; 11];
        assert!(vlen_decode_u64(&data).is_err());
    }

    #[test]
    fn vlen_decode_i64_positive() {
        // Positive value, bit 6 = 0.
        let (val, consumed) = vlen_decode_i64(&[0x05]).unwrap();
        assert_eq!(val, 5);
        assert_eq!(consumed, 1);
    }

    #[test]
    fn vlen_decode_i64_negative() {
        // Negative value, bit 6 = 1.
        // 0x45 = 0b01000101 => sign=1, value=5 => -5
        let (val, consumed) = vlen_decode_i64(&[0x45]).unwrap();
        assert_eq!(val, -5);
        assert_eq!(consumed, 1);
    }

    #[test]
    fn vlen_decode_i64_zero() {
        let (val, consumed) = vlen_decode_i64(&[0x00]).unwrap();
        assert_eq!(val, 0);
        assert_eq!(consumed, 1);
    }

    #[test]
    fn vlen_decode_i64_large_positive() {
        // Two-byte positive: bit 7=1 (continuation), bit 6=0 (positive),
        // bits 0-5 = high part, second byte = low 7 bits.
        // [0x81, 0x00] => value = (1 << 7) | 0 = 128, but with signed format:
        // First byte: 0x81 => sign=0, data=0x01, continuation
        // Second byte: 0x00 => data=0x00
        // value = (0x01 << 7) | 0x00 = 128
        let (val, consumed) = vlen_decode_i64(&[0x81, 0x00]).unwrap();
        assert_eq!(val, 128);
        assert_eq!(consumed, 2);
    }

    #[test]
    fn vlen_decode_i64_large_negative() {
        // [0xC1, 0x00] => sign=1, data bits = 0x01 (from first byte & 0x3F = 1)
        // continuation, second byte 0x00
        // magnitude = (1 << 7) | 0 = 128 => value = -128
        let (val, consumed) = vlen_decode_i64(&[0xC1, 0x00]).unwrap();
        assert_eq!(val, -128);
        assert_eq!(consumed, 2);
    }

    // -----------------------------------------------------------------------
    // vlen array decode tests
    // -----------------------------------------------------------------------

    #[test]
    fn vlen_decode_u64_array_basic() {
        // Three single-byte values: 1, 2, 3
        let (vals, consumed) = vlen_decode_u64_array(&[1, 2, 3], 3).unwrap();
        assert_eq!(vals, vec![1, 2, 3]);
        assert_eq!(consumed, 3);
    }

    #[test]
    fn vlen_decode_u64_array_mixed() {
        // 1 (single byte), 128 (two bytes: 0x81, 0x00), 3 (single byte)
        let data = [1, 0x81, 0x00, 3];
        let (vals, consumed) = vlen_decode_u64_array(&data, 3).unwrap();
        assert_eq!(vals, vec![1, 128, 3]);
        assert_eq!(consumed, 4);
    }

    // -----------------------------------------------------------------------
    // Page map tests
    // -----------------------------------------------------------------------

    #[test]
    fn page_map_variant0_fixed() {
        // variant=0, version=0 => byte0 = 0x00
        // row_length=10 => vlen encoded as single byte [0x0A]
        let data = [0x00, 0x0A];
        let pm = page_map_deserialize(&data, 100).unwrap();
        assert_eq!(pm.data_recs, 100);
        assert_eq!(pm.lengths, vec![10]);
        assert_eq!(pm.leng_runs, vec![100]);
        assert_eq!(pm.mapping, RowMapping::Identity);
    }

    #[test]
    fn page_map_variant1_fixed_variable_data_run() {
        // variant=1, version=0 => byte0 = 0x01
        // row_length=5, data_recs=3, data_runs=[2, 3, 1]
        let data = [0x01, 5, 3, 2, 3, 1];
        let pm = page_map_deserialize(&data, 6).unwrap();
        assert_eq!(pm.data_recs, 3);
        assert_eq!(pm.lengths, vec![5]);
        assert_eq!(pm.leng_runs, vec![6]);
        assert_eq!(pm.mapping, RowMapping::RepeatCounts(vec![2, 3, 1]));
    }

    #[test]
    fn page_map_variant2_variable_length() {
        // variant=2, version=0 => byte0 = 0x02
        // leng_recs=2
        // combined = [10, 20, 5, 3] (lengths=[10,20], leng_runs=[5,3])
        let data = [0x02, 2, 10, 20, 5, 3];
        let pm = page_map_deserialize(&data, 8).unwrap();
        assert_eq!(pm.data_recs, 8);
        assert_eq!(pm.lengths, vec![10, 20]);
        assert_eq!(pm.leng_runs, vec![5, 3]);
        assert_eq!(pm.mapping, RowMapping::Identity);
    }

    /// Raw deflate (no zlib wrapper), matching the `deflateInit2(..., -15,
    /// ...)` the page map serializer uses for versions 1 and 2.
    fn deflate_raw(data: &[u8]) -> Vec<u8> {
        use flate2::{Compression, write::DeflateEncoder};
        use std::io::Write;
        let mut enc = DeflateEncoder::new(Vec::new(), Compression::fast());
        enc.write_all(data).unwrap();
        enc.finish().unwrap()
    }

    /// Version 2 is the random-access encoding: the array after the header is
    /// `data_offset[row_count]`, one element offset per logical row, NOT
    /// repeat counts. Misreading this is issue #101.
    #[test]
    fn page_map_version2_variant0_is_random_access() {
        // byte0 = (version 2 << 2) | variant 0 = 0x08, row_length = 3.
        // Six rows drawn from a three-row vocabulary: offsets repeat and are
        // not monotonic, both of which the format allows.
        let mut data = vec![0x08, 3];
        data.extend_from_slice(&deflate_raw(&[0, 0, 3, 3, 0, 6]));
        let pm = page_map_deserialize(&data, 6).unwrap();

        assert_eq!(pm.lengths, vec![3]);
        assert_eq!(pm.leng_runs, vec![6]);
        assert_eq!(
            pm.mapping,
            RowMapping::RandomAccessOffsets(vec![0, 0, 3, 3, 0, 6])
        );
        // ncbi-vdb sets data_recs = row_count here (page-map.c:1288). Deriving
        // it from the offsets instead (e.g. max + 1 = 7) is meaningless: the
        // offsets are neither sorted nor unique.
        assert_eq!(pm.data_recs, 6);

        // Nine stored bytes back six rows of three.
        let expanded = pm.expand_rows(b"AAABBBCCC", 1).unwrap();
        assert_eq!(expanded, b"AAAAAABBBBBBAAACCC");
    }

    #[test]
    fn page_map_version2_variant2_is_random_access() {
        // byte0 = (2 << 2) | 2 = 0x0A, leng_recs = 2.
        // lengths=[2,3], leng_runs=[2,2], then data_offset[4].
        let mut data = vec![0x0A, 2];
        data.extend_from_slice(&deflate_raw(&[2, 3, 2, 2, 0, 0, 2, 2]));
        let pm = page_map_deserialize(&data, 4).unwrap();

        assert_eq!(pm.lengths, vec![2, 3]);
        assert_eq!(pm.leng_runs, vec![2, 2]);
        assert_eq!(
            pm.mapping,
            RowMapping::RandomAccessOffsets(vec![0, 0, 2, 2])
        );
        assert_eq!(pm.data_recs, 4);

        // Rows 0-1 are two bytes at offset 0; rows 2-3 are three at offset 2.
        let expanded = pm.expand_rows(b"ABCDE", 1).unwrap();
        assert_eq!(expanded, b"ABABCDECDE");
    }

    /// Version 1 keeps the run-length reading, so the same trailing array
    /// means something entirely different from the version-2 case above.
    #[test]
    fn page_map_version1_variant1_is_repeat_counts() {
        // byte0 = (1 << 2) | 1 = 0x05, row_length = 3, data_recs = 2.
        let mut data = vec![0x05, 3, 2];
        data.extend_from_slice(&deflate_raw(&[2, 4]));
        let pm = page_map_deserialize(&data, 6).unwrap();

        assert_eq!(pm.mapping, RowMapping::RepeatCounts(vec![2, 4]));
        // Record 0 backs rows 0-1, record 1 backs rows 2-5: 2x"AAA" + 4x"BBB".
        let expanded = pm.expand_rows(b"AAABBB", 1).unwrap();
        assert_eq!(expanded, b"AAAAAABBBBBBBBBBBB".to_vec());
    }

    /// Random access is only ever emitted for variants 0 and 2 — the encoder
    /// derives variant bit 0 from `data_recs != row_count` and
    /// `PageMapToRandomAccess` always sets them equal. Variants 1 and 3
    /// reserve no space for `data_offset[]`, so ncbi-vdb would read a NULL
    /// pointer; we refuse instead.
    #[test]
    fn page_map_version2_rejects_variants_with_data_runs() {
        for (byte0, variant) in [(0x09u8, 1), (0x0Bu8, 3)] {
            let mut data = vec![byte0, 3, 2];
            data.extend_from_slice(&deflate_raw(&[2, 4, 1, 1]));
            let err = page_map_deserialize(&data, 6)
                .expect_err("version 2 on variant {variant} must be rejected");
            assert!(
                matches!(err, Error::Format(ref m) if m.contains("random access")),
                "variant {variant}: got {err:?}"
            );
        }
    }

    #[test]
    fn page_map_rejects_unsupported_version() {
        // Version 3 is past everything ncbi-vdb will deserialize.
        let err = page_map_deserialize(&[0x0C, 1], 1).expect_err("version 3 must be rejected");
        assert!(matches!(err, Error::Format(_)), "got {err:?}");
    }

    #[test]
    fn page_map_variant3_variable_all() {
        // variant=3, version=0 => byte0 = 0x03
        // leng_recs=2, data_recs=3
        // combined = [10, 20, 5, 3, 1, 1, 1]
        //   lengths=[10,20], leng_runs=[5,3], data_runs=[1,1,1]
        let data = [0x03, 2, 3, 10, 20, 5, 3, 1, 1, 1];
        let pm = page_map_deserialize(&data, 8).unwrap();
        assert_eq!(pm.data_recs, 3);
        assert_eq!(pm.lengths, vec![10, 20]);
        assert_eq!(pm.leng_runs, vec![5, 3]);
        assert_eq!(pm.mapping, RowMapping::RepeatCounts(vec![1, 1, 1]));
    }

    // -----------------------------------------------------------------------
    // Blob header tests
    // -----------------------------------------------------------------------

    #[test]
    fn blob_headers_empty_frame() {
        // Version byte 0, then: flags=0, version=0, fmt=0, osize=0, ops=0, args=0
        let data = [0, 0, 0, 0, 0, 0, 0];
        let frames = blob_headers_deserialize(&data).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].flags, 0);
        assert_eq!(frames[0].version, 0);
        assert_eq!(frames[0].fmt, 0);
        assert_eq!(frames[0].osize, 0);
        assert!(frames[0].ops.is_empty());
        assert!(frames[0].args.is_empty());
    }

    #[test]
    fn blob_headers_with_fmt_and_osize() {
        // Verify our signed vlen encoder understanding:
        // vlen_encode1(signed) for 100: 100 >= 0x40, so 2 bytes:
        //   byte0 = 0x80 | (sign=0) | ((100 >> 7) & 0x3F) = 0x80 | 0 = 0x80
        //   byte1 = 100 & 0x7F = 0x64
        let (val, sz) = vlen_decode_i64(&[0x80, 0x64]).unwrap();
        assert_eq!(val, 100);
        assert_eq!(sz, 2);

        // Version byte 0, then: flags=1, version=2, fmt=5, osize=100, ops=0, args=0
        let data = [0, 1, 2, 5, 0x80, 0x64, 0, 0];
        let frames = blob_headers_deserialize(&data).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].flags, 1);
        assert_eq!(frames[0].version, 2);
        assert_eq!(frames[0].fmt, 5);
        assert_eq!(frames[0].osize, 100);
    }

    #[test]
    fn blob_headers_bad_version() {
        let data = [1]; // version 1 not supported
        assert!(blob_headers_deserialize(&data).is_err());
    }

    // -----------------------------------------------------------------------
    // Blob envelope v2 tests
    // -----------------------------------------------------------------------

    #[test]
    fn blob_v2_variant0_decode() {
        // Build a v2 header byte:
        // adjust=0, byte_order=LE(0), variant=0, version=2
        // = (2 << 6) | (0 << 4) | (0 << 3) | 0 = 0x80
        // But adjust encoding: (8 - x) & 7 = 0 means x = 0.
        // header_byte = adjust(0) | (byte_order(0) << 3) | (variant(0) << 4) | (version(2) << 6)
        // = 0b10_00_0_000 = 0x80
        let hdr_byte: u8 = 0x80;
        // variant 0: offset=3, hdr_size=src[1], map_size=src[2]
        let data = [
            hdr_byte, 5, 10, /* then 5 bytes header, 10 bytes map, then data... */
        ];
        let env = decode_blob_v2(&data).unwrap();
        assert_eq!(env.adjust, 0);
        assert!(!env.big_endian);
        assert_eq!(env.hdr_size, 5);
        assert_eq!(env.map_size, 10);
        assert_eq!(env.envelope_size, 3);
    }

    #[test]
    fn blob_v2_variant1_decode() {
        // variant=1, version=2, adjust=0, LE
        let hdr_byte: u8 = (2 << 6) | (1 << 4);
        let data = [hdr_byte, 5, 0x0A, 0x00]; // map_size = 10
        let env = decode_blob_v2(&data).unwrap();
        assert_eq!(env.hdr_size, 5);
        assert_eq!(env.map_size, 10);
        assert_eq!(env.envelope_size, 4);
    }

    #[test]
    fn blob_v2_bad_version() {
        // version = 1 (not 2)
        let hdr_byte: u8 = 1 << 6;
        let data = [hdr_byte, 0, 0];
        assert!(decode_blob_v2(&data).is_err());
    }

    // -----------------------------------------------------------------------
    // Bit unpack tests
    // -----------------------------------------------------------------------

    #[test]
    fn unpack_2bit_to_8bit() {
        // 2-bit packed: [0b00_01_10_11] = [0x1B]
        // Should produce 4 elements: [0, 1, 2, 3]
        let result = unpack(2, 8, &[0x1B], 4).unwrap();
        assert_eq!(result, vec![0, 1, 2, 3]);
    }

    #[test]
    fn unpack_2bit_to_8bit_two_bytes() {
        // [0b11_10_01_00, 0b00_01_10_11] -> [3,2,1,0, 0,1,2,3]
        let result = unpack(2, 8, &[0xE4, 0x1B], 8).unwrap();
        assert_eq!(result, vec![3, 2, 1, 0, 0, 1, 2, 3]);
    }

    #[test]
    fn unpack_4bit_to_8bit() {
        // [0xA5] -> high nibble=0xA=10, low nibble=0x5=5
        let result = unpack(4, 8, &[0xA5], 2).unwrap();
        assert_eq!(result, vec![10, 5]);
    }

    #[test]
    fn unpack_8bit_to_8bit_passthrough() {
        let data = vec![10, 20, 30];
        let result = unpack(8, 8, &data, 3).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn unpack_partial_last_byte() {
        // 3 elements from 2-bit packed in one byte.
        // [0b11_10_01_00] -> first 3 elements: [3, 2, 1]
        let result = unpack(2, 8, &[0xE4], 3).unwrap();
        assert_eq!(result, vec![3, 2, 1]);
    }

    #[test]
    fn unpack_empty() {
        let result = unpack(2, 8, &[], 0).unwrap();
        assert!(result.is_empty());
    }

    // -----------------------------------------------------------------------
    // read_bits_be tests
    // -----------------------------------------------------------------------

    #[test]
    fn read_bits_be_basic() {
        // 0xAB = 0b10101011
        // bit 0 (MSB) = 1, bit 1 = 0, bit 2 = 1, ...
        let val = read_bits_be(&[0xAB], 0, 2).unwrap();
        assert_eq!(val, 2); // bits: 1,0 => 0b10 = 2

        let val = read_bits_be(&[0xAB], 2, 2).unwrap();
        assert_eq!(val, 2); // bits: 1,0 => 0b10 = 2

        let val = read_bits_be(&[0xAB], 4, 2).unwrap();
        assert_eq!(val, 2); // bits: 1,0 => 0b10 = 2

        let val = read_bits_be(&[0xAB], 6, 2).unwrap();
        assert_eq!(val, 3); // bits: 1,1 => 0b11 = 3
    }

    // -----------------------------------------------------------------------
    // izip simple type tests
    // -----------------------------------------------------------------------

    #[test]
    fn izip_decode_type1_simple() {
        // Type 1: zipped. We create a minimal test with deflate-compressed data.
        use flate2::Compression;
        use flate2::write::DeflateEncoder;
        use std::io::Write as _;

        // Encode 4 u8 values: [10, 20, 30, 40]
        let values: [u8; 4] = [10, 20, 30, 40];
        let mut encoder = DeflateEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&values).unwrap();
        let compressed = encoder.finish().unwrap();

        // Build izip header: flags=1, data_count=4
        let mut data = Vec::new();
        data.push(0x01); // flags: type=1
        data.extend_from_slice(&4u32.to_le_bytes()); // data_count
        data.extend_from_slice(&compressed);

        let result = izip_decode(&data, 8).unwrap();
        assert_eq!(result, vec![10, 20, 30, 40]);
    }

    #[test]
    fn izip_decode_type3_packed_zipped() {
        // Type 3: packed + zipped. Compressed u8 values with min offset.
        use flate2::Compression;
        use flate2::write::DeflateEncoder;
        use std::io::Write as _;

        // We want values [100, 101, 102, 103], stored as offsets from min=100.
        // So packed values are [0, 1, 2, 3] as u8.
        let packed: [u8; 4] = [0, 1, 2, 3];
        let mut encoder = DeflateEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&packed).unwrap();
        let compressed = encoder.finish().unwrap();

        let mut data = Vec::new();
        data.push(0x03); // flags: type=3 (packed+zipped)
        data.extend_from_slice(&4u32.to_le_bytes()); // data_count
        data.extend_from_slice(&100i64.to_le_bytes()); // min
        data.extend_from_slice(&compressed);

        let result = izip_decode(&data, 8).unwrap();
        assert_eq!(result, vec![100, 101, 102, 103]);
    }

    #[test]
    fn izip_decode_type2_packed() {
        // Type 2: packed (no zlib), with min offset.
        // Values [50, 51, 52], stored as u8 offsets [0, 1, 2] from min=50.
        let mut data = Vec::new();
        data.push(0x02); // flags: type=2
        data.extend_from_slice(&3u32.to_le_bytes()); // data_count
        data.extend_from_slice(&50i64.to_le_bytes()); // min
        data.extend_from_slice(&[0u8, 1, 2]); // packed data

        let result = izip_decode(&data, 8).unwrap();
        assert_eq!(result, vec![50, 51, 52]);
    }

    #[test]
    fn izip_decode_rejects_oversized_data_count() {
        // Issue #30 reproducer: a 5-byte buffer with type=1 and
        // data_count=u32::MAX claims a 4 GiB output. `check_alloc_bytes`
        // must reject this rather than calling `vec![0u8; 4 GiB]`.
        let mut data = Vec::new();
        data.push(0x01); // flags: type=1
        data.extend_from_slice(&u32::MAX.to_le_bytes());
        let err = izip_decode(&data, 8).expect_err("oversized data_count must error");
        let msg = err.to_string();
        assert!(
            msg.contains("izip_decode output") || msg.contains("exceeds"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn izip_decode_rejects_overflowing_n_times_out_bytes() {
        // data_count near usize::MAX/4 would silently wrap when multiplied
        // by 8 (elem_bits=64). `checked_mul` must catch the overflow.
        let huge = u32::MAX / 2;
        let mut data = Vec::new();
        data.push(0x01); // flags: type=1
        data.extend_from_slice(&huge.to_le_bytes());
        let err = izip_decode(&data, 64).expect_err("overflow must error");
        let msg = err.to_string();
        assert!(
            msg.contains("overflows") || msg.contains("exceeds"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn irzip_decode_rejects_oversized_num_elements() {
        // Caller-supplied num_elements is unbounded; check_alloc_bytes
        // rejects requests larger than data.len() * MAX_DECODE_RATIO.
        let data = vec![0u8; 16];
        let err = irzip_decode(&data, 8, u32::MAX, 0, 0, 0xFF, None)
            .expect_err("oversized num_elements must error");
        let msg = err.to_string();
        assert!(
            msg.contains("irzip_decode") && (msg.contains("exceeds") || msg.contains("overflows")),
            "unexpected error: {msg}"
        );
    }

    // -----------------------------------------------------------------------
    // decode_blob tests
    // -----------------------------------------------------------------------

    #[test]
    fn decode_blob_v1_no_checksum() {
        // v1 header: byte_order=LE(1), adjust=0, rls=3 (implicit row_len=1)
        // header_byte = (3 << 5) | (0 << 2) | 1 = 0x61
        let mut blob = vec![0x61u8];
        blob.extend_from_slice(&[0xAA, 0xBB, 0xCC]); // data

        let decoded = decode_blob(&blob, 0, 1, 8).unwrap();
        assert_eq!(decoded.data, vec![0xAA, 0xBB, 0xCC]);
        assert_eq!(decoded.adjust, 0);
        assert_eq!(decoded.row_length, Some(1));
    }

    #[test]
    fn decode_blob_v1_with_crc32() {
        // Build a v1 blob with CRC32.
        let mut blob = vec![0x61u8]; // header
        blob.extend_from_slice(&[0xAA, 0xBB]); // data

        // Compute CRC32 of the blob content.
        let crc = ncbi_crc32(&blob);
        blob.extend_from_slice(&crc.to_le_bytes());

        let decoded = decode_blob(&blob, 1, 1, 8).unwrap();
        assert_eq!(decoded.data, vec![0xAA, 0xBB]);
    }

    #[test]
    fn decode_blob_v1_crc32_mismatch() {
        let mut blob = vec![0x61u8, 0xAA, 0xBB];
        blob.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // wrong CRC
        let err = decode_blob(&blob, 1, 1, 8).unwrap_err();
        assert!(matches!(err, Error::BlobIntegrity { kind: "CRC32", .. }));
    }

    #[test]
    fn ncbi_crc32_empty_is_zero() {
        // init=0, no xorout — empty input must yield 0.
        assert_eq!(ncbi_crc32(b""), 0);
    }

    #[test]
    fn decode_blob_v1_with_md5() {
        let mut blob = vec![0x61u8];
        blob.extend_from_slice(&[0xAA, 0xBB]);

        let digest = Md5::digest(&blob);
        blob.extend_from_slice(digest.as_slice());

        let decoded = decode_blob(&blob, 2, 1, 8).unwrap();
        assert_eq!(decoded.data, vec![0xAA, 0xBB]);
    }

    #[test]
    fn decode_blob_v1_md5_mismatch() {
        let mut blob = vec![0x61u8, 0xAA, 0xBB];
        blob.extend_from_slice(&[0u8; 16]); // wrong MD5 (all zeros)
        let err = decode_blob(&blob, 2, 1, 8).unwrap_err();
        assert!(matches!(err, Error::BlobIntegrity { kind: "MD5", .. }));
    }

    #[test]
    fn decode_blob_v2_minimal() {
        // v2 header byte: adjust=0, LE, variant=0, version=2 => 0x80
        // hdr_size=0, map_size=0
        let mut blob = vec![0x80, 0x00, 0x00]; // envelope
        blob.extend_from_slice(&[0xDD, 0xEE]); // data

        let decoded = decode_blob(&blob, 0, 1, 8).unwrap();
        assert_eq!(decoded.data, vec![0xDD, 0xEE]);
        assert_eq!(decoded.adjust, 0);
        assert!(decoded.headers.is_empty());
        assert!(decoded.page_map.is_none());
    }

    #[test]
    fn decode_blob_empty() {
        let decoded = decode_blob(&[], 0, 0, 8).unwrap();
        assert!(decoded.data.is_empty());
    }

    // -----------------------------------------------------------------------
    // PageMap expand_rows tests
    // -----------------------------------------------------------------------

    #[test]
    fn page_map_total_rows_single_run() {
        let pm = PageMap {
            data_recs: 100,
            lengths: vec![10],
            leng_runs: vec![100],
            mapping: RowMapping::Identity,
        };
        assert_eq!(pm.total_rows(), 100);
    }

    #[test]
    fn page_map_total_rows_multiple_runs() {
        let pm = PageMap {
            data_recs: 3,
            lengths: vec![5],
            leng_runs: vec![2048],
            mapping: RowMapping::RepeatCounts(vec![1000, 500, 548]),
        };
        assert_eq!(pm.total_rows(), 2048);
    }

    // The old `page_map_expand_data_runs_empty_runs` test (generic
    // `expand_data_runs` over `&[u32]`, empty runs) is folded into
    // `page_map_expand_rows_identity_is_passthrough` below: with the typed
    // mapping, "no data runs" is exactly `RowMapping::Identity`.

    #[test]
    fn page_map_expand_rows_repeat_counts_u32() {
        // RepeatCounts [2, 3, 1]: record 0 covers 2 rows, record 1 covers 3
        // rows, record 2 covers 1 row. One u32 element per row.
        let pm = PageMap {
            data_recs: 3,
            lengths: vec![1],
            leng_runs: vec![6],
            mapping: RowMapping::RepeatCounts(vec![2, 3, 1]),
        };
        let mut data = Vec::new();
        for v in [100u32, 200, 300] {
            data.extend_from_slice(&v.to_le_bytes());
        }
        let expanded = pm.expand_rows(&data, 4).unwrap();
        let vals: Vec<u32> = expanded
            .chunks_exact(4)
            .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
            .collect();
        assert_eq!(vals, vec![100, 100, 200, 200, 200, 300]);
    }

    #[test]
    fn page_map_expand_rows_repeat_counts_bytes_u32() {
        let pm = PageMap {
            data_recs: 2,
            lengths: vec![1], // one u32 element per row
            leng_runs: vec![5],
            mapping: RowMapping::RepeatCounts(vec![3, 2]),
        };
        // Two u32 LE values: 42 and 99
        let mut data = Vec::new();
        data.extend_from_slice(&42u32.to_le_bytes());
        data.extend_from_slice(&99u32.to_le_bytes());

        let expanded = pm.expand_rows(&data, 4).unwrap();
        assert_eq!(expanded.len(), 5 * 4); // 5 rows * 4 bytes each

        let vals: Vec<u32> = expanded
            .chunks_exact(4)
            .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
            .collect();
        assert_eq!(vals, vec![42, 42, 42, 99, 99]);
    }

    #[test]
    fn page_map_expand_rows_identity_is_passthrough() {
        // Identity: every record is one row and rows are already contiguous
        // and in order, so expansion is a copy.
        let pm = PageMap {
            data_recs: 2,
            lengths: vec![1],
            leng_runs: vec![2],
            mapping: RowMapping::Identity,
        };
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let expanded = pm.expand_rows(&data, 4).unwrap();
        assert_eq!(expanded, data);
    }

    #[test]
    fn page_map_expand_rows_random_access_offsets() {
        // RandomAccessOffsets: offsets are ELEMENT offsets, may repeat, and
        // need not increase. Rows 0 and 2 share record 0's copy; row 1 points
        // past it. This is the shape a version-2 page map writes after
        // VBlobPageMapOptimize dedups identical rows.
        let pm = PageMap {
            data_recs: 3,
            lengths: vec![1],
            leng_runs: vec![3],
            mapping: RowMapping::RandomAccessOffsets(vec![0, 1, 0]),
        };
        let mut data = Vec::new();
        for v in [42u32, 99] {
            data.extend_from_slice(&v.to_le_bytes());
        }
        let expanded = pm.expand_rows(&data, 4).unwrap();
        let vals: Vec<u32> = expanded
            .chunks_exact(4)
            .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
            .collect();
        assert_eq!(vals, vec![42, 99, 42]);
    }

    // `page_map_expand_records_to_rows_uniform_matches_fixed` compared the
    // fixed-width and variable-width walks against each other; both are now
    // the single `expand_rows` path, so the uniform-record case is covered by
    // `page_map_expand_rows_repeat_counts_bytes_u32` above.

    #[test]
    fn page_map_expand_rows_variable_lengths() {
        // Variable-length array column (e.g. READ_START on PacBio SMRT): records
        // have differing element counts, given by the lengths/leng_runs runs,
        // and each record is replicated repeat_counts[i] times. Mirrors the real
        // DRR032988 first-blob layout where data runs align with leng runs.
        //
        //   3 records: lengths 1, 3, 1 (sum = 5 u32s of source data)
        //   record 0 (len 1) -> repeated 2 rows
        //   record 1 (len 3) -> repeated 1 row
        //   record 2 (len 1) -> repeated 2 rows                (total 5 rows)
        let pm = PageMap {
            data_recs: 3,
            lengths: vec![1, 3, 1],
            leng_runs: vec![2, 1, 2],
            mapping: RowMapping::RepeatCounts(vec![2, 1, 2]),
        };
        // Source u32 stream: rec0=[10], rec1=[20,21,22], rec2=[30]  (5 u32s)
        let src: Vec<u32> = vec![10, 20, 21, 22, 30];
        let mut data = Vec::new();
        for v in &src {
            data.extend_from_slice(&v.to_le_bytes());
        }

        let expanded = pm.expand_rows(&data, 4).unwrap();
        let vals: Vec<u32> = expanded
            .chunks_exact(4)
            .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
            .collect();
        // row0=[10] row1=[10] row2=[20,21,22] row3=[30] row4=[30]
        assert_eq!(vals, vec![10, 10, 20, 21, 22, 30, 30]);
    }

    #[test]
    fn page_map_expand_rows_byte_column() {
        // Same shape, 1-byte elements (e.g. READ_TYPE).
        let pm = PageMap {
            data_recs: 3,
            lengths: vec![1, 3, 1],
            leng_runs: vec![2, 1, 2],
            mapping: RowMapping::RepeatCounts(vec![2, 1, 2]),
        };
        let data = vec![1u8, 0, 1, 0, 1]; // rec0=[1] rec1=[0,1,0] rec2=[1]
        let expanded = pm.expand_rows(&data, 1).unwrap();
        assert_eq!(expanded, vec![1, 1, 0, 1, 0, 1, 1]);
    }

    #[test]
    fn page_map_expand_rows_rejects_truncated() {
        // Source shorter than the rows demand -> error, not silent garbage.
        let pm = PageMap {
            data_recs: 2,
            lengths: vec![1, 3],
            leng_runs: vec![1, 1],
            mapping: RowMapping::RepeatCounts(vec![1, 1]),
        };
        let data = vec![0u8; 4]; // only 1 u32, needs 1 + 3 = 4 u32s
        assert!(pm.expand_rows(&data, 4).is_err());
    }

    #[test]
    fn page_map_expand_rows_rejects_incomplete_mapping() {
        // The repeat counts cover 2 + 3 = 5 rows but leng_runs claims 6, so the
        // map is internally inconsistent — refuse rather than emit a short
        // buffer that callers would silently mis-slice.
        let pm = PageMap {
            data_recs: 2,
            lengths: vec![1],
            leng_runs: vec![6],
            mapping: RowMapping::RepeatCounts(vec![2, 3]),
        };
        let data = vec![1u8, 2, 3, 4, 5, 6, 7, 8];
        assert!(pm.expand_rows(&data, 4).is_err());
    }

    #[test]
    fn page_map_expand_rows_row_length_gt_1() {
        // Regression for iter-1 validation: columns with row_length > 1 crashed
        // sracha with `data runs has N entries, expected at least 2N for 4N
        // bytes at 4 bytes/elem`. Row width now comes from the page map's own
        // `lengths`, so callers pass elem_bytes and the walk derives the rest.
        let pm = PageMap {
            data_recs: 3,
            lengths: vec![2], // two u32s per row
            leng_runs: vec![6],
            mapping: RowMapping::RepeatCounts(vec![2, 1, 3]),
        };
        // Three records, two u32 LE each = 24 bytes.
        let mut data = Vec::new();
        for v in [10u32, 11, 20, 21, 30, 31] {
            data.extend_from_slice(&v.to_le_bytes());
        }

        let expanded = pm.expand_rows(&data, 4).unwrap();
        // 2 + 1 + 3 = 6 logical rows, each 2 u32s.
        assert_eq!(expanded.len(), 6 * 8);
        let vals: Vec<u32> = expanded
            .chunks_exact(4)
            .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
            .collect();
        assert_eq!(vals, vec![10, 11, 10, 11, 20, 21, 30, 31, 30, 31, 30, 31]);

        // The units flipped relative to the old API: passing entry_bytes
        // (row_length × elem_bytes) now double-counts the row width and runs
        // off the end of the data. Kept so a caller can't regress to it
        // silently.
        let entry_bytes = pm.lengths[0] as usize * 4;
        assert!(pm.expand_rows(&data, entry_bytes).is_err());
    }

    #[test]
    fn page_map_expand_rows_rejects_truncated_data() {
        // Row length is 4 elements, so row 0 needs 4 bytes; give it only 2.
        let pm = PageMap {
            data_recs: 1,
            lengths: vec![4],
            leng_runs: vec![3],
            mapping: RowMapping::RepeatCounts(vec![3]),
        };
        let data = vec![1u8, 2];
        assert!(pm.expand_rows(&data, 1).is_err());
    }

    #[test]
    fn page_map_expand_rows_rejects_zero_elem_bytes() {
        let pm = PageMap {
            data_recs: 1,
            lengths: vec![1],
            leng_runs: vec![1],
            mapping: RowMapping::RepeatCounts(vec![1]),
        };
        assert!(pm.expand_rows(&[0u8; 4], 0).is_err());
    }

    // -----------------------------------------------------------------------
    // pad_trimmed_rows_fixed
    // -----------------------------------------------------------------------

    #[test]
    fn pad_trimmed_rows_leading_right_aligns_and_replicates() {
        // Mirrors ALTREAD `trim<0,0>`: each record's stored bytes land at
        // the END of a fixed-width row, and the repeat counts copy those
        // bytes into multiple consecutive rows.
        let pm = PageMap {
            data_recs: 2,
            lengths: vec![3, 0],
            leng_runs: vec![2, 3],
            mapping: RowMapping::RepeatCounts(vec![2, 3]),
        };
        // Record 0 = 3 bytes, record 1 = 0 bytes. Data = record 0 only.
        let data = vec![0xAA, 0xBB, 0xCC];
        let padded = pm
            .pad_trimmed_rows_fixed(&data, 5, TrimSide::Leading)
            .unwrap();
        // 5 rows × 5 bytes. First 2 rows: right-aligned [00 00 AA BB CC].
        // Next 3 rows: all zero (record 1 was empty).
        assert_eq!(
            padded,
            vec![
                0x00, 0x00, 0xAA, 0xBB, 0xCC, // row 0
                0x00, 0x00, 0xAA, 0xBB, 0xCC, // row 1
                0x00, 0x00, 0x00, 0x00, 0x00, // row 2
                0x00, 0x00, 0x00, 0x00, 0x00, // row 3
                0x00, 0x00, 0x00, 0x00, 0x00, // row 4
            ]
        );
    }

    #[test]
    fn pad_trimmed_rows_trailing_left_aligns() {
        let pm = PageMap {
            data_recs: 1,
            lengths: vec![2],
            leng_runs: vec![3],
            mapping: RowMapping::RepeatCounts(vec![3]),
        };
        let data = vec![0x01, 0x02];
        let padded = pm
            .pad_trimmed_rows_fixed(&data, 4, TrimSide::Trailing)
            .unwrap();
        assert_eq!(
            padded,
            vec![
                0x01, 0x02, 0x00, 0x00, // row 0
                0x01, 0x02, 0x00, 0x00, // row 1
                0x01, 0x02, 0x00, 0x00, // row 2
            ]
        );
    }

    #[test]
    fn pad_trimmed_rows_rejects_stored_longer_than_row() {
        let pm = PageMap {
            data_recs: 1,
            lengths: vec![5],
            leng_runs: vec![1],
            mapping: RowMapping::RepeatCounts(vec![1]),
        };
        let data = vec![1u8, 2, 3, 4, 5];
        // row_bytes=3 is shorter than the 5-byte record — must error.
        assert!(
            pm.pad_trimmed_rows_fixed(&data, 3, TrimSide::Leading)
                .is_err()
        );
    }

    // -----------------------------------------------------------------------
    // pad_trimmed_rows_variable — Illumina adapter-trim ALTREAD case
    // -----------------------------------------------------------------------

    #[test]
    fn pad_trimmed_rows_variable_leading_handles_mixed_row_widths() {
        // Three logical rows, three distinct true widths (4, 6, 3).
        // Each logical row stores its own 2-byte ALTREAD suffix; the
        // variable pad must right-align each suffix inside its own
        // target width, not a shared one.
        let pm = PageMap {
            data_recs: 3,
            lengths: vec![2],
            leng_runs: vec![3],
            mapping: RowMapping::Identity,
        };
        let data = vec![0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F];
        let row_lens = vec![4u32, 6, 3];
        let padded = pm
            .pad_trimmed_rows_variable(&data, &row_lens, TrimSide::Leading)
            .unwrap();
        assert_eq!(
            padded,
            vec![
                0x00, 0x00, 0x0A, 0x0B, // row 0 (width 4)
                0x00, 0x00, 0x00, 0x00, 0x0C, 0x0D, // row 1 (width 6)
                0x00, 0x0E, 0x0F, // row 2 (width 3, stored 2)
            ]
        );
    }

    #[test]
    fn pad_trimmed_rows_variable_replicates_via_repeat_counts() {
        // Two data records (3-byte, 2-byte). The repeat counts replicate
        // record 0 across two logical rows and record 1 across three. Each
        // replicated logical row gets its own target width — mimics the
        // ALTREAD case where deduplicated records expand into rows of
        // differing true lengths.
        let pm = PageMap {
            data_recs: 2,
            lengths: vec![3, 2],
            leng_runs: vec![2, 3],
            mapping: RowMapping::RepeatCounts(vec![2, 3]),
        };
        let data = vec![0xAA, 0xBB, 0xCC, 0xDD, 0xEE];
        let row_lens = vec![5u32, 4, 3, 2, 4];
        let padded = pm
            .pad_trimmed_rows_variable(&data, &row_lens, TrimSide::Leading)
            .unwrap();
        assert_eq!(
            padded,
            vec![
                0x00, 0x00, 0xAA, 0xBB, 0xCC, // row 0 (width 5, stored 3)
                0x00, 0xAA, 0xBB, 0xCC, // row 1 (width 4, stored 3)
                0x00, 0xDD, 0xEE, // row 2 (width 3, stored 2)
                0xDD, 0xEE, // row 3 (width 2, stored 2 fills exactly)
                0x00, 0x00, 0xDD, 0xEE, // row 4 (width 4, stored 2)
            ]
        );
    }

    #[test]
    fn pad_trimmed_rows_variable_rejects_oversized_record() {
        let pm = PageMap {
            data_recs: 1,
            lengths: vec![5],
            leng_runs: vec![1],
            mapping: RowMapping::Identity,
        };
        let data = vec![1u8, 2, 3, 4, 5];
        // Target row width 3 < stored record length 5.
        assert!(
            pm.pad_trimmed_rows_variable(&data, &[3], TrimSide::Leading)
                .is_err()
        );
    }

    #[test]
    fn pad_trimmed_rows_variable_rejects_wrong_row_lens_length() {
        let pm = PageMap {
            data_recs: 2,
            lengths: vec![1],
            leng_runs: vec![2],
            mapping: RowMapping::Identity,
        };
        let data = vec![0x01, 0x02];
        // total_rows is 2 but caller supplied only one length.
        assert!(
            pm.pad_trimmed_rows_variable(&data, &[4], TrimSide::Leading)
                .is_err()
        );
    }

    #[test]
    fn pad_trimmed_rows_random_access_dedups_offset_zero_across_rows() {
        // Variant-2 RA shape: 4 rows, two length runs (3 rows of length 2,
        // 1 row of length 3). Rows 0-2 share offset=0; row 3 sits at
        // offset 2. Logical width 5 — leading-trim restoration leaves
        // `width - trimmed` zero bytes at the start of each row.
        let pm = PageMap {
            data_recs: 4,
            lengths: vec![2, 3],
            leng_runs: vec![3, 1],
            mapping: RowMapping::RandomAccessOffsets(vec![0, 0, 0, 2]),
        };
        // data: bytes 0..2 = the dedup'd 2-byte payload for the first
        // run; bytes 2..5 = the unique 3-byte payload for the last row.
        let data = vec![0x0a, 0x0b, 0xcc, 0xdd, 0xee];
        let widths = [5u32; 4];
        let out = pm
            .pad_trimmed_rows_variable(&data, &widths, TrimSide::Leading)
            .unwrap();
        // Each output row is 5 bytes; trimmed bytes right-aligned with
        // leading zeros padding the gap.
        assert_eq!(
            out,
            vec![
                0, 0, 0, 0x0a, 0x0b, // row 0
                0, 0, 0, 0x0a, 0x0b, // row 1 (shares offset)
                0, 0, 0, 0x0a, 0x0b, // row 2 (shares offset)
                0, 0, 0xcc, 0xdd, 0xee, // row 3
            ]
        );
    }

    #[test]
    fn pad_trimmed_rows_random_access_zero_trim_emits_all_zeros() {
        // A zero-trim run means "no overlay for these rows" — the output
        // width should still be respected; bytes stay zero.
        let pm = PageMap {
            data_recs: 2,
            lengths: vec![0],
            leng_runs: vec![2],
            mapping: RowMapping::RandomAccessOffsets(vec![0, 0]),
        };
        let data = vec![];
        let out = pm
            .pad_trimmed_rows_variable(&data, &[4, 4], TrimSide::Leading)
            .unwrap();
        assert_eq!(out, vec![0u8; 8]);
    }

    #[test]
    fn pad_trimmed_rows_random_access_trailing_side_left_aligns() {
        // TrimSide::Trailing: trim<0,0> with side=1 strips trailing
        // zeros, so restoration places stored bytes at the start of
        // each row.
        let pm = PageMap {
            data_recs: 2,
            lengths: vec![2],
            leng_runs: vec![2],
            mapping: RowMapping::RandomAccessOffsets(vec![0, 0]),
        };
        let data = vec![0x11, 0x22];
        let out = pm
            .pad_trimmed_rows_variable(&data, &[5, 5], TrimSide::Trailing)
            .unwrap();
        assert_eq!(
            out,
            vec![
                0x11, 0x22, 0, 0, 0, // row 0
                0x11, 0x22, 0, 0, 0, // row 1
            ]
        );
    }

    #[test]
    fn pad_trimmed_rows_random_access_rejects_trim_greater_than_logical() {
        // A trimmed length wider than the row's logical width would mean
        // trim<0,0> output more bytes than its input held — corrupt
        // page_map. Refuse.
        let pm = PageMap {
            data_recs: 1,
            lengths: vec![10],
            leng_runs: vec![1],
            mapping: RowMapping::RandomAccessOffsets(vec![0]),
        };
        let data = vec![0u8; 10];
        assert!(
            pm.pad_trimmed_rows_variable(&data, &[5], TrimSide::Leading)
                .is_err()
        );
    }

    #[test]
    fn pad_trimmed_rows_random_access_rejects_offset_past_data_end() {
        // Offset+length out of bounds. Loud failure beats silent bad
        // FASTQ; the caller logs the error and skips the merge.
        let pm = PageMap {
            data_recs: 1,
            lengths: vec![3],
            leng_runs: vec![1],
            // 5 + 3 = 8 > data.len() = 6
            mapping: RowMapping::RandomAccessOffsets(vec![5]),
        };
        let data = vec![0u8; 6];
        assert!(
            pm.pad_trimmed_rows_variable(&data, &[5], TrimSide::Leading)
                .is_err()
        );
    }

    #[test]
    fn pad_trimmed_rows_random_access_rejects_wrong_logical_lens_count() {
        let pm = PageMap {
            data_recs: 2,
            lengths: vec![1],
            leng_runs: vec![2],
            mapping: RowMapping::RandomAccessOffsets(vec![0, 0]),
        };
        let data = vec![0x01];
        assert!(
            pm.pad_trimmed_rows_variable(&data, &[4], TrimSide::Leading)
                .is_err()
        );
    }

    // -----------------------------------------------------------------------
    // irzip delta / dual-series tests
    // -----------------------------------------------------------------------

    /// Build irzip test data: single plane of raw-deflate bytes.
    fn make_irzip_single_plane(values: &[u8]) -> Vec<u8> {
        use flate2::Compression;
        use flate2::write::DeflateEncoder;
        use std::io::Write as _;

        let mut encoder = DeflateEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(values).unwrap();
        encoder.finish().unwrap()
    }

    #[test]
    fn irzip_decode_delta_both_single_series() {
        // DELTA_BOTH single series: values encode sign in low bit.
        // Target output: [100, 110, 105, 115]
        // min=100, element 0 = 100 (written as min).
        // Deltas from previous: +10, -5, +10
        // DELTA_BOTH encoding: +10 → 20 (even), -5 → 11 (5<<1|1), +10 → 20
        let raw_values: Vec<u8> = vec![0, 20, 11, 20];
        let data = make_irzip_single_plane(&raw_values);
        let delta_both: i64 = 0x7ffffffffffffff2_u64 as i64;

        let result = irzip_decode(&data, 32, 4, 100, delta_both, 0x01, None).unwrap();
        let vals: Vec<u32> = result
            .chunks_exact(4)
            .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
            .collect();
        assert_eq!(vals, vec![100, 110, 105, 115]);
    }

    #[test]
    fn irzip_decode_dual_series() {
        // Dual-series (irzip v3, series_count=2).
        // Low bit selects series: 0 → series 0, 1 → series 1.
        // Series 0: min=100, slope=DELTA_POS (cumulative positive deltas)
        // Series 1: min=200, slope=DELTA_POS
        //
        // Target output: [100, 200, 105, 203]
        //   idx 0: series 0 (even), first → min[0]=100
        //   idx 1: series 1 (odd),  first → min[1]=200
        //   idx 2: series 0 (even), delta=5 from 100 → 105
        //   idx 3: series 1 (odd),  delta=3 from 200 → 203
        //
        // Packed values (before series bit removal):
        //   idx 0: 0 (series 0: val=0, first element)
        //   idx 1: 1 (series 1: val=0, first element) — low bit=1 selects series 1
        //   idx 2: 10 (series 0: val=5<<1=10, delta=+5)
        //   idx 3: 7 (series 1: val=3<<1|1=7, delta=+3)
        let raw_values: Vec<u8> = vec![0, 1, 10, 7];
        let data = make_irzip_single_plane(&raw_values);

        let delta_pos: i64 = 0x7ffffffffffffff0_u64 as i64;
        let series2 = Some((200i64, delta_pos));

        let result = irzip_decode(&data, 32, 4, 100, delta_pos, 0x01, series2).unwrap();
        let vals: Vec<u32> = result
            .chunks_exact(4)
            .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
            .collect();
        assert_eq!(vals, vec![100, 200, 105, 203]);
    }

    #[test]
    fn irzip_decode_dual_series_delta_both() {
        // Dual-series with DELTA_BOTH on both series.
        // Low bit = series selector, then after >> 1, low bit = direction.
        //
        // Target: [1000, 2000, 1015, 1990]
        //   idx 0: series 0, first → 1000
        //   idx 1: series 1, first → 2000
        //   idx 2: series 0, delta +15 from 1000 → 1015
        //   idx 3: series 1, delta -10 from 2000 → 1990
        //
        // Series selector is lowest bit. After removing it:
        //   For DELTA_BOTH: low bit = direction (0=+, 1=-)
        //   +15: val_inner = 15<<1 = 30 (even=positive)
        //   -10: val_inner = 10<<1|1 = 21 (odd=negative)
        //
        // Packed values (with series bit):
        //   idx 0: 0 (series 0, val=0)
        //   idx 1: 1 (series 1, val=0)
        //   idx 2: 30<<1|0 = 60 (series 0, val_inner=30)
        //   idx 3: 21<<1|1 = 43 (series 1, val_inner=21)
        let raw_values: Vec<u8> = vec![0, 1, 60, 43];
        let data = make_irzip_single_plane(&raw_values);

        let delta_both: i64 = 0x7ffffffffffffff2_u64 as i64;
        let series2 = Some((2000i64, delta_both));

        let result = irzip_decode(&data, 32, 4, 1000, delta_both, 0x01, series2).unwrap();
        let vals: Vec<u32> = result
            .chunks_exact(4)
            .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
            .collect();
        assert_eq!(vals, vec![1000, 2000, 1015, 1990]);
    }

    // -----------------------------------------------------------------------
    // PageMap property tests — regression fence for issue-#22 class bugs
    //
    // Every `expand_*` method had silent-wrong-output failure modes when
    // variable repeat counts interacted with heterogeneous row lengths. These
    // tests compare against a naive "obvious" reference expansion over
    // randomly-shaped page maps to catch any such divergence.
    // -----------------------------------------------------------------------

    use proptest::prelude::*;

    /// Build a PageMap from `(repeat_count, length)` pairs — one per data record.
    /// The on-disk format assumes rows within the same data-run share a
    /// length, so we expand + RLE to produce canonical `lengths`/`leng_runs`.
    fn page_map_from_pairs(pairs: &[(u32, u32)]) -> PageMap {
        let mut logical_lens: Vec<u32> = Vec::new();
        for &(run, len) in pairs {
            for _ in 0..run {
                logical_lens.push(len);
            }
        }
        let mut lengths: Vec<u32> = Vec::new();
        let mut leng_runs: Vec<u32> = Vec::new();
        for &l in &logical_lens {
            match lengths.last() {
                Some(&last) if last == l => *leng_runs.last_mut().unwrap() += 1,
                _ => {
                    lengths.push(l);
                    leng_runs.push(1);
                }
            }
        }
        PageMap {
            data_recs: pairs.len() as u64,
            lengths,
            leng_runs,
            mapping: RowMapping::RepeatCounts(pairs.iter().map(|&(r, _)| r).collect()),
        }
    }

    proptest! {
        /// expand_rows matches the naive "emit record i's bytes
        /// repeat_counts[i] times" expansion for every well-formed page
        /// map. Direct fence against issue #22: the original bug silently
        /// skipped expansion whenever `lengths` wasn't all-equal, and a
        /// property test like this one would have caught it before landing.
        #[test]
        fn prop_expand_variable_matches_reference(
            pairs in proptest::collection::vec(
                (1u32..=4u32, 0u32..=8u32),
                1..12,
            ),
        ) {
            let pm = page_map_from_pairs(&pairs);
            let record_lens: Vec<u32> = pairs.iter().map(|&(_, l)| l).collect();
            let repeats: Vec<u32> = pairs.iter().map(|&(r, _)| r).collect();

            // data: record i is `length` bytes of value `i as u8`. Picking
            // per-record distinct bytes lets a mis-stitched output be
            // detected by vec-equality alone.
            let mut data = Vec::new();
            for (i, &len) in record_lens.iter().enumerate() {
                for _ in 0..len {
                    data.push(i as u8);
                }
            }

            let got = pm.expand_rows(&data, 1).unwrap();

            let mut expected = Vec::new();
            let mut cursor = 0usize;
            for (i, &len) in record_lens.iter().enumerate() {
                let chunk = &data[cursor..cursor + len as usize];
                for _ in 0..repeats[i] as usize {
                    expected.extend_from_slice(chunk);
                }
                cursor += len as usize;
            }
            prop_assert_eq!(got, expected);
        }

        /// expand_rows over fixed-width (one element per row) records
        /// matches a flat_map expansion for every well-formed repeat-count
        /// vector.
        #[test]
        fn prop_expand_rows_fixed_matches_reference(
            runs in proptest::collection::vec(1u32..=4u32, 1..15),
        ) {
            let values: Vec<u32> = (0..runs.len() as u32).collect();
            let mut data = Vec::new();
            for v in &values {
                data.extend_from_slice(&v.to_le_bytes());
            }
            let total_rows: u32 = runs.iter().sum();
            // One u32 element per row: lengths=[1], leng_runs=[total].
            let pm = PageMap {
                data_recs: values.len() as u64,
                lengths: vec![1],
                leng_runs: vec![total_rows],
                mapping: RowMapping::RepeatCounts(runs.clone()),
            };
            let got: Vec<u32> = pm.expand_rows(&data, 4).unwrap()
                .chunks_exact(4)
                .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                .collect();
            let expected: Vec<u32> = values
                .iter()
                .zip(runs.iter())
                .flat_map(|(&v, &r)| std::iter::repeat_n(v, r as usize))
                .collect();
            prop_assert_eq!(got, expected);
        }

        /// logical_row_lengths reproduces one length per logical row: record
        /// i's `length` repeated `repeat_count` times. Round-trip over the
        /// RLE (`lengths`/`leng_runs`) encoding built by page_map_from_pairs.
        #[test]
        fn prop_logical_row_lengths_round_trip(
            pairs in proptest::collection::vec(
                (1u32..=4u32, 0u32..=8u32),
                1..12,
            ),
        ) {
            let pm = page_map_from_pairs(&pairs);
            let got = pm.logical_row_lengths();
            let expected: Vec<u32> = pairs
                .iter()
                .flat_map(|&(r, l)| std::iter::repeat_n(l, r as usize))
                .collect();
            prop_assert_eq!(got, expected);
        }

        /// total_rows always equals both sum(leng_runs) and sum(repeat counts)
        /// for well-formed page maps. Cross-checks the two RLE encodings.
        #[test]
        fn prop_total_rows_consistent(
            pairs in proptest::collection::vec(
                (1u32..=4u32, 0u32..=8u32),
                1..12,
            ),
        ) {
            let pm = page_map_from_pairs(&pairs);
            let total = pm.total_rows();
            let sum_leng: u64 = pm.leng_runs.iter().map(|&r| u64::from(r)).sum();
            let sum_data: u64 = pm
                .repeat_counts()
                .expect("page_map_from_pairs builds RepeatCounts")
                .iter()
                .map(|&r| u64::from(r))
                .sum();
            prop_assert_eq!(total, sum_leng);
            prop_assert_eq!(total, sum_data);
        }

        /// row_extents_range(skip, take) returns exactly the window it was
        /// asked for out of the full row_extents() walk — the windowed walk
        /// must not drift from the full one.
        #[test]
        fn prop_row_extents_range_matches_full(
            pairs in proptest::collection::vec(
                (1u32..=4u32, 0u32..=8u32),
                1..12,
            ),
            skip in 0usize..12,
            take in 0usize..12,
        ) {
            let pm = page_map_from_pairs(&pairs);
            let full = pm.row_extents().unwrap();
            let window = pm.row_extents_range(skip, take).unwrap();
            let expected: Vec<RowExtent> = full
                .iter()
                .skip(skip)
                .take(take)
                .copied()
                .collect();
            prop_assert_eq!(window, expected);
        }
    }
}
