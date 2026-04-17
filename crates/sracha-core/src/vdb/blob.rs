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

// ---------------------------------------------------------------------------
// Variable-length integer encoding (vlen)
// ---------------------------------------------------------------------------

/// Decode a variable-length encoded unsigned integer.
///
/// The encoding uses 7 data bits per byte with the high bit as a continuation
/// flag: if bit 7 is set, more bytes follow.
///
/// Returns `(value, bytes_consumed)`.
pub fn vlen_decode_u64(data: &[u8]) -> Result<(u64, usize)> {
    if data.is_empty() {
        return Err(Error::Vdb("vlen_decode_u64: empty input".into()));
    }

    let limit = data.len().min(10);
    let mut value: u64 = 0;
    let mut i = 0;

    loop {
        if i >= limit {
            return Err(Error::Vdb(
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
pub fn vlen_decode_i64(data: &[u8]) -> Result<(i64, usize)> {
    if data.is_empty() {
        return Err(Error::Vdb("vlen_decode_i64: empty input".into()));
    }

    let limit = data.len().min(10);
    let first = data[0];
    let negative = first & 0x40 != 0;
    let mut value: i64 = i64::from(first & 0x3F);
    let mut i = 1;

    if first & 0x80 != 0 {
        loop {
            if i >= limit {
                return Err(Error::Vdb(
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

/// Deserialized page map describing row boundaries within a blob.
#[derive(Debug, Clone)]
pub struct PageMap {
    /// Number of data records (rows) in the blob.
    pub data_recs: u64,
    /// Row lengths (one per unique length run).
    pub lengths: Vec<u32>,
    /// Length runs (how many consecutive rows share the same length).
    pub leng_runs: Vec<u32>,
    /// Data runs (how many rows share the same physical data position).
    /// Empty for variants where data_run is always 1.
    pub data_runs: Vec<u32>,
}

impl PageMap {
    /// Total number of logical rows described by this page map.
    ///
    /// This is the sum of `leng_runs` (each entry tells how many consecutive
    /// rows share the same length).
    pub fn total_rows(&self) -> u64 {
        self.leng_runs.iter().map(|&r| u64::from(r)).sum()
    }

    /// Expand run-length-encoded data to full row data.
    ///
    /// Takes decoded values (one per `data_rec`) and returns expanded values
    /// (one per logical row). Each data entry `i` covers `data_runs[i]`
    /// consecutive rows. If `data_runs` is empty, each data entry covers
    /// exactly one row (no expansion needed).
    ///
    /// This is used for columns like READ_LEN where `irzip_decode` produces
    /// `data_recs` unique values, but the actual row count is larger because
    /// some values repeat via `data_runs`.
    pub fn expand_data_runs<T: Clone>(&self, data: &[T]) -> Vec<T> {
        if self.data_runs.is_empty() {
            // No run-length encoding — each data entry = one row.
            return data.to_vec();
        }

        let total = self.total_rows() as usize;
        let mut expanded = Vec::with_capacity(total);

        for (i, item) in data.iter().enumerate() {
            let repeat = self.data_runs.get(i).copied().unwrap_or(1) as usize;
            for _ in 0..repeat {
                expanded.push(item.clone());
            }
        }

        expanded
    }

    /// Compute per-data-record byte lengths from the page map.
    ///
    /// The page map stores `lengths` (unique length values) and `leng_runs`
    /// (how many consecutive rows share each length). This function expands
    /// to one length per data record (not per logical row — data_runs are
    /// NOT applied here).
    fn data_record_lengths(&self) -> Vec<u32> {
        if self.data_runs.is_empty() {
            // No data_runs: data_recs == total_rows, expand leng_runs directly
            let mut row_lens = Vec::with_capacity(self.data_recs as usize);
            for (length, &run) in self.lengths.iter().zip(self.leng_runs.iter()) {
                for _ in 0..run {
                    row_lens.push(*length);
                }
            }
            return row_lens;
        }

        // With data_runs: expand leng_runs to get per-logical-row lengths,
        // then collapse back to per-data-record lengths by skipping repeated rows.
        let mut logical_lens = Vec::with_capacity(self.total_rows() as usize);
        for (length, &run) in self.lengths.iter().zip(self.leng_runs.iter()) {
            for _ in 0..run {
                logical_lens.push(*length);
            }
        }

        let mut record_lens = Vec::with_capacity(self.data_recs as usize);
        let mut row = 0usize;
        for i in 0..self.data_recs as usize {
            let repeat = self.data_runs.get(i).copied().unwrap_or(1) as usize;
            if row < logical_lens.len() {
                record_lens.push(logical_lens[row]);
            }
            row += repeat;
        }
        record_lens
    }

    /// Expand variable-length data via data_runs.
    ///
    /// Unlike [`expand_data_runs_bytes`](Self::expand_data_runs_bytes) which
    /// handles fixed-size elements, this handles variable-length rows (e.g.,
    /// quality data where each row has a different number of bytes).
    ///
    /// Fails if `data` is shorter than the sum of record lengths or if
    /// `data_runs` has fewer entries than there are records — either signals
    /// a truncated blob or a corrupt page map that would otherwise drop rows
    /// silently.
    pub fn expand_variable_data_runs(&self, data: &[u8]) -> Result<Vec<u8>> {
        if self.data_runs.is_empty() {
            return Ok(data.to_vec());
        }

        let record_lens = self.data_record_lengths();

        if self.data_runs.len() < record_lens.len() {
            return Err(Error::Vdb(format!(
                "page_map: data_runs has {} entries, expected at least {}",
                self.data_runs.len(),
                record_lens.len(),
            )));
        }

        let total_data: usize = record_lens.iter().map(|&l| l as usize).sum();
        if data.len() < total_data {
            return Err(Error::Vdb(format!(
                "page_map: variable data truncated — have {} bytes, need {}",
                data.len(),
                total_data,
            )));
        }

        let expected_out: usize = record_lens
            .iter()
            .zip(self.data_runs.iter())
            .map(|(&len, &rep)| len as usize * rep as usize)
            .sum();
        let mut expanded = Vec::with_capacity(expected_out);
        let mut cursor = 0usize;
        for (i, &len) in record_lens.iter().enumerate() {
            let repeat = self.data_runs[i] as usize;
            let chunk = &data[cursor..cursor + len as usize];
            for _ in 0..repeat {
                expanded.extend_from_slice(chunk);
            }
            cursor += len as usize;
        }

        if expanded.len() != expected_out {
            return Err(Error::Vdb(format!(
                "page_map: expanded length {} != expected {}",
                expanded.len(),
                expected_out,
            )));
        }

        Ok(expanded)
    }

    /// Expand run-length-encoded byte data to full row data.
    ///
    /// Like [`expand_data_runs`](Self::expand_data_runs), but operates on
    /// fixed-size elements packed into a byte slice. Each element is
    /// `elem_bytes` wide.
    ///
    /// Fails if `data_runs` has fewer entries than the number of elements in
    /// `data` — missing entries used to default to repeat=1, silently losing
    /// the intended per-record repeat counts.
    pub fn expand_data_runs_bytes(&self, data: &[u8], elem_bytes: usize) -> Result<Vec<u8>> {
        if self.data_runs.is_empty() || elem_bytes == 0 {
            return Ok(data.to_vec());
        }

        let n = data.len() / elem_bytes;
        if self.data_runs.len() < n {
            return Err(Error::Vdb(format!(
                "page_map: data_runs has {} entries, expected at least {} for {} bytes at {} bytes/elem",
                self.data_runs.len(),
                n,
                data.len(),
                elem_bytes,
            )));
        }

        let expected_out: usize = self
            .data_runs
            .iter()
            .take(n)
            .map(|&r| r as usize * elem_bytes)
            .sum();
        let mut expanded = Vec::with_capacity(expected_out);

        for i in 0..n {
            let repeat = self.data_runs[i] as usize;
            let start = i * elem_bytes;
            let end = start + elem_bytes;
            let chunk = &data[start..end];
            for _ in 0..repeat {
                expanded.extend_from_slice(chunk);
            }
        }

        Ok(expanded)
    }
}

/// Deserialize a page map from its serialized form.
///
/// The first byte encodes `variant` (bits 0-1) and `version` (bits 2+).
/// Version 0 uses the v0 deserializer directly. Versions 1-2 use v1 which
/// may delegate to v0 after decompression.
pub fn page_map_deserialize(data: &[u8], row_count: u64) -> Result<PageMap> {
    if data.is_empty() {
        return Err(Error::Vdb("page_map_deserialize: empty input".into()));
    }

    let version = data[0] >> 2;

    match version {
        0 => page_map_deserialize_v0(data, row_count),
        1 | 2 => page_map_deserialize_v1(data, row_count),
        _ => Err(Error::Vdb(format!(
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
        return Err(Error::Vdb("page_map_v0: empty input".into()));
    }

    let variant = data[0] & 3;
    let mut cur = 1;

    let random_access = (data[0] >> 2) == 2;

    match variant {
        0 => {
            // Fixed row length.
            let (row_len, sz) = vlen_decode_u64(&data[cur..])?;
            cur += sz;

            if random_access {
                // Random access: data_offset array maps each row to a data position.
                // The array has row_count entries, each indicating which data_rec
                // that row uses. This allows compacting N rows into fewer unique entries.
                let (data_offsets, _) = deserialize_lengths(&data[cur..], row_count as usize)?;
                // data_recs = max(data_offsets) + 1
                let max_off = data_offsets.iter().copied().max().unwrap_or(0);
                Ok(PageMap {
                    data_recs: (max_off + 1) as u64,
                    lengths: vec![row_len as u32],
                    leng_runs: vec![row_count as u32],
                    data_runs: data_offsets, // repurpose data_runs as data_offsets
                })
            } else {
                Ok(PageMap {
                    data_recs: row_count,
                    lengths: vec![row_len as u32],
                    leng_runs: vec![row_count as u32],
                    data_runs: vec![],
                })
            }
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
                data_runs,
            })
        }
        2 => {
            // Variable row length, data_run = 1.
            let (leng_recs, sz) = vlen_decode_u64(&data[cur..])?;
            cur += sz;

            // Both lengths and leng_runs are serialized sequentially.
            let total = 2 * leng_recs as usize;
            let (combined, _) = deserialize_lengths(&data[cur..], total)?;

            let lengths = combined[..leng_recs as usize].to_vec();
            let leng_runs = combined[leng_recs as usize..].to_vec();

            Ok(PageMap {
                data_recs: row_count,
                lengths,
                leng_runs,
                data_runs: vec![],
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
                data_runs,
            })
        }
        _ => Err(Error::Vdb(format!(
            "page_map_v0: unsupported variant {variant}"
        ))),
    }
}

fn page_map_deserialize_v1(data: &[u8], row_count: u64) -> Result<PageMap> {
    if data.is_empty() {
        return Err(Error::Vdb("page_map_v1: empty input".into()));
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
            return Err(Error::Vdb(format!(
                "page_map_v1: unsupported variant {variant}"
            )));
        }
    };

    // Decompress the body (zlib after the header portion).
    let compressed = &data[hsize..];
    if compressed.is_empty() {
        return Err(Error::Vdb("page_map_v1: no compressed data".into()));
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
        return Err(Error::Vdb("blob_headers: empty input".into()));
    }
    if data[0] != 0 {
        return Err(Error::Vdb(format!(
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
            return Err(Error::Vdb(
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
                return Err(Error::Vdb("blob_headers: insufficient ops data".into()));
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
        return Err(Error::Vdb("blob v1: empty".into()));
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
            return Err(Error::Vdb("blob v1: header too short".into()));
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
        return Err(Error::Vdb("blob v2: empty".into()));
    }

    let hdr_byte = data[0];
    let adjust = (8u8.wrapping_sub(hdr_byte & 7)) & 7;
    let big_endian = ((hdr_byte >> 3) & 1) != 0;
    let variant = (hdr_byte >> 4) & 3;
    let version = hdr_byte >> 6;

    if version != 2 {
        return Err(Error::Vdb(format!(
            "blob v2: bad version {version}, expected 2"
        )));
    }

    let (hdr_size, map_size, envelope_size) = match variant {
        0 => {
            if data.len() < 3 {
                return Err(Error::Vdb("blob v2.0: too short".into()));
            }
            (u32::from(data[1]), u32::from(data[2]), 3u32)
        }
        1 => {
            if data.len() < 4 {
                return Err(Error::Vdb("blob v2.1: too short".into()));
            }
            let ms = u32::from(data[2]) | (u32::from(data[3]) << 8);
            (u32::from(data[1]), ms, 4)
        }
        2 => {
            if data.len() < 6 {
                return Err(Error::Vdb("blob v2.2: too short".into()));
            }
            let ms = u32::from_le_bytes(data[2..6].try_into().unwrap());
            (u32::from(data[1]), ms, 6)
        }
        3 => {
            if data.len() < 9 {
                return Err(Error::Vdb("blob v2.3: too short".into()));
            }
            let hs = u32::from_le_bytes(data[1..5].try_into().unwrap());
            let ms = u32::from_le_bytes(data[5..9].try_into().unwrap());
            (hs, ms, 9)
        }
        _ => {
            return Err(Error::Vdb(format!(
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
            return Err(Error::Vdb(format!("unknown checksum type {checksum_type}")));
        }
    };

    if raw.len() < cs_size {
        return Err(Error::Vdb("blob too short for checksum".into()));
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
        let computed_crc = crc32fast::hash(blob_data);
        if stored_crc != computed_crc {
            return Err(Error::Vdb(format!(
                "CRC32 mismatch: stored={stored_crc:#010x}, computed={computed_crc:#010x}"
            )));
        }
    } else if checksum_type == 2 && cs_size == 16 {
        let stored: [u8; 16] = raw[raw.len() - 16..]
            .try_into()
            .expect("slice length is 16");
        let computed = Md5::digest(blob_data);
        if stored != computed.as_slice() {
            return Err(Error::Vdb(format!(
                "MD5 mismatch: stored={}, computed={}",
                hex16(&stored),
                hex16(computed.as_slice()),
            )));
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
            return Err(Error::Vdb(
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
        return Err(Error::Vdb("izip: read_u32_le out of bounds".into()));
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
        return Err(Error::Vdb("izip: read_i64_le out of bounds".into()));
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
        return Err(Error::Vdb("izip: data too short".into()));
    }

    let flags = src[0];
    let data_count = read_u32_le(src, 1)?;
    let mut i: usize = 5;
    let enc_type = flags & 0x03;

    match enc_type {
        // Type 2 or 3: packed (optionally zipped)
        2 | 3 => {
            if src.len() < i + 8 {
                return Err(Error::Vdb("izip: packed data too short for min".into()));
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
                    return Err(Error::Vdb("izip: type_data too short".into()));
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
                    return Err(Error::Vdb("izip: diff_data too short".into()));
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
                    return Err(Error::Vdb("izip: length_data too short".into()));
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
                    return Err(Error::Vdb("izip: dy_data too short".into()));
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
                    return Err(Error::Vdb("izip: dx_data too short".into()));
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
                    return Err(Error::Vdb("izip: a_data too short".into()));
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
                    return Err(Error::Vdb("izip: outlier_data too short".into()));
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
        _ => Err(Error::Vdb(format!(
            "izip: unknown encoding type {enc_type}"
        ))),
    }
}

/// Helper to decompress or copy a sub-array buffer.
fn izip_decompress_buf(data: &[u8], flag: u32, max_out: usize) -> Result<Vec<u8>> {
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
pub(crate) fn deflate_decompress(data: &[u8], expected_size: usize) -> Result<Vec<u8>> {
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
pub(crate) fn zlib_decompress(data: &[u8], expected_size: usize) -> Result<Vec<u8>> {
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
        return Err(Error::Vdb(
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
                .map_err(|e| Error::Vdb(format!("deflate_ex fallback failed: {e}")))?;
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
        .map_err(|e| Error::Vdb(format!("deflate decompression failed: {e}")))?;
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
        .map_err(|e| Error::Vdb(format!("zlib decompression failed: {e}")))?;
    Ok(out)
}

/// Determine the nbuf variant from elem_bits.
fn variant_from_elem_bits(elem_bits: u32) -> Result<u32> {
    match elem_bits {
        8 => Ok(4),
        16 => Ok(3),
        32 => Ok(2),
        64 => Ok(1),
        _ => Err(Error::Vdb(format!("izip: invalid elem_bits {elem_bits}"))),
    }
}

/// Read a raw value from a buffer using the nbuf variant encoding.
fn nbuf_read(data: &[u8], idx: usize, variant: u32) -> i64 {
    match variant {
        4 => i64::from(data[idx]),
        3 => {
            let off = idx * 2;
            i64::from(u16::from_le_bytes([data[off], data[off + 1]]))
        }
        2 => {
            let off = idx * 4;
            i64::from(u32::from_le_bytes([
                data[off],
                data[off + 1],
                data[off + 2],
                data[off + 3],
            ]))
        }
        _ => {
            let off = idx * 8;
            i64::from_le_bytes([
                data[off],
                data[off + 1],
                data[off + 2],
                data[off + 3],
                data[off + 4],
                data[off + 5],
                data[off + 6],
                data[off + 7],
            ])
        }
    }
}

/// Read a single element from an nbuf, adding `min`.
#[inline(always)]
fn nbuf_read_min(data: &[u8], idx: usize, variant: u32, min: i64) -> i64 {
    nbuf_read(data, idx, variant).wrapping_add(min)
}

/// Decode types bitmap: each bit in `src` maps to one segment type (0=line, 1=outlier).
fn decode_types(n: usize, src: &[u8]) -> Vec<u8> {
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
    dst
}

/// Decode izip-compressed integers.
///
/// `data` is the raw izip-encoded byte stream (as found in the blob's column
/// data after envelope/header stripping). `elem_bits` is the output element
/// size in bits (8, 16, 32, or 64). `num_elements` is the expected number of
/// output elements.
///
/// Returns the decoded integers as a byte vector in native (little-endian)
/// format, with `num_elements * (elem_bits / 8)` bytes.
pub fn izip_decode(data: &[u8], elem_bits: u32, _num_elements_hint: u32) -> Result<Vec<u8>> {
    let encoded = deserialize_izip_encoded(data)?;
    // Use the data_count from the izip header as the authoritative element count.
    // The caller's hint may not match (e.g., blob id_range vs actual element count).
    let n = encoded.data_count as usize;

    let enc_type = encoded.flags & 0x03;
    let _size_type = ((encoded.flags >> 2) & 3) as u32;

    let out_bytes = (elem_bits / 8) as usize;
    let mut output = vec![0u8; n * out_bytes];

    match enc_type {
        // Type 1: zlib-compressed, no min offset.
        // Type 3: zlib-compressed with min offset.
        1 | 3 => {
            let decompressed = zlib_raw_decompress(encoded.simple_data, n * 8)?;
            let elem_size_bits = (decompressed.len() * 8) / n;
            let var = variant_from_elem_bits(elem_size_bits as u32)?;

            let min = if enc_type == 3 { encoded.simple_min } else { 0 };

            for i in 0..n {
                let raw = nbuf_read(&decompressed, i, var);
                let val = (raw as i64).wrapping_add(min);
                write_element(&mut output, i, val, elem_bits);
            }
        }
        // Type 2: packed (no zlib), with min offset.
        2 => {
            let elem_size_bits = (encoded.simple_data.len() * 8) / n;
            let var = variant_from_elem_bits(elem_size_bits as u32)?;

            for i in 0..n {
                let raw = nbuf_read(encoded.simple_data, i, var);
                let val = (raw as i64).wrapping_add(encoded.simple_min);
                write_element(&mut output, i, val, elem_bits);
            }
        }
        // Type 0: full izip with line segments.
        0 => {
            let iz = encoded
                .izipped
                .as_ref()
                .ok_or_else(|| Error::Vdb("izip type 0: missing izip fields".into()))?;

            // Decode diff buffer.
            let flag_diff = flag_extract(iz.data_flags, FLAG_BITS);
            let diff_raw = if flag_diff == DATA_CONSTANT {
                vec![0u8; iz.diff_size as usize]
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
            let segment_types = if iz.outliers > 0 {
                let flag_type = flag_extract(iz.data_flags, 0);
                let type_raw = if flag_type == DATA_ZIPPED {
                    zlib_raw_decompress(iz.type_data, iz.segments as usize)?
                } else {
                    iz.type_data.to_vec()
                };
                decode_types(iz.segments as usize, &type_raw)
            } else {
                vec![0u8; iz.segments as usize]
            };

            let lines = segment_types.iter().filter(|&&t| t == 0).count();
            let outlier_count = segment_types.iter().filter(|&&t| t != 0).count();

            // Decode raw byte buffers for each component.  The packed values
            // are read inline during reconstruction via nbuf_read_min(),
            // avoiding intermediate Vec<i64> allocations.
            let flag_length = flag_extract(iz.data_flags, 2 * FLAG_BITS);
            let total_segs = lines + outlier_count;
            let length_raw = if flag_length == DATA_CONSTANT {
                vec![0u8; iz.length_size as usize * total_segs]
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
                vec![0u8; lines * 8]
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
                vec![0u8; lines * 8]
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
                vec![0u8; lines * 8]
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
                    vec![0u8; outlier_count * 8]
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

            // Reconstruct output, reading packed values inline to avoid
            // materializing intermediate Vec<i64> buffers.
            let mut k = 0usize; // output element index
            let mut u = 0usize; // line segment index
            let mut v = 0usize; // outlier value index

            for (seg_idx, &seg_type) in segment_types.iter().enumerate() {
                let seg_len =
                    nbuf_read_min(&length_raw, seg_idx, length_var, iz.min_length) as usize;

                if seg_type != 0 {
                    // Outlier segment: copy values directly.
                    for j in 0..seg_len {
                        if k + j >= n {
                            break;
                        }
                        let val = nbuf_read_min(&outlier_raw, v + j, outlier_var, iz.min_outlier);
                        write_element(&mut output, k + j, val, elem_bits);
                    }
                    k += seg_len;
                    v += seg_len;
                } else {
                    // Line segment: reconstruct using diff + linear model.
                    let dx_val = nbuf_read_min(&dx_raw, u, dx_var, iz.min_dx);
                    let dy_val = nbuf_read_min(&dy_raw, u, dy_var, iz.min_dy);
                    let a_val = nbuf_read_min(&a_raw, u, a_var, iz.min_a);

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
                        let diff_val = nbuf_read_min(&diff_raw, k + j, diff_var, iz.min_diff);
                        let val = diff_val.wrapping_add(predicted as i64);
                        write_element(&mut output, k + j, val, elem_bits);
                    }
                    k += seg_len;
                    u += 1;
                }
            }
        }
        _ => {
            return Err(Error::Vdb(format!(
                "izip: unsupported encoding type {enc_type}"
            )));
        }
    }

    Ok(output)
}

/// Write a single element value to the output buffer.
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

    // Decompress each byte-plane from concatenated zlib streams.
    let mut values = vec![0i64; n];
    let mut offset = 0usize;
    let mut first_plane = true;

    for bit in 0..8u32 {
        let mask = 1u8 << bit;
        if planes & mask == 0 {
            continue;
        }

        // Each plane is a separate raw-deflate stream producing N bytes.
        let remaining = &data[offset..];
        let (plane_bytes, consumed) = deflate_decompress_ex(remaining, n)?;
        if plane_bytes.len() < n {
            tracing::debug!(
                "irzip plane {bit}: decompressed {} of {n} expected bytes",
                plane_bytes.len()
            );
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

    let mut output = vec![0u8; n * out_bytes];

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
            return Err(Error::Vdb("read_bits_be: out of bounds".into()));
        }

        let bit_val = (src[byte_idx] >> bit_in_byte) & 1;
        value = (value << 1) | u64::from(bit_val);
    }
    Ok(value)
}

#[cfg(test)]
fn unpack(packed_bits: u32, unpacked_bits: u32, src: &[u8], num_elements: u32) -> Result<Vec<u8>> {
    if packed_bits == 0 || unpacked_bits == 0 {
        return Err(Error::Vdb("unpack: zero bit width".into()));
    }
    if packed_bits > unpacked_bits {
        return Err(Error::Vdb(format!(
            "unpack: packed_bits ({packed_bits}) > unpacked_bits ({unpacked_bits})"
        )));
    }
    if !matches!(unpacked_bits, 8 | 16 | 32 | 64) {
        return Err(Error::Vdb(format!(
            "unpack: unpacked_bits must be 8/16/32/64, got {unpacked_bits}"
        )));
    }
    if num_elements == 0 {
        return Ok(vec![]);
    }
    if packed_bits == unpacked_bits && unpacked_bits == 8 {
        let count = num_elements as usize;
        if src.len() < count {
            return Err(Error::Vdb("unpack: source too short".into()));
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
        assert!(pm.data_runs.is_empty());
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
        assert_eq!(pm.data_runs, vec![2, 3, 1]);
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
        assert!(pm.data_runs.is_empty());
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
        assert_eq!(pm.data_runs, vec![1, 1, 1]);
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

        let result = izip_decode(&data, 8, 4).unwrap();
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

        let result = izip_decode(&data, 8, 4).unwrap();
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

        let result = izip_decode(&data, 8, 3).unwrap();
        assert_eq!(result, vec![50, 51, 52]);
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
        let crc = crc32fast::hash(&blob);
        blob.extend_from_slice(&crc.to_le_bytes());

        let decoded = decode_blob(&blob, 1, 1, 8).unwrap();
        assert_eq!(decoded.data, vec![0xAA, 0xBB]);
    }

    #[test]
    fn decode_blob_v1_crc32_mismatch() {
        let mut blob = vec![0x61u8, 0xAA, 0xBB];
        blob.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // wrong CRC
        assert!(decode_blob(&blob, 1, 1, 8).is_err());
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
        assert!(err.to_string().contains("MD5 mismatch"));
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
    // PageMap expand_data_runs tests
    // -----------------------------------------------------------------------

    #[test]
    fn page_map_total_rows_single_run() {
        let pm = PageMap {
            data_recs: 100,
            lengths: vec![10],
            leng_runs: vec![100],
            data_runs: vec![],
        };
        assert_eq!(pm.total_rows(), 100);
    }

    #[test]
    fn page_map_total_rows_multiple_runs() {
        let pm = PageMap {
            data_recs: 3,
            lengths: vec![5],
            leng_runs: vec![2048],
            data_runs: vec![1000, 500, 548],
        };
        assert_eq!(pm.total_rows(), 2048);
    }

    #[test]
    fn page_map_expand_data_runs_empty_runs() {
        // No data_runs means each entry covers 1 row (no expansion).
        let pm = PageMap {
            data_recs: 3,
            lengths: vec![10],
            leng_runs: vec![3],
            data_runs: vec![],
        };
        let data = vec![10u32, 20, 30];
        let expanded = pm.expand_data_runs(&data);
        assert_eq!(expanded, vec![10, 20, 30]);
    }

    #[test]
    fn page_map_expand_data_runs_with_repeats() {
        // data_runs = [2, 3, 1] means:
        //   entry 0 covers 2 rows, entry 1 covers 3 rows, entry 2 covers 1 row
        let pm = PageMap {
            data_recs: 3,
            lengths: vec![5],
            leng_runs: vec![6],
            data_runs: vec![2, 3, 1],
        };
        let data = vec![100u32, 200, 300];
        let expanded = pm.expand_data_runs(&data);
        assert_eq!(expanded, vec![100, 100, 200, 200, 200, 300]);
    }

    #[test]
    fn page_map_expand_data_runs_bytes_u32() {
        let pm = PageMap {
            data_recs: 2,
            lengths: vec![4],
            leng_runs: vec![5],
            data_runs: vec![3, 2],
        };
        // Two u32 LE values: 42 and 99
        let mut data = Vec::new();
        data.extend_from_slice(&42u32.to_le_bytes());
        data.extend_from_slice(&99u32.to_le_bytes());

        let expanded = pm.expand_data_runs_bytes(&data, 4).unwrap();
        assert_eq!(expanded.len(), 5 * 4); // 5 rows * 4 bytes each

        let vals: Vec<u32> = expanded
            .chunks_exact(4)
            .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
            .collect();
        assert_eq!(vals, vec![42, 42, 42, 99, 99]);
    }

    #[test]
    fn page_map_expand_data_runs_bytes_empty_runs() {
        let pm = PageMap {
            data_recs: 2,
            lengths: vec![4],
            leng_runs: vec![2],
            data_runs: vec![],
        };
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let expanded = pm.expand_data_runs_bytes(&data, 4).unwrap();
        assert_eq!(expanded, data);
    }

    #[test]
    fn page_map_expand_data_runs_bytes_rejects_short_data_runs() {
        // data has 3 elements (12 bytes / 4), but data_runs only has 2.
        let pm = PageMap {
            data_recs: 2,
            lengths: vec![4],
            leng_runs: vec![5],
            data_runs: vec![2, 3],
        };
        let data = vec![1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
        assert!(pm.expand_data_runs_bytes(&data, 4).is_err());
    }

    #[test]
    fn page_map_expand_variable_data_runs_rejects_truncated_data() {
        // record_lens will be [4], needs 4 bytes; give it only 2.
        let pm = PageMap {
            data_recs: 1,
            lengths: vec![4],
            leng_runs: vec![3],
            data_runs: vec![3],
        };
        let data = vec![1u8, 2];
        assert!(pm.expand_variable_data_runs(&data).is_err());
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
}
