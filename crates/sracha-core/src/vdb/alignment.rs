//! PRIMARY_ALIGNMENT table reader for reference-compressed cSRA.
//!
//! Exposes the per-row columns required by `align_restore_read` to
//! reconstruct aligned bases from REFERENCE + mismatch overlay.
//! See `docs/internal/csra-format-notes.md` for the algorithm.

use std::io::{Read, Seek};
use std::path::Path;

use crate::error::{Error, Result};
use crate::vdb::blob::{self, DecodedBlob};
use crate::vdb::inspect;
use crate::vdb::kar::KarArchive;
use crate::vdb::kdb::ColumnReader;

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
    /// Absolute reference position (across concatenated references) where the
    /// alignment starts.
    global_ref_start: ColumnReader,
    /// Number of reference bases covered by the alignment (alignment span).
    ref_len: ColumnReader,
    /// 1 byte per base of the reconstructed read; nonzero means the base
    /// differs from the reference at that position.
    has_mismatch: ColumnReader,
    /// 1 byte per base of the reconstructed read; nonzero at positions where
    /// `ref_offset` advances or rewinds the reference cursor (indels).
    has_ref_offset: ColumnReader,
    /// 4na-bin bases, one per `has_mismatch == 1` position.
    mismatch: ColumnReader,
    /// Signed i32 offsets, one per `has_ref_offset == 1` position.
    ref_offset: ColumnReader,
    row_count: u64,
    first_row: i64,
}

impl AlignmentCursor {
    /// Open the PRIMARY_ALIGNMENT columns in the archive.
    pub fn open<R: Read + Seek>(archive: &mut KarArchive<R>, sra_path: &Path) -> Result<Self> {
        let col_base = inspect::column_base_path_public(archive, Some("PRIMARY_ALIGNMENT"))?;
        let open = |archive: &mut KarArchive<R>, name: &str| -> Result<ColumnReader> {
            ColumnReader::open(archive, &format!("{col_base}/{name}"), sra_path)
                .map_err(|e| Error::Vdb(format!("PRIMARY_ALIGNMENT/{name}: {e}")))
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
            global_ref_start,
            ref_len,
            has_mismatch,
            has_ref_offset,
            mismatch,
            ref_offset,
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

    pub fn global_ref_start_col(&self) -> &ColumnReader {
        &self.global_ref_start
    }

    pub fn ref_len_col(&self) -> &ColumnReader {
        &self.ref_len
    }

    pub fn has_mismatch_col(&self) -> &ColumnReader {
        &self.has_mismatch
    }

    pub fn has_ref_offset_col(&self) -> &ColumnReader {
        &self.has_ref_offset
    }

    pub fn mismatch_col(&self) -> &ColumnReader {
        &self.mismatch
    }

    pub fn ref_offset_col(&self) -> &ColumnReader {
        &self.ref_offset
    }

    /// Read a single u64 fixed-length column value for `row_id`.
    fn read_u64(col: &ColumnReader, row_id: i64) -> Result<u64> {
        read_scalar_int(col, row_id, 64).map(|v| v as u64)
    }

    /// Read a single u32 fixed-length column value for `row_id`.
    fn read_u32(col: &ColumnReader, row_id: i64) -> Result<u32> {
        read_scalar_int(col, row_id, 32).map(|v| v as u32)
    }

    /// Return the alignment row 1's GLOBAL_REF_START — smoke-test hook for
    /// wiring verification during early Phase 1 development.
    pub fn first_global_ref_start(&self) -> Result<u64> {
        Self::read_u64(&self.global_ref_start, self.first_row)
    }

    pub fn first_ref_len(&self) -> Result<u32> {
        Self::read_u32(&self.ref_len, self.first_row)
    }

    /// Read one alignment row end-to-end: all seven columns' values for
    /// `row_id`. Re-decodes each column's blob on every call; callers doing
    /// many sequential reads should add a blob cache. Intended for Phase 1
    /// / Phase 2 validation before the pipeline integration lands.
    pub fn read_row(&self, row_id: i64) -> Result<AlignmentRow> {
        let global_ref_start = Self::read_u64(&self.global_ref_start, row_id)?;
        let ref_len = Self::read_u32(&self.ref_len, row_id)?;
        let has_mismatch = read_bool_row_as_bytes(&self.has_mismatch, row_id)?;
        let has_ref_offset = read_bool_row_as_bytes(&self.has_ref_offset, row_id)?;
        let mismatch = read_byte_row(&self.mismatch, row_id)?;
        let ref_offset = read_i32_row(&self.ref_offset, row_id)?;

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

/// What physical encoding layer wraps a variable-length column's payload.
#[derive(Clone, Copy)]
enum VarEncoding {
    /// `zip_encoding` — HAS_MISMATCH / HAS_REF_OFFSET (bit-packed) and MISMATCH
    /// (bytes). The element width comes from the column's page_map, not the
    /// envelope.
    Zip,
    /// `irzip` at the given element width — REF_OFFSET (32 bits per value).
    IrzipAtBitWidth(u32),
}

/// Open the blob containing `row_id`, decode it with the given encoding, and
/// hand back the uncompressed payload + page map + row offset within the blob.
fn read_variable_payload(
    col: &ColumnReader,
    row_id: i64,
    enc: VarEncoding,
) -> Result<(Vec<u8>, blob::PageMap, usize)> {
    let blob = col
        .find_blob(row_id)
        .ok_or_else(|| Error::Vdb(format!("no blob for row {row_id}")))?;
    let raw = col.read_raw_blob_slice(row_id)?;
    let decoded = blob::decode_blob(raw, col.meta().checksum_type, u64::from(blob.id_range), 8)?;
    let pm = decoded
        .page_map
        .clone()
        .ok_or_else(|| Error::Vdb("variable column: page_map required".into()))?;
    let bytes = match enc {
        VarEncoding::Zip => decode_bytes_payload(&decoded)?,
        VarEncoding::IrzipAtBitWidth(bits) => decode_integer_bytes(&decoded, bits)?,
    };
    Ok((bytes, pm, (row_id - blob.start_id) as usize))
}

/// Read one row of a `bool_encoding` column, returning one byte (0 or 1)
/// per logical value. The payload is bit-packed (LSB-first), so we unpack
/// the slice belonging to `row_id` before returning.
fn read_bool_row_as_bytes(col: &ColumnReader, row_id: i64) -> Result<Vec<u8>> {
    let (bytes, pm, row_offset) = read_variable_payload(col, row_id, VarEncoding::Zip)?;
    let record_lens = pm.data_record_lengths();

    // record_lens[i] is in bits; cumulative start in bits then divides to
    // byte offset since records are stored back-to-back bit-packed.
    let start_bits: u32 = record_lens
        .iter()
        .take(resolve_record_idx(&pm, row_offset, row_id)?)
        .sum();
    let rec_idx = resolve_record_idx(&pm, row_offset, row_id)?;
    let len_bits = record_lens[rec_idx] as usize;

    // B1 bit-packing is MSB-first within each byte (verified by comparing
    // sracha's unpack output against vdb-dump on HAS_MISMATCH row 1 of
    // VDB-3418: LSB-first drifts one off in the 1s count, MSB-first matches).
    let mut out = Vec::with_capacity(len_bits);
    for i in 0..len_bits {
        let bit_idx = start_bits as usize + i;
        let byte = bit_idx / 8;
        let bit = 7 - (bit_idx % 8);
        let b = bytes
            .get(byte)
            .copied()
            .ok_or_else(|| Error::Vdb(format!("bool row {row_id}: bit {bit_idx} past payload")))?;
        out.push((b >> bit) & 1);
    }
    Ok(out)
}

/// Read one row of an ascii/byte column (MISMATCH). Each record length
/// counts bytes directly.
fn read_byte_row(col: &ColumnReader, row_id: i64) -> Result<Vec<u8>> {
    let (bytes, pm, row_offset) = read_variable_payload(col, row_id, VarEncoding::Zip)?;
    let record_lens = pm.data_record_lengths();
    let rec_idx = resolve_record_idx(&pm, row_offset, row_id)?;

    let start: usize = record_lens.iter().take(rec_idx).map(|&n| n as usize).sum();
    let len = record_lens[rec_idx] as usize;
    let end = start + len;
    if end > bytes.len() {
        return Err(Error::Vdb(format!(
            "byte row {row_id}: slice [{start}..{end}] past payload {}",
            bytes.len()
        )));
    }
    Ok(bytes[start..end].to_vec())
}

/// Read one row of a signed-32 column (REF_OFFSET). Each record length
/// counts i32 elements; multiply by 4 for the byte offset into payload.
fn read_i32_row(col: &ColumnReader, row_id: i64) -> Result<Vec<i32>> {
    let (bytes, pm, row_offset) =
        read_variable_payload(col, row_id, VarEncoding::IrzipAtBitWidth(32))?;
    let record_lens = pm.data_record_lengths();
    let rec_idx = resolve_record_idx(&pm, row_offset, row_id)?;

    let start: usize = record_lens
        .iter()
        .take(rec_idx)
        .map(|&n| n as usize * 4)
        .sum();
    let len_elems = record_lens[rec_idx] as usize;
    let end = start + len_elems * 4;
    if end > bytes.len() {
        return Err(Error::Vdb(format!(
            "i32 row {row_id}: slice [{start}..{end}] past payload {}",
            bytes.len()
        )));
    }
    Ok(bytes[start..end]
        .chunks_exact(4)
        .map(|c| i32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect())
}

/// Resolve the record index for a logical row offset, honouring `data_runs`.
fn resolve_record_idx(pm: &blob::PageMap, logical_offset: usize, row_id: i64) -> Result<usize> {
    if pm.data_runs.is_empty() {
        return Ok(logical_offset);
    }
    let mut seen = 0usize;
    for (i, &repeat) in pm.data_runs.iter().enumerate() {
        let end = seen + repeat as usize;
        if logical_offset < end {
            return Ok(i);
        }
        seen = end;
    }
    Err(Error::Vdb(format!(
        "row {row_id}: logical offset {logical_offset} outside data_runs"
    )))
}

/// Decode a zip_encoding payload to raw bytes.
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
    Err(Error::Vdb(format!(
        "byte column: no decoder succeeded (data.len={}, osize={osize})",
        decoded.data.len()
    )))
}

/// Read one scalar (single-value-per-row) integer column value, honouring
/// the page_map's data_runs compression.
///
/// GLOBAL_REF_START / REF_LEN are stored as unique values plus a
/// `data_runs` RLE (most alignments in a blob share the same length /
/// chunk, so the 7-row blob's payload holds only the unique values). The
/// run-length walk mirrors `ReferenceCursor::read_chunk_len`.
fn read_scalar_int(col: &ColumnReader, row_id: i64, elem_bits: u32) -> Result<i64> {
    let blob = col
        .find_blob(row_id)
        .ok_or_else(|| Error::Vdb(format!("scalar int: no blob for row {row_id}")))?;
    let raw = col.read_raw_blob_slice(row_id)?;
    let decoded = blob::decode_blob(
        raw,
        col.meta().checksum_type,
        u64::from(blob.id_range),
        elem_bits,
    )?;
    let bytes = decode_integer_bytes(&decoded, elem_bits)?;

    let logical_offset = (row_id - blob.start_id) as usize;
    let data_idx = if let Some(pm) = &decoded.page_map
        && !pm.data_runs.is_empty()
    {
        if pm.data_runs.len() as u64 >= pm.total_rows() {
            *pm.data_runs.get(logical_offset).ok_or_else(|| {
                Error::Vdb(format!(
                    "scalar row {row_id}: data_runs[{logical_offset}] missing"
                ))
            })? as usize
        } else {
            let mut seen = 0u64;
            let mut chosen: Option<usize> = None;
            for (i, &repeat) in pm.data_runs.iter().enumerate() {
                let end = seen + u64::from(repeat);
                if (logical_offset as u64) < end {
                    chosen = Some(i);
                    break;
                }
                seen = end;
            }
            chosen.ok_or_else(|| {
                Error::Vdb(format!(
                    "scalar row {row_id}: logical offset {logical_offset} outside data_runs"
                ))
            })?
        }
    } else {
        logical_offset
    };

    let bytes_per = (elem_bits / 8) as usize;
    let byte_off = data_idx * bytes_per;
    if byte_off + bytes_per > bytes.len() {
        return Err(Error::Vdb(format!(
            "scalar row {row_id}: byte offset {byte_off} past decoded {}",
            bytes.len()
        )));
    }
    let val = match elem_bits {
        32 => i64::from(i32::from_le_bytes([
            bytes[byte_off],
            bytes[byte_off + 1],
            bytes[byte_off + 2],
            bytes[byte_off + 3],
        ])),
        64 => i64::from_le_bytes([
            bytes[byte_off],
            bytes[byte_off + 1],
            bytes[byte_off + 2],
            bytes[byte_off + 3],
            bytes[byte_off + 4],
            bytes[byte_off + 5],
            bytes[byte_off + 6],
            bytes[byte_off + 7],
        ]),
        _ => return Err(Error::Vdb(format!("unsupported elem_bits {elem_bits}"))),
    };
    Ok(val)
}

/// Interpret a decoded blob's `data` as compressed integers and return the
/// uncompressed byte buffer.
///
/// VDB envelopes wrap integer payloads under three transforms: irzip
/// (identified by a non-empty `ops` plane mask on the header frame), plain
/// zip_encoding (header has empty `ops` — data is deflate or raw depending
/// on whether `data.len() == osize`), or legacy izip (no header frame at
/// all — version 0).
fn decode_integer_bytes(decoded: &DecodedBlob<'_>, elem_bits: u32) -> Result<Vec<u8>> {
    let hdr = decoded.headers.first();
    let osize = hdr.map(|h| h.osize as usize).unwrap_or(decoded.data.len());
    let expected_bytes = osize;

    // irzip: has plane mask in ops.
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

    // zip_encoding (hdr with empty ops) or no header:
    //   - if data.len() == osize the payload was stored raw (deflate didn't help),
    //   - else it's a raw-deflate stream that decompresses to osize bytes.
    if decoded.data.len() == expected_bytes {
        return Ok(decoded.data.to_vec());
    }
    if let Ok(out) = blob::deflate_decompress(&decoded.data, expected_bytes)
        && out.len() == expected_bytes
    {
        return Ok(out);
    }

    // Legacy fallback: try izip (v0 blobs without transform header).
    if hdr.is_none() && !decoded.data.is_empty() {
        let num_elems = (expected_bytes as u32) / (elem_bits / 8);
        return blob::izip_decode(&decoded.data, elem_bits, num_elems);
    }

    Err(Error::Vdb(format!(
        "alignment column: no decoder succeeded (elem_bits={elem_bits}, data.len={}, osize={osize})",
        decoded.data.len()
    )))
}
