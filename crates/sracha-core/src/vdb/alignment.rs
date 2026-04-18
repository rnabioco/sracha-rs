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

/// Handle to the PRIMARY_ALIGNMENT table columns we consume during cSRA decode.
pub struct AlignmentCursor {
    /// Absolute reference position (across concatenated references) where the
    /// alignment starts.
    global_ref_start: ColumnReader,
    /// Number of reference bases covered by the alignment (alignment span).
    ref_len: ColumnReader,
    /// Packed-bit column: `true` when the read was reverse-aligned.
    ref_orientation: ColumnReader,
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
        let ref_orientation = open(archive, "REF_ORIENTATION")?;
        let has_mismatch = open(archive, "HAS_MISMATCH")?;
        let has_ref_offset = open(archive, "HAS_REF_OFFSET")?;
        let mismatch = open(archive, "MISMATCH")?;
        let ref_offset = open(archive, "REF_OFFSET")?;

        let first_row = global_ref_start.first_row_id().unwrap_or(1);
        let row_count = global_ref_start.row_count();

        Ok(Self {
            global_ref_start,
            ref_len,
            ref_orientation,
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

    pub fn ref_orientation_col(&self) -> &ColumnReader {
        &self.ref_orientation
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
        let values = decode_u_column(col, row_id, 64)?;
        let idx = row_offset_in_blob(col, row_id)?;
        values
            .get(idx)
            .copied()
            .ok_or_else(|| Error::Vdb(format!("row {row_id} out of range in u64 column")))
    }

    /// Read a single u32 fixed-length column value for `row_id`.
    fn read_u32(col: &ColumnReader, row_id: i64) -> Result<u32> {
        let values = decode_u_column(col, row_id, 32)?;
        let idx = row_offset_in_blob(col, row_id)?;
        values
            .get(idx)
            .copied()
            .map(|v| v as u32)
            .ok_or_else(|| Error::Vdb(format!("row {row_id} out of range in u32 column")))
    }

    /// Return the alignment row 1's GLOBAL_REF_START — smoke-test hook for
    /// wiring verification during early Phase 1 development.
    pub fn first_global_ref_start(&self) -> Result<u64> {
        Self::read_u64(&self.global_ref_start, self.first_row)
    }

    pub fn first_ref_len(&self) -> Result<u32> {
        Self::read_u32(&self.ref_len, self.first_row)
    }
}

/// Row's 0-based offset within the decoded blob array for its column.
fn row_offset_in_blob(col: &ColumnReader, row_id: i64) -> Result<usize> {
    let blob = col
        .find_blob(row_id)
        .ok_or_else(|| Error::Vdb(format!("no blob for row {row_id}")))?;
    Ok((row_id - blob.start_id) as usize)
}

/// Decode an integer column's blob that contains `row_id` into a Vec of u64
/// values. Handles irzip (headers v1+) and izip (v0) decode paths.
fn decode_u_column(col: &ColumnReader, row_id: i64, elem_bits: u32) -> Result<Vec<u64>> {
    let blob = col
        .find_blob(row_id)
        .ok_or_else(|| Error::Vdb(format!("no blob for row {row_id}")))?;
    let raw = col.read_raw_blob_slice(row_id)?;
    let decoded = blob::decode_blob(
        raw,
        col.meta().checksum_type,
        u64::from(blob.id_range),
        elem_bits,
    )?;
    let bytes = decode_integer_bytes(&decoded, elem_bits)?;
    bytes_to_u64_vec(&bytes, elem_bits)
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

/// Convert a decoded integer byte buffer (little-endian) into a Vec<u64>.
fn bytes_to_u64_vec(bytes: &[u8], elem_bits: u32) -> Result<Vec<u64>> {
    match elem_bits {
        32 => {
            if !bytes.len().is_multiple_of(4) {
                return Err(Error::Vdb(format!(
                    "expected multiple-of-4 bytes for u32, got {}",
                    bytes.len()
                )));
            }
            Ok(bytes
                .chunks_exact(4)
                .map(|c| u64::from(u32::from_le_bytes([c[0], c[1], c[2], c[3]])))
                .collect())
        }
        64 => {
            if !bytes.len().is_multiple_of(8) {
                return Err(Error::Vdb(format!(
                    "expected multiple-of-8 bytes for u64, got {}",
                    bytes.len()
                )));
            }
            Ok(bytes
                .chunks_exact(8)
                .map(|c| u64::from_le_bytes([c[0], c[1], c[2], c[3], c[4], c[5], c[6], c[7]]))
                .collect())
        }
        _ => Err(Error::Vdb(format!(
            "unsupported elem_bits {elem_bits} for bytes_to_u64_vec"
        ))),
    }
}
