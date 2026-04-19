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

use crate::blob::{self, DecodedBlob};
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
    /// 2na-packed base bytes per chunk (ASCII / 4na_bin externally via the
    /// schema, but the physical bits are 2na; we unpack on read).
    cmp_read: ColumnReader,
    /// Real chunk length in bases (≤ `max_seq_len`). Stored as u32 izip.
    seq_len: ColumnReader,
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
            cmp_read,
            seq_len,
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
    /// low nibble populated). Spans chunk boundaries as needed.
    pub fn fetch_span(&self, global_ref_start: u64, ref_len: u32) -> Result<Vec<u8>> {
        let msl = u64::from(self.max_seq_len);
        let mut remaining = ref_len as usize;
        let mut chunk_row = (global_ref_start / msl) as i64 + 1; // 1-based
        let mut offset_in_chunk = (global_ref_start % msl) as usize;

        let mut out = Vec::with_capacity(ref_len as usize);
        while remaining > 0 {
            let chunk_len = self.read_chunk_len(chunk_row)?;
            if offset_in_chunk > chunk_len {
                return Err(Error::Format(format!(
                    "reference: offset {offset_in_chunk} past chunk {chunk_row} len {chunk_len}"
                )));
            }
            let chunk_bases = self.read_chunk_bases(chunk_row, chunk_len)?;
            let available = chunk_len - offset_in_chunk;
            let take = remaining.min(available);
            out.extend_from_slice(&chunk_bases[offset_in_chunk..offset_in_chunk + take]);
            remaining -= take;
            chunk_row += 1;
            offset_in_chunk = 0;
        }
        Ok(out)
    }

    /// SEQ_LEN for one REFERENCE row.
    ///
    /// The column stores only the *unique* lengths (e.g. `[5000, 1620, …]`
    /// for REFERENCE rows where almost every chunk is 5000 and one tail
    /// chunk is 1620). `page_map.data_runs[logical_row]` is the 0-based
    /// index into those unique values. See `pipeline::blob_decode::
    /// expand_via_page_map` for the same dispatch on SEQUENCE-side ints.
    fn read_chunk_len(&self, row_id: i64) -> Result<usize> {
        let blob = self
            .seq_len
            .find_blob(row_id)
            .ok_or_else(|| Error::Format(format!("SEQ_LEN: no blob for row {row_id}")))?;
        let raw = self.seq_len.read_raw_blob_slice(row_id)?;
        let decoded = blob::decode_blob(
            raw,
            self.seq_len.meta().checksum_type,
            u64::from(blob.id_range),
            32,
        )?;
        let bytes = decode_integer_bytes(&decoded, 32)?;

        let logical_offset = (row_id - blob.start_id) as usize;
        let data_idx = if let Some(pm) = &decoded.page_map
            && !pm.data_runs.is_empty()
        {
            if pm.data_runs.len() as u64 >= pm.total_rows() {
                // Random-access variant: data_runs[i] is the unique-value
                // index for logical row i.
                *pm.data_runs.get(logical_offset).ok_or_else(|| {
                    Error::Format(format!(
                        "SEQ_LEN row {row_id}: data_runs[{logical_offset}] missing"
                    ))
                })? as usize
            } else {
                // Run-length variant: data_runs[i] is the repeat count for
                // unique value i (see PageMap::expand_data_runs).
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
                    Error::Format(format!(
                        "SEQ_LEN row {row_id}: logical offset {logical_offset} outside data_runs coverage"
                    ))
                })?
            }
        } else {
            logical_offset
        };
        let byte_off = data_idx * 4;
        if byte_off + 4 > bytes.len() {
            return Err(Error::Format(format!(
                "SEQ_LEN row {row_id}: data_idx {data_idx} × 4 = {byte_off} past decoded {}",
                bytes.len()
            )));
        }
        let val = u32::from_le_bytes([
            bytes[byte_off],
            bytes[byte_off + 1],
            bytes[byte_off + 2],
            bytes[byte_off + 3],
        ]);
        Ok(val as usize)
    }

    /// Read one REFERENCE row's CMP_READ, 2na-unpacked into 4na-bin bytes
    /// of length `chunk_len`. The physical column is 2na-packed (4 bases
    /// per byte) with whole-chunk boundaries in the page_map.
    fn read_chunk_bases(&self, row_id: i64, chunk_len: usize) -> Result<Vec<u8>> {
        let blob = self
            .cmp_read
            .find_blob(row_id)
            .ok_or_else(|| Error::Format(format!("CMP_READ: no blob for row {row_id}")))?;
        let raw = self.cmp_read.read_raw_blob_slice(row_id)?;
        let decoded = blob::decode_blob(
            raw,
            self.cmp_read.meta().checksum_type,
            u64::from(blob.id_range),
            2,
        )?;
        let pm = decoded
            .page_map
            .as_ref()
            .ok_or_else(|| Error::Format("REFERENCE.CMP_READ: page_map required".into()))?;
        let record_lens = pm.data_record_lengths();

        let row_offset = (row_id - blob.start_id) as usize;
        if row_offset >= record_lens.len() {
            return Err(Error::Format(format!(
                "REFERENCE.CMP_READ row {row_id}: record index {row_offset} past {}",
                record_lens.len()
            )));
        }
        let rec_len_bases = record_lens[row_offset] as usize;
        if rec_len_bases != chunk_len {
            return Err(Error::Format(format!(
                "REFERENCE.CMP_READ row {row_id}: page_map says {rec_len_bases} bases, \
                 SEQ_LEN says {chunk_len}"
            )));
        }

        // All chunks sit in the decoded data back-to-back at 2 bits per base.
        let start_bits: usize = record_lens
            .iter()
            .take(row_offset)
            .map(|&n| n as usize * 2)
            .sum();
        let len_bits = rec_len_bases * 2;

        // Data is 2na bytes with 4 bases per byte, MSB-first ordering
        // (verified against vdb-dump on HAS_MISMATCH; reusing the same
        // convention here until we add a REFERENCE.CMP_READ parity test).
        let lut_2na_to_4na = [0x1u8, 0x2, 0x4, 0x8]; // A C G T → 4na bins
        let mut out = Vec::with_capacity(rec_len_bases);
        for i in 0..rec_len_bases {
            let bit_idx = start_bits + i * 2;
            let byte = bit_idx / 8;
            let shift = 6 - (bit_idx % 8);
            let b = decoded.data.get(byte).copied().ok_or_else(|| {
                Error::Format(format!(
                    "REFERENCE.CMP_READ row {row_id}: bit {bit_idx} past payload"
                ))
            })?;
            let code = (b >> shift) & 0x03;
            out.push(lut_2na_to_4na[code as usize]);
        }
        if len_bits == 0 {
            // suppress unused-variable lint if chunk is empty
        }
        Ok(out)
    }
}

/// Integer column decode dispatch (shared semantics with alignment.rs).
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
    Err(Error::Format(format!(
        "reference column: no decoder succeeded (elem_bits={elem_bits}, data.len={}, osize={osize})",
        decoded.data.len()
    )))
}
