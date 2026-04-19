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

use crate::blob::{self, DecodedBlob, PageMap};
use crate::error::{Error, Result};
use crate::kdb::ColumnReader;

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
    /// `page_map.data_record_lengths()` cached once (empty if no page map).
    /// Unit matches what the column's encoding stores: bits for
    /// bit-packed rows, bytes for byte rows, elements for integer rows,
    /// bases for 2na rows.
    pub record_lens: Vec<u32>,
    /// `record_prefix[i] = sum(record_lens[0..i]) as u64`, length = `record_lens.len() + 1`.
    /// Lets row slicing be O(1) instead of the previous O(n²) `take(i).sum()`.
    pub record_prefix: Vec<u64>,
    /// Per-logical-row rec_idx (honouring `page_map.data_runs`). Empty
    /// when the mapping is identity (no `data_runs`).
    pub logical_to_rec: Vec<u32>,
}

impl DecodedColumnBlob {
    /// Map a blob-relative row offset to the rec_idx of its unique value.
    pub fn rec_idx(&self, logical_offset: usize) -> Result<usize> {
        if self.logical_to_rec.is_empty() {
            return Ok(logical_offset);
        }
        self.logical_to_rec
            .get(logical_offset)
            .map(|&v| v as usize)
            .ok_or_else(|| {
                Error::Format(format!(
                    "logical offset {logical_offset} out of bounds ({} rows)",
                    self.logical_to_rec.len()
                ))
            })
    }
}

/// A `ColumnReader` plus a single-slot cache of its most-recent decoded blob.
///
/// Not thread-safe — expects to be owned by a single-threaded consumer
/// (cSRA decode spawns one `CsraCursor` per rayon worker).
pub(crate) struct CachedColumn {
    col: ColumnReader,
    kind: ColumnKind,
    cache: RefCell<Option<DecodedColumnBlob>>,
}

impl CachedColumn {
    pub fn new(col: ColumnReader, kind: ColumnKind) -> Self {
        Self {
            col,
            kind,
            cache: RefCell::new(None),
        }
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
            let borrow = self.cache.borrow();
            if let Some(cached) = borrow.as_ref()
                && cached.start_id == start_id
            {
                return f(cached, logical_offset);
            }
        }

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
        let record_lens: Vec<u32> = page_map
            .as_ref()
            .map(|pm| pm.data_record_lengths())
            .unwrap_or_default();
        let record_prefix = compute_prefix(&record_lens);
        let logical_to_rec = page_map
            .as_ref()
            .map(compute_logical_to_rec)
            .unwrap_or_default();

        *self.cache.borrow_mut() = Some(DecodedColumnBlob {
            start_id,
            bytes,
            page_map,
            record_lens,
            record_prefix,
            logical_to_rec,
        });

        let borrow = self.cache.borrow();
        let cached = borrow.as_ref().expect("just inserted");
        f(cached, logical_offset)
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
            let rec_idx = blob.rec_idx(logical_offset)?;
            let start_bits = blob.record_prefix[rec_idx] as usize;
            let len_bits = blob.record_lens[rec_idx] as usize;
            let mut out = Vec::with_capacity(len_bits);
            for i in 0..len_bits {
                let bit_idx = start_bits + i;
                let byte = bit_idx / 8;
                let bit = 7 - (bit_idx % 8);
                let b = blob.bytes.get(byte).copied().ok_or_else(|| {
                    Error::Format(format!("bool row {row_id}: bit {bit_idx} past payload"))
                })?;
                out.push((b >> bit) & 1);
            }
            Ok(out)
        })
    }

    /// Variable-length byte record (MISMATCH, READ_TYPE, QUALITY).
    pub fn read_byte_row(&self, row_id: i64) -> Result<Vec<u8>> {
        debug_assert!(matches!(self.kind, ColumnKind::Zip));
        self.with_blob(row_id, |blob, logical_offset| {
            let rec_idx = blob.rec_idx(logical_offset)?;
            let start = blob.record_prefix[rec_idx] as usize;
            let len = blob.record_lens[rec_idx] as usize;
            let end = start + len;
            if end > blob.bytes.len() {
                return Err(Error::Format(format!(
                    "byte row {row_id}: slice [{start}..{end}] past payload {}",
                    blob.bytes.len()
                )));
            }
            Ok(blob.bytes[start..end].to_vec())
        })
    }

    /// Variable-length i32 record (REF_OFFSET).
    pub fn read_i32_row(&self, row_id: i64) -> Result<Vec<i32>> {
        debug_assert!(matches!(self.kind, ColumnKind::Irzip { elem_bits: 32 }));
        self.with_blob(row_id, |blob, logical_offset| {
            let rec_idx = blob.rec_idx(logical_offset)?;
            let start = (blob.record_prefix[rec_idx] as usize) * 4;
            let len_elems = blob.record_lens[rec_idx] as usize;
            let end = start + len_elems * 4;
            if end > blob.bytes.len() {
                return Err(Error::Format(format!(
                    "i32 row {row_id}: slice [{start}..{end}] past payload {}",
                    blob.bytes.len()
                )));
            }
            Ok(blob.bytes[start..end]
                .chunks_exact(4)
                .map(|c| i32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                .collect())
        })
    }

    /// Variable-length u32 record (SEQUENCE.READ_LEN).
    pub fn read_u32_row(&self, row_id: i64) -> Result<Vec<u32>> {
        debug_assert!(matches!(self.kind, ColumnKind::Irzip { elem_bits: 32 }));
        self.with_blob(row_id, |blob, logical_offset| {
            let rec_idx = blob.rec_idx(logical_offset)?;
            let start = (blob.record_prefix[rec_idx] as usize) * 4;
            let len_elems = blob.record_lens[rec_idx] as usize;
            let end = start + len_elems * 4;
            if end > blob.bytes.len() {
                return Err(Error::Format(format!(
                    "u32 row {row_id}: slice [{start}..{end}] past payload {}",
                    blob.bytes.len()
                )));
            }
            Ok(blob.bytes[start..end]
                .chunks_exact(4)
                .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                .collect())
        })
    }

    /// Variable-length i64 record (SEQUENCE.PRIMARY_ALIGNMENT_ID).
    pub fn read_i64_row(&self, row_id: i64) -> Result<Vec<i64>> {
        debug_assert!(matches!(self.kind, ColumnKind::Irzip { elem_bits: 64 }));
        self.with_blob(row_id, |blob, logical_offset| {
            let rec_idx = blob.rec_idx(logical_offset)?;
            let start = (blob.record_prefix[rec_idx] as usize) * 8;
            let len_elems = blob.record_lens[rec_idx] as usize;
            let end = start + len_elems * 8;
            if end > blob.bytes.len() {
                return Err(Error::Format(format!(
                    "i64 row {row_id}: slice [{start}..{end}] past payload {}",
                    blob.bytes.len()
                )));
            }
            Ok(blob.bytes[start..end]
                .chunks_exact(8)
                .map(|c| i64::from_le_bytes([c[0], c[1], c[2], c[3], c[4], c[5], c[6], c[7]]))
                .collect())
        })
    }

    /// Scalar fixed-width integer with optional `data_runs` RLE (used by
    /// `GLOBAL_REF_START`, `REF_LEN`, `REFERENCE.SEQ_LEN`). The blob
    /// payload holds only the unique values; `rec_idx(offset)` resolves
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
            let rec_idx = blob.rec_idx(logical_offset)?;
            let bytes_per = (elem_bits / 8) as usize;
            let byte_off = rec_idx * bytes_per;
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

    /// 2na-packed variable-length record, unpacked into 4na-bin bytes
    /// (one nibble per byte). Used for `REFERENCE.CMP_READ` and
    /// `SEQUENCE.CMP_READ`.
    pub fn read_2na_row(&self, row_id: i64) -> Result<Vec<u8>> {
        debug_assert!(matches!(self.kind, ColumnKind::TwoNa));
        self.with_blob(row_id, |blob, logical_offset| {
            let pm = blob.page_map.as_ref().ok_or_else(|| {
                Error::Format("2na column: page_map required for record boundaries".into())
            })?;
            let _ = pm; // presence-only check; record_lens already cached.
            let rec_idx = blob.rec_idx(logical_offset)?;
            let len_bases = blob.record_lens[rec_idx] as usize;
            if len_bases == 0 {
                return Ok(Vec::new());
            }
            let start_bits = (blob.record_prefix[rec_idx] as usize) * 2;

            // 2na → 4na nibble lookup. Bases are MSB-first within each
            // byte (verified by cross-check against vdb-dump on
            // HAS_MISMATCH row 1 of VDB-3418).
            const LUT_2NA_TO_4NA: [u8; 4] = [0x1, 0x2, 0x4, 0x8]; // A C G T
            let mut out = Vec::with_capacity(len_bases);
            for i in 0..len_bases {
                let bit_idx = start_bits + i * 2;
                let byte = bit_idx / 8;
                let shift = 6 - (bit_idx % 8);
                let b = blob.bytes.get(byte).copied().ok_or_else(|| {
                    Error::Format(format!("2na row {row_id}: bit {bit_idx} past payload"))
                })?;
                let code = (b >> shift) & 0x03;
                out.push(LUT_2NA_TO_4NA[code as usize]);
            }
            Ok(out)
        })
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

fn compute_prefix(record_lens: &[u32]) -> Vec<u64> {
    let mut prefix = Vec::with_capacity(record_lens.len() + 1);
    let mut acc: u64 = 0;
    prefix.push(0);
    for &l in record_lens {
        acc += u64::from(l);
        prefix.push(acc);
    }
    prefix
}

fn compute_logical_to_rec(pm: &PageMap) -> Vec<u32> {
    if pm.data_runs.is_empty() {
        return Vec::new();
    }
    let total = pm.total_rows();
    // Random-access variant: data_runs[i] is the rec_idx for logical row i.
    if (pm.data_runs.len() as u64) >= total {
        return pm.data_runs.clone();
    }
    // Run-length variant: data_runs[i] is the repeat count for rec_idx i.
    let mut out = Vec::with_capacity(total as usize);
    for (i, &repeat) in pm.data_runs.iter().enumerate() {
        for _ in 0..repeat {
            out.push(i as u32);
        }
    }
    out
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
        let num_elems = (osize as u32) / (elem_bits / 8);
        return blob::izip_decode(&decoded.data, elem_bits, num_elems);
    }

    Err(Error::Format(format!(
        "integer column: no decoder succeeded (elem_bits={elem_bits}, data.len={}, osize={osize})",
        decoded.data.len()
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::PageMap;

    fn pm(data_runs: Vec<u32>, lengths: Vec<u32>, leng_runs: Vec<u32>) -> PageMap {
        PageMap {
            data_recs: data_runs.len() as u64,
            lengths,
            leng_runs,
            data_runs,
        }
    }

    #[test]
    fn prefix_sum_empty() {
        let p = compute_prefix(&[]);
        assert_eq!(p, vec![0]);
    }

    #[test]
    fn prefix_sum_basic() {
        let p = compute_prefix(&[3, 5, 2]);
        assert_eq!(p, vec![0, 3, 8, 10]);
    }

    #[test]
    fn logical_to_rec_empty_is_identity() {
        let map = compute_logical_to_rec(&pm(vec![], vec![1], vec![1]));
        assert!(
            map.is_empty(),
            "no data_runs → identity mapping (empty vec)"
        );
    }

    #[test]
    fn logical_to_rec_run_length() {
        // data_runs = [3, 2, 4] → rows 0..3 → rec 0, 3..5 → rec 1, 5..9 → rec 2.
        let map = compute_logical_to_rec(&pm(vec![3, 2, 4], vec![1], vec![9]));
        assert_eq!(map, vec![0, 0, 0, 1, 1, 2, 2, 2, 2]);
    }

    #[test]
    fn logical_to_rec_random_access() {
        // data_runs has one entry per logical row → treat as rec_idx lookup.
        // total_rows (leng_runs sum) = 4; data_runs.len()=4 → random-access.
        let dr = vec![2, 0, 1, 3];
        let map = compute_logical_to_rec(&pm(dr.clone(), vec![1], vec![4]));
        assert_eq!(map, dr);
    }

    #[test]
    fn decoded_rec_idx_identity() {
        let blob = DecodedColumnBlob {
            start_id: 0,
            bytes: Vec::new(),
            page_map: None,
            record_lens: Vec::new(),
            record_prefix: vec![0],
            logical_to_rec: Vec::new(),
        };
        assert_eq!(blob.rec_idx(0).unwrap(), 0);
        assert_eq!(blob.rec_idx(42).unwrap(), 42);
    }

    #[test]
    fn decoded_rec_idx_lookup() {
        let blob = DecodedColumnBlob {
            start_id: 0,
            bytes: Vec::new(),
            page_map: None,
            record_lens: Vec::new(),
            record_prefix: vec![0],
            logical_to_rec: vec![0, 0, 1, 1, 2],
        };
        assert_eq!(blob.rec_idx(0).unwrap(), 0);
        assert_eq!(blob.rec_idx(2).unwrap(), 1);
        assert_eq!(blob.rec_idx(4).unwrap(), 2);
        assert!(blob.rec_idx(5).is_err());
    }
}
