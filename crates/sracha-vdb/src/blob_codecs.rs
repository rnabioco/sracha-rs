//! High-level decoders that compose on top of [`crate::blob`] primitives.
//!
//! These helpers take the raw per-blob byte slice, validate the trailing
//! checksum (via [`blob::decode_blob`]), and dispatch to the correct
//! decompression or integer-decode pipeline. Shared between the fastq
//! pipeline (`sracha-core`) and the `sracha vdb dump` command — every
//! codec here operates on already-parsed [`blob::DecodedBlob`] structures
//! so callers are free to inspect page maps, envelopes, etc. independently.

use crate::blob;
use crate::error::{Error, Result};

/// Decode a raw blob, validating the trailing CRC32/MD5 checksum and
/// stripping envelope/headers/page_map.
///
/// The blob locator `size` field includes trailing checksum bytes, which
/// [`blob::decode_blob`] checks against the on-disk data before returning.
/// A mismatch surfaces as [`Error::BlobIntegrity`] so callers can abort
/// rather than produce wrong reads.
pub fn decode_raw(raw: &[u8], checksum_type: u8, row_count: u64) -> Result<blob::DecodedBlob<'_>> {
    blob::decode_blob(raw, checksum_type, row_count, 8)
}

/// Decode irzip-compressed integers from a blob, detecting single vs dual
/// series via the transform header's argument count.
///
/// Output is a byte-aligned `u32` stream (little-endian). For columns like
/// `READ_LEN`, `READ_START`, `X`, and `Y` the caller reinterprets these
/// bytes as `u32`/`i32` values.
pub fn decode_irzip_column(decoded: &blob::DecodedBlob<'_>) -> Result<Vec<u8>> {
    let hdr_version = decoded.headers.first().map(|h| h.version).unwrap_or(0);
    let decoded_ints = if hdr_version >= 1 {
        let hdr = &decoded.headers[0];
        // Raw passthrough case: when an iunzip-eligible blob compresses to
        // `osize == data_len` bytes with no ops/args, the reference encoder
        // skipped the bit-plane step — the data is the output verbatim.
        // This pattern shows up on ENA-origin long-read runs (ERR15141550,
        // issue #20) whose small-n blobs bypass plane encoding entirely.
        // Without this check we'd fall through to `irzip_decode` with
        // `planes = 0xFF` (our bogus default) and fail to deflate-decode
        // the raw integer bytes as a compressed stream.
        if hdr.ops.is_empty() && hdr.args.is_empty() && hdr.osize as usize == decoded.data.len() {
            decoded.data.to_vec()
        } else {
            let planes = hdr.ops.first().copied().unwrap_or(0xFF);
            let min = hdr.args.first().copied().unwrap_or(0);
            let slope = hdr.args.get(1).copied().unwrap_or(0);
            let num_elems = (hdr.osize as u32) / 4;
            // Dual-series (irzip v3): 4 args = min[0], slope[0], min[1], slope[1].
            let series2 = hdr
                .args
                .get(2)
                .and_then(|&min2| hdr.args.get(3).map(|&slope2| (min2, slope2)));
            blob::irzip_decode(&decoded.data, 32, num_elems, min, slope, planes, series2)?
        }
    } else {
        blob::izip_decode(&decoded.data, 32)?
    };
    expand_via_page_map(decoded_ints, &decoded.page_map)
}

/// Expand decoded integer data to one entry per logical row via the page map.
///
/// For columns like X, Y, and READ_LEN, the irzip/izip decoder produces
/// unique data entries and the page map says which entry each row uses.
///
/// Offsets in a random-access page map are *element* offsets: ncbi-vdb's
/// `data_offset` is an `elem_count_t` indexed into the blob's element stream
/// (`interfaces/kdb/page-map.h:77`), consumed by `PageMapIteratorDataOffset`
/// and `KDataBufferSub`, both of which take element counts. Earlier revisions
/// guessed between that and an "entry index" reading by testing which one fit
/// the decoded buffer, which silently picked wrong whenever both fit.
pub fn expand_via_page_map(
    decoded_ints: Vec<u8>,
    page_map: &Option<blob::PageMap>,
) -> Result<Vec<u8>> {
    let Some(pm) = page_map else {
        return Ok(decoded_ints);
    };
    if pm.mapping.is_identity() {
        return Ok(decoded_ints);
    }
    pm.expand_rows(&decoded_ints, 4)
}

/// Decode a zip_encoding data section.
///
/// The blob header tells us the version. Version 1 = raw deflate,
/// byte-aligned output. Version 2 = raw deflate with trailing-bits argument.
/// No headers (v1 blob) = the data is already the raw-deflate stream or
/// uncompressed.
///
/// When a compression header is present (hdr_version >= 1), both deflate
/// and zlib failing is treated as an error — silently returning the still-
/// compressed bytes would produce corrupt downstream output. For v0 blobs
/// (no headers), the raw-bytes fallback remains, since those are often
/// already-uncompressed payloads.
pub fn decode_zip_encoding(decoded: &blob::DecodedBlob<'_>) -> Result<Vec<u8>> {
    let hdr_version = decoded.headers.first().map(|h| h.version).unwrap_or(0);

    if decoded.data.is_empty() {
        return Ok(Vec::new());
    }

    let osize = decoded
        .headers
        .first()
        .map(|h| h.osize as usize)
        .filter(|&s| s > 0);
    let estimated = osize.unwrap_or(decoded.data.len() * 4);

    // Raw passthrough: when the encoder skipped compression (ops/args empty
    // and the header's osize matches the on-disk byte count), the payload
    // is already the uncompressed data. This shows up on small ALTREAD
    // blobs (DRR019046 blob 160 — 16 raw 4na bytes for 32,768 rows whose
    // page_map expands them into one trailing N per spot) where deflate
    // and zlib would both fail and the previous size-only fallback only
    // covered ≤12-byte payloads, silently dropping every N. Mirrors the
    // pattern already used by `decode_irzip_column`.
    if hdr_version >= 1
        && let Some(hdr) = decoded.headers.first()
        && hdr.ops.is_empty()
        && hdr.args.is_empty()
        && hdr.osize as usize == decoded.data.len()
    {
        return Ok(decoded.data.to_vec());
    }

    if let Ok(mut out) = blob::deflate_decompress(&decoded.data, estimated)
        && !out.is_empty()
    {
        if hdr_version == 2
            && let Some(trailing_bits) = decoded.headers.first().and_then(|h| h.args.first())
        {
            let total_bits = out.len() as i64 * 8;
            let actual_bits = total_bits - (8 - trailing_bits);
            let actual_bytes = ((actual_bits + 7) / 8) as usize;
            out.truncate(actual_bytes);
        }
        if let Some(expected) = osize
            && out.len() != expected
        {
            tracing::debug!(
                "zip_encoding: decompressed {} bytes, header osize={}",
                out.len(),
                expected,
            );
        }
        return Ok(out);
    }

    if let Ok(out2) = blob::zlib_decompress(&decoded.data, estimated)
        && !out2.is_empty()
    {
        return Ok(out2);
    }

    if hdr_version >= 1 {
        // Very small payloads (under the ~12-byte deflate/zlib minimum) can't
        // realistically be compressed — any such bytes are the raw data. NCBI
        // writes these for tiny ALTREAD / secondary-column blobs where the
        // compression header is set but the payload skips compression because
        // it's too short to benefit.
        if decoded.data.len() <= 12 {
            tracing::debug!(
                "zip_encoding v{hdr_version}: treating {}-byte payload as raw \
                 (below deflate/zlib minimum)",
                decoded.data.len(),
            );
            return Ok(decoded.data.to_vec());
        }
        return Err(Error::Format(format!(
            "zip_encoding v{hdr_version}: both deflate and zlib failed on {}-byte payload",
            decoded.data.len(),
        )));
    }

    Ok(decoded.data.to_vec())
}

/// Decode `NCBI:SRA:qual4_encode` output to four log-odds channels per base.
///
/// A byte-oriented codebook over whole 4-tuples, emitted one variable-length
/// code per base (`libs/sraxf/qual4_decode.c`, codebook in `qual4_codec.h`):
///
/// | leading byte | bytes | quad |
/// |---|---|---|
/// | `0..=80` | 4 | literal `[b0-40, b1-40, b2-40, b3-40]` |
/// | `81` | 1 | `[-5, -5, -5, -5]` — the `N` marker |
/// | `82` | 1 | `[qmax, qmin, qmin, qmin]` |
/// | `83..=91` | 2 | `[v, ..]` with one slot set from `v`, rest `qmin` |
/// | `92..=255` | — | malformed |
///
/// For the pattern codes `v = b1 - 40`, and the non-`qmin` slot holds `-v`
/// (83-85), `-v + 1` (86-88) or `-v - 1` (89-91); which slot is 1, 2 or 3 as
/// the code cycles.
///
/// Channel 0 is the *called* base's quality — the stored order is "swapped",
/// with index 0 exchanged with the called base's 2na code. Recovering A/C/G/T
/// order would need the basecalls, but the phred column is `cut<0>`, so this
/// decoder needs no READ input.
///
/// Value bytes are not range-checked by the reference; arithmetic wraps at
/// `i8`. Mirrored here so corrupt input decodes bit-identically rather than
/// saturating or panicking.
pub fn qual4_decode(src: &[u8], dcount: usize, qmin: i8, qmax: i8) -> Result<Vec<u8>> {
    const KNOWN_BAD: u8 = 81;
    const KNOWN_GOOD: u8 = 82;
    const PATTERN_FIRST: u8 = 83;
    const PATTERN_LAST: u8 = 91;

    let out_len = dcount
        .checked_mul(4)
        .ok_or_else(|| Error::Format("qual4: output size overflows".into()))?;
    let mut out = vec![0u8; out_len];
    let mut j = 0usize;
    let mut st = 0u8;
    let mut pending = 0u8;

    for &b in src {
        if j >= dcount {
            break;
        }
        let val = b.wrapping_sub(40) as i8;
        let q = j * 4;
        match st {
            0 => {
                if b < KNOWN_BAD {
                    out[q] = val as u8;
                    st = 1;
                } else if b == KNOWN_BAD {
                    out[q..q + 4].copy_from_slice(&[(-5i8) as u8; 4]);
                } else if b == KNOWN_GOOD {
                    out[q] = qmax as u8;
                    out[q + 1] = qmin as u8;
                    out[q + 2] = qmin as u8;
                    out[q + 3] = qmin as u8;
                } else {
                    pending = b;
                    st = 4;
                }
            }
            1 => {
                out[q + 1] = val as u8;
                st = 2;
            }
            2 => {
                out[q + 2] = val as u8;
                st = 3;
            }
            3 => {
                out[q + 3] = val as u8;
                st = 0;
            }
            _ => {
                if !(PATTERN_FIRST..=PATTERN_LAST).contains(&pending) {
                    return Err(Error::Format(format!(
                        "qual4: unknown codebook byte {pending}"
                    )));
                }
                let idx = (pending - PATTERN_FIRST) as usize;
                let v = val as i32;
                let other = match idx / 3 {
                    0 => -v,
                    1 => -v + 1,
                    _ => -v - 1,
                };
                let slot = idx % 3 + 1;
                out[q] = val as u8;
                out[q + 1] = qmin as u8;
                out[q + 2] = qmin as u8;
                out[q + 3] = qmin as u8;
                out[q + slot] = (other as i8) as u8;
                st = 0;
            }
        }
        if st == 0 {
            j += 1;
        }
    }

    // The reference rejects the blob unless every requested quad was produced;
    // trailing input past `dcount` is ignored, an incomplete quad is not.
    if j != dcount {
        return Err(Error::Format(format!(
            "qual4: decoded {j} of {dcount} quads"
        )));
    }
    Ok(out)
}

/// Decode the QUALITY blob payload.
///
/// Three encodings appear in the wild and the blob does not name which one it
/// is, so the header is consulted first and the payload probed only after:
///
/// - `qual4_encoding` (`q4` Illumina schema) — a two-frame chain, `zip`
///   outside and the qual4 codebook inside. Recognised by the second header
///   frame's two ops, and collapsed to one phred byte per base here (#113).
/// - `izip_encoding` (srf-load-era Illumina) — byte-plane data whose plane
///   count and min/slope live in the blob header (#111).
/// - `zip_encoding` (modern Illumina) — deflate/zlib.
///
/// zlib streams always start with a `0x78` CMF byte for the window sizes NCBI
/// uses, so seeing one routes straight to [`decode_zip_encoding`] and skips
/// the iZip probe — that closes the path behind issue #30, where SRA-Lite
/// quality blobs were read as iZip with arbitrary `data_count` values.
pub fn decode_quality_encoding(decoded: &blob::DecodedBlob<'_>) -> Result<Vec<u8>> {
    if decoded.data.is_empty() {
        return decode_zip_encoding(decoded);
    }
    // `NCBI:SRA:qual4_encoding#1` is a two-stage chain — `zip` outside, the
    // qual4 codebook inside — so the blob carries two header frames:
    //
    //   frame0  version 1, no ops,      osize = inflated size
    //   frame1  version 0, ops=[a, b],  osize = 4 bytes per base
    //
    // Every other codec here reads `headers.first()` and assumes a single
    // transform, which left this column stopping after the inflate and
    // handing the intermediate `encoded_qual4` bytes out as phred (#113).
    // The ops are `qmin + 40` and `qmax + 40` (qual4_decode.c:176-188).
    if decoded.headers.len() >= 2
        && let inner = &decoded.headers[1]
        && inner.ops.len() == 2
        && inner.osize > 0
        && inner.osize % 4 == 0
    {
        let inflated = decode_zip_encoding(decoded)?;
        let qmin = (i32::from(inner.ops[0]) - 40) as i8;
        let qmax = (i32::from(inner.ops[1]) - 40) as i8;
        let dcount = (inner.osize / 4) as usize;
        if let Ok(q4) = qual4_decode(&inflated, dcount, qmin, qmax) {
            // Collapse to one phred byte per base here rather than handing
            // four channels upward. The page map counts *elements*, and every
            // caller expands it at one byte per element — a 4-byte element
            // silently mis-expands on any blob whose map is not identity
            // (which is most of them on this archive's blob 200).
            return Ok(crate::encoding::qual4_log_odds_to_phred(&q4));
        }
    }
    if decoded.data.first() == Some(&0x78) {
        return decode_zip_encoding(decoded);
    }
    // Header-driven irzip. `izip_encoding` quality (srf-load-era Illumina)
    // arrives with a v1+ compression header carrying the plane count in `ops`
    // and min/slope in `args`, exactly like the integer columns — the payload
    // is bit-plane encoded, not an izip container and not deflate. Probing it
    // as either yields plausible-looking garbage rather than an error, so the
    // header has to be consulted before the probes run. See
    // `decode_irzip_column`, which has always done this for X/Y/READ_LEN;
    // quality never did (#111).
    if let Some(hdr) = decoded.headers.first().filter(|h| h.version >= 1)
        && !(hdr.ops.is_empty() && hdr.args.is_empty())
    {
        let planes = hdr.ops.first().copied().unwrap_or(0xFF);
        let min = hdr.args.first().copied().unwrap_or(0);
        let slope = hdr.args.get(1).copied().unwrap_or(0);
        // Quality is one byte per element, so osize is the element count.
        let num_elems = hdr.osize as u32;
        if let Ok(q) = blob::irzip_decode(&decoded.data, 8, num_elems, min, slope, planes, None)
            && q.len() == hdr.osize as usize
        {
            return Ok(q);
        }
    }
    if let Ok(qdata) = blob::izip_decode(&decoded.data, 8)
        && !qdata.is_empty()
    {
        return Ok(qdata);
    }
    decode_zip_encoding(decoded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::{DecodedBlob, PageMap, RowMapping};
    use std::borrow::Cow;

    /// Build a minimal v1 blob: header byte with rls=3 (implicit row_length=1),
    /// big-endian=0, adjust=0, then `payload` bytes, then optional checksum.
    /// header = 0b0110_0000 = 0x60.
    fn build_v1_blob_crc32(payload: &[u8]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(1 + payload.len() + 4);
        buf.push(0x60);
        buf.extend_from_slice(payload);
        // CRC32 over envelope + data.
        let crc = crate::blob::ncbi_crc32(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf
    }

    #[test]
    fn decode_raw_empty_returns_empty_blob() {
        let got = decode_raw(&[], 0, 0).expect("empty is valid");
        assert!(got.data.is_empty());
        assert!(got.headers.is_empty());
        assert!(got.page_map.is_none());
    }

    #[test]
    fn decode_raw_unknown_checksum_type_errors() {
        let raw = [0x60, 0xAA];
        let err = decode_raw(&raw, 99, 1).expect_err("unknown checksum must error");
        assert!(matches!(err, Error::Format(_)), "got {err:?}");
    }

    #[test]
    fn decode_raw_too_short_for_crc32() {
        let raw = [0x01, 0x02]; // 2 bytes, but CRC32 needs 4
        let err = decode_raw(&raw, 1, 0).expect_err("short blob must error");
        assert!(matches!(err, Error::Format(_)), "got {err:?}");
    }

    #[test]
    fn decode_raw_valid_crc32_roundtrips() {
        let blob = build_v1_blob_crc32(&[0x11, 0x22, 0x33]);
        let got = decode_raw(&blob, 1, 3).expect("valid blob must decode");
        assert_eq!(&*got.data, &[0x11, 0x22, 0x33]);
    }

    #[test]
    fn decode_raw_crc32_mismatch_returns_integrity_error() {
        let mut blob = build_v1_blob_crc32(&[0x11, 0x22, 0x33]);
        let last = blob.len() - 1;
        blob[last] ^= 0x01; // flip a checksum bit
        let err = decode_raw(&blob, 1, 3).expect_err("bad CRC must error");
        assert!(
            matches!(err, Error::BlobIntegrity { kind: "CRC32", .. }),
            "got {err:?}"
        );
    }

    fn make_blob(data: Vec<u8>, page_map: Option<PageMap>) -> DecodedBlob<'static> {
        DecodedBlob {
            data: Cow::Owned(data),
            adjust: 0,
            big_endian: false,
            headers: vec![],
            page_map,
            row_length: None,
        }
    }

    #[test]
    fn expand_via_page_map_none_passthrough() {
        let out = expand_via_page_map(vec![1, 2, 3, 4], &None).unwrap();
        assert_eq!(out, vec![1, 2, 3, 4]);
    }

    #[test]
    fn expand_via_page_map_identity_passthrough() {
        let pm = PageMap {
            data_recs: 1,
            lengths: vec![1],
            leng_runs: vec![1],
            mapping: RowMapping::Identity,
        };
        let out = expand_via_page_map(vec![0xAA; 4], &Some(pm)).unwrap();
        assert_eq!(out, vec![0xAA; 4]);
    }

    #[test]
    fn expand_via_page_map_per_row_expansion_uses_repeat_counts() {
        // 2 records, each of row_length=1 u32. repeats=[1,3] means the
        // second record is replicated 3 times → 4 rows total × 4 bytes.
        let pm = PageMap {
            data_recs: 2,
            lengths: vec![1],
            leng_runs: vec![4],
            mapping: RowMapping::RepeatCounts(vec![1, 3]),
        };
        let mut data = Vec::new();
        data.extend_from_slice(&1u32.to_le_bytes());
        data.extend_from_slice(&2u32.to_le_bytes());
        let out = expand_via_page_map(data, &Some(pm)).unwrap();
        assert_eq!(out.len(), 16);
        assert_eq!(&out[0..4], &1u32.to_le_bytes());
        assert_eq!(&out[4..8], &2u32.to_le_bytes());
        assert_eq!(&out[8..12], &2u32.to_le_bytes());
        assert_eq!(&out[12..16], &2u32.to_le_bytes());
    }

    #[test]
    fn expand_via_page_map_random_access_single_element_rows() {
        // One u32 per row, so an element offset and an entry index coincide.
        // Offsets are used verbatim and may reorder rows.
        let pm = PageMap {
            data_recs: 3,
            lengths: vec![1],
            leng_runs: vec![3],
            mapping: RowMapping::RandomAccessOffsets(vec![0, 2, 1]),
        };
        let mut data = Vec::new();
        data.extend_from_slice(&10u32.to_le_bytes());
        data.extend_from_slice(&20u32.to_le_bytes());
        data.extend_from_slice(&30u32.to_le_bytes());
        let out = expand_via_page_map(data, &Some(pm)).unwrap();
        assert_eq!(&out[0..4], &10u32.to_le_bytes());
        assert_eq!(&out[4..8], &30u32.to_le_bytes());
        assert_eq!(&out[8..12], &20u32.to_le_bytes());
    }

    #[test]
    fn expand_via_page_map_random_access_multi_element_rows() {
        // Two u32s per row. ncbi-vdb stores `data_offset` in elements, so
        // offset 2 means "start at the third u32" — NOT "start at the third
        // two-u32 entry". Earlier revisions guessed between those readings by
        // testing which one fit the buffer; this shape is where the entry
        // reading is wrong (it would run off the end at 2 * 8 + 8 = 24 > 16).
        let pm = PageMap {
            data_recs: 2,
            lengths: vec![2],
            leng_runs: vec![2],
            mapping: RowMapping::RandomAccessOffsets(vec![0, 2]),
        };
        let mut data = Vec::new();
        for v in [100u32, 200, 300, 400] {
            data.extend_from_slice(&v.to_le_bytes());
        }
        let out = expand_via_page_map(data, &Some(pm)).unwrap();
        assert_eq!(out.len(), 16);
        assert_eq!(&out[0..4], &100u32.to_le_bytes());
        assert_eq!(&out[4..8], &200u32.to_le_bytes());
        assert_eq!(&out[8..12], &300u32.to_le_bytes());
        assert_eq!(&out[12..16], &400u32.to_le_bytes());
    }

    #[test]
    fn expand_via_page_map_random_access_offset_overflow_errors() {
        // Buffer has 2 u32s (8 bytes); element offset 5 runs past the end.
        let pm = PageMap {
            data_recs: 3,
            lengths: vec![1],
            leng_runs: vec![3],
            mapping: RowMapping::RandomAccessOffsets(vec![0, 1, 5]),
        };
        let data = vec![0u8; 8];
        let err =
            expand_via_page_map(data, &Some(pm)).expect_err("out-of-bounds offset must error");
        assert!(matches!(err, Error::Format(_)), "got {err:?}");
    }

    #[test]
    fn decode_irzip_column_raw_passthrough_when_ops_empty_and_sizes_match() {
        // Simulates the iunzip blob layout seen on issue #20 / ERR15141550:
        // a v2 header with no ops/args where `osize` equals `data.len()`.
        // The encoder skipped the bit-plane + deflate step entirely — the
        // data is raw u32 output. We must pass it through, not attempt to
        // deflate-decode it as an irzip payload (which would fail with
        // "corrupt deflate stream").
        use crate::blob::BlobHeaderFrame;
        let raw = vec![
            0xc6, 0x1a, 0x01, 0x00, 0x6f, 0x29, 0x00, 0x00, 0x60, 0x1e, 0x00, 0x00, 0x9f, 0x33,
            0x00, 0x00, 0x46, 0x01, 0x00, 0x00, 0x7a, 0x07, 0x00, 0x00, 0xfe, 0x53, 0x02, 0x00,
        ];
        let decoded = DecodedBlob {
            data: Cow::Owned(raw.clone()),
            adjust: 0,
            big_endian: false,
            headers: vec![BlobHeaderFrame {
                flags: 0,
                version: 2,
                fmt: 0,
                osize: raw.len() as u64,
                ops: vec![],
                args: vec![],
            }],
            page_map: None,
            row_length: None,
        };
        let got = decode_irzip_column(&decoded).expect("passthrough must succeed");
        assert_eq!(got, raw);
    }

    #[test]
    fn decode_zip_encoding_empty_data_returns_empty() {
        let blob = make_blob(vec![], None);
        let out = decode_zip_encoding(&blob).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn decode_zip_encoding_raw_passthrough_when_ops_empty_and_sizes_match() {
        // Reproduces DRR019046 ALTREAD blob 160: a v1 zip_encoding header
        // with no ops/args where the 16-byte payload is the uncompressed
        // 4na overlay. Both deflate and zlib reject the bytes (0x0F is not
        // a valid header), and the previous size-only ≤12-byte fallback
        // didn't catch it — silently dropping every N annotation in the
        // blob's 32,768 spots. The header's `osize == data.len()` signal
        // is the canonical "encoder skipped compression" tell.
        use crate::blob::BlobHeaderFrame;
        let raw = vec![
            0x0f, 0x0f, 0x00, 0x00, 0x0f, 0x0f, 0x0f, 0x00, 0x00, 0x0f, 0x0f, 0x0f, 0x00, 0x00,
            0x0f, 0x0f,
        ];
        let blob = DecodedBlob {
            data: Cow::Owned(raw.clone()),
            adjust: 0,
            big_endian: false,
            headers: vec![BlobHeaderFrame {
                flags: 0,
                version: 1,
                fmt: 0,
                osize: raw.len() as u64,
                ops: vec![],
                args: vec![],
            }],
            page_map: None,
            row_length: None,
        };
        let out = decode_zip_encoding(&blob).expect("raw passthrough must succeed");
        assert_eq!(out, raw);
    }

    #[test]
    fn decode_quality_encoding_empty_data_returns_empty() {
        let blob = make_blob(vec![], None);
        let out = decode_quality_encoding(&blob).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn decode_quality_encoding_uses_zlib_fast_path_for_0x78_payloads() {
        // Issue #30: SRA-Lite quality blobs are stock zlib streams. The
        // probe must short-circuit on the 0x78 CMF byte and route to
        // decode_zip_encoding, never attempting izip_decode on bytes that
        // happen to look like an iZip header.
        let payload: Vec<u8> = vec![b'F'; 100];
        let mut zlib = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        std::io::Write::write_all(&mut zlib, &payload).unwrap();
        let compressed = zlib.finish().unwrap();
        assert_eq!(compressed[0], 0x78, "zlib stream must start with 0x78");

        let osize = payload.len() as u64;
        let blob = DecodedBlob {
            data: Cow::Owned(compressed),
            adjust: 0,
            big_endian: false,
            headers: vec![crate::blob::BlobHeaderFrame {
                version: 1,
                osize,
                ..Default::default()
            }],
            page_map: None,
            row_length: None,
        };
        let out = decode_quality_encoding(&blob).expect("zlib fast path must decode");
        assert_eq!(out, payload);
    }

    #[test]
    fn decode_quality_encoding_rejects_oversized_izip_header() {
        // A non-zlib payload whose first 5 bytes spell out
        // `flags=0x01, data_count=u32::MAX` previously drove a 4 GiB
        // allocation in `izip_decode`. After the fix, `izip_decode` errors
        // out and we fall through to deflate, which also fails on the
        // garbage payload — neither path may panic with handle_alloc_error.
        let mut data = vec![0u8; 32];
        data[0] = 0x01;
        data[1..5].copy_from_slice(&u32::MAX.to_le_bytes());
        let blob = make_blob(data, None);
        let _ = decode_quality_encoding(&blob); // must not abort the process
    }

    // -----------------------------------------------------------------------
    // Property tests
    // -----------------------------------------------------------------------

    use proptest::prelude::*;

    proptest! {
        /// `expand_via_page_map` with `page_map = None` is a pure
        /// passthrough — no matter what bytes we feed in.
        #[test]
        fn prop_expand_via_page_map_none_is_identity(
            data in proptest::collection::vec(any::<u8>(), 0..256)
        ) {
            let out = expand_via_page_map(data.clone(), &None).unwrap();
            prop_assert_eq!(out, data);
        }

        /// Under repeat counts the output byte count is exactly
        /// `sum(repeats) * elem_bytes` for row_length=1, and each record's
        /// bytes appear once per row it covers.
        #[test]
        fn prop_expand_via_page_map_per_row_length_is_sum_of_runs(
            runs in proptest::collection::vec(2u32..4, 1..16)
        ) {
            let n = runs.len();
            let total: u32 = runs.iter().sum();
            let pm = PageMap {
                data_recs: n as u64,
                lengths: vec![1],
                leng_runs: vec![total],
                mapping: RowMapping::RepeatCounts(runs.clone()),
            };
            // row_length=1 → each record is one u32 (4 bytes).
            let mut data = Vec::with_capacity(n * 4);
            for i in 0..n {
                data.extend_from_slice(&(i as u32).to_le_bytes());
            }
            let out = expand_via_page_map(data, &Some(pm)).unwrap();
            prop_assert_eq!(out.len(), total as usize * 4);
            let mut cursor = 0;
            for (i, &rep) in runs.iter().enumerate() {
                let expected = (i as u32).to_le_bytes();
                for _ in 0..rep {
                    prop_assert_eq!(&out[cursor..cursor + 4], &expected[..]);
                    cursor += 4;
                }
            }
        }

        /// Random access: for row_length=1 every offset picks one u32, so
        /// the output length always equals `offsets.len() * 4` regardless of
        /// how the offsets repeat or reorder.
        #[test]
        fn prop_expand_via_page_map_random_access_length(
            entries in 1usize..16,
            refs in proptest::collection::vec(0u32..16, 1..32),
        ) {
            let max_ref = (entries - 1) as u32;
            let refs: Vec<u32> = refs.into_iter().map(|r| r % max_ref.max(1)).collect();
            let pm = PageMap {
                data_recs: refs.len() as u64,
                lengths: vec![1],
                leng_runs: vec![refs.len() as u32],
                mapping: RowMapping::RandomAccessOffsets(refs.clone()),
            };
            let mut data = Vec::with_capacity(entries * 4);
            for i in 0..entries {
                data.extend_from_slice(&(i as u32).to_le_bytes());
            }
            let out = expand_via_page_map(data, &Some(pm)).unwrap();
            prop_assert_eq!(out.len(), refs.len() * 4);
        }
    }

    // -----------------------------------------------------------------
    // qual4 codebook (#113)
    // -----------------------------------------------------------------

    /// Literal quad: leading byte < 81 means four value bytes, each biased +40.
    #[test]
    fn qual4_literal_quad() {
        let src = [40u8, 30, 20, 10]; // -> 0, -10, -20, -30
        let out = qual4_decode(&src, 1, -40, 40).unwrap();
        assert_eq!(
            out.iter().map(|&b| b as i8).collect::<Vec<_>>(),
            vec![0, -10, -20, -30]
        );
    }

    /// Code 81 is the `N` marker and expands to (-5, -5, -5, -5) — the
    /// 0xFBFBFBFB the schema maps to log-odds -6.
    #[test]
    fn qual4_known_bad_is_the_n_quad() {
        let out = qual4_decode(&[81], 1, -40, 40).unwrap();
        assert_eq!(
            out.iter().map(|&b| b as i8).collect::<Vec<_>>(),
            vec![-5, -5, -5, -5]
        );
    }

    /// Code 82 expands to (qmax, qmin, qmin, qmin) using the header's bounds.
    #[test]
    fn qual4_known_good_uses_header_bounds() {
        let out = qual4_decode(&[82], 1, -40, 40).unwrap();
        assert_eq!(
            out.iter().map(|&b| b as i8).collect::<Vec<_>>(),
            vec![40, -40, -40, -40]
        );
    }

    /// The nine pattern codes place one derived value and fill the rest with
    /// qmin. 83-85 use -v, 86-88 use -v+1, 89-91 use -v-1; the slot cycles
    /// 1, 2, 3 within each group.
    #[test]
    fn qual4_pattern_codes_place_value_and_fill_qmin() {
        let cases: [(u8, [i8; 4]); 9] = [
            (83, [10, -10, -40, -40]),
            (84, [10, -40, -10, -40]),
            (85, [10, -40, -40, -10]),
            (86, [10, -9, -40, -40]),
            (87, [10, -40, -9, -40]),
            (88, [10, -40, -40, -9]),
            (89, [10, -11, -40, -40]),
            (90, [10, -40, -11, -40]),
            (91, [10, -40, -40, -11]),
        ];
        for (code, want) in cases {
            let out = qual4_decode(&[code, 50], 1, -40, 40).unwrap();
            let got: Vec<i8> = out.iter().map(|&b| b as i8).collect();
            assert_eq!(got, want.to_vec(), "code {code}");
        }
    }

    /// The reference rejects a blob that does not yield exactly `dcount`
    /// quads, so a truncated trailing code must error rather than pad.
    #[test]
    fn qual4_incomplete_trailing_quad_errors() {
        assert!(qual4_decode(&[40, 30], 1, -40, 40).is_err());
        assert!(qual4_decode(&[81], 2, -40, 40).is_err());
    }

    /// Codes past the table are malformed. The reference notices on the
    /// *following* byte, because state 0 defers an unknown code.
    #[test]
    fn qual4_unknown_code_errors() {
        assert!(qual4_decode(&[92, 40], 1, -40, 40).is_err());
    }

    /// Trailing input past `dcount` is ignored, not an error.
    #[test]
    fn qual4_extra_trailing_input_is_ignored() {
        let out = qual4_decode(&[81, 81, 81], 2, -40, 40).unwrap();
        assert_eq!(out.len(), 8);
    }

    /// Mixed stream: every code shape back to back stays in sync.
    #[test]
    fn qual4_mixed_stream_stays_aligned() {
        // literal, known_good, pattern, known_bad
        let src = [40u8, 30, 20, 10, 82, 84, 50, 81];
        let out = qual4_decode(&src, 4, -40, 40).unwrap();
        let got: Vec<i8> = out.iter().map(|&b| b as i8).collect();
        assert_eq!(
            got,
            vec![
                0, -10, -20, -30, 40, -40, -40, -40, 10, -40, -10, -40, -5, -5, -5, -5
            ]
        );
    }
}
