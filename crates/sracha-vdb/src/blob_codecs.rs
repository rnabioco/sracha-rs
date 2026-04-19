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
    } else {
        let num_elems = decoded
            .row_length
            .unwrap_or_else(|| (decoded.data.len() as u64 * 8) / 32) as u32;
        blob::izip_decode(&decoded.data, 32, num_elems)?
    };
    expand_via_page_map(decoded_ints, &decoded.page_map)
}

/// Expand decoded integer data via a page map's data_runs, if present.
///
/// For columns like X, Y, and READ_LEN, the irzip/izip decoder produces
/// unique data entries and the page map maps each row to its data entry.
pub fn expand_via_page_map(
    decoded_ints: Vec<u8>,
    page_map: &Option<blob::PageMap>,
) -> Result<Vec<u8>> {
    let Some(pm) = page_map else {
        return Ok(decoded_ints);
    };
    let elem_bytes = 4usize; // u32
    let row_length = pm.lengths.first().copied().unwrap_or(1) as usize;
    let entry_bytes = row_length * elem_bytes;

    if !pm.data_runs.is_empty() && pm.data_runs.len() as u64 >= pm.total_rows() {
        // Random-access variant: data_runs[i] picks the source for logical
        // row i. Two offset-unit conventions coexist in practice:
        //
        // - entry-index (default): offset is the index of a row_length-wide
        //   entry in the decoded buffer, so `start = offset * entry_bytes`.
        // - u32-index (rare; e.g. DRR045255's READ_LEN blob with 1032 rows
        //   where the decoded buffer holds a flat u32 stream): offset is
        //   the index of a single u32, so `start = offset * elem_bytes` and
        //   row_length consecutive u32s are consumed per row.
        //
        // We can't statically tell them apart — the page_map serialisation
        // uses the same on-disk shape for both. Dispatch adaptively: if
        // the max data_run fits the entry-index interpretation, use it;
        // otherwise fall back to u32-index. Either way a dispatch that
        // can't reconstruct row_count * entry_bytes of output is an error.
        let max_offset = pm.data_runs.iter().max().copied().unwrap_or(0) as usize;
        let entry_index_fits = max_offset * entry_bytes + entry_bytes <= decoded_ints.len();
        let u32_index_fits =
            row_length >= 2 && max_offset * elem_bytes + entry_bytes <= decoded_ints.len();
        let stride = if entry_index_fits {
            entry_bytes
        } else if u32_index_fits {
            elem_bytes
        } else {
            return Err(Error::Format(format!(
                "page_map: max offset {max_offset} overflows decoded buffer \
                 ({} bytes) under both entry-index (×{entry_bytes}) and u32-index \
                 (×{elem_bytes}) interpretations",
                decoded_ints.len(),
            )));
        };

        let mut expanded = Vec::with_capacity(pm.data_runs.len() * entry_bytes);
        for &offset in &pm.data_runs {
            let start = offset as usize * stride;
            let end = start + entry_bytes;
            if end > decoded_ints.len() {
                return Err(Error::Format(format!(
                    "page_map: offset {offset} × {stride} + {entry_bytes} out of {} decoded bytes",
                    decoded_ints.len(),
                )));
            }
            expanded.extend_from_slice(&decoded_ints[start..end]);
        }
        Ok(expanded)
    } else if !pm.data_runs.is_empty() {
        // data_runs is per-row; each row is `entry_bytes` (row_length × elem_bytes).
        // Passing elem_bytes here would trip the length check on any column with
        // row_length > 1, crashing with a 2:1 (or N:1) "expected at least" ratio.
        Ok(pm.expand_data_runs_bytes(&decoded_ints, entry_bytes)?)
    } else {
        Ok(decoded_ints)
    }
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

/// Decode the QUALITY blob payload, handling both `zip_encoding` (deflate/
/// zlib — modern Illumina) and `izip_encoding` (NCBI integer compression —
/// older srf-load-era Illumina such as DRR001816).
///
/// The encoding isn't tagged in the blob header in a way that's trivially
/// inspectable, so we probe by attempting `izip_decode` first: its 5-byte
/// header validation rejects non-iZip payloads cleanly, so any file whose
/// QUALITY is standard deflate falls straight through to
/// [`decode_zip_encoding`].
pub fn decode_quality_encoding(decoded: &blob::DecodedBlob<'_>) -> Result<Vec<u8>> {
    if !decoded.data.is_empty()
        && let Ok(qdata) = blob::izip_decode(&decoded.data, 8, decoded.data.len() as u32)
        && !qdata.is_empty()
    {
        return Ok(qdata);
    }
    decode_zip_encoding(decoded)
}
