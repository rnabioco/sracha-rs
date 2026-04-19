//! Per-blob fastq decode — the hot path that takes the raw bytes of each
//! column blob ([`RawBlobData`]) and produces `(OutputSlot, bytes)`
//! records ready for the writer.
//!
//! Covers: column raw-blob decompression (izip/irzip/zip), page_map
//! expansion, ALTREAD 4na merge with blob-boundary-aware alignment,
//! SRA-Lite quality synthesis, READ_LEN fallbacks, spot segmentation,
//! split-mode routing.
//!
//! Extracted from `pipeline/mod.rs` as part of the pipeline refactor
//! (no behavior change). Tests for the pure helpers (`phred_to_ascii`
//! offset, variant-2 page_map fail-fast, zero-length segment filter,
//! etc.) live in the parent test module where the existing invariants
//! were pinned; keep them there to preserve git-blame traceability.

use std::sync::atomic::Ordering;

use itoa;

use crate::error::{Error, Result};
use crate::fastq::{
    FastqConfig, IntegrityDiag, OutputSlot, SplitMode, append_fasta_record, append_fastq_record,
};

use super::BlobSlotOutput;

// Shared blob decoders live in `sracha-vdb::blob_codecs` so the fastq
// pipeline and the `sracha vdb dump` command can reach them. Re-exported
// here so the rest of this module and `pipeline::mod` / `pipeline::validate`
// keep using the short unqualified names they've always used.
#[cfg(test)]
pub(crate) use crate::vdb::blob_codecs::expand_via_page_map;
pub(crate) use crate::vdb::blob_codecs::{
    decode_irzip_column, decode_quality_encoding, decode_raw, decode_zip_encoding,
};

// ---------------------------------------------------------------------------
// Phase 2: Parse VDB + output FASTQ
// ---------------------------------------------------------------------------

/// Convert raw Phred quality bytes (as read from the physical `QUALITY`
/// column) into Phred+33 ASCII-encoded bytes ready for FASTQ output.
///
/// Returns `(encoded, is_empty)` where `is_empty` is true when the input
/// is empty or all-zero (both cases mean "no real quality data — caller
/// should synthesize a fallback"). Every other byte goes through
/// [`crate::vdb::encoding::phred_to_ascii`], which adds `+33` verbatim.
/// Raw Q0 maps to `!` — older srf-load archives (e.g. DRR000918) do
/// legitimately store Q0 and fasterq-dump preserves it as-is.
///
/// Extracted so the invariant "raw column bytes always get +33 applied,
/// never passed through as ASCII" is unit-testable independently of the
/// rest of the decode pipeline. An earlier implementation tried to
/// detect already-encoded inputs via an `all_valid_ascii` heuristic and
/// silently skipped the offset — that branch misfired on files whose
/// raw Phred distribution happened to be entirely in [33, 126] (e.g.
/// DRR040728) and produced systematic off-by-33 quality divergence.
pub(super) fn encode_raw_quality_for_fastq(quality_data: &[u8]) -> (Vec<u8>, bool) {
    let is_empty = quality_data.is_empty() || quality_data.iter().all(|&b| b == 0);
    if is_empty {
        return (Vec::new(), true);
    }
    (crate::vdb::encoding::phred_to_ascii(quality_data), false)
}

// ---------------------------------------------------------------------------
// Batch-parallel blob decode types and helpers
// ---------------------------------------------------------------------------

/// Raw bytes for a single blob across all columns.
/// Holds borrowed slices into the mmap (zero-copy) so this is Send
/// as long as the mmap outlives the parallel closure.
pub(crate) struct RawBlobData<'a> {
    /// READ column raw bytes.
    pub(crate) read_raw: &'a [u8],
    /// Row count (id_range) for the READ blob.
    pub(crate) read_id_range: u64,
    /// Start row id of the READ blob — used to align ALTREAD (which may
    /// be chunked into differently-sized blobs).
    pub(crate) read_start_id: i64,
    /// QUALITY column raw bytes (empty if column absent or blob out of range).
    pub(crate) quality_raw: &'a [u8],
    /// Row count for the QUALITY blob (0 if absent).
    pub(crate) quality_id_range: u64,
    /// Checksum type for the QUALITY column.
    pub(crate) quality_cs: u8,
    /// READ_LEN column raw bytes (empty if column absent).
    pub(crate) read_len_raw: &'a [u8],
    /// Row count for the READ_LEN blob (0 if absent).
    pub(crate) read_len_id_range: u64,
    /// Checksum type for the READ_LEN column.
    pub(crate) read_len_cs: u8,
    /// NAME column raw bytes (empty if column absent).
    pub(crate) name_raw: &'a [u8],
    /// Row count for the NAME blob (0 if absent).
    pub(crate) name_id_range: u64,
    /// Checksum type for the NAME column.
    pub(crate) name_cs: u8,
    /// READ_TYPE column raw bytes (empty if column absent).
    pub(crate) read_type_raw: &'a [u8],
    /// Row count for the READ_TYPE blob (0 if absent).
    pub(crate) read_type_id_range: u64,
    /// Checksum type for the READ_TYPE column.
    pub(crate) read_type_cs: u8,
    /// Whether there is a READ_LEN column at all.
    pub(crate) has_read_len: bool,
    /// Whether there is a NAME column at all.
    pub(crate) has_name: bool,
    /// Whether there is a READ_TYPE column at all.
    pub(crate) has_read_type: bool,
    /// ALTREAD column raw bytes (4na ambiguity mask; also used for name reconstruction detection).
    pub(crate) altread_raw: &'a [u8],
    /// Row count for the ALTREAD blob (0 if absent).
    pub(crate) altread_id_range: u64,
    /// Start row id of the ALTREAD blob. Needed because ALTREAD and READ
    /// can be chunked into blobs with different row counts — e.g.
    /// DRR035866 has READ in 4096-row blobs but ALTREAD in 8192-row
    /// blobs. We look up the ALTREAD blob by row id rather than by
    /// index, and use this field to compute the row offset within the
    /// padded ALTREAD buffer when the blobs don't align 1:1.
    pub(crate) altread_start_id: i64,
    /// Checksum type for the ALTREAD column.
    pub(crate) altread_cs: u8,
    /// Whether the ALTREAD column exists (independent of Illumina name parts).
    pub(crate) has_altread: bool,
    /// X column raw bytes.
    pub(crate) x_raw: &'a [u8],
    pub(crate) x_id_range: u64,
    pub(crate) x_cs: u8,
    /// Y column raw bytes.
    pub(crate) y_raw: &'a [u8],
    pub(crate) y_id_range: u64,
    pub(crate) y_cs: u8,
    /// Whether Illumina name parts (ALTREAD + X + Y) are available.
    pub(crate) has_illumina_name_parts: bool,
    /// Shared name format templates (from skey index), sorted by spot_start.
    pub(crate) name_templates: &'a [Vec<u8>],
    /// Starting spot_id for each name template (parallel to name_templates).
    pub(crate) name_spot_starts: &'a [i64],
    /// Reads-per-spot from table metadata (fallback when READ_LEN absent).
    pub(crate) metadata_reads_per_spot: Option<usize>,
    /// Fixed spot length in bases (from READ column page_size).
    pub(crate) fixed_spot_len: Option<u32>,
    /// Per-read lengths from NCBI EUtils API or VDB metadata (used as
    /// fallback when READ_LEN column is absent). Borrowed from the outer
    /// `decode_and_write` scope so we don't clone a Vec per blob.
    pub(crate) fallback_read_lengths: Option<&'a [u32]>,
    /// Per-read types (0=biological, 1=technical) from VDB metadata,
    /// used as a fallback when the physical READ_TYPE column is absent.
    pub(crate) fallback_read_types: Option<&'a [u8]>,
}

/// Decode a single blob and produce FASTQ records directly.
///
/// This fused function replaces the former two-step decode_blob_to_spots +
/// format_spot, eliminating intermediate `SpotRecord` allocations. It operates
/// only on borrowed data (Send-safe for rayon).
///
/// Returns `(records, num_spots)`.
/// Per-call immutable context shared across every blob decode of a run.
/// Replaces the prior 8-argument `decode_blob_to_fastq` signature with a
/// single `ctx` reference plus per-blob position.
pub(crate) struct BlobDecodeCtx<'a> {
    pub(crate) run_name: &'a str,
    pub(crate) config: &'a FastqConfig,
    pub(crate) diag: &'a IntegrityDiag,
    pub(crate) is_lite: bool,
    pub(crate) read_cs: u8,
}

/// Decode the ALTREAD blob (when present) and merge its per-base 4na
/// ambiguity mask into `read_data`. Best-effort: small or malformed
/// ALTREAD blobs log at debug and leave the 2na basecalls untouched
/// rather than refusing the whole spot. One exception is the variant-2
/// page_map shape (no data_runs + multiple distinct lengths), which
/// can silently leak real N annotations — that case errors out with an
/// [`Error::UnsupportedFormat`] so we never emit wrong FASTQ.
///
/// Extracted from [`decode_blob_to_fastq`] — behavior unchanged.
fn apply_altread_merge(
    read_data: &mut [u8],
    raw: &RawBlobData<'_>,
    actual_bases: usize,
    blob_idx: usize,
) -> Result<()> {
    // ALTREAD decode is best-effort: some (usually very small) blobs
    // fail to decompress — e.g. a 5-byte payload where neither deflate
    // nor zlib produces valid output. Previously this was masked by
    // only running the merge when !has_illumina_name_parts, but that
    // also skipped the merge for modern Illumina runs that do need
    // it. Now we attempt the merge always and tolerate decode errors.
    let alt_result =
        decode_raw(raw.altread_raw, raw.altread_cs, raw.altread_id_range).and_then(|alt_decoded| {
            let alt_page_map = alt_decoded.page_map.clone();
            let altread_data = decode_zip_encoding(&alt_decoded)?;
            Ok((alt_page_map, altread_data))
        });
    let (alt_page_map, altread_data) = match alt_result {
        Ok(v) => v,
        Err(e) => {
            tracing::debug!("blob {blob_idx}: ALTREAD decode skipped: {e}");
            return Ok(());
        }
    };

    // Physical ALTREAD is `<INSDC:4na:bin>zip_encoding#1 .ALTREAD =
    // trim<0,0>(…)` — one byte per base in the low nibble, stored
    // as variable-length rows with leading zeros stripped. Pad each
    // row back to the full per-row length (right-aligning the
    // stored bytes because `trim<0, 0>` trims from the left) using
    // the page_map, then merge byte-per-base.
    //
    // Fail-fast on variant 2 page maps: when `data_runs` is empty
    // *and* we have more than one distinct length value, the
    // stored data layout doesn't match either of the two
    // interpretations that work elsewhere (`sum(lengths)` nor
    // `sum(lengths × leng_runs)` equals `stored_len` on real
    // data — validated against DRR024182 blob 162). We don't
    // have a correct decoder for that variant yet, and silently
    // skipping the merge leaks real N annotations into output
    // (0.01–1.5% sequence divergence from fasterq-dump).
    // Erroring out with an actionable message is the safer
    // choice — better to refuse than produce wrong FASTQ.
    if let Some(pm) = alt_page_map.as_ref()
        && pm.data_runs.is_empty()
        && pm.lengths.len() > 1
        && altread_data.iter().any(|&b| b != 0)
    {
        return Err(Error::UnsupportedFormat {
            format: "page_map v1 variant 2 in ALTREAD".into(),
            hint: format!(
                "ALTREAD blob {blob_idx} stores {} rows with {} unique length \
                 values via a variant-2 page_map (no data_runs); sracha's \
                 decoder for this variant doesn't produce byte-identical \
                 output vs fasterq-dump. Refusing to emit potentially-wrong \
                 FASTQ — use fasterq-dump for this file, or file an issue \
                 with the accession so we can add proper variant-2 support. \
                 See comment in pipeline::decode_blob_to_fastq.",
                pm.leng_runs.iter().sum::<u32>(),
                pm.lengths.len(),
            ),
        });
    }

    // Pad ALTREAD across its *whole* blob (which may be larger
    // than the READ blob), then slice the portion covering
    // READ's row range. DRR035866 has READ in 4096-row blobs
    // but ALTREAD in 8192-row blobs — pairing by blob index
    // merges ALTREAD rows 1..4096 against READ rows 4097..8192,
    // producing N's at random positions in unrelated records.
    let row_bases = actual_bases / raw.read_id_range.max(1) as usize;
    let padded_ok = alt_page_map.as_ref().and_then(|pm| {
        if row_bases > 0 && !altread_data.is_empty() {
            match pm.pad_trimmed_rows_fixed(
                &altread_data,
                row_bases,
                crate::vdb::blob::TrimSide::Leading,
            ) {
                Ok(v) => Some(v),
                Err(e) => {
                    tracing::debug!("blob {blob_idx}: ALTREAD pad_trimmed_rows_fixed err: {e}");
                    None
                }
            }
        } else {
            None
        }
    });

    match padded_ok {
        Some(padded) => {
            // Offset into padded for READ's starting row within
            // the ALTREAD blob. 0 when the two blobs align 1:1.
            let row_offset = (raw.read_start_id - raw.altread_start_id).max(0) as usize;
            let byte_offset = row_offset * row_bases;
            if byte_offset < padded.len() {
                let slice_end = byte_offset + actual_bases.min(padded.len() - byte_offset);
                crate::vdb::encoding::merge_altread_bin(
                    read_data,
                    &padded[byte_offset..slice_end],
                    actual_bases,
                );
            }
        }
        None => {
            // Remaining path for the case where alt_page_map is
            // None or row_bases math doesn't work out. The
            // variant-2 case (most common cause of misalignment)
            // is caught above and errors out; reaching here means
            // either no page map or a more unusual shape. Skip
            // the merge rather than corrupt — this was the
            // behavior before the variant-2 check was added, and
            // it produces 0 divergence on the validation fixtures
            // we do have coverage for.
            tracing::debug!(
                "blob {blob_idx}: ALTREAD merge skipped — cannot align \
                 (row_bases={row_bases}, actual={actual_bases}, stored={})",
                altread_data.len(),
            );
        }
    }

    Ok(())
}

pub(crate) fn decode_blob_to_fastq(
    raw: &RawBlobData<'_>,
    ctx: &BlobDecodeCtx<'_>,
    blob_idx: usize,
    spots_before: u64,
) -> Result<(Vec<BlobSlotOutput>, u64)> {
    let BlobDecodeCtx {
        run_name,
        config,
        diag,
        is_lite,
        read_cs,
    } = *ctx;
    // ------------------------------------------------------------------
    // Decode READ blob -> 2na -> ASCII bases.
    // ------------------------------------------------------------------
    let read_decoded = decode_raw(raw.read_raw, read_cs, raw.read_id_range)?;
    let total_bits = read_decoded.data.len() * 8;
    let adjust = read_decoded.adjust as usize;
    let actual_bases = (total_bits.saturating_sub(adjust)) / 2;
    let mut read_data = crate::vdb::encoding::unpack_2na(&read_decoded.data, actual_bases);
    let read_page_map = read_decoded.page_map;

    // V2 blobs may deduplicate identical rows via the page map's
    // `data_runs` — e.g., two spots with identical base calls get
    // written once and replicated on read. For fixed-row-length
    // columns we replicate the 2na-decoded ASCII bytes accordingly so
    // downstream slicing sees the full logical row count; without this
    // trailing duplicate rows would drop out at blob boundaries and
    // every subsequent spot would drift.
    if let Some(ref pm) = read_page_map
        && !pm.data_runs.is_empty()
        && !pm.lengths.is_empty()
        && pm.lengths.iter().all(|&l| l == pm.lengths[0])
        && pm.lengths[0] > 0
    {
        let row_bytes = pm.lengths[0] as usize;
        if read_data.len().is_multiple_of(row_bytes) {
            match pm.expand_data_runs_bytes(&read_data, row_bytes) {
                Ok(expanded) => read_data = expanded,
                Err(e) => {
                    tracing::debug!("blob {blob_idx}: READ expand_data_runs_bytes skipped: {e}");
                }
            }
        }
    }
    let actual_bases = read_data.len();

    // ------------------------------------------------------------------
    // Decode QUALITY blob (skipped entirely in FASTA mode, and also for
    // SRA-Lite runs — the stored bytes there are synthetic placeholders
    // (e.g. Q3 reject/'$') that don't match fasterq-dump's syn_quality
    // output, so we force the synthesis fallback below).
    // ------------------------------------------------------------------
    let (quality_all, quality_is_empty): (Vec<u8>, bool) = if config.fasta || is_lite {
        (Vec::new(), true)
    } else {
        let quality_data: Vec<u8> = if !raw.quality_raw.is_empty() {
            let qdecoded = decode_raw(raw.quality_raw, raw.quality_cs, raw.quality_id_range)?;
            let qpage_map = qdecoded.page_map.clone();
            let mut qdata = decode_quality_encoding(&qdecoded)?;
            // Expand quality data via page map data_runs if present.
            // Some blobs (e.g., PacBio) store repeated rows once with
            // data_runs > 1; the decompressed data only contains unique
            // rows and must be expanded to match the total base count.
            if let Some(ref pm) = qpage_map
                && !pm.data_runs.is_empty()
            {
                qdata = pm.expand_variable_data_runs(&qdata)?;
            }
            qdata
        } else {
            Vec::new()
        };

        let (all, is_empty) = encode_raw_quality_for_fastq(&quality_data);
        if is_empty && !quality_data.is_empty() {
            diag.all_zero_quality_blobs.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                "quality blob {} is all-zero ({} bytes) — synthesizing Phred fallback",
                blob_idx,
                quality_data.len(),
            );
        }
        (all, is_empty)
    };

    // ------------------------------------------------------------------
    // N-masking: ALTREAD (4na) ambiguity merge.
    //
    // Physical ALTREAD is always <INSDC:4na:bin> — it holds the 4na
    // ambiguity mask that the VDB schema `bit_or`s over the 2na basecalls
    // to produce the canonical READ column. The X+Y columns enable NAME
    // reconstruction but don't change ALTREAD's semantics; earlier
    // versions of this code mistakenly assumed X+Y-present meant
    // "ALTREAD is a name template" and skipped the merge, producing
    // garbage for Illumina runs.
    //
    // No quality-based N-masking fallback: fasterq-dump emits the raw
    // 2na basecalls for low-quality positions (Q <= 2 is common in
    // late-cycle Illumina reads) and only replaces with N where ALTREAD
    // says so. A Q-based mask over-writes real bases, causing
    // FAIL_SEQ divergences validated on accessions like DRR006688.
    // ------------------------------------------------------------------
    if raw.has_altread && !raw.altread_raw.is_empty() {
        apply_altread_merge(&mut read_data, raw, actual_bases, blob_idx)?;
    }

    // Quality fallback buffer for SRA-lite / empty quality. Default to the
    // SRA-Lite *pass* constant (Q30 / '?') because fasterq-dump's
    // syn_quality emits Q30 for any spot that passes the read filter, and
    // fast-filter data is the overwhelming majority of SRA-Lite runs. The
    // previous code used REJECT (Q3 / '$') under `is_lite`, which diverged
    // from fasterq-dump's output for every record on DRR035195-style
    // delite-processed runs.
    let lite_qual_char =
        crate::vdb::encoding::SRA_LITE_PASS_QUAL + crate::vdb::encoding::QUAL_PHRED_OFFSET;
    let mut lite_qual_buf: Option<Vec<u8>> = if quality_is_empty {
        Some(vec![lite_qual_char; read_data.len()])
    } else {
        None
    };

    // ------------------------------------------------------------------
    // Decode READ_LEN blob -> irzip/izip -> u32 lengths.
    // ------------------------------------------------------------------
    let (read_lengths, reads_per_spot): (Vec<u32>, usize) = if raw.has_read_len
        && !raw.read_len_raw.is_empty()
    {
        let rldecoded = decode_raw(raw.read_len_raw, raw.read_len_cs, raw.read_len_id_range)?;

        let rps = rldecoded
            .page_map
            .as_ref()
            .and_then(|pm| pm.lengths.first().copied())
            .unwrap_or(1) as usize;

        let rl_bytes = decode_irzip_column(&rldecoded)?;

        let lengths: Vec<u32> = rl_bytes
            .chunks_exact(4)
            .map(|chunk| u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
            .collect();

        if blob_idx == 0 {
            tracing::debug!(
                "READ_LEN: {} values, reads_per_spot={}, first_10={:?}",
                lengths.len(),
                rps,
                &lengths[..lengths.len().min(10)],
            );
        }

        (lengths, rps)
    } else if !raw.has_read_len {
        // No READ_LEN column. Use fallback read structure from API or
        // VDB metadata.
        if let Some(ref api_lens) = raw.fallback_read_lengths {
            // Fallback: we know the exact per-read lengths.
            let rps = api_lens.len();
            let spot_len: u32 = api_lens.iter().sum();
            let total_bases = read_data.len() as u32;
            let n_spots = total_bases / spot_len.max(1);

            if blob_idx == 0 {
                tracing::debug!(
                    "using fallback read lengths: {:?}, spot_len={}, n_spots={}",
                    api_lens,
                    spot_len,
                    n_spots,
                );
            }

            let mut row_lengths = Vec::with_capacity(n_spots as usize * rps);
            for _ in 0..n_spots {
                row_lengths.extend_from_slice(api_lens);
            }
            // Handle remainder (partial last spot).
            let used = n_spots * spot_len;
            if used < total_bases {
                let remainder = total_bases - used;
                row_lengths.push(remainder);
            }
            (row_lengths, rps)
        } else {
            // Fallback: use page map / metadata heuristics.
            let meta_rps = raw.metadata_reads_per_spot.unwrap_or(1);
            if let Some(ref pm) = read_page_map {
                if blob_idx == 0 {
                    tracing::debug!(
                        "READ page_map (no READ_LEN): lengths={:?}, leng_runs={:?}, data_recs={}",
                        &pm.lengths[..pm.lengths.len().min(10)],
                        &pm.leng_runs[..pm.leng_runs.len().min(10)],
                        pm.data_recs,
                    );
                }
                let mut row_lengths = Vec::new();
                // Determine the expected per-spot length from the most
                // common (smallest reasonable) page-map entry.
                let typical_spot_len = pm
                    .lengths
                    .iter()
                    .copied()
                    .filter(|&l| l > 0 && l <= 100_000)
                    .min()
                    .unwrap_or(0);

                for (len, run) in pm.lengths.iter().zip(pm.leng_runs.iter()) {
                    for _ in 0..*run {
                        if *len <= 100_000 || typical_spot_len == 0 {
                            // Normal per-spot entry: split into reads.
                            let spot_len = *len;
                            if meta_rps > 1 && spot_len > 0 {
                                let per_read = spot_len / meta_rps as u32;
                                for r in 0..meta_rps as u32 {
                                    if r < meta_rps as u32 - 1 {
                                        row_lengths.push(per_read);
                                    } else {
                                        row_lengths.push(spot_len - per_read * r);
                                    }
                                }
                            } else {
                                row_lengths.push(spot_len);
                            }
                        } else {
                            // Blob-aggregate entry: split into individual
                            // spots using the typical spot length.
                            let n_spots = *len / typical_spot_len;
                            let remainder = *len - n_spots * typical_spot_len;
                            for s in 0..n_spots {
                                let spot_len = if s == n_spots - 1 {
                                    typical_spot_len + remainder
                                } else {
                                    typical_spot_len
                                };
                                if meta_rps > 1 {
                                    let per_read = spot_len / meta_rps as u32;
                                    for r in 0..meta_rps as u32 {
                                        if r < meta_rps as u32 - 1 {
                                            row_lengths.push(per_read);
                                        } else {
                                            row_lengths.push(spot_len - per_read * r);
                                        }
                                    }
                                } else {
                                    row_lengths.push(spot_len);
                                }
                            }
                        }
                    }
                }
                (row_lengths, meta_rps)
            } else if let Some(spot_len) = raw.fixed_spot_len.filter(|_| meta_rps > 1) {
                // No page map but we know the fixed spot length from the
                // READ column metadata. Split blob into uniform spots.
                let total = read_data.len() as u32;
                let n_spots = total / spot_len.max(1);
                let mut row_lengths = Vec::with_capacity(n_spots as usize * meta_rps);
                for s in 0..n_spots {
                    let sl = if s < n_spots - 1 {
                        spot_len
                    } else {
                        total - spot_len * s
                    };
                    let per_read = sl / meta_rps as u32;
                    for r in 0..meta_rps as u32 {
                        if r < meta_rps as u32 - 1 {
                            row_lengths.push(per_read);
                        } else {
                            row_lengths.push(sl - per_read * r);
                        }
                    }
                }
                (row_lengths, meta_rps)
            } else {
                (vec![read_data.len() as u32], 1)
            }
        }
    } else {
        // has_read_len but raw is empty (blob out of range).
        (vec![read_data.len() as u32], 1)
    };

    // ------------------------------------------------------------------
    // Decode NAME blob.
    // ------------------------------------------------------------------
    let spot_names: Option<Vec<Vec<u8>>> = if raw.has_name && !raw.name_raw.is_empty() {
        let ndecoded = decode_raw(raw.name_raw, raw.name_cs, raw.name_id_range)?;
        let name_bytes = decode_zip_encoding(&ndecoded)?;

        let num_spots = read_lengths
            .len()
            .checked_div(reads_per_spot)
            .unwrap_or(read_lengths.len());

        if let Some(ref pm) = ndecoded.page_map {
            let mut names = Vec::with_capacity(num_spots);
            let mut offset = 0usize;
            for (len, run) in pm.lengths.iter().zip(pm.leng_runs.iter()) {
                let name_len = *len as usize;
                for _ in 0..*run {
                    if offset + name_len <= name_bytes.len() {
                        names.push(name_bytes[offset..offset + name_len].to_vec());
                        offset += name_len;
                    }
                }
            }
            if blob_idx == 0 {
                tracing::debug!(
                    "NAME: {} names decoded, first={:?}",
                    names.len(),
                    names
                        .first()
                        .map(|n| String::from_utf8_lossy(n).to_string()),
                );
            }
            Some(names)
        } else if let Some(row_len) = ndecoded.row_length {
            let rl = row_len as usize;
            if rl > 0 {
                let names: Vec<Vec<u8>> = name_bytes.chunks(rl).map(|c| c.to_vec()).collect();
                Some(names)
            } else {
                None
            }
        } else {
            let delimiter = if name_bytes.contains(&0) { 0u8 } else { b'\n' };
            let names: Vec<Vec<u8>> = name_bytes
                .split(|&b| b == delimiter)
                .filter(|s| !s.is_empty())
                .map(|s| s.to_vec())
                .collect();
            if !names.is_empty() { Some(names) } else { None }
        }
    } else {
        None
    };

    // ------------------------------------------------------------------
    // Illumina name reconstruction from ALTREAD + X + Y columns.
    // If the NAME column is absent but ALTREAD/X/Y are present, reconstruct
    // the original Illumina read name by substituting $X and $Y placeholders
    // in the ALTREAD template string with per-spot X/Y coordinates.
    // ------------------------------------------------------------------
    let spot_names: Option<Vec<Vec<u8>>> = if spot_names.is_none()
        && raw.has_illumina_name_parts
        && !raw.altread_raw.is_empty()
        && !raw.x_raw.is_empty()
        && !raw.y_raw.is_empty()
    {
        // Decode ALTREAD blob → name format template(s).
        // ALTREAD stores ASCII name templates — use raw decoded data directly
        // (not zip_encoding, as these are plain text strings).
        // Use name templates preloaded from the skey index.

        // Decode X column → u32 coordinates (irzip/izip encoded integers).
        let x_decoded = decode_raw(raw.x_raw, raw.x_cs, raw.x_id_range)?;
        let x_bytes = decode_irzip_column(&x_decoded)?;

        // Decode Y column → u32 coordinates (irzip/izip encoded integers).
        let y_decoded = decode_raw(raw.y_raw, raw.y_cs, raw.y_id_range)?;
        let y_bytes = decode_irzip_column(&y_decoded)?;

        // ALTREAD is a per-spot ASCII template (may vary per tile).
        // Parse templates: each spot's template is stored as a fixed-length or
        // page-map-delineated string.
        let num_spots = read_lengths
            .len()
            .checked_div(reads_per_spot)
            .unwrap_or(read_lengths.len());

        // The skey index maps spot ranges to name templates. Each blob covers
        // a contiguous spot range corresponding to one tile. Use the ALTREAD
        // blob index to determine which template this blob uses.
        // For now, use blob_idx to index into the templates list. If the
        // blob count exceeds the template count, wrap around.
        let all_templates: &[Vec<u8>] = raw.name_templates;

        // X/Y values are already decoded as u32 by irzip/izip (stored as
        // little-endian 4-byte groups in the output Vec<u8>).
        let x_vals: Vec<u32> = x_bytes
            .chunks_exact(4)
            .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
            .collect();

        let y_vals: Vec<u32> = y_bytes
            .chunks_exact(4)
            .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
            .collect();

        // Pick the template per spot using the skey spot_start mapping.
        // A single blob can span multiple tile ranges, so we look up the
        // correct template for each spot_id via binary search.
        let has_templates = !all_templates.is_empty() && !raw.name_spot_starts.is_empty();

        if blob_idx == 0 && has_templates {
            tracing::debug!(
                "name reconstruction: {} templates, x_vals={}, y_vals={}",
                all_templates.len(),
                x_vals.len(),
                y_vals.len(),
            );
        }

        if has_templates && !x_vals.is_empty() && !y_vals.is_empty() {
            let mut names = Vec::with_capacity(num_spots);
            let mut itoa_x = itoa::Buffer::new();
            let mut itoa_y = itoa::Buffer::new();
            for spot_i in 0..num_spots {
                let spot_id = spots_before as i64 + spot_i as i64 + 1;
                let tmpl_idx = match raw.name_spot_starts.binary_search(&spot_id) {
                    Ok(i) => i,
                    Err(i) => i.saturating_sub(1),
                };
                let tmpl = &all_templates[tmpl_idx.min(all_templates.len() - 1)];
                let x = x_vals.get(spot_i).copied().unwrap_or(0);
                let y = y_vals.get(spot_i).copied().unwrap_or(0);

                // Substitute $X and $Y in the template.
                let x_str = itoa_x.format(x);
                let y_str = itoa_y.format(y);
                let mut name = Vec::with_capacity(tmpl.len() + 10);
                let mut ti = 0;
                while ti < tmpl.len() {
                    if tmpl[ti] == b'$' && ti + 1 < tmpl.len() {
                        if tmpl[ti + 1] == b'X' {
                            name.extend_from_slice(x_str.as_bytes());
                            ti += 2;
                            continue;
                        } else if tmpl[ti + 1] == b'Y' {
                            name.extend_from_slice(y_str.as_bytes());
                            ti += 2;
                            continue;
                        }
                    }
                    name.push(tmpl[ti]);
                    ti += 1;
                }
                names.push(name);
            }

            if blob_idx == 0 && !names.is_empty() {
                tracing::debug!(
                    "Illumina name reconstructed: first={:?}",
                    String::from_utf8_lossy(&names[0]),
                );
            }
            Some(names)
        } else {
            None
        }
    } else {
        spot_names
    };

    // ------------------------------------------------------------------
    // Decode READ_TYPE blob -> byte array of per-read type codes.
    // 0 = biological, 1 = technical (SRA_READ_TYPE values).
    // ------------------------------------------------------------------
    let read_type_data: Vec<u8> = if raw.has_read_type && !raw.read_type_raw.is_empty() {
        let rtdecoded = decode_raw(raw.read_type_raw, raw.read_type_cs, raw.read_type_id_range)?;
        let raw_bytes = decode_zip_encoding(&rtdecoded)?;
        if !raw_bytes.is_empty() {
            raw_bytes
        } else {
            rtdecoded.data.into_owned()
        }
    } else {
        Vec::new()
    };

    // ------------------------------------------------------------------
    // Iterate spots and produce FASTQ records directly (fused path).
    //
    // Instead of building intermediate SpotRecord structs, we slice
    // directly into the decoded column data and call format_read.
    // ------------------------------------------------------------------
    let rps = reads_per_spot.max(1);
    // Per-slot output accumulators. At most 2-4 slots in realistic configs
    // (Split3 -> Read1/Read2/Unpaired, SplitSpot/Interleaved -> Single,
    // SplitFiles -> ReadN(0..rps)). A linear scan over this small Vec beats
    // a HashMap lookup in the hot path.
    let mut records: Vec<BlobSlotOutput> = Vec::with_capacity(4);
    let mut seq_offset: usize = 0;
    let mut qual_offset: usize = 0;
    let mut rt_offset: usize = 0;
    let mut spot_idx_in_blob: usize = 0;
    let mut rl_cursor = 0usize;

    // Reusable buffer for itoa spot-name formatting.
    let mut itoa_buf = itoa::Buffer::new();

    // Per-spot segment list reused across spots — allocated once per blob
    // instead of once per spot. `rps` is typically 1 or 2.
    struct ReadSeg {
        start: usize,
        len: usize,
    }
    let mut segments: Vec<ReadSeg> = Vec::with_capacity(rps);

    while rl_cursor + rps <= read_lengths.len() {
        let spot_read_lengths = &read_lengths[rl_cursor..rl_cursor + rps];
        let spot_total_bases: usize = spot_read_lengths.iter().map(|&l| l as usize).sum();

        let seq_end = seq_offset + spot_total_bases;
        if seq_end > read_data.len() {
            diag.truncated_spots.fetch_add(1, Ordering::Relaxed);
            tracing::debug!(
                "blob {blob_idx}, spot {spot_idx_in_blob}: sequence overrun at offset \
                 {seq_offset} + {spot_total_bases} > {}; stopping blob",
                read_data.len(),
            );
            break;
        }

        // Borrow slices directly -- no .to_vec().
        let sequence = &read_data[seq_offset..seq_end];
        seq_offset = seq_end;

        let quality: &[u8] = if quality_is_empty {
            &lite_qual_buf.as_ref().unwrap()[seq_offset - spot_total_bases..seq_offset]
        } else {
            let qual_end = qual_offset + spot_total_bases;
            if qual_end > quality_all.len() {
                diag.quality_overruns.fetch_add(1, Ordering::Relaxed);
                tracing::debug!(
                    "blob {blob_idx}, spot {spot_idx_in_blob}: quality overrun at offset \
                     {qual_offset} + {spot_total_bases} > {}; using fallback quality",
                    quality_all.len(),
                );
                // Advance qual_offset so subsequent spots don't re-read stale data.
                qual_offset = qual_end;
                // Lazily allocate fallback buffer on first overrun (rare path).
                if lite_qual_buf.is_none() {
                    lite_qual_buf = Some(vec![lite_qual_char; read_data.len()]);
                }
                &lite_qual_buf.as_ref().unwrap()[seq_offset - spot_total_bases..seq_offset]
            } else {
                let q = &quality_all[qual_offset..qual_end];
                qual_offset = qual_end;
                q
            }
        };

        // Invariant: quality must be exactly as long as sequence.
        debug_assert_eq!(
            quality.len(),
            sequence.len(),
            "blob {blob_idx}, spot {spot_idx_in_blob}: quality length {} != sequence length {}",
            quality.len(),
            sequence.len(),
        );

        // Spot number: always numeric 1-based index.
        let spot_number_str = itoa_buf.format(spots_before as usize + spot_idx_in_blob + 1);
        let spot_number = spot_number_str.as_bytes();

        // Original read name from the NAME column (if present).
        let original_name: Option<&[u8]> = if let Some(ref names) = spot_names {
            if spot_idx_in_blob < names.len() {
                Some(&names[spot_idx_in_blob])
            } else {
                None
            }
        } else {
            None
        };

        // Read types for this spot: borrow from decoded data or default to biological.
        let spot_read_types: &[u8] =
            if !read_type_data.is_empty() && rt_offset + rps <= read_type_data.len() {
                let rt = &read_type_data[rt_offset..rt_offset + rps];
                rt_offset += rps;
                rt
            } else if let Some(meta_rt) = raw.fallback_read_types.filter(|rt| rt.len() == rps) {
                rt_offset += rps;
                meta_rt
            } else {
                rt_offset += rps;
                &[] // empty = all biological (checked below)
            };

        // ------------------------------------------------------------------
        // Inline format_spot logic: split reads, filter, route, format.
        // ------------------------------------------------------------------
        segments.clear();
        let mut read_offset: usize = 0;
        for (i, &rlen) in spot_read_lengths.iter().enumerate() {
            let rlen_usize = rlen as usize;
            let end = read_offset + rlen_usize;
            if end > spot_total_bases {
                break;
            }

            // Filter: skip zero-length reads — fasterq-dump drops these,
            // so mirroring keeps split-3 routing consistent (e.g., SINGLE
            // runs with a 0-length placeholder segment would otherwise
            // route into `_1.fastq`/`_2.fastq` instead of `.fastq`).
            if rlen_usize == 0 {
                read_offset = end;
                continue;
            }

            // Filter: skip technical reads if configured.
            if config.skip_technical {
                let rtype = spot_read_types.get(i).copied().unwrap_or(0);
                if rtype != 0 {
                    read_offset = end;
                    continue;
                }
            }

            // Filter: skip reads shorter than the minimum length.
            if let Some(min_len) = config.min_read_len
                && rlen < min_len
            {
                read_offset = end;
                continue;
            }

            segments.push(ReadSeg {
                start: read_offset,
                len: rlen_usize,
            });
            read_offset = end;
        }

        if !segments.is_empty() {
            // Append a segment's formatted record to the slot's accumulated
            // buffer, inserting a new slot on first use.
            let mut emit = |slot: OutputSlot, seg: &ReadSeg| {
                let buf = match records.iter_mut().find(|s| s.slot == slot) {
                    Some(s) => s,
                    None => {
                        records.push(BlobSlotOutput {
                            slot,
                            bytes: Vec::new(),
                            records: 0,
                        });
                        records.last_mut().unwrap()
                    }
                };
                let seq = &sequence[seg.start..seg.start + seg.len];
                if config.fasta {
                    append_fasta_record(&mut buf.bytes, run_name, spot_number, original_name, seq);
                } else {
                    let qual = &quality[seg.start..seg.start + seg.len];
                    append_fastq_record(
                        &mut buf.bytes,
                        run_name,
                        spot_number,
                        original_name,
                        seq,
                        qual,
                        Some(diag),
                    );
                }
                buf.records += 1;
            };

            match config.split_mode {
                SplitMode::Split3 => {
                    if segments.len() == 2 {
                        emit(OutputSlot::Read1, &segments[0]);
                        emit(OutputSlot::Read2, &segments[1]);
                    } else {
                        for seg in &segments {
                            emit(OutputSlot::Unpaired, seg);
                        }
                    }
                }
                SplitMode::Interleaved | SplitMode::SplitSpot => {
                    for seg in &segments {
                        emit(OutputSlot::Single, seg);
                    }
                }
                SplitMode::SplitFiles => {
                    for (file_idx, seg) in segments.iter().enumerate() {
                        emit(OutputSlot::ReadN(file_idx as u32), seg);
                    }
                }
            }
        }

        rl_cursor += rps;
        spot_idx_in_blob += 1;
    }

    Ok((records, spot_idx_in_blob as u64))
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // encode_raw_quality_for_fastq — PHRED+33 invariant
    //
    // The contract: raw numeric Phred bytes always get +33 applied. An
    // earlier heuristic mis-detected already-encoded inputs and silently
    // skipped the offset, producing systematic off-by-33 quality divergence
    // on DRR040728-style archives whose raw distribution happened to fall
    // within [33, 126]. These tests pin that boundary.
    // -----------------------------------------------------------------------

    #[test]
    fn encode_raw_quality_empty_input_is_empty() {
        let (out, is_empty) = encode_raw_quality_for_fastq(&[]);
        assert!(out.is_empty());
        assert!(is_empty);
    }

    #[test]
    fn encode_raw_quality_all_zero_reports_empty() {
        // All-zero is the sentinel for "quality column absent or
        // placeholder" — the caller must synthesize a fallback rather
        // than emitting a run of '!'.
        let (out, is_empty) = encode_raw_quality_for_fastq(&[0; 8]);
        assert!(out.is_empty());
        assert!(is_empty);
    }

    #[test]
    fn encode_raw_quality_q0_maps_to_bang_when_mixed() {
        // Q0 mixed with other scores must pass through as '!' — srf-load
        // archives (e.g. DRR000918) legitimately store Q0.
        let raw = [0u8, 30, 0, 40, 0];
        let (out, is_empty) = encode_raw_quality_for_fastq(&raw);
        assert!(!is_empty);
        assert_eq!(out, vec![b'!', b'?', b'!', b'I', b'!']);
    }

    #[test]
    fn encode_raw_quality_known_phred_values() {
        let raw = [0u8, 10, 20, 30, 40, 50, 60];
        let (out, is_empty) = encode_raw_quality_for_fastq(&raw);
        assert!(!is_empty);
        assert_eq!(out, raw.iter().map(|&b| b + 33).collect::<Vec<_>>());
    }

    #[test]
    fn encode_raw_quality_does_not_double_offset_ascii_like_input() {
        // Bytes in [33, 126] are NOT treated as already-encoded. This
        // regression test pins the boundary: any non-zero numeric input
        // must have +33 applied.
        let raw = [b'?', b'I']; // 63, 73 — plausibly ASCII, must still +33
        let (out, is_empty) = encode_raw_quality_for_fastq(&raw);
        assert!(!is_empty);
        assert_eq!(out, vec![63 + 33, 73 + 33]);
    }

    #[test]
    fn encode_raw_quality_saturates_on_overflow() {
        let raw = [250u8];
        let (out, is_empty) = encode_raw_quality_for_fastq(&raw);
        assert!(!is_empty);
        assert_eq!(out, vec![255u8]); // saturating_add caps at u8::MAX
    }
}
