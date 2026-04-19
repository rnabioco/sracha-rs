//! FASTQ formatting and output routing for SRA spot records.
//!
//! This module converts decoded SRA spot data into properly formatted FASTQ
//! records and routes them to the correct output file based on the configured
//! split mode.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

/// Aggregated data-integrity counters captured during FASTQ formatting.
///
/// Populated by [`format_read`] / [`format_spot`] and aggregated upstream by
/// the pipeline. A non-zero count indicates that the run produced output that
/// had to be repaired (quality padded, invalid bytes coerced to Q30, mate
/// invariants violated, etc.); surface these to the user and, in
/// `--strict` mode, treat any non-zero count as a hard failure.
#[derive(Default, Debug)]
pub struct IntegrityDiag {
    /// Reads whose quality length did not match their sequence length.
    pub quality_length_mismatches: AtomicU64,
    /// Reads containing Phred+33 bytes outside `[33, 126]`.
    pub quality_invalid_bytes: AtomicU64,
    /// Blobs where the decoded quality stream ran short of the sequence
    /// stream, forcing per-spot synthesized quality fallback.
    pub quality_overruns: AtomicU64,
    /// Blobs whose full quality payload was all-zero (SRA-lite style) and
    /// was replaced with a uniform Phred fallback.
    pub all_zero_quality_blobs: AtomicU64,
    /// Paired-end spots (Split3) that did not yield exactly two biological
    /// reads. Typically indicates READ_LEN / READ_TYPE disagreement.
    pub paired_spot_violations: AtomicU64,
    /// Spots whose READ_LEN values would extend past the decoded sequence.
    pub truncated_spots: AtomicU64,
}

impl IntegrityDiag {
    /// Return `true` if any counter is non-zero.
    pub fn any(&self) -> bool {
        self.quality_length_mismatches.load(Ordering::Relaxed) != 0
            || self.quality_invalid_bytes.load(Ordering::Relaxed) != 0
            || self.quality_overruns.load(Ordering::Relaxed) != 0
            || self.all_zero_quality_blobs.load(Ordering::Relaxed) != 0
            || self.paired_spot_violations.load(Ordering::Relaxed) != 0
            || self.truncated_spots.load(Ordering::Relaxed) != 0
    }

    /// Human-readable summary used by pipeline stats and stats.json.
    pub fn summary(&self) -> String {
        format!(
            "quality_length_mismatches={}, quality_invalid_bytes={}, quality_overruns={}, \
             all_zero_quality_blobs={}, paired_spot_violations={}, truncated_spots={}",
            self.quality_length_mismatches.load(Ordering::Relaxed),
            self.quality_invalid_bytes.load(Ordering::Relaxed),
            self.quality_overruns.load(Ordering::Relaxed),
            self.all_zero_quality_blobs.load(Ordering::Relaxed),
            self.paired_spot_violations.load(Ordering::Relaxed),
            self.truncated_spots.load(Ordering::Relaxed),
        )
    }
}

// ---------------------------------------------------------------------------
// Data model
// ---------------------------------------------------------------------------

/// A single spot (row) from an SRA file, containing one or more reads.
pub struct SpotRecord {
    /// Spot name/ID (ASCII bytes).
    pub name: Vec<u8>,
    /// Concatenated bases for all reads in the spot.
    pub sequence: Vec<u8>,
    /// Concatenated quality scores (Phred+33 ASCII) for all reads.
    pub quality: Vec<u8>,
    /// Length of each read segment within `sequence`/`quality`.
    pub read_lengths: Vec<u32>,
    /// Read type per segment: 0 = biological, 1 = technical.
    pub read_types: Vec<u8>,
    /// Read filter per segment: 0 = pass, 1 = reject.
    pub read_filter: Vec<u8>,
    /// Barcode / sample group (may be empty).
    pub spot_group: Vec<u8>,
}

/// Split mode for FASTQ output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SplitMode {
    /// Paired reads go to `_1`/`_2` files; unpaired to `_0` (or single file).
    Split3,
    /// Nth read goes to Nth output file.
    SplitFiles,
    /// All reads from a spot go to a single file.
    SplitSpot,
    /// R1/R2 alternate in a single file.
    Interleaved,
}

impl fmt::Display for SplitMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SplitMode::Split3 => write!(f, "split-3"),
            SplitMode::SplitFiles => write!(f, "split-files"),
            SplitMode::SplitSpot => write!(f, "split-spot"),
            SplitMode::Interleaved => write!(f, "interleaved"),
        }
    }
}

/// Configuration for FASTQ formatting.
pub struct FastqConfig {
    /// Which split strategy to use.
    pub split_mode: SplitMode,
    /// Skip technical reads (default: `true`).
    pub skip_technical: bool,
    /// Discard reads shorter than this length, if set.
    pub min_read_len: Option<u32>,
    /// Output FASTA instead of FASTQ (drops quality line).
    pub fasta: bool,
}

impl Default for FastqConfig {
    fn default() -> Self {
        Self {
            split_mode: SplitMode::Split3,
            skip_technical: true,
            min_read_len: None,
            fasta: false,
        }
    }
}

/// A fully formatted FASTQ record ready to be written.
///
/// Contains the complete `@defline\nsequence\n+\nquality\n` block as raw bytes.
pub struct FastqRecord {
    /// The complete FASTQ record bytes.
    pub data: Vec<u8>,
}

/// Output routing -- which "file slot" a record belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OutputSlot {
    /// Single output file (used by SplitSpot mode).
    Single,
    /// `_1.fastq[.gz]` (first mate of a pair).
    Read1,
    /// `_2.fastq[.gz]` (second mate of a pair).
    Read2,
    /// `.fastq[.gz]` or `_0` (unpaired reads in Split3).
    Unpaired,
    /// `_N.fastq[.gz]` (Nth read in SplitFiles mode, 0-indexed).
    ReadN(u32),
}

// ---------------------------------------------------------------------------
// Core formatting
// ---------------------------------------------------------------------------

/// Minimum valid quality byte in Phred+33 encoding (ASCII `!`).
const MIN_QUAL_BYTE: u8 = 33;
/// Maximum valid quality byte in Phred+33 encoding (ASCII `~`).
const MAX_QUAL_BYTE: u8 = 126;
/// Fallback quality byte used when quality data is invalid or missing.
/// Phred 30 + 33 = ASCII `?`.
const FALLBACK_QUAL_BYTE: u8 = b'?';

/// Format a single read segment into a [`FastqRecord`].
///
/// Defline format matches fasterq-dump: `@{run}.{spot_num} {description} length={len}`
/// where `description` is the original read name if available, or the spot number again.
/// The `+` line repeats the defline content.
///
/// Bases with Phred quality 0 (ASCII `!`) are replaced with `N` to match
/// the NCBI convention for no-call bases stored in 2na encoding.
///
/// Quality is validated: if its length differs from the sequence, it is
/// padded or truncated to match. Any byte outside the valid Phred+33
/// range `[33, 126]` is replaced with `?` (Q30).
pub fn format_read(
    run_name: &str,
    spot_number: &[u8],
    original_name: Option<&[u8]>,
    sequence: &[u8],
    quality: &[u8],
) -> FastqRecord {
    format_read_with_diag(
        run_name,
        spot_number,
        original_name,
        sequence,
        quality,
        None,
    )
}

/// Variant of [`format_read`] that increments counters on a shared
/// [`IntegrityDiag`] instead of emitting per-event tracing warnings. The
/// pipeline uses this on the hot path to avoid log spam while still surfacing
/// exact counts to the user at run completion.
pub fn format_read_with_diag(
    run_name: &str,
    spot_number: &[u8],
    original_name: Option<&[u8]>,
    sequence: &[u8],
    quality: &[u8],
    diag: Option<&IntegrityDiag>,
) -> FastqRecord {
    let mut data = Vec::new();
    append_fastq_record(
        &mut data,
        run_name,
        spot_number,
        original_name,
        sequence,
        quality,
        diag,
    );
    FastqRecord { data }
}

/// Append a FASTQ record to `out` — the hot-path variant that avoids
/// allocating a fresh `Vec<u8>` per call.
///
/// Semantics match [`format_read_with_diag`]. The defline is constructed
/// once and copied to the `+` line via [`Vec::extend_from_within`], saving
/// one round of `itoa` + string concatenation per record.
#[inline]
pub fn append_fastq_record(
    out: &mut Vec<u8>,
    run_name: &str,
    spot_number: &[u8],
    original_name: Option<&[u8]>,
    sequence: &[u8],
    quality: &[u8],
    diag: Option<&IntegrityDiag>,
) {
    let len = sequence.len();
    let description = original_name.unwrap_or(spot_number);
    let quality = repair_quality(quality, len, diag);

    let mut itoa_buf = itoa::Buffer::new();
    let len_str = itoa_buf.format(len);
    let defline_body_len = defline_body_len(run_name, spot_number, description, len_str);

    // Reserve up front: @defline\nseq\n+defline\nqual\n.
    out.reserve(1 + defline_body_len + 1 + len + 2 + defline_body_len + 1 + len + 1);

    out.push(b'@');
    let body_start = out.len();
    append_defline_body(out, run_name, spot_number, description, len_str);
    let body_end = out.len();
    out.push(b'\n');

    out.extend_from_slice(sequence);
    out.push(b'\n');

    // + line repeats defline. Single intra-Vec memcpy, no re-formatting.
    out.push(b'+');
    out.extend_from_within(body_start..body_end);
    out.push(b'\n');

    out.extend_from_slice(&quality);
    out.push(b'\n');
}

/// Scan `q` for any byte outside `[33, 126]`. Written as a branchless OR
/// fold so LLVM vectorizes it (the earlier `.iter().any()` short-circuits
/// on first match, which blocks auto-vectorization and made this the top
/// self-time hotspot in `append_fastq_record`).
#[inline]
fn any_invalid_quality_byte(q: &[u8]) -> bool {
    // Valid bytes are 33..=126. `b.wrapping_sub(33)` is in 0..=93 iff valid,
    // so `> 93` flags everything outside the range in one comparison.
    let mut bad: u8 = 0;
    for &b in q {
        bad |= (b.wrapping_sub(MIN_QUAL_BYTE) > (MAX_QUAL_BYTE - MIN_QUAL_BYTE)) as u8;
    }
    bad != 0
}

/// Quality validation: returns the input unchanged on the fast path, or a
/// corrected owned Vec on pad/sanitize. Keeps [`append_fastq_record`]'s top
/// level short and lets the hot path `Cow::Borrowed` straight through.
fn repair_quality<'a>(
    quality: &'a [u8],
    len: usize,
    diag: Option<&IntegrityDiag>,
) -> std::borrow::Cow<'a, [u8]> {
    use std::borrow::Cow;

    if quality.len() != len {
        if let Some(d) = diag {
            d.quality_length_mismatches.fetch_add(1, Ordering::Relaxed);
        } else {
            tracing::warn!(
                "quality length ({}) != sequence length ({}) for spot; padding/truncating",
                quality.len(),
                len,
            );
        }
        let mut q = quality[..quality.len().min(len)].to_vec();
        q.resize(len, FALLBACK_QUAL_BYTE);
        return Cow::Owned(q);
    }

    if any_invalid_quality_byte(quality) {
        if let Some(d) = diag {
            d.quality_invalid_bytes.fetch_add(1, Ordering::Relaxed);
        } else {
            tracing::warn!("quality contains invalid bytes outside [33, 126] range; sanitizing");
        }
        let corrected: Vec<u8> = quality
            .iter()
            .map(|&b| {
                if (MIN_QUAL_BYTE..=MAX_QUAL_BYTE).contains(&b) {
                    b
                } else {
                    FALLBACK_QUAL_BYTE
                }
            })
            .collect();
        return Cow::Owned(corrected);
    }

    Cow::Borrowed(quality)
}

/// Shared defline body: `{run}.{spot} {description} length={len}`.
/// Used by both the `@` line and the `>` line (FASTA).
fn append_defline_body(
    out: &mut Vec<u8>,
    run_name: &str,
    spot_number: &[u8],
    description: &[u8],
    len_str: &str,
) {
    out.extend_from_slice(run_name.as_bytes());
    out.push(b'.');
    out.extend_from_slice(spot_number);
    out.push(b' ');
    out.extend_from_slice(description);
    out.extend_from_slice(b" length=");
    out.extend_from_slice(len_str.as_bytes());
}

/// Byte length of the defline body (no `@`/`>` prefix, no trailing `\n`).
fn defline_body_len(
    run_name: &str,
    spot_number: &[u8],
    description: &[u8],
    len_str: &str,
) -> usize {
    run_name.len() + 1 + spot_number.len() + 1 + description.len() + 8 + len_str.len()
}

/// Format a single read segment into a FASTA record (no quality line).
///
/// Defline format: `>{run}.{spot_num} {description} length={len}`
///
/// Bases with Phred quality 0 are NOT replaced with N in FASTA mode since
/// no quality information is available to the caller.
pub fn format_fasta_read(
    run_name: &str,
    spot_number: &[u8],
    original_name: Option<&[u8]>,
    sequence: &[u8],
) -> FastqRecord {
    let mut data = Vec::new();
    append_fasta_record(&mut data, run_name, spot_number, original_name, sequence);
    FastqRecord { data }
}

/// Append a FASTA record to `out` — hot-path variant that avoids a
/// per-record Vec allocation. Semantics match [`format_fasta_read`].
#[inline]
pub fn append_fasta_record(
    out: &mut Vec<u8>,
    run_name: &str,
    spot_number: &[u8],
    original_name: Option<&[u8]>,
    sequence: &[u8],
) {
    let len = sequence.len();
    let description = original_name.unwrap_or(spot_number);

    let mut itoa_buf = itoa::Buffer::new();
    let len_str = itoa_buf.format(len);
    let defline_body_len = defline_body_len(run_name, spot_number, description, len_str);

    // >defline\nseq\n
    out.reserve(1 + defline_body_len + 1 + len + 1);

    out.push(b'>');
    append_defline_body(out, run_name, spot_number, description, len_str);
    out.push(b'\n');

    out.extend_from_slice(sequence);
    out.push(b'\n');
}

/// A read segment extracted from a spot, after filtering.
struct ReadSegment<'a> {
    sequence: &'a [u8],
    quality: &'a [u8],
}

/// Format a spot into FASTQ records, routing each to the appropriate output slot.
///
/// Returns `(OutputSlot, FastqRecord)` pairs. The caller is responsible for
/// writing each record to the file corresponding to its slot.
///
/// Filtering rules applied in order:
/// 1. Technical reads are dropped when `config.skip_technical` is `true`.
/// 2. Reads shorter than `config.min_read_len` are dropped.
///
/// Routing depends on `config.split_mode`:
/// - **Split3**: 2 biological reads -> `Read1` + `Read2`; otherwise each to `Unpaired`.
/// - **SplitFiles**: Nth surviving read -> `ReadN(N)` (0-indexed).
/// - **SplitSpot**: all reads -> `Single`.
/// - **Interleaved**: all reads -> `Single` (R1/R2 alternating in one file).
pub fn format_spot(
    spot: &SpotRecord,
    run_name: &str,
    config: &FastqConfig,
) -> Vec<(OutputSlot, FastqRecord)> {
    let segments = filter_spot_segments(spot, config);
    if segments.is_empty() {
        return Vec::new();
    }
    route_segments_to_slots(&segments, &spot.name, run_name, config.split_mode)
}

/// Split a spot's concatenated sequence/quality into per-read segments,
/// applying the filters governed by [`FastqConfig`] (zero-length, technical,
/// min-length). Extracted from [`format_spot`] so the filter invariants can
/// be tested without also exercising the output-slot routing.
fn filter_spot_segments<'a>(spot: &'a SpotRecord, config: &FastqConfig) -> Vec<ReadSegment<'a>> {
    let mut segments: Vec<ReadSegment<'a>> = Vec::with_capacity(spot.read_lengths.len());
    let mut offset: usize = 0;

    for (i, &rlen) in spot.read_lengths.iter().enumerate() {
        let rlen = rlen as usize;
        let end = offset + rlen;

        // Bounds check: if the read extends past our data, skip it.
        if end > spot.sequence.len() || end > spot.quality.len() {
            break;
        }

        let seq = &spot.sequence[offset..end];
        let qual = &spot.quality[offset..end];
        offset = end;

        // Filter: skip zero-length reads — fasterq-dump drops these.
        // (Some SRA schemas store empty placeholder segments alongside
        // real reads; including them inflates the apparent segment count
        // and mis-routes single-end spots into `_1.fastq`/`_2.fastq`.)
        if rlen == 0 {
            continue;
        }

        // Filter: skip technical reads if configured.
        if config.skip_technical
            && let Some(&rtype) = spot.read_types.get(i)
            && rtype != 0
        {
            continue;
        }

        // Filter: skip reads shorter than the minimum length.
        if let Some(min_len) = config.min_read_len
            && (rlen as u32) < min_len
        {
            continue;
        }

        segments.push(ReadSegment {
            sequence: seq,
            quality: qual,
        });
    }

    segments
}

/// Route the filtered `segments` to output slots per [`SplitMode`].
/// Extracted from [`format_spot`]: the routing decision tree is the only
/// thing that varies across split modes, and pulling it out makes the
/// Split3 / Interleaved / SplitFiles / SplitSpot behaviors trivial to
/// eyeball side-by-side.
fn route_segments_to_slots(
    segments: &[ReadSegment<'_>],
    spot_name: &[u8],
    run_name: &str,
    split_mode: SplitMode,
) -> Vec<(OutputSlot, FastqRecord)> {
    let mut results = Vec::with_capacity(segments.len());
    let format =
        |seg: &ReadSegment<'_>| format_read(run_name, spot_name, None, seg.sequence, seg.quality);

    match split_mode {
        SplitMode::Split3 => {
            if segments.len() == 2 {
                results.push((OutputSlot::Read1, format(&segments[0])));
                results.push((OutputSlot::Read2, format(&segments[1])));
            } else {
                for seg in segments {
                    results.push((OutputSlot::Unpaired, format(seg)));
                }
            }
        }
        SplitMode::Interleaved | SplitMode::SplitSpot => {
            for seg in segments {
                results.push((OutputSlot::Single, format(seg)));
            }
        }
        SplitMode::SplitFiles => {
            for (file_idx, seg) in segments.iter().enumerate() {
                results.push((OutputSlot::ReadN(file_idx as u32), format(seg)));
            }
        }
    }

    results
}

// ---------------------------------------------------------------------------
// Compression mode
// ---------------------------------------------------------------------------

/// Compression mode for output files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionMode {
    /// No compression.
    None,
    /// Gzip compression (parallel pigz-style).
    Gzip { level: u32 },
    /// Zstd compression (native multi-threaded).
    Zstd { level: i32, threads: u32 },
}

// ---------------------------------------------------------------------------
// Output filename generation
// ---------------------------------------------------------------------------

/// Generate an output file name for a given slot.
///
/// Examples (FASTQ, gzip):
/// - `OutputSlot::Single`   -> `SRR123456.fastq.gz`
/// - `OutputSlot::Read1`    -> `SRR123456_1.fastq.gz`
///
/// Examples (FASTA, zstd):
/// - `OutputSlot::Single`   -> `SRR123456.fasta.zst`
pub fn output_filename(
    accession: &str,
    slot: OutputSlot,
    fasta: bool,
    compression: &CompressionMode,
) -> String {
    let base = if fasta { ".fasta" } else { ".fastq" };
    let ext = match compression {
        CompressionMode::None => base.to_string(),
        CompressionMode::Gzip { .. } => format!("{base}.gz"),
        CompressionMode::Zstd { .. } => format!("{base}.zst"),
    };
    match slot {
        OutputSlot::Single => format!("{accession}{ext}"),
        OutputSlot::Read1 => format!("{accession}_1{ext}"),
        OutputSlot::Read2 => format!("{accession}_2{ext}"),
        // Matches fasterq-dump: split-3 orphans and single-end runs both
        // land in `{accession}.fastq` (no `_0` suffix).
        OutputSlot::Unpaired => format!("{accession}{ext}"),
        // ReadN is 0-indexed internally but 1-indexed in the filename to match
        // the convention used by SRA tools (read number, not array index).
        OutputSlot::ReadN(n) => format!("{accession}_{}{ext}", n + 1),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /// Build a simple single-read spot with the given sequence.
    fn single_read_spot(name: &[u8], seq: &[u8], qual: &[u8]) -> SpotRecord {
        let len = seq.len() as u32;
        SpotRecord {
            name: name.to_vec(),
            sequence: seq.to_vec(),
            quality: qual.to_vec(),
            read_lengths: vec![len],
            read_types: vec![0],  // biological
            read_filter: vec![0], // pass
            spot_group: Vec::new(),
        }
    }

    /// Build a paired-read spot (two biological reads concatenated).
    fn paired_read_spot(
        name: &[u8],
        seq1: &[u8],
        qual1: &[u8],
        seq2: &[u8],
        qual2: &[u8],
    ) -> SpotRecord {
        let mut seq = seq1.to_vec();
        seq.extend_from_slice(seq2);
        let mut qual = qual1.to_vec();
        qual.extend_from_slice(qual2);
        SpotRecord {
            name: name.to_vec(),
            sequence: seq,
            quality: qual,
            read_lengths: vec![seq1.len() as u32, seq2.len() as u32],
            read_types: vec![0, 0], // both biological
            read_filter: vec![0, 0],
            spot_group: Vec::new(),
        }
    }

    /// Build a spot with a leading technical read followed by two biological reads.
    fn spot_with_technical(
        name: &[u8],
        tech_seq: &[u8],
        tech_qual: &[u8],
        bio1_seq: &[u8],
        bio1_qual: &[u8],
        bio2_seq: &[u8],
        bio2_qual: &[u8],
    ) -> SpotRecord {
        let mut seq = tech_seq.to_vec();
        seq.extend_from_slice(bio1_seq);
        seq.extend_from_slice(bio2_seq);
        let mut qual = tech_qual.to_vec();
        qual.extend_from_slice(bio1_qual);
        qual.extend_from_slice(bio2_qual);
        SpotRecord {
            name: name.to_vec(),
            sequence: seq,
            quality: qual,
            read_lengths: vec![
                tech_seq.len() as u32,
                bio1_seq.len() as u32,
                bio2_seq.len() as u32,
            ],
            read_types: vec![1, 0, 0], // technical, biological, biological
            read_filter: vec![0, 0, 0],
            spot_group: Vec::new(),
        }
    }

    fn default_config() -> FastqConfig {
        FastqConfig::default()
    }

    // -----------------------------------------------------------------------
    // Defline formatting
    // -----------------------------------------------------------------------

    #[test]
    fn defline_format_single_read() {
        let spot = single_read_spot(b"42", b"ACGT", b"????");
        let config = default_config();
        let results = format_spot(&spot, "SRR123456", &config);

        assert_eq!(results.len(), 1);
        let data = &results[0].1.data;
        let text = std::str::from_utf8(data).unwrap();
        assert!(text.starts_with("@SRR123456.42 42 length=4\n"));
    }

    #[test]
    fn defline_format_paired_read() {
        let spot = paired_read_spot(b"99", b"AAAA", b"????", b"CCCC", b"????");
        let config = default_config();
        let results = format_spot(&spot, "SRR999", &config);

        assert_eq!(results.len(), 2);
        let r1 = std::str::from_utf8(&results[0].1.data).unwrap();
        let r2 = std::str::from_utf8(&results[1].1.data).unwrap();
        assert!(r1.starts_with("@SRR999.99 99 length=4\n"));
        assert!(r2.starts_with("@SRR999.99 99 length=4\n"));
    }

    #[test]
    fn full_fastq_record_format() {
        let spot = single_read_spot(b"1", b"ACGT", b"IIII");
        let config = FastqConfig {
            split_mode: SplitMode::SplitSpot,
            ..default_config()
        };
        let results = format_spot(&spot, "SRR1", &config);

        assert_eq!(results.len(), 1);
        let text = std::str::from_utf8(&results[0].1.data).unwrap();
        assert_eq!(text, "@SRR1.1 1 length=4\nACGT\n+SRR1.1 1 length=4\nIIII\n");
    }

    // -----------------------------------------------------------------------
    // Single-read spot (unpaired)
    // -----------------------------------------------------------------------

    #[test]
    fn single_read_routes_to_unpaired_in_split3() {
        let spot = single_read_spot(b"1", b"ACGT", b"????");
        let config = FastqConfig {
            split_mode: SplitMode::Split3,
            ..default_config()
        };
        let results = format_spot(&spot, "SRR1", &config);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, OutputSlot::Unpaired);
    }

    #[test]
    fn single_read_routes_to_single_in_split_spot() {
        let spot = single_read_spot(b"1", b"ACGT", b"????");
        let config = FastqConfig {
            split_mode: SplitMode::SplitSpot,
            ..default_config()
        };
        let results = format_spot(&spot, "SRR1", &config);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, OutputSlot::Single);
    }

    #[test]
    fn single_read_routes_to_read_n0_in_split_files() {
        let spot = single_read_spot(b"1", b"ACGT", b"????");
        let config = FastqConfig {
            split_mode: SplitMode::SplitFiles,
            ..default_config()
        };
        let results = format_spot(&spot, "SRR1", &config);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, OutputSlot::ReadN(0));
    }

    #[test]
    fn single_read_routes_to_single_in_interleaved() {
        let spot = single_read_spot(b"1", b"ACGT", b"????");
        let config = FastqConfig {
            split_mode: SplitMode::Interleaved,
            ..default_config()
        };
        let results = format_spot(&spot, "SRR1", &config);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, OutputSlot::Single);
    }

    // -----------------------------------------------------------------------
    // Paired-read spot
    // -----------------------------------------------------------------------

    #[test]
    fn paired_read_routes_to_read1_read2_in_split3() {
        let spot = paired_read_spot(b"1", b"AAAA", b"????", b"CCCC", b"????");
        let config = FastqConfig {
            split_mode: SplitMode::Split3,
            ..default_config()
        };
        let results = format_spot(&spot, "SRR1", &config);

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, OutputSlot::Read1);
        assert_eq!(results[1].0, OutputSlot::Read2);
    }

    #[test]
    fn paired_read_routes_to_single_in_interleaved() {
        let spot = paired_read_spot(b"1", b"AAAA", b"????", b"CCCC", b"????");
        let config = FastqConfig {
            split_mode: SplitMode::Interleaved,
            ..default_config()
        };
        let results = format_spot(&spot, "SRR1", &config);

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, OutputSlot::Single);
        assert_eq!(results[1].0, OutputSlot::Single);
    }

    #[test]
    fn paired_read_routes_to_read_n_in_split_files() {
        let spot = paired_read_spot(b"1", b"AAAA", b"????", b"CCCC", b"????");
        let config = FastqConfig {
            split_mode: SplitMode::SplitFiles,
            ..default_config()
        };
        let results = format_spot(&spot, "SRR1", &config);

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, OutputSlot::ReadN(0));
        assert_eq!(results[1].0, OutputSlot::ReadN(1));
    }

    #[test]
    fn paired_read_routes_to_single_in_split_spot() {
        let spot = paired_read_spot(b"1", b"AAAA", b"????", b"CCCC", b"????");
        let config = FastqConfig {
            split_mode: SplitMode::SplitSpot,
            ..default_config()
        };
        let results = format_spot(&spot, "SRR1", &config);

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, OutputSlot::Single);
        assert_eq!(results[1].0, OutputSlot::Single);
    }

    #[test]
    fn paired_read_sequence_content_is_correct() {
        let spot = paired_read_spot(b"10", b"AACC", b"IIII", b"GGTT", b"!!!!");
        let config = FastqConfig {
            split_mode: SplitMode::Split3,
            ..default_config()
        };
        let results = format_spot(&spot, "SRR1", &config);

        assert_eq!(results.len(), 2);
        let r1 = std::str::from_utf8(&results[0].1.data).unwrap();
        let r2 = std::str::from_utf8(&results[1].1.data).unwrap();

        assert_eq!(
            r1,
            "@SRR1.10 10 length=4\nAACC\n+SRR1.10 10 length=4\nIIII\n"
        );
        // N-masking is now handled in the pipeline layer, not format_read.
        // Bases pass through unchanged regardless of quality.
        assert_eq!(
            r2,
            "@SRR1.10 10 length=4\nGGTT\n+SRR1.10 10 length=4\n!!!!\n"
        );
    }

    // -----------------------------------------------------------------------
    // Technical read filtering
    // -----------------------------------------------------------------------

    #[test]
    fn technical_read_is_filtered_by_default() {
        let spot = spot_with_technical(
            b"1", b"NNNN", b"!!!!", // technical
            b"AACC", b"IIII", // bio 1
            b"GGTT", b"????", // bio 2
        );
        let config = default_config();
        let results = format_spot(&spot, "SRR1", &config);

        // Only the two biological reads should remain.
        assert_eq!(results.len(), 2);
        let r1 = std::str::from_utf8(&results[0].1.data).unwrap();
        let r2 = std::str::from_utf8(&results[1].1.data).unwrap();
        assert!(r1.contains("AACC"));
        assert!(r2.contains("GGTT"));
    }

    #[test]
    fn technical_read_included_when_skip_technical_false() {
        let spot = spot_with_technical(b"1", b"NNNN", b"!!!!", b"AACC", b"IIII", b"GGTT", b"????");
        let config = FastqConfig {
            skip_technical: false,
            ..default_config()
        };
        let results = format_spot(&spot, "SRR1", &config);

        // All three reads should be present. With 3 reads in Split3 mode, all
        // go to Unpaired because there are not exactly 2.
        assert_eq!(results.len(), 3);
        assert!(
            results
                .iter()
                .all(|(slot, _)| *slot == OutputSlot::Unpaired)
        );
    }

    #[test]
    fn technical_read_filtered_leaves_paired_in_split3() {
        let spot = spot_with_technical(b"5", b"TTTT", b"$$$$", b"AAAA", b"????", b"CCCC", b"????");
        let config = FastqConfig {
            split_mode: SplitMode::Split3,
            ..default_config()
        };
        let results = format_spot(&spot, "SRR1", &config);

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, OutputSlot::Read1);
        assert_eq!(results[1].0, OutputSlot::Read2);
    }

    // -----------------------------------------------------------------------
    // Minimum read length filtering
    // -----------------------------------------------------------------------

    #[test]
    fn min_read_len_filters_short_reads() {
        let spot = paired_read_spot(b"1", b"AC", b"??", b"GGTTAA", b"??????");
        let config = FastqConfig {
            min_read_len: Some(3),
            ..default_config()
        };
        let results = format_spot(&spot, "SRR1", &config);

        // First read (len=2) should be filtered, only second remains.
        assert_eq!(results.len(), 1);
        let text = std::str::from_utf8(&results[0].1.data).unwrap();
        assert!(text.contains("GGTTAA"));
        // With only 1 surviving read in Split3, it goes to Unpaired.
        assert_eq!(results[0].0, OutputSlot::Unpaired);
    }

    #[test]
    fn min_read_len_keeps_reads_at_exact_threshold() {
        let spot = single_read_spot(b"1", b"ACG", b"???");
        let config = FastqConfig {
            min_read_len: Some(3),
            ..default_config()
        };
        let results = format_spot(&spot, "SRR1", &config);

        assert_eq!(results.len(), 1);
    }

    #[test]
    fn min_read_len_filters_all_reads_returns_empty() {
        let spot = paired_read_spot(b"1", b"AC", b"??", b"GT", b"??");
        let config = FastqConfig {
            min_read_len: Some(10),
            ..default_config()
        };
        let results = format_spot(&spot, "SRR1", &config);

        assert!(results.is_empty());
    }

    #[test]
    fn min_read_len_none_keeps_all_reads() {
        let spot = paired_read_spot(b"1", b"A", b"?", b"C", b"?");
        let config = FastqConfig {
            min_read_len: None,
            ..default_config()
        };
        let results = format_spot(&spot, "SRR1", &config);

        assert_eq!(results.len(), 2);
    }

    // -----------------------------------------------------------------------
    // Zero-length read filtering
    //
    // Regression for iter-1 validation: runs like DRR032766 (SINGLE-layout
    // on paper) declare two reads per spot where one has length 0. Without
    // filtering, sracha emitted `_1.fastq` + `_2.fastq` while fasterq-dump
    // dropped the empty segment and wrote a single `.fastq`.
    // -----------------------------------------------------------------------

    #[test]
    fn zero_length_trailing_segment_filtered_in_split3() {
        // Build a spot manually: 4bp biological read + 0bp empty segment.
        let spot = SpotRecord {
            name: b"1".to_vec(),
            sequence: b"ACGT".to_vec(),
            quality: b"????".to_vec(),
            read_lengths: vec![4, 0],
            read_types: vec![0, 0],
            read_filter: vec![0, 0],
            spot_group: Vec::new(),
        };
        let config = FastqConfig {
            split_mode: SplitMode::Split3,
            ..default_config()
        };
        let results = format_spot(&spot, "SRR1", &config);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, OutputSlot::Unpaired);
    }

    #[test]
    fn zero_length_leading_segment_filtered_in_split3() {
        // Flipped order: empty first, then a biological read.
        let spot = SpotRecord {
            name: b"1".to_vec(),
            sequence: b"ACGT".to_vec(),
            quality: b"????".to_vec(),
            read_lengths: vec![0, 4],
            read_types: vec![0, 0],
            read_filter: vec![0, 0],
            spot_group: Vec::new(),
        };
        let config = FastqConfig {
            split_mode: SplitMode::Split3,
            ..default_config()
        };
        let results = format_spot(&spot, "SRR1", &config);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, OutputSlot::Unpaired);
    }

    #[test]
    fn zero_length_segment_in_paired_drops_to_unpaired() {
        // Paired schema, but one read is empty — collapses to single unpaired.
        let spot = SpotRecord {
            name: b"1".to_vec(),
            sequence: b"ACGTACGT".to_vec(),
            quality: b"????????".to_vec(),
            read_lengths: vec![8, 0],
            read_types: vec![0, 0],
            read_filter: vec![0, 0],
            spot_group: Vec::new(),
        };
        let config = FastqConfig {
            split_mode: SplitMode::Split3,
            ..default_config()
        };
        let results = format_spot(&spot, "SRR1", &config);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, OutputSlot::Unpaired);
    }

    // -----------------------------------------------------------------------
    // Combined filtering
    // -----------------------------------------------------------------------

    #[test]
    fn technical_and_min_len_filter_combined() {
        // Spot: tech(4bp) + bio(2bp) + bio(6bp)
        let spot = spot_with_technical(b"1", b"NNNN", b"!!!!", b"AC", b"??", b"GGTTAA", b"??????");
        let config = FastqConfig {
            skip_technical: true,
            min_read_len: Some(3),
            ..default_config()
        };
        let results = format_spot(&spot, "SRR1", &config);

        // Technical filtered, bio(2bp) filtered by min_len, only bio(6bp) remains.
        assert_eq!(results.len(), 1);
        let text = std::str::from_utf8(&results[0].1.data).unwrap();
        assert!(text.contains("GGTTAA"));
        assert_eq!(results[0].0, OutputSlot::Unpaired);
    }

    // -----------------------------------------------------------------------
    // Edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn empty_spot_returns_empty() {
        let spot = SpotRecord {
            name: b"1".to_vec(),
            sequence: Vec::new(),
            quality: Vec::new(),
            read_lengths: Vec::new(),
            read_types: Vec::new(),
            read_filter: Vec::new(),
            spot_group: Vec::new(),
        };
        let config = default_config();
        let results = format_spot(&spot, "SRR1", &config);
        assert!(results.is_empty());
    }

    #[test]
    fn three_biological_reads_in_split3_are_unpaired() {
        let mut seq = b"AAAA".to_vec();
        seq.extend_from_slice(b"CCCC");
        seq.extend_from_slice(b"GGGG");
        let mut qual = b"????".to_vec();
        qual.extend_from_slice(b"????");
        qual.extend_from_slice(b"????");

        let spot = SpotRecord {
            name: b"1".to_vec(),
            sequence: seq,
            quality: qual,
            read_lengths: vec![4, 4, 4],
            read_types: vec![0, 0, 0],
            read_filter: vec![0, 0, 0],
            spot_group: Vec::new(),
        };

        let config = FastqConfig {
            split_mode: SplitMode::Split3,
            ..default_config()
        };
        let results = format_spot(&spot, "SRR1", &config);

        assert_eq!(results.len(), 3);
        assert!(
            results
                .iter()
                .all(|(slot, _)| *slot == OutputSlot::Unpaired)
        );
    }

    #[test]
    fn three_reads_in_split_files_routes_to_read_n() {
        let mut seq = b"AAAA".to_vec();
        seq.extend_from_slice(b"CCCC");
        seq.extend_from_slice(b"GGGG");
        let mut qual = b"????".to_vec();
        qual.extend_from_slice(b"????");
        qual.extend_from_slice(b"????");

        let spot = SpotRecord {
            name: b"1".to_vec(),
            sequence: seq,
            quality: qual,
            read_lengths: vec![4, 4, 4],
            read_types: vec![0, 0, 0],
            read_filter: vec![0, 0, 0],
            spot_group: Vec::new(),
        };

        let config = FastqConfig {
            split_mode: SplitMode::SplitFiles,
            ..default_config()
        };
        let results = format_spot(&spot, "SRR1", &config);

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, OutputSlot::ReadN(0));
        assert_eq!(results[1].0, OutputSlot::ReadN(1));
        assert_eq!(results[2].0, OutputSlot::ReadN(2));
    }

    #[test]
    fn spot_name_with_special_characters() {
        let spot = single_read_spot(b"spot.123_abc", b"ACGT", b"????");
        let config = default_config();
        let results = format_spot(&spot, "SRR1", &config);

        let text = std::str::from_utf8(&results[0].1.data).unwrap();
        assert!(text.starts_with("@SRR1.spot.123_abc spot.123_abc length=4\n"));
    }

    // -----------------------------------------------------------------------
    // Output filename generation
    // -----------------------------------------------------------------------

    #[test]
    fn output_filename_single_gzip() {
        let gz = CompressionMode::Gzip { level: 6 };
        assert_eq!(
            output_filename("SRR123456", OutputSlot::Single, false, &gz),
            "SRR123456.fastq.gz"
        );
    }

    #[test]
    fn output_filename_single_no_gzip() {
        assert_eq!(
            output_filename(
                "SRR123456",
                OutputSlot::Single,
                false,
                &CompressionMode::None
            ),
            "SRR123456.fastq"
        );
    }

    #[test]
    fn output_filename_read1() {
        let gz = CompressionMode::Gzip { level: 6 };
        assert_eq!(
            output_filename("SRR123456", OutputSlot::Read1, false, &gz),
            "SRR123456_1.fastq.gz"
        );
    }

    #[test]
    fn output_filename_read2() {
        let gz = CompressionMode::Gzip { level: 6 };
        assert_eq!(
            output_filename("SRR123456", OutputSlot::Read2, false, &gz),
            "SRR123456_2.fastq.gz"
        );
    }

    #[test]
    fn output_filename_unpaired() {
        // Regression guard: fasterq-dump names split-3 orphans (and single-end
        // runs) plain `{acc}.fastq`, not `{acc}_0.fastq`. Divergence here
        // causes FAIL_MISSING in validation comparisons.
        let gz = CompressionMode::Gzip { level: 6 };
        assert_eq!(
            output_filename("SRR123456", OutputSlot::Unpaired, false, &gz),
            "SRR123456.fastq.gz"
        );
    }

    #[test]
    fn output_filename_read_n_zero_indexed() {
        let gz = CompressionMode::Gzip { level: 6 };
        // ReadN(0) -> _1 in the filename (1-indexed).
        assert_eq!(
            output_filename("SRR123456", OutputSlot::ReadN(0), false, &gz),
            "SRR123456_1.fastq.gz"
        );
    }

    #[test]
    fn output_filename_read_n_higher() {
        assert_eq!(
            output_filename(
                "SRR123456",
                OutputSlot::ReadN(2),
                false,
                &CompressionMode::None
            ),
            "SRR123456_3.fastq"
        );
    }

    #[test]
    fn output_filename_unpaired_no_gzip() {
        assert_eq!(
            output_filename(
                "SRR999",
                OutputSlot::Unpaired,
                false,
                &CompressionMode::None
            ),
            "SRR999.fastq"
        );
    }

    #[test]
    fn output_filename_read1_no_gzip() {
        assert_eq!(
            output_filename("SRR999", OutputSlot::Read1, false, &CompressionMode::None),
            "SRR999_1.fastq"
        );
    }

    #[test]
    fn output_filename_read2_no_gzip() {
        assert_eq!(
            output_filename("SRR999", OutputSlot::Read2, false, &CompressionMode::None),
            "SRR999_2.fastq"
        );
    }

    #[test]
    fn output_filename_fasta_gzip() {
        let gz = CompressionMode::Gzip { level: 6 };
        assert_eq!(
            output_filename("SRR123456", OutputSlot::Single, true, &gz),
            "SRR123456.fasta.gz"
        );
    }

    #[test]
    fn output_filename_fasta_no_compression() {
        assert_eq!(
            output_filename("SRR123456", OutputSlot::Read1, true, &CompressionMode::None),
            "SRR123456_1.fasta"
        );
    }

    #[test]
    fn output_filename_fastq_zstd() {
        let zst = CompressionMode::Zstd {
            level: 3,
            threads: 4,
        };
        assert_eq!(
            output_filename("SRR123456", OutputSlot::Single, false, &zst),
            "SRR123456.fastq.zst"
        );
    }

    #[test]
    fn output_filename_fasta_zstd() {
        let zst = CompressionMode::Zstd {
            level: 3,
            threads: 4,
        };
        assert_eq!(
            output_filename("SRR123456", OutputSlot::Read2, true, &zst),
            "SRR123456_2.fasta.zst"
        );
    }

    // -----------------------------------------------------------------------
    // SplitMode Display
    // -----------------------------------------------------------------------

    #[test]
    fn split_mode_display() {
        assert_eq!(SplitMode::Split3.to_string(), "split-3");
        assert_eq!(SplitMode::SplitFiles.to_string(), "split-files");
        assert_eq!(SplitMode::SplitSpot.to_string(), "split-spot");
        assert_eq!(SplitMode::Interleaved.to_string(), "interleaved");
    }

    // -----------------------------------------------------------------------
    // FastqConfig default
    // -----------------------------------------------------------------------

    #[test]
    fn fastq_config_default_values() {
        let config = FastqConfig::default();
        assert_eq!(config.split_mode, SplitMode::Split3);
        assert!(config.skip_technical);
        assert!(config.min_read_len.is_none());
    }

    // -----------------------------------------------------------------------
    // format_read quality validation
    // -----------------------------------------------------------------------

    #[test]
    fn format_read_pads_short_quality() {
        let rec = format_read("RUN", b"1", None, b"ACGT", b"II");
        let text = std::str::from_utf8(&rec.data).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines[1], "ACGT");
        assert_eq!(lines[3].len(), 4);
        assert!(lines[3].starts_with("II"));
        assert!(lines[3].ends_with("??"));
    }

    #[test]
    fn format_read_truncates_long_quality() {
        let rec = format_read("RUN", b"1", None, b"AC", b"IIII");
        let text = std::str::from_utf8(&rec.data).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines[1], "AC");
        assert_eq!(lines[3], "II");
    }

    #[test]
    fn format_read_sanitizes_invalid_quality_bytes() {
        let qual = &[b'I', 10u8, 0u8, b'I'];
        let rec = format_read("RUN", b"1", None, b"ACGT", qual);
        let text = std::str::from_utf8(&rec.data).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines[1], "ACGT");
        assert_eq!(lines[3].len(), 4);
        assert_eq!(lines[3], "I??I");
    }

    #[test]
    fn format_read_valid_quality_passthrough() {
        let rec = format_read("RUN", b"1", None, b"ACGT", b"IIII");
        let text = std::str::from_utf8(&rec.data).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines[0], "@RUN.1 1 length=4");
        assert_eq!(lines[1], "ACGT");
        assert_eq!(lines[2], "+RUN.1 1 length=4");
        assert_eq!(lines[3], "IIII");
    }

    #[test]
    fn format_read_with_original_name() {
        let rec = format_read(
            "SRR1",
            b"42",
            Some(b"INSTRUMENT:1:FLOW:1:1234:5678"),
            b"ACGT",
            b"IIII",
        );
        let text = std::str::from_utf8(&rec.data).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines[0], "@SRR1.42 INSTRUMENT:1:FLOW:1:1234:5678 length=4");
        assert_eq!(lines[2], "+SRR1.42 INSTRUMENT:1:FLOW:1:1234:5678 length=4");
    }

    #[test]
    fn format_read_no_n_masking() {
        // N-masking is now handled upstream in the pipeline layer.
        // format_read() should pass bases through unchanged regardless of quality.
        let rec = format_read("RUN", b"1", None, b"ACGT", b"#I#I");
        let text = std::str::from_utf8(&rec.data).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines[1], "ACGT"); // bases pass through unchanged

        let rec2 = format_read("RUN", b"1", None, b"ACGT", b"!I!I");
        let text2 = std::str::from_utf8(&rec2.data).unwrap();
        let lines2: Vec<&str> = text2.lines().collect();
        assert_eq!(lines2[1], "ACGT");
    }

    // -----------------------------------------------------------------------
    // IntegrityDiag + format_read_with_diag counter plumbing
    // -----------------------------------------------------------------------

    #[test]
    fn integrity_diag_default_is_clean() {
        let d = IntegrityDiag::default();
        assert!(!d.any());
        assert!(d.summary().contains("quality_length_mismatches=0"));
        assert!(d.summary().contains("truncated_spots=0"));
    }

    #[test]
    fn integrity_diag_any_fires_on_any_counter() {
        let d = IntegrityDiag::default();
        assert!(!d.any());
        d.quality_overruns.fetch_add(1, Ordering::Relaxed);
        assert!(d.any());
        let s = d.summary();
        assert!(s.contains("quality_overruns=1"), "summary was: {s}");
    }

    #[test]
    fn format_read_with_diag_counts_length_mismatch() {
        let diag = IntegrityDiag::default();
        // sequence 4 bytes, quality 2 bytes → pad path, mismatch counter bumps.
        let _ = format_read_with_diag("RUN", b"1", None, b"ACGT", b"II", Some(&diag));
        assert_eq!(
            diag.quality_length_mismatches.load(Ordering::Relaxed),
            1,
            "expected one quality-length-mismatch count"
        );
        assert_eq!(diag.quality_invalid_bytes.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn format_read_with_diag_counts_invalid_bytes() {
        let diag = IntegrityDiag::default();
        // sequence 4 bytes, quality 4 bytes but contains invalid 0x00 / 0x0a.
        let _ = format_read_with_diag(
            "RUN",
            b"1",
            None,
            b"ACGT",
            &[b'I', 10u8, 0u8, b'I'],
            Some(&diag),
        );
        assert_eq!(diag.quality_length_mismatches.load(Ordering::Relaxed), 0);
        assert_eq!(
            diag.quality_invalid_bytes.load(Ordering::Relaxed),
            1,
            "expected one quality-invalid-bytes count"
        );
    }

    #[test]
    fn format_read_with_diag_clean_path_bumps_nothing() {
        let diag = IntegrityDiag::default();
        let _ = format_read_with_diag("RUN", b"1", None, b"ACGT", b"IIII", Some(&diag));
        assert!(!diag.any(), "clean call must not mutate counters");
    }

    #[test]
    fn format_read_without_diag_falls_back_to_tracing() {
        // With diag=None, format_read_with_diag must still produce a
        // correctly padded record (the pre-existing tracing::warn path).
        let rec = format_read_with_diag("RUN", b"1", None, b"ACGT", b"II", None);
        let text = std::str::from_utf8(&rec.data).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines[1].len(), lines[3].len());
        assert_eq!(lines[1], "ACGT");
    }
}
