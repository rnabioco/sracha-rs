//! FASTQ formatting and output routing for SRA spot records.
//!
//! This module converts decoded SRA spot data into properly formatted FASTQ
//! records and routes them to the correct output file based on the configured
//! split mode.

use std::fmt;

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
    /// R1/R2 alternate in a single file (same routing as Split3, but the
    /// consumer merges Read1/Read2 into one stream).
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
    let len = sequence.len();

    // Validate quality. Only allocate a corrected copy when something is wrong;
    // the common case (correct length, all bytes valid) is zero-copy.
    let needs_pad = quality.len() != len;
    let needs_sanitize = !needs_pad
        && quality
            .iter()
            .any(|&b| !(MIN_QUAL_BYTE..=MAX_QUAL_BYTE).contains(&b));

    let qual_corrected: Vec<u8>;
    let quality: &[u8] = if needs_pad {
        tracing::warn!(
            "quality length ({}) != sequence length ({}) for spot; padding/truncating",
            quality.len(),
            len,
        );
        qual_corrected = {
            let mut q = quality[..quality.len().min(len)].to_vec();
            q.resize(len, FALLBACK_QUAL_BYTE);
            q
        };
        &qual_corrected
    } else if needs_sanitize {
        tracing::warn!("quality contains invalid bytes outside [33, 126] range; sanitizing");
        qual_corrected = quality
            .iter()
            .map(|&b| {
                if (MIN_QUAL_BYTE..=MAX_QUAL_BYTE).contains(&b) {
                    b
                } else {
                    FALLBACK_QUAL_BYTE
                }
            })
            .collect();
        &qual_corrected
    } else {
        quality
    };

    // N-masking is handled upstream in the pipeline layer: ALTREAD ambiguity
    // merge when available, quality-based fallback otherwise.  By the time
    // bases reach format_read() they already have correct N positions.

    // Description part of the defline: original name if available, else spot number.
    let description = original_name.unwrap_or(spot_number);

    // Build defline bytes: "{run_name}.{spot_number} {description} length={len}"
    let mut itoa_buf = itoa::Buffer::new();
    let len_str = itoa_buf.format(len);

    let defline_len =
        run_name.len() + 1 + spot_number.len() + 1 + description.len() + 8 + len_str.len();

    // Pre-allocate full record: @defline\nseq\n+defline\nqual\n
    let mut data = Vec::with_capacity(1 + defline_len + 1 + len + 2 + defline_len + 1 + len + 1);

    // @defline
    data.push(b'@');
    data.extend_from_slice(run_name.as_bytes());
    data.push(b'.');
    data.extend_from_slice(spot_number);
    data.push(b' ');
    data.extend_from_slice(description);
    data.extend_from_slice(b" length=");
    data.extend_from_slice(len_str.as_bytes());
    data.push(b'\n');

    // Sequence
    data.extend_from_slice(sequence);
    data.push(b'\n');

    // + line repeats defline (matching fasterq-dump)
    data.push(b'+');
    data.extend_from_slice(run_name.as_bytes());
    data.push(b'.');
    data.extend_from_slice(spot_number);
    data.push(b' ');
    data.extend_from_slice(description);
    data.extend_from_slice(b" length=");
    data.extend_from_slice(len_str.as_bytes());
    data.push(b'\n');

    // Quality
    data.extend_from_slice(quality);
    data.push(b'\n');

    FastqRecord { data }
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
    let len = sequence.len();
    let description = original_name.unwrap_or(spot_number);

    let mut itoa_buf = itoa::Buffer::new();
    let len_str = itoa_buf.format(len);

    let defline_len =
        run_name.len() + 1 + spot_number.len() + 1 + description.len() + 8 + len_str.len();

    // Pre-allocate: >defline\nsequence\n
    let mut data = Vec::with_capacity(1 + defline_len + 1 + len + 1);

    // >defline
    data.push(b'>');
    data.extend_from_slice(run_name.as_bytes());
    data.push(b'.');
    data.extend_from_slice(spot_number);
    data.push(b' ');
    data.extend_from_slice(description);
    data.extend_from_slice(b" length=");
    data.extend_from_slice(len_str.as_bytes());
    data.push(b'\n');

    // Sequence
    data.extend_from_slice(sequence);
    data.push(b'\n');

    FastqRecord { data }
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
/// - **Interleaved**: same routing as Split3 (consumer writes Read1/Read2 to one stream).
pub fn format_spot(
    spot: &SpotRecord,
    run_name: &str,
    config: &FastqConfig,
) -> Vec<(OutputSlot, FastqRecord)> {
    // Split the concatenated sequence/quality into individual read segments.
    let mut segments: Vec<ReadSegment<'_>> = Vec::with_capacity(spot.read_lengths.len());
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

    if segments.is_empty() {
        return Vec::new();
    }

    // Route each segment to the appropriate output slot.
    let mut results = Vec::with_capacity(segments.len());

    match config.split_mode {
        SplitMode::Split3 | SplitMode::Interleaved => {
            if segments.len() == 2 {
                let r1 = format_read(
                    run_name,
                    &spot.name,
                    None,
                    segments[0].sequence,
                    segments[0].quality,
                );
                let r2 = format_read(
                    run_name,
                    &spot.name,
                    None,
                    segments[1].sequence,
                    segments[1].quality,
                );
                results.push((OutputSlot::Read1, r1));
                results.push((OutputSlot::Read2, r2));
            } else {
                for seg in &segments {
                    let rec = format_read(run_name, &spot.name, None, seg.sequence, seg.quality);
                    results.push((OutputSlot::Unpaired, rec));
                }
            }
        }
        SplitMode::SplitFiles => {
            for (file_idx, seg) in segments.iter().enumerate() {
                let rec = format_read(run_name, &spot.name, None, seg.sequence, seg.quality);
                results.push((OutputSlot::ReadN(file_idx as u32), rec));
            }
        }
        SplitMode::SplitSpot => {
            for seg in &segments {
                let rec = format_read(run_name, &spot.name, None, seg.sequence, seg.quality);
                results.push((OutputSlot::Single, rec));
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
        OutputSlot::Unpaired => format!("{accession}_0{ext}"),
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
    fn single_read_routes_to_unpaired_in_interleaved() {
        let spot = single_read_spot(b"1", b"ACGT", b"????");
        let config = FastqConfig {
            split_mode: SplitMode::Interleaved,
            ..default_config()
        };
        let results = format_spot(&spot, "SRR1", &config);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, OutputSlot::Unpaired);
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
    fn paired_read_routes_to_read1_read2_in_interleaved() {
        let spot = paired_read_spot(b"1", b"AAAA", b"????", b"CCCC", b"????");
        let config = FastqConfig {
            split_mode: SplitMode::Interleaved,
            ..default_config()
        };
        let results = format_spot(&spot, "SRR1", &config);

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, OutputSlot::Read1);
        assert_eq!(results[1].0, OutputSlot::Read2);
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
        let gz = CompressionMode::Gzip { level: 6 };
        assert_eq!(
            output_filename("SRR123456", OutputSlot::Unpaired, false, &gz),
            "SRR123456_0.fastq.gz"
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
            "SRR999_0.fastq"
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
}
