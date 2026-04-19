//! DNA and quality score encoding/decoding for NCBI SRA VDB files.
//!
//! This module converts packed binary representations of DNA sequences and
//! quality scores into ASCII text suitable for FASTQ output. All functions
//! are pure (no I/O) and operate on byte slices.

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Phred+33 ASCII offset applied when converting numeric quality scores.
pub const QUAL_PHRED_OFFSET: u8 = 33;

/// Quality score assigned to passing reads in SRA-lite files (Q30).
pub const SRA_LITE_PASS_QUAL: u8 = 30;

/// Quality score assigned to rejected reads in SRA-lite files (Q3).
pub const SRA_LITE_REJECT_QUAL: u8 = 3;

// ---------------------------------------------------------------------------
// Lookup tables (const, lives in .rodata)
// ---------------------------------------------------------------------------

/// 4-bit IUPAC ambiguity code lookup (indices 0..=15).
///
/// | bits | base | bits | base |
/// |------|------|------|------|
/// | 0000 | N    | 1000 | T    |
/// | 0001 | A    | 1001 | W    |
/// | 0010 | C    | 1010 | Y    |
/// | 0011 | M    | 1011 | H    |
/// | 0100 | G    | 1100 | K    |
/// | 0101 | R    | 1101 | D    |
/// | 0110 | S    | 1110 | B    |
/// | 0111 | V    | 1111 | N    |
const DNA_4NA: [u8; 16] = [
    b'N', // 0
    b'A', // 1
    b'C', // 2
    b'M', // 3
    b'G', // 4
    b'R', // 5
    b'S', // 6
    b'V', // 7
    b'T', // 8
    b'W', // 9
    b'Y', // 10
    b'H', // 11
    b'K', // 12
    b'D', // 13
    b'B', // 14
    b'N', // 15
];

// ---------------------------------------------------------------------------
// 2na helpers
// ---------------------------------------------------------------------------

/// Byte-level lookup table for 2na decoding: `LUT_2NA[byte]` yields the 4 ASCII
/// bases packed in that byte (MSB-first). Built at compile time so it lives in
/// `.rodata` with zero runtime cost.
const LUT_2NA: [[u8; 4]; 256] = {
    let mut lut = [[0u8; 4]; 256];
    let bases = [b'A', b'C', b'G', b'T'];
    let mut i = 0u16;
    while i < 256 {
        let b = i as u8;
        lut[i as usize] = [
            bases[((b >> 6) & 0x03) as usize],
            bases[((b >> 4) & 0x03) as usize],
            bases[((b >> 2) & 0x03) as usize],
            bases[(b & 0x03) as usize],
        ];
        i += 1;
    }
    lut
};

// ---------------------------------------------------------------------------
// Public API — DNA unpacking
// ---------------------------------------------------------------------------

/// Unpack 2na-encoded DNA into ASCII bases.
///
/// In 2na encoding each byte contains 4 bases packed as 2 bits each, with the
/// most-significant bits holding the first base:
///
/// ```text
/// byte: [b1 b1 b0 b0 | b3 b3 b2 b2]   (bit positions 7..0)
///        ^^^^           ^^^^
///        base 0         base 2
/// ```
///
/// Mapping: `0b00 = A`, `0b01 = C`, `0b10 = G`, `0b11 = T`.
///
/// `num_bases` may be less than `packed.len() * 4` when the last byte contains
/// padding bits.
pub fn unpack_2na(packed: &[u8], num_bases: usize) -> Vec<u8> {
    let full_bytes = num_bases / 4;
    let trailing = num_bases % 4;

    // Pre-size the output. Previously `Vec::with_capacity` + per-iter
    // `extend_from_slice` forced LLVM to emit a capacity check and len
    // writeback inside the hot loop (~33% of its time per `perf
    // annotate`). With a sized Vec and `chunks_exact_mut(4)` the loop
    // becomes load-byte → LUT-load → 4-byte store, with no bookkeeping.
    let mut bases = vec![0u8; num_bases];

    for (chunk, &byte) in bases.chunks_exact_mut(4).zip(packed[..full_bytes].iter()) {
        chunk.copy_from_slice(&LUT_2NA[byte as usize]);
    }

    if trailing > 0 && full_bytes < packed.len() {
        let entry = &LUT_2NA[packed[full_bytes] as usize];
        bases[full_bytes * 4..].copy_from_slice(&entry[..trailing]);
    }

    bases
}

/// Unpack 4na-encoded DNA into ASCII bases.
///
/// In 4na encoding each byte holds 2 bases packed as 4-bit nibbles, high
/// nibble first. The nibble value is an index into the IUPAC ambiguity table
/// (see [`DNA_4NA`]).
///
/// `num_bases` may be less than `packed.len() * 2` when the last byte contains
/// only one meaningful nibble.
pub fn unpack_4na(packed: &[u8], num_bases: usize) -> Vec<u8> {
    // Output truncates when `packed` is shorter than `num_bases` implies; this
    // matches the prior `push`-per-nibble loop.
    let output_len = num_bases.min(packed.len() * 2);
    let full_bytes = output_len / 2;
    let trailing = output_len % 2;

    // Sized upfront so the hot loop drops the capacity check and length
    // writeback — mirrors unpack_2na's pattern.
    let mut bases = vec![0u8; output_len];

    for (chunk, &byte) in bases.chunks_exact_mut(2).zip(packed[..full_bytes].iter()) {
        chunk[0] = DNA_4NA[((byte >> 4) & 0x0F) as usize];
        chunk[1] = DNA_4NA[(byte & 0x0F) as usize];
    }

    if trailing > 0 {
        bases[full_bytes * 2] = DNA_4NA[((packed[full_bytes] >> 4) & 0x0F) as usize];
    }

    bases
}

// ---------------------------------------------------------------------------
// Public API — ALTREAD ambiguity merge
// ---------------------------------------------------------------------------

/// Reverse map from ASCII base to 4na code.
/// Only valid DNA bases are populated: A=1, C=2, G=4, T=8, N=15.
const ASCII_TO_4NA: [u8; 256] = {
    let mut lut = [0u8; 256];
    lut[b'A' as usize] = 1;
    lut[b'C' as usize] = 2;
    lut[b'G' as usize] = 4;
    lut[b'T' as usize] = 8;
    lut[b'N' as usize] = 15;
    lut
};

/// Merge 2na-decoded ASCII bases with an ALTREAD 4na-packed ambiguity mask.
///
/// Used when the physical ALTREAD column stores `INSDC:4na:packed` (2 bases
/// per byte, high nibble first). Reproduces the VDB schema's
/// `bit_or(out_2na_4na_bin, .ALTREAD)` followed by `map<4na→text>`.
#[inline]
pub fn merge_altread(bases: &mut [u8], altread_packed: &[u8], num_bases: usize) {
    let count = bases.len().min(num_bases);
    for i in 0..count {
        let byte_idx = i / 2;
        if byte_idx >= altread_packed.len() {
            break;
        }
        let nibble = if i % 2 == 0 {
            altread_packed[byte_idx] >> 4
        } else {
            altread_packed[byte_idx] & 0x0F
        };
        if nibble == 0 {
            continue;
        }
        let base_4na = ASCII_TO_4NA[bases[i] as usize];
        let merged = base_4na | nibble;
        bases[i] = DNA_4NA[(merged & 0x0F) as usize];
    }
}

/// Merge 2na-decoded ASCII bases with an ALTREAD 4na-bin ambiguity mask.
///
/// Used when the physical ALTREAD column stores `INSDC:4na:bin` (one byte
/// per base, low nibble carries the 4na code). This is the common case for
/// tables that inherit `NCBI:tbl:base_space` or `NCBI:align:tbl:seq`:
///
/// ```text
/// physical column <INSDC:4na:bin>zip_encoding#1 .ALTREAD = trim#1<0,0>(…);
/// ```
///
/// `altread_bytes` is expected to be padded to `num_bases` bytes — one
/// entry per base position (zeros beyond the end of a per-row trim are
/// restored by the caller before calling this function).
#[inline]
pub fn merge_altread_bin(bases: &mut [u8], altread_bytes: &[u8], num_bases: usize) {
    let count = bases.len().min(num_bases).min(altread_bytes.len());
    for i in 0..count {
        let nibble = altread_bytes[i] & 0x0F;
        if nibble == 0 {
            continue;
        }
        let base_4na = ASCII_TO_4NA[bases[i] as usize];
        let merged = base_4na | nibble;
        bases[i] = DNA_4NA[(merged & 0x0F) as usize];
    }
}

// ---------------------------------------------------------------------------
// Public API — Quality scores
// ---------------------------------------------------------------------------

/// Convert binary Phred quality scores to Phred+33 ASCII encoding.
///
/// Each input byte is treated as a numeric Phred score; the output byte is
/// `score + 33` (the standard Sanger / Illumina 1.8+ encoding).
#[inline]
pub fn phred_to_ascii(quality: &[u8]) -> Vec<u8> {
    quality
        .iter()
        .map(|&q| q.saturating_add(QUAL_PHRED_OFFSET))
        .collect()
}

/// Generate a uniform quality string for SRA-lite files.
///
/// SRA-lite runs omit per-base quality scores. Instead, every position gets a
/// single fixed score that indicates whether the read passed the platform
/// quality filter:
///
/// * `pass_filter = true`  -> Q30 (ASCII `?`)
/// * `pass_filter = false` -> Q3  (ASCII `$`)
pub fn sra_lite_quality(length: usize, pass_filter: bool) -> Vec<u8> {
    let score = if pass_filter {
        SRA_LITE_PASS_QUAL
    } else {
        SRA_LITE_REJECT_QUAL
    };
    let ascii = score + QUAL_PHRED_OFFSET;
    vec![ascii; length]
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // 2na tests
    // -----------------------------------------------------------------------

    #[test]
    fn unpack_2na_empty() {
        assert_eq!(unpack_2na(&[], 0), Vec::<u8>::new());
    }

    #[test]
    fn unpack_2na_single_byte_all_four() {
        // Byte 0b00_01_10_11 = 0x1B -> A C G T
        let packed = [0b00_01_10_11u8];
        assert_eq!(unpack_2na(&packed, 4), b"ACGT".to_vec());
    }

    #[test]
    fn unpack_2na_single_byte_all_same() {
        // All T: 0b11_11_11_11 = 0xFF
        assert_eq!(unpack_2na(&[0xFF], 4), b"TTTT".to_vec());
        // All A: 0b00_00_00_00 = 0x00
        assert_eq!(unpack_2na(&[0x00], 4), b"AAAA".to_vec());
    }

    #[test]
    fn unpack_2na_multiple_bytes() {
        // Two bytes: ACGT TGCA
        let packed = [0b00_01_10_11, 0b11_10_01_00];
        assert_eq!(unpack_2na(&packed, 8), b"ACGTTGCA".to_vec());
    }

    #[test]
    fn unpack_2na_partial_last_byte_one() {
        // Only first base from 0b10_00_00_00 -> G
        let packed = [0b10_00_00_00u8];
        assert_eq!(unpack_2na(&packed, 1), b"G".to_vec());
    }

    #[test]
    fn unpack_2na_partial_last_byte_two() {
        // First two bases from 0b01_11_00_00 -> C T
        let packed = [0b01_11_00_00u8];
        assert_eq!(unpack_2na(&packed, 2), b"CT".to_vec());
    }

    #[test]
    fn unpack_2na_partial_last_byte_three() {
        // Three bases from 0b11_01_10_00 -> T C G
        let packed = [0b11_01_10_00u8];
        assert_eq!(unpack_2na(&packed, 3), b"TCG".to_vec());
    }

    #[test]
    fn unpack_2na_multi_byte_partial() {
        // 5 bases across 2 bytes, last byte only uses first base.
        let packed = [0b00_01_10_11, 0b11_00_00_00]; // ACGT + T(padding)
        assert_eq!(unpack_2na(&packed, 5), b"ACGTT".to_vec());
    }

    #[test]
    fn unpack_2na_num_bases_zero_with_data() {
        // If num_bases is 0, output should be empty regardless of data.
        assert_eq!(unpack_2na(&[0xFF, 0xFF], 0), Vec::<u8>::new());
    }

    // -----------------------------------------------------------------------
    // 4na tests
    // -----------------------------------------------------------------------

    #[test]
    fn unpack_4na_empty() {
        assert_eq!(unpack_4na(&[], 0), Vec::<u8>::new());
    }

    #[test]
    fn unpack_4na_single_byte_two_bases() {
        // 0x18 = high nibble 1 (A), low nibble 8 (T)
        assert_eq!(unpack_4na(&[0x18], 2), b"AT".to_vec());
    }

    #[test]
    fn unpack_4na_all_sixteen_codes() {
        // Test every IUPAC nibble value. We pack pairs: (0,1), (2,3), ... (14,15).
        let packed: Vec<u8> = (0u8..=7).map(|i| (i * 2) << 4 | (i * 2 + 1)).collect();
        let result = unpack_4na(&packed, 16);
        let expected = b"NACMGRSVTWYHKDBN";
        assert_eq!(result, expected.to_vec());
    }

    #[test]
    fn unpack_4na_partial_last_byte() {
        // 3 bases across 2 bytes: byte0 = (A, C) = 0x12, byte1 = (G, _) = 0x40
        let packed = [0x12, 0x40];
        assert_eq!(unpack_4na(&packed, 3), b"ACG".to_vec());
    }

    #[test]
    fn unpack_4na_single_base() {
        // High nibble 8 = T, low nibble unused.
        let packed = [0x80];
        assert_eq!(unpack_4na(&packed, 1), b"T".to_vec());
    }

    #[test]
    fn unpack_4na_num_bases_zero_with_data() {
        assert_eq!(unpack_4na(&[0xFF], 0), Vec::<u8>::new());
    }

    #[test]
    fn unpack_4na_ambiguity_codes() {
        // Spot-check a few ambiguity codes.
        // M=3 -> 0x30 high nibble, R=5 -> 0x05 low nibble => 0x35
        assert_eq!(unpack_4na(&[0x35], 2), b"MR".to_vec());
        // S=6, V=7 => 0x67
        assert_eq!(unpack_4na(&[0x67], 2), b"SV".to_vec());
        // W=9, Y=10 => 0x9A
        assert_eq!(unpack_4na(&[0x9A], 2), b"WY".to_vec());
        // H=11, K=12 => 0xBC
        assert_eq!(unpack_4na(&[0xBC], 2), b"HK".to_vec());
        // D=13, B=14 => 0xDE
        assert_eq!(unpack_4na(&[0xDE], 2), b"DB".to_vec());
    }

    // -----------------------------------------------------------------------
    // Quality score tests
    // -----------------------------------------------------------------------

    #[test]
    fn phred_to_ascii_empty() {
        assert_eq!(phred_to_ascii(&[]), Vec::<u8>::new());
    }

    #[test]
    fn phred_to_ascii_known_values() {
        // Q0 -> '!', Q30 -> '?', Q40 -> 'I'
        assert_eq!(phred_to_ascii(&[0]), b"!".to_vec());
        assert_eq!(phred_to_ascii(&[30]), b"?".to_vec());
        assert_eq!(phred_to_ascii(&[40]), b"I".to_vec());
    }

    #[test]
    fn phred_to_ascii_sequence() {
        // Direct +33 offset, no flooring: older srf-load-produced
        // archives (e.g. DRR000918) legitimately store Q0 at some
        // positions and fasterq-dump preserves them as `!`. An earlier
        // Q2 floor helped DRR002048-style 2011 fastq-load files but
        // regressed these older srf-load files on every Q0/Q1 byte.
        let input: Vec<u8> = (0..=10).collect();
        let expected: Vec<u8> = (33..=43).collect();
        assert_eq!(phred_to_ascii(&input), expected);
    }

    #[test]
    fn phred_to_ascii_edge_zero() {
        // Q0 must map to '!' verbatim for srf-load parity (DRR000918).
        assert_eq!(phred_to_ascii(&[0]), vec![33]); // '!'
    }

    #[test]
    fn phred_to_ascii_edge_q63() {
        // Q63 + 33 = 96 = '`'
        assert_eq!(phred_to_ascii(&[63]), vec![96]);
    }

    #[test]
    fn phred_to_ascii_saturates() {
        // u8::MAX (255) + 33 would overflow; saturating_add caps at 255.
        assert_eq!(phred_to_ascii(&[255]), vec![255]);
        // 223 + 33 = 256 -> saturates to 255.
        assert_eq!(phred_to_ascii(&[223]), vec![255]);
        // 222 + 33 = 255 -> exact boundary, no saturation.
        assert_eq!(phred_to_ascii(&[222]), vec![255]);
    }

    // -----------------------------------------------------------------------
    // SRA-lite quality tests
    // -----------------------------------------------------------------------

    #[test]
    fn sra_lite_pass_quality() {
        let q = sra_lite_quality(5, true);
        assert_eq!(q.len(), 5);
        // Q30 + 33 = 63 = '?'
        assert!(q.iter().all(|&b| b == b'?'));
    }

    #[test]
    fn sra_lite_reject_quality() {
        let q = sra_lite_quality(3, false);
        assert_eq!(q.len(), 3);
        // Q3 + 33 = 36 = '$'
        assert!(q.iter().all(|&b| b == b'$'));
    }

    #[test]
    fn sra_lite_empty() {
        assert_eq!(sra_lite_quality(0, true), Vec::<u8>::new());
        assert_eq!(sra_lite_quality(0, false), Vec::<u8>::new());
    }

    #[test]
    fn sra_lite_large() {
        let len = 10_000;
        let q = sra_lite_quality(len, true);
        assert_eq!(q.len(), len);
        assert!(q.iter().all(|&b| b == b'?'));
    }

    // -----------------------------------------------------------------------
    // Constant sanity checks
    // -----------------------------------------------------------------------

    #[test]
    fn constants_are_correct() {
        assert_eq!(QUAL_PHRED_OFFSET, 33);
        assert_eq!(SRA_LITE_PASS_QUAL, 30);
        assert_eq!(SRA_LITE_REJECT_QUAL, 3);
    }

    #[test]
    fn sra_lite_ascii_values() {
        // Double-check the expected ASCII characters.
        assert_eq!(SRA_LITE_PASS_QUAL + QUAL_PHRED_OFFSET, b'?');
        assert_eq!(SRA_LITE_REJECT_QUAL + QUAL_PHRED_OFFSET, b'$');
    }

    // -----------------------------------------------------------------------
    // merge_altread tests
    // -----------------------------------------------------------------------

    #[test]
    fn merge_altread_no_ambiguity() {
        // ALTREAD all zeros → bases unchanged.
        let mut bases = b"ACGT".to_vec();
        let altread = [0x00, 0x00]; // 4 nibbles, all 0
        merge_altread(&mut bases, &altread, 4);
        assert_eq!(bases, b"ACGT");
    }

    #[test]
    fn merge_altread_all_n() {
        // ALTREAD all 0xF (15) → every base becomes N.
        let mut bases = b"ACGT".to_vec();
        let altread = [0xFF, 0xFF]; // nibbles: 15, 15, 15, 15
        merge_altread(&mut bases, &altread, 4);
        assert_eq!(bases, b"NNNN");
    }

    #[test]
    fn merge_altread_selective() {
        // Mark positions 0 and 2 as N, leave 1 and 3 unchanged.
        // Packed: pos0=0xF, pos1=0x0, pos2=0xF, pos3=0x0
        let mut bases = b"ACGT".to_vec();
        let altread = [0xF0, 0xF0]; // high nibbles = 15, low nibbles = 0
        merge_altread(&mut bases, &altread, 4);
        assert_eq!(bases, b"NCNT");
    }

    #[test]
    fn merge_altread_partial_ambiguity() {
        // A (4na=1) | G (4na=4) = 5 → R (purine ambiguity).
        let mut bases = b"A".to_vec();
        let altread = [0x40]; // high nibble = 4 (G)
        merge_altread(&mut bases, &altread, 1);
        assert_eq!(bases, b"R");
    }

    #[test]
    fn merge_altread_empty() {
        let mut bases = b"ACGT".to_vec();
        merge_altread(&mut bases, &[], 4);
        assert_eq!(bases, b"ACGT");
    }

    #[test]
    fn merge_altread_odd_bases() {
        // 3 bases: last byte only uses high nibble.
        let mut bases = b"ACG".to_vec();
        let altread = [0x00, 0xF0]; // pos0=0, pos1=0, pos2=F (high nibble of byte1)
        merge_altread(&mut bases, &altread, 3);
        assert_eq!(bases, b"ACN");
    }

    // -----------------------------------------------------------------------
    // merge_altread_bin tests
    // -----------------------------------------------------------------------

    #[test]
    fn merge_altread_bin_no_ambiguity() {
        let mut bases = b"ACGT".to_vec();
        let altread = [0x00, 0x00, 0x00, 0x00];
        merge_altread_bin(&mut bases, &altread, 4);
        assert_eq!(bases, b"ACGT");
    }

    #[test]
    fn merge_altread_bin_selective_n() {
        // 4na_bin: one byte per base, low nibble = 4na code. 0x0F = N.
        let mut bases = b"ACGT".to_vec();
        let altread = [0x00, 0x0F, 0x00, 0x0F];
        merge_altread_bin(&mut bases, &altread, 4);
        assert_eq!(bases, b"ANGN");
    }

    #[test]
    fn merge_altread_bin_iupac_merge() {
        // A (4na=1) OR G-code (4na=4) = 5 → R (purine).
        let mut bases = b"A".to_vec();
        let altread = [0x04];
        merge_altread_bin(&mut bases, &altread, 1);
        assert_eq!(bases, b"R");
    }

    #[test]
    fn merge_altread_bin_short_altread_stops_cleanly() {
        // When altread is shorter than bases we stop at its end, leaving
        // trailing bases untouched.
        let mut bases = b"ACGT".to_vec();
        let altread = [0x0F, 0x0F];
        merge_altread_bin(&mut bases, &altread, 4);
        assert_eq!(bases, b"NNGT");
    }

    #[test]
    fn merge_altread_bin_ignores_high_nibble() {
        // Writer should only populate low nibble; high nibble must be ignored.
        let mut bases = b"A".to_vec();
        let altread = [0xF0]; // high nibble set, low nibble zero → no change
        merge_altread_bin(&mut bases, &altread, 1);
        assert_eq!(bases, b"A");
    }

    // -----------------------------------------------------------------------
    // Property tests
    // -----------------------------------------------------------------------

    use proptest::prelude::*;

    /// Pack 4 ASCII A/C/G/T bases into one 2na byte (MSB-first).
    fn pack_2na_nibble(base: u8) -> u8 {
        match base {
            b'A' => 0b00,
            b'C' => 0b01,
            b'G' => 0b10,
            b'T' => 0b11,
            _ => unreachable!(),
        }
    }

    fn pack_2na(bases: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(bases.len().div_ceil(4));
        for chunk in bases.chunks(4) {
            let mut byte = 0u8;
            for (i, &b) in chunk.iter().enumerate() {
                byte |= pack_2na_nibble(b) << (6 - 2 * i);
            }
            out.push(byte);
        }
        out
    }

    proptest! {
        /// Packing then unpacking any ACGT sequence returns the original.
        #[test]
        fn prop_unpack_2na_round_trip(ref seq in "[ACGT]{0,256}") {
            let bases = seq.as_bytes();
            let packed = pack_2na(bases);
            let unpacked = unpack_2na(&packed, bases.len());
            prop_assert_eq!(unpacked.as_slice(), bases);
        }

        /// Packing then unpacking any IUPAC 4-bit nibble sequence round-trips.
        #[test]
        fn prop_unpack_4na_round_trip(nibbles in proptest::collection::vec(0u8..16, 0..256)) {
            let num_bases = nibbles.len();
            let mut packed = vec![0u8; num_bases.div_ceil(2)];
            for (i, &n) in nibbles.iter().enumerate() {
                if i % 2 == 0 {
                    packed[i / 2] |= n << 4;
                } else {
                    packed[i / 2] |= n;
                }
            }
            let unpacked = unpack_4na(&packed, num_bases);
            prop_assert_eq!(unpacked.len(), num_bases);
            for (i, &n) in nibbles.iter().enumerate() {
                prop_assert_eq!(unpacked[i], DNA_4NA[n as usize]);
            }
        }

        /// PHRED+33 invariant: every byte must be mapped to min(b+33, 255).
        /// The "all-zero = empty" sentinel only applies to the wrapper
        /// `encode_raw_quality_for_fastq`, not to `phred_to_ascii` itself.
        #[test]
        fn prop_phred_to_ascii_adds_33_saturating(ref q in proptest::collection::vec(any::<u8>(), 0..512)) {
            let out = phred_to_ascii(q);
            prop_assert_eq!(out.len(), q.len());
            for (i, &b) in q.iter().enumerate() {
                let expected = b.saturating_add(QUAL_PHRED_OFFSET);
                prop_assert_eq!(out[i], expected);
            }
        }

        /// All-zero ALTREAD leaves 2na-decoded bases untouched, regardless
        /// of base content or length. Regression guard on the "early exit
        /// on zero nibble" fast path.
        #[test]
        fn prop_merge_altread_all_zero_is_identity(ref seq in "[ACGT]{0,128}") {
            let mut bases = seq.as_bytes().to_vec();
            let before = bases.clone();
            let altread = vec![0u8; bases.len().div_ceil(2)];
            merge_altread(&mut bases, &altread, before.len());
            prop_assert_eq!(bases, before);
        }

        /// SRA-lite quality is a uniform run of the configured score, for
        /// any length within reason.
        #[test]
        fn prop_sra_lite_quality_uniform(length in 0usize..2048, pass in any::<bool>()) {
            let q = sra_lite_quality(length, pass);
            prop_assert_eq!(q.len(), length);
            let expected = if pass { b'?' } else { b'$' };
            prop_assert!(q.iter().all(|&b| b == expected));
        }
    }
}
