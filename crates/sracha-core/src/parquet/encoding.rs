//! DNA pre-packing for Parquet output.
//!
//! Parquet's general-purpose codecs (zstd, snappy) cannot match VDB's domain
//! encoding on their own — VDB packs DNA at 2 bits/base before applying any
//! generic compression. To give Parquet a fair shot, we pre-pack DNA into
//! 2na or 4na bytes ourselves and let Parquet add light entropy coding on
//! top.

/// Pack ASCII DNA bases (`A`, `C`, `G`, `T` only) into 2na.
///
/// Each output byte holds 4 bases, MSB-first:
///
/// ```text
/// byte: [b0 b0 b1 b1 | b2 b2 b3 b3]   bit positions 7..0
/// ```
///
/// `0b00 = A`, `0b01 = C`, `0b10 = G`, `0b11 = T`.
///
/// Any non-ACGT base (N, IUPAC ambiguity) is encoded as `A` (0b00) — the
/// caller should fall back to 4na or ASCII when the input contains
/// ambiguities. The output length is `ceil(bases.len() / 4)`.
pub fn pack_2na(bases: &[u8]) -> Vec<u8> {
    let out_len = bases.len().div_ceil(4);
    let mut out = vec![0u8; out_len];
    for (i, &b) in bases.iter().enumerate() {
        let code = base_to_2na(b);
        let byte_idx = i / 4;
        let shift = (3 - (i % 4)) * 2;
        out[byte_idx] |= code << shift;
    }
    out
}

/// Pack ASCII DNA bases (IUPAC) into 4na.
///
/// Each output byte holds 2 bases as 4-bit nibbles, high nibble first.
/// Output length is `ceil(bases.len() / 2)`.
pub fn pack_4na(bases: &[u8]) -> Vec<u8> {
    let out_len = bases.len().div_ceil(2);
    let mut out = vec![0u8; out_len];
    for (i, &b) in bases.iter().enumerate() {
        let code = base_to_4na(b);
        let byte_idx = i / 2;
        if i % 2 == 0 {
            out[byte_idx] = code << 4;
        } else {
            out[byte_idx] |= code & 0x0F;
        }
    }
    out
}

/// Detect whether a slice contains only `ACGT` (suitable for 2na packing).
pub fn is_pure_acgt(bases: &[u8]) -> bool {
    bases
        .iter()
        .all(|&b| matches!(b, b'A' | b'C' | b'G' | b'T'))
}

#[inline]
fn base_to_2na(b: u8) -> u8 {
    match b {
        b'A' | b'a' => 0b00,
        b'C' | b'c' => 0b01,
        b'G' | b'g' => 0b10,
        b'T' | b't' => 0b11,
        _ => 0b00,
    }
}

#[inline]
fn base_to_4na(b: u8) -> u8 {
    // Mirror of crate::vdb::encoding::DNA_4NA reverse mapping.
    match b {
        b'A' | b'a' => 1,
        b'C' | b'c' => 2,
        b'M' | b'm' => 3,
        b'G' | b'g' => 4,
        b'R' | b'r' => 5,
        b'S' | b's' => 6,
        b'V' | b'v' => 7,
        b'T' | b't' => 8,
        b'W' | b'w' => 9,
        b'Y' | b'y' => 10,
        b'H' | b'h' => 11,
        b'K' | b'k' => 12,
        b'D' | b'd' => 13,
        b'B' | b'b' => 14,
        _ => 15,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vdb::encoding::{unpack_2na, unpack_4na};

    #[test]
    fn pack_2na_roundtrip() {
        let bases = b"ACGTACGT";
        let packed = pack_2na(bases);
        assert_eq!(packed.len(), 2);
        assert_eq!(unpack_2na(&packed, bases.len()), bases.to_vec());
    }

    #[test]
    fn pack_2na_partial_byte() {
        let bases = b"ACG";
        let packed = pack_2na(bases);
        assert_eq!(packed.len(), 1);
        assert_eq!(unpack_2na(&packed, bases.len()), bases.to_vec());
    }

    #[test]
    fn pack_4na_roundtrip_with_ambiguity() {
        let bases = b"ACGTNRY";
        let packed = pack_4na(bases);
        assert_eq!(packed.len(), 4);
        assert_eq!(unpack_4na(&packed, bases.len()), bases.to_vec());
    }

    #[test]
    fn pack_2na_lossy_on_n() {
        // N collapses to A in 2na; this is intentional — caller should check
        // is_pure_acgt() before choosing 2na.
        let packed = pack_2na(b"AN");
        let unpacked = unpack_2na(&packed, 2);
        assert_eq!(unpacked, b"AA".to_vec());
    }

    #[test]
    fn detects_pure_acgt() {
        assert!(is_pure_acgt(b"ACGTACGT"));
        assert!(!is_pure_acgt(b"ACGN"));
        assert!(!is_pure_acgt(b"ACGTR"));
    }
}
