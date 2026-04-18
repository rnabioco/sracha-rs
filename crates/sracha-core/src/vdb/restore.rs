//! Reconstruction primitives for reference-compressed cSRA.
//!
//! Two pure functions, transcribed from
//! `ncbi-vdb/libs/axf/{align,seq}-restore-read.c`:
//!
//! - [`align_restore_read`] — builds one aligned read in *reference
//!   orientation* from the REFERENCE span plus HAS_MISMATCH / MISMATCH /
//!   HAS_REF_OFFSET / REF_OFFSET.
//! - [`seq_restore_read`] — splices a spot's full READ together from
//!   CMP_READ (unaligned halves) and per-read alignment results, applying
//!   strand based on SEQUENCE.READ_TYPE.
//!
//! Output byte format is INSDC:4na:bin: one nibble per base in the low
//! four bits, with values 0x1=A, 0x2=C, 0x4=G, 0x8=T and combinations for
//! ambiguity codes (0xF = N). The 4na → ASCII mapping lives next to the
//! FASTQ formatter once Phase 3 wires this into the pipeline.

use crate::error::{Error, Result};

/// 4na-bin reverse-complement lookup (from `seq-restore-read.c:362-379`):
/// A↔T (1↔8), C↔G (2↔4), N↔N (15↔15), ambiguity codes map to their
/// complement bits.
pub const COMPLEMENT_4NA: [u8; 16] = [0, 8, 4, 12, 2, 10, 6, 14, 1, 9, 5, 13, 3, 11, 7, 15];

/// READ_TYPE bits consumed by `seq_restore_read` (see
/// `ncbi-vdb/libs/sra.h` and the C source at
/// `seq-restore-read.c:531-546`). Only the low two bits affect
/// reconstruction; higher bits are FASTQ-formatting hints.
pub const SRA_READ_TYPE_FORWARD: u8 = 0x01;
pub const SRA_READ_TYPE_REVERSE: u8 = 0x02;

/// Build the aligned read's bases in reference orientation.
///
/// Transcribed from `libs/axf/align-restore-read.c:45-135`. Inputs are
/// already decoded into byte-per-element buffers by `AlignmentCursor`:
///
/// - `ref_read`: 4na-bin reference bases covering the alignment's span
///   (fetched via `ReferenceCursor::fetch_span`).
/// - `has_mismatch`: `0` or `1` per base of the output read; length =
///   `read_len`.
/// - `mismatch`: 4na-bin bases for the `has_mismatch == 1` positions
///   only, in order.
/// - `has_ref_offset`: `0` or `1` per base of the output read; length =
///   `read_len`. A `1` signals that the reference cursor jumps by
///   `ref_offset[roi]` before consuming the next base (positive jumps
///   skip reference bases for an insertion; negative jumps rewind for a
///   deletion).
/// - `ref_offset`: signed jump amounts, in order, one per
///   `has_ref_offset == 1` position.
/// - `read_len`: the alignment's read length (= length of HAS_MISMATCH).
///
/// Returns a fresh `Vec<u8>` of length `read_len` in 4na-bin form.
pub fn align_restore_read(
    ref_read: &[u8],
    has_mismatch: &[u8],
    mismatch: &[u8],
    has_ref_offset: &[u8],
    ref_offset: &[i32],
    read_len: usize,
) -> Result<Vec<u8>> {
    if has_mismatch.len() != read_len || has_ref_offset.len() != read_len {
        return Err(Error::Vdb(format!(
            "align_restore_read: length mismatch — read_len={read_len}, \
             has_mismatch.len={}, has_ref_offset.len={}",
            has_mismatch.len(),
            has_ref_offset.len(),
        )));
    }
    let mut out = vec![0u8; read_len];
    let mut mmi = 0usize; // mismatch read cursor
    let mut roi = 0usize; // ref_offset read cursor
    let mut rri: i64 = 0; // reference read cursor (signed — ref_offset may rewind)

    for di in 0..read_len {
        if has_ref_offset[di] != 0 {
            let off = *ref_offset.get(roi).ok_or_else(|| {
                Error::Vdb(format!(
                    "align_restore_read: ref_offset cursor {roi} past array of {}",
                    ref_offset.len()
                ))
            })?;
            rri += off as i64;
            roi += 1;
        }

        if has_mismatch[di] != 0 {
            let m = *mismatch.get(mmi).ok_or_else(|| {
                Error::Vdb(format!(
                    "align_restore_read: mismatch cursor {mmi} past array of {}",
                    mismatch.len()
                ))
            })?;
            out[di] = m;
            mmi += 1;
        } else {
            if rri < 0 || rri as usize >= ref_read.len() {
                return Err(Error::Vdb(format!(
                    "align_restore_read: ref cursor {rri} outside ref_read ({})",
                    ref_read.len()
                )));
            }
            out[di] = ref_read[rri as usize];
        }

        rri += 1;
    }
    Ok(out)
}

/// Reverse-complement a 4na-bin read in place.
pub fn reverse_complement_4na(bases: &mut [u8]) {
    bases.reverse();
    for b in bases.iter_mut() {
        *b = COMPLEMENT_4NA[(*b & 0x0F) as usize];
    }
}

/// Convert a 4na-bin slice to ASCII IUPAC characters (A/C/G/T plus
/// ambiguity codes; `N` for anything outside the standard 15-value
/// range, including the 0-nibble "gap").
pub fn fourna_to_ascii(bases: &[u8]) -> Vec<u8> {
    // Indexed by low 4 bits. 0x0 = gap (we emit 'N' rather than '-' to
    // keep FASTQ callers happy). This is the same mapping vdb-dump uses.
    const LUT: [u8; 16] = [
        b'N', b'A', b'C', b'M', b'G', b'R', b'S', b'V', b'T', b'W', b'Y', b'H', b'K', b'D', b'B',
        b'N',
    ];
    bases.iter().map(|&b| LUT[(b & 0x0F) as usize]).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complement_involution() {
        for i in 0u8..16 {
            let c = COMPLEMENT_4NA[i as usize];
            assert_eq!(COMPLEMENT_4NA[c as usize], i, "complement at {i}");
        }
    }

    #[test]
    fn all_match_simple_case() {
        // read_len=4, no mismatches, no indels → output == ref_read.
        let ref_read = [0x1, 0x2, 0x4, 0x8]; // A C G T
        let out = align_restore_read(&ref_read, &[0; 4], &[], &[0; 4], &[], 4).unwrap();
        assert_eq!(out, ref_read);
    }

    #[test]
    fn mismatch_overlay() {
        // read_len=4, positions 1 and 3 mismatch.
        let ref_read = [0x1, 0x2, 0x4, 0x8]; // A C G T
        // has_mismatch = 0 1 0 1 → mismatches at i=1 and i=3
        let out = align_restore_read(
            &ref_read,
            &[0, 1, 0, 1],
            &[0x4, 0x1], // G, A
            &[0; 4],
            &[],
            4,
        )
        .unwrap();
        assert_eq!(out, [0x1, 0x4, 0x4, 0x1]); // A G G A
    }

    #[test]
    fn insertion_uses_mismatch_no_ref_advance() {
        // One-base insertion: positions 0..=2 match, position 1 is an
        // insertion in the read (ref cursor rewinds so the inserted base
        // doesn't consume a ref base).
        //   ref_read  = A C G
        //   read      = A X C G  where X is a mismatch-sourced inserted base
        //   has_mismatch   = 0 1 0 0
        //   has_ref_offset = 0 1 0 0   (rewind by -1 before consuming X)
        //   ref_offset     = [-1]
        //   mismatch       = [T]       // 0x8
        let ref_read = [0x1, 0x2, 0x4]; // A C G
        let out =
            align_restore_read(&ref_read, &[0, 1, 0, 0], &[0x8], &[0, 1, 0, 0], &[-1], 4).unwrap();
        assert_eq!(out, [0x1, 0x8, 0x2, 0x4]); // A T C G
    }
}
