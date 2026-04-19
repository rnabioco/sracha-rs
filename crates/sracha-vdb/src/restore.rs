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

/// READ_TYPE bit values, verified against
/// `ncbi-vdb/interfaces/insdc/insdc.h:330-342`:
///
/// - `BIOLOGICAL` (bit 0 = `0x01`): biological vs technical read; technical
///   reads are `0`.
/// - `FORWARD` (bit 1 = `0x02`): read is in forward orientation (w.r.t. the
///   sequencing template).
/// - `REVERSE` (bit 2 = `0x04`): read is reverse-complemented.
///
/// `seq_restore_read` consults only `FORWARD` and `REVERSE` (C source at
/// `seq-restore-read.c:531-546`); `BIOLOGICAL` is a FASTQ-formatting hint.
pub const SRA_READ_TYPE_BIOLOGICAL: u8 = 0x01;
pub const SRA_READ_TYPE_FORWARD: u8 = 0x02;
pub const SRA_READ_TYPE_REVERSE: u8 = 0x04;

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
        return Err(Error::Format(format!(
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
                Error::Format(format!(
                    "align_restore_read: ref_offset cursor {roi} past array of {}",
                    ref_offset.len()
                ))
            })?;
            rri += off as i64;
            roi += 1;
        }

        if has_mismatch[di] != 0 {
            let m = *mismatch.get(mmi).ok_or_else(|| {
                Error::Format(format!(
                    "align_restore_read: mismatch cursor {mmi} past array of {}",
                    mismatch.len()
                ))
            })?;
            out[di] = m;
            mmi += 1;
        } else {
            if rri < 0 || rri as usize >= ref_read.len() {
                return Err(Error::Format(format!(
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

/// Splice a full SEQUENCE spot's bases together from per-read alignment
/// lookups plus the spot's CMP_READ (unaligned halves).
///
/// Transcribed from `ncbi-vdb/libs/axf/seq-restore-read.c:454-574`.
///
/// Inputs are per-spot arrays of length `num_reads` (= SEQUENCE.NREADS):
/// - `cmp_rd`: 4na-bin bases for the unaligned halves of this spot,
///   stored contiguously in read order. Consumed sequentially as the
///   splice walks reads where `align_ids[i] == 0`.
/// - `align_ids`: PRIMARY_ALIGNMENT row ids, one per read. `0` means
///   the read is unaligned and bases live in `cmp_rd`.
/// - `read_lens`: final length in bases of each read in the output.
/// - `read_types`: per-read bitfield. Only `SRA_READ_TYPE_FORWARD`
///   (`0x01`) and `SRA_READ_TYPE_REVERSE` (`0x02`) matter for strand;
///   higher bits are FASTQ formatting flags and are ignored here.
/// - `fetch_aligned`: closure that, given an alignment row id, returns
///   the 4na-bin bases in *reference orientation* (what
///   `align_restore_read` produces). Length must equal the
///   corresponding `read_lens[i]`; mismatch is an error. The closure is
///   a callback so the caller controls caching of PRIMARY_ALIGNMENT /
///   REFERENCE blob reads.
///
/// Strand handling follows C source lines 531-546: a read with
/// `READ_TYPE & FORWARD` copies aligned bases as-is; a read with
/// `READ_TYPE & REVERSE` reverses and complements them before copying.
/// Either neither or both unset is an error (bam-load always sets
/// exactly one).
///
/// Returns a fresh `Vec<u8>` of total length `sum(read_lens)` in 4na-bin.
pub fn seq_restore_read(
    cmp_rd: &[u8],
    align_ids: &[i64],
    read_lens: &[u32],
    read_types: &[u8],
    mut fetch_aligned: impl FnMut(i64) -> Result<Vec<u8>>,
) -> Result<Vec<u8>> {
    let num_reads = align_ids.len();
    if read_lens.len() != num_reads || read_types.len() != num_reads {
        return Err(Error::Format(format!(
            "seq_restore_read: inconsistent per-read arrays — \
             align_ids.len={num_reads}, read_lens.len={}, read_types.len={}",
            read_lens.len(),
            read_types.len(),
        )));
    }

    let total: usize = read_lens.iter().map(|&n| n as usize).sum();
    let mut out = Vec::with_capacity(total);
    let mut cmp_cursor = 0usize;

    for i in 0..num_reads {
        let len = read_lens[i] as usize;
        if align_ids[i] > 0 {
            let aligned = fetch_aligned(align_ids[i])?;
            if aligned.len() != len {
                return Err(Error::Format(format!(
                    "seq_restore_read: alignment {} returned {} bases, expected {}",
                    align_ids[i],
                    aligned.len(),
                    len
                )));
            }
            let rt = read_types[i];
            if rt & SRA_READ_TYPE_FORWARD != 0 {
                out.extend_from_slice(&aligned);
            } else if rt & SRA_READ_TYPE_REVERSE != 0 {
                // Reverse order AND complement each nibble.
                for j in (0..len).rev() {
                    out.push(COMPLEMENT_4NA[(aligned[j] & 0x0F) as usize]);
                }
            } else {
                return Err(Error::Format(format!(
                    "seq_restore_read: read {i} has READ_TYPE={rt:#x} without FORWARD or REVERSE bit"
                )));
            }
        } else {
            // Unaligned: consume len bases from cmp_rd.
            let end = cmp_cursor + len;
            if end > cmp_rd.len() {
                return Err(Error::Format(format!(
                    "seq_restore_read: read {i} wants {len} unaligned bases at offset {cmp_cursor}, cmp_rd has {}",
                    cmp_rd.len()
                )));
            }
            out.extend_from_slice(&cmp_rd[cmp_cursor..end]);
            cmp_cursor = end;
        }
    }

    Ok(out)
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
    fn seq_splice_all_unaligned() {
        // One-read spot of length 4, no alignment — bases all from cmp_rd.
        let out = seq_restore_read(
            &[0x1, 0x2, 0x4, 0x8], // ACGT
            &[0],
            &[4],
            &[SRA_READ_TYPE_FORWARD | 0x08], // BIOLOGICAL|FORWARD
            |_| panic!("fetch_aligned should not be called for unaligned read"),
        )
        .unwrap();
        assert_eq!(out, [0x1, 0x2, 0x4, 0x8]);
    }

    #[test]
    fn seq_splice_aligned_forward_pair_with_unaligned_tail() {
        // Two-read spot: read 0 aligned (forward), read 1 unaligned.
        let out = seq_restore_read(
            &[0x8, 0x1], // TA — unaligned portion for read 1
            &[42, 0],
            &[3, 2],
            &[SRA_READ_TYPE_FORWARD, SRA_READ_TYPE_FORWARD],
            |id| {
                assert_eq!(id, 42);
                Ok(vec![0x1, 0x2, 0x4]) // ACG
            },
        )
        .unwrap();
        assert_eq!(out, [0x1, 0x2, 0x4, 0x8, 0x1]); // ACG + TA
    }

    #[test]
    fn seq_splice_reverse_read_reverse_complements() {
        // Single aligned read with REVERSE bit set → RC the aligned bases.
        let out = seq_restore_read(&[], &[7], &[4], &[SRA_READ_TYPE_REVERSE], |id| {
            assert_eq!(id, 7);
            Ok(vec![0x1, 0x2, 0x4, 0x8]) // ACGT
        })
        .unwrap();
        // RC(ACGT) = ACGT (palindrome), but let's verify complement order:
        // reverse(ACGT) = TGCA, complement = ACGT. So RC(ACGT) = ACGT.
        assert_eq!(out, [0x1, 0x2, 0x4, 0x8]);
    }

    #[test]
    fn seq_splice_reverse_non_palindrome() {
        // Single aligned read, bases "AAAT" → reverse = TAAA, complement = ATTT.
        let out = seq_restore_read(
            &[],
            &[1],
            &[4],
            &[SRA_READ_TYPE_REVERSE],
            |_| Ok(vec![0x1, 0x1, 0x1, 0x8]), // AAAT
        )
        .unwrap();
        assert_eq!(out, [0x1, 0x8, 0x8, 0x8]); // ATTT
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
