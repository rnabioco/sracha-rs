#!/usr/bin/env python3
"""Stream-compare two FASTQ files, checking sequence and quality identity.

Reads both files line-by-line in 4-line FASTQ records. Reports:
- Total records in each file
- First N mismatches with context
- Summary: total mismatches, quality-length violations

Ignores defline differences (line 1 of each record).
"""
import sys


def compare_fastq(file_a, file_b, max_report=100, allow_n_masking=False):
    mismatches_seq = 0
    mismatches_qual = 0
    qual_len_errors = 0
    n_mask_seqs = 0
    n_mask_positions = 0
    record_num = 0
    reported = 0

    with open(file_a) as fa, open(file_b) as fb:
        while True:
            lines_a = [fa.readline() for _ in range(4)]
            lines_b = [fb.readline() for _ in range(4)]

            # Both done
            if not lines_a[0] and not lines_b[0]:
                break

            # One ended early
            if not lines_a[0] or not lines_b[0]:
                eof_file = "A" if not lines_a[0] else "B"
                other = "B" if eof_file == "A" else "A"
                remaining = fb if eof_file == "A" else fa
                extra = sum(1 for line in remaining if line.startswith("@"))
                print(
                    f"ERROR: File {eof_file} ended at record {record_num + 1} "
                    f"but file {other} has {extra} more records"
                )
                return False

            record_num += 1
            defline_a, seq_a, plus_a, qual_a = [l.rstrip("\n") for l in lines_a]
            defline_b, seq_b, plus_b, qual_b = [l.rstrip("\n") for l in lines_b]

            if not defline_a.startswith("@") or not defline_b.startswith("@"):
                print(f"ERROR: record {record_num}: malformed defline")
                print(f"  A: {defline_a[:80]}")
                print(f"  B: {defline_b[:80]}")
                return False

            # Check sequence identity
            if seq_a != seq_b:
                if allow_n_masking and len(seq_a) == len(seq_b):
                    # Check if all differing positions have N in file B
                    is_n_mask = all(
                        cb == 'N' for ca, cb in zip(seq_a, seq_b) if ca != cb
                    )
                    if is_n_mask:
                        n_mask_seqs += 1
                        n_mask_positions += sum(
                            1 for ca, cb in zip(seq_a, seq_b) if ca != cb
                        )
                    else:
                        mismatches_seq += 1
                        if reported < max_report:
                            print(f"SEQ MISMATCH at record {record_num}:")
                            print(f"  A: {defline_a[:80]}")
                            print(f"  B: {defline_b[:80]}")
                            print(f"  A seq[:{min(80, len(seq_a))}]: {seq_a[:80]}")
                            print(f"  B seq[:{min(80, len(seq_b))}]: {seq_b[:80]}")
                            reported += 1
                else:
                    mismatches_seq += 1
                    if reported < max_report:
                        print(f"SEQ MISMATCH at record {record_num}:")
                        print(f"  A: {defline_a[:80]}")
                        print(f"  B: {defline_b[:80]}")
                        print(f"  A seq[:{min(80, len(seq_a))}]: {seq_a[:80]}")
                        print(f"  B seq[:{min(80, len(seq_b))}]: {seq_b[:80]}")
                        reported += 1

            # Check quality identity
            if qual_a != qual_b:
                mismatches_qual += 1
                if reported < max_report:
                    print(f"QUAL MISMATCH at record {record_num}:")
                    print(f"  A: {defline_a[:80]}")
                    print(f"  B: {defline_b[:80]}")
                    print(f"  A qual len={len(qual_a)}, seq len={len(seq_a)}")
                    print(f"  B qual len={len(qual_b)}, seq len={len(seq_b)}")
                    for i, (a, b) in enumerate(zip(qual_a, qual_b)):
                        if a != b:
                            print(
                                f"  First diff at pos {i}: "
                                f"A='{a}'(ord={ord(a)}) B='{b}'(ord={ord(b)})"
                            )
                            break
                    reported += 1

            # Quality length == sequence length (the STAR bug)
            if len(qual_a) != len(seq_a):
                qual_len_errors += 1
                if reported < max_report:
                    print(
                        f"QUAL LEN ERROR (file A) at record {record_num}: "
                        f"seq={len(seq_a)} qual={len(qual_a)}"
                    )
                    reported += 1

            if len(qual_b) != len(seq_b):
                qual_len_errors += 1
                if reported < max_report:
                    print(
                        f"QUAL LEN ERROR (file B) at record {record_num}: "
                        f"seq={len(seq_b)} qual={len(qual_b)}"
                    )
                    reported += 1

            if record_num % 10_000_000 == 0:
                print(
                    f"  ... {record_num / 1e6:.0f}M records "
                    f"(seq_mm={mismatches_seq}, qual_mm={mismatches_qual})",
                    file=sys.stderr,
                )

    print(f"\n{'=' * 60}")
    print(f"COMPARISON COMPLETE")
    print(f"  Total records compared: {record_num:,}")
    print(f"  Sequence mismatches:    {mismatches_seq:,}")
    print(f"  Quality mismatches:     {mismatches_qual:,}")
    print(f"  Qual-length errors:     {qual_len_errors:,}")
    if allow_n_masking:
        print(f"  N-masked sequences:     {n_mask_seqs:,} ({n_mask_positions:,} positions)")
    if mismatches_seq == 0 and mismatches_qual == 0 and qual_len_errors == 0:
        print(f"  RESULT: PASS")
        return True
    else:
        print(f"  RESULT: FAIL")
        return False


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Compare two FASTQ files record-by-record"
    )
    parser.add_argument("file_a", help="First FASTQ file")
    parser.add_argument("file_b", help="Second FASTQ file")
    parser.add_argument(
        "--allow-n-masking", action="store_true",
        help="Treat N-substitutions in file B as acceptable (not mismatches)"
    )
    args = parser.parse_args()
    ok = compare_fastq(args.file_a, args.file_b, allow_n_masking=args.allow_n_masking)
    sys.exit(0 if ok else 1)
