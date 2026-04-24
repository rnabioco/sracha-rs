#!/usr/bin/env python3
"""Structured diagnostic report for paired FASTQ outputs that disagree.

Given two FASTQs of the same records (typically sracha vs. fasterq-dump),
classifies mismatched records and emits:

  summary.tsv   — totals, record-level class counts, position-bucket histogram
  confusion.tsv — 5x5 base confusion matrix (A/C/G/T/N)
  positions.tsv — per-position mismatch counts across the read length
  samples.txt   — first N mismatched records with a caret indicator

Record classes:

  IDENTICAL      both sequences match exactly
  N_MASK_ONLY    equal length; every differing position has exactly one side = N
  BASE_FLIP      equal length; no N at any differing position
  MIXED          equal length; some N-mask positions and some base-flip positions
  LENGTH_DIFF    sequences differ in length
"""
import argparse
import os
import sys
from collections import Counter


BASES = ("A", "C", "G", "T", "N")
BASE_INDEX = {b: i for i, b in enumerate(BASES)}
OTHER = len(BASES)  # catch-all bucket for anything outside ACGTN


def classify_record(seq_a, seq_b):
    """Return (class, diff_positions) for one record pair.

    diff_positions is a list of (i, char_a, char_b) for each mismatched index,
    or None if the record class is IDENTICAL or LENGTH_DIFF (no per-position
    comparison is valid for length-mismatched reads in this pass).
    """
    if seq_a == seq_b:
        return "IDENTICAL", None
    if len(seq_a) != len(seq_b):
        return "LENGTH_DIFF", None

    diffs = [(i, seq_a[i], seq_b[i]) for i in range(len(seq_a)) if seq_a[i] != seq_b[i]]
    saw_n_mask = False
    saw_base_flip = False
    for _, ca, cb in diffs:
        if ca == "N" or cb == "N":
            saw_n_mask = True
        else:
            saw_base_flip = True
        if saw_n_mask and saw_base_flip:
            break
    if saw_n_mask and not saw_base_flip:
        return "N_MASK_ONLY", diffs
    if saw_base_flip and not saw_n_mask:
        return "BASE_FLIP", diffs
    return "MIXED", diffs


def bucket_index(pos, length, n_buckets):
    if length <= 0:
        return 0
    idx = int(pos * n_buckets / length)
    return min(idx, n_buckets - 1)


def iter_fastq(path):
    with open(path) as fh:
        while True:
            defline = fh.readline()
            if not defline:
                return
            seq = fh.readline()
            plus = fh.readline()
            qual = fh.readline()
            if not qual:
                return
            yield (
                defline.rstrip("\n"),
                seq.rstrip("\n"),
                plus.rstrip("\n"),
                qual.rstrip("\n"),
            )


def format_sample(record_num, class_, defline_a, seq_a, defline_b, seq_b,
                  label_a, label_b, diffs, preview_len=120):
    # Pad the leading label column so sequence / diff rows line up under each
    # other. Column width accommodates "  {longer_label} seq:  ".
    col_width = max(len(label_a), len(label_b), len("diff")) + len("  _ seq:  ")

    def prefix(text):
        return f"  {text}:".ljust(col_width)

    lines = [f"# record {record_num}  class={class_}"]
    lines.append(f"{prefix(label_a + ' name')}{defline_a[:preview_len]}")
    lines.append(f"{prefix(label_b + ' name')}{defline_b[:preview_len]}")

    if class_ == "LENGTH_DIFF":
        lines.append(
            f"  len: {label_a}={len(seq_a)}  {label_b}={len(seq_b)}"
        )
        lines.append(f"{prefix(label_a + ' seq')}{seq_a[:preview_len]}")
        lines.append(f"{prefix(label_b + ' seq')}{seq_b[:preview_len]}")
        lines.append("")
        return "\n".join(lines)

    caret = ["."] * min(len(seq_a), preview_len)
    for i, _, _ in diffs or []:
        if i < preview_len:
            caret[i] = "^"

    lines.append(f"{prefix(label_a + ' seq')}{seq_a[:preview_len]}")
    lines.append(f"{prefix(label_b + ' seq')}{seq_b[:preview_len]}")
    lines.append(f"{prefix('diff')}{''.join(caret)}")
    if diffs:
        first = diffs[0]
        lines.append(
            f"  first diff: pos={first[0]} "
            f"{label_a}={first[1]!r} {label_b}={first[2]!r}  "
            f"total_diffs={len(diffs)}"
        )
    lines.append("")
    return "\n".join(lines)


def run(file_a, file_b, out_dir, max_samples, label_a, label_b,
        position_buckets, quiet):
    os.makedirs(out_dir, exist_ok=True)

    class_counts = Counter()
    confusion = [[0] * (OTHER + 1) for _ in range(OTHER + 1)]
    position_hist = [0] * position_buckets
    total_records = 0
    qual_mismatches = 0
    qual_len_errors_a = 0
    qual_len_errors_b = 0
    saved_samples = 0
    read_length = None  # establish from first identical-length record

    samples_path = os.path.join(out_dir, "samples.txt")
    with open(samples_path, "w") as samples_fh:
        for (rec_a, rec_b) in zip(iter_fastq(file_a), iter_fastq(file_b)):
            total_records += 1
            defline_a, seq_a, _plus_a, qual_a = rec_a
            defline_b, seq_b, _plus_b, qual_b = rec_b

            # Quality length invariant (per-file)
            if len(qual_a) != len(seq_a):
                qual_len_errors_a += 1
            if len(qual_b) != len(seq_b):
                qual_len_errors_b += 1

            # Quality identity (we still report, but it's not the focus)
            if qual_a != qual_b:
                qual_mismatches += 1

            class_, diffs = classify_record(seq_a, seq_b)
            class_counts[class_] += 1

            if class_ == "IDENTICAL":
                continue

            if class_ != "LENGTH_DIFF" and read_length is None:
                read_length = len(seq_a)

            if diffs:
                use_length = read_length if read_length is not None else max(len(seq_a), 1)
                for pos, ca, cb in diffs:
                    row = BASE_INDEX.get(ca, OTHER)
                    col = BASE_INDEX.get(cb, OTHER)
                    confusion[row][col] += 1
                    position_hist[bucket_index(pos, use_length, position_buckets)] += 1

            if saved_samples < max_samples:
                samples_fh.write(format_sample(
                    total_records, class_,
                    defline_a, seq_a, defline_b, seq_b,
                    label_a, label_b, diffs,
                ))
                saved_samples += 1

    # zip() truncates to the shorter file; do a separate count pass so we can
    # surface file-length mismatches.
    count_a = sum(1 for _ in iter_fastq(file_a))
    count_b = sum(1 for _ in iter_fastq(file_b))
    truncated_compare = (count_a != count_b)

    # Emit summary.tsv
    summary_path = os.path.join(out_dir, "summary.tsv")
    total_diffs = sum(sum(row) for row in confusion)
    with open(summary_path, "w") as fh:
        fh.write("metric\tvalue\n")
        fh.write(f"file_a\t{file_a}\n")
        fh.write(f"file_b\t{file_b}\n")
        fh.write(f"label_a\t{label_a}\n")
        fh.write(f"label_b\t{label_b}\n")
        fh.write(f"records_compared\t{total_records}\n")
        fh.write(f"records_in_{label_a}\t{count_a}\n")
        fh.write(f"records_in_{label_b}\t{count_b}\n")
        fh.write(f"file_length_mismatch\t{int(truncated_compare)}\n")
        fh.write(f"read_length_sample\t{read_length if read_length is not None else 0}\n")
        for cls in ("IDENTICAL", "N_MASK_ONLY", "BASE_FLIP", "MIXED", "LENGTH_DIFF"):
            fh.write(f"class_{cls}\t{class_counts.get(cls, 0)}\n")
        fh.write(f"quality_mismatch_records\t{qual_mismatches}\n")
        fh.write(f"quality_length_errors_{label_a}\t{qual_len_errors_a}\n")
        fh.write(f"quality_length_errors_{label_b}\t{qual_len_errors_b}\n")
        fh.write(f"total_position_diffs\t{total_diffs}\n")

        seq_mismatch_records = (total_records - class_counts.get("IDENTICAL", 0))
        if seq_mismatch_records > 0:
            n_only = class_counts.get("N_MASK_ONLY", 0)
            fh.write(
                f"n_mask_only_fraction\t"
                f"{n_only / seq_mismatch_records:.4f}\n"
            )
        else:
            fh.write("n_mask_only_fraction\t0.0000\n")

        for i, count in enumerate(position_hist):
            fh.write(f"position_bucket_{i}_of_{position_buckets}\t{count}\n")

    # Emit confusion.tsv
    confusion_path = os.path.join(out_dir, "confusion.tsv")
    with open(confusion_path, "w") as fh:
        header = [f"{label_a}\\{label_b}"] + list(BASES) + ["OTHER"]
        fh.write("\t".join(header) + "\n")
        for i, row_label in enumerate(list(BASES) + ["OTHER"]):
            cells = [row_label] + [str(confusion[i][j]) for j in range(OTHER + 1)]
            fh.write("\t".join(cells) + "\n")

    # Emit positions.tsv
    positions_path = os.path.join(out_dir, "positions.tsv")
    with open(positions_path, "w") as fh:
        fh.write("bucket\tstart_pos\tend_pos\tmismatches\n")
        if read_length is not None and read_length > 0:
            for i, count in enumerate(position_hist):
                start = int(i * read_length / position_buckets)
                end = int((i + 1) * read_length / position_buckets) - 1
                fh.write(f"{i}\t{start}\t{end}\t{count}\n")
        else:
            for i, count in enumerate(position_hist):
                fh.write(f"{i}\t-\t-\t{count}\n")

    if not quiet:
        print(f"=== mismatch_report: {file_a} vs {file_b} ===")
        print(f"  records compared:  {total_records:,}")
        if truncated_compare:
            print(
                f"  WARNING: file length mismatch "
                f"({label_a}={count_a:,} {label_b}={count_b:,})"
            )
        for cls in ("IDENTICAL", "N_MASK_ONLY", "BASE_FLIP", "MIXED", "LENGTH_DIFF"):
            pct = (100 * class_counts.get(cls, 0) / total_records) if total_records else 0
            print(f"  {cls:<12} {class_counts.get(cls, 0):>12,}  ({pct:5.1f}%)")
        print(f"  total position diffs: {total_diffs:,}")
        if total_diffs > 0:
            n_col = sum(confusion[i][BASE_INDEX['N']] for i in range(OTHER + 1))
            n_row = sum(confusion[BASE_INDEX['N']][j] for j in range(OTHER + 1))
            print(
                f"  {label_a}→N positions: {n_row:,} "
                f"({100 * n_row / total_diffs:.1f}%)"
            )
            print(
                f"  {label_b}→N positions: {n_col:,} "
                f"({100 * n_col / total_diffs:.1f}%)"
            )
        print(f"  report dir:        {out_dir}")

    return {
        "total_records": total_records,
        "class_counts": dict(class_counts),
        "total_diffs": total_diffs,
        "truncated_compare": truncated_compare,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Structured mismatch report for two FASTQ files"
    )
    parser.add_argument("file_a", help="First FASTQ (typically sracha output)")
    parser.add_argument("file_b", help="Second FASTQ (typically fasterq-dump output)")
    parser.add_argument(
        "--out-dir", default=None,
        help="Directory to write reports (default: ./mismatch-report-<basename>)",
    )
    parser.add_argument("--max-samples", type=int, default=50)
    parser.add_argument("--label-a", default="sracha")
    parser.add_argument("--label-b", default="fasterq")
    parser.add_argument("--position-buckets", type=int, default=20)
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    out_dir = args.out_dir or f"./mismatch-report-{os.path.basename(args.file_a)}"
    stats = run(
        args.file_a, args.file_b, out_dir,
        args.max_samples, args.label_a, args.label_b,
        args.position_buckets, args.quiet,
    )
    # Exit code: 0 if all records identical, 1 otherwise. Useful for scripting.
    sys.exit(0 if stats["class_counts"].get("IDENTICAL", 0) == stats["total_records"]
             and not stats["truncated_compare"] else 1)


if __name__ == "__main__":
    main()
