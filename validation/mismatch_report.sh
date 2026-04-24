#!/usr/bin/env bash
#
# Deep-dive diagnostic on a single SRA accession (or local .sra file):
# runs sracha fastq + fasterq-dump on the same input and produces a
# structured mismatch report via validation/mismatch_report.py.
#
# Usage:
#   bash validation/mismatch_report.sh <ACC|PATH> [--split split-3] [--out DIR]
#
# Example:
#   bash validation/mismatch_report.sh DRR035183
#   bash validation/mismatch_report.sh /path/to/local.sra
#
# Writes reports to:
#   validation/mismatch-reports/<YYYYMMDD-HHMMSS>/<ACC>/<slot>/
# where <slot> is "_1", "_2", or "unpaired" depending on split-3 output.

set -uo pipefail

# ---------- args ----------
INPUT=""
SPLIT="split-3"
OUT_BASE=""
MAX_SAMPLES=50

while [[ $# -gt 0 ]]; do
    case "$1" in
        --split)        SPLIT="$2"; shift 2 ;;
        --out)          OUT_BASE="$2"; shift 2 ;;
        --max-samples)  MAX_SAMPLES="$2"; shift 2 ;;
        -h|--help)
            sed -n '3,15p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *)
            if [[ -z "$INPUT" ]]; then INPUT="$1"; shift
            else echo "unexpected arg: $1" >&2; exit 2
            fi
            ;;
    esac
done

if [[ -z "$INPUT" ]]; then
    echo "usage: $(basename "$0") <ACC|PATH> [--split MODE] [--out DIR]" >&2
    exit 2
fi

# ---------- paths ----------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
SRACHA="${SRACHA:-$ROOT_DIR/target/release/sracha}"
REPORT_PY="$SCRIPT_DIR/mismatch_report.py"

if [[ ! -x "$SRACHA" ]]; then
    echo "ERROR: sracha binary not found at $SRACHA" >&2
    echo "  set SRACHA=<path> or run 'cargo build --release'" >&2
    exit 1
fi

# ---------- decide accession vs local file ----------
if [[ -f "$INPUT" ]]; then
    SRA_FILE="$INPUT"
    ACC=$(basename "$INPUT" .sra)
    ACC=$(basename "$ACC" .sralite)
    FETCH_NEEDED=0
else
    ACC="$INPUT"
    FETCH_NEEDED=1
fi

# ---------- output layout ----------
TS=$(date +%Y%m%d-%H%M%S)
if [[ -z "$OUT_BASE" ]]; then
    OUT_BASE="$ROOT_DIR/validation/mismatch-reports/$TS/$ACC"
fi
mkdir -p "$OUT_BASE"
WORK_DIR="$OUT_BASE/work"
mkdir -p "$WORK_DIR" "$WORK_DIR/sracha" "$WORK_DIR/fasterq"

echo "# mismatch_report driver"
echo "#   input:       $INPUT"
echo "#   accession:   $ACC"
echo "#   split:       $SPLIT"
echo "#   output dir:  $OUT_BASE"
echo

# ---------- fetch via sracha (if accession) ----------
if [[ $FETCH_NEEDED -eq 1 ]]; then
    echo "=== sracha fetch $ACC ==="
    if ! "$SRACHA" fetch "$ACC" -O "$WORK_DIR" --no-progress; then
        echo "ERROR: sracha fetch failed" >&2
        exit 1
    fi
    SRA_FILE=$(find "$WORK_DIR" -maxdepth 2 -type f \( -name '*.sra' -o -name '*.sralite' \) | head -1)
    if [[ -z "$SRA_FILE" ]]; then
        echo "ERROR: no .sra file found after fetch" >&2
        exit 1
    fi
    echo "  got: $SRA_FILE"
    echo
fi

# ---------- sracha fastq ----------
echo "=== sracha fastq ==="
if ! "$SRACHA" fastq "$SRA_FILE" --split "$SPLIT" --no-gzip \
        -O "$WORK_DIR/sracha" -f --no-progress; then
    echo "ERROR: sracha fastq failed" >&2
    exit 1
fi
echo

# ---------- fasterq-dump ----------
# Resolve fasterq-dump. Per `reference_sra_tools_module.md` the system
# `sratoolkit/3.2.1` module ships a binary that matches sra-tools parity
# (pixi's 3.4.1 segfaults). Use $FASTERQ_DUMP if set, else fall back to
# PATH, else the hardcoded module path.
echo "=== fasterq-dump ==="
FASTERQ_DUMP="${FASTERQ_DUMP:-}"
if [[ -z "$FASTERQ_DUMP" ]]; then
    if command -v fasterq-dump >/dev/null 2>&1; then
        FASTERQ_DUMP="$(command -v fasterq-dump)"
    elif [[ -x /cluster/software/modules-sw/sratoolkit/3.2.1/bin/fasterq-dump ]]; then
        FASTERQ_DUMP=/cluster/software/modules-sw/sratoolkit/3.2.1/bin/fasterq-dump
    else
        echo "ERROR: fasterq-dump not found — set FASTERQ_DUMP=<path> or 'module load sratoolkit/3.2.1'" >&2
        exit 1
    fi
fi
echo "  using: $FASTERQ_DUMP"
mkdir -p "$WORK_DIR/fasterq/tmp"
if ! "$FASTERQ_DUMP" "$SRA_FILE" "--${SPLIT}" -O "$WORK_DIR/fasterq" \
        -f -t "$WORK_DIR/fasterq/tmp" >/dev/null 2>&1; then
    echo "ERROR: fasterq-dump failed" >&2
    exit 1
fi
rm -rf "$WORK_DIR/fasterq/tmp"
echo "  fasterq-dump output ready"
echo

# ---------- pair up files by suffix ----------
compare_pair() {
    local srach="$1"
    local fasterq="$2"
    local slot="$3"

    if [[ ! -f "$srach" ]]; then
        echo "  skip $slot: sracha missing $(basename "$srach")"
        return
    fi
    if [[ ! -f "$fasterq" ]]; then
        echo "  skip $slot: fasterq-dump missing $(basename "$fasterq")"
        return
    fi

    local slot_out="$OUT_BASE/$slot"
    mkdir -p "$slot_out"
    echo "--- $slot: $(basename "$srach") vs $(basename "$fasterq") ---"
    python3 "$REPORT_PY" "$srach" "$fasterq" \
        --out-dir "$slot_out" \
        --max-samples "$MAX_SAMPLES" \
        --label-a sracha --label-b fasterq
    echo
}

case "$SPLIT" in
    split-3)
        compare_pair "$WORK_DIR/sracha/${ACC}_1.fastq" "$WORK_DIR/fasterq/${ACC}_1.fastq" "_1"
        compare_pair "$WORK_DIR/sracha/${ACC}_2.fastq" "$WORK_DIR/fasterq/${ACC}_2.fastq" "_2"
        compare_pair "$WORK_DIR/sracha/${ACC}.fastq"   "$WORK_DIR/fasterq/${ACC}.fastq"   "unpaired"
        ;;
    split-files)
        for n in 1 2 3 4; do
            compare_pair "$WORK_DIR/sracha/${ACC}_${n}.fastq" \
                         "$WORK_DIR/fasterq/${ACC}_${n}.fastq" "_${n}"
        done
        ;;
    split-spot|interleaved)
        compare_pair "$WORK_DIR/sracha/${ACC}.fastq" "$WORK_DIR/fasterq/${ACC}.fastq" "all"
        ;;
    *)
        echo "ERROR: unsupported split mode '$SPLIT'" >&2
        exit 2
        ;;
esac

echo "done — reports under: $OUT_BASE"
