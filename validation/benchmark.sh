#!/usr/bin/env bash
#SBATCH --job-name=sracha-bench
#SBATCH --output=validation/benchmark_%A_%a.log
#SBATCH --partition=normal
#SBATCH --cpus-per-task=8
#SBATCH --mem=16G
#SBATCH --time=2:00:00
#SBATCH --array=0-3
#
# Benchmark sracha vs fastq-dump vs fasterq-dump.
#
# Interactive:
#   bash validation/benchmark.sh                  # all stages sequentially
#   bash validation/benchmark.sh --only=small     # one stage
#   bash validation/benchmark.sh --skip-large     # everything except large
#   bash validation/benchmark.sh \                # persist outputs
#       --only=medium --results-dir=validation/bench-results
#
# Array submission (parallel across stages):
#   sbatch validation/benchmark.sh
#   # each array task picks its stage from SLURM_ARRAY_TASK_ID and writes
#   # to validation/bench-results/<stage>.md
#
# Stages: small | medium | large | gzip
#
# Requires:
#   - hyperfine
#   - sra-tools installed in validation/sra-tools/ (auto-downloaded if missing)
#   - Test fixtures in crates/sracha-core/tests/fixtures/

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

SRACHA="$ROOT_DIR/target/release/sracha"
SRATOOLS_DIR="$SCRIPT_DIR/sra-tools"

SMALL_SRA="$ROOT_DIR/crates/sracha-core/tests/fixtures/SRR28588231.sra"   # 23 MiB
MEDIUM_SRA="$SCRIPT_DIR/SRR2584863.sra"                                   # 288 MiB
LARGE_SRA="$SCRIPT_DIR/SRR14724462.sra"                                   # 3.78 GiB

SKIP_LARGE=false
ONLY=""
RESULTS_DIR=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --only=*)         ONLY="${1#*=}"; shift ;;
        --only)           ONLY="${2:?}"; shift 2 ;;
        --results-dir=*)  RESULTS_DIR="${1#*=}"; shift ;;
        --results-dir)    RESULTS_DIR="${2:?}"; shift 2 ;;
        --skip-large)     SKIP_LARGE=true; shift ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

# Array-job dispatch: SLURM_ARRAY_TASK_ID picks a stage and persistent
# output dir so all tasks deposit into the same aggregation folder.
if [[ -n "${SLURM_ARRAY_TASK_ID:-}" ]]; then
    STAGES=(small medium large gzip)
    ONLY="${STAGES[$SLURM_ARRAY_TASK_ID]:?array index out of range}"
    : "${RESULTS_DIR:=$ROOT_DIR/validation/bench-results}"
    echo "=== array task $SLURM_ARRAY_TASK_ID → stage=$ONLY on $(hostname) ==="
fi

case "$ONLY" in
    ""|small|medium|large|gzip) ;;
    *) echo "--only must be one of: small medium large gzip (got $ONLY)" >&2; exit 2 ;;
esac

# Returns 0 if the given stage should run in this invocation.
should_run() {
    [[ -z "$ONLY" || "$ONLY" == "$1" ]]
}

BOLD='\033[1m'
RESET='\033[0m'
log() { echo -e "\n${BOLD}==> $1${RESET}"; }

# ---------- check prerequisites ----------

if ! command -v hyperfine &>/dev/null; then
    echo "ERROR: hyperfine not found. Install it first." >&2
    exit 1
fi

if [[ ! -x "$SRACHA" ]]; then
    echo "ERROR: sracha release binary not found at $SRACHA" >&2
    echo "  Run: cargo build --release" >&2
    exit 1
fi

# ---------- locate sra-tools ----------

FASTERQ_DUMP=""
FASTQ_DUMP=""
if compgen -G "$SRATOOLS_DIR/sratoolkit.*/bin/fasterq-dump" > /dev/null 2>&1; then
    FASTERQ_DUMP=$(ls "$SRATOOLS_DIR"/sratoolkit.*/bin/fasterq-dump | head -1)
    FASTQ_DUMP=$(ls "$SRATOOLS_DIR"/sratoolkit.*/bin/fastq-dump | head -1)
elif command -v fasterq-dump &>/dev/null; then
    FASTERQ_DUMP=$(command -v fasterq-dump)
    FASTQ_DUMP=$(command -v fastq-dump)
fi

if [[ -z "$FASTERQ_DUMP" ]]; then
    log "Installing sra-tools to $SRATOOLS_DIR..."
    mkdir -p "$SRATOOLS_DIR"
    TARBALL_URL="https://ftp-trace.ncbi.nlm.nih.gov/sra/sdk/current/sratoolkit.current-centos_linux64.tar.gz"
    TARBALL="$SRATOOLS_DIR/sratoolkit.tar.gz"
    curl -fSL -o "$TARBALL" "$TARBALL_URL"
    tar -xzf "$TARBALL" -C "$SRATOOLS_DIR"
    rm -f "$TARBALL"
    FASTERQ_DUMP=$(ls "$SRATOOLS_DIR"/sratoolkit.*/bin/fasterq-dump | head -1)
    FASTQ_DUMP=$(ls "$SRATOOLS_DIR"/sratoolkit.*/bin/fastq-dump | head -1)
fi

# ---------- setup ----------

# OUTDIR holds the markdown results; it persists when --results-dir is set,
# otherwise self-cleans on exit. SCRATCH always self-cleans — it holds the
# produced FASTQ files used by hyperfine --prepare, which we never want to
# pile up in a persistent --results-dir.
if [[ -n "$RESULTS_DIR" ]]; then
    OUTDIR="$RESULTS_DIR"
    mkdir -p "$OUTDIR"
    SCRATCH=$(mktemp -d "${TMPDIR:-/tmp}/sracha-bench-scratch.XXXXXX")
    trap 'rm -rf "$SCRATCH"' EXIT
else
    OUTDIR=$(mktemp -d "${TMPDIR:-/tmp}/sracha-bench.XXXXXX")
    SCRATCH="$OUTDIR"
    trap 'rm -rf "$OUTDIR"' EXIT
fi

NCPUS=$(nproc)

log "Benchmark configuration"
echo "  sracha:       $($SRACHA --version 2>&1)"
echo "  fasterq-dump: $("$FASTERQ_DUMP" --version 2>&1 | head -1)"
echo "  fastq-dump:   $("$FASTQ_DUMP" --version 2>&1 | head -1)"
echo "  CPUs:         $NCPUS"
echo "  Results dir:  $OUTDIR"
echo "  Scratch dir:  $SCRATCH"
if [[ -n "$ONLY" ]]; then
    echo "  Only stage:   $ONLY"
fi

# ---------- helper: timed single run ----------
#
# Run a command once and report wall-clock seconds, appending to $TIMING_FILE.
# Usage: TIMING_FILE=<path> timed_run <label> <command...>
timed_run() {
    local label="$1"; shift
    local start end elapsed
    start=$(date +%s.%N)
    "$@" >/dev/null 2>&1
    end=$(date +%s.%N)
    elapsed=$(echo "$end - $start" | bc)
    printf "  %-20s %s s\n" "$label:" "$elapsed"
    echo "$label $elapsed" >> "$TIMING_FILE"
}

# =====================================================================
# Benchmark: Small file (SRR28588231, 23 MiB / 66K spots)
# =====================================================================
if should_run small; then
    log "Benchmark: SRR28588231 (23 MiB, 66K spots) — uncompressed FASTQ"

    if [[ ! -f "$SMALL_SRA" ]]; then
        echo "  SKIP: $SMALL_SRA not found"
    else
        SMALL_OUT="$SCRATCH/small"
        mkdir -p "$SMALL_OUT/sracha" "$SMALL_OUT/fasterq" "$SMALL_OUT/fastq"

        hyperfine \
            --warmup 1 \
            --min-runs 5 \
            --prepare "rm -f $SMALL_OUT/sracha/*" \
            --prepare "rm -f $SMALL_OUT/fasterq/*" \
            --prepare "rm -f $SMALL_OUT/fastq/*" \
            -n "sracha" \
                "$SRACHA fastq $SMALL_SRA --no-gzip --no-progress -O $SMALL_OUT/sracha -f -q" \
            -n "fasterq-dump" \
                "$FASTERQ_DUMP $SMALL_SRA --split-3 -O $SMALL_OUT/fasterq -f" \
            -n "fastq-dump" \
                "$FASTQ_DUMP $SMALL_SRA --split-3 --outdir $SMALL_OUT/fastq" \
            --export-markdown "$OUTDIR/small.md" \
            2>&1

        echo
        echo "  Results saved to $OUTDIR/small.md"
        cat "$OUTDIR/small.md"
    fi
fi

# =====================================================================
# Benchmark: Medium file (SRR2584863, 288 MiB / ~1.6M spots)
# =====================================================================
if should_run medium; then
    log "Benchmark: SRR2584863 (288 MiB) — uncompressed FASTQ"

    if [[ ! -f "$MEDIUM_SRA" ]]; then
        log "Downloading SRR2584863 for medium benchmark..."
        "$SRACHA" fetch SRR2584863 -O "$SCRIPT_DIR" --no-progress
    fi

    if [[ ! -f "$MEDIUM_SRA" ]]; then
        echo "  SKIP: $MEDIUM_SRA not found"
    else
        MEDIUM_OUT="$SCRATCH/medium"
        mkdir -p "$MEDIUM_OUT/sracha" "$MEDIUM_OUT/fasterq" "$MEDIUM_OUT/fastq"

        hyperfine \
            --warmup 1 \
            --min-runs 3 \
            --prepare "rm -f $MEDIUM_OUT/sracha/*" \
            --prepare "rm -f $MEDIUM_OUT/fasterq/*" \
            --prepare "rm -f $MEDIUM_OUT/fastq/*" \
            -n "sracha" \
                "$SRACHA fastq $MEDIUM_SRA --no-gzip --no-progress -O $MEDIUM_OUT/sracha -f -q" \
            -n "fasterq-dump" \
                "$FASTERQ_DUMP $MEDIUM_SRA --split-3 -O $MEDIUM_OUT/fasterq -f" \
            -n "fastq-dump" \
                "$FASTQ_DUMP $MEDIUM_SRA --split-3 --outdir $MEDIUM_OUT/fastq" \
            --export-markdown "$OUTDIR/medium.md" \
            2>&1

        echo
        echo "  Results saved to $OUTDIR/medium.md"
        cat "$OUTDIR/medium.md"
    fi
fi

# =====================================================================
# Benchmark: Large file (SRR14724462, 3.78 GiB) — single run
# =====================================================================
if should_run large && [[ "$SKIP_LARGE" != true ]]; then
    if [[ ! -f "$LARGE_SRA" ]]; then
        log "Downloading SRR14724462 for large benchmark..."
        "$SRACHA" fetch SRR14724462 -O "$SCRIPT_DIR" --no-progress
    fi
    log "Benchmark: SRR14724462 (3.78 GiB) — uncompressed FASTQ (single run)"
    echo "  (fastq-dump skipped for large file — too slow)"

    LARGE_OUT="$SCRATCH/large"
    mkdir -p "$LARGE_OUT/sracha" "$LARGE_OUT/fasterq"

    TIMING_FILE="$OUTDIR/large-timing.txt"
    : > "$TIMING_FILE"

    timed_run "sracha" \
        "$SRACHA" fastq "$LARGE_SRA" --no-gzip --no-progress -O "$LARGE_OUT/sracha" -f -q

    rm -f "$LARGE_OUT/sracha"/*

    timed_run "fasterq-dump" \
        "$FASTERQ_DUMP" "$LARGE_SRA" --split-3 -O "$LARGE_OUT/fasterq" -f

    rm -f "$LARGE_OUT/fasterq"/*

    echo
    cat "$TIMING_FILE"

    # Render a markdown table so aggregation matches the hyperfine stages.
    {
        echo "| Command | Time [s] |"
        echo "|:---|---:|"
        awk '{ printf "| `%s` | %.2f |\n", $1, $2 }' "$TIMING_FILE"
    } > "$OUTDIR/large.md"
elif should_run large; then
    log "Benchmark: SRR14724462 SKIPPED (--skip-large)"
fi

# =====================================================================
# Benchmark: sracha gzip vs no-gzip (compression overhead)
# =====================================================================
if should_run gzip; then
    log "Benchmark: sracha gzip compression overhead (SRR28588231)"

    if [[ ! -f "$SMALL_SRA" ]]; then
        echo "  SKIP: $SMALL_SRA not found"
    else
        GZIP_OUT="$SCRATCH/gzip"
        mkdir -p "$GZIP_OUT/plain" "$GZIP_OUT/gzip"

        hyperfine \
            --warmup 1 \
            --min-runs 5 \
            --prepare "rm -f $GZIP_OUT/plain/*" \
            --prepare "rm -f $GZIP_OUT/gzip/*" \
            -n "sracha (no compression)" \
                "$SRACHA fastq $SMALL_SRA --no-gzip --no-progress -O $GZIP_OUT/plain -f -q" \
            -n "sracha (gzip)" \
                "$SRACHA fastq $SMALL_SRA --no-progress -O $GZIP_OUT/gzip -f -q" \
            --export-markdown "$OUTDIR/gzip.md" \
            2>&1

        echo
        cat "$OUTDIR/gzip.md"
    fi
fi

log "Benchmarking complete!"
