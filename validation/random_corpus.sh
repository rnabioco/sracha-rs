#!/usr/bin/env bash
#
# Run sracha vs fasterq-dump across a random corpus of SRA accessions,
# one accession per slurm array task (or serially, for local dev).
#
# Usage:
#   # Submit as slurm array (recommended for N>=10):
#   bash validation/random_corpus.sh --sbatch                # samples 30, submits --array=1-30%5
#   bash validation/random_corpus.sh --sbatch -n 10 --concurrency 3
#   bash validation/random_corpus.sh --sbatch -a list.txt    # explicit accessions
#
#   # Run serially on current host (for smoke tests):
#   bash validation/random_corpus.sh                         # samples 30, runs in-process
#   bash validation/random_corpus.sh -a list.txt
#
#   # Collect summary / retry failures:
#   bash validation/random_corpus.sh --summary --resume-dir DIR
#
# Results: validation/random-corpus-results/<YYYYMMDD-HHMMSS>/
#   accessions.txt, results.tsv, logs/<ACC>.log (fails only), logs/slurm-*.out

set -uo pipefail

# ---------- defaults / args ----------
N=30
SEED=""
ACCESSIONS_FILE=""
PLATFORM="ILLUMINA"
TIMEOUT_MIN=15
SPLIT="split-3"
# --aligned: sample from WGS-only to target reference-compressed cSRA
# archives (see validation/sample_accessions.sh). The sracha vs
# fasterq-dump comparison itself is cSRA-aware — sracha's pipeline
# dispatches decodable cSRA through CsraCursor automatically.
ALIGNED=0
KEEP_INTERMEDIATES=0
RESUME_DIR=""
WORK_DIR=""
SBATCH_SUBMIT=0
SUMMARY_ONLY=0
STREAM=0
CONCURRENCY=10
CPUS_PER_TASK=8
MEM="16G"
# --tmp is opt-in: this cluster's idle normal-partition nodes advertise
# TmpDisk=0 to slurm even though physical /tmp exists, so a non-empty
# default makes the whole array un-schedulable.
TMP=""
PARTITION="rna"
JOB_NAME="sracha-corpus"

while [[ $# -gt 0 ]]; do
    case "$1" in
        -n)                   N="$2"; shift 2 ;;
        -s|--seed)            SEED="$2"; shift 2 ;;
        -a|--accessions)      ACCESSIONS_FILE="$2"; shift 2 ;;
        -p|--platform)        PLATFORM="$2"; shift 2 ;;
        --aligned)            ALIGNED=1; shift ;;
        --timeout)            TIMEOUT_MIN="$2"; shift 2 ;;
        --split)              SPLIT="$2"; shift 2 ;;
        --keep-intermediates) KEEP_INTERMEDIATES=1; shift ;;
        --resume-dir)         RESUME_DIR="$2"; shift 2 ;;
        --work-dir)           WORK_DIR="$2"; shift 2 ;;
        --sbatch)             SBATCH_SUBMIT=1; shift ;;
        --summary)            SUMMARY_ONLY=1; shift ;;
        --stream)             STREAM=1; shift ;;
        --concurrency)        CONCURRENCY="$2"; shift 2 ;;
        --cpus)               CPUS_PER_TASK="$2"; shift 2 ;;
        --mem)                MEM="$2"; shift 2 ;;
        --tmp)                TMP="$2"; shift 2 ;;
        --partition)          PARTITION="$2"; shift 2 ;;
        -h|--help)
            sed -n '3,20p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

# ---------- paths ----------
# Under sbatch, BASH_SOURCE[0] points to the spool copy. Fall back to
# SLURM_SUBMIT_DIR so sibling scripts (compare_fastq.py, sample_accessions.sh)
# still resolve.
if [[ -n "${SLURM_SUBMIT_DIR:-}" ]]; then
    ROOT_DIR="$SLURM_SUBMIT_DIR"
    SCRIPT_DIR="$ROOT_DIR/validation"
    SCRIPT_SELF="$SCRIPT_DIR/random_corpus.sh"
else
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
    SCRIPT_SELF="$SCRIPT_DIR/random_corpus.sh"
fi
SRACHA="$ROOT_DIR/target/release/sracha"
COMPARE_PY="$SCRIPT_DIR/compare_fastq.py"
SAMPLE_SH="$SCRIPT_DIR/sample_accessions.sh"

# ---------- results dir bootstrap ----------
# This block sets up RESULTS_DIR + accessions.txt. Runs in all modes (submit,
# summary, array-task, serial) so the rest of the script can assume it exists.
if [[ -n "$RESUME_DIR" ]]; then
    RESULTS_DIR="$RESUME_DIR"
    if [[ ! -d "$RESULTS_DIR" ]]; then
        echo "ERROR: --resume-dir does not exist: $RESULTS_DIR" >&2
        exit 1
    fi
elif [[ -n "${SLURM_ARRAY_TASK_ID:-}" ]]; then
    echo "ERROR: running as array task but --resume-dir not provided" >&2
    exit 1
else
    TS=$(date +%Y%m%d-%H%M%S)
    RESULTS_DIR="$SCRIPT_DIR/random-corpus-results/$TS"
    mkdir -p "$RESULTS_DIR/logs"
fi

RESULTS_TSV="$RESULTS_DIR/results.tsv"
RESULTS_LOCK="$RESULTS_DIR/results.lock"
ACC_LIST="$RESULTS_DIR/accessions.txt"
SEED_FILE="$RESULTS_DIR/seed"
META_FILE="$RESULTS_DIR/metadata.tsv"

if [[ ! -f "$RESULTS_TSV" ]]; then
    printf 'accession\tstatus\tnote\tsracha_secs\tfasterq_secs\tsplit_files\n' > "$RESULTS_TSV"
fi

# Build accession list if we don't have one yet (not needed in summary-only).
if [[ "$SUMMARY_ONLY" -eq 0 && ! -f "$ACC_LIST" ]]; then
    if [[ -n "$ACCESSIONS_FILE" ]]; then
        cp "$ACCESSIONS_FILE" "$ACC_LIST"
    else
        SAMPLER_ARGS=(-n "$N" -p "$PLATFORM")
        [[ -n "$SEED" ]] && SAMPLER_ARGS+=(-s "$SEED")
        [[ "$ALIGNED" == "1" ]] && SAMPLER_ARGS+=(--aligned)
        bash "$SAMPLE_SH" "${SAMPLER_ARGS[@]}" > "$ACC_LIST" 2> "$META_FILE"
        grep -oP '(?<=seed=)\S+' "$META_FILE" | head -1 > "$SEED_FILE" 2>/dev/null || true
    fi
fi

# ---------- summary-only mode ----------
print_summary() {
    echo "=== summary ($RESULTS_DIR) ==="
    awk -F'\t' 'NR>1 {c[$2]++; tot++} END {
        printf "  total: %d\n", tot
        for (k in c) printf "  %-15s %d\n", k, c[k]
    }' "$RESULTS_TSV" | sort
    echo
    echo "failures:"
    awk -F'\t' 'NR>1 && $2 ~ /^FAIL|^ERROR|^TIMEOUT/ {printf "  %s\t%s\t%s\n", $1, $2, $3}' "$RESULTS_TSV"
}

if [[ "$SUMMARY_ONLY" -eq 1 ]]; then
    print_summary
    exit 0
fi

# ---------- sbatch submission mode ----------
if [[ "$SBATCH_SUBMIT" -eq 1 ]]; then
    if ! command -v sbatch >/dev/null 2>&1; then
        echo "ERROR: sbatch not found" >&2
        exit 1
    fi
    TOTAL=$(grep -cv '^$\|^#' "$ACC_LIST")
    if [[ "$TOTAL" -lt 1 ]]; then
        echo "ERROR: no accessions in $ACC_LIST" >&2
        exit 1
    fi
    # slurm time = 2x per-accession timeout, rounded up
    SLURM_TIME=$(( TIMEOUT_MIN * 2 + 10 ))
    echo "# results dir:  $RESULTS_DIR"
    echo "# accessions:   $TOTAL"
    echo "# job name:     $JOB_NAME"
    echo "# partition:    $PARTITION"
    echo "# concurrency:  $CONCURRENCY (max simultaneous array tasks)"
    echo "# per-task:     ${CPUS_PER_TASK}cpu ${MEM}${TMP:+ ${TMP}tmp} ${SLURM_TIME}min"
    echo
    SBATCH_ARGS=(
        --job-name="$JOB_NAME"
        --comment="$JOB_NAME"
        --partition="$PARTITION"
        --array="1-${TOTAL}%${CONCURRENCY}"
        --cpus-per-task="$CPUS_PER_TASK"
        --mem="$MEM"
        --time="${SLURM_TIME}"
        --output="$RESULTS_DIR/logs/slurm-%A_%a.out"
        --parsable
    )
    [[ -n "$TMP" ]] && SBATCH_ARGS+=(--tmp="$TMP")
    STREAM_FORWARD=()
    [[ "$STREAM" -eq 1 ]] && STREAM_FORWARD=(--stream)
    JOB_ID=$(sbatch "${SBATCH_ARGS[@]}" \
        "$SCRIPT_SELF" --resume-dir "$RESULTS_DIR" --split "$SPLIT" --timeout "$TIMEOUT_MIN" \
        "${STREAM_FORWARD[@]}")
    echo "# submitted:    $JOB_NAME ($JOB_ID)"
    echo
    echo "to watch: squeue -u \$USER -n $JOB_NAME   # or: squeue -j $JOB_ID"
    echo "to summarize: bash $0 --summary --resume-dir $RESULTS_DIR"
    exit 0
fi

# ---------- from here down: running accessions (serial or array task) ----------
if [[ ! -x "$SRACHA" ]]; then
    echo "ERROR: sracha binary not found at $SRACHA — run 'cargo build --release' first" >&2
    exit 1
fi

if command -v module >/dev/null 2>&1; then
    # shellcheck disable=SC1091
    source /etc/profile.d/modules.sh 2>/dev/null || true
    module load sratoolkit/3.2.1 2>/dev/null || true
fi
FASTERQ_DUMP="$(command -v fasterq-dump || true)"
if [[ -z "$FASTERQ_DUMP" ]]; then
    echo "ERROR: fasterq-dump not on PATH. Try: module load sratoolkit/3.2.1" >&2
    exit 1
fi
# We use sracha fetch (not prefetch) — parallel chunked, no sra-tools network
# dep, and produces SRA at the expected path without the extra ACC/ layer.

# Per-task work dir. Array tasks on the same node must not collide.
if [[ -z "$WORK_DIR" ]]; then
    WORK_DIR="${TMPDIR:-/tmp}/sracha-random-corpus.${SLURM_ARRAY_JOB_ID:-$$}.${SLURM_ARRAY_TASK_ID:-0}"
fi
mkdir -p "$WORK_DIR"

CURRENT_ACC=""
cleanup_current() {
    if [[ -n "$CURRENT_ACC" && "$KEEP_INTERMEDIATES" -eq 0 ]]; then
        rm -rf "$WORK_DIR/sra/$CURRENT_ACC" \
               "$WORK_DIR/sracha/$CURRENT_ACC" \
               "$WORK_DIR/fasterq/$CURRENT_ACC" 2>/dev/null
    fi
}
cleanup_workdir() {
    [[ "$KEEP_INTERMEDIATES" -eq 0 ]] && rm -rf "$WORK_DIR" 2>/dev/null
}
trap 'echo; echo "interrupted"; cleanup_current; cleanup_workdir; exit 130' INT TERM
trap 'cleanup_current; cleanup_workdir' EXIT

# ---------- helpers ----------
# Append a row under flock so concurrent array tasks don't garble results.tsv.
record() {
    (
        flock -x 200
        printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" "$5" "$6" >> "$RESULTS_TSV"
    ) 200>"$RESULTS_LOCK"
}

classify_pair() {
    local a="$1" b="$2" log="$3"
    local md5_a md5_b
    md5_a=$(md5sum "$a" | awk '{print $1}')
    md5_b=$(md5sum "$b" | awk '{print $1}')
    if [[ "$md5_a" == "$md5_b" ]]; then
        echo "PASS_MD5"
        return
    fi
    local cmp_log
    cmp_log=$(python3 "$COMPARE_PY" "$a" "$b" 2>&1)
    local rc=$?
    {
        echo "--- compare_fastq.py $(basename "$a") vs $(basename "$b") ---"
        echo "$cmp_log"
    } >> "$log"
    if [[ $rc -eq 0 ]]; then
        echo "PASS_CONTENT"
    elif echo "$cmp_log" | grep -q "ended at record"; then
        echo "FAIL_COUNT"
    elif echo "$cmp_log" | grep -q "SEQ MISMATCH"; then
        echo "FAIL_SEQ"
    elif echo "$cmp_log" | grep -Eq "QUAL MISMATCH|QUAL LEN|quality"; then
        echo "FAIL_QUAL"
    else
        echo "FAIL_CONTENT"
    fi
}

# Worst-case merge: any FAIL_* beats PASS_CONTENT beats PASS_MD5.
merge_status() {
    local current="$1" incoming="$2"
    [[ -z "$current" ]] && { echo "$incoming"; return; }
    for s in FAIL_MISSING FAIL_SEQ FAIL_COUNT FAIL_QUAL FAIL_CONTENT PASS_CONTENT; do
        if [[ "$current" == "$s" || "$incoming" == "$s" ]]; then
            echo "$s"; return
        fi
    done
    echo "PASS_MD5"
}

# Streaming variant: pipe both tools through `| md5sum` instead of
# materialising FASTQs on disk. Cuts ~2 GiB of disk I/O per ~500 MiB
# SRA, but on a mismatch we only learn "differs" — no per-record diff
# is possible without re-running with disk output. Uses interleaved
# (`--split-spot`) so a single stream covers all reads of every spot.
process_accession_stream() {
    local ACC="$1"
    CURRENT_ACC="$ACC"
    local LOG="$RESULTS_DIR/logs/${ACC}.log"
    : > "$LOG"

    local SRA_DIR="$WORK_DIR/sra/$ACC"
    mkdir -p "$SRA_DIR"

    echo "=== sracha fetch (stream) ===" >> "$LOG"
    local rc status
    if ! timeout "${TIMEOUT_MIN}m" "$SRACHA" fetch "$ACC" -O "$SRA_DIR" --no-progress >> "$LOG" 2>&1; then
        rc=$?
        status="ERROR_FETCH"
        [[ $rc -eq 124 ]] && status="TIMEOUT"
        echo "  $ACC: $status"
        record "$ACC" "$status" "sracha fetch rc=$rc" "" "" ""
        cleanup_current; CURRENT_ACC=""
        return
    fi

    local SRA_FILE
    SRA_FILE=$(find "$SRA_DIR" -maxdepth 2 -type f \( -name '*.sra' -o -name '*.sralite' \) | head -1)
    if [[ -z "$SRA_FILE" || ! -f "$SRA_FILE" ]]; then
        echo "  $ACC: ERROR_FETCH (no .sra file found)"
        record "$ACC" "ERROR_FETCH" "no sra file after prefetch" "" "" ""
        cleanup_current; CURRENT_ACC=""
        return
    fi
    local SRA_SIZE
    SRA_SIZE=$(du -h "$SRA_FILE" | awk '{print $1}')
    echo "  $ACC: sra=$SRA_SIZE"

    # sracha: interleaved+stdout → md5
    local SRACHA_MD5 SRACHA_RC SRACHA_SECS T0
    T0=$(date +%s)
    SRACHA_MD5=$(timeout "${TIMEOUT_MIN}m" "$SRACHA" fastq "$SRA_FILE" \
                    --split interleaved -Z --no-progress 2>>"$LOG" | md5sum | awk '{print $1}')
    SRACHA_RC=${PIPESTATUS[0]}
    SRACHA_SECS=$(( $(date +%s) - T0 ))
    if [[ $SRACHA_RC -ne 0 || -z "$SRACHA_MD5" ]]; then
        if grep -qE "aligned SRA|cSRA" "$LOG"; then
            status="REJECT_CSRA"
        elif grep -qE "platform.*reject|LS454|ION_TORRENT" "$LOG"; then
            status="REJECT_PLATFORM"
        elif grep -qE "page_map v1 variant 2" "$LOG"; then
            status="REJECT_VARIANT2"
        else
            status="FAIL_SRACHA"
        fi
        [[ $SRACHA_RC -eq 124 ]] && status="TIMEOUT"
        echo "  $ACC: $status (rc=$SRACHA_RC, ${SRACHA_SECS}s)"
        record "$ACC" "$status" "sracha rc=$SRACHA_RC" "$SRACHA_SECS" "" ""
        cleanup_current; CURRENT_ACC=""
        return
    fi

    # fasterq-dump: --split-spot stdout → md5
    local FASTERQ_MD5 FASTERQ_RC FASTERQ_SECS
    mkdir -p "$WORK_DIR/fasterq-tmp/$ACC"
    T0=$(date +%s)
    FASTERQ_MD5=$(timeout "${TIMEOUT_MIN}m" "$FASTERQ_DUMP" "$SRA_FILE" \
                    --split-spot --stdout -t "$WORK_DIR/fasterq-tmp/$ACC" 2>>"$LOG" \
                    | md5sum | awk '{print $1}')
    FASTERQ_RC=${PIPESTATUS[0]}
    FASTERQ_SECS=$(( $(date +%s) - T0 ))
    rm -rf "$WORK_DIR/fasterq-tmp/$ACC" 2>/dev/null
    if [[ $FASTERQ_RC -ne 0 || -z "$FASTERQ_MD5" ]]; then
        status="FAIL_FASTERQ"
        [[ $FASTERQ_RC -eq 124 ]] && status="TIMEOUT"
        echo "  $ACC: $status (rc=$FASTERQ_RC, ${FASTERQ_SECS}s) — reference tool failure"
        record "$ACC" "$status" "fasterq-dump rc=$FASTERQ_RC" "$SRACHA_SECS" "$FASTERQ_SECS" ""
        cleanup_current; CURRENT_ACC=""
        return
    fi

    if [[ "$SRACHA_MD5" == "$FASTERQ_MD5" ]]; then
        status="PASS_MD5"
        echo "  $ACC: PASS_MD5 (sracha=${SRACHA_SECS}s, fasterq=${FASTERQ_SECS}s)"
        record "$ACC" "$status" "stream md5=$SRACHA_MD5" "$SRACHA_SECS" "$FASTERQ_SECS" 1
        rm -f "$LOG"
    else
        status="FAIL_STREAM_MD5"
        echo "  $ACC: FAIL_STREAM_MD5 sracha=$SRACHA_MD5 fasterq=$FASTERQ_MD5 (sracha=${SRACHA_SECS}s, fasterq=${FASTERQ_SECS}s)"
        echo "stream md5 mismatch: sracha=$SRACHA_MD5 fasterq=$FASTERQ_MD5" >> "$LOG"
        record "$ACC" "$status" "sracha=$SRACHA_MD5 fasterq=$FASTERQ_MD5" "$SRACHA_SECS" "$FASTERQ_SECS" 1
    fi

    cleanup_current; CURRENT_ACC=""
}

# Process one accession. Writes one row to results.tsv and one log file.
process_accession() {
    if [[ "$STREAM" -eq 1 ]]; then
        process_accession_stream "$1"
        return
    fi
    local ACC="$1"
    CURRENT_ACC="$ACC"
    local LOG="$RESULTS_DIR/logs/${ACC}.log"
    : > "$LOG"

    local SRA_DIR="$WORK_DIR/sra/$ACC"
    local SRACHA_OUT="$WORK_DIR/sracha/$ACC"
    local FASTERQ_OUT="$WORK_DIR/fasterq/$ACC"
    mkdir -p "$SRA_DIR" "$SRACHA_OUT" "$FASTERQ_OUT"

    # --- fetch via sracha ---
    echo "=== sracha fetch ===" >> "$LOG"
    local rc status
    if ! timeout "${TIMEOUT_MIN}m" "$SRACHA" fetch "$ACC" -O "$SRA_DIR" --no-progress >> "$LOG" 2>&1; then
        rc=$?
        status="ERROR_FETCH"
        [[ $rc -eq 124 ]] && status="TIMEOUT"
        echo "  $ACC: $status"
        record "$ACC" "$status" "sracha fetch rc=$rc" "" "" ""
        cleanup_current; CURRENT_ACC=""
        return
    fi

    local SRA_FILE
    SRA_FILE=$(find "$SRA_DIR" -maxdepth 2 -type f \( -name '*.sra' -o -name '*.sralite' \) | head -1)
    if [[ -z "$SRA_FILE" || ! -f "$SRA_FILE" ]]; then
        echo "  $ACC: ERROR_FETCH (no .sra file found)"
        record "$ACC" "ERROR_FETCH" "no sra file after prefetch" "" "" ""
        cleanup_current; CURRENT_ACC=""
        return
    fi
    local SRA_SIZE
    SRA_SIZE=$(du -h "$SRA_FILE" | awk '{print $1}')
    echo "  $ACC: sra=$SRA_SIZE"

    # --- sracha ---
    local FASTERQ_SPLIT="--${SPLIT}"
    echo "=== sracha ===" >> "$LOG"
    local T0 SRACHA_RC SRACHA_SECS
    T0=$(date +%s)
    timeout "${TIMEOUT_MIN}m" "$SRACHA" fastq "$SRA_FILE" --split "$SPLIT" \
        --no-gzip -O "$SRACHA_OUT" -f --no-progress >> "$LOG" 2>&1
    SRACHA_RC=$?
    SRACHA_SECS=$(( $(date +%s) - T0 ))
    if [[ $SRACHA_RC -ne 0 ]]; then
        # Distinguish intentional rejections from real failures. sracha
        # exits non-zero for aligned cSRA / unsupported-platform runs
        # with a specific error string; those are correct behavior
        # ("handled" rather than "passed"), not bugs to chase.
        if grep -qE "aligned SRA|cSRA" "$LOG"; then
            status="REJECT_CSRA"
        elif grep -qE "platform.*reject|LS454|ION_TORRENT" "$LOG"; then
            status="REJECT_PLATFORM"
        elif grep -qE "page_map v1 variant 2" "$LOG"; then
            # Fail-fast on an ALTREAD page_map shape we don't yet decode
            # correctly — better to refuse than emit wrong FASTQ. Tracks
            # the note in pipeline::decode_blob_to_fastq.
            status="REJECT_VARIANT2"
        else
            status="FAIL_SRACHA"
        fi
        [[ $SRACHA_RC -eq 124 ]] && status="TIMEOUT"
        echo "  $ACC: $status (rc=$SRACHA_RC, ${SRACHA_SECS}s)"
        record "$ACC" "$status" "sracha rc=$SRACHA_RC" "$SRACHA_SECS" "" ""
        cleanup_current; CURRENT_ACC=""
        return
    fi

    # --- fasterq-dump ---
    echo "=== fasterq-dump ===" >> "$LOG"
    mkdir -p "$FASTERQ_OUT/tmp"
    local FASTERQ_RC FASTERQ_SECS
    T0=$(date +%s)
    timeout "${TIMEOUT_MIN}m" "$FASTERQ_DUMP" "$SRA_FILE" "$FASTERQ_SPLIT" \
        -O "$FASTERQ_OUT" -f -t "$FASTERQ_OUT/tmp" >> "$LOG" 2>&1
    FASTERQ_RC=$?
    FASTERQ_SECS=$(( $(date +%s) - T0 ))
    rm -rf "$FASTERQ_OUT/tmp" 2>/dev/null
    if [[ $FASTERQ_RC -ne 0 ]]; then
        status="FAIL_FASTERQ"
        [[ $FASTERQ_RC -eq 124 ]] && status="TIMEOUT"
        echo "  $ACC: $status (rc=$FASTERQ_RC, ${FASTERQ_SECS}s) — reference tool failure"
        record "$ACC" "$status" "fasterq-dump rc=$FASTERQ_RC" "$SRACHA_SECS" "$FASTERQ_SECS" ""
        cleanup_current; CURRENT_ACC=""
        return
    fi

    # --- compare ---
    local agg_status="" note="" file_count=0
    local fname sf ff pair_status
    local all_files
    all_files=$( ( cd "$SRACHA_OUT" && ls *.fastq 2>/dev/null
                   cd "$FASTERQ_OUT" && ls *.fastq 2>/dev/null ) | sort -u )
    for fname in $all_files; do
        [[ -z "$fname" ]] && continue
        sf="$SRACHA_OUT/$fname"
        ff="$FASTERQ_OUT/$fname"
        if [[ ! -f "$sf" ]]; then
            agg_status=$(merge_status "$agg_status" "FAIL_MISSING")
            note="${note}sracha missing $fname; "
            continue
        fi
        if [[ ! -f "$ff" ]]; then
            agg_status=$(merge_status "$agg_status" "FAIL_MISSING")
            note="${note}fasterq missing $fname; "
            continue
        fi
        pair_status=$(classify_pair "$sf" "$ff" "$LOG")
        agg_status=$(merge_status "$agg_status" "$pair_status")
        file_count=$((file_count + 1))
    done

    [[ -z "$agg_status" ]] && agg_status="FAIL_MISSING"
    [[ -z "$note" ]] && note="-"

    echo "  $ACC: $agg_status (files=$file_count, sracha=${SRACHA_SECS}s, fasterq=${FASTERQ_SECS}s)"
    record "$ACC" "$agg_status" "$note" "$SRACHA_SECS" "$FASTERQ_SECS" "$file_count"

    if [[ "$agg_status" == PASS_* ]]; then
        rm -f "$LOG"
    fi

    cleanup_current; CURRENT_ACC=""
}

# ---------- dispatch: one-accession (array) or loop (serial) ----------
if [[ -n "${SLURM_ARRAY_TASK_ID:-}" ]]; then
    # Array-task mode: process accession at this line number, skipping comments/blanks.
    ACC=$(awk 'NF && !/^#/' "$ACC_LIST" | sed -n "${SLURM_ARRAY_TASK_ID}p")
    if [[ -z "$ACC" ]]; then
        echo "ERROR: no accession at index $SLURM_ARRAY_TASK_ID in $ACC_LIST" >&2
        exit 1
    fi
    echo "# array task ${SLURM_ARRAY_TASK_ID} on $(hostname) → $ACC"
    process_accession "$ACC"
    exit 0
fi

# Serial mode: loop. Honor DONE set for resume-after-interrupt.
declare -A DONE
if [[ -s "$RESULTS_TSV" ]]; then
    while IFS=$'\t' read -r acc _rest; do
        [[ "$acc" == "accession" ]] && continue
        DONE["$acc"]=1
    done < "$RESULTS_TSV"
fi

TOTAL=$(grep -cv '^$\|^#' "$ACC_LIST")
echo "# results dir: $RESULTS_DIR"
echo "# accessions:  $TOTAL (serial)"
echo "# split mode:  $SPLIT"
echo "# timeout:     ${TIMEOUT_MIN}m per accession"

START_ALL=$(date +%s)
IDX=0
while IFS= read -r ACC || [[ -n "$ACC" ]]; do
    [[ -z "$ACC" || "$ACC" =~ ^# ]] && continue
    IDX=$((IDX + 1))
    if [[ -n "${DONE[$ACC]:-}" ]]; then
        echo "[$IDX/$TOTAL] $ACC — already in results.tsv, skipping"
        continue
    fi
    echo
    echo "[$IDX/$TOTAL] $ACC"
    process_accession "$ACC"
done < "$ACC_LIST"

ELAPSED=$(( $(date +%s) - START_ALL ))
echo
echo "elapsed: ${ELAPSED}s"
print_summary
