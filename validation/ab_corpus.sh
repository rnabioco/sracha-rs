#!/usr/bin/env bash
#
# Phase 2 of the #76 impact assessment: for accessions flagged by
# impact_sweep.sh (plus controls), run the pre-fix binary, the post-fix
# binary, and fasterq-dump over the same archive and record exactly what
# changed.
#
# random_corpus.sh answers "does sracha match fasterq-dump?". That is not
# quite the question here. Before releasing a change that renames output
# files we want the three-way verdict per accession:
#
#   FIXED        baseline disagreed with fasterq-dump, candidate agrees
#   UNCHANGED    both agree with fasterq-dump, byte-identical to each other
#   CHANGED_OK   output changed and candidate agrees (baseline also agreed --
#                worth eyeballing, means we altered already-correct output)
#   REGRESSED    baseline agreed with fasterq-dump, candidate does not
#   STILL_BROKEN neither agrees with fasterq-dump
#
# Usage:
#   # build the two binaries first (they cannot both live at target/release)
#   git worktree add /tmp/sracha-base main
#   cargo build --release --manifest-path /tmp/sracha-base/Cargo.toml
#   cargo build --release            # candidate, on the PR branch
#
#   bash validation/ab_corpus.sh --sbatch \
#       --baseline /tmp/sracha-base/target/release/sracha \
#       --candidate target/release/sracha \
#       -a hits.txt
#
#   bash validation/ab_corpus.sh --summary --resume-dir DIR
#
# Results: validation/ab-corpus-results/<YYYYMMDD-HHMMSS>/
#   accessions.txt, results.tsv, logs/<ACC>.<split>.log (non-UNCHANGED only)

set -uo pipefail

# ---------- defaults ----------
BASELINE=""
CANDIDATE=""
ACCESSIONS_FILE=""
SPLITS="split-files,split-3"
TIMEOUT_MIN=25
RESUME_DIR=""
SBATCH_SUBMIT=0
SUMMARY_ONLY=0
RUN_FASTERQ=1
KEEP_INTERMEDIATES=0
CONCURRENCY=8
CPUS_PER_TASK=8
MEM="16G"
TMP=""
PARTITION="rna"
JOB_NAME="sracha-ab-corpus"
WORK_DIR=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --baseline) BASELINE="$2"; shift 2 ;;
        --candidate) CANDIDATE="$2"; shift 2 ;;
        -a|--accessions) ACCESSIONS_FILE="$2"; shift 2 ;;
        --splits) SPLITS="$2"; shift 2 ;;
        --timeout) TIMEOUT_MIN="$2"; shift 2 ;;
        --resume-dir) RESUME_DIR="$2"; shift 2 ;;
        --sbatch) SBATCH_SUBMIT=1; shift ;;
        --summary) SUMMARY_ONLY=1; shift ;;
        --no-fasterq) RUN_FASTERQ=0; shift ;;
        --keep-intermediates) KEEP_INTERMEDIATES=1; shift ;;
        --concurrency) CONCURRENCY="$2"; shift 2 ;;
        --cpus) CPUS_PER_TASK="$2"; shift 2 ;;
        --mem) MEM="$2"; shift 2 ;;
        --tmp) TMP="$2"; shift 2 ;;
        --partition) PARTITION="$2"; shift 2 ;;
        --work-dir) WORK_DIR="$2"; shift 2 ;;
        -h|--help) sed -n '3,32p' "$0"; exit 0 ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

# ---------- paths ----------
if [[ -n "${SLURM_SUBMIT_DIR:-}" ]]; then
    ROOT_DIR="$SLURM_SUBMIT_DIR"
else
    ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fi
SCRIPT_DIR="$ROOT_DIR/validation"
SCRIPT_SELF="$SCRIPT_DIR/ab_corpus.sh"
COMPARE_PY="$SCRIPT_DIR/compare_fastq.py"

# ---------- results dir ----------
if [[ -n "$RESUME_DIR" ]]; then
    RESULTS_DIR="$RESUME_DIR"
    [[ -d "$RESULTS_DIR" ]] || { echo "no such results dir: $RESULTS_DIR" >&2; exit 1; }
elif [[ -n "${SLURM_ARRAY_TASK_ID:-}" ]]; then
    echo "array task requires --resume-dir" >&2; exit 1
else
    RESULTS_DIR="$SCRIPT_DIR/ab-corpus-results/$(date +%Y%m%d-%H%M%S)"
    mkdir -p "$RESULTS_DIR/logs"
fi
RESULTS_TSV="$RESULTS_DIR/results.tsv"
RESULTS_LOCK="$RESULTS_DIR/results.lock"
ACC_LIST="$RESULTS_DIR/accessions.txt"

if [[ ! -f "$RESULTS_TSV" ]]; then
    printf 'accession\tsplit\tverdict\tbaseline_files\tcandidate_files\tfasterq_files\tbase_vs_cand\tcand_vs_fasterq\tbase_vs_fasterq\tnote\n' > "$RESULTS_TSV"
fi

print_summary() {
    echo "== A/B corpus: $RESULTS_DIR"
    awk -F'\t' 'NR>1 {c[$2"/"$3]++; tot++} END {
        printf "  total rows: %d\n", tot
        for (k in c) printf "  %-28s %5d\n", k, c[k]
    }' "$RESULTS_TSV" | sort
    echo
    echo "  non-UNCHANGED rows:"
    awk -F'\t' 'NR>1 && $3 != "UNCHANGED" {printf "    %-14s %-12s %-14s %s\n", $1, $2, $3, $10}' "$RESULTS_TSV"
}

[[ "$SUMMARY_ONLY" == "1" ]] && { print_summary; exit 0; }

# ---------- accession list ----------
if [[ ! -f "$ACC_LIST" ]]; then
    [[ -n "$ACCESSIONS_FILE" ]] || { echo "need -a <accessions file> (e.g. impact_sweep.sh --hits)" >&2; exit 2; }
    cp "$ACCESSIONS_FILE" "$ACC_LIST"
    echo "accessions: $(grep -cv '^$\|^#' "$ACC_LIST") -> $ACC_LIST"
fi

# ---------- binaries ----------
# Resolve to absolute paths before sbatch: array tasks start in a different cwd.
[[ -n "$BASELINE" ]] && BASELINE="$(cd "$(dirname "$BASELINE")" && pwd)/$(basename "$BASELINE")"
[[ -n "$CANDIDATE" ]] && CANDIDATE="$(cd "$(dirname "$CANDIDATE")" && pwd)/$(basename "$CANDIDATE")"
for b in "$BASELINE" "$CANDIDATE"; do
    [[ -x "$b" ]] || { echo "not executable: '$b' (pass --baseline and --candidate)" >&2; exit 1; }
done

# ---------- sbatch submission ----------
if [[ "$SBATCH_SUBMIT" == "1" ]]; then
    command -v sbatch >/dev/null || { echo "sbatch not found" >&2; exit 1; }
    TOTAL=$(grep -cv '^$\|^#' "$ACC_LIST")
    (( TOTAL < 1 )) && { echo "empty accession list" >&2; exit 1; }
    SLURM_TIME=$(( TIMEOUT_MIN * 4 + 20 ))

    echo "results dir : $RESULTS_DIR"
    echo "accessions  : $TOTAL   splits: $SPLITS"
    echo "baseline    : $BASELINE"
    echo "candidate   : $CANDIDATE"
    echo "partition   : $PARTITION   concurrency: $CONCURRENCY"

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

    FORWARD=(--resume-dir "$RESULTS_DIR" --baseline "$BASELINE" --candidate "$CANDIDATE"
             --splits "$SPLITS" --timeout "$TIMEOUT_MIN")
    (( RUN_FASTERQ )) || FORWARD+=(--no-fasterq)
    (( KEEP_INTERMEDIATES )) && FORWARD+=(--keep-intermediates)
    [[ -n "$WORK_DIR" ]] && FORWARD+=(--work-dir "$WORK_DIR")

    JOB_ID=$(sbatch "${SBATCH_ARGS[@]}" "$SCRIPT_SELF" "${FORWARD[@]}")
    echo "submitted array job $JOB_ID"
    echo "watch   : squeue -u \$USER -n $JOB_NAME"
    echo "summary : bash validation/ab_corpus.sh --summary --resume-dir $RESULTS_DIR"
    exit 0
fi

# ---------- tooling ----------
FASTERQ_DUMP=""
if (( RUN_FASTERQ )); then
    if ! command -v fasterq-dump >/dev/null 2>&1; then
        if declare -F module >/dev/null 2>&1 || [[ -f /etc/profile.d/modules.sh ]]; then
            # shellcheck disable=SC1091
            source /etc/profile.d/modules.sh 2>/dev/null || true
            module load sratoolkit/3.2.1 2>/dev/null || true
        fi
    fi
    FASTERQ_DUMP="$(command -v fasterq-dump || true)"
    [[ -z "$FASTERQ_DUMP" ]] && { echo "fasterq-dump not found. Try: module load sratoolkit/3.2.1, or pass --no-fasterq" >&2; exit 1; }
fi

[[ -z "$WORK_DIR" ]] && WORK_DIR="${TMPDIR:-/tmp}/sracha-ab-corpus.${SLURM_ARRAY_JOB_ID:-$$}.${SLURM_ARRAY_TASK_ID:-0}"
mkdir -p "$WORK_DIR"
CURRENT_ACC=""
cleanup_current() {
    [[ "$KEEP_INTERMEDIATES" == "1" || -z "$CURRENT_ACC" ]] && return
    rm -rf "${WORK_DIR:?}/sra/$CURRENT_ACC" "${WORK_DIR:?}/out/$CURRENT_ACC"
}
trap 'cleanup_current; rm -rf "${WORK_DIR:?}"; exit 130' INT TERM
trap 'cleanup_current; [[ "$KEEP_INTERMEDIATES" == "1" ]] || rm -rf "${WORK_DIR:?}"' EXIT

if command -v flock >/dev/null 2>&1; then
    record() {
        flock -x 200
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$@" >> "$RESULTS_TSV"
        flock -u 200
    } 200>"$RESULTS_LOCK"
else
    record() { printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$@" >> "$RESULTS_TSV"; }
fi

# Sorted list of FASTQ basenames a tool produced. The file *set* is half the
# answer here -- slot-based numbering means a run can legitimately emit _2 and
# _4 with no _1/_3, so comparing a fixed _1/_2 pair would miss the change
# entirely.
file_set() { (cd "$1" 2>/dev/null && ls -- *.fastq 2>/dev/null | sort | tr '\n' ',' ); }

# Compare two output dirs over the union of their basenames.
# Echoes one of: IDENTICAL | CONTENT (deflines differ) | DIFFER:<detail>
compare_dirs() {
    local a="$1" b="$2" log="$3" strict_content="${4:-0}"
    local names detail="" worst="IDENTICAL"
    names=$( { ls "$a"/*.fastq "$b"/*.fastq; } 2>/dev/null | xargs -n1 basename 2>/dev/null | sort -u )
    if [[ -z "$names" ]]; then echo "DIFFER:no-fastq-either-side"; return; fi
    local n fa fb ma mb
    for n in $names; do
        fa="$a/$n"; fb="$b/$n"
        if [[ ! -f "$fa" ]]; then detail="${detail}${n}:missing-left "; worst="DIFFER"; continue; fi
        if [[ ! -f "$fb" ]]; then detail="${detail}${n}:missing-right "; worst="DIFFER"; continue; fi
        ma=$(md5sum "$fa" 2>/dev/null | cut -d' ' -f1); mb=$(md5sum "$fb" 2>/dev/null | cut -d' ' -f1)
        # Empty digests would compare equal and silently report a false match --
        # the exact failure mode this harness exists to catch. Fail loudly.
        if [[ -z "$ma" || -z "$mb" ]]; then
            detail="${detail}${n}:md5-unavailable "; worst="DIFFER"; continue
        fi
        [[ "$ma" == "$mb" ]] && continue
        if (( strict_content )); then
            detail="${detail}${n}:md5 "; worst="DIFFER"
        else
            # Tolerate defline-only differences the way random_corpus.sh does,
            # so a cosmetic header change is not reported as a content change.
            if python3 "$COMPARE_PY" "$fa" "$fb" >>"$log" 2>&1; then
                [[ "$worst" == "IDENTICAL" ]] && worst="CONTENT"
            else
                detail="${detail}${n}:content "; worst="DIFFER"
            fi
        fi
    done
    if [[ "$worst" == "DIFFER" ]]; then echo "DIFFER:${detail% }"; else echo "$worst"; fi
}

run_tool() {
    local bin="$1" sra="$2" out="$3" split="$4" log="$5"
    mkdir -p "$out"
    timeout "${TIMEOUT_MIN}m" "$bin" fastq "$sra" --split "$split" --no-gzip -O "$out" -f --no-progress >>"$log" 2>&1
}

process_accession() {
    local acc="$1"
    CURRENT_ACC="$acc"
    local sra_dir="$WORK_DIR/sra/$acc"
    mkdir -p "$sra_dir"

    local fetch_log="$RESULTS_DIR/logs/${acc}.fetch.log"
    : > "$fetch_log"
    # Fetch once with the candidate; download path is not what is under test.
    local fetch_rc=0
    timeout "${TIMEOUT_MIN}m" "$CANDIDATE" fetch "$acc" -O "$sra_dir" --no-progress >>"$fetch_log" 2>&1 || fetch_rc=$?
    if (( fetch_rc != 0 )); then
        # Capture rc on the same line as the call: a bare `local s` in between
        # would reset $? to local's own (zero) status and lose the 124.
        local s=ERROR_FETCH
        (( fetch_rc == 124 )) && s=TIMEOUT
        record "$acc" "-" "$s" - - - - - - "fetch failed (rc=$fetch_rc), see logs/${acc}.fetch.log"
        cleanup_current; return
    fi
    rm -f "$fetch_log"

    local sra
    sra=$(find "$sra_dir" -maxdepth 2 -type f \( -name '*.sra' -o -name '*.sralite' \) | head -1)
    if [[ -z "$sra" ]]; then
        record "$acc" "-" ERROR_FETCH - - - - - - "no archive after fetch"
        cleanup_current; return
    fi

    local split
    IFS=',' read -r -a split_arr <<< "$SPLITS"
    for split in "${split_arr[@]}"; do
        local log="$RESULTS_DIR/logs/${acc}.${split}.log"
        : > "$log"
        local base_out="$WORK_DIR/out/$acc/base" cand_out="$WORK_DIR/out/$acc/cand" fq_out="$WORK_DIR/out/$acc/fq"
        rm -rf "$WORK_DIR/out/$acc"; mkdir -p "$base_out" "$cand_out" "$fq_out"

        local base_rc=0 cand_rc=0
        run_tool "$BASELINE" "$sra" "$base_out" "$split" "$log" || base_rc=$?
        run_tool "$CANDIDATE" "$sra" "$cand_out" "$split" "$log" || cand_rc=$?

        # A run both binaries refuse (aligned cSRA, unsupported platform) is
        # handled, not a finding -- record and move on.
        if (( base_rc != 0 && cand_rc != 0 )); then
            local why=REJECTED_BOTH
            grep -qiE 'aligned SRA|cSRA' "$log" && why=REJECT_CSRA
            grep -qiE 'unsupported platform' "$log" && why=REJECT_PLATFORM
            record "$acc" "$split" "$why" - - - - - - "both binaries exited non-zero"
            continue
        fi
        if (( cand_rc != 0 )); then
            record "$acc" "$split" REGRESSED "$(file_set "$base_out")" - - - - - "candidate failed (rc=$cand_rc), baseline succeeded"
            continue
        fi
        if (( base_rc != 0 )); then
            record "$acc" "$split" FIXED - "$(file_set "$cand_out")" - - - - "baseline failed (rc=$base_rc), candidate succeeded"
            continue
        fi

        local fq_rc=0 fq_files="-" cand_fq="-" base_fq="-"
        if (( RUN_FASTERQ )); then
            timeout "${TIMEOUT_MIN}m" "$FASTERQ_DUMP" "$sra" "--${split}" -O "$fq_out" -f -t "$fq_out/tmp" >>"$log" 2>&1 || fq_rc=$?
            rm -rf "$fq_out/tmp"
        fi

        # baseline vs candidate: strict md5, since "did the bytes change" is
        # exactly the question. vs fasterq-dump uses the defline tolerance.
        local bvc; bvc=$(compare_dirs "$base_out" "$cand_out" "$log" 1)
        if (( RUN_FASTERQ && fq_rc == 0 )); then
            cand_fq=$(compare_dirs "$cand_out" "$fq_out" "$log" 0)
            base_fq=$(compare_dirs "$base_out" "$fq_out" "$log" 0)
            fq_files=$(file_set "$fq_out")
        elif (( RUN_FASTERQ )); then
            cand_fq="FASTERQ_FAILED"; base_fq="FASTERQ_FAILED"
        fi

        local ok_c=0 ok_b=0
        [[ "$cand_fq" == IDENTICAL || "$cand_fq" == CONTENT ]] && ok_c=1
        [[ "$base_fq" == IDENTICAL || "$base_fq" == CONTENT ]] && ok_b=1

        local verdict
        if (( ! RUN_FASTERQ )); then
            [[ "$bvc" == "IDENTICAL" ]] && verdict=UNCHANGED || verdict=CHANGED
        elif [[ "$cand_fq" == "FASTERQ_FAILED" ]]; then
            verdict=FASTERQ_FAILED
        elif (( ok_c && ! ok_b )); then verdict=FIXED
        elif (( ok_c && ok_b )); then
            [[ "$bvc" == "IDENTICAL" ]] && verdict=UNCHANGED || verdict=CHANGED_OK
        elif (( ! ok_c && ok_b )); then verdict=REGRESSED
        else verdict=STILL_BROKEN
        fi

        record "$acc" "$split" "$verdict" \
            "$(file_set "$base_out")" "$(file_set "$cand_out")" "$fq_files" \
            "$bvc" "$cand_fq" "$base_fq" "-"

        [[ "$verdict" == "UNCHANGED" ]] && rm -f "$log"
        rm -rf "$WORK_DIR/out/$acc"
    done

    cleanup_current
}

# ---------- dispatch ----------
if [[ -n "${SLURM_ARRAY_TASK_ID:-}" ]]; then
    ACC=$(awk 'NF && !/^#/' "$ACC_LIST" | sed -n "${SLURM_ARRAY_TASK_ID}p")
    [[ -z "$ACC" ]] && { echo "no accession at index $SLURM_ARRAY_TASK_ID" >&2; exit 1; }
    echo "# array task $SLURM_ARRAY_TASK_ID on $(hostname) -> $ACC"
    process_accession "$ACC"
    exit 0
fi

declare -A DONE=()
while IFS=$'\t' read -r a _rest; do
    [[ "$a" == "accession" ]] && continue
    DONE["$a"]=1
done < "$RESULTS_TSV"

while read -r acc; do
    [[ -z "$acc" || "$acc" == \#* ]] && continue
    [[ -n "${DONE[$acc]:-}" ]] && { echo "skip $acc (already recorded)"; continue; }
    echo "-> $acc"
    process_accession "$acc"
done < <(awk 'NF && !/^#/' "$ACC_LIST")

print_summary
