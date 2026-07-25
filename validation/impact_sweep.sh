#!/usr/bin/env bash
#
# Phase 1 of the #76 impact assessment: classify how many public runs change
# output under the split-mode / READ_TYPE fixes, WITHOUT downloading archives.
#
# Why this exists as a separate pass from random_corpus.sh: that harness
# downloads each archive and runs both tools, which is minutes and gigabytes
# per accession. Deciding whether a run is even *affected* only needs the
# READ_LEN and READ_TYPE columns, and sra-tools' vdb-dump streams those over
# HTTP range requests — seconds per run, no download. So we screen cheaply
# here, then spend the expensive A/B pass (ab_corpus.sh) only on the hits.
#
# NOTE: the prober is deliberately vdb-dump, not sracha. The thing being
# measured is sracha's own routing/READ_TYPE handling, so classifying with
# sracha's decoder would be circular — a decoder bug would hide exactly the
# runs that matter. See #78 for giving sracha its own remote-inspection path;
# even then an independent oracle is the right call for this sweep.
#
# Usage:
#   bash validation/impact_sweep.sh --sbatch                 # sample 500, submit array
#   bash validation/impact_sweep.sh --sbatch -n 2000 --concurrency 40
#   bash validation/impact_sweep.sh -a accessions.txt        # serial, explicit list
#   bash validation/impact_sweep.sh --summary --resume-dir DIR
#   bash validation/impact_sweep.sh --hits --resume-dir DIR  # affected accessions, for ab_corpus.sh
#
# Results: validation/impact-sweep-results/<YYYYMMDD-HHMMSS>/
#   accessions.txt, results.tsv, strata.tsv, logs/<ACC>.log (errors only)

set -uo pipefail

# ---------- defaults ----------
N=500
SEED=""
ACCESSIONS_FILE=""
# Platforms sracha actually supports; legacy ones are rejected outright and
# would only add PROBE_ERR noise. Kept as an ENA instrument_platform list.
PLATFORMS="ILLUMINA,BGISEQ,DNBSEQ,ELEMENT,ULTIMA,PACBIO_SMRT,OXFORD_NANOPORE"
# first_public strata. The ENA portal returns rows in accession order and
# rejects `offset`, so a single query is heavily skewed toward the earliest
# accessions (a naive 90-run draw came back 100% DRR). Slicing by release
# year and platform is the cheapest way to spread the sample across
# submitters and loader versions -- which is what actually drives these
# archive shapes.
YEARS="2011,2013,2015,2017,2019,2021,2023,2025"
MIN_BASES=50000000
MAX_BASES=50000000000
# 0 = scan every row (exact). >0 = probe that many evenly spaced rows (fast,
# but can miss a shape change: SRR18959644 flips at its exact midpoint, and a
# run that flips at 90% would slip past a coarse probe). Full scan of a 44M
# row archive is ~4 min, so exact is affordable and is the default.
SAMPLE_ROWS=0
TIMEOUT_MIN=20
RESUME_DIR=""
# Added to SLURM_ARRAY_TASK_ID to get the accessions.txt line. Non-zero only
# for the second and later chunks when the list exceeds Slurm MaxArraySize.
INDEX_OFFSET=0
SBATCH_SUBMIT=0
SUMMARY_ONLY=0
HITS_ONLY=0
CONCURRENCY=25
CPUS_PER_TASK=2
MEM="2G"
TMP=""
PARTITION="rna"
JOB_NAME="sracha-impact-sweep"

while [[ $# -gt 0 ]]; do
    case "$1" in
        -n) N="$2"; shift 2 ;;
        -s|--seed) SEED="$2"; shift 2 ;;
        -a|--accessions) ACCESSIONS_FILE="$2"; shift 2 ;;
        --platforms) PLATFORMS="$2"; shift 2 ;;
        --years) YEARS="$2"; shift 2 ;;
        --min-bases) MIN_BASES="$2"; shift 2 ;;
        --max-bases) MAX_BASES="$2"; shift 2 ;;
        --sample) SAMPLE_ROWS="$2"; shift 2 ;;
        --timeout) TIMEOUT_MIN="$2"; shift 2 ;;
        --resume-dir) RESUME_DIR="$2"; shift 2 ;;
        --index-offset) INDEX_OFFSET="$2"; shift 2 ;;
        --sbatch) SBATCH_SUBMIT=1; shift ;;
        --summary) SUMMARY_ONLY=1; shift ;;
        --hits) HITS_ONLY=1; shift ;;
        --concurrency) CONCURRENCY="$2"; shift 2 ;;
        --cpus) CPUS_PER_TASK="$2"; shift 2 ;;
        --mem) MEM="$2"; shift 2 ;;
        --tmp) TMP="$2"; shift 2 ;;
        --partition) PARTITION="$2"; shift 2 ;;
        -h|--help) sed -n '3,30p' "$0"; exit 0 ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

# ---------- paths ----------
# Under sbatch, BASH_SOURCE points at the spool copy, so prefer the submit dir
# and hand sbatch the repo copy of this script (same trick as random_corpus.sh).
if [[ -n "${SLURM_SUBMIT_DIR:-}" ]]; then
    ROOT_DIR="$SLURM_SUBMIT_DIR"
else
    ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fi
SCRIPT_DIR="$ROOT_DIR/validation"
SCRIPT_SELF="$SCRIPT_DIR/impact_sweep.sh"

# ---------- results dir ----------
if [[ -n "$RESUME_DIR" ]]; then
    RESULTS_DIR="$RESUME_DIR"
    [[ -d "$RESULTS_DIR" ]] || { echo "no such results dir: $RESULTS_DIR" >&2; exit 1; }
elif [[ -n "${SLURM_ARRAY_TASK_ID:-}" ]]; then
    echo "array task requires --resume-dir" >&2; exit 1
else
    RESULTS_DIR="$SCRIPT_DIR/impact-sweep-results/$(date +%Y%m%d-%H%M%S)"
    mkdir -p "$RESULTS_DIR/logs"
fi
RESULTS_TSV="$RESULTS_DIR/results.tsv"
RESULTS_LOCK="$RESULTS_DIR/results.lock"
ACC_LIST="$RESULTS_DIR/accessions.txt"

if [[ ! -f "$RESULTS_TSV" ]]; then
    printf 'accession\tclass\tspots\tmax_slots\tsplit_files_changed\tsplit3_changed\torientation_bits\thas_technical\trows_scanned\tsecs\tnote\n' > "$RESULTS_TSV"
fi

# ---------- reporting modes ----------
print_summary() {
    echo "== impact sweep: $RESULTS_DIR"
    awk -F'\t' 'NR>1 {c[$2]++; tot++} END {
        printf "  total classified: %d\n", tot
        for (k in c) printf "  %-22s %6d  (%5.1f%%)\n", k, c[k], 100*c[k]/tot
    }' "$RESULTS_TSV" | sort
    # Wilson score interval for the affected proportion. A plain point estimate
    # invites over-reading a handful of hits out of a few hundred runs.
    awk -F'\t' 'NR>1 && $2 != "PROBE_ERR" {
        n++; if ($2 != "UNAFFECTED") k++
    } END {
        if (n == 0) { print "  no classified runs"; exit }
        p = k/n; z = 1.96
        d = 1 + z*z/n
        c = (p + z*z/(2*n)) / d
        m = z*sqrt(p*(1-p)/n + z*z/(4*n*n)) / d
        printf "\n  affected: %d/%d = %.2f%%  (95%% CI %.2f%% - %.2f%%)\n", k, n, 100*p, 100*(c-m), 100*(c+m)
    }' "$RESULTS_TSV"
}

print_hits() {
    # Affected accessions only, for feeding straight into ab_corpus.sh -a.
    awk -F'\t' 'NR>1 && $2 != "UNAFFECTED" && $2 != "PROBE_ERR" {print $1}' "$RESULTS_TSV"
}

[[ "$SUMMARY_ONLY" == "1" ]] && { print_summary; exit 0; }
[[ "$HITS_ONLY" == "1" ]] && { print_hits; exit 0; }

# ---------- sampling ----------
# Stratified draw: for each (platform, year) cell pull a pool from ENA, then
# take an equal share from each cell. Cells that return nothing are skipped and
# their quota is absorbed by the shuffle at the end.
sample_accessions() {
    local seed="$1" want="$2"
    local strata_log="$RESULTS_DIR/strata.tsv"
    : > "$strata_log"
    IFS=',' read -r -a plats <<< "$PLATFORMS"
    IFS=',' read -r -a yrs <<< "$YEARS"
    local cells=$(( ${#plats[@]} * ${#yrs[@]} ))
    (( cells < 1 )) && cells=1
    # Over-draw per cell so the final shuffle has slack when cells come up short.
    local per_cell=$(( want / cells * 4 + 25 ))

    local p y q
    for p in "${plats[@]}"; do
        for y in "${yrs[@]}"; do
            q="instrument_platform=${p} AND base_count>=${MIN_BASES} AND base_count<=${MAX_BASES}"
            q="$q AND first_public>=${y}-01-01 AND first_public<=${y}-12-31"
            curl -sS -X POST "https://www.ebi.ac.uk/ena/portal/api/search" \
                --data-urlencode "result=read_run" \
                --data-urlencode "query=$q" \
                --data-urlencode "fields=run_accession,instrument_platform,library_layout,base_count,first_public" \
                --data-urlencode "limit=${per_cell}" \
                --data-urlencode "format=tsv" 2>/dev/null \
                | tail -n +2 | awk -F'\t' -v p="$p" -v y="$y" 'NF {print $0"\t"p"\t"y}'
        done
    done > "$strata_log"

    local pool
    pool=$(wc -l < "$strata_log")
    echo "# stratified pool: $pool rows across $cells cells (platforms=$PLATFORMS years=$YEARS)" >&2
    if (( pool == 0 )); then
        echo "ENA returned no rows for any stratum" >&2
        return 1
    fi
    # Deterministic shuffle, same construction as sample_accessions.sh.
    local src
    src=$(mktemp)
    openssl enc -aes-256-ctr -pass "pass:${seed}" -nosalt < /dev/zero 2>/dev/null | head -c 1048576 > "$src"
    cut -f1 "$strata_log" | sort -u | shuf -n "$want" --random-source="$src"
    rm -f "$src"
}

if [[ ! -f "$ACC_LIST" ]]; then
    if [[ -n "$ACCESSIONS_FILE" ]]; then
        cp "$ACCESSIONS_FILE" "$ACC_LIST"
    else
        [[ -z "$SEED" ]] && SEED="${RANDOM}${RANDOM}"
        echo "$SEED" > "$RESULTS_DIR/seed"
        sample_accessions "$SEED" "$N" > "$ACC_LIST" || exit 1
    fi
    echo "accessions: $(grep -cv '^$\|^#' "$ACC_LIST") -> $ACC_LIST"
fi

# ---------- sbatch submission ----------
if [[ "$SBATCH_SUBMIT" == "1" ]]; then
    command -v sbatch >/dev/null || { echo "sbatch not found" >&2; exit 1; }
    TOTAL=$(grep -cv '^$\|^#' "$ACC_LIST")
    (( TOTAL < 1 )) && { echo "empty accession list" >&2; exit 1; }
    SLURM_TIME=$(( TIMEOUT_MIN * 2 + 10 ))

    echo "results dir : $RESULTS_DIR"
    echo "accessions  : $TOTAL"
    echo "partition   : $PARTITION   concurrency: $CONCURRENCY"
    echo "per task    : ${CPUS_PER_TASK} cpu, ${MEM}, ${SLURM_TIME}min"

    # Slurm caps the highest array index (MaxArraySize - 1), and it is a cap on
    # the *index*, not the element count -- so a 2000-run list cannot be
    # submitted as 1-2000 nor as 1001-2000. Split it into chunks of at most
    # MAX_IDX and give each chunk an --index-offset into accessions.txt.
    MAX_ARRAY=$(scontrol show config 2>/dev/null \
        | awk -F= '/^MaxArraySize/ {gsub(/ /,"",$2); print $2}')
    [[ -n "$MAX_ARRAY" ]] || MAX_ARRAY=1001
    MAX_IDX=$(( MAX_ARRAY - 1 ))
    (( MAX_IDX < 1 )) && MAX_IDX=1

    # Spread the requested concurrency across chunks so the total in flight
    # matches what the caller asked for.
    CHUNKS=$(( (TOTAL + MAX_IDX - 1) / MAX_IDX ))
    PER_CHUNK_CONC=$(( CONCURRENCY / CHUNKS ))
    (( PER_CHUNK_CONC < 1 )) && PER_CHUNK_CONC=1
    (( CHUNKS > 1 )) && echo "chunks      : $CHUNKS x <=${MAX_IDX} (MaxArraySize=$MAX_ARRAY), ${PER_CHUNK_CONC} concurrent each"

    JOB_IDS=()
    offset=0
    while (( offset < TOTAL )); do
        span=$(( TOTAL - offset ))
        (( span > MAX_IDX )) && span=$MAX_IDX

        SBATCH_ARGS=(
            --job-name="$JOB_NAME"
            --comment="$JOB_NAME"
            --partition="$PARTITION"
            --array="1-${span}%${PER_CHUNK_CONC}"
            --cpus-per-task="$CPUS_PER_TASK"
            --mem="$MEM"
            --time="${SLURM_TIME}"
            --output="$RESULTS_DIR/logs/slurm-%A_%a.out"
            --parsable
        )
        # --tmp stays opt-in: idle nodes on this cluster advertise TmpDisk=0 even
        # though /tmp exists, and a non-empty value makes the array un-schedulable.
        [[ -n "$TMP" ]] && SBATCH_ARGS+=(--tmp="$TMP")

        # An unchecked sbatch here used to print "submitted array job " with an
        # empty id and exit 0, so a rejected array looked like a running sweep
        # until someone noticed results.tsv never filled up.
        if ! JOB_ID=$(sbatch "${SBATCH_ARGS[@]}" "$SCRIPT_SELF" \
            --resume-dir "$RESULTS_DIR" --timeout "$TIMEOUT_MIN" \
            --sample "$SAMPLE_ROWS" --index-offset "$offset") || [[ -z "$JOB_ID" ]]; then
            echo "sbatch rejected the array for lines $((offset+1))-$((offset+span))" >&2
            (( ${#JOB_IDS[@]} )) && echo "already-submitted jobs: ${JOB_IDS[*]} (scancel them if you resubmit)" >&2
            exit 1
        fi
        JOB_IDS+=("$JOB_ID")
        offset=$(( offset + span ))
    done
    echo "submitted array job(s) ${JOB_IDS[*]}"
    echo "watch   : squeue -u \$USER -n $JOB_NAME"
    echo "summary : bash validation/impact_sweep.sh --summary --resume-dir $RESULTS_DIR"
    echo "hits    : bash validation/impact_sweep.sh --hits --resume-dir $RESULTS_DIR"
    exit 0
fi

# ---------- tooling ----------
if ! command -v vdb-dump >/dev/null 2>&1; then
    if declare -F module >/dev/null 2>&1 || [[ -f /etc/profile.d/modules.sh ]]; then
        # shellcheck disable=SC1091
        source /etc/profile.d/modules.sh 2>/dev/null || true
        module load sratoolkit/3.2.1 2>/dev/null || true
    fi
fi
VDB_DUMP="$(command -v vdb-dump || true)"
[[ -z "$VDB_DUMP" ]] && { echo "vdb-dump not found. Try: module load sratoolkit/3.2.1" >&2; exit 1; }

# sra-tools wants a writable settings file; array tasks share $HOME, so give
# each one its own to avoid concurrent first-run config writes racing.
export NCBI_SETTINGS="${NCBI_SETTINGS:-${TMPDIR:-/tmp}/sracha-sweep-ncbi.${SLURM_ARRAY_JOB_ID:-$$}.${SLURM_ARRAY_TASK_ID:-0}.mkfg}"

# Array tasks append concurrently, so serialise on a lock file. flock is
# Linux/util-linux; on a dev mac it is simply absent, and a serial run has no
# concurrent writers to protect anyway.
if command -v flock >/dev/null 2>&1; then
    record() {
        flock -x 200
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$@" >> "$RESULTS_TSV"
        flock -u 200
    } 200>"$RESULTS_LOCK"
else
    record() {
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$@" >> "$RESULTS_TSV"
    }
fi

# ---------- the classifier ----------
#
# Reproduces the filter/routing rules from crates/sracha-core/src/fastq/mod.rs
# under default options (skip technical, no --min-read-len):
#
#   kept(i)  = READ_LEN[i] > 0 AND READ_TYPE[i] is biological
#
#   split-files changes iff some kept slot i has a dropped slot j < i.
#     Old code numbered by position among survivors, new code by slot, so the
#     two differ exactly when something ahead of a survivor was dropped.
#
#   split-3 changes iff a spot has >= 3 kept biological reads.
#     Old rule was `== 2 -> _1/_2, else unpaired`; new rule is
#     `>= 2 -> _1.._N`, so only the 3+ case moves.
#
#   orientation_bits: READ_TYPE carries FORWARD/REVERSE. The old `rtype != 0`
#     test read those as technical, so these runs change content, not just
#     filenames.
#
# A missing READ_TYPE column means "all biological", matching the decoder's
# fallback and fasterq-dump's `num_read_type > read_id_0` guard.
classify_stream() {
    awk -F'\t' '
    {
        nl = split($1, L, /[, ]+/)
        nt = split($2, T, /[, ]+/)
        kept = 0; dropped_before = 0; changed = 0
        for (i = 1; i <= nl; i++) {
            len = L[i] + 0
            if (nt == 0) bio = 1
            else bio = (T[i] ~ /BIOLOGICAL/) ? 1 : 0
            if (nt > 0 && T[i] ~ /(FORWARD|REVERSE)/) orient = 1
            if (len > 0 && !bio) tech = 1
            if (len > 0 && bio) {
                kept++
                if (dropped_before) changed = 1
            } else {
                dropped_before = 1
            }
        }
        if (changed) sf = 1
        if (kept >= 3) s3 = 1
        if (nl > maxn) maxn = nl
        rows++
    }
    END { printf "%d\t%d\t%d\t%d\t%d\t%d\n", rows+0, maxn+0, sf+0, s3+0, orient+0, tech+0 }
    '
}

process_accession() {
    local acc="$1"
    local log="$RESULTS_DIR/logs/${acc}.log"
    local t0 secs spots info rows_arg out
    t0=$(date +%s)

    info=$(timeout "${TIMEOUT_MIN}m" "$VDB_DUMP" "$acc" --info 2>>"$log")
    spots=$(awk -F': *' '/^SEQ/ {gsub(/[, ]/, "", $2); print $2}' <<< "$info")
    if [[ -z "$spots" || "$spots" -lt 1 ]] 2>/dev/null; then
        record "$acc" PROBE_ERR 0 0 0 0 0 0 0 "$(( $(date +%s) - t0 ))" "no SEQ row count from vdb-dump --info"
        return
    fi

    rows_arg=""
    if (( SAMPLE_ROWS > 0 )); then
        # Evenly spaced offsets including both ends.
        rows_arg=$(awk -v n="$spots" -v k="$SAMPLE_ROWS" 'BEGIN {
            if (k > n) k = n
            for (i = 0; i < k; i++) printf "%s%d", (i ? "," : ""), 1 + int(i * (n - 1) / (k > 1 ? k - 1 : 1))
        }')
    fi

    # Runs with no physical READ_TYPE column make the two-column form fail;
    # fall back to READ_LEN alone, which the classifier reads as all-biological.
    if [[ -n "$rows_arg" ]]; then
        out=$(timeout "${TIMEOUT_MIN}m" "$VDB_DUMP" "$acc" -R "$rows_arg" -C READ_LEN,READ_TYPE -f tab 2>>"$log" | classify_stream)
        [[ "${out%%$'\t'*}" == "0" ]] && out=$(timeout "${TIMEOUT_MIN}m" "$VDB_DUMP" "$acc" -R "$rows_arg" -C READ_LEN -f tab 2>>"$log" | classify_stream)
    else
        out=$(timeout "${TIMEOUT_MIN}m" "$VDB_DUMP" "$acc" -C READ_LEN,READ_TYPE -f tab 2>>"$log" | classify_stream)
        [[ "${out%%$'\t'*}" == "0" ]] && out=$(timeout "${TIMEOUT_MIN}m" "$VDB_DUMP" "$acc" -C READ_LEN -f tab 2>>"$log" | classify_stream)
    fi

    secs=$(( $(date +%s) - t0 ))
    IFS=$'\t' read -r rows maxn sf s3 orient tech <<< "$out"

    if [[ -z "${rows:-}" || "$rows" == "0" ]]; then
        record "$acc" PROBE_ERR "$spots" 0 0 0 0 0 0 "$secs" "vdb-dump produced no rows"
        return
    fi

    local class=UNAFFECTED
    if (( sf && s3 )); then class="SPLIT_FILES+SPLIT3"
    elif (( sf )); then class="SPLIT_FILES"
    elif (( s3 )); then class="SPLIT3"
    elif (( orient )); then class="READTYPE_ORIENTATION"
    fi

    local note="-"
    (( SAMPLE_ROWS > 0 )) && note="sampled ${rows}/${spots} rows"

    record "$acc" "$class" "$spots" "$maxn" "$sf" "$s3" "$orient" "$tech" "$rows" "$secs" "$note"
    # Keep logs only for genuine problems.
    [[ "$class" != "PROBE_ERR" ]] && rm -f "$log"
}

# ---------- dispatch ----------
if [[ -n "${SLURM_ARRAY_TASK_ID:-}" ]]; then
    IDX=$(( SLURM_ARRAY_TASK_ID + INDEX_OFFSET ))
    ACC=$(awk 'NF && !/^#/' "$ACC_LIST" | sed -n "${IDX}p")
    [[ -z "$ACC" ]] && { echo "no accession at index $IDX" >&2; exit 1; }
    echo "# array task $SLURM_ARRAY_TASK_ID (line $IDX) on $(hostname) -> $ACC"
    process_accession "$ACC"
    exit 0
fi

# Serial mode: skip accessions already recorded so an interrupted run resumes.
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
