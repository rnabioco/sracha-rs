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
# Three legs, selected with --legs (default: fastq,dump):
#
#   fastq  the three-way FASTQ diff above; one row per --splits entry
#   dump   `sracha vdb dump` for a pinned row window, per column, against
#          `vdb-dump` on the same rows (#107). Recorded as split `vdb-dump`,
#          same verdict vocabulary. This is the only leg that reaches cSRA
#          and the legacy platforms, which `sracha fastq` refuses outright.
#          On by default: it measured ~15 s wall and ~2 s CPU per accession
#          on local archives, against minutes for a full decode, and a check
#          that has to be opted into is the check #104 walked past. Drop it
#          with --legs fastq (or --no-fasterq, which says there is no
#          sra-tools here and takes vdb-dump with it).
#   probe  a short prefix decode timed for cost only (#108). Recorded as
#          split `cost-probe`, verdicts COST_OK / SLOWER / FASTER. Off by
#          default: the fastq leg's own timings are free, since both binaries
#          have to run anyway, and answer the same question for any run that
#          is doing the FASTQ diff. The probe earns its keep as `--legs probe`
#          -- a cost-only sweep that skips the diff entirely.
#
# The fastq and probe legs are timed with /usr/bin/time and the wall/user/sys
# of each invocation lands in results.tsv. Compare CPU (user+sys), never wall:
# on a shared cluster the same binary on the same archive varies ~2x in wall
# purely from load, while CPU repeats to within a few percent.
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
#   # cheap cost-only sweep: no FASTQ diff, ~20 s of decode per accession
#   bash validation/ab_corpus.sh --legs probe --sra-dir /path/to/archives \
#       --baseline OLD --candidate NEW -a hits.txt
#
#   bash validation/ab_corpus.sh --summary --resume-dir DIR
#
# Options beyond the originals:
#   --legs L1,L2       legs to run (default fastq,dump)
#   --sra-dir D1:D2    use DIR/<acc>.sra if present instead of fetching
#   --dump-rows N      rows in the dump window (default 300)
#   --dump-columns C   columns the dump leg compares
#   --dump-table T     table to dump from (default: the archive's default)
#   --probe-bytes N    output bytes the cost probe decodes (default 200000000)
#   --cost-threshold R flag a row above this candidate/baseline CPU ratio (1.20)
#   --cost-min-delta S ...and only when the absolute gap exceeds S seconds (0.5)
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
LEGS="fastq,dump"
SRA_DIRS=""
DUMP_ROWS=300
# Columns whose sracha and vdb-dump renderings can be reconciled exactly --
# see canon_sracha/canon_vdbdump for the per-column normalisation, which was
# established by running both tools rather than reasoning about the schema.
# ALTREAD and CSREAD are deliberately absent: vdb-dump does not expose them as
# readable columns, and sracha renders them as raw hex. ALTREAD is still
# covered, indirectly and more usefully, through READ -- folding its N-mask
# into the basecalls is exactly what #104 got wrong.
DUMP_COLUMNS="READ,QUALITY,X,Y,READ_LEN,READ_START,SPOT_GROUP,RD_FILTER,SPOT_FILTER"
DUMP_TABLE=""
PROBE_BYTES=200000000
COST_THRESHOLD=1.20
# Absolute floor beneath the ratio, calibrated against a run of the same
# binary against itself: the worst same-binary CPU gap over 22 timed rows was
# 0.36 s, so 0.5 s clears the measured noise with headroom while still
# flagging a 2x regression on an archive that decodes in a second. It has to
# be an absolute gap and not a ratio alone -- a 0.05 s archive that takes
# 0.07 s is a 1.4x ratio and means nothing.
COST_MIN_DELTA=0.5

while [[ $# -gt 0 ]]; do
    case "$1" in
        --baseline) BASELINE="$2"; shift 2 ;;
        --candidate) CANDIDATE="$2"; shift 2 ;;
        -a|--accessions) ACCESSIONS_FILE="$2"; shift 2 ;;
        --splits) SPLITS="$2"; shift 2 ;;
        --legs) LEGS="$2"; shift 2 ;;
        --sra-dir) SRA_DIRS="$2"; shift 2 ;;
        --dump-rows) DUMP_ROWS="$2"; shift 2 ;;
        --dump-columns) DUMP_COLUMNS="$2"; shift 2 ;;
        --dump-table) DUMP_TABLE="$2"; shift 2 ;;
        --probe-bytes) PROBE_BYTES="$2"; shift 2 ;;
        --cost-threshold) COST_THRESHOLD="$2"; shift 2 ;;
        --cost-min-delta) COST_MIN_DELTA="$2"; shift 2 ;;
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
        -h|--help) sed -n '3,71p' "$0"; exit 0 ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

leg_enabled() { [[ ",$LEGS," == *",$1,"* ]]; }
RUN_FASTQ_LEG=0; leg_enabled fastq && RUN_FASTQ_LEG=1
RUN_DUMP_LEG=0;  leg_enabled dump  && RUN_DUMP_LEG=1
RUN_PROBE_LEG=0; leg_enabled probe && RUN_PROBE_LEG=1
if (( ! RUN_FASTQ_LEG && ! RUN_DUMP_LEG && ! RUN_PROBE_LEG )); then
    echo "--legs '$LEGS' selects nothing; valid legs: fastq, dump, probe" >&2; exit 2
fi
# The dump leg's reference is vdb-dump, which ships with fasterq-dump. Asking
# for --no-fasterq means "no sra-tools here", so honour that for both.
(( RUN_FASTERQ )) || RUN_DUMP_LEG=0

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

# Timing columns are appended after `note` rather than slotted in beside the
# comparison columns: field numbers 1-10 keep their existing meaning, so every
# awk one-liner written against an older results.tsv still works.
RESULTS_COLUMNS=19
if [[ ! -f "$RESULTS_TSV" ]]; then
    printf 'accession\tsplit\tverdict\tbaseline_files\tcandidate_files\tfasterq_files\tbase_vs_cand\tcand_vs_fasterq\tbase_vs_fasterq\tnote\tbase_wall\tbase_user\tbase_sys\tcand_wall\tcand_user\tcand_sys\tref_wall\tref_user\tref_sys\n' > "$RESULTS_TSV"
fi

print_summary() {
    echo "== A/B corpus: $RESULTS_DIR"
    awk -F'\t' 'NR>1 {c[$2"/"$3]++; tot++} END {
        printf "  total rows: %d\n", tot
        for (k in c) printf "  %-28s %5d\n", k, c[k]
    }' "$RESULTS_TSV" | sort
    echo
    echo "  non-UNCHANGED rows:"
    awk -F'\t' 'NR>1 && $2 != "cost-probe" && $3 != "UNCHANGED" {printf "    %-14s %-12s %-14s %s\n", $1, $2, $3, $10}' "$RESULTS_TSV"
    echo
    print_cost_summary
}

# Cost report. Ratios are candidate CPU / baseline CPU, where CPU is
# user+sys. Wall is recorded too but never compared: it tracks cluster load,
# not the binary. A row is flagged only when it clears both the ratio
# threshold and an absolute delta, so a 0.05 s archive that happened to take
# 0.07 s does not read as a 40% regression. Every row's ratio is in the note
# column regardless of whether it was flagged.
print_cost_summary() {
    echo "  cost (CPU = user+sys seconds; ratio = candidate/baseline, flagged above ${COST_THRESHOLD}x and +${COST_MIN_DELTA}s):"
    awk -F'\t' -v thr="$COST_THRESHOLD" -v mind="$COST_MIN_DELTA" '
        NR > 1 {
            if ($12 == "-" || $15 == "-" || $12 == "" || $15 == "") next
            b = $12 + $13; c = $15 + $16
            if (b <= 0) next
            n++; tb += b; tc += c
            r = c / b
            if (r > thr && c - b > mind)      { flag = "SLOWER" }
            else if (r < 1/thr && b - c > mind) { flag = "FASTER" }
            else                                { next }
            printf "    %-14s %-12s %9.2f %9.2f  %5.2fx  %s\n", $1, $2, b, c, r, flag
            flagged++
        }
        END {
            if (n == 0) { print "    (no timings recorded)"; exit }
            if (!flagged) printf "    none flagged\n"
            printf "    ---- %d timed rows: baseline %.1f s CPU, candidate %.1f s CPU, overall %.3fx\n", n, tb, tc, tc / tb
        }' "$RESULTS_TSV"
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
    echo "legs        : $LEGS"
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
             --splits "$SPLITS" --timeout "$TIMEOUT_MIN" --legs "$LEGS"
             --dump-rows "$DUMP_ROWS" --dump-columns "$DUMP_COLUMNS"
             --probe-bytes "$PROBE_BYTES"
             --cost-threshold "$COST_THRESHOLD" --cost-min-delta "$COST_MIN_DELTA")
    (( RUN_FASTERQ )) || FORWARD+=(--no-fasterq)
    (( KEEP_INTERMEDIATES )) && FORWARD+=(--keep-intermediates)
    [[ -n "$WORK_DIR" ]] && FORWARD+=(--work-dir "$WORK_DIR")
    [[ -n "$SRA_DIRS" ]] && FORWARD+=(--sra-dir "$SRA_DIRS")
    [[ -n "$DUMP_TABLE" ]] && FORWARD+=(--dump-table "$DUMP_TABLE")

    JOB_ID=$(sbatch "${SBATCH_ARGS[@]}" "$SCRIPT_SELF" "${FORWARD[@]}")
    echo "submitted array job $JOB_ID"
    echo "watch   : squeue -u \$USER -n $JOB_NAME"
    echo "summary : bash validation/ab_corpus.sh --summary --resume-dir $RESULTS_DIR"
    exit 0
fi

# ---------- tooling ----------
FASTERQ_DUMP=""
VDB_DUMP=""
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
    VDB_DUMP="$(command -v vdb-dump || true)"
fi
if (( RUN_DUMP_LEG )) && [[ -z "$VDB_DUMP" ]]; then
    echo "vdb-dump not found; the dump leg has no reference. Try: module load sratoolkit/3.2.1, or drop 'dump' from --legs" >&2
    exit 1
fi

# /usr/bin/time, not the bash `time` keyword: the keyword cannot write to a
# file, and a trailing `2>&1` on the keyword's output silently merges the
# timing into the tool's log where it is never seen again.
TIME_BIN=""
[[ -x /usr/bin/time ]] && TIME_BIN=/usr/bin/time
if (( RUN_PROBE_LEG )) && [[ -z "$TIME_BIN" ]]; then
    echo "/usr/bin/time not found; the probe leg measures nothing without it" >&2; exit 1
fi

[[ -z "$WORK_DIR" ]] && WORK_DIR="${TMPDIR:-/tmp}/sracha-ab-corpus.${SLURM_ARRAY_JOB_ID:-$$}.${SLURM_ARRAY_TASK_ID:-0}"
mkdir -p "$WORK_DIR"
CURRENT_ACC=""
cleanup_current() {
    [[ "$KEEP_INTERMEDIATES" == "1" || -z "$CURRENT_ACC" ]] && return
    rm -rf "${WORK_DIR:?}/sra/$CURRENT_ACC" "${WORK_DIR:?}/out/$CURRENT_ACC" \
           "${WORK_DIR:?}/dump/$CURRENT_ACC"
}
trap 'cleanup_current; rm -rf "${WORK_DIR:?}"; exit 130' INT TERM
trap 'cleanup_current; [[ "$KEEP_INTERMEDIATES" == "1" ]] || rm -rf "${WORK_DIR:?}"' EXIT

# Pad short calls with `-` so the existing ten-argument call sites keep
# working now that the row carries timing columns as well.
_record_line() {
    local -a f=("$@")
    while (( ${#f[@]} < RESULTS_COLUMNS )); do f+=("-"); done
    local IFS=$'\t'
    printf '%s\n' "${f[*]}"
}
if command -v flock >/dev/null 2>&1; then
    record() {
        flock -x 200
        _record_line "$@" >> "$RESULTS_TSV"
        flock -u 200
    } 200>"$RESULTS_LOCK"
else
    record() { _record_line "$@" >> "$RESULTS_TSV"; }
fi

# ---------- timing ----------
# Run a command under /usr/bin/time, leaving "wall user sys" in $1. The -o
# form is not optional: without it the timing goes to stderr, and every call
# site here redirects stderr into the tool's log.
timed() {
    local tf="$1"; shift
    rm -f "$tf"
    if [[ -n "$TIME_BIN" ]]; then
        "$TIME_BIN" -f '%e %U %S' -o "$tf" "$@"
    else
        "$@"
    fi
}

# Echo "wall user sys", or "- - -" when nothing usable was captured. On a
# non-zero exit /usr/bin/time prepends "Command exited with non-zero status
# N", so match the format line rather than taking the first line.
read_time() {
    local tf="$1"
    [[ -s "$tf" ]] || { printf -- '- - -'; return; }
    awk '/^[0-9.]+ [0-9.]+ [0-9.]+$/ {w=$1; u=$2; s=$3}
         END {if (w == "") printf "- - -"; else printf "%s %s %s", w, u, s}' "$tf"
}

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
    local bin="$1" sra="$2" out="$3" split="$4" log="$5" tf="${6:-/dev/null}"
    mkdir -p "$out"
    # /usr/bin/time wraps `timeout`, not the other way round: if the timeout
    # fires it kills the tool and `time` still records what it saw. Inverted,
    # the timeout would kill `time` before it wrote anything.
    timed "$tf" timeout "${TIMEOUT_MIN}m" \
        "$bin" fastq "$sra" --split "$split" --no-gzip -O "$out" -f --no-progress >>"$log" 2>&1
}

# ---------- vdb dump leg (#107) ----------
#
# The FASTQ legs never touch `sracha vdb dump`, which is how #104 (READ
# dropping ALTREAD's N-mask) survived a 369-accession A/B. This leg dumps a
# pinned row window per column from both binaries and diffs each against
# vdb-dump over the same rows.
#
# Things that are easy to get wrong here, each found by running it rather
# than by reading the schema (the third is at strip_row_id, where it bites):
#
#   * rows are addressed by ROW ID, not by ordinal. `id-range` reports a
#     first-row well above 1 on plenty of archives, and both tools clamp -R
#     to the column's actual range, so the window is computed from the
#     reported first-row rather than assumed to start at 1. The range comes
#     from vdb-dump so that the window is not defined by the thing under test.
#
#   * the two tools render the same column differently. The normalisation
#     below was established by running them side by side, not from the
#     schema: READ agrees byte for byte under -f tab, QUALITY does not
#     (vdb-dump prints Phred integers, sracha prints Phred+33 ASCII), and the
#     array columns disagree only on the separator.

# Strip sracha's leading row-id field, then undo its CSV quoting.
#
# `cut -f2-` is wrong for the first part: it passes a line with no delimiter
# through whole, which would turn an empty cell into a copy of the row id.
#
# The second part is not cosmetic. sracha's delimited writer wraps a text cell
# in double quotes and doubles any interior quote whenever the value contains
# one; vdb-dump never quotes. Phred+33 maps quality 1 to '"', so on SRR000001
# 84 of the first 300 QUALITY rows carry a quote and every one of them looked
# like a decode mismatch until the quoting was undone. Nothing else can
# produce a leading quote here -- the delimiter is a tab and neither Phred+33
# nor DNA contains one -- so a cell that starts with '"' is a quoted cell.
strip_row_id() {
    awk '{
        i = index($0, "\t")
        s = (i ? substr($0, i + 1) : "")
        if (substr(s, 1, 1) == "\"") { s = substr(s, 2, length(s) - 2); gsub(/""/, "\"", s) }
        print s
    }'
}

canon_sracha() {
    case "$1" in
        READ_LEN|READ_START|RD_FILTER|READ_FILTER)
            strip_row_id | tr ';' ',' ;;
        *)  strip_row_id ;;
    esac
}

canon_vdbdump() {
    case "$1" in
        QUALITY)
            # "34, 34, 38" -> "CCG". LC_ALL=C keeps %c single-byte.
            LC_ALL=C awk -F', ' '
                NF == 1 && $1 == "" {print ""; next}
                {s = ""; for (i = 1; i <= NF; i++) s = s sprintf("%c", $i + 33); print s}' ;;
        RD_FILTER|READ_FILTER)
            # vdb-dump prints the INSDC:SRA:read_filter enum by name.
            sed -e 's/SRA_READ_FILTER_PASS/0/g' -e 's/SRA_READ_FILTER_REJECT/1/g' \
                -e 's/SRA_READ_FILTER_CRITERIA/2/g' -e 's/SRA_READ_FILTER_REDACTED/3/g' \
                -e 's/, /,/g' ;;
        READ_LEN|READ_START)
            sed 's/, /,/g' ;;
        *)  cat ;;
    esac
}

# Dump one column from one sracha binary into $4. Exit status is the tool's.
sracha_dump() {
    local bin="$1" sra="$2" col="$3" out="$4" rows="$5" log="$6"
    local -a targs=()
    [[ -n "$DUMP_TABLE" ]] && targs=(-T "$DUMP_TABLE")
    if "$bin" vdb dump "$sra" "${targs[@]}" -C "$col" -R "$rows" -f tab 2>>"$log" \
        | canon_sracha "$col" > "$out"; then return 0; fi
    # A physical archive may store quality under ORIGINAL_QUALITY; vdb-dump
    # only ever exposes the logical QUALITY, so retry under the other name
    # before concluding the column is missing.
    if [[ "$col" == "QUALITY" ]]; then
        "$bin" vdb dump "$sra" "${targs[@]}" -C ORIGINAL_QUALITY -R "$rows" -f tab 2>>"$log" \
            | canon_sracha "$col" > "$out"
        return $?
    fi
    return 1
}

vdbdump_column() {
    local sra="$1" col="$2" out="$3" rows="$4" log="$5"
    local -a targs=()
    [[ -n "$DUMP_TABLE" ]] && targs=(-T "$DUMP_TABLE")
    "$VDB_DUMP" "$sra" "${targs[@]}" -C "$col" -R "$rows" -f tab 2>>"$log" \
        | canon_vdbdump "$col" > "$out"
}

dump_leg() {
    local acc="$1" sra="$2"
    local log="$RESULTS_DIR/logs/${acc}.vdb-dump.log"
    : > "$log"
    local dir="$WORK_DIR/dump/$acc"
    rm -rf "$dir"; mkdir -p "$dir"

    local -a targs=()
    [[ -n "$DUMP_TABLE" ]] && targs=(-T "$DUMP_TABLE")
    local range first count
    range=$("$VDB_DUMP" "$sra" "${targs[@]}" -r 2>>"$log")
    first=$(sed -n 's/.*first-row *= *\([0-9,]*\).*/\1/p' <<<"$range" | tr -d ',')
    count=$(sed -n 's/.*row-count *= *\([0-9,]*\).*/\1/p' <<<"$range" | tr -d ',')
    if [[ -z "$first" || -z "$count" ]] || (( count == 0 )); then
        record "$acc" "vdb-dump" VDBDUMP_FAILED - - - - - - "no id-range from vdb-dump, see logs/${acc}.vdb-dump.log"
        return
    fi
    local want="$DUMP_ROWS"
    (( want > count )) && want="$count"
    local rows="${first}-$(( first + want - 1 ))"
    echo "# window: rows $rows (first-row=$first row-count=$count)" >> "$log"

    local compared="" skipped="" bvc_detail="" cvr_detail="" bvr_detail=""
    local col brc crc
    local -a dump_cols
    IFS=',' read -r -a dump_cols <<< "$DUMP_COLUMNS"
    for col in "${dump_cols[@]}"; do
        [[ -z "$col" ]] && continue
        if ! vdbdump_column "$sra" "$col" "$dir/$col.ref" "$rows" "$log" || [[ ! -s "$dir/$col.ref" ]]; then
            skipped="${skipped}${col}:no-reference "; continue
        fi
        brc=0; crc=0
        sracha_dump "$BASELINE" "$sra" "$col" "$dir/$col.base" "$rows" "$log" || brc=$?
        sracha_dump "$CANDIDATE" "$sra" "$col" "$dir/$col.cand" "$rows" "$log" || crc=$?
        if (( brc != 0 && crc != 0 )); then
            # Neither binary reads it: a column this archive does not have,
            # or one sracha does not render. Not a finding, so do not let it
            # masquerade as agreement either.
            skipped="${skipped}${col}:unsupported "; continue
        fi
        compared="${compared}${col},"
        if (( crc != 0 )) || ! cmp -s "$dir/$col.cand" "$dir/$col.ref"; then
            cvr_detail="${cvr_detail}${col} "
        fi
        if (( brc != 0 )) || ! cmp -s "$dir/$col.base" "$dir/$col.ref"; then
            bvr_detail="${bvr_detail}${col} "
        fi
        if (( brc != crc )) || ! cmp -s "$dir/$col.base" "$dir/$col.cand"; then
            bvc_detail="${bvc_detail}${col} "
        fi
    done

    if [[ -z "$compared" ]]; then
        record "$acc" "vdb-dump" DUMP_SKIP - - - - - - "no comparable columns (${skipped:-none tried})"
        rm -rf "$dir"; return
    fi

    local bvc="IDENTICAL" cvr="IDENTICAL" bvr="IDENTICAL"
    [[ -n "$bvc_detail" ]] && bvc="DIFFER:${bvc_detail% }"
    [[ -n "$cvr_detail" ]] && cvr="DIFFER:${cvr_detail% }"
    [[ -n "$bvr_detail" ]] && bvr="DIFFER:${bvr_detail% }"

    local verdict
    if   [[ "$cvr" == IDENTICAL && "$bvr" != IDENTICAL ]]; then verdict=FIXED
    elif [[ "$cvr" != IDENTICAL && "$bvr" == IDENTICAL ]]; then verdict=REGRESSED
    elif [[ "$cvr" != IDENTICAL ]]; then                        verdict=STILL_BROKEN
    elif [[ "$bvc" == IDENTICAL ]]; then                        verdict=UNCHANGED
    else                                                        verdict=CHANGED_OK
    fi

    record "$acc" "vdb-dump" "$verdict" "${compared%,}" "${compared%,}" "${compared%,}" \
        "$bvc" "$cvr" "$bvr" "rows=$rows${skipped:+ skipped: ${skipped% }}"
    [[ "$verdict" == "UNCHANGED" ]] && rm -f "$log"
    rm -rf "$dir"
}

# ---------- cost probe leg (#108) ----------
#
# A full decode of a large archive is minutes; the cost signal shows up in
# seconds. Decoding to stdout through `head -c` stops at a fixed output
# prefix, which makes the measurement the same size of work on every archive
# and cheap enough to run on every row. sracha dies of SIGPIPE when head
# closes (rc 141) -- that is the intended end of the probe, not a failure.
#
# -Z implies uncompressed, so this times decode and formatting without the
# compressor, which is what a decoder change moves.
cost_probe() {
    local bin="$1" sra="$2" tf="$3" log="$4"
    timed "$tf" timeout "${TIMEOUT_MIN}m" \
        "$bin" fastq "$sra" --split interleaved -Z --no-progress 2>>"$log" \
        | head -c "$PROBE_BYTES" > /dev/null
    local rc=${PIPESTATUS[0]}
    (( rc == 141 )) && rc=0
    return "$rc"
}

probe_leg() {
    local acc="$1" sra="$2"
    local log="$RESULTS_DIR/logs/${acc}.cost-probe.log"
    : > "$log"
    local brc=0 crc=0
    cost_probe "$BASELINE"  "$sra" "$WORK_DIR/probe.base.time" "$log" || brc=$?
    cost_probe "$CANDIDATE" "$sra" "$WORK_DIR/probe.cand.time" "$log" || crc=$?
    local bt ct
    bt=$(read_time "$WORK_DIR/probe.base.time")
    ct=$(read_time "$WORK_DIR/probe.cand.time")

    if (( brc != 0 || crc != 0 )); then
        # shellcheck disable=SC2086  # $bt/$ct are "wall user sys" triples
        record "$acc" "cost-probe" COST_FAILED - - - - - - \
            "probe rc base=$brc cand=$crc, see logs/${acc}.cost-probe.log" $bt $ct - - -
        return
    fi

    local verdict
    verdict=$(awk -v b="$bt" -v c="$ct" -v thr="$COST_THRESHOLD" -v mind="$COST_MIN_DELTA" '
        BEGIN {
            split(b, x, " "); split(c, y, " ")
            if (x[2] == "-" || y[2] == "-") { print "COST_FAILED"; exit }
            bc = x[2] + x[3]; cc = y[2] + y[3]
            if (bc <= 0) { print "COST_FAILED"; exit }
            r = cc / bc
            if (r > thr && cc - bc > mind)        print "SLOWER"
            else if (r < 1/thr && bc - cc > mind) print "FASTER"
            else                                  print "COST_OK"
        }')
    local note
    note=$(awk -v b="$bt" -v c="$ct" -v n="$PROBE_BYTES" 'BEGIN {
        split(b, x, " "); split(c, y, " ")
        bc = x[2] + x[3]; cc = y[2] + y[3]
        printf "cpu %.2f -> %.2f (%.2fx) over %s output bytes", bc, cc, (bc > 0 ? cc / bc : 0), n
    }')

    # shellcheck disable=SC2086  # $bt/$ct are "wall user sys" triples
    record "$acc" "cost-probe" "$verdict" - - - - - - "$note" $bt $ct - - -
    [[ "$verdict" == "COST_OK" ]] && rm -f "$log"
}

# Look for an already-downloaded archive under --sra-dir before fetching.
# Re-downloading the corpus on every harness change is the slowest part of a
# development loop and none of it tests anything the harness is measuring.
local_archive() {
    local acc="$1" d p
    local -a dirs
    [[ -n "$SRA_DIRS" ]] || return 1
    IFS=':' read -r -a dirs <<< "$SRA_DIRS"
    for d in "${dirs[@]}"; do
        for p in "$d/$acc.sra" "$d/$acc.sralite" "$d/$acc/$acc.sra" "$d/$acc/$acc.sralite"; do
            [[ -f "$p" ]] && { echo "$p"; return 0; }
        done
    done
    return 1
}

process_accession() {
    local acc="$1"
    CURRENT_ACC="$acc"
    local sra_dir="$WORK_DIR/sra/$acc"
    mkdir -p "$sra_dir"

    local sra
    sra=$(local_archive "$acc" || true)
    if [[ -z "$sra" ]]; then
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
        sra=$(find "$sra_dir" -maxdepth 2 -type f \( -name '*.sra' -o -name '*.sralite' \) | head -1)
    fi
    if [[ -z "$sra" ]]; then
        record "$acc" "-" ERROR_FETCH - - - - - - "no archive after fetch"
        cleanup_current; return
    fi

    (( RUN_DUMP_LEG ))  && dump_leg  "$acc" "$sra"
    (( RUN_PROBE_LEG )) && probe_leg "$acc" "$sra"
    if (( ! RUN_FASTQ_LEG )); then cleanup_current; return; fi

    local split
    IFS=',' read -r -a split_arr <<< "$SPLITS"
    for split in "${split_arr[@]}"; do
        local log="$RESULTS_DIR/logs/${acc}.${split}.log"
        : > "$log"
        local base_out="$WORK_DIR/out/$acc/base" cand_out="$WORK_DIR/out/$acc/cand" fq_out="$WORK_DIR/out/$acc/fq"
        rm -rf "$WORK_DIR/out/$acc"; mkdir -p "$base_out" "$cand_out" "$fq_out"

        # Timing is free here: both binaries have to run for the diff anyway,
        # so wrapping them costs nothing and turns "did the bytes change" into
        # "did the bytes change, and what did they cost".
        local base_tf="$WORK_DIR/base.time" cand_tf="$WORK_DIR/cand.time" fq_tf="$WORK_DIR/fq.time"
        local base_rc=0 cand_rc=0
        run_tool "$BASELINE" "$sra" "$base_out" "$split" "$log" "$base_tf" || base_rc=$?
        run_tool "$CANDIDATE" "$sra" "$cand_out" "$split" "$log" "$cand_tf" || cand_rc=$?
        local bt ct ft
        bt=$(read_time "$base_tf"); ct=$(read_time "$cand_tf"); ft="- - -"

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
            timed "$fq_tf" timeout "${TIMEOUT_MIN}m" \
                "$FASTERQ_DUMP" "$sra" "--${split}" -O "$fq_out" -f -t "$fq_out/tmp" >>"$log" 2>&1 || fq_rc=$?
            ft=$(read_time "$fq_tf")
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

        # shellcheck disable=SC2086  # $bt/$ct/$ft are "wall user sys" triples
        record "$acc" "$split" "$verdict" \
            "$(file_set "$base_out")" "$(file_set "$cand_out")" "$fq_files" \
            "$bvc" "$cand_fq" "$base_fq" "-" $bt $ct $ft

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
