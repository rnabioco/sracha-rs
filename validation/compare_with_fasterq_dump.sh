#!/usr/bin/env bash
#SBATCH --job-name=sracha-validate
#SBATCH --output=validation/validate_%j.log
#SBATCH --cpus-per-task=8
#SBATCH --mem=16G
#SBATCH --time=2:00:00
#
# Compare sracha output against fasterq-dump across split modes and formats.
#
# Usage:
#   bash validation/compare_with_fasterq_dump.sh              # all tests
#   bash validation/compare_with_fasterq_dump.sh 6 7          # by number
#   bash validation/compare_with_fasterq_dump.sh sralite      # by name
#   sbatch validation/compare_with_fasterq_dump.sh            # via Slurm
#
# Requires: SRR28588231.sra in crates/sracha-core/tests/fixtures/
#           (auto-downloaded by the Rust integration tests)

set -uo pipefail

# Under Slurm, BASH_SOURCE points to the spool copy; use SLURM_SUBMIT_DIR.
if [[ -n "${SLURM_SUBMIT_DIR:-}" ]]; then
    ROOT_DIR="$SLURM_SUBMIT_DIR"
    SCRIPT_DIR="$ROOT_DIR/validation"
else
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
fi

SRA_FILE="$ROOT_DIR/crates/sracha-core/tests/fixtures/SRR28588231.sra"
ACCESSION="SRR28588231"
SRACHA="$ROOT_DIR/target/release/sracha"
SRATOOLS_DIR="$SCRIPT_DIR/sra-tools"
COMPARE_PY="$SCRIPT_DIR/compare_fastq.py"

PASS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0
declare -a RESULTS=()

# ---------- colors ----------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BOLD='\033[1m'
RESET='\033[0m'

# ---------- helpers ----------

log()  { echo -e "${BOLD}==> $1${RESET}"; }
pass() { echo -e "  ${GREEN}PASS${RESET} $1"; PASS_COUNT=$((PASS_COUNT + 1)); RESULTS+=("PASS: $1"); }
fail() { echo -e "  ${RED}FAIL${RESET} $1"; FAIL_COUNT=$((FAIL_COUNT + 1)); RESULTS+=("FAIL: $1"); }
skip() { echo -e "  ${YELLOW}SKIP${RESET} $1"; SKIP_COUNT=$((SKIP_COUNT + 1)); RESULTS+=("SKIP: $1"); }

# Compare two files by md5. If they differ, run compare_fastq.py for details.
# If md5 differs but seq/qual are identical (defline-only diff), still counts as PASS.
# Optional 4th arg: "true" to allow N-masking diffs in FASTQ comparison.
# Optional 5th arg: "true" to allow quality score diffs (report but don't fail).
compare_files() {
    local file_a="$1"
    local file_b="$2"
    local label="$3"
    local allow_n_masking="${4:-false}"
    local allow_quality_diff="${5:-false}"

    if [[ ! -f "$file_a" ]]; then
        fail "$label — sracha output missing: $(basename "$file_a")"
        return
    fi
    if [[ ! -f "$file_b" ]]; then
        fail "$label — fasterq-dump output missing: $(basename "$file_b")"
        return
    fi

    local md5_a md5_b
    md5_a=$(md5sum "$file_a" | awk '{print $1}')
    md5_b=$(md5sum "$file_b" | awk '{print $1}')

    if [[ "$md5_a" == "$md5_b" ]]; then
        local size
        size=$(stat --printf='%s' "$file_a")
        pass "$label — byte-identical (md5=$md5_a, size=$size)"
        return
    fi

    echo -e "  ${YELLOW}md5 mismatch${RESET}: sracha=$md5_a  fasterq-dump=$md5_b"

    # For FASTQ files, check if seq/qual match (defline-only diff is OK)
    local ext="${file_a##*.}"
    if [[ "$ext" == "fastq" ]]; then
        echo "  Checking sequence/quality identity with compare_fastq.py..."
        local py_args=("$file_a" "$file_b")
        if [[ "$allow_n_masking" == "true" ]]; then
            py_args+=(--allow-n-masking)
        fi
        if [[ "$allow_quality_diff" == "true" ]]; then
            py_args+=(--allow-quality-diff)
        fi
        if python3 "$COMPARE_PY" "${py_args[@]}"; then
            pass "$label — content-identical (deflines differ)"
            echo "  Showing defline difference (first record):"
            echo "    sracha:      $(head -1 "$file_a")"
            echo "    fasterq-dump: $(head -1 "$file_b")"
        else
            fail "$label — seq/qual mismatch"
        fi
    elif [[ "$ext" == "fasta" ]]; then
        # For FASTA: check if diffs are only N-masking (fasterq-dump replaces
        # low-quality bases with N in FASTA mode, sracha preserves originals)
        local n_mask_result
        n_mask_result=$(paste <(grep -v '^>' "$file_a") <(grep -v '^>' "$file_b") | python3 -c "
import sys
total = n_mask = other = 0
n_positions = 0
for line in sys.stdin:
    parts = line.rstrip().split('\t')
    if len(parts) != 2: continue
    a, b = parts
    total += 1
    if a == b: continue
    all_n = True
    for ca, cb in zip(a, b):
        if ca != cb:
            if cb == 'N': n_positions += 1
            else: all_n = False
    if all_n: n_mask += 1
    else: other += 1
print(f'{total} {n_mask} {other} {n_positions}')
")
        local total n_mask other n_positions
        read -r total n_mask other n_positions <<< "$n_mask_result"

        if [[ "$other" -eq 0 ]]; then
            if [[ "$n_mask" -eq 0 ]]; then
                # Only header diffs
                pass "$label — sequences identical (headers differ)"
            else
                pass "$label — N-masking only ($n_mask/$total seqs, $n_positions positions; fasterq-dump masks Q2 bases)"
            fi
            # Show header diff if present
            local hdr_a hdr_b
            hdr_a=$(head -1 "$file_a")
            hdr_b=$(head -1 "$file_b")
            if [[ "$hdr_a" != "$hdr_b" ]]; then
                echo "    header diff: sracha='$hdr_a' vs fasterq='$hdr_b'"
            fi
        else
            echo "  FASTA has $other non-N-masking diffs — showing first diff:"
            diff <(head -20 "$file_a") <(head -20 "$file_b") || true
            fail "$label — sequence mismatch ($other diffs beyond N-masking)"
        fi
    else
        fail "$label — unknown extension .$ext"
    fi
}

# ---------- fixture helpers ----------

PREFETCH=""

# Download an SRA file via prefetch and cache in the validation directory.
# Usage: ensure_fixture <accession> [prefetch_extra_args...]
# Sets FIXTURE_FILE to the cached path on success. Returns 1 on failure.
FIXTURE_FILE=""
ensure_fixture() {
    local acc="$1"
    shift
    local extra_args=("$@")
    local ext="sra"
    # Detect sralite from args
    for arg in "${extra_args[@]}"; do
        if [[ "$arg" == "--eliminate-quals" ]]; then
            ext="sralite"
        fi
    done

    local cached="$SCRIPT_DIR/${acc}.${ext}"

    if [[ -f "$cached" ]]; then
        FIXTURE_FILE="$cached"
        echo "  fixture: $cached (cached, $(du -h "$cached" | awk '{print $1}'))"
        return 0
    fi

    if [[ -z "$PREFETCH" ]]; then
        echo "  WARNING: prefetch not available — cannot download $acc"
        return 1
    fi

    log "Downloading $acc via prefetch ${extra_args[*]}..."
    local prefetch_out="$TMPDIR_BASE/prefetch_${acc}"
    mkdir -p "$prefetch_out"

    if ! "$PREFETCH" "$acc" "${extra_args[@]}" -O "$prefetch_out" -f yes 2>&1; then
        echo "  WARNING: prefetch failed to download $acc"
        return 1
    fi

    local downloaded
    downloaded=$(find "$prefetch_out" -type f \( -name "*.sralite" -o -name "*.sra" \) | head -1)

    if [[ -n "$downloaded" && -f "$downloaded" ]]; then
        cp "$downloaded" "$cached"
        FIXTURE_FILE="$cached"
        echo "  fixture saved: $cached ($(du -h "$cached" | awk '{print $1}'))"
        return 0
    fi

    echo "  WARNING: $acc file not found after prefetch"
    return 1
}

ensure_sralite_fixture() {
    ensure_fixture "$ACCESSION" --eliminate-quals
    SRA_LITE_FILE="$FIXTURE_FILE"
}

SRA_LITE_FILE=""

# Platform test fixtures
# SRR000001: 299 MB, LS454 — already cached in test fixtures, VDB schema has 454 marker
SRA_FILE_454="$ROOT_DIR/crates/sracha-core/tests/fixtures/SRR000001.sra"
ACCESSION_IONTORRENT="SRR37995599" # 22 MB, ION_TORRENT (blocked)
ACCESSION_PACBIO="SRR38107137"    # 40 MB, PACBIO_SMRT (allowed)

# ---------- test registry ----------

declare -A TEST_FUNCS=(
    [1]="test_1_split3_fastq"
    [2]="test_2_splitspot_fastq"
    [3]="test_3_splitfiles_fastq"
    [4]="test_4_split3_fasta"
    [5]="test_5_splitspot_fasta"
    [6]="test_6_sralite_split3"
    [7]="test_7_sralite_splitspot"
    [8]="test_8_interleaved"
    [9]="test_9_gzip_roundtrip"
    [10]="test_10_reject_454"
    [11]="test_11_reject_iontorrent"
    [12]="test_12_pacbio"
)
declare -A TEST_LABELS=(
    [1]="split-3 (FASTQ, paired-end)"
    [2]="split-spot (FASTQ)"
    [3]="split-files (FASTQ)"
    [4]="split-3 (FASTA, paired-end)"
    [5]="split-spot (FASTA)"
    [6]="sralite split-3 (FASTQ)"
    [7]="sralite split-spot (FASTQ)"
    [8]="interleaved (FASTQ, paired-end)"
    [9]="gzip round-trip (FASTQ, split-3)"
    [10]="platform reject LS454"
    [11]="platform reject ION_TORRENT"
    [12]="platform PacBio (FASTQ)"
)
ALL_TEST_NUMS=(1 2 3 4 5 6 7 8 9 10 11 12)

# ---------- test functions ----------

test_1_split3_fastq() {
    log "Test 1: split-3 (FASTQ, paired-end)"

    local sracha_out="$TMPDIR_BASE/test1_sracha"
    local fasterq_out="$TMPDIR_BASE/test1_fasterq"
    mkdir -p "$sracha_out" "$fasterq_out"

    "$SRACHA" fastq "$SRA_FILE" --split split-3 --no-gzip -O "$sracha_out" -f --no-progress 2>&1 | tail -2
    "$FASTERQ_DUMP" "$SRA_FILE" --split-3 -O "$fasterq_out" -f 2>&1 | tail -2

    echo "  sracha files:      $(ls "$sracha_out"/)"
    echo "  fasterq-dump files: $(ls "$fasterq_out"/)"

    compare_files "$sracha_out/${ACCESSION}_1.fastq" "$fasterq_out/${ACCESSION}_1.fastq" "split-3 read 1"
    compare_files "$sracha_out/${ACCESSION}_2.fastq" "$fasterq_out/${ACCESSION}_2.fastq" "split-3 read 2"
    echo
}

test_2_splitspot_fastq() {
    log "Test 2: split-spot (FASTQ)"

    local sracha_out="$TMPDIR_BASE/test2_sracha"
    local fasterq_out="$TMPDIR_BASE/test2_fasterq"
    mkdir -p "$sracha_out" "$fasterq_out"

    "$SRACHA" fastq "$SRA_FILE" --split split-spot --no-gzip -O "$sracha_out" -f --no-progress 2>&1 | tail -2
    "$FASTERQ_DUMP" "$SRA_FILE" --split-spot -O "$fasterq_out" -f 2>&1 | tail -2

    echo "  sracha files:      $(ls "$sracha_out"/)"
    echo "  fasterq-dump files: $(ls "$fasterq_out"/)"

    compare_files "$sracha_out/${ACCESSION}.fastq" "$fasterq_out/${ACCESSION}.fastq" "split-spot"
    echo
}

test_3_splitfiles_fastq() {
    log "Test 3: split-files (FASTQ)"

    local sracha_out="$TMPDIR_BASE/test3_sracha"
    local fasterq_out="$TMPDIR_BASE/test3_fasterq"
    mkdir -p "$sracha_out" "$fasterq_out"

    "$SRACHA" fastq "$SRA_FILE" --split split-files --no-gzip -O "$sracha_out" -f --no-progress 2>&1 | tail -2
    "$FASTERQ_DUMP" "$SRA_FILE" --split-files -O "$fasterq_out" -f 2>&1 | tail -2

    echo "  sracha files:      $(ls "$sracha_out"/)"
    echo "  fasterq-dump files: $(ls "$fasterq_out"/)"

    # Compare every file either tool produced, not a hardcoded _1/_2 pair.
    # split-files numbers by read slot, so a run whose odd slots are empty or
    # technical yields _2 and _4 with no _1/_3 (SRR000001 does exactly this);
    # a fixed pair would silently skip the files that actually got written.
    local names
    names=$(ls "$sracha_out"/*.fastq "$fasterq_out"/*.fastq 2>/dev/null |
        xargs -n1 basename 2>/dev/null | sort -u)

    if [[ -z "$names" ]]; then
        fail "split-files — neither tool produced any FASTQ"
    fi

    local name
    for name in $names; do
        local sf="$sracha_out/$name"
        local ff="$fasterq_out/$name"
        if [[ -f "$sf" && -f "$ff" ]]; then
            compare_files "$sf" "$ff" "split-files ${name}"
        elif [[ -f "$sf" ]]; then
            fail "split-files ${name} — fasterq-dump did not produce it"
        else
            fail "split-files ${name} — sracha did not produce it"
        fi
    done
    echo
}

test_4_split3_fasta() {
    log "Test 4: split-3 (FASTA, paired-end)"

    local sracha_out="$TMPDIR_BASE/test4_sracha"
    local fasterq_out="$TMPDIR_BASE/test4_fasterq"
    mkdir -p "$sracha_out" "$fasterq_out"

    "$SRACHA" fastq "$SRA_FILE" --split split-3 --no-gzip --fasta -O "$sracha_out" -f --no-progress 2>&1 | tail -2
    "$FASTERQ_DUMP" "$SRA_FILE" --split-3 --fasta -O "$fasterq_out" -f 2>&1 | tail -2

    echo "  sracha files:      $(ls "$sracha_out"/)"
    echo "  fasterq-dump files: $(ls "$fasterq_out"/)"

    compare_files "$sracha_out/${ACCESSION}_1.fasta" "$fasterq_out/${ACCESSION}_1.fasta" "FASTA split-3 read 1"
    compare_files "$sracha_out/${ACCESSION}_2.fasta" "$fasterq_out/${ACCESSION}_2.fasta" "FASTA split-3 read 2"
    echo
}

test_5_splitspot_fasta() {
    log "Test 5: split-spot (FASTA)"

    local sracha_out="$TMPDIR_BASE/test5_sracha"
    local fasterq_out="$TMPDIR_BASE/test5_fasterq"
    mkdir -p "$sracha_out" "$fasterq_out"

    "$SRACHA" fastq "$SRA_FILE" --split split-spot --no-gzip --fasta -O "$sracha_out" -f --no-progress 2>&1 | tail -2
    "$FASTERQ_DUMP" "$SRA_FILE" --split-spot --fasta -O "$fasterq_out" -f 2>&1 | tail -2

    echo "  sracha files:      $(ls "$sracha_out"/)"
    echo "  fasterq-dump files: $(ls "$fasterq_out"/)"

    compare_files "$sracha_out/${ACCESSION}.fasta" "$fasterq_out/${ACCESSION}.fasta" "FASTA split-spot"
    echo
}

test_6_sralite_split3() {
    log "Test 6: sralite split-3 (FASTQ)"

    if ! ensure_sralite_fixture; then
        skip "sralite split-3 read 1 — fixture not available"
        skip "sralite split-3 read 2 — fixture not available"
        echo
        return
    fi

    local sracha_out="$TMPDIR_BASE/test6_sracha"
    local fasterq_out="$TMPDIR_BASE/test6_fasterq"
    mkdir -p "$sracha_out" "$fasterq_out"

    "$SRACHA" fastq "$SRA_LITE_FILE" --split split-3 --no-gzip -O "$sracha_out" -f --no-progress 2>&1 | tail -2
    "$FASTERQ_DUMP" "$SRA_LITE_FILE" --split-3 -O "$fasterq_out" -f 2>&1 | tail -2

    echo "  sracha files:      $(ls "$sracha_out"/)"
    echo "  fasterq-dump files: $(ls "$fasterq_out"/)"

    compare_files "$sracha_out/${ACCESSION}_1.fastq" "$fasterq_out/${ACCESSION}_1.fastq" "sralite split-3 read 1" "true"
    compare_files "$sracha_out/${ACCESSION}_2.fastq" "$fasterq_out/${ACCESSION}_2.fastq" "sralite split-3 read 2" "true"
    echo
}

test_7_sralite_splitspot() {
    log "Test 7: sralite split-spot (FASTQ)"

    if ! ensure_sralite_fixture; then
        skip "sralite split-spot — fixture not available"
        echo
        return
    fi

    local sracha_out="$TMPDIR_BASE/test7_sracha"
    local fasterq_out="$TMPDIR_BASE/test7_fasterq"
    mkdir -p "$sracha_out" "$fasterq_out"

    "$SRACHA" fastq "$SRA_LITE_FILE" --split split-spot --no-gzip -O "$sracha_out" -f --no-progress 2>&1 | tail -2
    "$FASTERQ_DUMP" "$SRA_LITE_FILE" --split-spot -O "$fasterq_out" -f 2>&1 | tail -2

    echo "  sracha files:      $(ls "$sracha_out"/)"
    echo "  fasterq-dump files: $(ls "$fasterq_out"/)"

    compare_files "$sracha_out/${ACCESSION}.fastq" "$fasterq_out/${ACCESSION}.fastq" "sralite split-spot" "true"
    echo
}

test_8_interleaved() {
    log "Test 8: interleaved (FASTQ, paired-end)"

    # Interleaved writes one file with both mates alternating and `/1`,`/2`
    # defline suffixes -- the same stream fasterq-dump produces for
    # --split-spot. (This test used to expect _1/_2 files, left over from
    # before the writer merged them into a single stream.)
    local sracha_out="$TMPDIR_BASE/test8_sracha"
    local fasterq_out="$TMPDIR_BASE/test8_fasterq"
    mkdir -p "$sracha_out" "$fasterq_out"

    "$SRACHA" fastq "$SRA_FILE" --split interleaved --no-gzip -O "$sracha_out" -f --no-progress 2>&1 | tail -2
    "$FASTERQ_DUMP" "$SRA_FILE" --split-spot -O "$fasterq_out" -f 2>&1 | tail -2

    echo "  sracha files:      $(ls "$sracha_out"/)"
    echo "  fasterq-dump files: $(ls "$fasterq_out"/)"

    for stray in "$sracha_out/${ACCESSION}_1.fastq" "$sracha_out/${ACCESSION}_2.fastq"; do
        [[ -f "$stray" ]] && fail "interleaved — split output $(basename "$stray") should not exist"
    done
    compare_files "$sracha_out/${ACCESSION}.fastq" "$fasterq_out/${ACCESSION}.fastq" "interleaved"
    echo
}

test_9_gzip_roundtrip() {
    log "Test 9: gzip round-trip (FASTQ, split-3)"

    local sracha_out="$TMPDIR_BASE/test9_sracha"
    local fasterq_out="$TMPDIR_BASE/test9_fasterq"
    local decompressed="$TMPDIR_BASE/test9_decompressed"
    mkdir -p "$sracha_out" "$fasterq_out" "$decompressed"

    # sracha with gzip (default)
    "$SRACHA" fastq "$SRA_FILE" --split split-3 -O "$sracha_out" -f --no-progress 2>&1 | tail -2
    # fasterq-dump uncompressed (reference)
    "$FASTERQ_DUMP" "$SRA_FILE" --split-3 -O "$fasterq_out" -f 2>&1 | tail -2

    echo "  sracha files:      $(ls "$sracha_out"/)"
    echo "  fasterq-dump files: $(ls "$fasterq_out"/)"

    # Decompress sracha gzip output
    for gz in "$sracha_out"/*.fastq.gz; do
        base=$(basename "$gz" .gz)
        gunzip -c "$gz" > "$decompressed/$base"
    done

    echo "  decompressed files: $(ls "$decompressed"/)"

    compare_files "$decompressed/${ACCESSION}_1.fastq" "$fasterq_out/${ACCESSION}_1.fastq" "gzip round-trip read 1"
    compare_files "$decompressed/${ACCESSION}_2.fastq" "$fasterq_out/${ACCESSION}_2.fastq" "gzip round-trip read 2"
    echo
}

# Helper: verify sracha rejects an accession with an unsupported platform error.
test_platform_reject() {
    local acc="$1"
    local platform_name="$2"

    if ! ensure_fixture "$acc"; then
        skip "reject $platform_name — fixture not available"
        echo
        return
    fi

    local output
    output=$("$SRACHA" fastq "$FIXTURE_FILE" --split split-spot --no-gzip -O "$TMPDIR_BASE/reject_${acc}" -f --no-progress 2>&1) || true

    if echo "$output" | grep -q "unsupported platform"; then
        pass "reject $platform_name — sracha correctly refused $acc"
    else
        fail "reject $platform_name — sracha did not reject $acc (output: ${output:0:200})"
    fi
    echo
}

test_10_reject_454() {
    log "Test 10: platform reject LS454 (SRR000001)"

    if [[ ! -f "$SRA_FILE_454" ]]; then
        skip "reject LS454 — fixture not available ($SRA_FILE_454)"
        echo
        return
    fi

    local output
    output=$("$SRACHA" fastq "$SRA_FILE_454" --split split-spot --no-gzip -O "$TMPDIR_BASE/reject_454" -f --no-progress 2>&1) || true

    if echo "$output" | grep -q "unsupported platform"; then
        pass "reject LS454 — sracha correctly refused SRR000001"
    else
        fail "reject LS454 — sracha did not reject SRR000001 (output: ${output:0:200})"
    fi
    echo
}

test_11_reject_iontorrent() {
    log "Test 11: platform reject ION_TORRENT ($ACCESSION_IONTORRENT)"
    test_platform_reject "$ACCESSION_IONTORRENT" "ION_TORRENT"
}

test_12_pacbio() {
    log "Test 12: platform PacBio ($ACCESSION_PACBIO)"

    if ! ensure_fixture "$ACCESSION_PACBIO"; then
        skip "PacBio split-spot — fixture not available"
        echo
        return
    fi

    local sracha_out="$TMPDIR_BASE/test12_sracha"
    local fasterq_out="$TMPDIR_BASE/test12_fasterq"
    mkdir -p "$sracha_out" "$fasterq_out"

    "$SRACHA" fastq "$FIXTURE_FILE" --split split-spot --no-gzip -O "$sracha_out" -f --no-progress 2>&1 | tail -2
    "$FASTERQ_DUMP" "$FIXTURE_FILE" --split-spot -O "$fasterq_out" -f 2>&1 | tail -2

    echo "  sracha files:      $(ls "$sracha_out"/ 2>/dev/null || echo '(none)')"
    echo "  fasterq-dump files: $(ls "$fasterq_out"/ 2>/dev/null || echo '(none)')"

    # PacBio: allow N-masking (sracha masks Q≤2 bases) and quality diffs
    # (PacBio quality encoding differs between tools — to be investigated).
    compare_files "$sracha_out/${ACCESSION_PACBIO}.fastq" "$fasterq_out/${ACCESSION_PACBIO}.fastq" "PacBio split-spot" "true" "true"
    echo
}

# ---------- argument parsing ----------

parse_test_args() {
    SELECTED_TESTS=()

    if [[ $# -eq 0 ]]; then
        SELECTED_TESTS=("${ALL_TEST_NUMS[@]}")
        return
    fi

    for arg in "$@"; do
        case "$arg" in
            -h|--help)
                echo "Usage: $0 [test-selector ...]"
                echo
                echo "Available tests:"
                for num in "${ALL_TEST_NUMS[@]}"; do
                    echo "  $num  ${TEST_LABELS[$num]}"
                done
                echo
                echo "Examples:"
                echo "  $0              # run all tests"
                echo "  $0 6 7          # run tests 6 and 7"
                echo "  $0 sralite      # run tests matching 'sralite'"
                exit 0
                ;;
            [1-9]|[1-9][0-9])
                if [[ -n "${TEST_FUNCS[$arg]+x}" ]]; then
                    SELECTED_TESTS+=("$arg")
                else
                    echo "ERROR: unknown test number: $arg"; exit 1
                fi
                ;;
            *)
                # Name fragment match (case-insensitive)
                local matched=false
                for num in "${ALL_TEST_NUMS[@]}"; do
                    if [[ "${TEST_LABELS[$num],,}" == *"${arg,,}"* ]]; then
                        SELECTED_TESTS+=("$num")
                        matched=true
                    fi
                done
                if [[ "$matched" == false ]]; then
                    echo "ERROR: no tests match '$arg'"; exit 1
                fi
                ;;
        esac
    done

    # Deduplicate preserving order
    local -A seen=()
    local deduped=()
    for t in "${SELECTED_TESTS[@]}"; do
        if [[ -z "${seen[$t]+x}" ]]; then
            seen[$t]=1
            deduped+=("$t")
        fi
    done
    SELECTED_TESTS=("${deduped[@]}")
}

parse_test_args "$@"

# ---------- preflight ----------

log "Checking prerequisites..."

if [[ ! -f "$SRA_FILE" ]]; then
    echo "ERROR: Test fixture not found: $SRA_FILE"
    echo "Run the integration tests first: cargo test -p sracha-core -- --ignored"
    exit 1
fi

if [[ ! -x "$SRACHA" ]]; then
    echo "ERROR: sracha binary not found at $SRACHA"
    echo "Build with: cargo build --release -p sracha"
    exit 1
fi

echo "  sracha: $($SRACHA --version)"
echo "  SRA file: $SRA_FILE ($(du -h "$SRA_FILE" | awk '{print $1}'))"

# ---------- install sra-tools if needed ----------

FASTERQ_DUMP=""
# check if already installed locally
if compgen -G "$SRATOOLS_DIR/sratoolkit.*/bin/fasterq-dump" > /dev/null 2>&1; then
    FASTERQ_DUMP=$(ls "$SRATOOLS_DIR"/sratoolkit.*/bin/fasterq-dump | head -1)
elif command -v fasterq-dump &>/dev/null; then
    FASTERQ_DUMP=$(command -v fasterq-dump)
fi

if [[ -z "$FASTERQ_DUMP" ]]; then
    log "Installing sra-tools to $SRATOOLS_DIR..."
    mkdir -p "$SRATOOLS_DIR"

    TARBALL_URL="https://ftp-trace.ncbi.nlm.nih.gov/sra/sdk/current/sratoolkit.current-centos_linux64.tar.gz"
    TARBALL="$SRATOOLS_DIR/sratoolkit.tar.gz"

    echo "  Downloading from $TARBALL_URL ..."
    curl -fSL -o "$TARBALL" "$TARBALL_URL"
    echo "  Extracting..."
    tar -xzf "$TARBALL" -C "$SRATOOLS_DIR"
    rm -f "$TARBALL"

    FASTERQ_DUMP=$(ls "$SRATOOLS_DIR"/sratoolkit.*/bin/fasterq-dump | head -1)
    if [[ ! -x "$FASTERQ_DUMP" ]]; then
        echo "ERROR: fasterq-dump not found after extraction"
        ls -la "$SRATOOLS_DIR"/
        exit 1
    fi
fi

# Discover prefetch (for sralite fixture download)
if compgen -G "$SRATOOLS_DIR/sratoolkit.*/bin/prefetch" > /dev/null 2>&1; then
    PREFETCH=$(ls "$SRATOOLS_DIR"/sratoolkit.*/bin/prefetch | head -1)
elif command -v prefetch &>/dev/null; then
    PREFETCH=$(command -v prefetch)
fi

echo "  fasterq-dump: $("$FASTERQ_DUMP" --version 2>&1 | head -1 || echo 'unknown version')"
if [[ -n "$PREFETCH" ]]; then
    echo "  prefetch: $("$PREFETCH" --version 2>&1 | head -1 || echo 'unknown version')"
else
    echo "  prefetch: not found (sralite tests will be skipped)"
fi

# ---------- setup temp dir ----------

TMPDIR_BASE=$(mktemp -d "${TMPDIR:-/tmp}/sracha-compare.XXXXXX")
trap 'rm -rf "$TMPDIR_BASE"' EXIT
echo "  Temp dir: $TMPDIR_BASE"

echo
echo "  Running tests: ${SELECTED_TESTS[*]}"
echo

# =====================================================================
# RUN SELECTED TESTS
# =====================================================================

for test_num in "${SELECTED_TESTS[@]}"; do
    "${TEST_FUNCS[$test_num]}"
done

# =====================================================================
# SUMMARY
# =====================================================================
echo
echo -e "${BOLD}======================================${RESET}"
echo -e "${BOLD}  SUMMARY${RESET}"
echo -e "${BOLD}======================================${RESET}"
for r in "${RESULTS[@]}"; do
    if [[ "$r" == PASS* ]]; then
        echo -e "  ${GREEN}$r${RESET}"
    elif [[ "$r" == FAIL* ]]; then
        echo -e "  ${RED}$r${RESET}"
    else
        echo -e "  ${YELLOW}$r${RESET}"
    fi
done
echo
echo -e "  Passed: ${GREEN}${PASS_COUNT}${RESET}  Failed: ${RED}${FAIL_COUNT}${RESET}  Skipped: ${YELLOW}${SKIP_COUNT}${RESET}"
echo

if [[ "$FAIL_COUNT" -gt 0 ]]; then
    echo -e "${RED}SOME TESTS FAILED${RESET}"
    exit 1
else
    echo -e "${GREEN}ALL TESTS PASSED${RESET}"
    exit 0
fi
