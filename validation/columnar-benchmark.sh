#!/usr/bin/env bash
#
# Columnar-format benchmark for Issue #9: compare file sizes and encode/decode
# throughput across VDB (.sra), Parquet, and Vortex on the same SRA fixture.
#
# Usage:
#   bash validation/columnar-benchmark.sh [--sra PATH] [--runs N] [--skip-zstd22]
#
# Defaults:
#   - SRA fixture: crates/sracha-core/tests/fixtures/SRR2584863.sra
#   - Hyperfine runs: 3 per config
#   - zstd-22 included (slow — single-threaded parquet compression)
#
# Requires:
#   - hyperfine
#   - sracha release binary built with vortex support
#   - read_parquet / read_vortex examples built in release mode

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

SRACHA="$ROOT_DIR/target/release/sracha"
READ_PARQUET="$ROOT_DIR/target/release/examples/read_parquet"
READ_VORTEX="$ROOT_DIR/target/release/examples/read_vortex"

SRA_INPUT="$ROOT_DIR/crates/sracha-core/tests/fixtures/SRR2584863.sra"
RUNS=3
SKIP_ZSTD22=false

# ---------- arg parse ----------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --sra) SRA_INPUT="$2"; shift 2 ;;
        --runs) RUNS="$2"; shift 2 ;;
        --skip-zstd22) SKIP_ZSTD22=true; shift ;;
        -h|--help)
            sed -n '1,20p' "$0" >&2
            exit 0
            ;;
        *) echo "unknown flag: $1" >&2; exit 2 ;;
    esac
done

# ---------- prerequisites ----------
command -v hyperfine >/dev/null || { echo "ERROR: hyperfine not in PATH" >&2; exit 1; }
[[ -x "$SRACHA" ]]       || { echo "ERROR: not built: $SRACHA" >&2; exit 1; }
[[ -x "$READ_PARQUET" ]] || { echo "ERROR: not built: $READ_PARQUET (run: cargo build --release --examples -p sracha-core)" >&2; exit 1; }
[[ -x "$READ_VORTEX" ]]  || { echo "ERROR: not built: $READ_VORTEX (run: cargo build --release --examples -p sracha-core)" >&2; exit 1; }
[[ -f "$SRA_INPUT" ]]    || { echo "ERROR: SRA fixture not found: $SRA_INPUT" >&2; exit 1; }

OUTDIR=$(mktemp -d "${TMPDIR:-/tmp}/sracha-columnar.XXXXXX")
trap 'rm -rf "$OUTDIR"' EXIT

BOLD='\033[1m'; RESET='\033[0m'
log() { echo -e "\n${BOLD}==> $1${RESET}"; }

INPUT_BYTES=$(stat -c '%s' "$SRA_INPUT")
STEM=$(basename "$SRA_INPUT" .sra)
FIXTURES_DIR="$OUTDIR/fixtures"
mkdir -p "$FIXTURES_DIR"

log "Configuration"
echo "  sracha:       $("$SRACHA" --version)"
echo "  input:        $SRA_INPUT ($(numfmt --to=iec-i --suffix=B --format='%.1f' $INPUT_BYTES))"
echo "  runs each:    $RUNS"
echo "  tempdir:      $OUTDIR"
echo "  zstd-22:      $([ "$SKIP_ZSTD22" = true ] && echo skipped || echo included)"

# ---------- config matrix ----------
# label :: sracha convert args (pack, format-specific)
CONFIGS=(
    "parquet.ascii.none        --format parquet --pack-dna ascii   --compression none"
    "parquet.ascii.zstd3       --format parquet --pack-dna ascii   --compression zstd --zstd-level 3"
    "parquet.two-na.none       --format parquet --pack-dna two-na  --compression none"
    "parquet.two-na.zstd3      --format parquet --pack-dna two-na  --compression zstd --zstd-level 3"
    "parquet.four-na.zstd3     --format parquet --pack-dna four-na --compression zstd --zstd-level 3"
    "vortex.ascii              --format vortex  --pack-dna ascii"
    "vortex.two-na             --format vortex  --pack-dna two-na"
    "vortex.four-na            --format vortex  --pack-dna four-na"
)

if [[ "$SKIP_ZSTD22" == false ]]; then
    CONFIGS+=(
        "parquet.ascii.zstd22      --format parquet --pack-dna ascii   --compression zstd --zstd-level 22"
        "parquet.two-na.zstd22     --format parquet --pack-dna two-na  --compression zstd --zstd-level 22"
    )
fi

# ---------- encode phase: measure wall time + output size ----------
log "Encoding (SRA -> target format)"
ENCODE_TABLE="$OUTDIR/encode.tsv"
printf "config\tsize_bytes\tratio_vs_sra\tencode_mean_s\tencode_stddev_s\n" > "$ENCODE_TABLE"

for entry in "${CONFIGS[@]}"; do
    label="${entry%% *}"
    args="${entry#* }"
    ext="${label%%.*}"   # parquet or vortex
    outfile="$FIXTURES_DIR/$STEM.$label.$ext"

    rm -f "$outfile"
    # hyperfine produces JSON with mean/stddev; we grep it out
    json="$OUTDIR/encode-$label.json"
    hyperfine \
        --warmup 0 \
        --min-runs "$RUNS" \
        --max-runs "$RUNS" \
        --prepare "rm -f $outfile" \
        --export-json "$json" \
        -n "$label" \
        "$SRACHA convert $SRA_INPUT -O $FIXTURES_DIR $args -f --quiet && mv $FIXTURES_DIR/$STEM.$ext $outfile" \
        >/dev/null 2>&1 || { echo "  $label: FAILED"; continue; }

    mean=$(python3 -c "import json; print(f\"{json.load(open('$json'))['results'][0]['mean']:.3f}\")")
    sd=$(python3 -c "import json; print(f\"{json.load(open('$json'))['results'][0]['stddev']:.3f}\")")
    size=$(stat -c '%s' "$outfile")
    ratio=$(python3 -c "print(f\"{$size / $INPUT_BYTES:.3f}\")")
    printf "%s\t%d\t%s\t%s\t%s\n" "$label" "$size" "$ratio" "$mean" "$sd" >> "$ENCODE_TABLE"
    printf "  %-24s  %10s   ratio %s   encode %s±%s s\n" \
        "$label" "$(numfmt --to=iec-i --suffix=B --format='%.1f' $size)" "$ratio" "$mean" "$sd"
done

# ---------- decode phase: full-scan wall time ----------
log "Decoding (full scan -> /dev/null)"
DECODE_TABLE="$OUTDIR/decode.tsv"
printf "config\tdecode_mean_s\tdecode_stddev_s\n" > "$DECODE_TABLE"

# VDB baseline: decode via sracha fastq --stdout
SRA_DECODE_JSON="$OUTDIR/decode-sra.json"
hyperfine \
    --warmup 0 \
    --min-runs "$RUNS" \
    --max-runs "$RUNS" \
    --export-json "$SRA_DECODE_JSON" \
    -n "sra (baseline)" \
    "$SRACHA fastq $SRA_INPUT --stdout --split interleaved --no-progress --quiet > /dev/null" \
    >/dev/null 2>&1 || echo "  sra decode: FAILED"
sra_mean=$(python3 -c "import json; print(f\"{json.load(open('$SRA_DECODE_JSON'))['results'][0]['mean']:.3f}\")")
sra_sd=$(python3 -c "import json; print(f\"{json.load(open('$SRA_DECODE_JSON'))['results'][0]['stddev']:.3f}\")")
printf "sra\t%s\t%s\n" "$sra_mean" "$sra_sd" >> "$DECODE_TABLE"
printf "  %-24s  decode %s±%s s\n" "sra (baseline)" "$sra_mean" "$sra_sd"

# Per-config: pick reader by extension
for entry in "${CONFIGS[@]}"; do
    label="${entry%% *}"
    ext="${label%%.*}"
    outfile="$FIXTURES_DIR/$STEM.$label.$ext"
    [[ -f "$outfile" ]] || continue

    case "$ext" in
        parquet) reader="$READ_PARQUET" ;;
        vortex)  reader="$READ_VORTEX" ;;
        *)       continue ;;
    esac

    json="$OUTDIR/decode-$label.json"
    hyperfine \
        --warmup 0 \
        --min-runs "$RUNS" \
        --max-runs "$RUNS" \
        --export-json "$json" \
        -n "$label" \
        "$reader $outfile" \
        >/dev/null 2>&1 || { echo "  $label: FAILED"; continue; }
    mean=$(python3 -c "import json; print(f\"{json.load(open('$json'))['results'][0]['mean']:.3f}\")")
    sd=$(python3 -c "import json; print(f\"{json.load(open('$json'))['results'][0]['stddev']:.3f}\")")
    printf "%s\t%s\t%s\n" "$label" "$mean" "$sd" >> "$DECODE_TABLE"
    printf "  %-24s  decode %s±%s s\n" "$label" "$mean" "$sd"
done

# ---------- final combined markdown table ----------
log "Final table"
OUT_MD="${RESULTS_MD:-$ROOT_DIR/validation/columnar-benchmark.md}"
{
    echo "# Columnar benchmark — $STEM"
    echo
    echo "Fixture: \`$SRA_INPUT\` ($(numfmt --to=iec-i --suffix=B --format='%.1f' $INPUT_BYTES))"
    echo "Machine: $(hostname), $(nproc) CPUs"
    echo "Date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo
    echo "| Config | Size | Ratio vs .sra | Encode (s) | Decode (s) |"
    echo "|---|---|---|---|---|"
    echo "| sra (baseline) | $(numfmt --to=iec-i --suffix=B --format='%.1f' $INPUT_BYTES) | 1.000 | — | $sra_mean±$sra_sd |"
    tail -n +2 "$ENCODE_TABLE" | while IFS=$'\t' read -r label size ratio emean esd; do
        dec_row=$(grep -P "^$label\t" "$DECODE_TABLE" || echo "")
        if [[ -n "$dec_row" ]]; then
            dmean=$(echo "$dec_row" | cut -f2)
            dsd=$(echo "$dec_row" | cut -f3)
            dec_cell="${dmean}±${dsd}"
        else
            dec_cell="—"
        fi
        size_h=$(numfmt --to=iec-i --suffix=B --format='%.1f' "$size")
        printf "| %s | %s | %s | %s±%s | %s |\n" \
            "$label" "$size_h" "$ratio" "$emean" "$esd" "$dec_cell"
    done
} | tee "$OUT_MD"

echo
echo "Wrote: $OUT_MD"
echo "Raw TSVs kept until script exit: $ENCODE_TABLE $DECODE_TABLE"
