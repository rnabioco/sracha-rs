#!/usr/bin/env bash
#
# Select accessions from the distilled corpus (validation/corpus.tsv).
#
# The corpus is one row per archive worth re-checking after a decoder change:
# every entry either broke something once or is the only cover for a platform,
# layout, read length, or archive shape. Prefer it over a fresh random sample
# when you want a regression check rather than a survey.
#
# Usage:
#   bash validation/corpus.sh                    # core tier, one accession per line
#   bash validation/corpus.sh --tier all         # include the large extended tier
#   bash validation/corpus.sh --shape csra       # only reference-compressed archives
#   bash validation/corpus.sh --platform PACBIO_SMRT
#   bash validation/corpus.sh --why              # accession + what it covers
#   bash validation/corpus.sh --summary          # coverage counts and download size
#
# Typical A/B run:
#   bash validation/corpus.sh > /tmp/core.txt
#   bash validation/ab_corpus.sh --sbatch \
#       --baseline OLD/sracha --candidate NEW/sracha -a /tmp/core.txt

set -uo pipefail

TSV="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/corpus.tsv"
[[ -f "$TSV" ]] || { echo "missing $TSV" >&2; exit 1; }

TIER="core"
SHAPE=""
PLATFORM=""
MODE="list"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --tier) TIER="$2"; shift 2 ;;
        --shape) SHAPE="$2"; shift 2 ;;
        --platform) PLATFORM="$2"; shift 2 ;;
        --why) MODE="why"; shift ;;
        --summary) MODE="summary"; shift ;;
        -h|--help) sed -n '3,25p' "$0"; exit 0 ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

awk -F'\t' -v tier="$TIER" -v shape="$SHAPE" -v plat="$PLATFORM" -v mode="$MODE" '
    /^#/ || $1 == "accession" { next }
    tier != "all" && $2 != tier { next }
    shape != "" && $3 != shape { next }
    plat  != "" && $4 != plat  { next }
    {
        rows++
        n_shape[$3]++; n_plat[$4]++; n_layout[$6]++
        bytes += $10
        if (mode == "list") print $1
        else if (mode == "why") printf "%-13s %s\n", $1, $11
    }
    END {
        if (mode != "summary") exit
        printf "accessions   : %d\n", rows
        printf "download     : %.1f GB\n", bytes / 1e9
        printf "shapes       :"; for (k in n_shape)  printf " %s=%d", k, n_shape[k];  printf "\n"
        printf "platforms    :"; for (k in n_plat)   printf " %s=%d", k, n_plat[k];   printf "\n"
        printf "layouts      :"; for (k in n_layout) printf " %s=%d", k, n_layout[k]; printf "\n"
    }
' "$TSV"
