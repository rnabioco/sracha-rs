#!/usr/bin/env bash
#
# Sample random SRA accessions from ENA's portal API.
#
# Usage:
#   bash validation/sample_accessions.sh [-n N] [-s SEED] [-p PLATFORM] [--min-bases N] [--max-bases N]
#
# Defaults: N=30, SEED=$RANDOM, PLATFORM=ILLUMINA, bases 100M-3G
#
# Prints one run_accession per line to stdout.
# Sends TSV with (accession, platform, strategy, layout, bases) to stderr
# so the caller can log the metadata alongside the accession list.

set -uo pipefail

N=30
SEED=""
PLATFORM="ILLUMINA"
MIN_BASES=100000000
MAX_BASES=3000000000
POOL=5000
LIBRARY_STRATEGY=""
# cSRA probe mode: tightens the ENA query to favour aligned / BAM-loaded
# submissions (library_strategy=WGS). The final cSRA filter still happens
# at `sracha fastq` time via CsraCursor's archive-shape check, but this
# raises the hit rate dramatically over sampling all Illumina runs.
ALIGNED=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        -n)                   N="$2"; shift 2 ;;
        -s|--seed)            SEED="$2"; shift 2 ;;
        -p|--platform)        PLATFORM="$2"; shift 2 ;;
        --min-bases)          MIN_BASES="$2"; shift 2 ;;
        --max-bases)          MAX_BASES="$2"; shift 2 ;;
        --pool)               POOL="$2"; shift 2 ;;
        --library-strategy)   LIBRARY_STRATEGY="$2"; shift 2 ;;
        --aligned)            ALIGNED=1; shift ;;
        -h|--help)
            sed -n '3,15p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

if [[ -z "$SEED" ]]; then
    SEED="$RANDOM$RANDOM"
fi

# --aligned is a shortcut for library_strategy=WGS, which is BAM-submitted
# in practice and therefore almost always cSRA. Override with an explicit
# --library-strategy if needed.
if [[ "$ALIGNED" == "1" && -z "$LIBRARY_STRATEGY" ]]; then
    LIBRARY_STRATEGY="WGS"
fi

if [[ "$PLATFORM" == "all" ]]; then
    QUERY="base_count>=${MIN_BASES} AND base_count<=${MAX_BASES}"
else
    QUERY="instrument_platform=${PLATFORM} AND base_count>=${MIN_BASES} AND base_count<=${MAX_BASES}"
fi
if [[ -n "$LIBRARY_STRATEGY" ]]; then
    QUERY="${QUERY} AND library_strategy=${LIBRARY_STRATEGY}"
fi

echo "# sampling N=${N} from ENA pool=${POOL} query=\"${QUERY}\" seed=${SEED}" >&2

TMP=$(mktemp)
trap 'rm -f "$TMP"' EXIT

# ENA portal API. POST with --data-urlencode to dodge shell-escape headaches.
# Sort by run_accession to make the pool deterministic; seed controls shuffle.
HTTP_STATUS=$(curl -sS -o "$TMP" -w '%{http_code}' \
    -X POST "https://www.ebi.ac.uk/ena/portal/api/search" \
    --data-urlencode "result=read_run" \
    --data-urlencode "query=${QUERY}" \
    --data-urlencode "fields=run_accession,instrument_platform,library_strategy,library_layout,base_count" \
    --data-urlencode "limit=${POOL}" \
    --data-urlencode "format=tsv")

if [[ "$HTTP_STATUS" != "200" ]]; then
    echo "ERROR: ENA query failed with HTTP $HTTP_STATUS" >&2
    head -5 "$TMP" >&2
    exit 1
fi

LINES=$(wc -l < "$TMP")
if [[ "$LINES" -lt 2 ]]; then
    echo "ERROR: ENA returned empty result set" >&2
    exit 1
fi
echo "# pool=$((LINES - 1)) records returned" >&2

# Random source from seed. /dev/urandom would work but isn't reproducible.
# openssl enc with a fixed key gives us a deterministic byte stream.
SEED_SOURCE=$(mktemp)
trap 'rm -f "$TMP" "$SEED_SOURCE"' EXIT
openssl enc -aes-256-ctr -pass "pass:${SEED}" -nosalt < /dev/zero 2>/dev/null \
    | head -c 1048576 > "$SEED_SOURCE"

# Skip header, shuffle with seeded random source, take N.
# Emit metadata (all columns) to stderr, accession only to stdout.
tail -n +2 "$TMP" \
    | shuf -n "$N" --random-source="$SEED_SOURCE" \
    | awk -F'\t' 'BEGIN{OFS="\t"} {print $1; print "# "$0 > "/dev/stderr"}'
