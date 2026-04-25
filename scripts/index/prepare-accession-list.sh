#!/usr/bin/env bash
#
# Stream NCBI's SRA_Accessions.tab and write a filtered accession
# list ready to feed to `sracha-index build|append`.
#
# Usage:
#   prepare-accession-list.sh <output-prefix> [SINCE]
#
# Arguments:
#   <output-prefix>  Path stem; writes <prefix>.txt (accession list)
#                    and <prefix>.tsv (sidecar with md5/spots/bases/published).
#   [SINCE]          Earliest Published date to keep, ISO format
#                    (YYYY-MM-DD). Defaults to 5 years before today.
#                    Lexicographic comparison against the file's full
#                    ISO-8601 timestamp; "-" (missing) rows are
#                    filtered out automatically.
#
# Environment overrides:
#   SRA_ACCESSIONS_URL  Source URL (default: NCBI metadata FTP).
#   CURL_OPTS           Extra args to pass to curl (e.g. --retry 5).
#
# Filter (all four must pass):
#   Type       == RUN
#   Status     == live
#   Visibility == public
#   Loaded     == 1
#   Published  >= SINCE
#
# The sidecar TSV carries fields the catalog can use without an
# extra S3 round-trip per accession (md5, spot/base counts, the
# publication date). The build CLI doesn't read it yet, but it's
# useful for downstream auditing and is cheap to emit alongside
# the accession list.

set -euo pipefail

if [[ $# -lt 1 || "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    sed -n '3,33p' "$0"
    exit 1
fi

OUTPUT="$1"
DEFAULT_SINCE="$(date -u -d '5 years ago' +%Y-%m-%d)"
SINCE="${2:-$DEFAULT_SINCE}"
URL="${SRA_ACCESSIONS_URL:-https://ftp.ncbi.nlm.nih.gov/sra/reports/Metadata/SRA_Accessions.tab}"

# Validate SINCE is parseable; rejects typos like "2020/01/01" before
# we burn 32 GB of bandwidth.
if ! date -u -d "$SINCE" >/dev/null 2>&1; then
    echo "error: SINCE='$SINCE' is not a valid date" >&2
    exit 2
fi

mkdir -p "$(dirname -- "$OUTPUT")"

echo "[$(date -u +%FT%TZ)] source : $URL"
echo "[$(date -u +%FT%TZ)] since  : $SINCE"
echo "[$(date -u +%FT%TZ)] output : $OUTPUT.txt + $OUTPUT.tsv"

# shellcheck disable=SC2086
curl -fsS ${CURL_OPTS:-} "$URL" \
| awk -F'\t' \
    -v since="$SINCE" \
    -v out_txt="$OUTPUT.txt" \
    -v out_tsv="$OUTPUT.tsv" '
    BEGIN {
        # Sidecar header. Column order mirrors what the catalog
        # extractor would otherwise have to chase per-accession.
        print "accession\tmd5\tspots\tbases\tpublished" > out_tsv
        kept = 0
        scanned = 0
    }
    NR == 1 { next }                  # skip header row
    {
        scanned++
        # Field positions are stable in SRA_Accessions.tab (since
        # NCBI added BioSample/BioProject/ReplacedBy in 2014):
        #   1 Accession   3 Status    5 Published   7 Type
        #   9 Visibility  14 Loaded   15 Spots      16 Bases
        #  17 Md5sum
        if ($7 != "RUN") next
        if ($3 != "live") next
        if ($9 != "public") next
        if ($14 != "1") next
        if ($5 < since) next         # also rules out "-" rows
        print $1 > out_txt
        printf "%s\t%s\t%s\t%s\t%s\n", $1, $17, $15, $16, $5 > out_tsv
        kept++
    }
    END {
        printf "scanned %d row(s); kept %d run(s)\n", scanned, kept | "cat 1>&2"
    }
'

echo "[$(date -u +%FT%TZ)] done: $(wc -l < "$OUTPUT.txt") accession(s) in $OUTPUT.txt"
