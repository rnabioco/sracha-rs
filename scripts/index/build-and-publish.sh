#!/usr/bin/env bash
#
# Build a sracha-index catalog shard from an accession list and publish
# it to the hosted S3 prefix. Designed to be invoked by
# `build-and-publish.sbatch` on a SLURM compute node.
#
# Required env vars (or override on the command line):
#   ACCESSION_LIST   — path to a newline-delimited accession list. If
#                      unset, the script generates one from
#                      SRA_Accessions.tab via prepare-accession-list.sh
#                      with PUBLISHED_SINCE as the cutoff (default: 5
#                      years ago).
#   PUBLISHED_SINCE  — optional ISO date (YYYY-MM-DD). When generating
#                      the accession list, only RUNs published on/after
#                      this date are kept. Ignored if ACCESSION_LIST is
#                      already set. Default: 5 years before today.
#   CATALOG_DIR      — directory the catalog tree is built in. Defaults
#                      to a per-job tmpdir under $TMPDIR.
#   S3_BUCKET        — destination bucket name (no scheme, no trailing
#                      slash). Set this; there is no safe default.
#   S3_PREFIX        — key prefix inside the bucket. Default: "v1".
#   SHARD_NAME       — shard name. Default: today's UTC date.
#   WORKERS          — parallel extractor workers. Default: 32.
#   SRACHA_INDEX_BIN — path to the sracha-index binary. Default: looks
#                      up `sracha-index` on PATH (CI release artifact).
#
# Behavior:
#   1. If CATALOG_DIR doesn't exist or has no manifest.json, do an
#      initial `build`. Otherwise do an `append` (delta shard).
#   2. After the local build succeeds, sync the entire catalog tree
#      up to s3://$S3_BUCKET/$S3_PREFIX/ (manifest last, so any
#      reader hitting the manifest will find every shard it lists).
#
# Exits non-zero on any sub-step failure. The S3 sync is the only
# step that mutates shared state; if anything before it fails, we
# never touch the bucket.

set -euo pipefail

S3_BUCKET="${S3_BUCKET:?set S3_BUCKET=name-of-bucket}"
S3_PREFIX="${S3_PREFIX:-v1}"
WORKERS="${WORKERS:-32}"
SHARD_NAME="${SHARD_NAME:-$(date -u +%Y-%m-%d)}"
SRACHA_INDEX_BIN="${SRACHA_INDEX_BIN:-sracha-index}"

if [[ -z "${CATALOG_DIR:-}" ]]; then
    CATALOG_DIR="${TMPDIR:-/tmp}/sracha-index-build-$$"
    mkdir -p "$CATALOG_DIR"
fi

# Generate accession list from SRA_Accessions.tab when none was
# supplied. Filter is applied by prepare-accession-list.sh; see that
# script for the exact criteria.
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
if [[ -z "${ACCESSION_LIST:-}" ]]; then
    PREP_PREFIX="$CATALOG_DIR/accessions-$(date -u +%Y%m%d)"
    PUBLISHED_SINCE="${PUBLISHED_SINCE:-$(date -u -d '5 years ago' +%Y-%m-%d)}"
    echo "[$(date -u +%FT%TZ)] no ACCESSION_LIST set; preparing one (since=$PUBLISHED_SINCE)"
    "$SCRIPT_DIR/prepare-accession-list.sh" "$PREP_PREFIX" "$PUBLISHED_SINCE"
    ACCESSION_LIST="$PREP_PREFIX.txt"
fi

echo "[$(date -u +%FT%TZ)] catalog dir : $CATALOG_DIR"
echo "[$(date -u +%FT%TZ)] accessions  : $ACCESSION_LIST"
echo "[$(date -u +%FT%TZ)] shard name  : $SHARD_NAME"
echo "[$(date -u +%FT%TZ)] workers     : $WORKERS"
echo "[$(date -u +%FT%TZ)] s3 target   : s3://$S3_BUCKET/$S3_PREFIX/"

# ---------------------------------------------------------------- build
if [[ -f "$CATALOG_DIR/manifest.json" ]]; then
    echo "[$(date -u +%FT%TZ)] manifest exists; running APPEND"
    "$SRACHA_INDEX_BIN" -v append \
        --catalog "$CATALOG_DIR" \
        --accession-list "$ACCESSION_LIST" \
        --shard-name "$SHARD_NAME" \
        -j "$WORKERS"
else
    echo "[$(date -u +%FT%TZ)] no manifest; running fresh BUILD"
    "$SRACHA_INDEX_BIN" -v build \
        --output "$CATALOG_DIR" \
        --accession-list "$ACCESSION_LIST" \
        --shard-name "${SHARD_NAME:-base}" \
        -j "$WORKERS"
fi

# -------------------------------------------------------------- publish
# Push shard files first, manifest last. `aws s3 sync` skips files
# whose size + mtime match, so the second-pass manifest copy is
# effectively the only "atomic flip" point readers see.
echo "[$(date -u +%FT%TZ)] syncing shards to s3"
aws s3 sync \
    --exclude '*' \
    --include 'shards/*' \
    --cache-control 'public, max-age=2592000, immutable' \
    "$CATALOG_DIR/" \
    "s3://$S3_BUCKET/$S3_PREFIX/"

echo "[$(date -u +%FT%TZ)] uploading manifest"
aws s3 cp \
    --cache-control 'public, max-age=300' \
    "$CATALOG_DIR/manifest.json" \
    "s3://$S3_BUCKET/$S3_PREFIX/manifest.json"

echo "[$(date -u +%FT%TZ)] done."
