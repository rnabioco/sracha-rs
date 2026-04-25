#!/usr/bin/env bash
#SBATCH --job-name=sracha-index
#SBATCH --partition=rna
#SBATCH --nodelist=compute18
#SBATCH --cpus-per-task=32
#SBATCH --mem=64G
#SBATCH --time=12:00:00
#SBATCH --output=logs/sracha-index-%j.out
#SBATCH --error=logs/sracha-index-%j.err
#
# SLURM wrapper around build-and-publish.sh. Submit with environment
# overrides for the destination bucket and the accession list, e.g.
#
#   sbatch \
#     --export=ALL,S3_BUCKET=sracha-catalog,ACCESSION_LIST=/path/runs.txt,CATALOG_DIR=/scratch/sracha-catalog \
#     scripts/index/build-and-publish.sbatch
#
# Recurring: install a cron entry on the submit host (NOT inside SLURM)
# that runs `sbatch ...` on the 1st of every month. Keeps SLURM as the
# executor without baking schedule semantics into the cluster.
#
#   # crontab -l
#   0 4 1 * * cd /path/to/sracha-rs && sbatch scripts/index/build-and-publish.sbatch
#
# Each invocation produces a delta shard named YYYY-MM-DD; the catalog
# itself is cumulative (readers union all shards, newest wins on
# collision). Plan a quarterly compaction run that rebuilds the base
# shard from scratch — we'll add a separate `compact.sbatch` once the
# delta count starts hurting first-byte latency.

set -euo pipefail

mkdir -p logs

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
exec "$SCRIPT_DIR/build-and-publish.sh"
