#!/bin/bash
#SBATCH --job-name=sracha-big-split
#SBATCH --cpus-per-task=24
#SBATCH --mem=16G
#SBATCH --time=2:00:00
#SBATCH --output=sracha-big-split-%j.out

set -euo pipefail

ACC=SRR17778123
SRADIR=/tmp/sracha-sra
OUTDIR=/tmp/sracha-big-split-${SLURM_JOB_ID}
SRA_FILE=${SRADIR}/${ACC}.sra
SRACHA=/beevol/home/jhessel/devel/rnabioco/sracha-rs/target/release/sracha

echo "Starting sracha split test for ${ACC} at $(date)"
echo "Cores: $SLURM_CPUS_PER_TASK"

# Phase 1: Fetch the SRA-lite file once (skip if already present).
if [ -f "${SRA_FILE}" ]; then
    echo "SRA file already cached: ${SRA_FILE} ($(du -h ${SRA_FILE} | cut -f1))"
else
    echo "Fetching ${ACC} (sralite) to ${SRADIR}..."
    mkdir -p ${SRADIR}
    time ${SRACHA} fetch ${ACC} -O ${SRADIR} --format sralite
fi

# Phase 2: Decode to FASTQ with split-3 (default).
echo ""
echo "Decoding ${SRA_FILE} -> FASTQ (split-3) at $(date)"
time ${SRACHA} fastq ${SRA_FILE} \
    -O ${OUTDIR} \
    --force -t ${SLURM_CPUS_PER_TASK} --gzip-level 1

echo ""
echo "Done at $(date)"
ls -lh ${OUTDIR}/

# Report line counts and read counts for each file.
echo ""
echo "=== Line / read counts ==="
for f in ${OUTDIR}/*.fastq.gz; do
    lines=$(zcat "$f" | wc -l)
    reads=$((lines / 4))
    echo "$(basename $f): ${lines} lines, ${reads} reads"
done
