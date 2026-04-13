#!/bin/bash
#SBATCH --job-name=sracha-big
#SBATCH --cpus-per-task=24
#SBATCH --mem=16G
#SBATCH --time=2:00:00
#SBATCH --output=sracha-big-%j.out

set -euo pipefail

echo "Starting sracha get SRR17778108 at $(date)"
echo "Cores: $SLURM_CPUS_PER_TASK"

time ./target/release/sracha get SRR17778108 \
    -O /tmp/sracha-big-${SLURM_JOB_ID} \
    --force --no-gzip

echo "Done at $(date)"
ls -lh /tmp/sracha-big-${SLURM_JOB_ID}/
head -4 /tmp/sracha-big-${SLURM_JOB_ID}/*_1.fastq 2>/dev/null || head -4 /tmp/sracha-big-${SLURM_JOB_ID}/*_0.fastq 2>/dev/null
