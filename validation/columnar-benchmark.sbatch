#!/usr/bin/env bash
#SBATCH --job-name=sracha-columnar
#SBATCH --output=validation/columnar-benchmark_%j.log
#SBATCH --cpus-per-task=32
#SBATCH --mem=64G
#SBATCH --time=6:00:00

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "=== Slurm job $SLURM_JOB_ID on $(hostname) ==="
echo "CPUs: ${SLURM_CPUS_PER_TASK:-?}, Memory: ${SLURM_MEM_PER_NODE:-?} MB"
echo "Started: $(date)"
echo

# libclang is needed to build vortex's custom-labels -> bindgen step if the
# binary was not pre-built. Set it unconditionally; harmless if already built.
if [[ -d "$ROOT_DIR/.pixi/envs/default/lib" ]]; then
    export LIBCLANG_PATH="$ROOT_DIR/.pixi/envs/default/lib"
fi

# Build once up front so the benchmark only measures runtime, not compile time.
if [[ ! -x target/release/sracha ]] \
   || [[ ! -x target/release/examples/read_parquet ]] \
   || [[ ! -x target/release/examples/read_vortex ]]; then
    echo "=== Building release binaries + examples ==="
    CARGO_BUILD_JOBS="${SLURM_CPUS_PER_TASK:-8}" \
        cargo build --profile release -p sracha
    CARGO_BUILD_JOBS="${SLURM_CPUS_PER_TASK:-8}" \
        cargo build --profile release -p sracha-core --examples
fi

# Forward any extra args to the benchmark script.
# Default: include zstd-22 (slow), 3 hyperfine runs each.
bash validation/columnar-benchmark.sh "$@"

echo
echo "Finished: $(date)"
