# Columnar benchmark — VDB (`.sra`) vs Parquet vs Vortex

Encoding and decoding numbers across all three columnar formats for the two
committed SRA fixtures. Produced by `validation/columnar-benchmark.sh`
(hyperfine-based, 3 runs per config) against the release build with both
features enabled, on a 96-core compute node.

Date: 2026-04-17T18:00:00Z. `--skip-zstd22` (zstd-22 on this workload runs
~20 min per config on a single core — not a useful datapoint for per-file
encode latency).

## SRR28588231 — Illumina 2×301 bp, 66K spots (22.4 MiB `.sra`)

| Config               |      Size | Ratio vs .sra | Encode (s)      | Decode (s)      |
|----------------------|----------:|--------------:|----------------:|----------------:|
| sra (baseline)       |  22.4 MiB |      **1.00** | —               | 0.369 ± 0.187   |
| parquet.ascii.none   |  76.9 MiB |          3.44 | 1.518 ± 1.316   | 0.483 ± 0.765   |
| parquet.ascii.zstd3  |  15.5 MiB |          0.69 | 1.772 ± 0.872   | 0.212 ± 0.107   |
| parquet.two-na.none  |  48.4 MiB |          2.17 | 0.768 ± 0.152   | 0.157 ± 0.235   |
| parquet.two-na.zstd3 |  **14.9 MiB** | **0.67** | 1.185 ± 0.112   | 0.180 ± 0.121   |
| parquet.four-na.zstd3|  15.1 MiB |          0.67 | 1.270 ± 0.247   | 0.197 ± 0.111   |
| vortex.ascii         |  15.3 MiB |          0.68 | 2.695 ± 1.336   | 0.228 ± 0.255   |
| vortex.two-na        |  24.5 MiB |          1.09 | 1.432 ± 0.344   | **0.121 ± 0.114** |
| vortex.four-na       |  33.9 MiB |          1.52 | 1.298 ± 0.009   | 0.285 ± 0.206   |

## SRR2584863 — Illumina 2×150 bp, 1.55M spots (288.1 MiB `.sra`)

| Config               |      Size | Ratio vs .sra | Encode (s)      | Decode (s)      |
|----------------------|----------:|--------------:|----------------:|----------------:|
| sra (baseline)       | 288.1 MiB |      **1.00** | —               | 1.768 ± 0.359   |
| parquet.ascii.none   | 916.7 MiB |          3.18 | 3.618 ± 0.079   | 0.809 ± 0.371   |
| parquet.ascii.zstd3  | 306.5 MiB |          1.06 | 9.117 ± 0.098   | 1.922 ± 0.018   |
| parquet.two-na.none  | 585.6 MiB |          2.03 | 7.140 ± 0.093   | 0.503 ± 0.411   |
| parquet.two-na.zstd3 | **280.7 MiB** | **0.97** | 12.058 ± 0.475 | 1.405 ± 0.131   |
| parquet.four-na.zstd3| 286.7 MiB |          0.99 | 11.061 ± 0.930  | 1.987 ± 0.130   |
| vortex.ascii         | 300.3 MiB |          1.04 | 15.388 ± 0.586  | 0.497 ± 0.514   |
| vortex.two-na        | 329.4 MiB |          1.14 | 17.797 ± 3.289  | **0.230 ± 0.198** |
| vortex.four-na       | 439.0 MiB |          1.52 | 10.850 ± 0.128  | 0.341 ± 0.215   |

## Takeaways

- **Smallest lossless**: `parquet.two-na.zstd3` beats VDB on both fixtures
  (0.67× / 0.97×). 2na pre-packing gives Parquet the same domain trick
  VDB has had all along; zstd-3 finishes the job.
- **Fastest decode**: `vortex.two-na` — ~0.12 s / 0.23 s vs VDB's
  0.37 s / 1.77 s. Vortex scan is zero-copy and most of its wall time is
  I/O, so the decode wins widen on the larger fixture. For workflows
  that read the whole file (alignment, FASTQ emission) Vortex is the
  clear choice.
- **Encode**: Parquet with zstd-3 lands in 1–12 s; Vortex is slower
  (1.5–18 s) because FSST dictionary training + buffer-level zstd runs
  over the full column, not per row-group. Both are trivially fast next
  to the original NCBI `.sra` assembly.
- **Vortex `two-na` / `four-na` ratios >1.0**: the Vortex 0.68
  `CascadingCompressor` hardcodes `// We do not compress binary arrays.`
  for `DType::Binary`, so 2na/4na-packed sequence columns bypass every
  compression scheme and land raw. That's why `vortex.ascii` (which
  flips sequence to UTF-8 and lets FSST run) is substantially smaller
  than the pre-packed variants. Until upstream lifts the restriction,
  `--format vortex` should stay in ASCII mode (sracha's default).

## Reproduce

```bash
# Release build with both format features.
export LIBCLANG_PATH=$PWD/.pixi/envs/default/lib
cargo build --profile release -p sracha
cargo build --profile release --examples -p sracha-core

# Each fixture writes its own markdown table.
RESULTS_MD=$PWD/validation/columnar-benchmark.SRR28588231.md \
    bash validation/columnar-benchmark.sh \
    --sra crates/sracha-core/tests/fixtures/SRR28588231.sra \
    --skip-zstd22 --runs 3
RESULTS_MD=$PWD/validation/columnar-benchmark.SRR2584863.md \
    bash validation/columnar-benchmark.sh \
    --sra crates/sracha-core/tests/fixtures/SRR2584863.sra \
    --skip-zstd22 --runs 3
```
