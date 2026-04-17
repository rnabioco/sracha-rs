# Columnar benchmark — VDB `.sra` vs Parquet vs Vortex

Machine: `compute21`, 96 CPUs, Rust release build (LTO-fat, codegen-units=1).
Date: 2026-04-17. Each config measured with `hyperfine` over 3 runs.

`sra (baseline)` decode = `sracha fastq --stdout --split interleaved` (VDB → FASTQ).
Parquet decode = iterate `RecordBatch`es, sum `num_rows()`.
Vortex decode = scan into `ArrayStream`, sum per-chunk lengths.

All Parquet rows with `zstd3` use the default level-3 zstd. `zstd22` rows are
omitted here because single-threaded zstd-22 takes 20+ min per run on the
big fixture; they're in the writer's design space, not the everyday path.

## SRR28588231 — Illumina 2×301 bp, 22.4 MiB `.sra`, 66 K spots

| Config                | Size     | Ratio vs .sra | Encode (s)    | Decode (s)    |
|-----------------------|---------:|--------------:|--------------:|--------------:|
| sra (baseline)        | 22.4 MiB |        1.000  |           —   | 0.151 ± 0.006 |
| parquet.ascii.none    | 76.9 MiB |        3.442  | 0.488 ± 0.006 | 0.058 ± 0.004 |
| parquet.ascii.zstd3   | 15.5 MiB |        0.691  | 0.759 ± 0.012 | 0.145 ± 0.002 |
| parquet.two-na.none   | 48.4 MiB |        2.169  | 0.591 ± 0.010 | 0.054 ± 0.005 |
| parquet.two-na.zstd3  | **14.9 MiB** |    **0.667**  | 0.852 ± 0.004 | 0.124 ± 0.001 |
| parquet.four-na.zstd3 | 15.1 MiB |        0.672  | 0.790 ± 0.014 | 0.131 ± 0.001 |
| vortex.ascii          | 15.3 MiB |        0.682  | 1.407 ± 0.129 | 0.076 ± 0.018 |
| vortex.two-na         | 24.5 MiB |        1.094  | 0.960 ± 0.002 | 0.065 ± 0.003 |
| vortex.four-na        | 33.9 MiB |        1.519  | 0.833 ± 0.012 | 0.075 ± 0.003 |

## SRR2584863 — Illumina 2×150 bp, 288 MiB `.sra`, 1.55 M spots

| Config                | Size       | Ratio vs .sra | Encode (s)     | Decode (s)     |
|-----------------------|-----------:|--------------:|---------------:|---------------:|
| sra (baseline)        | 288.1 MiB  |        1.000  |           —    | 1.202 ± 0.078  |
| parquet.ascii.none    | 916.7 MiB  |        3.182  |  4.211 ± 0.212 | 0.403 ± 0.005  |
| parquet.ascii.zstd3   | 306.5 MiB  |        1.064  |  9.626 ± 0.169 | 1.962 ± 0.117  |
| parquet.two-na.none   | 585.6 MiB  |        2.033  |  8.004 ± 0.295 | 0.248 ± 0.001  |
| parquet.two-na.zstd3  | **280.7 MiB** |   **0.974**   | 12.360 ± 0.154 | 1.113 ± 0.004  |
| parquet.four-na.zstd3 | 286.7 MiB  |        0.995  | 10.138 ± 0.339 | 1.459 ± 0.008  |
| vortex.ascii          | 300.3 MiB  |        1.042  | 13.835 ± 0.207 | 0.345 ± 0.405  |
| vortex.two-na         | 329.4 MiB  |        1.143  | 13.726 ± 0.089 | 0.129 ± 0.015  |
| vortex.four-na        | 439.0 MiB  |        1.524  |  9.441 ± 0.225 | 0.144 ± 0.002  |

## Takeaways

- **Parquet + 2na + zstd3 is the size winner** on both fixtures: beats `.sra`
  by ~3 % on the big run and by 33 % on the small run.
- **Vortex has the decode-speed ceiling**: `vortex.two-na` decodes in 0.13 s
  on the big run (9× faster than `parquet.two-na.zstd3`, 9× faster than the
  `.sra` baseline).
- **Vortex-ascii matches Parquet-zstd3 on size**, because FSST reaches
  near-entropy on the 4-letter DNA alphabet. 2na pre-packing *hurts* Vortex
  because Vortex 0.68's `CascadingCompressor` hardcodes
  `// We do not compress binary arrays.` — `VarBinView(Binary)` bypasses
  every scheme, so 2na bytes land uncompressed in the file.
- **Parquet encode is ~1.5× faster than Vortex** on the same data — Vortex
  pays for FSST training and cascading compression at write time.

## Reproducing

```bash
# Release build + examples (needed for the decode-side binaries).
export LIBCLANG_PATH=$PWD/.pixi/envs/default/lib
cargo build --profile release -p sracha --jobs 96
cargo build --profile release -p sracha-core --examples --jobs 96

# Benchmark one fixture at a time (the script writes a single markdown
# table; RESULTS_MD overrides the output path).
bash validation/columnar-benchmark.sh \
    --sra crates/sracha-core/tests/fixtures/SRR28588231.sra \
    --skip-zstd22 --runs 3
bash validation/columnar-benchmark.sh \
    --sra crates/sracha-core/tests/fixtures/SRR2584863.sra \
    --skip-zstd22 --runs 3
```

The Vortex row-block size and coalescing byte target can be swept via
`SRACHA_VORTEX_ROW_BLOCK` and `SRACHA_VORTEX_COALESCE_MIB` — the shipped
defaults (524 288 rows, 16 MiB) came from a 5×5 grid sweep on these same
two fixtures.
