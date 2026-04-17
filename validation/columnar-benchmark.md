# Columnar benchmark — VDB `.sra` vs Parquet vs Vortex

Machine: `compute14`, 16 CPUs, Rust release build (LTO-fat, codegen-units=1).
Date: 2026-04-17. Each config measured with `hyperfine` over 3 runs.

`sra (baseline)` decode = `sracha fastq --stdout --split interleaved` (VDB → FASTQ).
Parquet decode = iterate `RecordBatch`es, sum `num_rows()`.
Vortex decode = scan into `ArrayStream`, sum per-chunk lengths.

All Parquet rows with `zstd3` use the default level-3 zstd. `zstd22` rows are
omitted here because single-threaded zstd-22 takes 20+ min per run on the
big fixture; they're in the writer's design space, not the everyday path.

## SRR28588231 — Illumina 2×301 bp, 22.4 MiB `.sra`, 66 K spots

| Config                | Size         | Ratio vs .sra | Encode (s)    | Decode (s)    |
|-----------------------|-------------:|--------------:|--------------:|--------------:|
| sra (baseline)        | 22.4 MiB     |        1.000  |           —   | 0.567 ± 0.327 |
| parquet.ascii.none    | 76.9 MiB     |        3.442  | 0.796 ± 0.218 | 0.246 ± 0.147 |
| parquet.ascii.zstd3   | 15.5 MiB     |        0.691  | 1.079 ± 0.284 | 0.178 ± 0.031 |
| parquet.two-na.none   | 48.4 MiB     |        2.169  | 0.979 ± 0.202 | 0.208 ± 0.243 |
| parquet.two-na.zstd3  | **14.9 MiB** |    **0.667**  | 1.125 ± 0.222 | 0.278 ± 0.108 |
| parquet.four-na.zstd3 | 15.1 MiB     |        0.672  | 1.348 ± 0.213 | 0.320 ± 0.121 |
| vortex.ascii          | 15.3 MiB     |        0.682  | 1.749 ± 0.285 | 0.266 ± 0.226 |
| vortex.two-na         | 22.4 MiB     |        1.004  | 1.387 ± 0.328 | 0.245 ± 0.151 |
| vortex.four-na        | 24.8 MiB     |        1.108  | 1.567 ± 0.228 | 0.254 ± 0.139 |

## SRR2584863 — Illumina 2×150 bp, 288 MiB `.sra`, 1.55 M spots

| Config                | Size          | Ratio vs .sra | Encode (s)     | Decode (s)     |
|-----------------------|--------------:|--------------:|---------------:|---------------:|
| sra (baseline)        | 288.1 MiB     |        1.000  |           —    | 1.685 ± 0.120  |
| parquet.ascii.none    | 916.7 MiB     |        3.182  |  6.753 ± 0.370 | 0.605 ± 0.130  |
| parquet.ascii.zstd3   | 306.5 MiB     |        1.064  | 11.607 ± 0.483 | 1.808 ± 0.017  |
| parquet.two-na.none   | 585.6 MiB     |        2.033  | 11.032 ± 0.182 | 0.387 ± 0.239  |
| parquet.two-na.zstd3  | **280.7 MiB** |    **0.974**  | 13.929 ± 0.282 | 1.209 ± 0.123  |
| parquet.four-na.zstd3 | 286.7 MiB     |        0.995  | 12.216 ± 1.606 | 1.428 ± 0.096  |
| vortex.ascii          | 300.3 MiB     |        1.042  | 12.924 ± 0.148 | 0.297 ± 0.149  |
| vortex.two-na         | 282.0 MiB     |        0.979  | 12.997 ± 0.244 | **0.125 ± 0.008** |
| vortex.four-na        | 308.3 MiB     |        1.070  | 14.214 ± 0.487 | 0.170 ± 0.154  |

## Takeaways

- **Parquet + 2na + zstd3 is still the size winner**, but `vortex.two-na` now
  ties on the big run (282.0 vs 280.7 MiB, both below `.sra`) — up from a
  14 % regression before. Changes driving the win:
  - Pack 2na/4na sequence as `List<u8>` instead of `VarBinView(Binary)`,
    which Vortex 0.68's `CascadingCompressor` hardcodes to bypass every
    scheme (`// We do not compress binary arrays.`). `Canonical::List`
    recurses into the primitive child, so BitPack / Delta / RLE / Pco fire.
  - Enable the `pco` feature on `vortex-btrblocks`, adding `PcoScheme` for
    integer / float primitives (including the list-child u8 array).
  - Drop the undocumented `exclude_schemes([IntDictScheme])` — the
    cascading compressor picks per column, so including it only helps on
    low-cardinality columns.
- **Vortex still has the decode-speed ceiling**: `vortex.two-na` decodes in
  0.125 s on the big run — 13× faster than the `.sra` baseline and ~10×
  faster than `parquet.two-na.zstd3`.
- **Vortex-ascii** is unchanged (15.3 / 300 MiB) — FSST on the 4-letter
  alphabet was already near-entropy. Quality stays as Utf8 there: a
  `List<u8>` of raw Phred was tried and regressed by ~2× because the
  BtrBlocks numeric cascade doesn't find a scheme that beats FSST on real
  quality-score distributions.
- **Parquet encode is still ~1.5× faster than Vortex** on the same data —
  Vortex pays for FSST training and cascading compression at write time.

## Reproducing

```bash
# Release build + examples (needed for the decode-side binaries).
export LIBCLANG_PATH=$PWD/.pixi/envs/default/lib
srun -p normal -c 16 --mem=16G cargo build --profile release -p sracha --features vortex
srun -p normal -c 16 --mem=16G cargo build --profile release --examples -p sracha-core --features vortex,parquet

# Benchmark one fixture at a time (the script writes a single markdown
# table; RESULTS_MD overrides the output path).
srun -p normal -c 16 --mem=16G bash validation/columnar-benchmark.sh \
    --sra crates/sracha-core/tests/fixtures/SRR28588231.sra \
    --skip-zstd22 --runs 3
srun -p normal -c 16 --mem=16G bash validation/columnar-benchmark.sh \
    --sra crates/sracha-core/tests/fixtures/SRR2584863.sra \
    --skip-zstd22 --runs 3
```

The Vortex row-block size and coalescing byte target can be swept via
`SRACHA_VORTEX_ROW_BLOCK` and `SRACHA_VORTEX_COALESCE_MIB` — the shipped
defaults (524 288 rows, 16 MiB) came from a 5×5 grid sweep on these same
two fixtures.
