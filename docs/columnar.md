# Columnar formats

`sracha convert` writes SRA data into columnar storage formats — either
**Apache Parquet** or **Vortex** (SpiralDB). It's an experimental feature
aimed at storage benchmarking and downstream analytics workflows that
want Arrow-native access to reads instead of plain FASTQ.

!!! note
    `sracha convert` is gated on the `parquet` and `vortex` Cargo
    features. Both are on by default — release binaries and the bioconda
    package include both. A `--no-default-features` build must opt in.

## When to use it

- **Archival with better compression than `.sra`** — Parquet with
  2-bit DNA packing and zstd beats the SRA baseline on both fixtures
  we tested.
- **Arrow / DuckDB / DataFusion workflows** — read `sequence` and
  `quality` columns directly without a FASTQ round-trip.
- **Fast random-access decode** — Vortex decodes 9× faster than `.sra`
  (see below).
- **Benchmarking** — comparing encoding/compression strategies for
  sequencing data.

If you just want FASTQ, use [`sracha get`](cli.md#sracha-get) or
[`sracha fastq`](cli.md#sracha-fastq).

## Quick start

```bash
# Defaults: Parquet, two-na packed sequence, zstd level 22
sracha convert SRR28588231.sra

# Vortex output (ascii sequence, Vortex picks the encoding cascade)
sracha convert SRR28588231.sra --format vortex

# Parquet tuned for write speed — zstd-3 is what the benchmarks use
sracha convert SRR28588231.sra --compression zstd --zstd-level 3
```

Output files are written to the current directory with the stem of the
input and the format-appropriate extension (`.parquet` or `.vortex`).
Use `-O` to redirect and `-f` to overwrite.

## Output schema

Each row is a single read. Column types depend on `--pack-dna` and
`--length-mode`:

| Column | Type | Notes |
|--------|------|-------|
| `spot_id` | UInt64 | Parent spot identifier |
| `read_num` | UInt8 | 0-based read index within the spot |
| `name` | Utf8 (nullable) | Spot name |
| `read_len` | UInt32 | Variable-length mode only — omitted in fixed mode |
| `sequence` | Binary or FixedSizeBinary | Width depends on packing and mode |
| `quality` | Binary or FixedSizeBinary (nullable) | Width matches `read_len` |

For **fixed**-length runs, `sequence` and `quality` are
`FixedSizeBinary`, the read length is declared once in the schema, and
there's no per-row `read_len` column. For **variable**-length runs,
both are plain `Binary` and a `read_len` column records the length of
each read.

## DNA packing

The `--pack-dna` flag controls how bases are encoded in the `sequence`
column.

| Mode | Bits/base | Density | IUPAC ambiguity | Best with |
|------|-----------|---------|-----------------|-----------|
| `ascii` | 8 | 1× | preserved | vortex (see below) |
| `two-na` | 2 | 4× | **lossy** — only A/C/G/T/N | parquet + zstd |
| `four-na` | 4 | 2× | preserved | mixed-alphabet reads |

**Why the format-specific defaults?** Parquet's page-level zstd
compresses packed 2-bit sequence well, so `two-na` wins on size. Vortex
0.68's `CascadingCompressor`, by design, skips every scheme for
`Binary`-typed columns — pre-packed 2na/4na bytes land uncompressed in
the file. Leaving the sequence as `ascii` lets BtrBlocks' FSST cascade
fire on the 4-letter DNA alphabet and reach near-entropy size without
any pre-packing.

## Length mode

The `--length-mode` flag picks the schema shape.

- `auto` (default) — detects from the data: fixed if all reads share a
  length, else variable.
- `fixed` — forces fixed-length schema and errors on a length mismatch.
- `variable` — forces the variable-length schema even when reads are
  uniform.

Fixed-mode files are smaller and faster to scan because there is no
per-row length column and column readers can skip by a known stride.
`auto` is the right answer for almost all modern Illumina / DNBSEQ /
Element runs.

## Choosing a format

| Goal | Recommendation |
|------|----------------|
| Smallest file | `--format parquet --pack-dna two-na --compression zstd --zstd-level 22` |
| Fast encode + good size | `--format parquet --pack-dna two-na --compression zstd --zstd-level 3` |
| Fastest decode | `--format vortex --pack-dna ascii` |
| Arrow ecosystem compatibility | `--format parquet` |

## Benchmark summary

Headline numbers from `SRR2584863` (Illumina 2×150 bp, 288 MiB `.sra`,
1.55 M spots) on a 96-core machine. Lower is better for both columns.

| Config | Size vs `.sra` | Decode time |
|--------|---------------:|------------:|
| `.sra` baseline | 1.00× | 1.20 s |
| `parquet.two-na.zstd3` | **0.97×** | 1.11 s |
| `vortex.ascii` | 1.04× | 0.35 s |
| `vortex.two-na` | 1.14× | **0.13 s** |

Takeaways:

- **Parquet + 2na + zstd-3 is the size winner**, edging out `.sra` by
  3% on this fixture and by 33% on the small fixture we also tested.
- **Vortex + 2na hits the decode-speed ceiling** — 9× faster than
  `.sra`, 9× faster than Parquet at roughly the same level.
- **Vortex-ascii matches Parquet-zstd on size** because FSST reaches
  entropy on the DNA alphabet.

Full per-fixture tables, encode times, and the small-fixture numbers
are in
[`validation/columnar-benchmark.md`](https://github.com/rnabioco/sracha-rs/blob/main/validation/columnar-benchmark.md)
on GitHub.

## Reproducing

```bash
# Release build (LTO). LIBCLANG_PATH is only needed on the pixi env.
export LIBCLANG_PATH=$PWD/.pixi/envs/default/lib
cargo build --profile release -p sracha
cargo build --profile release -p sracha-core --examples

# Run the sweep over one fixture at a time
bash validation/columnar-benchmark.sh \
    --sra crates/sracha-core/tests/fixtures/SRR28588231.sra \
    --skip-zstd22 --runs 3
bash validation/columnar-benchmark.sh \
    --sra crates/sracha-core/tests/fixtures/SRR2584863.sra \
    --skip-zstd22 --runs 3
```

The Vortex row-block size and coalescing byte target can be swept via
`SRACHA_VORTEX_ROW_BLOCK` and `SRACHA_VORTEX_COALESCE_MIB`. The
shipped defaults (524 288 rows, 16 MiB) came from a 5×5 grid sweep on
the two fixtures above.
