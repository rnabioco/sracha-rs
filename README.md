# 🌶️ sracha 🌶️

[![Anaconda-Server Badge](https://anaconda.org/bioconda/sracha/badges/version.svg?v=1)](https://anaconda.org/bioconda/sracha) [![Anaconda-Server Badge](https://anaconda.org/bioconda/sracha/badges/downloads.svg?v=1)](https://anaconda.org/bioconda/sracha)

Fast SRA downloader and FASTQ converter, written in pure Rust.

![sracha demo](docs/images/readme.gif)

## Features

- **Fast** -- 5-13x faster than `fasterq-dump` on typical SRA files
- **One command** -- download, convert to FASTQ, and compress
- **Batch input** -- accessions, BioProjects (PRJNA), studies (SRP), or a file via `--accession-list`
- **gzip or zstd output** -- parallel compression, or plain FASTQ
- **FASTA output** -- `--fasta` drops quality scores
- **SRA and SRA-lite** -- full or simplified quality scores
- **Split modes** -- split-3, split-files, split-spot, interleaved
- **Resumable downloads** -- picks up where it left off
- **Stdout streaming** -- `-Z` pipes FASTQ straight into downstream tools
- **Integrity checks** -- MD5 verification on download and decode
- **Platform support** -- Illumina, BGISEQ/DNBSEQ, Element, Ultima, PacBio, Nanopore (legacy 454 and Ion Torrent are not supported)
- **Single static binary** -- no Python, no C dependencies

## Quick start

```bash
# Download, convert, and compress
sracha get SRR28588231

# Download all runs from a BioProject
sracha get PRJNA675068

# Batch download from an accession list
sracha get --accession-list SRR_Acc_List.txt

# Just download
sracha fetch SRR28588231

# Convert a local .sra file
sracha fastq SRR28588231.sra

# Show accession info
sracha info SRR28588231

# Validate a downloaded file
sracha validate SRR28588231.sra
```

## Benchmarks

### Local decode (SRA file on disk → FASTQ)

Uncompressed output, measured with
[hyperfine](https://github.com/sharkdp/hyperfine).

| File | Size | sracha | fasterq-dump | fastq-dump | Speedup vs fasterq-dump |
|:---|---:|---:|---:|---:|---:|
| SRR28588231 | 23 MiB | 0.14 s | 1.83 s | 1.87 s | **13.3x** |
| SRR2584863 | 288 MiB | 1.13 s | 5.37 s | 11.41 s | **4.8x** |
| ERR1018173 | 1.94 GiB | 6.76 s | 32.25 s | -- | **4.8x** |

Compression adds minimal overhead -- sracha produces gzipped FASTQ by default
with parallel block compression, so the integrated pipeline
(`sracha get`) is often faster end-to-end than `fasterq-dump` followed by a
separate gzip step.

### End-to-end (accession → FASTQ, including download)

Download + decode, 5 runs each from a fresh temp dir.

| Accession | Size | `sracha get` | `prefetch + fasterq-dump` | `prefetch + fastq-dump` | Speedup vs `prefetch + fasterq-dump` |
|:---|---:|---:|---:|---:|---:|
| SRR28588231 | 23 MiB | 1.44 s | 4.17 s | 4.27 s | **2.90x** |
| SRR2584863 | 288 MiB | 8.08 s | 12.55 s | 18.47 s | **1.55x** |

`sracha get` beats `prefetch + fasterq-dump` end-to-end even with the
network in the loop, because the parallel chunked downloader overlaps
with decode and the decode itself is 5x faster. See
`validation/bench-results/` for raw hyperfine output.

<details>
<summary>Full hyperfine output</summary>

**SRR28588231 (23 MiB, 66K spots, Illumina paired)**

| Command | Mean [ms] | Min [ms] | Max [ms] | Relative |
|:---|---:|---:|---:|---:|
| `sracha` | 137.5 ± 5.9 | 127.5 | 148.8 | 1.00 |
| `fasterq-dump` | 1832.8 ± 23.9 | 1799.1 | 1857.7 | 13.33 ± 0.60 |
| `fastq-dump` | 1871.7 ± 30.2 | 1840.8 | 1910.6 | 13.62 ± 0.62 |

**SRR2584863 (288 MiB, Illumina paired)**

| Command | Mean [s] | Min [s] | Max [s] | Relative |
|:---|---:|---:|---:|---:|
| `sracha` | 1.126 ± 0.091 | 1.059 | 1.230 | 1.00 |
| `fasterq-dump` | 5.368 ± 0.024 | 5.347 | 5.394 | 4.77 ± 0.39 |
| `fastq-dump` | 11.410 ± 0.025 | 11.392 | 11.438 | 10.13 ± 0.82 |

**ERR1018173 (1.94 GiB, 15.6M spots, Illumina paired, single run)**

| Command | Time [s] |
|:---|---:|
| `sracha` | 6.76 |
| `fasterq-dump` | 32.25 |

**sracha gzip overhead (SRR28588231)**

| Command | Mean [ms] | Min [ms] | Max [ms] | Relative |
|:---|---:|---:|---:|---:|
| `sracha (no compression)` | 131.3 ± 4.1 | 123.6 | 138.2 | 1.00 |
| `sracha (gzip)` | 189.7 ± 2.8 | 184.7 | 194.2 | 1.44 ± 0.05 |

**End-to-end: SRR28588231 (23 MiB) — accession → FASTQ (5 runs)**

| Command | Mean [s] | Min [s] | Max [s] | Relative |
|:---|---:|---:|---:|---:|
| `sracha get` | 1.437 ± 0.067 | 1.383 | 1.550 | 1.00 |
| `prefetch + fasterq-dump` | 4.169 ± 0.034 | 4.125 | 4.209 | 2.90 ± 0.14 |
| `prefetch + fastq-dump` | 4.270 ± 0.099 | 4.199 | 4.422 | 2.97 ± 0.15 |

**End-to-end: SRR2584863 (288 MiB) — accession → FASTQ (5 runs)**

| Command | Mean [s] | Min [s] | Max [s] | Relative |
|:---|---:|---:|---:|---:|
| `sracha get` | 8.078 ± 0.154 | 7.906 | 8.277 | 1.00 |
| `prefetch + fasterq-dump` | 12.547 ± 0.381 | 12.027 | 12.849 | 1.55 ± 0.06 |
| `prefetch + fastq-dump` | 18.466 ± 0.156 | 18.369 | 18.741 | 2.29 ± 0.05 |

</details>

Benchmarks run with `sracha` v0.3.0, `sra-tools` v3.4.1, on Linux
(16 CPUs). Install the reference toolkit with `pixi run install-sratools`
and reproduce with `validation/benchmark.sh`.

## Installation

Install via [Bioconda](https://bioconda.github.io/):

```bash
pixi add --channel bioconda sracha
```

Or download pre-built binaries from the
[releases page](https://github.com/rnabioco/sracha-rs/releases),
or install from source:

```bash
cargo install --git https://github.com/rnabioco/sracha-rs sracha
```

## Documentation

Full CLI reference and usage guide: <https://rnabioco.github.io/sracha-rs/>

## Acknowledgments

sracha builds on the [Sequence Read Archive](https://www.ncbi.nlm.nih.gov/sra),
maintained by the [National Center for Biotechnology Information](https://www.ncbi.nlm.nih.gov/)
at the National Library of Medicine. The SRA and its
[toolchain](https://github.com/ncbi/sra-tools) are public-domain software
developed by U.S. government employees — our tax dollars at work. Special
thanks to Kenneth Durbrow ([@durbrow](https://github.com/durbrow)) and the
SRA Toolkit team for building and maintaining the infrastructure that makes
projects like this possible.

This project wouldn't exist without NCBI's open infrastructure: the
VDB/KAR format, the SDL locate API, EUtils, and public S3 hosting of
sequencing data. sracha aims to make it easier for the community to
build on that foundation.

## License

MIT
