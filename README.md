# 🌶️ sracha 🌶️

[![Anaconda-Server Badge](https://anaconda.org/bioconda/sracha/badges/version.svg?v=2)](https://anaconda.org/bioconda/sracha) [![Anaconda-Server Badge](https://anaconda.org/bioconda/sracha/badges/downloads.svg?v=2)](https://anaconda.org/bioconda/sracha)

Fast SRA downloader and FASTQ converter, written in pure Rust.

![sracha demo](docs/images/readme.gif)

## Features

- **Fast** -- 4-11x faster than `fasterq-dump` on typical SRA files
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
| SRR28588231 | 23 MiB | 0.17 s | 1.86 s | 2.09 s | **10.9x** |
| SRR2584863 | 288 MiB | 1.51 s | 5.80 s | 13.30 s | **3.8x** |
| ERR1018173 | 1.94 GiB | 9.40 s | 34.35 s | -- | **3.7x** |

`sracha` produces gzipped FASTQ by default (level 1, ~1.4× the
uncompressed time on small files thanks to parallel block compression),
so the integrated pipeline (`sracha get`) writes ready-to-use `.fastq.gz`
without a separate gzip step.

<details>
<summary>Full hyperfine output</summary>

**SRR28588231 (23 MiB, 66K spots, Illumina paired)**

| Command | Mean [ms] | Min [ms] | Max [ms] | Relative |
|:---|---:|---:|---:|---:|
| `sracha` | 170.9 ± 1.8 | 168.2 | 175.4 | 1.00 |
| `fasterq-dump` | 1856.4 ± 14.2 | 1838.3 | 1871.6 | 10.86 ± 0.14 |
| `fastq-dump` | 2090.5 ± 33.3 | 2052.5 | 2125.0 | 12.23 ± 0.23 |

**SRR2584863 (288 MiB, Illumina paired)**

| Command | Mean [s] | Min [s] | Max [s] | Relative |
|:---|---:|---:|---:|---:|
| `sracha` | 1.512 ± 0.018 | 1.496 | 1.532 | 1.00 |
| `fasterq-dump` | 5.799 ± 0.130 | 5.667 | 5.927 | 3.83 ± 0.10 |
| `fastq-dump` | 13.297 ± 0.157 | 13.192 | 13.478 | 8.79 ± 0.15 |

**ERR1018173 (1.94 GiB, 15.6M spots, Illumina paired, single run)**

| Command | Time [s] |
|:---|---:|
| `sracha` | 9.40 |
| `fasterq-dump` | 34.35 |

**sracha gzip overhead (SRR28588231, default `--gzip-level 1`)**

| Command | Mean [ms] | Min [ms] | Max [ms] | Relative |
|:---|---:|---:|---:|---:|
| `sracha (no compression)` | 172.1 ± 5.6 | 165.1 | 185.6 | 1.00 |
| `sracha (gzip)` | 239.5 ± 5.9 | 230.9 | 249.4 | 1.39 ± 0.06 |

</details>

Benchmarks run with `sracha` v0.3.5, `sra-tools` v3.4.1, on Linux
(8 CPUs). Install the reference toolkit with `pixi run install-sratools`
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
