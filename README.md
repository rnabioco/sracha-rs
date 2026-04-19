# 🌶️ sracha 🌶️

[![Bioconda](https://anaconda.org/bioconda/sracha/badges/version.svg)](https://anaconda.org/bioconda/sracha) [![Anaconda-Server Badge](https://anaconda.org/bioconda/sracha/badges/downloads.svg)](https://anaconda.org/bioconda/sracha)

Fast SRA downloader and FASTQ converter, written in pure Rust.

![sracha get](docs/images/get.gif)

## Features

- **Fast** -- 3-11x faster than `fasterq-dump` on typical SRA files
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

Local SRA-to-FASTQ conversion (no network), uncompressed output,
8 CPU cores, measured with [hyperfine](https://github.com/sharkdp/hyperfine).

| File | Size | sracha | fasterq-dump | fastq-dump | Speedup vs fasterq-dump |
|:---|---:|---:|---:|---:|---:|
| SRR28588231 | 23 MiB | 0.19 s | 2.03 s | 2.03 s | **10.9x** |
| SRR2584863 | 288 MiB | 1.53 s | 6.60 s | 12.57 s | **4.3x** |
| ERR1018173 | 1.94 GiB | 13.4 s | 44.9 s | -- | **3.4x** |

Compression adds minimal overhead -- sracha produces gzipped FASTQ by default
with parallel block compression, so the integrated pipeline
(`sracha get`) is often faster end-to-end than `fasterq-dump` followed by a
separate gzip step.

<details>
<summary>Full hyperfine output</summary>

**SRR28588231 (23 MiB, 66K spots, Illumina paired)**

| Command | Mean [ms] | Min [ms] | Max [ms] | Relative |
|:---|---:|---:|---:|---:|
| `sracha` | 187.2 ± 2.9 | 183.5 | 191.5 | 1.00 |
| `fasterq-dump` | 2033.1 ± 36.1 | 1992.1 | 2086.3 | 10.86 ± 0.25 |
| `fastq-dump` | 2031.7 ± 14.1 | 2018.8 | 2050.2 | 10.85 ± 0.18 |

**SRR2584863 (288 MiB, Illumina paired)**

| Command | Mean [s] | Min [s] | Max [s] | Relative |
|:---|---:|---:|---:|---:|
| `sracha` | 1.532 ± 0.017 | 1.519 | 1.552 | 1.00 |
| `fasterq-dump` | 6.599 ± 0.099 | 6.499 | 6.697 | 4.31 ± 0.08 |
| `fastq-dump` | 12.574 ± 0.051 | 12.527 | 12.628 | 8.21 ± 0.10 |

**ERR1018173 (1.94 GiB, 15.6M spots, Illumina paired, single run)**

| Command | Time [s] |
|:---|---:|
| `sracha` | 13.38 |
| `fasterq-dump` | 44.92 |

**sracha gzip overhead (SRR28588231)**

| Command | Mean [ms] | Min [ms] | Max [ms] | Relative |
|:---|---:|---:|---:|---:|
| `sracha (no compression)` | 191.4 ± 6.9 | 176.5 | 200.4 | 1.00 |
| `sracha (gzip)` | 358.6 ± 10.4 | 348.6 | 376.6 | 1.87 ± 0.09 |

</details>

Benchmarks run with `sracha` v0.1.10+6cec62e, `sra-tools` v3.2.0, on Linux
(8 CPUs). See `validation/benchmark.sh` to reproduce.

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
