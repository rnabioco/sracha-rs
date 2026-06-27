# 🌶️ sracha 🌶️

[![Anaconda-Server Badge](https://anaconda.org/bioconda/sracha/badges/version.svg?v=2)](https://anaconda.org/bioconda/sracha) [![Anaconda-Server Badge](https://anaconda.org/bioconda/sracha/badges/downloads.svg?v=2)](https://anaconda.org/bioconda/sracha)

Fast SRA downloader and FASTQ converter, written in pure Rust.

![sracha demo](docs/images/readme.gif)

## Features

- **Fast** -- 5-12x faster than `fasterq-dump` on typical SRA files
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
| SRR28588231 | 23 MiB | 0.15 s | 1.78 s | 1.94 s | **12.3x** |
| SRR2584863 | 288 MiB | 1.07 s | 5.53 s | 12.99 s | **5.2x** |
| ERR1018173 | 1.94 GiB | 6.45 s | 33.41 s | -- | **5.2x** |

`sracha` produces gzipped FASTQ by default (level 1, ~1.4× the
uncompressed time on small files thanks to parallel block compression),
so the integrated pipeline (`sracha get`) writes ready-to-use `.fastq.gz`
without a separate gzip step.

<details>
<summary>Full hyperfine output</summary>

**SRR28588231 (23 MiB, 66K spots, Illumina paired)**

| Command | Mean [ms] | Min [ms] | Max [ms] | Relative |
|:---|---:|---:|---:|---:|
| `sracha` | 145.1 ± 2.3 | 141.7 | 149.8 | 1.00 |
| `fasterq-dump` | 1782.6 ± 11.7 | 1769.3 | 1794.4 | 12.28 ± 0.21 |
| `fastq-dump` | 1942.0 ± 3.6 | 1938.0 | 1945.6 | 13.38 ± 0.22 |

**SRR2584863 (288 MiB, Illumina paired)**

| Command | Mean [s] | Min [s] | Max [s] | Relative |
|:---|---:|---:|---:|---:|
| `sracha` | 1.070 ± 0.006 | 1.064 | 1.076 | 1.00 |
| `fasterq-dump` | 5.526 ± 0.081 | 5.441 | 5.602 | 5.16 ± 0.08 |
| `fastq-dump` | 12.989 ± 0.031 | 12.967 | 13.025 | 12.14 ± 0.07 |

**ERR1018173 (1.94 GiB, 15.6M spots, Illumina paired, single run)**

| Command | Time [s] |
|:---|---:|
| `sracha` | 6.45 |
| `fasterq-dump` | 33.41 |

**sracha gzip overhead (SRR28588231, default `--gzip-level 1`)**

| Command | Mean [ms] | Min [ms] | Max [ms] | Relative |
|:---|---:|---:|---:|---:|
| `sracha (no compression)` | 150.4 ± 2.5 | 145.7 | 154.4 | 1.00 |
| `sracha (gzip)` | 213.7 ± 2.8 | 208.9 | 218.3 | 1.42 ± 0.03 |

</details>

Benchmarks run with `sracha` v0.3.8, `sra-tools` v3.4.1, on Linux
(8 CPUs). Install the reference toolkit with `pixi run install-sratools`
and reproduce with `validation/benchmark.sh`.

## Installation

Install via [Bioconda](https://bioconda.github.io/):

```bash
pixi add bioconda::sracha
```

Or download pre-built binaries from the
[releases page](https://github.com/rnabioco/sracha-rs/releases),
or install from source:

```bash
cargo install --git https://github.com/rnabioco/sracha-rs sracha
```

On x86_64 the release page carries two variants per platform: pick **`-v2`**
(the safe default — runs on any CPU since ~2009), or **`-v3`** for extra SIMD
throughput on Haswell-or-newer (2013+) hardware. A `-v3` binary aborts with an
illegal-instruction fault at startup on older CPUs, so prefer `-v2` unless you
know the host has AVX2. ARM builds (`aarch64`) ship a single binary.

To build from source tuned for the current machine, set
`RUSTFLAGS="-C target-cpu=native"` before `cargo install`/`cargo build --release`.

### Containers

Because sracha is on Bioconda, [BioContainers](https://biocontainers.pro/)
automatically publishes a Docker/Singularity image for every release — no
local build required.

```bash
# Docker / Podman
docker run --rm quay.io/biocontainers/sracha:0.3.7--h54198d6_0 sracha --help

# Singularity / Apptainer
singularity run \
  https://depot.galaxyproject.org/singularity/sracha:0.3.7--h54198d6_0 sracha --help
```

The tags above are examples — check
[quay.io](https://quay.io/repository/biocontainers/sracha?tab=tags) for the
latest `<version>--<build>` tag and substitute it in.

In [Nextflow](https://www.nextflow.io/), point a process at the image directly
or let the `conda` directive resolve it:

```groovy
process SRACHA_GET {
    container 'quay.io/biocontainers/sracha:0.3.7--h54198d6_0'
    // or: conda 'bioconda::sracha=0.3.7'
    // ...
}
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
