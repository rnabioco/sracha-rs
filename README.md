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
- **Integrity checks** -- MD5 verification on download and decode, plus strict
  checks that refuse to write records sracha cannot vouch for (`--no-strict`
  downgrades them to warnings; `--verify` additionally checks quality *values*
  against the archive's recorded histogram)
- **Platform support** -- Illumina, BGISEQ/DNBSEQ, Element, Ultima, PacBio, Nanopore (legacy 454, SOLiD, Ion Torrent, Helicos and Capillary are declined with a clear error)
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
| SRR28588231 | 23 MiB | 0.17 s | 2.17 s | 1.95 s | **13.0x** |
| SRR2584863 | 288 MiB | 1.33 s | 7.91 s | 13.36 s | **6.0x** |
| ERR1018173 | 1.94 GiB | 6.41 s | 36.06 s | -- | **5.6x** |

`sracha` produces gzipped FASTQ by default (level 1, roughly 1.5× the
uncompressed time on small files thanks to parallel block compression),
so the integrated pipeline (`sracha get`) writes ready-to-use `.fastq.gz`
without a separate gzip step.

<details>
<summary>Full hyperfine output</summary>

**SRR28588231 (23 MiB, 66K spots, Illumina paired)**

| Command | Mean [ms] | Min [ms] | Max [ms] | Relative |
|:---|---:|---:|---:|---:|
| `sracha` | 166.7 ± 66.1 | 139.7 | 343.5 | 1.00 |
| `fasterq-dump` | 2170.6 ± 262.4 | 1970.9 | 2536.9 | 13.02 ± 5.40 |
| `fastq-dump` | 1953.6 ± 3.7 | 1948.8 | 1957.9 | 11.72 ± 4.65 |

**SRR2584863 (288 MiB, Illumina paired)**

| Command | Mean [s] | Min [s] | Max [s] | Relative |
|:---|---:|---:|---:|---:|
| `sracha` | 1.328 ± 0.120 | 1.229 | 1.462 | 1.00 |
| `fasterq-dump` | 7.911 ± 1.868 | 5.827 | 9.436 | 5.96 ± 1.51 |
| `fastq-dump` | 13.363 ± 0.275 | 13.191 | 13.680 | 10.06 ± 0.94 |

**ERR1018173 (1.94 GiB, 15.6M spots, Illumina paired, single run)**

| Command | Time [s] |
|:---|---:|
| `sracha` | 6.41 |
| `fasterq-dump` | 36.06 |

**sracha gzip overhead (SRR28588231, default `--gzip-level 1`)**

| Command | Mean [ms] | Min [ms] | Max [ms] | Relative |
|:---|---:|---:|---:|---:|
| `sracha (no compression)` | 145.2 ± 2.6 | 141.2 | 150.3 | 1.00 |
| `sracha (gzip)` | 274.0 ± 98.0 | 207.8 | 417.1 | 1.89 ± 0.68 |

</details>

Benchmarks run with `sracha` v0.9.0, `sra-tools` v3.4.1, on Linux
(8 CPUs, Xeon E5-2699 v4, shared cluster node, so expect run-to-run noise
on the small files — see the ± columns; the stages ran on separate nodes,
each comparing the tools on the same node). Install the reference toolkit with `pixi run install-sratools`
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

The release page covers Linux (x86_64, aarch64), macOS (Intel, Apple
silicon), and Windows (x86_64) — a `.tar.gz` per Unix target and a `.zip` for
Windows.

On Linux and macOS x86_64 there are two variants per platform: pick **`-v2`**
(the safe default — runs on any CPU since ~2009), or **`-v3`** for extra SIMD
throughput on Haswell-or-newer (2013+) hardware. A `-v3` binary aborts with an
illegal-instruction fault at startup on older CPUs, so prefer `-v2` unless you
know the host has AVX2. ARM (`aarch64`) and Windows builds ship a single
binary.

**Windows users:** take the `.zip` from the release page, or build with
`cargo install` above. Bioconda cannot help here — it builds for Linux and
macOS only, and never for Windows, so `bioconda::sracha` and the
BioContainers images below are unavailable on Windows. (Running sracha under
WSL2 also works, and gets you the Bioconda package.)

To build from source tuned for the current machine, set
`RUSTFLAGS="-C target-cpu=native"` before `cargo install`/`cargo build --release`.

### Containers

Because sracha is on Bioconda, [BioContainers](https://biocontainers.pro/)
automatically publishes a Docker/Singularity image for every release — no
local build required.

```bash
# Docker / Podman
docker run --rm quay.io/biocontainers/sracha:0.10.1--h54198d6_0 sracha --help

# Singularity / Apptainer
singularity run \
  https://depot.galaxyproject.org/singularity/sracha:0.10.1--h54198d6_0 sracha --help
```

These tags track the current Bioconda release and are refreshed weekly by a
CI job. BioContainers builds an image a day or two behind a new release, so
just after a release the tag here may still name the previous version — see
[quay.io](https://quay.io/repository/biocontainers/sracha?tab=tags) for every
published `<version>--<build>` tag.

Using sracha in a workflow manager? See
[Nextflow and workflow integration](https://rnabioco.github.io/sracha-rs/#nextflow)
in the docs.

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
