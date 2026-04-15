# 🌶️ sracha 🌶️

Fast SRA downloader and FASTQ converter, written in pure Rust.

## Features

- **Parallel downloads** -- chunked HTTP Range requests with multiple connections
- **Native VDB parsing** -- pure Rust, zero C dependencies
- **Integrated pipeline** -- download, convert, and compress in one command
- **Project-level accessions** -- pass a BioProject (PRJNA) or study (SRP) to download all runs
- **Accession lists** -- batch download from a file with `--accession-list`
- **Parallel gzip or zstd** -- pigz-style block compression via rayon
- **FASTA output** -- drop quality scores with `--fasta`
- **SRA and SRA-lite** -- full quality or simplified quality scores
- **Split modes** -- split-3, split-files, split-spot, interleaved
- **Platform support** -- Illumina, BGISEQ/DNBSEQ, Element, Ultima, PacBio, Nanopore (legacy platforms like 454 and Ion Torrent are not supported)
- **Resumable downloads** -- picks up where it left off on interruption
- **File validation** -- verify SRA file integrity

## Quick start

```bash
# Download, convert, and compress
sracha get SRR000001

# Download all runs from a BioProject
sracha get PRJNA675068

# Batch download from an accession list
sracha get --accession-list SRR_Acc_List.txt

# Just download
sracha fetch SRR000001

# Convert a local .sra file
sracha fastq SRR000001.sra

# Show accession info
sracha info SRR000001

# Validate a downloaded file
sracha validate SRR000001.sra
```

## Benchmarks

Local SRA-to-FASTQ conversion (no network), uncompressed output,
8 CPU cores, measured with [hyperfine](https://github.com/sharkdp/hyperfine).

| File | Size | sracha | fasterq-dump | fastq-dump | Speedup vs fasterq-dump |
|:---|---:|---:|---:|---:|---:|
| SRR28588231 | 23 MiB | 0.16 s | 1.79 s | 1.96 s | **11.6x** |
| SRR2584863 | 288 MiB | 1.46 s | 5.46 s | 13.27 s | **3.7x** |
| SRR14724462 | 3.78 GiB | 17.0 s | 108.8 s | -- | **6.4x** |

Compression adds minimal overhead -- sracha produces gzipped FASTQ by default
with parallel block compression, so the integrated pipeline
(`sracha get`) is often faster end-to-end than `fasterq-dump` followed by a
separate gzip step.

<details>
<summary>Full hyperfine output</summary>

**SRR28588231 (23 MiB, 66K spots)**

| Command | Mean [ms] | Min [ms] | Max [ms] | Relative |
|:---|---:|---:|---:|---:|
| `sracha` | 155.3 ± 1.6 | 151.8 | 158.2 | 1.00 |
| `fasterq-dump` | 1793.9 ± 11.0 | 1778.1 | 1806.5 | 11.55 ± 0.14 |
| `fastq-dump` | 1961.5 ± 2.6 | 1957.6 | 1964.5 | 12.63 ± 0.13 |

**SRR2584863 (288 MiB)**

| Command | Mean [s] | Min [s] | Max [s] | Relative |
|:---|---:|---:|---:|---:|
| `sracha` | 1.456 ± 0.006 | 1.450 | 1.461 | 1.00 |
| `fasterq-dump` | 5.456 ± 0.034 | 5.429 | 5.494 | 3.75 ± 0.03 |
| `fastq-dump` | 13.268 ± 0.049 | 13.226 | 13.322 | 9.11 ± 0.05 |

**SRR14724462 (3.78 GiB, single run)**

| Command | Time [s] |
|:---|---:|
| `sracha` | 17.0 |
| `fasterq-dump` | 108.8 |

**sracha gzip overhead (SRR28588231)**

| Command | Mean [ms] | Min [ms] | Max [ms] | Relative |
|:---|---:|---:|---:|---:|
| `sracha (no compression)` | 169.9 ± 51.8 | 151.9 | 363.9 | 1.00 |
| `sracha (gzip)` | 315.2 ± 3.5 | 310.6 | 319.4 | 1.86 ± 0.57 |

</details>

Benchmarks run with `sracha` v0.1.5, `sra-tools` v3.2.0, on Linux (8 CPUs).
See `validation/benchmark.sh` to reproduce.

## Installation

Download pre-built binaries from the
[releases page](https://github.com/rnabioco/sracha-rs/releases),
or install from source:

```bash
cargo install --git https://github.com/rnabioco/sracha-rs sracha
```

## Documentation

Full CLI reference and usage guide: <https://rnabioco.github.io/sracha-rs/>

## License

MIT
