# sracha

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
2 CPU cores, measured with [hyperfine](https://github.com/sharkdp/hyperfine).

| File | Size | sracha | fasterq-dump | fastq-dump | Speedup vs fasterq-dump |
|:---|---:|---:|---:|---:|---:|
| SRR28588231 | 23 MiB | 0.36 s | 2.70 s | 2.16 s | **7.5x** |
| SRR000001 | 299 MiB | 1.53 s | 4.57 s | 6.04 s | **3.0x** |
| SRR17778105 | 51 GiB | 49 s | did not complete\* | -- | |

\*fasterq-dump processed only 15% of spots after 15 min before being
terminated. fastq-dump omitted for the large file.

Compression adds minimal overhead -- sracha produces gzipped FASTQ by default
with parallel block compression, so the integrated pipeline
(`sracha get`) is often faster end-to-end than `fasterq-dump` followed by a
separate gzip step.

<details>
<summary>Full hyperfine output</summary>

**SRR28588231 (23 MiB, 66K spots)**

| Command | Mean [ms] | Min [ms] | Max [ms] | Relative |
|:---|---:|---:|---:|---:|
| `sracha` | 357.8 ± 11.4 | 346.2 | 374.0 | 1.00 |
| `fasterq-dump` | 2697.5 ± 48.6 | 2629.6 | 2740.4 | 7.54 ± 0.28 |
| `fastq-dump` | 2159.2 ± 52.6 | 2122.6 | 2252.2 | 6.03 ± 0.24 |

**SRR000001 (299 MiB)**

| Command | Mean [s] | Min [s] | Max [s] | Relative |
|:---|---:|---:|---:|---:|
| `sracha` | 1.528 ± 0.032 | 1.498 | 1.561 | 1.00 |
| `fasterq-dump` | 4.571 ± 0.051 | 4.520 | 4.622 | 2.99 ± 0.07 |
| `fastq-dump` | 6.040 ± 0.111 | 5.940 | 6.160 | 3.95 ± 0.11 |

**sracha gzip overhead (SRR28588231)**

| Command | Mean [ms] | Min [ms] | Max [ms] | Relative |
|:---|---:|---:|---:|---:|
| `sracha (no compression)` | 354.3 ± 12.2 | 340.1 | 376.7 | 1.00 |
| `sracha (gzip)` | 1412.1 ± 16.3 | 1395.6 | 1432.8 | 3.99 ± 0.15 |

</details>

Benchmarks run with `sracha` v0.1.4, `sra-tools` v3.2.0, on Linux (2 CPUs).
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
