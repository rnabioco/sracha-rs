# sracha

Fast SRA downloader and FASTQ converter, written in pure Rust.

## Features

- **Parallel downloads** -- chunked HTTP Range requests with multiple connections
- **Native VDB parsing** -- pure Rust, zero C dependencies
- **Integrated pipeline** -- download, convert, and compress in one command
- **Project-level accessions** -- pass a BioProject (PRJNA) or study (SRP) to download all runs
- **Accession lists** -- batch download from a file with `--accession-list`
- **Parallel gzip** -- pigz-style block compression via rayon
- **SRA and SRA-lite** -- full quality or simplified quality scores
- **Split modes** -- split-3, split-files, split-spot, interleaved

## Architecture

``` mermaid
graph LR
  A["sracha get SRR..."] --> B["SDL Resolve"]
  B --> C["Parallel Download"]
  C --> D["KAR Archive Parse"]
  D --> E["VDB Column Decode"]
  E --> F["FASTQ Format"]
  F --> G["Parallel Gzip"]
  G --> H["*.fastq.gz"]
```

## Quick start

```bash
# Download, convert, and compress in one shot
sracha get SRR000001

# Download all runs from a BioProject
sracha get PRJNA123456

# Batch download from an accession list
sracha get --accession-list SRR_Acc_List.txt

# Just download
sracha fetch SRR000001

# Convert a local .sra file
sracha fastq SRR000001.sra

# Show accession info
sracha info SRR000001
```

## Installation

### From binary releases

Download pre-built binaries from the
[releases page](https://github.com/rnabioco/sracha-rs/releases).

### From source

```bash
cargo install --git https://github.com/rnabioco/sracha-rs sracha
```

### With pixi

```bash
pixi global install sracha
```
