# sracha

Fast SRA downloader and FASTQ converter, written in pure Rust.

**Status: early development** -- download and VDB parsing work, FASTQ output coming soon.

## Features

- **Parallel downloads** -- chunked HTTP Range requests with multiple connections
- **Native VDB parsing** -- pure Rust, zero C dependencies
- **Integrated pipeline** -- download, convert, and compress in one command
- **Parallel gzip** -- pigz-style block compression via rayon
- **SRA and SRA-lite** -- full quality or simplified quality scores
- **Split modes** -- split-3, split-files, split-spot, interleaved

## Quick start

```bash
# Show accession info (works now)
sracha info SRR000001

# Download, convert, and compress (WIP)
sracha get SRR000001
```

## Building

Requires Rust 1.92+.

```bash
cargo build --release
```

Or with pixi:

```bash
pixi run release
```

## Architecture

```
sracha get SRR000001
  |
  +-- SDL resolve (NCBI API) --> mirror URLs
  +-- Parallel download (8 connections, HTTP Range)
  +-- KAR archive parse (container format)
  +-- VDB column decode (idx1/idx2 blob index, page maps)
  +-- FASTQ format (split-3, interleaved, etc.)
  +-- Parallel gzip (rayon + flate2)
  |
  --> SRR000001_1.fastq.gz, SRR000001_2.fastq.gz
```

## License

MIT
