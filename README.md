# sracha

Fast SRA downloader and FASTQ converter, written in pure Rust.

**Status: early development** -- download, VDB parsing, and FASTQ conversion are functional.

## Features

- **Parallel downloads** -- chunked HTTP Range requests with multiple connections
- **Native VDB parsing** -- pure Rust, zero C dependencies
- **Integrated pipeline** -- download, convert, and compress in one command
- **Parallel gzip** -- pigz-style block compression via rayon
- **SRA and SRA-lite** -- full quality or simplified quality scores
- **Split modes** -- split-3, split-files, split-spot, interleaved

## Quick start

```bash
# Show accession info
sracha info SRR000001

# Download, convert, and compress
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

```mermaid
flowchart LR
    A[sracha get SRR000001] --> B[SDL Resolve]
    B -->|mirror URLs| C[Parallel Download]
    C -->|".sra file"| D[KAR Archive Parse]
    D --> E[VDB Column Decode]
    E -->|"READ, QUALITY,\nREAD_LEN, NAME"| F[FASTQ Format]
    F --> G[Parallel gzip]
    G --> H["_1.fastq.gz\n_2.fastq.gz"]

    style A fill:#4a9eff,color:#fff
    style H fill:#2ecc71,color:#fff
```

## License

MIT
