# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project is

sracha-rs is a pure Rust SRA downloader and FASTQ converter — a fast replacement for NCBI's sra-tools (fasterq-dump/fastq-dump). It parses the VDB/KAR binary format natively (no C FFI, no subprocess calls to sra-tools or ncbi-vdb) and achieves 3-7.5x speedups through parallel HTTP downloads, streaming VDB decode, and integrated parallel compression.

## Build and test commands

```bash
cargo build                                          # dev build
cargo build --profile release                        # optimized release build (LTO)
cargo test                                           # unit tests only
cargo test -p sracha-core -- --ignored               # integration tests (downloads SRA fixtures from NCBI)
cargo test -p sracha-core -- test_name               # run a single test
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
```

The project uses `pixi` for environment management (Rust 1.92+, Python 3.14+, sra-tools for validation reference). All the above are also available as `pixi run build`, `pixi run test`, etc.

## Workspace layout

Two crates in `crates/`:

- **`sracha`** — CLI binary. Argument parsing (`cli.rs`), command orchestration and Ctrl-C handling (`main.rs`), ANSI styling (`style.rs`). User-facing output goes to stderr via `eprintln!`; tracing goes to stderr via `tracing::info!/debug!`.
- **`sracha-core`** — Library. All domain logic lives here.

## Architecture (data flow for `sracha get`)

```
1. Accession resolution (accession.rs, sdl/mod.rs, s3.rs)
   Input accessions → resolve projects to runs via EUtils → probe S3 directly,
   fall back to SDL API → fetch RunInfo metadata (read count, lengths, platform)

2. Download (download/mod.rs)
   Parallel chunked HTTP Range requests → adaptive chunk sizing (8-64 MiB) →
   resume via .sracha-progress sidecar → MD5 validation → temp file

3. Decode + output (pipeline/mod.rs → vdb/ → fastq/mod.rs → compress/mod.rs)
   Open KAR archive → VdbCursor over SEQUENCE table → batch-parallel blob decode
   via rayon → format FASTQ records → parallel gzip/zstd compression → output files
```

Key design: download of accession N+1 overlaps with decode of accession N (prefetch). Blobs are decoded in batches of 1024 via rayon, then written sequentially to preserve order. Compression uses block-based parallelism with backpressure to prevent decode from outrunning I/O.

## Core modules in sracha-core

- **`pipeline/mod.rs`** (~2000 lines) — Orchestrates download→decode→output. `PipelineConfig`, `PipelineStats`, `download_sra()`, `decode_sra()`, progress bars, cancellation polling.
- **`download/mod.rs`** — Parallel chunked HTTP downloads with resume support, retries with exponential backoff, adaptive chunk sizing.
- **`vdb/`** — Pure Rust VDB format parser. `kar.rs` (KAR archive/TOC), `kdb.rs` (column index/blob addressing), `cursor.rs` (high-level SEQUENCE table cursor), `blob.rs` (variable-length encoding, izip decompression, page maps), `metadata.rs` (table metadata).
- **`sdl/mod.rs`** — NCBI SDL locate API client + EUtils (ESearch/EFetch) for project-to-run resolution and RunInfo metadata.
- **`s3.rs`** — Direct S3 HEAD probes to `sra-pub-run-odp` bucket (fast path, avoids SDL round-trip).
- **`fastq/mod.rs`** — FASTQ/FASTA formatting, split modes (split-3/split-files/split-spot/interleaved), output slot routing, quality fallback for SRA-lite.
- **`compress/mod.rs`** — Block-based parallel gzip (libdeflater) and zstd compression with backpressure queue.

## Key conventions

- **Error handling**: Custom `Error` enum in `error.rs` with thiserror. `Result<T>` type alias throughout. `Error::Cancelled` carries partial output file paths for cleanup. No panics in library code.
- **User output vs logging**: User-facing messages use `eprintln!` with `style::*` helpers (bold, green, cyan via owo-colors). Internal diagnostics use `tracing::info!/debug!` and only show with `-v`/`-vv`.
- **Streaming constraint**: VDB data must be processed blob-by-blob, never loaded entirely into memory — SRA files can exceed 1 GiB. `ColumnReader` is `!Send` (reads sequentially from mmap'd tempfile), so decode batches are read sequentially then processed in parallel.
- **No C dependencies**: The VDB parser is pure Rust. When investigating VDB format questions, read the ncbi-vdb C source code rather than guessing.
- **Resume support**: Downloads preserve partial files and `.sracha-progress` sidecars on interruption. FASTQ conversion has no checkpoints — partial outputs are deleted on Ctrl-C, temp SRA file is kept.

## Testing

Unit tests are inline in each module. Integration tests in `crates/sracha-core/tests/pipeline.rs` are `#[ignore]`d by default (they download real SRA files from NCBI). Fixtures are cached in `tests/fixtures/`. The `validation/` directory contains shell scripts that compare sracha output against sra-tools reference output.
