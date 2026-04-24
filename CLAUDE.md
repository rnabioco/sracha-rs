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

Fast local dev builds via mold (pixi env: `dev`). `mold -run` intercepts the
linker exec via `LD_PRELOAD` and works with the compute nodes' system gcc
11.5 — no `-fuse-ld=mold` support or glibc-static needed. Release binaries
shipped via GitHub Releases are built in CI against musl targets; these
local builds remain dynamic gnu.

```bash
pixi run -e dev build          # cargo build
pixi run -e dev release        # cargo build --profile release
pixi run -e dev test           # cargo test
pixi run -e dev check          # cargo check
pixi run -e dev clippy         # cargo clippy --all-targets --all-features -- -D warnings
```

The project uses `pixi` for environment management (Rust 1.92+, Python 3.14+, sra-tools for validation reference). All the above are also available as `pixi run build`, `pixi run test`, etc. Swap `pixi run` for `pixi run -e dev` on any linking task to pick up mold.

## Workspace layout

Three crates in `crates/`:

- **`sracha`** — CLI binary. Argument parsing (`cli.rs`), command orchestration and Ctrl-C handling (`main.rs`), ANSI styling (`style.rs`). User-facing output goes to stderr via `eprintln!`; tracing goes to stderr via `tracing::info!/debug!`.
- **`sracha-core`** — Pipeline, download, FASTQ, compression, SDL/EUtils, and info-table formatting. Depends on `sracha-vdb`.
- **`sracha-vdb`** — Pure-Rust VDB/KAR parser, extracted from sracha-core. Internal-only (no semver guarantee across minor releases).

## Architecture (data flow for `sracha get`)

```
1. Accession resolution (accession.rs, sdl/mod.rs, s3.rs)
   Input accessions → resolve projects to runs via EUtils → probe S3 directly,
   fall back to SDL API → fetch RunInfo metadata (read count, lengths, platform)

2. Download (download/mod.rs)
   Parallel chunked HTTP Range requests → adaptive chunk sizing (8-64 MiB) →
   resume via .sracha-progress sidecar → MD5 validation → temp file

3. Decode + output (pipeline/mod.rs → sracha-vdb → fastq/mod.rs → compress/mod.rs)
   Open KAR archive → VdbCursor over SEQUENCE table → batch-parallel blob decode
   via rayon → format FASTQ records → parallel gzip/zstd compression → output files
```

Key design: download of accession N+1 overlaps with decode of accession N (prefetch). Blobs are decoded in batches of 1024 via rayon, then written sequentially to preserve order. Compression uses block-based parallelism with backpressure to prevent decode from outrunning I/O.

## Core modules in sracha-core

- **`pipeline/mod.rs`** (~2200 lines) — Orchestrates download→decode→output. `PipelineConfig`, `PipelineStats`, `download_sra()`, `decode_sra()`, progress bars, cancellation polling. Submodules: `blob_decode.rs`, `config.rs`, `validate.rs`.
- **`download/mod.rs`** — Parallel chunked HTTP downloads with resume support, retries with exponential backoff, adaptive chunk sizing, `pwrite` writer on a shared fd.
- **`sdl/mod.rs`** — NCBI SDL locate API client + EUtils (ESearch/EFetch) for project-to-run resolution and RunInfo metadata.
- **`ena.rs`** — ENA filereport client for the `--prefer-ena` fast path.
- **`s3.rs`** — Direct S3 HEAD probes to `sra-pub-run-odp` bucket (fast path, avoids SDL round-trip).
- **`fastq/mod.rs`** — FASTQ/FASTA formatting, split modes (split-3/split-files/split-spot/interleaved), output slot routing, quality fallback for SRA-lite.
- **`compress/mod.rs`** — Block-based parallel gzip (libdeflater) and zstd compression with backpressure queue.

## Core modules in sracha-vdb

- **`kar.rs`** — KAR archive container / TOC parser.
- **`kdb.rs`** — Column index / blob addressing.
- **`cursor.rs`** — High-level `VdbCursor` over the SEQUENCE table.
- **`blob.rs`** — Variable-length encoding, izip/iunzip/irzip decompression, page maps, row-padding helpers.
- **`blob_codecs.rs`** — Codec dispatch (zlib, bzip2, iunzip raw passthrough, etc.).
- **`cache.rs`** — Per-column decoded-blob cache with prefix-sum lookup; used by `alignment.rs`, `reference.rs`, and `csra.rs`.
- **`csra.rs`**, **`alignment.rs`**, **`reference.rs`** — cSRA (reference-compressed SRA) decode path: PRIMARY_ALIGNMENT → REFERENCE → restored SEQUENCE basecalls.
- **`metadata.rs`**, **`inspect.rs`**, **`dump.rs`** — metadata tree walking, `VdbKind` detection, and the `sracha vdb` subcommand backing.

## Key conventions

- **Error handling**: Custom `Error` enum in `error.rs` with thiserror. `Result<T>` type alias throughout. `Error::Cancelled` carries partial output file paths for cleanup. No panics in library code.
- **User output vs logging**: User-facing messages use `eprintln!` with `style::*` helpers (bold, green, cyan via owo-colors). Internal diagnostics use `tracing::info!/debug!` and only show with `-v`/`-vv`.
- **Streaming constraint**: VDB data must be processed blob-by-blob, never loaded entirely into memory — SRA files can exceed 1 GiB. `ColumnReader` is `!Send` (reads sequentially from mmap'd tempfile), so decode batches are read sequentially then processed in parallel.
- **No C dependencies**: The VDB parser is pure Rust. When investigating VDB format questions, read the ncbi-vdb C source code rather than guessing.
- **Resume support**: Downloads preserve partial files and `.sracha-progress` sidecars on interruption. FASTQ conversion has no checkpoints — partial outputs are deleted on Ctrl-C, temp SRA file is kept.

## Testing

Unit tests are inline in each module. Integration tests in `crates/sracha-core/tests/pipeline.rs` are `#[ignore]`d by default (they download real SRA files from NCBI). Fixtures are cached in `tests/fixtures/`. The `validation/` directory contains shell scripts that compare sracha output against sra-tools reference output.
