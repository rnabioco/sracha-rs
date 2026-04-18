# Changelog

## Unreleased

### Added

- **MD5 verification by default**: Downloads verify MD5 against SDL-reported
  hashes, decoded blobs verify per-blob MD5 and CRC32, and spot counts are
  cross-checked against RunInfo. Use `fetch --no-validate` to skip.
- **`sracha validate --md5 <HASH>` / `--offline`**: Check a file against an
  explicit MD5 or skip the SDL lookup for air-gapped use.
- **Local SRA files in `sracha info`**: Pass a `.sra` file path (including
  `~/...`) to print the table of contents, schema, and metadata without
  hitting NCBI.
- **Resolution spinners**: `get`, `fetch`, and `info` show progress while
  resolving projects and accessions.

### Changed

- **Silent decode corruption**: CRC32/MD5 mismatches and truncated
  variable-length columns now abort with an error instead of producing
  partial rows.
- **Download resume hardening**: Range requests validate `Content-Range` and
  track expected MD5 in `.sracha-progress`, catching servers that ignore
  ranges or files replaced mid-transfer.
- **Verbosity defaults**: Default log level hides `INFO`; use `-v` for `INFO`,
  `-vv` for `DEBUG`, `-vvv` for `TRACE`.

### Fixed

- **CRC32 computation**: Per-blob CRC32 validation used the standard
  CRC-32/ISO-HDLC (crc32fast) and disagreed with the variant emitted by
  ncbi-vdb (MSB-first polynomial 0x04C11DB7, init=0, no reflection, no
  final XOR). Previously the mismatch was swallowed; now that it's an
  error, decode would have spuriously rejected real SRA files. Replaced
  with a conforming implementation.
- **Aligned SRA / cSRA hang**: Extended cSRA rejection to cover the
  `bam-load`-style variant — files with a physical `SEQUENCE/col/READ`
  column but an `NCBI:align:db:...` schema that synthesizes
  `READ_LEN`/`READ_TYPE` through ncbi-vdb's schema-aware virtual cursor
  (e.g. SRR14724462). Without that cursor the decode fell through to
  fixed-length heuristics and wedged the pipeline. The existing
  CMP_READ/PRIMARY_ALIGNMENT path and the new schema-based path now
  return one unified `UnsupportedFormat` error pointing to
  `fasterq-dump`. A matching "Not yet supported" entry was added to the
  docs.

## 0.1.10 (2026-04-16)

### Added

- **Completion markers**: `get` writes `.sracha-done` markers so a second
  invocation with the same output skips re-download and re-decode.
- **`--format sra|sralite`**: Select full SRA or SRA-lite encoding via the
  SDL capability parameter.

### Changed

- **CLI reorganization**: Commands and flags grouped semantically under
  help headings for clearer `--help` output.
- **Strict flag validation**: Contradictory CLI flag combinations now error
  out instead of silently picking one.

### Fixed

- **Ctrl-C cleanup in stdout mode**: Interrupting `-Z` streaming now
  deletes the temp SRA file and prints the correct cancellation message.
- **Version string**: Release builds between tags now include the git SHA.
- **`--threads` help text**: Remove doubled `[default: 8]`.
- **Docs**: Size-gate threshold updated to 100 GiB; stdout streaming
  feature documented.
- **`fastq` / `get` help text**: Clarify accession wording in `fastq`
  subcommand; mention `-Z` in `get` docs.

## 0.1.9 (2026-04-16)

### Added

- **Stdout streaming**: New `-Z` flag streams FASTQ output to stdout for
  piping into downstream tools. (#7)
- **75 new tests**: Unit and integration tests covering previously untested
  modules.
- **Acknowledgments**: Added acknowledgments for NCBI and SRA Toolkit team.
- **Alignment docs page**: New documentation page covering alignment topics.

### Changed

- **VDB metadata read structure**: Read structure (count, lengths, platform)
  is now derived from VDB table metadata, making the EUtils RunInfo fetch
  optional and improving reliability for accessions with missing RunInfo.
- **Tabled output**: `info` and `validate` commands now use `tabled` for
  formatted table output.
- **Remove dead `--format` flag**: Removed unused `--format` argument; wired
  up `--no-resume` for the `get` command.

### Fixed

- **Interleaved output routing**: Fixed a bug in interleaved split mode
  output routing and corrected the `min_read_len` test.

## 0.1.8 (2026-04-15)

### Changed

- **Project downloads require confirmation**: Downloads from project accessions
  (SRP/ERP/DRP/PRJNA/PRJEB/PRJDB) now always require `--yes` / `-y` to proceed,
  preventing surprise multi-hundred-GiB downloads. The info table is shown for
  all project downloads so users can review what they're about to download.
- **Lower size confirmation threshold**: The size gate for non-project downloads
  was lowered from 500 GiB to 100 GiB.

### Added

- **Disk space check**: Downloads now check available disk space in the target
  directory before starting and bail with a clear error if there isn't enough
  room.

## 0.1.7 (2026-04-15)

### Fixed

- **PacBio sequence accuracy**: Replace quality-based N-masking with ALTREAD
  4na ambiguity merge, matching the VDB schema's `bit_or(2na, .ALTREAD)`
  derivation. PacBio SRR38107137 drops from 680 to 0 sequence mismatches and
  9,324 to 0 quality mismatches vs fasterq-dump. Illumina output remains
  byte-identical. Closes #4.

## 0.1.6 (2026-04-15)

### Added

- **Dev version strings**: Non-release builds now show git SHA and dirty flag
  (e.g. `0.1.6-dev+abc1234.dirty`) via a build script.
- **cSRA rejection**: Detect aligned SRA (cSRA) archives and return an
  actionable error pointing users to fasterq-dump.

### Changed

- **Benchmarks**: Updated README benchmarks to 8-core results with v0.1.5.
- **Integration tests**: Switched from LS454 fixture (SRR000001) to Illumina
  (SRR28588231) after adding legacy platform rejection.

### Fixed

- **Clippy**: Fixed collapsible-if and manual-contains warnings from Rust 1.94.
- **PacBio quality decode**: Expand page map data_runs for variable-length rows.

## 0.1.5 (2026-04-14)

### Added

- **Benchmarks**: Added `validation/benchmark.sh` script comparing sracha
  against fastq-dump and fasterq-dump, and added benchmark results to README.
- **Graceful Ctrl-C handling**: The `get` command now cancels in-flight
  downloads cleanly on SIGINT.

### Changed

- **Progress bars**: Switched to Unicode thin-bar style and extracted shared
  progress bar helper.
- **MIT license**: Added LICENSE file.

### Fixed

- **Cursor tests**: Fixed temp file name collision in parallel cursor tests.

## 0.1.4 (2026-04-14)

### Performance

- **Gzip backpressure**: `ParGzWriter` now blocks when too many blocks are
  pending, preventing the decode loop from outrunning compression. Eliminates
  a multi-second `finish()` stall and reduces overall decode+gzip time by ~47%
  (19s to 10s on SRR000001).

## 0.1.3 (2026-04-14)

### Performance

- **Thread-local compressor reuse**: Gzip compression reuses libdeflater
  `Compressor` and output buffer across blocks via thread-local storage,
  avoiding ~300 KiB malloc/free per 256 KiB block.
- **Cap gzip thread pool**: Compression pool threads are now capped at
  `available_parallelism()` to prevent oversubscription.
- **Lazy quality fallback buffer**: The lite quality buffer is only allocated
  when quality data is actually missing, skipping ~300 KiB per blob in the
  common case.
- **Inline izip type 0 reads**: Eliminated intermediate `Vec<i64>` allocations
  in izip decode by reading packed values directly from raw buffers during
  output reconstruction.
- **Zero-copy blob data**: `DecodedBlob` now borrows data directly from
  mmap'd slices via `Cow<'a, [u8]>`, eliminating ~9% of heap allocations.
- **Multi-accession download prefetch**: When processing multiple accessions,
  the next file's download starts while the current one is being decoded,
  overlapping network and CPU.

### Changed

- Added `profiling` cargo profile (optimized, no LTO) for heap profiling
  with valgrind/dhat.

### Fixed

- **Illumina tile boundaries**: Fixed skey id2ord delta unpacking to use
  big-endian bitstream order matching ncbi-vdb's `Unpack` function. Tile
  assignments at spot boundaries are now correct. Also fixed `span_bits`
  header offset for v2 index files. Closes #3.
- **Per-spot template selection**: Name templates are now looked up per spot
  (not per blob), so tile transitions within a blob produce correct deflines.
- **Fixed spot length for v1 blobs**: When READ_LEN is absent, the v1 blob
  header `row_length` is now used as a fallback for fixed spot length detection,
  enabling correct spot splitting without API access.
- **irzip v3 dual-series decoding**: Implemented the series_count=2 path for
  irzip decompression, fixing X/Y coordinate decoding for blobs that use
  interleaved dual-series delta encoding.
- **X/Y page map expansion**: X and Y column values are now expanded via
  page map data runs, matching the existing READ_LEN expansion logic.

## 0.1.2 (2026-04-14)

### Added

- **Direct S3 fetch**: Downloads now probe the NCBI SRA Open Data S3 bucket
  directly, skipping the SDL API round-trip. Falls back to SDL automatically
  when the direct URL is unavailable (old/non-public accessions). Stable URLs
  also improve resume reliability vs. expiring presigned SDL URLs. Use
  `--prefer-sdl` to opt out.

### Changed

- **Simplify KAR/VDB parsing**: Unified duplicated PBSTree parsers across
  `kar.rs` and `metadata.rs` into a single shared implementation. Removed dead
  code (unused metadata children parsing, leftover debug logging), eliminated
  unnecessary temporary allocations in idx2 block decoding, and moved test-only
  functions (`unpack`, `read_blob_for_row`) behind `#[cfg(test)]`. Net reduction
  of ~220 lines with identical output.
- **Batch API calls for `info` and `get`**: Multi-accession and project queries
  now resolve all runs in 2 HTTP requests (1 SDL + 1 EUtils) instead of 2N
  sequential calls. Significantly faster for projects with many runs.
- **Improved error messages**: Not-found accessions now include an NCBI search
  link to help verify the accession exists.

## 0.1.1 (2026-04-13)

### Added

- **FASTA output mode**: `--fasta` flag on `fastq` and `get` commands outputs
  `>defline\nsequence\n` records instead of FASTQ. Skips quality column decode
  entirely for faster conversion when quality scores are not needed.
- **zstd compression**: `--zstd` flag on `fastq` and `get` commands uses zstd
  compression instead of gzip. Native multi-threaded compression via the zstd
  crate. Configurable level with `--zstd-level` (1-22, default 3). Produces
  `.fastq.zst` or `.fasta.zst` output files.
- **`validate` subcommand**: `sracha validate <file.sra>` verifies SRA file
  integrity by opening the KAR archive, parsing the SEQUENCE table, and
  decoding all blobs in parallel without producing output. Reports columns
  found, spot/blob counts, and any decode errors. Exits with code 1 on failure.
- **Resume interrupted downloads**: Downloads now resume automatically.
  Completed files are skipped (verified by size + MD5). Parallel chunked
  downloads track progress in a `.sracha-progress` sidecar file; on retry,
  only incomplete chunks are re-downloaded. Single-stream downloads resume
  via HTTP Range. Use `--no-resume` to force a fresh download.

### Changed

- Compression is now configured via a `CompressionMode` enum (`None`, `Gzip`,
  `Zstd`) instead of separate `--gzip` / `--no-gzip` boolean flags. Existing
  flag behavior is preserved: gzip is the default, `--no-gzip` disables
  compression, `--zstd` selects zstd.
- `sracha get` temp downloads now preserve partial files on failure for
  automatic resume on the next attempt.

## 0.1.0 (2026-04-13)

### Added

- **Project-level accessions**: `sracha get PRJNA675068` and `sracha get SRP123456`
  resolve study/BioProject accessions to constituent runs via NCBI EUtils API.
- **Accession list input**: `--accession-list` flag on `get`, `fetch`, and `info`
  reads accessions from a file (one per line, `#` comments supported).
- **Illumina name reconstruction**: Deflines now include the original Illumina
  read name (instrument:run:flowcell:lane:tile:X:Y) reconstructed from the
  skey index and physical X/Y columns.
### Fixed

- **Quality string corruption**: Fixed three bugs that could produce invalid
  FASTQ quality strings causing STAR alignment failures:
  - ASCII quality heuristic now validates all bytes, not just the first 100.
  - Quality offset tracking always advances in the fallback path.
  - `format_read` validates quality length matches sequence and sanitizes
    invalid bytes (outside Phred+33 range [33, 126]).
- **N base handling**: Bases with quality <= Phred 2 are now emitted as `N`,
  matching the NCBI convention for Illumina no-call bases in 2na encoding.
- **Defline format**: Output now matches fasterq-dump format
  (`@RUN.SPOT_NUM DESCRIPTION length=LEN`) with the `+` line repeating the
  full defline.

