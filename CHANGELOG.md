# Changelog

## Unreleased

### Features

- `sracha get --stream`: pipe FASTQ to stdout with the `.sra` materialized
  into anonymous memory (Linux `memfd_create`, NamedTempFile fallback
  elsewhere). Output is byte-identical to the disk-backed `-Z` path.
  Enables `sracha get --stream ACC | bwa mem ref - | samtools sort > out.bam`.
- `sracha get --catalog DIR`: skip the S3 HEAD probe + SDL round-trip
  on accessions present in a hosted Vortex catalog.
- New `sracha-index` crate + `sracha index` CLI: extract per-accession
  metadata (file_size, kar_data_offset, schema, n_blobs, platform,
  layout, spots, read_lengths, md5) from the KAR header and
  `tbl/SEQUENCE/md/cur` only — never the full SRA. 5500 accessions
  → ~140 KB total (~48 B/accession). Multi-shard manifest +
  base/delta append; predicate-pushdown reader (~40 ms open,
  ~50 ms lookup, constant in catalog size).
- Streaming-decode plumbing: `ChunkReadyTracker` for per-chunk
  readiness, column-priority chunk hint, multi-chunk per-batch
  wait, streaming MD5 (~20% faster on 16 GiB downloads), single
  combined work bar.

## 0.3.4 (2026-04-25)

### Fixes

- Bound header-driven allocations to prevent SIGABRT on SRA-Lite
  quality blobs (#30). All 8 flagged accessions in PRJNA542889
  decode under `ulimit -v 4000000`.
- Decode random-access variant-2 page maps by reading the trailing
  `data_offset[row_count]` overlay into `data_runs`. 6 PASS_CONTENT →
  PASS_MD5 in the 100-accession corpus (DRR040793, DRR050206,
  DRR036255, DRR036514, DRR040777, DRR041132).
- Align READ_LEN with READ by row id rather than blob index. Fixes
  truncation on archives where the two columns have mismatched blob
  counts; DRR023226 and DRR023232 go from FAIL_COUNT to PASS_MD5.
- Read skey templates directly from the offset-indexed string table
  and loosen projection-count matching, replacing the byte-scan +
  dedup heuristics. DRR035881 and DRR026998 reach PASS_MD5.
- Support skey on flat-table archives (DRR019046) and trim adjacent-
  template prefix bytes that the backward `$X` walk swept into the
  next template (DRR053011). ~44 PASS_CONTENT → PASS_MD5 in the
  random corpus.
- Treat ALTREAD raw-passthrough zip blobs (no ops/args, header
  `osize` == on-disk size) as data instead of failing decode. Fixes
  DRR019046's lost trailing-N annotations.

### Features

- NAME_FMT column support: per-spot template overrides reproduce
  fine-grained tile interleave on HiSeq archives (DRR040793-class)
  that the skey range mapping can't capture. DRR002715 and DRR021982
  newly byte-identical.
- Emit `/N` mate suffix in interleaved and split-spot output for
  fasterq-dump byte parity in single-stream mode. Split-3 /
  split-files paths unchanged.
- `--stream` mode for `validation/random_corpus.sh`: pipe both
  decoders through `md5sum` instead of writing FASTQs to disk.
  4.2× faster (13.6k → 3.3k s on the 100-accession corpus).

## 0.3.3 (2026-04-24)

### Fixes

- **ALTREAD variable-row padding for N-mask byte-identity**:
  `apply_altread_merge` was calling `pad_trimmed_rows_fixed` with a
  uniform `row_bases = actual_bases / read_id_range` — the average row
  length. On Illumina runs with adapter-trimmed reads (per-row base
  counts differing by 10–200 bases) any stored record whose trimmed
  size exceeded the average errored inside the fixed-pad helper, the
  merge silently skipped, and ALTREAD's 4na N annotations leaked
  through as raw 2na bases — the `N_MASK_ONLY` divergences the
  mismatch-report harness (#26) captured on DRR035183, SRR33907345,
  and every `FAIL_SEQ` accession reclassified after PR #24.
  New `PageMap::pad_trimmed_rows_variable` takes per-logical-row
  targets so each padded row matches its READ row's true width;
  `apply_altread_merge` threads READ's page_map through and feeds its
  expanded per-row widths in whenever ALTREAD and READ rows align
  1:1. The old fixed path remains the fallback for mismatched-blob-
  size layouts (DRR035866's 2:1 ALTREAD-blob case). Verified 100.0%
  `IDENTICAL` on DRR035183 and SRR33907345 vs `fasterq-dump` 3.2.1
  (previously 73.7% / 94.5% `N_MASK_ONLY` on DRR035183).
- **READ 2na `data_runs` expansion for variable-length rows (#22)**:
  when a READ blob's page map has a non-empty `data_runs` run-length
  table, consecutive stored rows with identical 2na bytes are written
  once and replicated on read. The expansion path previously
  short-circuited whenever `lengths` wasn't uniform, silently
  dropping the duplicated row and producing a `SpotCountMismatch`
  plus asymmetric paired output. SRR33907345 blob 46 is the in-tree
  repro: 4,095 stored rows with variable 70–502-base lengths
  covering 4,096 logical rows via one `data_runs[i]=2` entry. The
  decoder now delegates to `PageMap::expand_variable_data_runs` —
  same path the QUALITY column already uses — which handles both
  uniform and variable per-row lengths correctly. Covered by the
  new `variable_length_data_runs_spot_count` regression test.

### Refactors

- **CLI utilities moved to `sracha-core`**: `thousands` and
  `format_bases` live in `sracha_core::util` alongside `format_size`;
  `InfoEntry` and the TSV/CSV writer moved into a new
  `sracha_core::info` module with dedicated unit tests. The
  `tabled`-rendered human `sracha info` table stays in the CLI crate.
  Drops ~150 lines from `sracha/src/main.rs`.
- **Izip type-0 reconstruction readability**: introduced
  `NbufStream` in `sracha-vdb::blob` to bundle
  `(data, variant, min, name)` so the reconstruction loop reads
  naturally (`stream.read(idx)?`) and out-of-bounds errors identify
  which buffer (length / outlier / dx / dy / a / diff / simple) was
  truncated.

### Documentation

- `docs/cli.md` documents `--prefer-ena` on `sracha get` and
  `sracha fetch`; `docs/getting-started.md` covers the ENA fast path,
  strict-integrity default / `--no-strict`, cSRA decoding,
  `--prefetch-depth`, and `--keep-sra`.
- Removed the orphan `docs/implementation.md` page; cSRA notes live
  in `docs/internal/csra-format-notes.md` for developers.
- `CLAUDE.md` updated for the three-crate workspace; prior doc
  described a two-crate layout and hid `sracha-vdb`.

## 0.3.2 (2026-04-24)

### Fixes

- **iunzip raw-passthrough decode (#20)**: some v2 iunzip blobs — seen
  on long-read ENA archives like ERR15141550 — carry `osize ==
  data.len()` with no `ops`/`args` because the encoder skipped the
  bit-plane + deflate step. `decode_irzip_column` now detects this
  shape and returns the bytes verbatim instead of force-routing them
  through `irzip_decode` with a default `planes = 0xFF` and failing
  with "corrupt deflate stream". Verified byte-identical against
  `fasterq-dump --split-3` on ERR15141550 (MD5
  `a063af39f57e9a09edae697fc99674a1`).
- **Writer-closure capture deadlock**: when a decode blob returned
  `Err`, the `decode_and_write` writer thread's early return left
  `batch_rx` alive in the parent stack frame (borrow-capture), so the
  decode loop deadlocked on a full `batch_tx.send()` instead of
  propagating the error. Writer now takes `batch_rx` by `move`; the
  error surfaces cleanly to the caller.
- **Decoder bounds hardening**: `nbuf_read`, `decode_types`, and the
  `izip_decode` segment reconstruction loop now return
  `Error::Format` on out-of-bounds / misaligned buffers instead of
  panicking a rayon worker.
- **KAR magic prefix probe on cached skip**: `download_file` accepts
  an optional `expected_prefix`; when the cached `.sracha-tmp-*.sra`
  matches on size but SDL gave no MD5 (multipart upload), sracha now
  verifies the first 8 bytes are `NCBI.sra` before skipping the
  download. Closes a secondary path from #20 where a stale temp file
  from a crashed prior run fed garbage into the decoder.

## 0.3.1 (2026-04-19)

### Performance

- **pwrite download writer + read_timeout**: per-chunk writer now sends
  hyper pieces over a bounded `mpsc` to a single `spawn_blocking` task
  doing positional `write_all_at` on a sync `std::fs::File`, avoiding
  tens of thousands of blocking-pool round-trips per download. Added a
  15 s `read_timeout` and 10 s `connect_timeout` to the reqwest client
  so a single stalled TCP connection no longer sets the floor for the
  whole parallel download; retry backoff tightened from 2 s/4 s to
  250 ms/500 ms. Post-fix on compute18: baseline 10.2 s for 288 MiB,
  slow runs capped at ~15 s (previously unbounded).

### Benchmarks / docs

- **End-to-end benchmark stage**: new `e2e` sbatch array index times
  the full accession → FASTQ workflow (`sracha get` vs `prefetch +
  fasterq-dump` vs `prefetch + fastq-dump`) on SRR28588231 and
  SRR2584863.
- **`pixi run install-sratools`**: pins the reference toolkit
  (default sra-tools 3.4.1) into `validation/sra-tools/`;
  `benchmark.sh` auto-discovers the newest installed version.
- **README refreshed against sra-tools 3.4.1** on the head node (stable
  S3): 11.6× / 4.5× / 4.4× local decode; `sracha get` 2.9× faster than
  `prefetch + fasterq-dump` on the small accession and 1.55× on the
  288 MiB medium.

## 0.3.0 (2026-04-19)

### Added

- **Broader `sracha vdb dump` column coverage**: name-based heuristic
  picks up per-row scalars (`PLATFORM`, `NREADS`, `SPOT_FILTER`,
  `SPOT_ID`, `TRIM_LEN`, `TRIM_START`, `CLIP_QUALITY_LEFT/RIGHT`),
  per-read arrays (`LABEL_LEN`, `LABEL_START`, `POSITION`, `RD_FILTER`),
  and ASCII templates (`CS_KEY`, `NAME_FMT`) in addition to the
  existing SEQUENCE columns. New `U8Scalar` / `U32Scalar` cell kinds
  render scalars as single numbers instead of one-element arrays. A
  hidden `--raw` flag bypasses type inference and hex-dumps every
  column — useful for debugging layouts the heuristic doesn't
  recognize. Closes #12.
- **Reference-compressed cSRA (aligned SRA) decode**: archives with a
  physical `SEQUENCE/col/CMP_READ` plus sibling `PRIMARY_ALIGNMENT` +
  `REFERENCE` tables are now decoded in pure Rust —
  `NCBI:align:seq_restore_read` and `NCBI:align:align_restore_read`
  are both reimplemented (see `vdb/restore.rs`). `sracha fastq` on a
  cSRA file produces output byte-identical to `fasterq-dump`
  (validated against ncbi-vdb's `VDB-3418.sra` test fixture, 985
  spots / ~36 Mbp in ~4 s release). Platform-agnostic; long-read and
  short-read aligned archives both work. Split / compression / stdout
  flags and parallel decode (`-t N`) all go through the existing FASTQ
  writer.
- **vdbcache-aware cSRA reader**: `CsraCursor::open_any` routes each
  sub-cursor (AlignmentCursor, ReferenceCursor) to whichever archive
  carries its table. `sracha fetch` downloads the `.sra.vdbcache`
  sidecar alongside the main `.sra` whenever SDL advertises one.
- **Narrowed `reject_if_csra`**: the iter-4 rule rejected any archive
  with aligned schema + `CMP_BASE_COUNT > 0` + no `unaligned` marker.
  Those archives still carry a full physical READ column in practice
  and decode cleanly through the plain VdbCursor path; validated on
  9 of the 10 past-rejected archives from prior random-corpus runs
  (DRR017176, DRR027259, DRR027597, DRR032355, DRR040407, DRR040559,
  DRR041303, DRR045227, DRR045255, DRR045332).
- **`validation/random_corpus.sh --aligned`**: targets WGS /
  BAM-loaded accessions via the ENA portal, passed through to
  `sample_accessions.sh`.
- **Actionable errors for known-unsupported cSRA shapes**: external
  refseq fetch (REFERENCE without embedded CMP_READ; SRR341578-class)
  and fixed-length SEQUENCE without physical READ_LEN both surface
  clear "decode with fasterq-dump for now" messages instead of opaque
  `column header (idx1) not found` diagnostics.

### Fixed

- **`spots_before` race across BATCH_SIZE=1024 boundaries**: the decode
  loop used to read `spots_read` atomically into per-batch cumulative
  offsets, racing with the writer thread across the bounded channel.
  Archives with > 1024 blobs (e.g. DRR045255) silently reset the FASTQ
  defline spot number to 1 at the 1,048,577th spot. Now tracked
  locally in the decode loop using blob metadata only.
- **page_map random-access offset unit**: variable-length integer
  columns with `row_length > 1` sometimes carry u32-indexed `data_runs`
  (rather than entry-indexed). Adaptive dispatch tries entry-index
  first and falls back to u32-index when the max offset would overflow
  the decoded buffer. Unblocks DRR045255's READ_LEN blob at row ~1 M.

## 0.2.0 (2026-04-18)

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

