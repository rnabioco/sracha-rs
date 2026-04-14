# Changelog

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

- **Project-level accessions**: `sracha get PRJNA123456` and `sracha get SRP123456`
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

### Known Issues

- Illumina tile assignment in deflines has boundary offsets for some spots near
  tile transitions due to incomplete skey id2ord delta unpacking. Sequence and
  quality data are unaffected. See #3.
