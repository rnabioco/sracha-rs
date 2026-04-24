# CLI reference

## Global options

These options can be used with any subcommand.

| Option | Description |
|--------|-------------|
| `-v, --verbose` | Increase log verbosity (`-v`, `-vv`, `-vvv`) |
| `-q, --quiet` | Suppress all output except errors |
| `--version` | Print version |
| `-h, --help` | Print help |

## Accession types

sracha accepts three types of accessions:

| Type | Prefixes | Example | Description |
|------|----------|---------|-------------|
| Run | SRR, ERR, DRR | `SRR2584863` | Single sequencing run (directly downloadable) |
| Study | SRP, ERP, DRP | `SRP123456` | Study containing multiple runs |
| BioProject | PRJNA, PRJEB, PRJDB | `PRJNA675068` | BioProject containing multiple runs |

Study and BioProject accessions are automatically resolved to their
constituent run accessions via the NCBI EUtils API.

## Accession lists

The `get`, `fetch`, and `info` commands accept `--accession-list` to read
accessions from a file (one per line). Blank lines and lines starting with
`#` are skipped. This can be combined with positional arguments.

```bash
# From a file
sracha get --accession-list SRR_Acc_List.txt

# Mixed: positional + file
sracha get SRR9999999 --accession-list more_accessions.txt
```

---

## sracha get

Download, convert, and compress SRA data in one shot.

```
sracha get [OPTIONS] [ACCESSION]...
```

### Arguments

| Argument | Description |
|----------|-------------|
| `ACCESSION` | One or more accessions (run, study, or BioProject) |

### Options

**Input / output**

| Option | Default | Description |
|--------|---------|-------------|
| `--accession-list <FILE>` | | Read accessions from a file (one per line) |
| `-O, --output-dir <DIR>` | `.` | Output directory |
| `--format <FORMAT>` | `sra` | Download format: `sra` (full quality) or `sralite` (simplified quality, smaller) |
| `-f, --force` | | Overwrite existing files |

**Sequence output**

| Option | Default | Description |
|--------|---------|-------------|
| `--split <MODE>` | `split-3` | Split mode: `split-3`, `split-files`, `split-spot`, `interleaved` |
| `--fasta` | | Output FASTA instead of FASTQ (drops quality scores) |
| `--min-read-len <N>` | | Minimum read length filter |
| `--include-technical` | | Include technical reads (skipped by default) |
| `-Z, --stdout` | | Write to stdout (stream interleaved FASTQ, auto-delete temp SRA) |

**Compression**

| Option | Default | Description |
|--------|---------|-------------|
| `--no-gzip` | | Disable gzip compression (compressed by default) |
| `--gzip-level <N>` | `6` | Gzip compression level (1-9) |
| `--zstd` | | Use zstd compression instead of gzip |
| `--zstd-level <N>` | `3` | Zstd compression level (1-22) |

**Performance**

| Option | Default | Description |
|--------|---------|-------------|
| `-t, --threads <N>` | `8` | Thread count for decode and compression |
| `--connections <N>` | `8` | HTTP connections per file |

**Download behavior**

| Option | Default | Description |
|--------|---------|-------------|
| `--no-resume` | | Disable download resume (re-download from scratch) |
| `-y, --yes` | | Confirm project downloads and large downloads (>100 GiB) |
| `--prefer-sdl` | | Skip direct S3 and resolve via the SDL API |
| `--prefer-ena` | | Try ENA FASTQ mirrors first; fall back to the NCBI SRA path if ENA has no FASTQ for the accession or its output config is incompatible with the requested split/compression |
| `--no-runinfo` | | Skip EUtils RunInfo API call (derive read structure from VDB metadata) |
| `--prefetch-depth <N>` | `2` | Number of accessions to download ahead of the decoder. Larger values hide slow networks behind decode at the cost of one extra temp SRA file per step. Multi-accession `get` only |
| `--keep-sra` | | Keep the downloaded SRA file in the output directory instead of deleting it after decode |
| `--no-progress` | | Disable progress bar |
| `--no-strict` | | Downgrade strict-fatal data-integrity anomalies (quality length mismatch, invalid quality bytes, quality overruns, paired-spot violations) from hard failures to warnings. Strict is the default. Benign-fallback counters (SRA-lite all-zero quality blobs, truncated-spot recovery) stay informational either way |

---

## sracha fetch

Download SRA files without conversion.

```
sracha fetch [OPTIONS] [ACCESSION]...
```

### Arguments

| Argument | Description |
|----------|-------------|
| `ACCESSION` | One or more accessions (run, study, or BioProject) |

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--accession-list <FILE>` | | Read accessions from a file (one per line) |
| `-O, --output-dir <DIR>` | `.` | Output directory |
| `--format <FORMAT>` | `sra` | Download format: `sra` (full quality) or `sralite` (simplified quality, smaller) |
| `--connections <N>` | `8` | HTTP connections per file |
| `--no-validate` | | Skip MD5 verification after download (verification is on by default) |
| `-f, --force` | | Overwrite existing files |
| `--no-resume` | | Disable download resume (re-download from scratch) |
| `-y, --yes` | | Confirm project downloads and large downloads (>100 GiB) |
| `--prefer-sdl` | | Skip direct S3 and resolve via the SDL API |
| `--prefer-ena` | | Fetch pre-computed FASTQ.gz from ENA's mirror instead of the SRA binary. Falls back to the NCBI path when ENA has no FASTQ for an accession |
| `--no-progress` | | Disable progress bar |

---

## sracha fastq

Convert SRA files to FASTQ (or FASTA).

```
sracha fastq [OPTIONS] <INPUT>...
```

### Arguments

| Argument | Description |
|----------|-------------|
| `INPUT` | Local `.sra` file path(s) (from `sracha fetch`) |

### Options

**Sequence output**

| Option | Default | Description |
|--------|---------|-------------|
| `--split <MODE>` | `split-3` | Split mode: `split-3`, `split-files`, `split-spot`, `interleaved` |
| `--fasta` | | Output FASTA instead of FASTQ (drops quality scores) |
| `--min-read-len <N>` | | Minimum read length filter |
| `--include-technical` | | Include technical reads (skipped by default) |
| `-Z, --stdout` | | Write to stdout (implies `--no-progress`) |

**Compression**

| Option | Default | Description |
|--------|---------|-------------|
| `--no-gzip` | | Disable gzip compression (compressed by default) |
| `--gzip-level <N>` | `6` | Gzip compression level (1-9) |
| `--zstd` | | Use zstd compression instead of gzip |
| `--zstd-level <N>` | `3` | Zstd compression level (1-22) |

**Other**

| Option | Default | Description |
|--------|---------|-------------|
| `-t, --threads <N>` | `8` | Thread count for decode and compression |
| `-O, --output-dir <DIR>` | `.` | Output directory |
| `-f, --force` | | Overwrite existing files |
| `--no-progress` | | Disable progress bar |
| `--no-strict` | | Downgrade strict-fatal data-integrity anomalies (quality length mismatch, invalid quality bytes, quality overruns, paired-spot violations) from hard failures to warnings. Strict is the default. Benign-fallback counters (SRA-lite all-zero quality blobs, truncated-spot recovery) stay informational either way |

---

## sracha info

Show accession metadata, or inspect a local `.sra` file.

```
sracha info [OPTIONS] [ACCESSION_OR_PATH]...
```

### Arguments

| Argument | Description |
|----------|-------------|
| `ACCESSION_OR_PATH` | Accession (run, study, or BioProject) or a local `.sra` file path |

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--accession-list <FILE>` | | Read accessions from a file (one per line) |
| `--prefer-ena` | | Also fetch ENA's FASTQ filereport and show it alongside the NCBI info |
| `--format <FMT>` | `table` | Output format: `table` (human-readable), `tsv`, or `csv` |

For accessions, displays file sizes, available formats, download mirrors,
and quality information. Study and BioProject accessions are resolved to
runs first.

For local file paths, opens the KAR archive directly (no network) and
prints its table of contents, schema, and metadata. Paths starting with
`~/` are expanded to `$HOME`.

`--format tsv` and `--format csv` emit a single header row followed by one
record per accession. Columns: `accession`, `archive_type` (`SRA`/`cSRA`),
`layout` (`SINGLE`/`PAIRED`/`N-read`), `nreads`, `spots`, `size_bytes`,
`platform`, `md5`. Missing fields are empty strings. Local `.sra` paths and
`--prefer-ena` are ignored with a stderr warning in these formats.

```
$ sracha info --format tsv SRR2584863 SRR14724462
accession	archive_type	layout	nreads	spots	size_bytes	platform	md5
SRR2584863	SRA	PAIRED	2	1553259	302057279	ILLUMINA	c486ca786ca83ec3cef04b7e32e1aa08
SRR14724462	SRA	PAIRED	2	41135235	4057553143	ILLUMINA	cfb98d8db26ad9ad28c501a4115f0cc5
```

---

## sracha validate

Validate SRA file integrity by decoding all records and checking for errors.

```
sracha validate [OPTIONS] <INPUT>...
```

### Arguments

| Argument | Description |
|----------|-------------|
| `INPUT` | SRA file(s) to validate |

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `-t, --threads <N>` | `8` | Thread count for decode |
| `--no-progress` | | Disable progress bar |
| `--md5 <HASH>` | | Expected MD5 hex; fail on mismatch. With multiple inputs every file must match |
| `--offline` | | Skip the SDL lookup for the expected MD5 (air-gapped use) |

---

## sracha vdb

Inspect the VDB structure of a local `.sra` file. Pure-Rust
replacement for `vdb-dump` — no network, no C FFI, no subprocess to
sra-tools.

```
sracha vdb <SUBCOMMAND> <FILE> [OPTIONS]
```

### sracha vdb info

Print summary metadata: schema, platform, table row counts, load
timestamp, and formatter / loader / update software events.

```
sracha vdb info <FILE> [--json]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--json` | | Emit a single JSON object instead of human-readable text |

### sracha vdb tables

List tables in the archive. Only meaningful for Database archives
(cSRA / aligned); flat Tables print a note.

```
sracha vdb tables <FILE>
```

### sracha vdb columns

List columns in a table.

```
sracha vdb columns <FILE> [-T TABLE] [-s]
```

| Option | Default | Description |
|--------|---------|-------------|
| `-T, --table <NAME>` | `SEQUENCE` (or first) | Table to inspect |
| `-s, --stats` | | Show row counts, blob counts, and first-blob header stats per column |

### sracha vdb meta

Dump the metadata tree (schema / stats / LOAD / SOFTWARE nodes).

```
sracha vdb meta <FILE> [-T TABLE] [-P PATH] [-d DEPTH] [--db]
```

| Option | Default | Description |
|--------|---------|-------------|
| `-T, --table <NAME>` | `SEQUENCE` | Table whose metadata tree to walk |
| `-P, --path <PATH>` | | Restrict to a sub-path like `STATS/TABLE` or `LOAD` |
| `-d, --depth <N>` | | Limit recursion depth below the chosen sub-path |
| `--db` | | Walk the database-level tree (root `md/cur`) instead of a table tree |

### sracha vdb schema

Print the embedded schema text.

```
sracha vdb schema <FILE>
```

### sracha vdb id-range

Print the first row id and row count for a table/column.

```
sracha vdb id-range <FILE> [-T TABLE] [-C COLUMN]
```

| Option | Default | Description |
|--------|---------|-------------|
| `-T, --table <NAME>` | `SEQUENCE` (or first) | Table to inspect |
| `-C, --column <NAME>` | first alphabetically | Column to read |
