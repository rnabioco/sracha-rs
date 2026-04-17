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
| `--no-runinfo` | | Skip EUtils RunInfo API call (derive read structure from VDB metadata) |
| `--no-progress` | | Disable progress bar |

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
| `--validate` | | Verify MD5 after download |
| `-f, --force` | | Overwrite existing files |
| `--no-resume` | | Disable download resume (re-download from scratch) |
| `-y, --yes` | | Confirm project downloads and large downloads (>100 GiB) |
| `--prefer-sdl` | | Skip direct S3 and resolve via the SDL API |
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

---

## sracha convert

Convert SRA file(s) to columnar formats — Apache Parquet or Vortex.
Experimental; aimed at storage benchmarking and downstream analytics
workflows. See [Columnar Formats](columnar.md) for format trade-offs,
DNA packing options, and benchmark numbers.

```
sracha convert [OPTIONS] <INPUT>...
```

!!! note
    `sracha convert` is only available when the binary is built with the
    `parquet` and/or `vortex` Cargo features. Both are enabled by default
    (release binaries, bioconda). A `--no-default-features` build must
    opt in.

### Arguments

| Argument | Description |
|----------|-------------|
| `INPUT` | Local `.sra` file path(s) (from `sracha fetch`) |

### Options

**Output**

| Option | Default | Description |
|--------|---------|-------------|
| `--format <FORMAT>` | `parquet` | Output format: `parquet` (`.parquet`) or `vortex` (`.vortex`) |
| `-O, --output-dir <DIR>` | `.` | Output directory |
| `-f, --force` | | Overwrite existing files |

**Encoding**

| Option | Default | Description |
|--------|---------|-------------|
| `--pack-dna <MODE>` | format-dependent | DNA packing: `ascii`, `two-na`, `four-na` (default: `two-na` for parquet, `ascii` for vortex) |
| `--length-mode <MODE>` | `auto` | Read-length schema: `auto` (detect from data), `fixed` (force FixedSizeBinary), `variable` (force variable-length) |

**Compression** (parquet only)

| Option | Default | Description |
|--------|---------|-------------|
| `--compression <CODEC>` | `zstd` | Page-level codec: `none`, `snappy`, `zstd` |
| `--zstd-level <N>` | `22` | Zstd level (1-22), used when `--compression zstd` |

**Advanced** (parquet only)

| Option | Default | Description |
|--------|---------|-------------|
| `--row-group-mib <N>` | `256` | Target row-group size in MiB |

The parquet-only flags are accepted but unused when `--format vortex`.
Vortex picks its own encoding cascade — there are no compression knobs
to turn.

---

## sracha info

Show accession metadata.

```
sracha info [OPTIONS] [ACCESSION]...
```

### Arguments

| Argument | Description |
|----------|-------------|
| `ACCESSION` | One or more accessions (run, study, or BioProject) |

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--accession-list <FILE>` | | Read accessions from a file (one per line) |

Displays file sizes, available formats, download mirrors, and quality
information for each accession. Study and BioProject accessions are
resolved to runs first.

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
