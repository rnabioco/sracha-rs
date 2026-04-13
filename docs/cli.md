# CLI reference

## Accession types

sracha accepts three types of accessions:

| Type | Prefixes | Example | Description |
|------|----------|---------|-------------|
| Run | SRR, ERR, DRR | `SRR1234567` | Single sequencing run (directly downloadable) |
| Study | SRP, ERP, DRP | `SRP123456` | Study containing multiple runs |
| BioProject | PRJNA, PRJEB, PRJDB | `PRJNA123456` | BioProject containing multiple runs |

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

| Option | Default | Description |
|--------|---------|-------------|
| `--accession-list` | | Read accessions from a file (one per line) |
| `-O, --output-dir` | `.` | Output directory |
| `--format` | `sra` | Preferred format: `sra` or `sralite` |
| `--split` | `split-3` | Split mode: `split-3`, `split-files`, `split-spot`, `interleaved` |
| `--no-gzip` | | Disable gzip (compressed by default) |
| `--gzip-level` | `6` | Compression level (1-9) |
| `-t, --threads` | all CPUs | Thread count |
| `--connections` | `8` | HTTP connections per file |
| `--min-read-len` | | Minimum read length filter |
| `--include-technical` | | Include technical reads |
| `-f, --force` | | Overwrite existing files |
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
| `--accession-list` | | Read accessions from a file (one per line) |
| `-O, --output-dir` | `.` | Output directory |
| `--format` | `sra` | Preferred format: `sra` or `sralite` |
| `--connections` | `8` | HTTP connections per file |
| `--validate` | | Verify MD5 after download |
| `-f, --force` | | Overwrite existing files |
| `--no-progress` | | Disable progress bar |

---

## sracha fastq

Convert local SRA files to FASTQ.

```
sracha fastq [OPTIONS] <INPUT>...
```

### Arguments

| Argument | Description |
|----------|-------------|
| `INPUT` | Local `.sra` file path(s) |

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--split` | `split-3` | Split mode |
| `--no-gzip` | | Disable gzip |
| `--gzip-level` | `6` | Compression level (1-9) |
| `-t, --threads` | all CPUs | Thread count |
| `--min-read-len` | | Minimum read length filter |
| `--include-technical` | | Include technical reads |
| `-Z, --stdout` | | Write to stdout |
| `-O, --output-dir` | `.` | Output directory |
| `-f, --force` | | Overwrite existing files |
| `--no-progress` | | Disable progress bar |

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
| `--accession-list` | | Read accessions from a file (one per line) |

Displays file sizes, available formats, download mirrors, and quality
information for each accession. Study and BioProject accessions are
resolved to runs first.
