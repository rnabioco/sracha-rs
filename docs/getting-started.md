# Getting started

## Basic usage

The simplest way to get FASTQ files from an SRA accession:

```bash
sracha get SRR000001
```

This will:

1. Resolve the accession via the NCBI SDL API
2. Download the SRA file using parallel chunked HTTP
3. Parse the VDB format natively
4. Output compressed FASTQ files (gzipped by default)

Output files: `SRR000001_1.fastq.gz`, `SRR000001_2.fastq.gz`

## Downloading entire projects

You can pass a BioProject or study accession to download all runs at once:

```bash
# Download all runs in a BioProject
sracha get PRJNA123456

# Download all runs in a study
sracha get SRP123456
```

sracha resolves project and study accessions to individual runs via the
NCBI EUtils API, then processes each run.

## Accession lists

For batch downloads, create a text file with one accession per line:

```bash
# SRR_Acc_List.txt
SRR1234567
SRR1234568
SRR1234569
```

Then pass it with `--accession-list`:

```bash
sracha get --accession-list SRR_Acc_List.txt
```

Lines starting with `#` are treated as comments and blank lines are
skipped. You can also combine positional accessions with a list file:

```bash
sracha get SRR9999999 --accession-list SRR_Acc_List.txt
```

## Step by step

If you prefer more control, use the individual subcommands:

```bash
# Download only
sracha fetch SRR000001 -O /data/sra/

# Convert to FASTQ
sracha fastq /data/sra/SRR000001.sra -O /data/fastq/

# Uncompressed output
sracha fastq SRR000001.sra --no-gzip
```

## SRA-lite

SRA-lite files are smaller (4-10x) because they use simplified quality
scores. To prefer SRA-lite downloads:

```bash
sracha get SRR000001 --format sralite
```

Quality scores will be uniform: Q30 for pass-filter reads, Q3 for rejects.

## Split modes

| Mode | Flag | Output |
|------|------|--------|
| split-3 (default) | `--split split-3` | `_1.fastq.gz`, `_2.fastq.gz`, `_0.fastq.gz` |
| split-files | `--split split-files` | `_1.fastq.gz`, `_2.fastq.gz`, ... |
| split-spot | `--split split-spot` | single file |
| interleaved | `--split interleaved` | single file, R1/R2 alternating |

## Performance tuning

```bash
# More download connections (default: 8)
sracha get SRR000001 --connections 12

# More threads for compression (default: all CPUs)
sracha get SRR000001 --threads 16

# Faster compression (lower ratio)
sracha get SRR000001 --gzip-level 1

# No compression at all
sracha get SRR000001 --no-gzip
```
