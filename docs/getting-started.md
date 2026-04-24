# Getting started

## Supported platforms

sracha supports modern sequencing platforms: Illumina, BGISEQ/DNBSEQ,
Element, Ultima, PacBio, and Oxford Nanopore. Legacy platforms (454,
SOLiD, Ion Torrent, Helicos) are not supported and will produce a
clear error message.

## Basic usage

The simplest way to get FASTQ files from an SRA accession:

```bash
sracha get SRR28588231
```

This will:

1. Resolve the accession via direct S3 URL (with SDL API fallback)
2. Download the SRA file using parallel chunked HTTP
3. Parse the VDB format natively
4. Output compressed FASTQ files (gzipped by default)

Output files: `SRR28588231_1.fastq.gz`, `SRR28588231_2.fastq.gz`

## Downloading entire projects

You can pass a BioProject or study accession to download all runs at once:

```bash
# Download all runs in a BioProject
sracha get PRJNA675068

# Download all runs in a study
sracha get SRP123456
```

sracha resolves project and study accessions to individual runs via the
NCBI EUtils API, then processes each run.

## Accession lists

For batch downloads, create a text file with one accession per line:

```bash
# SRR_Acc_List.txt
SRR2584863
SRR2584866
SRR2589044
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
# Download only (MD5 is verified by default; pass --no-validate to skip)
sracha fetch SRR28588231 -O /data/sra/

# Convert to FASTQ
sracha fastq /data/sra/SRR28588231.sra -O /data/fastq/

# Uncompressed output
sracha fastq SRR28588231.sra --no-gzip
```

## ENA FASTQ mirrors

The European Nucleotide Archive publishes pre-computed FASTQ.gz for most
public accessions. When the split/compression you asked for matches what
ENA serves (typically `split-3` gzip), skip the VDB decode step and pull
the FASTQs directly:

```bash
sracha get SRR28588231 --prefer-ena
```

sracha falls back to the NCBI SRA path automatically if ENA has no FASTQ
for the accession or the run layout is incompatible with your output
configuration. `sracha fetch --prefer-ena` downloads ENA's FASTQ.gz
instead of the `.sra` binary. `sracha info --prefer-ena` appends ENA's
filereport alongside the NCBI info table.

## Data integrity

Strict integrity checking is on by default. sracha fails the run when it
detects quality length mismatches, invalid quality bytes, quality
overruns, or paired-spot violations in the decoded data. Pass
`--no-strict` to downgrade these anomalies to warnings and keep going:

```bash
sracha get SRR28588231 --no-strict
```

Benign-fallback counters (SRA-lite all-zero quality blobs, truncated-spot
recovery) remain informational either way.

## cSRA (aligned SRA)

sracha decodes compressed/aligned SRA archives (cSRA) in pure Rust,
producing byte-identical FASTQ to fasterq-dump. No special flag is
required — sracha detects the schema and switches decoders automatically.
See [Implementation](implementation.md) for the decode strategy and
limitations.

## SRA-lite

SRA-lite files are smaller (4-10x) because they use simplified quality
scores. To prefer SRA-lite downloads:

```bash
sracha get SRR28588231 --format sralite
```

Quality scores will be uniform: Q30 for pass-filter reads, Q3 for rejects.

!!! note
    sracha's parallel downloads and streaming decode are typically
    3-7.5x faster than sra-tools. This speed gain may reduce the need
    for SRA-lite, since the download bottleneck that motivated smaller
    files is largely eliminated. Use `--format sralite` when quality
    scores genuinely aren't needed for your analysis (e.g., alignment-only
    workflows).

## Split modes

| Mode | Flag | Output |
|------|------|--------|
| split-3 (default) | `--split split-3` | `_1.fastq.gz`, `_2.fastq.gz`, `_0.fastq.gz` |
| split-files | `--split split-files` | `_1.fastq.gz`, `_2.fastq.gz`, ... |
| split-spot | `--split split-spot` | single file |
| interleaved | `--split interleaved` | single file, R1/R2 alternating |

## Compression options

By default, output is gzip-compressed. You can tune this or switch
to zstd:

```bash
# Faster gzip (lower ratio)
sracha get SRR28588231 --gzip-level 1

# No compression at all
sracha get SRR28588231 --no-gzip

# Use zstd instead of gzip
sracha get SRR28588231 --zstd

# Zstd with a specific level (1-22)
sracha get SRR28588231 --zstd --zstd-level 10
```

## FASTA output

To drop quality scores and output FASTA instead of FASTQ:

```bash
sracha get SRR28588231 --fasta
sracha fastq SRR28588231.sra --fasta
```

## Piping to stdout

Use `-Z` to write interleaved output to stdout, useful for piping
into other tools:

```bash
sracha get SRR28588231 -Z | bwa mem -p ref.fa -
```

See [Streaming Alignment](alignment.md) for a complete walkthrough
with bwa and samtools.

## Validating files

After downloading, you can verify that an SRA file is intact:

```bash
sracha validate SRR28588231.sra
```

This decodes all records and reports any errors. Useful after a
transfer that may have been interrupted.

## Performance tuning

```bash
# More download connections (default: 8)
sracha get SRR28588231 --connections 12

# More threads for decode and compression (default: 8)
sracha get SRR28588231 --threads 16

# Prefetch more accessions ahead of the decoder (default: 2)
# Useful on slow networks where decode consistently outpaces download.
sracha get --accession-list big_list.txt --prefetch-depth 4
```

`--prefetch-depth` only applies to multi-accession `sracha get`. Each
unit of depth costs one extra temp SRA file on disk while that run is
being decoded.

## Keeping the SRA file

By default `sracha get` deletes the temporary `.sra` after conversion.
Keep it around (e.g., to re-run with different tools or hand it off to
`sracha validate`) with `--keep-sra`:

```bash
sracha get SRR28588231 --keep-sra -O /data/
```

## Download behavior

Downloads are resumable by default — if a transfer is interrupted,
re-running the same command picks up where it left off. To force
a fresh download:

```bash
sracha fetch SRR28588231 --no-resume
```

For project downloads and large downloads (>100 GiB), sracha prompts for confirmation.
Skip it with `-y`:

```bash
sracha get --accession-list big_list.txt -y
```

## Verbose logging

Use `-v` for more detail, `-vv` for debug output, or `-q` to suppress
everything except errors:

```bash
sracha -vv get SRR28588231
sracha -q get --accession-list SRR_Acc_List.txt
```
