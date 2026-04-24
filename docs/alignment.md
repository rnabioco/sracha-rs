# Streaming alignment with bwa

sracha can stream FASTQ directly to an aligner via `-Z`, eliminating
intermediate files and reducing disk I/O.

### Prerequisites

Install bwa, samtools, and ucsc-twobittofa:

```bash
pixi add bwa samtools ucsc-twobittofa
# or: conda install -c bioconda bwa samtools ucsc-twobittofa
```

### Quick start: align to chr22

Stream chr22 from the hs1 (T2T-CHM13v2.0) 2bit file, gzip it, and index:

```bash
twoBitToFa -seq=chr22 https://hgdownload.soe.ucsc.edu/goldenPath/hs1/bigZips/hs1.2bit stdout \
  | gzip > chr22.fa.gz
bwa index chr22.fa.gz
```

Stream directly from NCBI to a sorted BAM — no intermediate files:

```bash
sracha get SRR28588231 --split interleaved -Z \
  | bwa mem -p -t 8 chr22.fa.gz - \
  | samtools sort -@ 4 -o SRR28588231.chr22.bam
samtools index SRR28588231.chr22.bam
```

sracha downloads the SRA file to a hidden temp file, streams
interleaved FASTQ to stdout, then auto-deletes the temp file.
No `.sra` or `.fastq.gz` files are left on disk.

Verify:

```bash
samtools flagstat SRR28588231.chr22.bam
```

### Full human genome workflow

Download and index hs1:

```bash
wget https://hgdownload.soe.ucsc.edu/goldenPath/hs1/bigZips/hs1.fa.gz
bwa index hs1.fa.gz    # ~1 hour, ~5 GB RAM
```

Stream alignment:

```bash
sracha get SRR28588231 --split interleaved -Z \
  | bwa mem -p -t 8 hs1.fa.gz - \
  | samtools sort -@ 4 -o SRR28588231.bam
samtools index SRR28588231.bam
```

### Two-step workflow

If you already have an SRA file on disk, use `fastq -Z`:

```bash
sracha fastq SRR28588231.sra --split interleaved -Z \
  | bwa mem -p -t 8 hs1.fa.gz - \
  | samtools sort -@ 4 -o SRR28588231.bam
```

### How it works

- `-Z` streams uncompressed FASTQ to stdout; pair it with `--split interleaved`
  so paired reads come out as a single interleaved stream
- `bwa mem -p` reads interleaved paired-end FASTQ from stdin
- `samtools sort` reads SAM from stdin and writes a coordinate-sorted BAM
- Backpressure flows naturally through the Unix pipe

### Performance tips

- Split threads between sracha and bwa (e.g., `-t 4` for sracha, `-t 8` for bwa on 12 cores)
- Streaming avoids writing intermediate FASTQ files (saves disk I/O and space)
- For maximum throughput, use `--format sralite` if quality scores aren't critical
