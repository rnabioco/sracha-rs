# Performance

Decode-speed benchmarks (sracha vs `fasterq-dump`/`fastq-dump`) live in the
[project README](https://github.com/rnabioco/sracha-rs#benchmarks) and
reproduce with `validation/benchmark.sh`. This page covers the memory side of
that story.

## Peak memory (RSS)

Peak resident set size for a local decode (SRA file on disk → uncompressed
FASTQ), measured with `/usr/bin/time -v` as the largest of three runs.

| File | Size | sracha | fasterq-dump | sracha ÷ fasterq |
|:---|---:|---:|---:|---:|
| SRR28588231 | 23 MiB | 195.9 MiB | 565.0 MiB | 0.35x |
| SRR2584863 | 288 MiB | 1198.2 MiB | 430.4 MiB | 2.78x |
| ERR1018173 | 1.94 GiB | 3398.8 MiB | 404.9 MiB | 8.39x |

sracha is leaner than `fasterq-dump` on small files but uses more on large
ones, and its peak RSS grows with archive size while `fasterq-dump` stays
flat. That difference is a deliberate design choice, and the reported number
overstates the real cost.

## Why the numbers differ

sracha **memory-maps** each column's data for zero-copy decode — blob reads
become plain slice operations with no syscall and no copy, which is a large
part of the decode speedup. The cost is that pages touched during the stream
count toward the process's resident set.

`fasterq-dump` takes the opposite approach: it reads each blob with a bounded
positional read into a small reused buffer (`KColumnDataRead` →
`KFileRead` in ncbi-vdb), and never maps the multi-GiB `QUALITY`/`READ`
columns. The file bytes still land in the OS page cache, but as *shared*,
reclaimable cache rather than private RSS — so its reported peak stays flat
regardless of file size.

!!! note "The large-file figure is mostly reclaimable cache"

    On the 1.94 GiB run, ~1.96 GiB of the ~3.4 GiB peak is clean,
    file-backed mmap page-cache — pages the kernel reclaims instantly under
    memory pressure, never an out-of-memory risk. The actual heap working
    set (transient decode/format buffers, bounded independent of run size) is
    ~1.4 GiB. `/usr/bin/time`'s *Maximum resident set size* counts the
    mapped pages, which is why the ratio looks large.

In short: sracha trades a higher *reported* peak RSS — most of it benign,
reclaimable cache — for zero-copy decode speed. On memory-constrained hosts
the mapped pages are reclaimed before any real pressure is felt.
