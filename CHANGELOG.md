# Changelog

## 0.6.0 (2026-08-26)

### Upgrade note

**Re-convert aligned (cSRA) output produced by 0.5.0 or earlier.** Archives
that store their unaligned bases as `CMP_READ` + `CMP_ALTREAD` had every
ambiguity code emitted as a confident `A` — 2na cannot represent `N` and the
mask was ignored. On SRR622461 that was ~1.7% of reads, including
fully-unaligned spots that came out as 100 `A`s instead of 100 `N`s. Spot
counts, read counts and qualities were all correct and the exit code was
zero, so nothing downstream would have flagged it.

### Features

- Decode aligned cSRA whose reference bases live outside the archive (#74).
  Runs aligned to a public assembly keep only the chunk layout in
  `REFERENCE`; the bases are fetched from NCBI refseq objects named by
  `SEQ_ID` and cached in `~/.cache/sracha/refseq` (override with
  `--refseq-cache` or `$SRACHA_REFSEQ_DIR`) for reuse across runs. Also
  handles fully-aligned archives, which store no `SEQUENCE.CMP_READ` at all.
- `--verify` checks decoded quality *values* against the archive's
  `STATS/QUALITY` histogram (#124). Every other quality check compares
  lengths, so a decode emitting the right number of plausible bytes at the
  wrong values passed all of them. Off by default — it costs one increment per
  base — and skipped where quality is synthesized rather than decoded
  (SRA-Lite, `--fasta`). Strict-fatal as `quality_histogram_mismatch`.
- `--no-disk-check` on `get` and `fetch` opts out of the free-disk-space
  preflight entirely, for output filesystems that autoscale or report a
  quota `statvfs` can't see through (#137).

### Performance

- Reading one row from a run-length-encoded blob no longer expands the whole
  blob's extents (#134). `SEQUENCE.READ_LEN` on a fixed-length run is stored
  as a single run covering every spot, and expanding it per decode accounted
  for 99.6% of all extent expansion — from 0.4% of the cache misses. Blobs
  over 1M rows now walk the page map's length runs instead: ERR10213669 goes
  150s → 87s wall and 2.5 GB → 860 MB peak RSS, a 400k-spot slice of
  SRR622461 18.4s → 7.4s.

### Fixes

- `sracha get --stdout` no longer demands disk for output it never writes
  (#137). The preflight sized a streaming run as if every accession's FASTQ
  landed on disk and every temp archive stayed there, so peak was
  `sum(archive + fastq)` across the whole batch. Streaming writes no FASTQ at
  all and deletes each archive as its decode ends, so the requirement is now
  the largest `--prefetch-depth + 1` archives — flat in batch size.
- Restore ambiguity codes in the unaligned half of a cSRA read
  (`SEQUENCE.CMP_ALTREAD`) — see the upgrade note.
- `sracha get` now routes reference-compressed cSRA through the cSRA decoder.
  Only `sracha fastq` did, so `get` on an aligned run died in the plain
  cursor's cSRA rejection even when the archive was decodable.
- Size an alignment's reference projection from `REF_OFFSET` rather than the
  stored `REF_LEN`, and by the reference cursor's peak rather than its close.
  Soft-clipped archives failed with "ref cursor N outside ref_read (N)".
- Wrap spans that cross the origin of a circular reference (the
  mitochondrion), and return a short span at the end of a linear one instead
  of failing.
- Don't consume a `REF_OFFSET` per flag during a BAM `B` (back-up) operation.
- cSRA deflines now carry the synthesized spot id, matching `fasterq-dump`
  and sracha's own unaligned path.
- `sracha info` on a split cSRA no longer errors: detection was sidecar-aware
  but the cursor open was not.
- cSRA decode no longer holds the entire FASTQ output in memory before
  writing (7.7 GiB resident on a 1 GiB archive; now streams in bounded
  batches).

### Validation

- `validation/ab_corpus.sh` gains a `vdb dump` leg — a pinned row window per
  column, diffed against `vdb-dump` — and records per-accession CPU and peak
  RSS for both binaries (#107, #108). The harness only ever diffed FASTQ
  bytes, which is how a `vdb dump` regression (#104) and a 2.4x decode-cost
  regression (#101) each survived a full corpus run unflagged.

## 0.5.0 (2026-08-02)

### Upgrade note

Three things change. The first two affect output; the third affects whether a
run succeeds at all.

**1. `--split-files` filenames change for single-read runs.** Archives storing
one read per spot now produce `ACC.fastq`, matching `fasterq-dump`, where
earlier releases produced `ACC_1.fastq`. Read content is unchanged, and
`--split-3` / `--split-spot` already matched.

This is the widest-reaching change: **123 of 386** accessions in a random SRA
sample are single-layout, so roughly a third of `--split-files` conversions
produce a differently-named file, and a pipeline globbing `${ACC}_1.fastq`
will find nothing. Runs with two or more *stored* reads are unaffected,
including when only one survives filtering — DRR004435, whose 2 bp adapter is
dropped, still writes `DRR004435_2.fastq`.

**2. Output content changes on some archives. Re-convert anything you hold
that was produced by 0.4.2 or earlier from these classes.**

| class | symptom before | accessions |
|---|---|---|
| `latf-load` on `NCBI:align:tbl:seq#1` | **half of all bases missing** | 5 |
| srf-load-era Illumina (`izip` quality) | every quality byte wrong | 4 |
| `q4` Illumina schema (4-channel log-odds) | every quality byte wrong | 1 |
| deduplicated ALTREAD page maps | `N` emitted as a confident basecall | 2 |

**12 of 386** sampled accessions (~3%) are affected, concentrated in older DDBJ
(`DRR`) submissions; modern Illumina runs are not. All four classes produced
correct spot counts, correct read names and a zero exit code, so nothing
downstream would have flagged them.

`sracha vdb dump -C READ` output also changes: it now applies the ALTREAD
ambiguity mask and so matches `vdb-dump` rather than showing the physical
column.

**3. Strict integrity checking can now refuse archives it previously
converted.** Five new checks compare the decode against itself and against the
archive's own recorded totals, and they are fatal under the default strict
mode. An archive whose decode is wrong in a way sracha cannot fix now fails
instead of writing plausible-looking output. `--no-strict` restores the
previous behaviour and downgrades every counter to a warning.

This is deliberate: refusing to convert is recoverable, silently wrong data in
a published dataset is not. If you hit a refusal on an archive you need, please
open an issue with the accession.

### Validation

A 386-accession x 2-split A/B against 0.4.2, with `fasterq-dump` as reference:
611 rows unchanged, 145 fixed, **1 regression** — a false positive in the new
base-count check on PacBio archives with a CONSENSUS table, fixed before
release. Every other new check held across the full breadth.

### Fixes

- Decline ABI SOLiD archives by name instead of blaming the KAR layout
  (#109). SOLiD keeps colorspace bases in `CSREAD`/`ALTCSREAD` and has no
  `READ` column, so the cursor's column probe failed before the platform check
  ran and reported "SEQUENCE table not found in KAR archive" — pointing users
  at a corrupt file or a bad download rather than at an encoding sracha does
  not decode. The platform now comes from `md/cur` before any column opens, so
  SOLiD gets the same message as the other legacy platforms.

- Verify the decoded spot count against the SEQUENCE table's row count
  (#117). `BIO_BASE_COUNT` cannot see a run that emits the right total bases
  across the wrong number of spots — issue #22 was exactly that shape.
  `sracha validate` has always made this comparison; the conversion path never
  did. Strict-fatal.

- Verify decoded bases against the archive's recorded `BIO_BASE_COUNT` (#117).
  Every other integrity check compares the decoder against itself, which
  cannot see a decode where all streams agree on the wrong answer — #118
  emitted half of five runs with static READ_LEN and the decoded buffer in
  perfect agreement. The loader records how many biological bases a run holds;
  comparing against it is the one check anchored outside the decode.
  Strict-fatal.

- Keep zero-length read slots in the archive's static READ_LEN (#118).
  `latf-load` archives declare `READ_LEN = [100, 0]` and carry no READ_LEN
  column, but the metadata accessor discarded any descriptor containing a
  zero. That sent them down the even-split heuristic, which cut every 100 bp
  spot into 50/50 — emitting **half the run's bases** with no error and a zero
  exit code. Five corpus accessions were affected; all now match
  `fasterq-dump` byte for byte.

- Check that a blob's spot boundaries consume exactly its decoded bases
  (#117). `READ_LEN` defines where each spot starts inside the sequence
  stream; the per-spot check saw only overrun, so a short accounting silently
  dropped the tail and shifted every later record. Strict-fatal, mirroring the
  quality check in #115. Note this does not catch #118, where `READ_LEN` and
  the decoded buffer agree with each other but both hold half the spot.

- Refuse to emit records whose quality does not correspond to their sequence
  (#115). Strict mode checked quality per spot, but the per-spot slice is
  correctly sized by construction, so a wrong-sized quality buffer produced
  correctly-sized slices of the wrong bytes and no counter moved — how three
  quality bugs (#101, #111, #113) shipped with correct sequences, correct
  counts and a zero exit code. A blob-level check now compares the decoded
  quality stream against the decoded sequence stream; both are one byte per
  base, so any difference is fatal under the default strict mode.

- Decode 4-channel log-odds QUALITY on the `q4` Illumina schema (#113). Those
  archives store `NCBI:qual4` — four log-odds channels per base behind a
  two-stage `zip`/`qual4_encode` chain — and sracha stopped after the inflate,
  emitting the intermediate encoded bytes as phred. Every base of every spot
  was wrong, silently. DRR001867's 14.2M spots now match `fasterq-dump` byte
  for byte.

- Decode `izip_encoding` QUALITY through the blob header instead of probing
  (#111). Quality on srf-load-era archives is byte-plane (irzip) data whose
  plane count and min/slope live in the blob header, not an izip container.
  Probing it as an izip container fails, and the fallback decoded it as
  deflate, yielding plausible-but-wrong scores for every base of every spot
  with no error — DRR001816's 35.9M spots now match `fasterq-dump` byte for
  byte. Sequences, names and counts were always correct, so nothing failed
  loudly.

- Apply the ALTREAD ambiguity mask in `vdb dump -C READ` (#104). The dump
  rendered the physical READ column while `vdb-dump` renders the logical one
  the schema defines, so the two disagreed wherever a submitter recorded an
  ambiguity — sracha printed a confident basecall where `vdb-dump` printed
  `N`. ALTREAD is stored `trim<0,0>` and its page maps deduplicate rows, so
  each row's stored nibbles right-align against READ's width; the FASTQ path
  was already correct.

- Name the `--split-files` output `ACC.fastq` for single-read archives (#103).
  fasterq-dump suffixes by mate index there, except when the archive stores
  one read per spot, where it writes a bare filename; sracha always wrote
  `ACC_1.fastq`. The decision keys off the stored read count rather than how
  many reads survive filtering, so DRR004435 — where a 2 bp adapter is dropped
  and only mate 2 remains — still writes `DRR004435_2.fastq`.

- Decode version-2 (random-access) page maps as per-row element offsets
  instead of repeat counts (#101). A page map's `data_offset[]` and
  `data_run[]` share one slot in the on-disk format, and sracha kept both in
  an untyped `Vec<u32>`, so a version-2 blob's offsets were walked as run
  lengths — `sracha fastq` failed on SRR35917722 with "variable data
  truncated", and `sracha vdb dump` failed on any archive with deduplicated
  rows because it never expanded them at all. `PageMap` now carries a typed
  `RowMapping`, and every consumer resolves rows through a single
  offset-and-length walk that mirrors ncbi-vdb's `PageMapFindRow`.

## 0.4.2 (2026-07-31)

### Fixes

- Account for FASTQ output and ENA sizes in the download confirmation and
  disk preflight (#91 follow-up). The >100 GiB confirmation prompt summed
  `.sra` sizes, so under `--prefer-ena` it could be evaluated against
  archives that are never downloaded — it now uses the same per-run sizes as
  the disk check, and the table shown alongside it reports them too. The
  `get` preflight also counted only the archive, but the decoder writes FASTQ
  beside the temp `.sra` before it is removed, so peak usage is both; output
  size is now estimated from RunInfo spot counts and read lengths (and left
  out entirely when RunInfo is unavailable, rather than guessed).

- Size the disk-space preflight by what will actually be downloaded (#91).
  The check summed the NCBI `.sra` object size even under `--prefer-ena`,
  where that object is never fetched — so it measured a file that would not
  exist. It could refuse a transfer that would have fit, and pass one that
  then ran out of disk partway (for SRR37428186 the `.sra` is 58.2 GiB while
  the ENA FASTQs total 72.9 GiB). ENA-served runs are now sized by their
  filereport totals, and `sracha fetch --prefer-ena` gets a preflight at all —
  its ENA downloads previously bypassed the check entirely.
- Stop the download progress bar from overstating progress when chunks retry
  (#89). Bytes were credited as they arrived, but a retried chunk re-fetches
  from byte zero, so every retry counted its bytes again — a stalled 37 GiB
  ENA transfer displayed `30.99 GiB/30.99 GiB  eta 0s` while chunks were
  still outstanding, reading as a near-miss when it was nowhere close. A
  failed attempt's bytes are now rolled back, so the bar reflects only data
  actually on disk.
- Retry an interrupted `--prefer-ena` transfer instead of aborting the run
  (#89). The per-chunk retry budget spans roughly a minute; ENA outages last
  longer, so a single chunk exhausting its attempts would abort a transfer
  that was nearly complete and leave the user to re-run it by hand. ENA
  downloads now re-enter the transfer up to 4 times, waiting 30 s → 5 min
  (doubling, with full jitter) between attempts and resuming from the
  `.sracha-progress` sidecar, so completed chunks are never re-fetched.
  Cancellations, checksum mismatches, and I/O errors such as a full disk are
  surfaced immediately rather than burning retries, and retrying is skipped
  under `--no-resume`, where every attempt would restart from byte zero.
- Make `--prefer-ena` transfers resilient to ENA instability (#89). ENA FASTQ
  URLs are now fetched over `https://` instead of plain `http://`. ENA serves
  from a single Apache host that chokes under the S3-tuned connection floor
  (which forced ≥24 parallel streams on large files regardless of
  `--connections`); ENA downloads are now capped at 6 connections and honor a
  lower `--connections`, and the S3 auto-scale floor is confined to the NCBI
  path. Per-chunk retry backoff was widened from 250 ms/500 ms (3 attempts, no
  jitter) to exponential 500 ms→15 s with full jitter over 5 attempts, so a
  transient connection-refusal window is ridden out instead of amplified by a
  thundering herd of lockstep retries. On a failed ENA transfer the partial
  file and `.sracha-progress` sidecar are preserved with a message that
  re-running resumes.
- Resolve the `idx0` write-ahead overlay together with the `idx1`/`idx2` block
  index instead of treating `idx0` as authoritative (#87). NCBI's VDB writer
  appends new blobs to `idx0`, periodically compacts them into `idx1`/`idx2`,
  then keeps appending to `idx0`, so a finalized archive can have both tiers
  populated at once. sracha previously decoded `idx1`/`idx2` only when `idx0`
  was empty, silently truncating large accessions to the recent `idx0` tail
  (SRR39695091 dropped ~2^29 rows and failed the spot-count check). Both tiers
  are now merged into one logical column, with `idx0` winning on any overlapping
  row range.

## 0.4.1 (2026-07-28)

### Upgrade note

Affected runs change shape: they now emit one file per submitted read instead
of an even 2-way split, so a 10x run that wrote `_1`/`_2` may now write
`_1`/`_2`/`_3`. Output from earlier versions for those runs is scrambled, not
merely mis-split, and should be regenerated.

### Fixes

- Read lengths now come from the archive's own static `READ_LEN` metadata in
  preference to the NCBI EUtils average (#84). RunInfo carries only
  `avgLength` and a SINGLE/PAIRED flag, so it can never describe more than two
  reads and always split the spot evenly — scrambling any run whose reads
  differ in length. SRR9827735 (10x, 26/55/8) was cut into 44/45; it now
  matches fasterq-dump byte for byte. A probe of 120 public runs put this at
  roughly 25% of 10x-style runs. When no static structure exists and the
  EUtils average is used, sracha now warns instead of guessing silently.

## 0.4.0 (2026-07-25)

### Upgrade note

The split-mode fixes below change which FASTQ file a read lands in, and can
change how many records a run emits. A sweep of 2,000 public runs put this at
roughly **9% of SRA**, heavily concentrated by platform:

| platform | runs affected |
|----------|---------------|
| PacBio SMRT | 65% |
| BGISEQ | 31% |
| DNBSEQ | 19% |
| Element | 8% |
| Illumina | 8% |
| Oxford Nanopore | 1% |
| Ultima | 0% |

Two consequences worth checking before upgrading a pipeline:

- Runs whose leading read slot is technical or zero-length now produce **no
  `_1` file at all** — the first output is `_2`. This matches fasterq-dump,
  but breaks globs that assume `_1.fastq.gz` exists.
- Runs affected by the READ_TYPE page-map bug previously **dropped a small
  number of reads** (2-4 per run on the reported archives). Output from
  earlier versions for those runs is incomplete and should be regenerated.

`sracha vdb dump <acc> -C READ_LEN,READ_TYPE -R 1` now works without
downloading, which is the quickest way to check whether a run stores reads in
a non-leading slot.

### Features

- `sracha vdb` subcommands now accept an accession or URL as well as a local
  path, reading the archive in place over HTTP range requests (#78).
  `sracha vdb info SRR18959644` answers in ~1 s and ~256 KiB against a 4.2 GB
  run. `dump` fetches only the blobs covering `-R`. Remote decode
  (`sracha fastq`) is unchanged — it still needs a local file.

### Fixes

- `--split-files` now numbers output files by each read's original slot
  instead of its position among the reads that survived filtering (#76).
  Zero-length, technical, and too-short reads still consume their file
  number, matching fasterq-dump. Runs whose spots store an empty leading
  slot (`READ_LEN=[0, 150]`, e.g. SRR18959644) wrote every read to `_1`,
  silently concatenating R2 onto R1; they now split into `_1`/`_2`.
  Runs whose leading read is marked technical (e.g. DRR004435, a 2 bp
  adapter ahead of a 34 bp biological read) now write that read to `_2`
  rather than `_1`, and produce no `_1` at all.
- `--split-3` decides pairing from the spot's biological read count rather
  than the number of surviving segments, so `--include-technical` no longer
  turns an unpaired spot into a `_1`/`_2` pair, and spots with more than two
  reads are numbered `_1.._N` instead of collapsing into the unpaired file.
- READ_TYPE is interpreted as INSDC `xread_type` bits throughout. Bit 0 is
  the biological flag and bits 1-2 carry orientation, so reads typed
  `BIOLOGICAL|FORWARD` or `BIOLOGICAL|REVERSE` — common in aligned cSRA — are
  no longer dropped as technical. Previously the physical `READ_TYPE` column
  was compared against 0 using the inverted convention that only the
  metadata and cSRA fallbacks produced.
- READ_TYPE blobs are expanded through their page map. VDB stores only the
  distinct type rows plus run lengths, so indexing the raw buffer per spot
  read the wrong row for every spot after the first — technical reads were
  effectively filtered on one spot per blob and ignored for the rest, and a
  spot sitting on a change in read types could lose its biological read
  entirely. SRR18959644 dropped exactly one read this way.
- The physical `READ_TYPE` column is located by row id rather than blob
  index, matching what ALTREAD and NAME_FMT already do — its blobs can be
  coarser than READ's, so index pairing read types from the wrong rows.

## 0.3.11 (2026-07-12)

### Features

- When sracha can't reconstruct a reference-compressed cSRA — its
  `PRIMARY_ALIGNMENT` + `REFERENCE` tables live in an undownloaded
  `.vdbcache`, or the reference is stored externally — `sracha get` and
  `sracha fastq` now check ENA for a FASTQ mirror and suggest
  `sracha get --prefer-ena <acc>` instead of failing (#75). The accession is
  skipped rather than aborting the whole batch, and the run exits non-zero so
  the skip stays visible to scripts.

### Fixes

- cSRA reference chunking derives `MAX_SEQ_LEN` from the archive instead of
  assuming 5000, fixing reconstruction of runs with a different reference
  chunk size (#71).

### Performance

- Pre-size per-slot FASTQ output buffers to cut reallocations during decode
  (#72).

### Changed

- Release binaries now ship x86-64 microarchitecture variants (v2 and v3)
  alongside the ARM build (#71).
- Minimum supported Rust version is now 1.95.

## 0.3.10 (2026-06-17)

### Fixes

- The info table no longer shows `?`/`-` placeholders for an occasional run
  when resolving a project. EUtils EFetch sometimes returns HTTP 200 with an
  incomplete RunInfo CSV, dropping rows; sracha now re-fetches the missing
  accessions with bounded backoff instead of leaving them unresolved.
- Runs that NCBI reports with `avgLength=0` keep their RunInfo metadata
  instead of being dropped, and the spurious warning is silenced (#67, #68).
- `sracha get` now deduplicates repeated accessions and backfills metadata
  on a cache hit, so previously-cached runs report complete info (#69, #70).

## 0.3.9 (2026-06-12)

### Fixes

- Large `--accession-list` runs no longer fail with `HTTP 414 URI Too Long`
  (#64). The SDL locate request is now split into chunks of 100 accessions
  instead of packing every accession into one URL, and EUtils RunInfo EFetch
  uses HTTP POST (no URL-length limit) instead of GET.

## 0.3.8 (2026-06-03)

### Performance

- Sharply lower peak memory during FASTQ decode of runs with many blobs
  (#54, #55). The decode→write pipeline buffered formatted FASTQ in
  fixed 1024-blob batches with a 4-deep hand-off queue, so a large
  full-quality run could hold tens of GiB of decoded output before the
  writer drained any of it — `SRR36401016` used 19.4 GiB even at
  `-t 1 --connections 1`. The buffer is now bounded by a thread-scaled
  batch size (`(threads × 8).clamp(64, 256)`) with a single queued batch,
  making peak decode RSS roughly independent of run size. The reporter
  measured 19.4 GiB → 1.1 GiB at `-t 1`, with a small wall-clock
  improvement; output is byte-identical.

### Improvements

- Better long-read (PacBio / Oxford Nanopore) support. Platform detection
  now reads the authoritative `col/PLATFORM/row` numeric id
  (`INSDC:SRA:platform_id`) before sniffing the schema table name, so runs
  submitted as plain FASTQ and loaded under the generic
  `NCBI:SRA:GenericFastq` schema — common for PacBio/ONT — report their real
  platform (e.g. `PACBIO_SMRT`) instead of `unknown`. The same id feeds the
  read-structure fallback, so generic-loaded long-read runs resolve to one
  biological read per spot even without read columns. Schema-based
  read-structure inference also resolves PacBio and Nanopore *schema-tagged*
  runs to one biological read per spot instead of erroring out to an untyped
  fallback, so single-end long-read spots decode with a known read type. The
  single-end advisory printed for `--split split-3` now also fires for
  `--split interleaved` and points at `--split split-spot` as the explicit
  single-file layout. `--seq-defline`'s `$sn` is documented as the
  platform-native read identifier for PacBio/ONT (e.g. `m64012_.../ccs`,
  ONT `<uuid> ch=.. start_time=..`); the channel/start-time/ZMW values
  long-read platforms embed there are substrings of that name rather than
  separate columns. Adds network-gated integration fixtures for a PacBio
  SMRT run (SRR38889541) and an Oxford Nanopore run (SRR38892122).
- PacBio/Oxford Nanopore **CONSENSUS** (CCS) support. When a database carries
  a `CONSENSUS` table, sracha now reads its reads from there by default —
  mirroring fasterq-dump's `insp_db_type()` table selection — so FASTQ output
  matches fasterq-dump byte-for-byte (verified on DRR032988: 4,004 reads,
  identical bases/quality/deflines, with empty consensus rows dropped). The
  default defline now follows fasterq-dump's `dflt_seq_defline` rule: the name
  field is emitted for tables that carry spot names (the reconstructed name, or
  the spot number as the synthesized fallback) and **omitted entirely** for
  tables with no NAME column (CONSENSUS), instead of repeating the spot number.

### Fixes

- Decode older PacBio SMRT archives (e.g. DRR032988) that previously failed
  with `page_map: data_runs has N entries, expected at least M`. Variable-length
  array columns (READ_START, READ_TYPE, LABEL_LEN/START, RD_FILTER) pack
  per-record arrays of differing lengths; the page-map expansion now derives
  each physical record's width from `lengths`/`leng_runs` instead of assuming a
  single fixed row length, so records are replicated to rows correctly.
- Stop mis-decoding raw, uncompressed 2na READ payloads. A header-less READ
  blob whose bytes happen to parse as a tiny deflate stream (PacBio CONSENSUS
  READ) is now recognised as raw when its size matches the expected packed base
  count, rather than being collapsed to a few bytes.
- Reconstruct native long-read names for PacBio Revio and Oxford Nanopore
  `GenericFastq` (sharq-loaded) runs, which store the read id
  (`<movie>/<zmw>/ccs`, or an ONT UUID) entirely in the `skey` text index with
  no physical NAME column. Two parts: (1) the PBSTree `data_idx` stride now uses
  raw-byte width thresholds (`≤256→u8, ≤65536→u16`) instead of `trans_off`'s
  `×4`-scaled thresholds, which silently corrupted any node-data region in the
  `(256, 1024]`-byte window (e.g. a Revio skey transition with a 1013-byte
  payload); (2) the dense (one-key-per-row) text-index projection
  (`KPTrieIndex_v2` variant 0: `[count][ord2node]`) is now decoded to map each
  spot to its trie node, so deflines carry the native name and match
  fasterq-dump byte-for-byte (SRR38889541, SRR38892122) instead of falling back
  to the spot number.

## 0.3.7 (2026-05-29)

### Features

- `sracha {get,fastq} --seq-defline <TEMPLATE>` sets a custom FASTQ/FASTA
  defline using fasterq-dump's `--seq-defline` syntax (#50). Supports
  `$ac` (accession), `$si` (spot id), `$ri` (read id), `$sn` (spot name),
  `$rl` (read length), and `$$` for a literal `$`; the `+` line mirrors
  the template. Templates are validated at startup. `$sg` (spot-group) is
  not supported. Without the flag, output is unchanged. Adds a "Coming
  from sra-tools" option-mapping table to the CLI docs.

## 0.3.6 (2026-05-16)

### Features

- `sracha get --metadata {tsv,json,both}` writes a
  `{accession}.metadata.{tsv,json}` sidecar alongside the FASTQ outputs
  after a successful decode (#37). Captures BioSample/SAMN, Sample/SRS,
  BioProject, library strategy/source/selection/layout, instrument
  model, experiment, study, scientific name, tax id, base count, and
  release/load dates from the EUtils RunInfo CSV. `RunInfo` gains 17
  optional fields and now derives `Default`.
- `sracha get --dry-run` resolves accessions and prints what would be
  downloaded as TSV (default) or JSON via `--dry-run-format`, then
  exits without downloading or decoding (#38). Honors `--prefer-sdl`,
  `--no-runinfo`, `--prefer-ena`, and project/study expansion.
- `sracha {get,fastq} --paired-suffix {numeric,r}` selects between
  `_1`/`_2` (default, matches fasterq-dump and ENA filenames) and
  `_R1`/`_R2` FASTQ filenames for paired/split outputs (#39). Matches
  the Illumina BCL convention many pipelines expect; applies uniformly
  to VDB decode, cSRA decode, split-files, and the ENA fast path.
- `sracha {get,fastq} --folder-per-accession` places each accession's
  outputs — FASTQ files, metadata sidecar, completion marker, temp
  SRA, `.sracha-progress` sidecar, and any `--keep-sra` artifact —
  inside its own `<output_dir>/<accession>/` subdirectory (#40). The
  shared `sracha-stats.jsonl` audit log stays at the top level so it
  aggregates across runs.

## 0.3.5 (2026-04-26)

### Fixes

- Honor `idx0_count` from the v3+ column header so columns whose idx0
  carries trailing bytes past the last valid BlobLoc parse cleanly
  (#32). Unblocks SRR15000000 and similar newer-writer archives.
- Decode variant-2 random-access ALTREAD page maps via
  `data_offset[row_count]` + per-run lengths, with write-time row
  dedup (#33). DRR024182 reaches byte-identical R1/R2 vs fasterq-dump.
- Walk persisted PTrie nodes to reconstruct full skey templates (the
  offset-table fast path only saw leaf suffixes), and drop the ALTREAD
  gate from Illumina X/Y detection (#35). DRR016241, DRR032228,
  DRR032250, DRR041584, DRR041585, DRR048907 reach PASS_MD5.

### Features

- `sracha get --head-concurrency <N>` (default 64) tunes the S3
  HEAD-probe fan-out used during accession resolution (#34). Bumps
  the built-in pool/probe defaults from 16 → 64.

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

