# cSRA format notes (Phase 0 spike)

Findings from reading ncbi-vdb source and inspecting `test/vdb/db/VDB-3418.sra`.
Authoritative references are `libs/axf/` and `interfaces/align/align.vschema` in a
local ncbi-vdb clone.

## Fixture used

`~/devel/ncbi-vdb/test/vdb/db/VDB-3418.sra`
- 12 MiB, BAM-loaded 2017 (`bam-load.2.8.2`), schema `NCBI:align:db:alignment_sorted#1.3`.
- 985 SEQUENCE rows, 938 PRIMARY_ALIGNMENT rows, 180 REFERENCE rows.
- Platform `SRA_PLATFORM_UNDEFINED` in metadata.

## Physical columns (from `sracha vdb columns`)

### SEQUENCE
`ALIGNMENT_COUNT, CMP_READ, PRIMARY_ALIGNMENT_ID, QUALITY, READ_LEN, READ_TYPE`

Quality is a normal physical column — full Phred bytes per spot.

### PRIMARY_ALIGNMENT
`GLOBAL_REF_START, HAS_MISMATCH, HAS_REF_OFFSET, MAPQ, MISMATCH, REF_LEN, REF_OFFSET, REF_OFFSET_TYPE, REF_ORIENTATION, SEQ_SPOT_ID`

**No RAW_READ.** This archive is fully reference-compressed; the aligned bases
live implicitly in REFERENCE + MISMATCH overrides.

### REFERENCE
`CGRAPH_*, CMP_READ, CS_KEY, OVERLAP_REF_LEN, OVERLAP_REF_POS, PRIMARY_ALIGNMENT_IDS, SEQ_ID, SEQ_LEN, SEQ_START`

`CMP_READ` here holds the reference bases in chunks.

## `NCBI:align:seq_restore_read` algorithm

From `libs/axf/seq-restore-read.c:454-574` (impl2, the shipping version).

Inputs:
- `cmp_rd` (SEQUENCE.CMP_READ accessed as `INSDC:4na:bin`): contiguous 4na-bin
  bytes, one nibble-per-byte, for all unaligned portions of the spot concatenated.
- `align_ids` (SEQUENCE.PRIMARY_ALIGNMENT_ID): `num_reads` int64 values. `> 0`
  means aligned; `== 0` means unaligned.
- `read_len` (SEQUENCE.READ_LEN): `num_reads` u32 values — final per-read length.
- `read_type` (SEQUENCE.READ_TYPE): `num_reads` u8 bitfields. For reconstruction
  only the two low bits matter:
  - `0x1` `SRA_READ_TYPE_FORWARD`: copy aligned bases as-is.
  - `0x2` `SRA_READ_TYPE_REVERSE`: copy reversed and 4na-complemented.

Pseudocode:
```
total = sum(read_len); out = reserve(total); cmp_cur = 0
if total == len(cmp_rd): out = cmp_rd          # shortcut: all unaligned
for i in 0..num_reads:
    len = read_len[i]
    if align_ids[i] > 0:
        ar = fetch_primary_alignment_READ(align_ids[i])   # length == len
        if read_type[i] & FORWARD:
            out.extend(ar)
        else if read_type[i] & REVERSE:
            for j in (len-1..=0): out.push(COMPLEMENT_4NA[ar[j] & 0xF])
        else: error
    else:
        out.extend(cmp_rd[cmp_cur..cmp_cur+len]); cmp_cur += len
```

**READ_START is not consulted.** CMP_READ bytes are consumed contiguously in
read-index order.

### 4na reverse-complement table (`seq-restore-read.c:362-379`)

```
0→0 1→8 2→4 3→12 4→2 5→10 6→6 7→14 8→1 9→9 10→5 11→13 12→3 13→11 14→7 15→15
```
A↔T (1↔8), C↔G (2↔4), N↔N (15↔15), ambiguities map to their complement code.

### Fetching `PRIMARY_ALIGNMENT.READ`

`seq-restore-read.c:315` — cursor is opened as `"( INSDC:4na:bin ) READ"`. This
is the virtual READ column on PRIMARY_ALIGNMENT, produced by
`align.vschema:914-916` in priority order:
1. `NCBI:align:align_restore_read(ref_read_internal, HAS_MISMATCH, tmp_mismatch_4na, HAS_REF_OFFSET, REF_OFFSET, READ_LEN)` — the normal reference-compressed path.
2. Same, without trailing `READ_LEN`.
3. `NCBI:align:raw_restore_read(out_raw_read, .REF_ORIENTATION)` — fallback when RAW_READ is physical.

For VDB-3418 (no RAW_READ), we must implement path (1).

### `align_restore_read` algorithm (`libs/axf/align-restore-read.c:45-135`)

Inputs (all 4na-bin / byte arrays unless noted):
- `ref_read` — 4na bases fetched from REFERENCE at the alignment's span.
- `has_mismatch` (byte-per-base, 0/1), length equals output length (READ_LEN).
- `mismatch` — 4na bases packed for the `has_mismatch==1` positions only.
- `has_ref_offset` (byte-per-base, 0/1), same length as `has_mismatch`.
- `ref_offset` — signed i32 values, one per `has_ref_offset==1` position.
  Positive = insertion (skip ref bases); negative = deletion on reference
  (rewind ref cursor).
- `read_len` (optional; if absent, derived from `has_mismatch.len()`).

Pseudocode for ploidy=1 (the Illumina case):
```
mmi = roi = rri = 0
for di in 0..read_len:
    if has_ref_offset[di]:
        rri += ref_offset[roi]
        roi += 1
    if has_mismatch[di]:
        dst[di] = mismatch[mmi]; mmi += 1
    else:
        dst[di] = ref_read[rri]
    rri += 1
```
Note: ploidy > 1 triggers per-segment state reset at `read_len[segment]`
boundaries — not needed for Illumina short reads.

### REFERENCE chunk geometry

- Chunks are rows in the REFERENCE table. Each row covers up to `MAX_SEQ_LEN`
  bases of one reference (typically 5000). A row's bases live in
  `REFERENCE.CMP_READ` (same 4na/2na scheme as SEQUENCE.CMP_READ — 2na packed
  with ALTREAD overlay for ambiguities).
- `MAX_SEQ_LEN` is a table-level constant column (static across rows).
- Mapping PRIMARY_ALIGNMENT's `GLOBAL_REF_START` → REFERENCE row (confirmed
  from `libs/axf/align-local_ref_id.c:124` and `align-local_ref_start.c:128`):
  - `ref_row_id = GLOBAL_REF_START / MAX_SEQ_LEN + 1` (1-based VDB row ID)
  - `offset_in_row = GLOBAL_REF_START % MAX_SEQ_LEN`
- Alignments don't cross reference boundaries; they may cross chunk boundaries
  within one reference.
- Last chunk of each reference has `SEQ_LEN < MAX_SEQ_LEN`. Addresses past
  `SEQ_LEN` in a chunk's nominal slot are unused.

### Strand handling

Two-step process:
1. `align_restore_read` output is in *reference* orientation.
2. `seq_restore_read` (C source lines 531-546) applies strand flip per
   SEQUENCE.READ_TYPE bit:
   - `READ_TYPE & 0x1 (FORWARD)` → copy aligned bases as-is.
   - `READ_TYPE & 0x2 (REVERSE)` → reverse-complement via the 4na LUT.

The `PRIMARY_ALIGNMENT.REF_ORIENTATION` column is redundant information at
decode time — bam-load sets READ_TYPE consistent with REF_ORIENTATION. We only
need READ_TYPE.

## Quality handling

Verified against `interfaces/ncbi/seq.vschema:742-764` (`NCBI:tbl:phred_quality #2.0.6`):

```
phys_qual_phred = .ORIGINAL_QUALITY | .QUALITY
syn_qual_phred  = syn_quality_read (Q30/Q3) | echo<30>
out_qual_phred  = phys_qual_phred | syn_qual_phred
```

For cSRA SEQUENCE rows QUALITY is a **physical full-length column** (present in
VDB-3418). Our existing SRA decode path already handles this correctly. We do
**not** need a cross-table quality join for v1 — the earlier plan's "force
SRA-Lite everywhere" assumption was unnecessarily conservative.

If physical QUALITY is ever absent, the existing `syn_quality_read` / Q30-echo
fallback in the schema matches sracha's current SRA-Lite path.

## Schema variants to gate

`seq_restore_read` is wired up exactly once in the schema: `NCBI:align:tbl:seq #2`
(`align.vschema:1180`). Any archive whose SEQUENCE table inherits from that one
table will follow the same decode rules. Database schema tags can vary
(`alignment_sorted#1.1..#1.3`) but the underlying SEQUENCE table is the same.

## Implications for the implementation plan

1. **Scope grew**: v1 must include REFERENCE-table read + `align_restore_read`.
   A narrow "RAW_READ-physical only" variant covers almost nothing useful.
2. **Quality shrinks**: v1 can read SEQUENCE.QUALITY normally. No SRA-Lite
   forcing required. Byte-identity with `fastq-dump` becomes achievable for
   both bases and quality.
3. **READ_START unused**: simplifies reconstruction loop.
4. **Only one schema variant** to support. Rejection logic for unsupported
   shapes simplifies to "this archive doesn't look like `NCBI:align:tbl:seq#2`"
   plus platform gate.
5. **Output encoding is 4na_bin**: a new 4na→ASCII conversion step is needed on
   the hot path (sracha currently decodes physical READ as 2na via
   `unpack_2na`). Cheap — a 16-byte LUT.

## Open items before Phase 1

- Read `libs/axf/align-restore-read.c` end-to-end; pin down `REF_OFFSET` /
  `REF_OFFSET_TYPE` semantics (indel encoding).
- Read `libs/axf/raw-restore-read.c` line-by-line to confirm the complement
  LUT and understand where REF_ORIENTATION is applied (before or after mismatch
  overlay).
- Understand REFERENCE.CMP_READ chunking: `SEQ_LEN`, `SEQ_START`, chunk-to-ref
  mapping, and whether we need a name-to-chunk index.
- Confirm all the above against a second, independent cSRA fixture (e.g., a
  small real accession rather than a ncbi-vdb test archive).
