//! High-level VDB cursor for reading the SEQUENCE table from an SRA archive.
//!
//! The [`VdbCursor`] opens all relevant columns in the SEQUENCE table and
//! provides metadata such as spot count and quality availability.

use std::io::{Read, Seek};

use crate::error::{Error, Result};
use crate::kar::KarArchive;
use crate::kdb::ColumnReader;
use crate::metadata::ReadDescriptor;

// ---------------------------------------------------------------------------
// Column names within the SEQUENCE table
// ---------------------------------------------------------------------------

const COL_READ: &str = "READ";
const COL_QUALITY: &str = "QUALITY";
const COL_QUALITY_ALT: &str = "ORIGINAL_QUALITY";
const COL_READ_LEN: &str = "READ_LEN";
const COL_READ_TYPE: &str = "READ_TYPE";
const COL_READ_FILTER: &str = "READ_FILTER";
const COL_NAME: &str = "NAME";
const COL_SPOT_NAME: &str = "SPOT_NAME";
const COL_SPOT_GROUP: &str = "SPOT_GROUP";
/// Illumina name format templates with `$X:$Y` placeholders.
const COL_ALTREAD: &str = "ALTREAD";
const COL_X: &str = "X";
const COL_Y: &str = "Y";
const COL_NAME_FMT: &str = "NAME_FMT";

// ---------------------------------------------------------------------------
// VdbCursor
// ---------------------------------------------------------------------------

/// High-level cursor that reads SEQUENCE table columns and yields spot data.
///
/// The cursor opens the required `READ` column and any available optional
/// columns (`QUALITY`, `READ_LEN`, `READ_TYPE`, `READ_FILTER`, `NAME`,
/// `SPOT_GROUP`).
pub struct VdbCursor {
    read_col: ColumnReader,
    quality_col: Option<ColumnReader>,
    read_len_col: Option<ColumnReader>,
    read_type_col: Option<ColumnReader>,
    read_filter_col: Option<ColumnReader>,
    name_col: Option<ColumnReader>,
    spot_group_col: Option<ColumnReader>,
    /// ALTREAD column: Illumina name format templates with `$X:$Y`.
    altread_col: Option<ColumnReader>,
    /// Per-spot X coordinate (Illumina tile X position).
    x_col: Option<ColumnReader>,
    /// Per-spot Y coordinate (Illumina tile Y position).
    y_col: Option<ColumnReader>,
    /// Per-spot NAME_FMT — when present, holds the (variable-length)
    /// template string that fasterq-dump uses for that specific spot.
    /// Authoritative source of the per-spot template; takes precedence
    /// over the skey-derived spot-range mapping for archives that
    /// store it (typical of Illumina HiSeq archives where two tiles
    /// interleave at fine granularity along the spot axis).
    name_fmt_col: Option<ColumnReader>,
    first_row: i64,
    row_count: u64,
    /// Read descriptors inferred from table metadata (used when READ_LEN
    /// column is absent, e.g. SRA-lite files).
    metadata_read_descs: Option<Vec<ReadDescriptor>>,
    /// Sequencing platform detected from VDB schema metadata.
    platform: Option<String>,
    /// `true` when the metadata shows a `SOFTWARE/delite` node — the tell-tale
    /// signature of an SRA-Lite submission with synthesized quality.
    is_sra_lite: bool,
}

impl VdbCursor {
    /// Open an SRA archive and prepare to read the SEQUENCE table.
    ///
    /// This auto-detects the root accession directory (the first directory
    /// entry in the KAR TOC) and locates the SEQUENCE table columns within it.
    ///
    /// `sra_path` is the path to the SRA file on disk, used for lazy
    /// on-demand reads of column data (avoids loading multi-GiB data files
    /// into memory).
    pub fn open<R: Read + Seek>(
        archive: &mut KarArchive<R>,
        sra_path: &std::path::Path,
    ) -> Result<Self> {
        let seq_col_base = find_sequence_col_base(archive)?;
        reject_if_csra(archive, &seq_col_base)?;

        // Parse table metadata (md/cur) to extract reads_per_spot and
        // platform for SRA-lite files that lack physical READ_LEN/NREADS columns.
        let (metadata_read_descs, platform) = Self::detect_metadata(archive);
        let is_sra_lite = Self::detect_sra_lite(archive);

        // READ is required.
        let read_col = ColumnReader::open(archive, &format!("{seq_col_base}/{COL_READ}"), sra_path)
            .map_err(|_| Error::ColumnNotFound {
                table: "SEQUENCE".into(),
                column: COL_READ.into(),
            })?;

        // Optional columns — open each, swallowing errors.
        // Try QUALITY first, then ORIGINAL_QUALITY as fallback.
        let quality_col =
            ColumnReader::open(archive, &format!("{seq_col_base}/{COL_QUALITY}"), sra_path)
                .or_else(|_| {
                    ColumnReader::open(
                        archive,
                        &format!("{seq_col_base}/{COL_QUALITY_ALT}"),
                        sra_path,
                    )
                })
                .ok();
        let read_len_col =
            ColumnReader::open(archive, &format!("{seq_col_base}/{COL_READ_LEN}"), sra_path).ok();
        let read_type_col = ColumnReader::open(
            archive,
            &format!("{seq_col_base}/{COL_READ_TYPE}"),
            sra_path,
        )
        .ok();
        let read_filter_col = ColumnReader::open(
            archive,
            &format!("{seq_col_base}/{COL_READ_FILTER}"),
            sra_path,
        )
        .ok();
        // NAME column: try NAME first, then SPOT_NAME.
        let name_col = ColumnReader::open(archive, &format!("{seq_col_base}/{COL_NAME}"), sra_path)
            .or_else(|_| {
                ColumnReader::open(
                    archive,
                    &format!("{seq_col_base}/{COL_SPOT_NAME}"),
                    sra_path,
                )
            })
            .ok();
        let spot_group_col = ColumnReader::open(
            archive,
            &format!("{seq_col_base}/{COL_SPOT_GROUP}"),
            sra_path,
        )
        .ok();

        // Illumina name reconstruction columns (ALTREAD templates + X/Y coords).
        let altread_col =
            ColumnReader::open(archive, &format!("{seq_col_base}/{COL_ALTREAD}"), sra_path).ok();
        let x_col = ColumnReader::open(archive, &format!("{seq_col_base}/{COL_X}"), sra_path).ok();
        let y_col = ColumnReader::open(archive, &format!("{seq_col_base}/{COL_Y}"), sra_path).ok();
        let name_fmt_col =
            ColumnReader::open(archive, &format!("{seq_col_base}/{COL_NAME_FMT}"), sra_path).ok();

        if name_col.is_some() {
            tracing::debug!("found physical NAME/SPOT_NAME column");
        } else if altread_col.is_some() && x_col.is_some() && y_col.is_some() {
            if name_fmt_col.is_some() {
                tracing::debug!(
                    "found ALTREAD + X + Y + NAME_FMT columns for Illumina name reconstruction"
                );
            } else {
                tracing::debug!("found ALTREAD + X + Y columns for Illumina name reconstruction");
            }
        }

        let first_row = read_col.first_row_id().unwrap_or(1);
        let row_count = read_col.row_count();

        Ok(Self {
            read_col,
            quality_col,
            read_len_col,
            read_type_col,
            read_filter_col,
            name_col,
            spot_group_col,
            altread_col,
            x_col,
            y_col,
            name_fmt_col,
            first_row,
            row_count,
            metadata_read_descs,
            platform,
            is_sra_lite,
        })
    }

    /// Total number of spots (rows) in the SEQUENCE table.
    pub fn spot_count(&self) -> u64 {
        self.row_count
    }

    /// First row ID in the SEQUENCE table.
    pub fn first_row(&self) -> i64 {
        self.first_row
    }

    /// Whether this SRA has per-base quality scores.
    ///
    /// Returns `false` for SRA-lite files that omit the QUALITY column.
    pub fn has_quality(&self) -> bool {
        self.quality_col.is_some()
    }

    /// Reference to the READ column reader.
    pub fn read_col(&self) -> &ColumnReader {
        &self.read_col
    }

    /// Reference to the QUALITY column reader, if present.
    pub fn quality_col(&self) -> Option<&ColumnReader> {
        self.quality_col.as_ref()
    }

    /// Reference to the READ_LEN column reader, if present.
    pub fn read_len_col(&self) -> Option<&ColumnReader> {
        self.read_len_col.as_ref()
    }

    /// Reference to the READ_TYPE column reader, if present.
    pub fn read_type_col(&self) -> Option<&ColumnReader> {
        self.read_type_col.as_ref()
    }

    /// Reference to the READ_FILTER column reader, if present.
    pub fn read_filter_col(&self) -> Option<&ColumnReader> {
        self.read_filter_col.as_ref()
    }

    /// Reference to the NAME column reader, if present.
    pub fn name_col(&self) -> Option<&ColumnReader> {
        self.name_col.as_ref()
    }

    /// Reference to the SPOT_GROUP column reader, if present.
    pub fn spot_group_col(&self) -> Option<&ColumnReader> {
        self.spot_group_col.as_ref()
    }

    /// Reference to the ALTREAD column reader (Illumina name templates), if present.
    pub fn altread_col(&self) -> Option<&ColumnReader> {
        self.altread_col.as_ref()
    }

    /// Reference to the X column reader (Illumina tile X), if present.
    pub fn x_col(&self) -> Option<&ColumnReader> {
        self.x_col.as_ref()
    }

    /// Reference to the Y column reader (Illumina tile Y), if present.
    pub fn y_col(&self) -> Option<&ColumnReader> {
        self.y_col.as_ref()
    }

    /// Reference to the NAME_FMT column reader (per-spot template
    /// authority), if present.
    pub fn name_fmt_col(&self) -> Option<&ColumnReader> {
        self.name_fmt_col.as_ref()
    }

    /// Whether Illumina name reconstruction is possible — requires X + Y
    /// per-spot coordinates plus skey templates (loaded separately).
    /// ALTREAD is **not** part of the gate: name templates live in the
    /// skey index, not in ALTREAD's bytes (ALTREAD carries the 4na
    /// ambiguity mask and is consulted independently). Some Illumina
    /// archives ship X + Y + skey but no ALTREAD (e.g. DRR041584,
    /// DRR041585) — fasterq-dump still reconstructs deflines for those.
    pub fn has_illumina_name_parts(&self) -> bool {
        self.x_col.is_some() && self.y_col.is_some()
    }

    /// Reads-per-spot inferred from table metadata, if available.
    ///
    /// This is used as a fallback when the physical `READ_LEN` column is
    /// absent (e.g. SRA-lite files). Returns `None` when the metadata
    /// doesn't provide enough info to determine the read structure.
    pub fn metadata_reads_per_spot(&self) -> Option<usize> {
        self.metadata_read_descs.as_ref().map(|d| d.len())
    }

    /// Per-read lengths from VDB metadata, if available.
    ///
    /// Returns `Some` only when all read descriptors have a known
    /// non-zero length. Returns `None` if any length is 0 (variable/unknown).
    pub fn metadata_read_lengths(&self) -> Option<Vec<u32>> {
        let descs = self.metadata_read_descs.as_ref()?;
        if descs.iter().all(|d| d.read_len > 0) {
            Some(descs.iter().map(|d| d.read_len).collect())
        } else {
            None
        }
    }

    /// Per-read technical/biological types from VDB metadata, if available.
    ///
    /// Returns a byte per read: `0` for biological (`B`), `1` for technical
    /// (`T`, any other flag). Used as a fallback when the physical
    /// `READ_TYPE` column is absent — common in older DDBJ/SRA submissions
    /// where read-type info lives only in the schema metadata.
    pub fn metadata_read_types(&self) -> Option<Vec<u8>> {
        let descs = self.metadata_read_descs.as_ref()?;
        Some(
            descs
                .iter()
                .map(|d| if d.read_type == b'B' { 0u8 } else { 1u8 })
                .collect(),
        )
    }

    /// Sequencing platform detected from VDB schema metadata.
    pub fn platform(&self) -> Option<&str> {
        self.platform.as_deref()
    }

    /// Whether the VDB metadata identifies this run as SRA-Lite
    /// (stored quality is synthetic; real per-base qualities were dropped
    /// during the `delite` conversion step).
    pub fn is_sra_lite_schema(&self) -> bool {
        self.is_sra_lite
    }

    // (helpers `parse_skey_offset_table` / `parse_skey_byte_scan` defined
    // at module scope below.)

    /// Load NAME_FMT templates from the skey index.
    ///
    /// The skey file at `tbl/SEQUENCE/idx/skey` (database layout) or
    /// `idx/skey` (flat-table layout) contains name format templates
    /// (e.g. `M05881:542:...:$X:$Y`) that map spot ranges to template strings.
    ///
    /// Returns `(templates, spot_starts)` where `spot_starts[i]` is the first
    /// spot_id that uses `templates[i]`. The last template extends to the end
    /// of the file. Templates are sorted by spot_start.
    pub fn load_name_templates<R: Read + Seek>(
        archive: &mut KarArchive<R>,
    ) -> (Vec<Vec<u8>>, Vec<i64>) {
        // Flat-table archives (e.g. fastq-load-origin DRR019046) drop the
        // `tbl/SEQUENCE/` prefix, so try both layouts before giving up.
        // Without the fallback, name reconstruction silently no-ops on
        // every flat-table accession with X/Y/ALTREAD columns and the
        // defline falls back to "<spot> <spot>" instead of the original
        // Illumina machine ID.
        let skey_data = archive
            .read_file("tbl/SEQUENCE/idx/skey")
            .or_else(|_| archive.read_file("idx/skey"))
            .ok()
            .unwrap_or_default();

        if skey_data.is_empty() {
            return (Vec::new(), Vec::new());
        }

        // Read templates from the skey "PTrie" wrapper. Despite the name,
        // the on-disk layout we see in real archives is a flat
        // offset-indexed string table starting at byte 0x44:
        //
        //   0x44: count        (u32)        — number of templates
        //   0x48: data_bytes   (u32)        — total bytes in template payload
        //   0x4C: offsets      ([u16; cnt]) — start offset of each template
        //                                     within the payload
        //   0x4C + cnt*2: payload          — concatenated null-terminated
        //                                     template strings
        //
        // Template index in this array equals PTrie node_id - 1, so the
        // projection block at the end of the file (count + ord2node +
        // id2ord) maps directly into it without any sort or rename.
        // Falls back to the older byte-scan-for-`$X` heuristic if the
        // offset-table doesn't validate (foreign archive layouts).
        let templates: Vec<Vec<u8>> = parse_skey_offset_table(&skey_data)
            .or_else(|| crate::ptrie::parse_ptrie_templates(&skey_data))
            .unwrap_or_else(|| parse_skey_byte_scan(&skey_data));

        // Parse the skey header to get first spot_id and count.
        // The header is at the start of skey_data. Try v4 (40 bytes) then v2 (32 bytes).
        let (first_spot, last_spot, count, id_bits) = if skey_data.len() >= 40 {
            let endian = u32::from_le_bytes(skey_data[0..4].try_into().unwrap());
            let version = u32::from_le_bytes(skey_data[4..8].try_into().unwrap());

            if endian == 0x05031988 && (version == 3 || version == 4) {
                // v3/v4 header: 16 bytes base + first(8) + last(8) + id_bits(2) + span_bits(2) + align(4)
                let first = i64::from_le_bytes(skey_data[16..24].try_into().unwrap());
                let last = i64::from_le_bytes(skey_data[24..32].try_into().unwrap());
                let id_bits = u16::from_le_bytes(skey_data[32..34].try_into().unwrap());
                (first, last, templates.len(), id_bits as usize)
            } else if endian == 0x05031988 && version <= 2 && skey_data.len() >= 32 {
                // v2: 8 bytes base + first(8) + last(8) + id_bits(2) + span_bits(2) + align(4)
                let first = i64::from_le_bytes(skey_data[8..16].try_into().unwrap());
                let last = i64::from_le_bytes(skey_data[16..24].try_into().unwrap());
                let id_bits = u16::from_le_bytes(skey_data[24..26].try_into().unwrap());
                (first, last, templates.len(), id_bits as usize)
            } else {
                (1i64, 0i64, templates.len(), 0)
            }
        } else {
            (1i64, 0i64, templates.len(), 0)
        };

        tracing::debug!(
            "skey: first={first_spot}, last={last_spot}, count={count}, \
             id_bits={id_bits}, {} templates extracted",
            templates.len()
        );

        // Parse the skey projection data (ord2node + id2ord).
        // Layout after the template table: count(u32) + ord2node[count](u32)
        // + packed_deltas, where packed_deltas is `ceil(span_bits*(count-1)/8)`
        // bytes of MSB-first bit-packed deltas (matches ncbi-vdb's
        // libs/klib/unpack.c). The projection's `count` is **not always**
        // equal to `templates.len()` — DRR040793-style archives carry
        // 188 entries for 96 templates, with `ord2node[i] == 0` marking
        // continuations of the previous non-zero node. Scan every
        // u32-aligned offset and accept the first one whose
        // `(skey_len - offset - 4 - count*4)` matches the expected
        // packed_size for some sensible count.
        //
        // span_bits offset depends on header version:
        //   v2: offset 26 (8-byte base header)
        //   v3/v4: offset 34 (16-byte base header)
        let span_bits = {
            let endian = u32::from_le_bytes(skey_data[0..4].try_into().unwrap());
            let version = u32::from_le_bytes(skey_data[4..8].try_into().unwrap());
            if endian == 0x05031988 && (version == 3 || version == 4) && skey_data.len() >= 36 {
                u16::from_le_bytes(skey_data[34..36].try_into().unwrap()) as usize
            } else if endian == 0x05031988 && version <= 2 && skey_data.len() >= 28 {
                u16::from_le_bytes(skey_data[26..28].try_into().unwrap()) as usize
            } else {
                0
            }
        };

        'skey_search: for scan_pos in 0..skey_data.len().saturating_sub(4) {
            let candidate =
                u32::from_le_bytes(skey_data[scan_pos..scan_pos + 4].try_into().unwrap());
            // Heuristic: candidate must be at least 1 and within a sane
            // absolute upper bound. The packed_size match below is the
            // real anchor that prevents latching onto random u32 noise.
            // We can't bound by templates.len() because the PTrie walker
            // returns a sparse Vec sized to max_node_id (id_coding-shifted),
            // not to the populated count.
            if candidate == 0 || candidate as usize > 10_000_000 {
                continue;
            }
            let proj_count = candidate as usize;
            let ord2node_start = scan_pos + 4;
            let ord2node_end = ord2node_start + proj_count * 4;
            if ord2node_end > skey_data.len() {
                continue;
            }
            let remaining = skey_data.len() - ord2node_end;
            let packed_size = if proj_count > 1 && span_bits > 0 {
                (span_bits * (proj_count - 1)).div_ceil(8)
            } else {
                0
            };
            if remaining != packed_size {
                continue;
            }

            tracing::debug!(
                "skey: found projection count={} at offset {}, packed_size={}, \
                 span_bits={}, templates={}",
                proj_count,
                scan_pos,
                packed_size,
                span_bits,
                templates.len(),
            );

            // Read ord2node entries.
            let ord2node: Vec<u32> = (0..proj_count)
                .map(|j| {
                    u32::from_le_bytes(
                        skey_data[ord2node_start + j * 4..ord2node_start + j * 4 + 4]
                            .try_into()
                            .unwrap(),
                    )
                })
                .collect();

            // Unpack id2ord deltas from span_bits-packed data
            // (MSB-first big-endian bitstream).
            let packed_data = &skey_data[ord2node_end..];
            let mut id2ord: Vec<i64> = vec![0i64; proj_count];
            if proj_count > 1 && span_bits > 0 {
                let mask = (1u64 << span_bits) - 1;
                for i in 0..proj_count - 1 {
                    let bit_offset = i * span_bits;
                    let first_byte = bit_offset / 8;
                    let last_byte = (bit_offset + span_bits - 1) / 8;
                    let mut raw: u128 = 0;
                    for j in first_byte..=last_byte {
                        if j < packed_data.len() {
                            raw = (raw << 8) | packed_data[j] as u128;
                        }
                    }
                    let num_bytes = last_byte - first_byte + 1;
                    let shift = num_bytes * 8 - (bit_offset % 8) - span_bits;
                    let delta = ((raw >> shift) as u64 & mask) as i64;
                    id2ord[i + 1] = delta;
                }
                for i in 1..proj_count {
                    id2ord[i] += id2ord[i - 1];
                }
            }
            for v in &mut id2ord {
                *v += first_spot;
            }

            // Carry-forward zero ord2node values: a zero marks "same
            // template as the previous entry"; this happens on archives
            // where a single tile re-appears in multiple sub-ranges
            // along the spot id axis (DRR040793: 188 sub-ranges across
            // 96 unique tile templates).
            let mut effective: Vec<u32> = Vec::with_capacity(proj_count);
            let mut last_node: u32 = 0;
            for &n in &ord2node {
                let v = if n == 0 {
                    last_node
                } else {
                    last_node = n;
                    n
                };
                effective.push(v);
            }

            tracing::debug!(
                "skey: ord2node[0..5]={:?}, id2ord[0..5]={:?}",
                &ord2node[..ord2node.len().min(5)],
                &id2ord[..id2ord.len().min(5)],
            );

            // Build sorted (template, spot_start) using
            // ord2node → template mapping. node_id N → template[N-1].
            let mut pairs: Vec<(i64, usize)> = id2ord
                .iter()
                .zip(effective.iter())
                .filter(|&(_, &n)| {
                    n > 0
                        && (n as usize) <= templates.len()
                        && !templates[n as usize - 1].is_empty()
                })
                .map(|(&s, &n)| (s, n as usize - 1))
                .collect();
            pairs.sort_by_key(|&(s, _)| s);
            // Collapse runs that map to the same template — only the
            // first spot_start in each run matters for binary search.
            pairs.dedup_by_key(|&mut (_, t)| t);

            let sorted_templates: Vec<Vec<u8>> =
                pairs.iter().map(|&(_, t)| templates[t].clone()).collect();
            let sorted_starts: Vec<i64> = pairs.iter().map(|&(s, _)| s).collect();

            if !sorted_templates.is_empty() {
                tracing::debug!(
                    "skey: {} entries (from {} projection rows), first \
                     spot_start={}, first template={:?}",
                    sorted_templates.len(),
                    proj_count,
                    sorted_starts.first().unwrap_or(&0),
                    String::from_utf8_lossy(&sorted_templates[0]),
                );
                return (sorted_templates, sorted_starts);
            }

            break 'skey_search;
        }

        // Fallback: return templates without spot range info.
        // They'll be assigned sequentially by blob index.
        let starts = vec![0i64; templates.len()];
        (templates, starts)
    }

    /// Detect reads_per_spot and platform from the metadata trees.
    ///
    /// Read structure lives in the **table-level** `tbl/SEQUENCE/md/cur`
    /// (it carries the `col/{READ_TYPE,READ_LEN}/row` static columns and
    /// `READ_0`/`READ_1` nodes). The **database-level** `md/cur` usually
    /// only has `LOAD`, `SOFTWARE`, and a DB-scope `schema` attribute.
    /// We try table-level first, then fall back to DB-level so flat
    /// tables (no `tbl/` wrapper) still work. Platform detection runs on
    /// whichever tree produced a schema attribute.
    fn detect_metadata<R: Read + Seek>(
        archive: &mut KarArchive<R>,
    ) -> (Option<Vec<ReadDescriptor>>, Option<String>) {
        fn try_tree<R: Read + Seek>(
            archive: &mut KarArchive<R>,
            path: &str,
        ) -> Option<(Option<Vec<ReadDescriptor>>, Option<String>)> {
            let md_bytes = archive.read_file(path).ok()?;
            if md_bytes.len() < 8 {
                return None;
            }
            let tree_data = md_bytes[8..].to_vec();
            let rps = match crate::metadata::parse_read_structure(&tree_data) {
                Ok(descs) => Some(descs),
                Err(e) => {
                    tracing::debug!("metadata {path}: no read structure: {e}");
                    None
                }
            };
            let platform = crate::metadata::detect_platform(&tree_data);
            Some((rps, platform))
        }

        let (rps_tbl, plat_tbl) = try_tree(archive, "tbl/SEQUENCE/md/cur").unwrap_or((None, None));
        let (rps_db, plat_db) = try_tree(archive, "md/cur").unwrap_or((None, None));

        let rps = rps_tbl.or(rps_db);
        let platform = plat_tbl.or(plat_db);
        if let Some(ref descs) = rps {
            tracing::debug!("metadata: detected {} reads per spot", descs.len());
        }
        if let Some(ref p) = platform {
            tracing::debug!("metadata: detected platform {p}");
        }
        (rps, platform)
    }

    /// Scan metadata trees (table + database root) for the `SOFTWARE/delite`
    /// node that marks a run as SRA-Lite.
    fn detect_sra_lite<R: Read + Seek>(archive: &mut KarArchive<R>) -> bool {
        for path in ["tbl/SEQUENCE/md/cur", "md/cur"] {
            let Ok(md_bytes) = archive.read_file(path) else {
                continue;
            };
            if md_bytes.len() < 8 {
                continue;
            }
            if crate::metadata::detect_sra_lite(&md_bytes[8..]) {
                tracing::debug!("metadata {path}: SOFTWARE/delite node present — SRA-Lite");
                return true;
            }
        }
        false
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Skey offset of the "PTrie wrapper" preamble inside the skey blob.
/// 0x44 = 40-byte v3/v4 index header + 28 bytes of secondary descriptors.
/// All real archives we've inspected (DRR019046/053011/040793/035881)
/// start the count+offsets+payload trio at exactly this offset.
const SKEY_TABLE_OFFSET: usize = 0x44;

/// Parse skey templates from the flat offset-indexed string table at
/// byte 0x44 of the skey blob. Returns `None` (caller falls back to the
/// byte-scan heuristic) when the layout doesn't validate — short files,
/// nonsense counts, or out-of-range offsets.
fn parse_skey_offset_table(skey_data: &[u8]) -> Option<Vec<Vec<u8>>> {
    if skey_data.len() < SKEY_TABLE_OFFSET + 8 {
        return None;
    }
    let count = u32::from_le_bytes(
        skey_data[SKEY_TABLE_OFFSET..SKEY_TABLE_OFFSET + 4]
            .try_into()
            .ok()?,
    ) as usize;
    let data_bytes = u32::from_le_bytes(
        skey_data[SKEY_TABLE_OFFSET + 4..SKEY_TABLE_OFFSET + 8]
            .try_into()
            .ok()?,
    ) as usize;
    if count == 0 || count > 1_000_000 || data_bytes == 0 || data_bytes > 100 * 1024 * 1024 {
        return None;
    }
    let offsets_start = SKEY_TABLE_OFFSET + 8;
    let offsets_end = offsets_start + count * 2;
    let payload_start = offsets_end;
    let payload_end = payload_start + data_bytes;
    if payload_end > skey_data.len() {
        return None;
    }
    let offsets: Vec<usize> = (0..count)
        .map(|i| {
            u16::from_le_bytes(
                skey_data[offsets_start + i * 2..offsets_start + i * 2 + 2]
                    .try_into()
                    .unwrap(),
            ) as usize
        })
        .collect();
    // First offset must be 0 and offsets must be strictly increasing.
    if offsets[0] != 0 {
        return None;
    }
    for w in offsets.windows(2) {
        if w[1] <= w[0] || w[1] >= data_bytes {
            return None;
        }
    }
    let payload = &skey_data[payload_start..payload_end];
    let mut templates = Vec::with_capacity(count);
    for i in 0..count {
        let s = offsets[i];
        let e = offsets.get(i + 1).copied().unwrap_or(data_bytes);
        // Strip trailing zero-padding (templates are stored padded to a
        // fixed stride per archive but real content is null-terminated).
        let bytes = &payload[s..e];
        let nul = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
        templates.push(bytes[..nul].to_vec());
    }
    // Sanity: every template should contain `$X` (the position
    // placeholder fasterq-dump also looks for); reject the parse if
    // most don't.
    let with_placeholder = templates
        .iter()
        .filter(|t| t.windows(2).any(|w| w == b"$X"))
        .count();
    if with_placeholder * 2 < templates.len() {
        return None;
    }
    Some(templates)
}

/// Legacy fallback: byte-scan for `$X` placeholders, walking back to
/// the previous extracted template's end and trimming a consensus
/// prefix. Used when [`parse_skey_offset_table`] doesn't recognise the
/// layout — kept around for archives whose skey doesn't follow the
/// 0x44-anchored offset-table shape we see on Illumina runs.
fn parse_skey_byte_scan(skey_data: &[u8]) -> Vec<Vec<u8>> {
    let mut templates = Vec::new();
    let mut last_end: usize = 0;
    let mut i = 0;
    while i < skey_data.len() {
        if skey_data[i] == b'$' && i + 1 < skey_data.len() && skey_data[i + 1] == b'X' {
            let mut start = i;
            while start > last_end && skey_data[start - 1] >= 32 && skey_data[start - 1] < 127 {
                start -= 1;
            }
            let mut end = i;
            while end < skey_data.len() && skey_data[end] >= 32 && skey_data[end] < 127 {
                end += 1;
            }
            if end > start {
                templates.push(skey_data[start..end].to_vec());
                last_end = end;
            }
            i = end;
        } else {
            i += 1;
        }
    }
    templates.dedup();
    if templates.len() >= 3 {
        let counts: std::collections::HashMap<&[u8], usize> = templates
            .iter()
            .flat_map(|t| (1..=t.len()).rev().map(move |n| &t[..n]))
            .fold(std::collections::HashMap::new(), |mut m, p| {
                *m.entry(p).or_insert(0) += 1;
                m
            });
        let majority = templates.len().div_ceil(2);
        let consensus_prefix: Vec<u8> = counts
            .iter()
            .filter(|&(_, &c)| c >= majority)
            .max_by_key(|(p, _)| p.len())
            .map(|(p, _)| p.to_vec())
            .unwrap_or_default();
        if !consensus_prefix.is_empty() {
            for t in templates.iter_mut() {
                if let Some(pos) = t
                    .windows(consensus_prefix.len())
                    .position(|w| w == consensus_prefix.as_slice())
                    && pos > 0
                {
                    t.drain(..pos);
                }
            }
            templates.dedup();
        }
    }
    templates
}

/// Find the base path for columns in the SEQUENCE table.
///
/// SRA files come in two layouts:
///
/// 1. **Database layout**: `{root}/tbl/SEQUENCE/col/{COLUMN}/{idx0,idx1,data}`
/// 2. **Flat table layout**: `col/{COLUMN}/{idx0,idx1,data}` (no root prefix,
///    no `tbl/SEQUENCE` — columns are directly under `col/`)
///
/// This function detects which layout is present and returns the path to the
/// `col/` directory containing the column subdirectories.
fn find_sequence_col_base<R: Read + Seek>(archive: &KarArchive<R>) -> Result<String> {
    tracing::debug!(
        "KAR archive has {} entries; looking for SEQUENCE column base",
        archive.entries().len()
    );
    for (path, entry) in archive.entries().iter() {
        tracing::trace!("  KAR entry: {path} ({entry:?})");
    }

    // Strategy 1: look for database-style `tbl/SEQUENCE/col` directory.
    // May be at root (`tbl/SEQUENCE/col`) or under a prefix (`SRR.../tbl/SEQUENCE/col`).
    for path in archive.entries().keys() {
        if (path == "tbl/SEQUENCE/col" || path.ends_with("/tbl/SEQUENCE/col"))
            && matches!(
                archive.entries().get(path.as_str()),
                Some(crate::kar::KarEntry::Directory)
            )
        {
            return Ok(path.clone());
        }
    }

    // Strategy 2: look for `tbl/SEQUENCE/col/` prefix in any entry.
    for path in archive.entries().keys() {
        if path.starts_with("tbl/SEQUENCE/col/") {
            return Ok("tbl/SEQUENCE/col".to_string());
        }
        if let Some(idx) = path.find("/tbl/SEQUENCE/col/") {
            let base = &path[..idx + "/tbl/SEQUENCE/col".len()];
            return Ok(base.to_string());
        }
    }

    // Strategy 3: flat-table layout — columns directly under `col/`.
    // Look for `col/READ` (the required column) as a directory.
    if archive.entries().contains_key("col/READ") {
        return Ok("col".to_string());
    }
    // Or with the common col/READ/data pattern.
    for path in archive.entries().keys() {
        if path.starts_with("col/READ/") {
            return Ok("col".to_string());
        }
    }

    Err(Error::Format(
        "SEQUENCE table not found in KAR archive (tried database and flat-table layouts)".into(),
    ))
}

/// Reject archives that need ncbi-vdb's schema-aware virtual cursor.
///
/// cSRA is NCBI's umbrella term for SRA files built from alignments.
/// The shape that actually blocks a pure-Rust decoder is classic
/// reference-compressed cSRA: a SEQUENCE table with `CMP_READ` (bases of
/// unaligned mate halves only) plus a sibling `PRIMARY_ALIGNMENT` table
/// that stores the aligned bases. Reconstructing per-spot reads requires
/// `NCBI:align:seq_restore_read` cross-table joins which sracha does not
/// yet implement.
///
/// Archives that merely *label* themselves with an aligned schema (e.g.
/// latf-load output that got `NCBI:align:db:alignment_sorted` stamped on,
/// or bam-load's `ConvertDatabaseToUnmapped` pathway which renames
/// CMP_READ→READ) carry a full physical READ column and decode correctly
/// through the existing SRA-lite code path. Only schema label + CMP_READ
/// (or PRIMARY_ALIGNMENT sibling) together mean we must bail out.
fn reject_if_csra<R: Read + Seek>(archive: &mut KarArchive<R>, seq_col_base: &str) -> Result<()> {
    let cmp_read_prefix = format!("{seq_col_base}/CMP_READ");
    let has_cmp_read = archive
        .entries()
        .keys()
        .any(|p| p == &cmp_read_prefix || p.starts_with(&format!("{cmp_read_prefix}/")));
    let has_primary_alignment = archive
        .entries()
        .keys()
        .any(|p| p == "tbl/PRIMARY_ALIGNMENT" || p.contains("/tbl/PRIMARY_ALIGNMENT"));

    // Some runs (e.g. DRR032766) carry the `NCBI:align:tbl:seq` schema but no
    // CMP_READ column and no PRIMARY_ALIGNMENT table — yet the physical READ
    // column still stores bam-load's compressed residue only. The
    // `STATS/TABLE/CMP_BASE_COUNT` metadata node records how many bases are
    // compressed away; when it's non-zero AND the archive has no `unaligned`
    // top-level marker (stamped by `ConvertDatabaseToUnmapped` after it
    // re-expands those bases into the physical READ column), reconstruction
    // via `seq_restore_read` is required and we cannot decode.
    //
    // SRR10358300-style fixtures — aligned schema + CMP_BASE_COUNT > 0 +
    // `unaligned` marker present — decode correctly because the physical
    // READ column already holds complete sequences; the CMP_BASE_COUNT is
    // stale pre-conversion bookkeeping.
    let cmp_base_count = read_cmp_base_count_from_archive(archive);
    let has_unaligned_marker = read_unaligned_marker_from_archive(archive);

    // When the archive has no CMP_READ, no PRIMARY_ALIGNMENT, and a
    // physical READ column, the plain VdbCursor path can handle it —
    // `CMP_BASE_COUNT > 0` on its own is stale pre-conversion metadata
    // left over from bam-load's compressed stage, not evidence the
    // physical READ column is residue-only. This covers DRR017176-class
    // archives (schema NCBI:align:db:alignment_sorted#1.2.1, flat
    // SEQUENCE table, READ present) that vdb-dump / fasterq-dump decode
    // cleanly via the physical READ column.
    let read_col_prefix = format!("{seq_col_base}/READ");
    let has_physical_read = archive
        .entries()
        .keys()
        .any(|p| p == &read_col_prefix || p.starts_with(&format!("{read_col_prefix}/")));
    let has_compressed_bases =
        cmp_base_count.unwrap_or(0) > 0 && !has_unaligned_marker && !has_physical_read;

    if !has_cmp_read && !has_primary_alignment && !has_compressed_bases {
        return Ok(());
    }

    // `pipeline::run_fastq` routes reference-compressed cSRA (CMP_READ +
    // PRIMARY_ALIGNMENT + REFERENCE) through `CsraCursor` before this
    // check fires, so anything arriving here is a rarer aligned-SRA
    // shape that the VDB-direct decoder can't handle — most commonly
    // DRR032766-style CMP_BASE_COUNT>0 without the sibling tables.
    let hint = match read_aligned_schema_name(archive) {
        Some(name) => format!(
            "schema={name}. VDB-direct decode requires a physical READ column. \
             For reference-compressed cSRA with PRIMARY_ALIGNMENT + REFERENCE \
             tables, use `sracha fastq`, which dispatches through CsraCursor. \
             This archive has neither a physical READ nor the alignment+reference \
             tables we need to reconstruct it."
        ),
        None => "reads are reference-compressed and this archive lacks the \
                 PRIMARY_ALIGNMENT + REFERENCE tables sracha's cSRA path needs; \
                 use fasterq-dump from sra-tools for this specific shape."
            .into(),
    };
    Err(Error::UnsupportedFormat {
        format: "aligned SRA (cSRA)".into(),
        hint,
    })
}

/// Scan table and database-level `md/cur` trees for `STATS/TABLE/CMP_BASE_COUNT`
/// and return the first non-`None` value found.
fn read_cmp_base_count_from_archive<R: Read + Seek>(archive: &mut KarArchive<R>) -> Option<u64> {
    for path in ["tbl/SEQUENCE/md/cur", "md/cur"] {
        if let Ok(md_bytes) = archive.read_file(path)
            && md_bytes.len() >= 8
            && let Some(n) = crate::metadata::read_cmp_base_count(&md_bytes[8..])
        {
            return Some(n);
        }
    }
    None
}

/// Scan table and database-level `md/cur` trees for the top-level
/// `unaligned` marker that `ConvertDatabaseToUnmapped` stamps.
fn read_unaligned_marker_from_archive<R: Read + Seek>(archive: &mut KarArchive<R>) -> bool {
    for path in ["tbl/SEQUENCE/md/cur", "md/cur"] {
        if let Ok(md_bytes) = archive.read_file(path)
            && md_bytes.len() >= 8
            && crate::metadata::has_unaligned_marker(&md_bytes[8..])
        {
            return true;
        }
    }
    false
}

/// Probe the table- and db-level `md/cur` trees for a schema attribute that
/// matches an alignment-database pattern (`NCBI:align:db:...`). Returns the
/// schema name when matched; `None` otherwise.
fn read_aligned_schema_name<R: Read + Seek>(archive: &mut KarArchive<R>) -> Option<String> {
    for path in ["md/cur", "tbl/SEQUENCE/md/cur"] {
        if let Ok(md_bytes) = archive.read_file(path)
            && md_bytes.len() >= 8
        {
            let tree_data = &md_bytes[8..];
            if let Some(schema_name) = crate::metadata::schema_attr_name(tree_data)
                && crate::metadata::is_aligned_database_schema(&schema_name)
            {
                return Some(schema_name);
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kar::test_helpers::*;
    use crate::kdb::test_helpers::{build_blob_loc as build_kdb_blob_loc, build_idx1_v1};
    use std::io::Cursor;

    /// Helper: build a minimal KAR archive containing a SEQUENCE table with
    /// a single READ column.
    fn build_minimal_sra_archive() -> Vec<u8> {
        // Column data: 5 bytes of sequence.
        let col_data = b"ACGTN";

        // idx1: v1 header, data_eof = 5, page_size = 1, no checksum
        let idx1 = build_idx1_v1(col_data.len() as u64, 1, 0);

        // idx0: one blob at pg=0, size=5, id_range=1, start_id=1
        let idx0 = build_kdb_blob_loc(0, 5, 1, 1);

        // Assemble the data section.
        let mut data_section = Vec::new();
        let idx1_off = 0u64;
        data_section.extend_from_slice(&idx1);
        let idx0_off = data_section.len() as u64;
        data_section.extend_from_slice(&idx0);
        let data_off = data_section.len() as u64;
        data_section.extend_from_slice(col_data);

        // Build KAR TOC.
        let idx1_node = build_file_node("idx1", idx1_off, idx1.len() as u64);
        let idx0_node = build_file_node("idx0", idx0_off, idx0.len() as u64);
        let data_node = build_file_node("data", data_off, col_data.len() as u64);

        let read_dir = build_dir_node("READ", &[&data_node, &idx0_node, &idx1_node]);
        let col_dir = build_dir_node("col", &[&read_dir]);
        let seq_dir = build_dir_node("SEQUENCE", &[&col_dir]);
        let tbl_dir = build_dir_node("tbl", &[&seq_dir]);
        let root_dir = build_dir_node("SRR000001", &[&tbl_dir]);

        build_kar_archive(&[&root_dir], &data_section)
    }

    /// Write archive bytes to a temporary file and return the path.
    ///
    /// The caller is responsible for cleanup (the temp file will be deleted
    /// when the test process exits thanks to `tempfile`'s auto-cleanup, or
    /// can be cleaned up manually).
    fn write_temp_sra(archive_bytes: &[u8]) -> std::path::PathBuf {
        use std::io::Write;
        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "sracha-test-{}-{}.sra",
            std::process::id(),
            next_id()
        ));
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(archive_bytes).unwrap();
        path
    }

    /// Monotonic counter for unique temp file names across parallel tests.
    fn next_id() -> u64 {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        COUNTER.fetch_add(1, Ordering::Relaxed)
    }

    #[test]
    fn open_cursor_finds_sequence_table() {
        let archive_bytes = build_minimal_sra_archive();
        let sra_path = write_temp_sra(&archive_bytes);
        let mut archive = KarArchive::open(Cursor::new(archive_bytes)).unwrap();
        let cursor = VdbCursor::open(&mut archive, &sra_path).unwrap();

        assert_eq!(cursor.spot_count(), 1);
        assert_eq!(cursor.first_row(), 1);
        // No QUALITY column in our minimal archive.
        assert!(!cursor.has_quality());
        let _ = std::fs::remove_file(&sra_path);
    }

    #[test]
    fn open_cursor_missing_sequence_table() {
        // Empty KAR archive with no SEQUENCE table.
        let archive_bytes = build_kar_archive(&[], b"");
        let sra_path = write_temp_sra(&archive_bytes);
        let mut archive = KarArchive::open(Cursor::new(archive_bytes)).unwrap();
        let result = VdbCursor::open(&mut archive, &sra_path);
        assert!(result.is_err());
        let _ = std::fs::remove_file(&sra_path);
    }

    #[test]
    fn cursor_read_column_accessible() {
        let archive_bytes = build_minimal_sra_archive();
        let sra_path = write_temp_sra(&archive_bytes);
        let mut archive = KarArchive::open(Cursor::new(archive_bytes)).unwrap();
        let cursor = VdbCursor::open(&mut archive, &sra_path).unwrap();

        let data = cursor.read_col().read_blob_for_row(1).unwrap();
        assert_eq!(data, b"ACGTN");
        let _ = std::fs::remove_file(&sra_path);
    }

    #[test]
    fn cursor_optional_columns_are_none() {
        let archive_bytes = build_minimal_sra_archive();
        let sra_path = write_temp_sra(&archive_bytes);
        let mut archive = KarArchive::open(Cursor::new(archive_bytes)).unwrap();
        let cursor = VdbCursor::open(&mut archive, &sra_path).unwrap();

        assert!(cursor.quality_col().is_none());
        assert!(cursor.read_len_col().is_none());
        assert!(cursor.read_type_col().is_none());
        assert!(cursor.read_filter_col().is_none());
        assert!(cursor.name_col().is_none());
        assert!(cursor.spot_group_col().is_none());
        let _ = std::fs::remove_file(&sra_path);
    }

    #[test]
    fn has_illumina_name_parts_false_in_minimal_archive() {
        let archive_bytes = build_minimal_sra_archive();
        let sra_path = write_temp_sra(&archive_bytes);
        let mut archive = KarArchive::open(Cursor::new(archive_bytes)).unwrap();
        let cursor = VdbCursor::open(&mut archive, &sra_path).unwrap();
        assert!(!cursor.has_illumina_name_parts());
        let _ = std::fs::remove_file(&sra_path);
    }

    #[test]
    fn metadata_none_without_md_cur() {
        let archive_bytes = build_minimal_sra_archive();
        let sra_path = write_temp_sra(&archive_bytes);
        let mut archive = KarArchive::open(Cursor::new(archive_bytes)).unwrap();
        let cursor = VdbCursor::open(&mut archive, &sra_path).unwrap();
        assert!(cursor.metadata_reads_per_spot().is_none());
        assert!(cursor.metadata_read_lengths().is_none());
        assert!(cursor.platform().is_none());
        let _ = std::fs::remove_file(&sra_path);
    }

    /// Build a metadata PBSTree (for md/cur) with READ_ nodes and/or schema.
    fn build_metadata_bytes(read_descs: &[(&str, &[u8])], schema_value: Option<&[u8]>) -> Vec<u8> {
        // Build meta nodes. Each metadata node is:
        //   bits(1) | name(N) | value(...)
        // where bits = (name_len - 1) << 2 | has_children << 1 | has_attrs
        let mut nodes: Vec<Vec<u8>> = Vec::new();
        for (name, value) in read_descs {
            let name_bytes = name.as_bytes();
            let bits: u8 = ((name_bytes.len() - 1) as u8) << 2;
            let mut node = vec![bits];
            node.extend_from_slice(name_bytes);
            node.extend_from_slice(value);
            nodes.push(node);
        }
        if let Some(sv) = schema_value {
            let name = b"schema";
            let bits: u8 = ((name.len() - 1) as u8) << 2;
            let mut node = vec![bits];
            node.extend_from_slice(name);
            node.extend_from_slice(sv);
            nodes.push(node);
        }
        let refs: Vec<&[u8]> = nodes.iter().map(|v| v.as_slice()).collect();
        let tree = crate::kar::test_helpers::build_pbstree(&refs);

        // Prepend 8-byte KDBHdr (endian tag + version).
        let mut md = Vec::new();
        md.extend_from_slice(&0x05031988u32.to_le_bytes()); // endian
        md.extend_from_slice(&1u32.to_le_bytes()); // version
        md.extend_from_slice(&tree);
        md
    }

    /// Build a KAR archive with SEQUENCE table + optional md/cur metadata.
    ///
    /// Uses `tbl/SEQUENCE/...` layout directly (no accession root wrapper)
    /// so that `detect_metadata` finds `tbl/SEQUENCE/md/cur` at the expected path.
    fn build_sra_archive_with_metadata(md_bytes: Option<&[u8]>) -> Vec<u8> {
        let col_data = b"ACGTN";
        let idx1 = build_idx1_v1(col_data.len() as u64, 1, 0);
        let idx0 = build_kdb_blob_loc(0, 5, 1, 1);

        let mut data_section = Vec::new();
        let idx1_off = 0u64;
        data_section.extend_from_slice(&idx1);
        let idx0_off = data_section.len() as u64;
        data_section.extend_from_slice(&idx0);
        let data_off = data_section.len() as u64;
        data_section.extend_from_slice(col_data);

        let md_off = data_section.len() as u64;
        let md_len = md_bytes.map(|b| {
            data_section.extend_from_slice(b);
            b.len() as u64
        });

        let idx1_node = build_file_node("idx1", idx1_off, idx1.len() as u64);
        let idx0_node = build_file_node("idx0", idx0_off, idx0.len() as u64);
        let data_node = build_file_node("data", data_off, col_data.len() as u64);

        let read_dir = build_dir_node("READ", &[&data_node, &idx0_node, &idx1_node]);
        let col_dir = build_dir_node("col", &[&read_dir]);
        let seq_dir = if let Some(ml) = md_len {
            let md_file = build_file_node("cur", md_off, ml);
            let md_dir = build_dir_node("md", &[&md_file]);
            build_dir_node("SEQUENCE", &[&col_dir, &md_dir])
        } else {
            build_dir_node("SEQUENCE", &[&col_dir])
        };
        let tbl_dir = build_dir_node("tbl", &[&seq_dir]);

        build_kar_archive(&[&tbl_dir], &data_section)
    }

    #[test]
    fn metadata_from_read_nodes() {
        let md = build_metadata_bytes(&[("READ_0", b"B|151|"), ("READ_1", b"B|151|")], None);
        let archive_bytes = build_sra_archive_with_metadata(Some(&md));
        let sra_path = write_temp_sra(&archive_bytes);
        let mut archive = KarArchive::open(Cursor::new(archive_bytes)).unwrap();
        let cursor = VdbCursor::open(&mut archive, &sra_path).unwrap();

        assert_eq!(cursor.metadata_reads_per_spot(), Some(2));
        assert_eq!(cursor.metadata_read_lengths(), Some(vec![151, 151]));
        let _ = std::fs::remove_file(&sra_path);
    }

    #[test]
    fn metadata_read_types_biological_and_technical() {
        // Regression for iter-2 validation: older DDBJ runs (e.g. DRR000065)
        // declare technical reads in schema metadata rather than a physical
        // READ_TYPE column. Without metadata_read_types + the pipeline
        // fallback, sracha defaulted every read to biological and emitted
        // technical reads as real output, causing FAIL_MISSING vs fasterq.
        let md = build_metadata_bytes(&[("READ_0", b"T|20|"), ("READ_1", b"B|75|")], None);
        let archive_bytes = build_sra_archive_with_metadata(Some(&md));
        let sra_path = write_temp_sra(&archive_bytes);
        let mut archive = KarArchive::open(Cursor::new(archive_bytes)).unwrap();
        let cursor = VdbCursor::open(&mut archive, &sra_path).unwrap();

        // 1 = technical, 0 = biological.
        assert_eq!(cursor.metadata_read_types(), Some(vec![1, 0]));
        let _ = std::fs::remove_file(&sra_path);
    }

    #[test]
    fn metadata_read_types_none_when_metadata_missing() {
        let archive_bytes = build_sra_archive_with_metadata(None);
        let sra_path = write_temp_sra(&archive_bytes);
        let mut archive = KarArchive::open(Cursor::new(archive_bytes)).unwrap();
        let cursor = VdbCursor::open(&mut archive, &sra_path).unwrap();

        assert_eq!(cursor.metadata_read_types(), None);
        let _ = std::fs::remove_file(&sra_path);
    }

    #[test]
    fn metadata_read_lengths_none_when_zero() {
        // READ_ nodes with len=0 → metadata_read_lengths returns None.
        let md = build_metadata_bytes(&[("READ_0", b"B|0|"), ("READ_1", b"B|0|")], None);
        let archive_bytes = build_sra_archive_with_metadata(Some(&md));
        let sra_path = write_temp_sra(&archive_bytes);
        let mut archive = KarArchive::open(Cursor::new(archive_bytes)).unwrap();
        let cursor = VdbCursor::open(&mut archive, &sra_path).unwrap();

        assert_eq!(cursor.metadata_reads_per_spot(), Some(2));
        assert_eq!(cursor.metadata_read_lengths(), None);
        let _ = std::fs::remove_file(&sra_path);
    }

    #[test]
    fn platform_detected_from_schema_value() {
        let md = build_metadata_bytes(&[], Some(b"NCBI:SRA:Illumina:tbl:phred:v2#1.0.4"));
        let archive_bytes = build_sra_archive_with_metadata(Some(&md));
        let sra_path = write_temp_sra(&archive_bytes);
        let mut archive = KarArchive::open(Cursor::new(archive_bytes)).unwrap();
        let cursor = VdbCursor::open(&mut archive, &sra_path).unwrap();

        assert_eq!(cursor.platform(), Some("ILLUMINA"));
        let _ = std::fs::remove_file(&sra_path);
    }

    #[test]
    fn reject_csra_with_cmp_read() {
        // Build archive with CMP_READ column (cSRA indicator).
        let col_data = b"ACGTN";
        let idx1 = build_idx1_v1(col_data.len() as u64, 1, 0);
        let idx0 = build_kdb_blob_loc(0, 5, 1, 1);

        let mut data_section = Vec::new();
        let idx1_off = 0u64;
        data_section.extend_from_slice(&idx1);
        let idx0_off = data_section.len() as u64;
        data_section.extend_from_slice(&idx0);
        let data_off = data_section.len() as u64;
        data_section.extend_from_slice(col_data);

        let idx1_node = build_file_node("idx1", idx1_off, idx1.len() as u64);
        let idx0_node = build_file_node("idx0", idx0_off, idx0.len() as u64);
        let data_node = build_file_node("data", data_off, col_data.len() as u64);

        let read_dir = build_dir_node("READ", &[&data_node, &idx0_node, &idx1_node]);
        // Add a CMP_READ column directory.
        let cmp_data_node = build_file_node("data", data_off, col_data.len() as u64);
        let cmp_read_dir = build_dir_node("CMP_READ", &[&cmp_data_node]);
        let col_dir = build_dir_node("col", &[&read_dir, &cmp_read_dir]);
        let seq_dir = build_dir_node("SEQUENCE", &[&col_dir]);
        let tbl_dir = build_dir_node("tbl", &[&seq_dir]);
        let root_dir = build_dir_node("SRR28588231", &[&tbl_dir]);

        let archive_bytes = build_kar_archive(&[&root_dir], &data_section);
        let sra_path = write_temp_sra(&archive_bytes);
        let mut archive = KarArchive::open(Cursor::new(archive_bytes)).unwrap();
        let result = VdbCursor::open(&mut archive, &sra_path);
        let err = match result {
            Err(e) => e.to_string(),
            Ok(_) => panic!("expected cSRA rejection error"),
        };
        assert!(
            err.contains("aligned SRA") || err.contains("cSRA"),
            "expected cSRA error, got: {err}"
        );
        let _ = std::fs::remove_file(&sra_path);
    }

    /// Build a minimal `md/cur` blob with a single `schema` node carrying
    /// `name=<schema_name>` as an attribute. Shape matches what ncbi-vdb
    /// writes: 8-byte KDBHdr prefix followed by a PBSTree.
    fn build_md_cur_with_schema(schema_name: &str) -> Vec<u8> {
        use crate::metadata::tests::{build_attrs_pbstree, build_meta_node, build_pbstree};
        let attrs = build_attrs_pbstree(&[("name", schema_name.as_bytes())]);
        let schema_node = build_meta_node("schema", b"", Some(&attrs));
        let tree = build_pbstree(&[&schema_node]);
        let mut buf = vec![0u8; 8];
        buf.extend_from_slice(&tree);
        buf
    }

    /// Build an archive: aligned-schema label in tbl/SEQUENCE/md/cur, but
    /// only a physical READ column — no CMP_READ, no PRIMARY_ALIGNMENT.
    /// After the narrowed reject, this shape should open cleanly.
    fn build_align_schema_plain_archive() -> Vec<u8> {
        let col_data = b"ACGTN";
        let idx1 = build_idx1_v1(col_data.len() as u64, 1, 0);
        let idx0 = build_kdb_blob_loc(0, 5, 1, 1);
        let md_cur = build_md_cur_with_schema("NCBI:align:db:alignment_sorted#1.3");

        let mut data_section = Vec::new();
        let idx1_off = 0u64;
        data_section.extend_from_slice(&idx1);
        let idx0_off = data_section.len() as u64;
        data_section.extend_from_slice(&idx0);
        let data_off = data_section.len() as u64;
        data_section.extend_from_slice(col_data);
        let md_cur_off = data_section.len() as u64;
        data_section.extend_from_slice(&md_cur);

        let idx1_node = build_file_node("idx1", idx1_off, idx1.len() as u64);
        let idx0_node = build_file_node("idx0", idx0_off, idx0.len() as u64);
        let data_node = build_file_node("data", data_off, col_data.len() as u64);
        let cur_node = build_file_node("cur", md_cur_off, md_cur.len() as u64);

        let read_dir = build_dir_node("READ", &[&data_node, &idx0_node, &idx1_node]);
        let col_dir = build_dir_node("col", &[&read_dir]);
        let md_dir = build_dir_node("md", &[&cur_node]);
        let seq_dir = build_dir_node("SEQUENCE", &[&col_dir, &md_dir]);
        let tbl_dir = build_dir_node("tbl", &[&seq_dir]);
        build_kar_archive(&[&tbl_dir], &data_section)
    }

    #[test]
    fn accept_align_schema_without_alignment_tables() {
        let archive_bytes = build_align_schema_plain_archive();
        let sra_path = write_temp_sra(&archive_bytes);
        let mut archive = KarArchive::open(Cursor::new(archive_bytes)).unwrap();
        // Narrowed reject_if_csra: schema label alone must not trigger
        // rejection when neither CMP_READ nor PRIMARY_ALIGNMENT is present.
        let cursor = VdbCursor::open(&mut archive, &sra_path)
            .expect("align-schema plain archive should open");
        assert_eq!(cursor.spot_count(), 1);
        let _ = std::fs::remove_file(&sra_path);
    }

    /// Build an md/cur blob with a `schema` node + nested STATS/TABLE/CMP_BASE_COUNT,
    /// optionally preceded by a top-level `unaligned` marker.
    fn build_md_cur_with_csra_stats(
        schema_name: &str,
        cmp_base_count: u64,
        with_unaligned: bool,
    ) -> Vec<u8> {
        use crate::metadata::tests::{
            build_attrs_pbstree, build_meta_node, build_meta_node_with_children, build_pbstree,
        };
        let attrs = build_attrs_pbstree(&[("name", schema_name.as_bytes())]);
        let schema_node = build_meta_node("schema", b"", Some(&attrs));
        let cmp_bc = build_meta_node("CMP_BASE_COUNT", &cmp_base_count.to_le_bytes(), None);
        let table = build_meta_node_with_children("TABLE", b"", &[&cmp_bc], None);
        let stats = build_meta_node_with_children("STATS", b"", &[&table], None);
        let mut children: Vec<&[u8]> = vec![&schema_node, &stats];
        let unaligned;
        if with_unaligned {
            unaligned = build_meta_node("unaligned", b"", None);
            children.push(&unaligned);
        }
        let tree = build_pbstree(&children);
        let mut buf = vec![0u8; 8]; // KDBHdr prefix
        buf.extend_from_slice(&tree);
        buf
    }

    fn build_align_archive_with_csra_stats(cmp_base_count: u64, with_unaligned: bool) -> Vec<u8> {
        let col_data = b"ACGTN";
        let idx1 = build_idx1_v1(col_data.len() as u64, 1, 0);
        let idx0 = build_kdb_blob_loc(0, 5, 1, 1);
        let md_cur =
            build_md_cur_with_csra_stats("NCBI:align:tbl:seq#1", cmp_base_count, with_unaligned);

        let mut data_section = Vec::new();
        let idx1_off = 0u64;
        data_section.extend_from_slice(&idx1);
        let idx0_off = data_section.len() as u64;
        data_section.extend_from_slice(&idx0);
        let data_off = data_section.len() as u64;
        data_section.extend_from_slice(col_data);
        let md_cur_off = data_section.len() as u64;
        data_section.extend_from_slice(&md_cur);

        let idx1_node = build_file_node("idx1", idx1_off, idx1.len() as u64);
        let idx0_node = build_file_node("idx0", idx0_off, idx0.len() as u64);
        let data_node = build_file_node("data", data_off, col_data.len() as u64);
        let cur_node = build_file_node("cur", md_cur_off, md_cur.len() as u64);

        let read_dir = build_dir_node("READ", &[&data_node, &idx0_node, &idx1_node]);
        let col_dir = build_dir_node("col", &[&read_dir]);
        let md_dir = build_dir_node("md", &[&cur_node]);
        let seq_dir = build_dir_node("SEQUENCE", &[&col_dir, &md_dir]);
        let tbl_dir = build_dir_node("tbl", &[&seq_dir]);
        build_kar_archive(&[&tbl_dir], &data_section)
    }

    #[test]
    fn accept_csra_with_nonzero_cmp_base_count_and_physical_read() {
        // Originally pinned the iter-4 DRR032766-class rejection (aligned
        // schema + CMP_BASE_COUNT>0 + no unaligned marker → reject). The
        // real-world follow-up on the csra-support branch showed that
        // these archives still carry a full physical READ column
        // (DRR017176 decodes through the plain VdbCursor path and the
        // output matches fasterq-dump modulo the sracha-wide Q0→N
        // ALTREAD-merge policy). `reject_if_csra` now allows them
        // through when a physical READ is present; this test pins the
        // narrower rejection.
        let archive_bytes = build_align_archive_with_csra_stats(1_000_000, false);
        let sra_path = write_temp_sra(&archive_bytes);
        let mut archive = KarArchive::open(Cursor::new(archive_bytes)).unwrap();
        let cursor = VdbCursor::open(&mut archive, &sra_path)
            .expect("aligned schema + CMP_BASE_COUNT + physical READ should decode");
        assert_eq!(cursor.spot_count(), 1);
        let _ = std::fs::remove_file(&sra_path);
    }

    #[test]
    fn accept_csra_with_unaligned_marker() {
        // Regression for iter-4: SRR10358300-class archives have aligned
        // schema AND non-zero CMP_BASE_COUNT but *also* carry the top-level
        // `unaligned` marker that ConvertDatabaseToUnmapped stamps. Those
        // have a complete physical READ column — sracha can decode them
        // without seq_restore_read and must NOT reject.
        let archive_bytes = build_align_archive_with_csra_stats(1_000_000, true);
        let sra_path = write_temp_sra(&archive_bytes);
        let mut archive = KarArchive::open(Cursor::new(archive_bytes)).unwrap();
        let cursor = VdbCursor::open(&mut archive, &sra_path)
            .expect("aligned + unaligned-marker should decode");
        assert_eq!(cursor.spot_count(), 1);
        let _ = std::fs::remove_file(&sra_path);
    }

    #[test]
    fn accept_csra_with_zero_cmp_base_count() {
        // Aligned schema + CMP_BASE_COUNT==0 means no bases live in a
        // compressed column — safe to decode without seq_restore_read.
        let archive_bytes = build_align_archive_with_csra_stats(0, false);
        let sra_path = write_temp_sra(&archive_bytes);
        let mut archive = KarArchive::open(Cursor::new(archive_bytes)).unwrap();
        let cursor =
            VdbCursor::open(&mut archive, &sra_path).expect("zero CMP_BASE_COUNT should decode");
        assert_eq!(cursor.spot_count(), 1);
        let _ = std::fs::remove_file(&sra_path);
    }

    #[test]
    fn flat_table_layout_detected() {
        // Build a flat-table layout: col/READ/{idx0,idx1,data} (no tbl/ or SEQUENCE/).
        let col_data = b"ACGTN";
        let idx1 = build_idx1_v1(col_data.len() as u64, 1, 0);
        let idx0 = build_kdb_blob_loc(0, 5, 1, 1);

        let mut data_section = Vec::new();
        let idx1_off = 0u64;
        data_section.extend_from_slice(&idx1);
        let idx0_off = data_section.len() as u64;
        data_section.extend_from_slice(&idx0);
        let data_off = data_section.len() as u64;
        data_section.extend_from_slice(col_data);

        let idx1_node = build_file_node("idx1", idx1_off, idx1.len() as u64);
        let idx0_node = build_file_node("idx0", idx0_off, idx0.len() as u64);
        let data_node = build_file_node("data", data_off, col_data.len() as u64);

        let read_dir = build_dir_node("READ", &[&data_node, &idx0_node, &idx1_node]);
        let col_dir = build_dir_node("col", &[&read_dir]);

        let archive_bytes = build_kar_archive(&[&col_dir], &data_section);
        let sra_path = write_temp_sra(&archive_bytes);
        let mut archive = KarArchive::open(Cursor::new(archive_bytes)).unwrap();
        let cursor = VdbCursor::open(&mut archive, &sra_path).unwrap();
        assert_eq!(cursor.spot_count(), 1);
        let _ = std::fs::remove_file(&sra_path);
    }

    #[test]
    fn load_name_templates_empty_on_no_skey() {
        let archive_bytes = build_minimal_sra_archive();
        let mut archive = KarArchive::open(Cursor::new(archive_bytes)).unwrap();
        let (templates, starts) = VdbCursor::load_name_templates(&mut archive);
        assert!(templates.is_empty());
        assert!(starts.is_empty());
    }
}
