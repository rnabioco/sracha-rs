//! High-level VDB cursor for reading the SEQUENCE table from an SRA archive.
//!
//! The [`VdbCursor`] opens all relevant columns in the SEQUENCE table and
//! provides metadata such as spot count and quality availability.

use std::io::{Read, Seek};

use crate::error::{Error, Result};
use crate::vdb::kar::KarArchive;
use crate::vdb::kdb::ColumnReader;

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
    first_row: i64,
    row_count: u64,
    /// Reads per spot inferred from table metadata (used when READ_LEN
    /// column is absent, e.g. SRA-lite files).
    metadata_reads_per_spot: Option<usize>,
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

        // Parse table metadata (md/cur) to extract reads_per_spot for
        // SRA-lite files that lack physical READ_LEN/NREADS columns.
        let metadata_reads_per_spot = Self::detect_reads_per_spot(archive);

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

        if name_col.is_some() {
            tracing::info!("found physical NAME/SPOT_NAME column");
        } else if altread_col.is_some() && x_col.is_some() && y_col.is_some() {
            tracing::info!("found ALTREAD + X + Y columns for Illumina name reconstruction");
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
            first_row,
            row_count,
            metadata_reads_per_spot,
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

    /// Whether Illumina name reconstruction is possible (ALTREAD + X + Y columns present).
    pub fn has_illumina_name_parts(&self) -> bool {
        self.altread_col.is_some() && self.x_col.is_some() && self.y_col.is_some()
    }

    /// Reads-per-spot inferred from table metadata, if available.
    ///
    /// This is used as a fallback when the physical `READ_LEN` column is
    /// absent (e.g. SRA-lite files). Returns `None` when the metadata
    /// doesn't provide enough info to determine the read structure.
    pub fn metadata_reads_per_spot(&self) -> Option<usize> {
        self.metadata_reads_per_spot
    }

    /// Load NAME_FMT templates from the skey index.
    ///
    /// The skey file at `tbl/SEQUENCE/idx/skey` contains name format templates
    /// (e.g. `M05881:542:...:$X:$Y`) that map spot ranges to template strings.
    ///
    /// Returns `(templates, spot_starts)` where `spot_starts[i]` is the first
    /// spot_id that uses `templates[i]`. The last template extends to the end
    /// of the file. Templates are sorted by spot_start.
    pub fn load_name_templates<R: Read + Seek>(
        archive: &mut KarArchive<R>,
    ) -> (Vec<Vec<u8>>, Vec<i64>) {
        let skey_data = archive
            .read_file("tbl/SEQUENCE/idx/skey")
            .ok()
            .unwrap_or_default();

        if skey_data.is_empty() {
            return (Vec::new(), Vec::new());
        }

        // Extract template strings by scanning for "$X" placeholders.
        let mut templates = Vec::new();
        let mut template_offsets = Vec::new(); // byte offset within skey data
        let mut i = 0;
        while i < skey_data.len() {
            if skey_data[i] == b'$' && i + 1 < skey_data.len() && skey_data[i + 1] == b'X' {
                let mut start = i;
                while start > 0 && skey_data[start - 1] >= 32 && skey_data[start - 1] < 127 {
                    start -= 1;
                }
                let mut end = i;
                while end < skey_data.len() && skey_data[end] >= 32 && skey_data[end] < 127 {
                    end += 1;
                }
                if end > start {
                    template_offsets.push(start);
                    templates.push(skey_data[start..end].to_vec());
                }
                i = end;
            } else {
                i += 1;
            }
        }

        templates.dedup();

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

        tracing::info!(
            "skey: first={first_spot}, last={last_spot}, count={count}, \
             id_bits={id_bits}, {} templates extracted",
            templates.len()
        );

        let expected_count = count as u32;

        // Parse the skey projection data (ord2node + id2ord).
        // Layout after PTrie: count(u32) + ord2node[count](u32) + packed_deltas
        // The packed deltas use span_bits per element with count-1 entries.
        // After unpacking, integrate (prefix sum) to get spot_id offsets.
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

        // Search for ord2node count in the skey data.
        // The remaining bytes after ord2node should equal ceil(span_bits * (count-1) / 8).
        let packed_size = if count > 1 && span_bits > 0 {
            (span_bits * (count - 1)).div_ceil(8)
        } else {
            0
        };

        'skey_search: for scan_pos in 0..skey_data.len().saturating_sub(4) {
            let candidate =
                u32::from_le_bytes(skey_data[scan_pos..scan_pos + 4].try_into().unwrap());
            if candidate != expected_count || candidate == 0 {
                continue;
            }

            let ord2node_start = scan_pos + 4;
            let ord2node_end = ord2node_start + count * 4;
            let remaining = skey_data.len().saturating_sub(ord2node_end);

            if remaining != packed_size {
                continue;
            }

            tracing::debug!(
                "skey: found count={} at offset {}, packed_size={}, span_bits={}",
                candidate,
                scan_pos,
                packed_size,
                span_bits,
            );

            // Read ord2node entries.
            let ord2node: Vec<u32> = (0..count)
                .map(|j| {
                    u32::from_le_bytes(
                        skey_data[ord2node_start + j * 4..ord2node_start + j * 4 + 4]
                            .try_into()
                            .unwrap(),
                    )
                })
                .collect();

            // Unpack id2ord deltas from span_bits-packed data.
            // The packed data is a big-endian bitstream: element[0] occupies
            // the most-significant bits of byte[0], matching the ncbi-vdb
            // Unpack function (libs/klib/unpack.c) which reads right-to-left
            // with byte-swapped 32-bit chunks.
            let packed_data = &skey_data[ord2node_end..];
            let mut id2ord: Vec<i64> = vec![0i64; count];
            if count > 1 && span_bits > 0 {
                let mask = (1u64 << span_bits) - 1;
                for i in 0..count - 1 {
                    let bit_offset = i * span_bits;
                    let first_byte = bit_offset / 8;
                    let last_byte = (bit_offset + span_bits - 1) / 8;
                    // Accumulate relevant bytes in big-endian order.
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
                // Integrate (prefix sum).
                for i in 1..count {
                    id2ord[i] += id2ord[i - 1];
                }
            }

            // Convert offsets to absolute spot_ids.
            for v in &mut id2ord {
                *v += first_spot;
            }

            tracing::debug!(
                "skey: ord2node[0..5]={:?}, id2ord[0..5]={:?}",
                &ord2node[..ord2node.len().min(5)],
                &id2ord[..id2ord.len().min(5)],
            );

            // Build sorted (template, spot_start) using ord2node → template mapping.
            // node_id N maps to template[N-1] (1-indexed in PTrie).
            let mut pairs: Vec<(i64, usize)> = id2ord
                .iter()
                .zip(ord2node.iter())
                .map(|(&spot_start, &node_id)| (spot_start, node_id.saturating_sub(1) as usize))
                .collect();
            pairs.sort_by_key(|&(s, _)| s);

            let mut sorted_templates = Vec::with_capacity(count);
            let mut sorted_starts = Vec::with_capacity(count);
            for (spot_start, tmpl_idx) in &pairs {
                if *tmpl_idx < templates.len() {
                    sorted_templates.push(templates[*tmpl_idx].clone());
                    sorted_starts.push(*spot_start);
                }
            }

            if !sorted_templates.is_empty() {
                tracing::info!(
                    "skey: {} entries, first spot_start={}, first template={:?}",
                    sorted_templates.len(),
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

    /// Detect reads_per_spot from the table metadata (`md/cur`).
    ///
    /// Strategy:
    /// 1. Try `READ_0`/`READ_1` metadata nodes (pipe-delimited descriptors).
    /// 2. Look for NREADS value in the STATS metadata tree.
    /// 3. Detect schema table name — if it contains "Illumina", the data
    ///    is almost certainly paired-end (nreads=2).
    fn detect_reads_per_spot<R: Read + Seek>(archive: &mut KarArchive<R>) -> Option<usize> {
        // Try table-level metadata first, then database-level.
        let md_bytes = match archive.read_file("md/cur") {
            Ok(b) => b,
            Err(_) => match archive.read_file("tbl/SEQUENCE/md/cur") {
                Ok(b) => b,
                Err(e) => {
                    tracing::debug!("no md/cur found: {e}");
                    return None;
                }
            },
        };

        if md_bytes.len() < 8 {
            return None;
        }

        // Skip 8-byte KDBHdr (endian + version).
        let tree_data = &md_bytes[8..];
        match crate::vdb::metadata::parse_read_structure(tree_data) {
            Ok(descs) => {
                let rps = descs.len();
                tracing::info!("metadata: detected {rps} reads per spot");
                Some(rps)
            }
            Err(e) => {
                tracing::debug!("metadata: could not determine read structure: {e}");
                None
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

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
                Some(crate::vdb::kar::KarEntry::Directory)
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

    Err(Error::Vdb(
        "SEQUENCE table not found in KAR archive (tried database and flat-table layouts)".into(),
    ))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vdb::kar::test_helpers::*;
    use crate::vdb::kdb::test_helpers::{build_blob_loc as build_kdb_blob_loc, build_idx1_v1};
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
}
