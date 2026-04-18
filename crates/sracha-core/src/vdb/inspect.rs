//! Generic VDB-archive structure inspection.
//!
//! Walks the on-disk KAR table-of-contents to enumerate tables, columns, and
//! row ranges without depending on the SRA SEQUENCE schema. Powers the
//! `sracha vdb` discovery commands (the replacement for the most common
//! `vdb-dump` modes: `--info`, `-E`, `-o`, `-A`, `-r`).

use std::io::{Read, Seek};
use std::path::Path;

use crate::error::{Error, Result};
use crate::vdb::blob::decode_blob;
use crate::vdb::kar::{KarArchive, KarEntry};
use crate::vdb::kdb::ColumnReader;
use crate::vdb::metadata::{self, MetaNode, SoftwareEvent};

/// Whether a VDB archive is a Database (has `tbl/` subdirectories) or a
/// flat Table (has `col/` at the root).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VdbKind {
    Database,
    Table,
}

impl VdbKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Database => "Database",
            Self::Table => "Table",
        }
    }
}

/// Detect whether the archive is a Database or a flat Table.
pub fn detect_kind<R: Read + Seek>(archive: &KarArchive<R>) -> Result<VdbKind> {
    if has_dir(archive, "tbl") {
        Ok(VdbKind::Database)
    } else if has_dir(archive, "col") {
        Ok(VdbKind::Table)
    } else {
        Err(Error::Vdb(
            "not a VDB archive: no tbl/ or col/ directory found at root".into(),
        ))
    }
}

fn has_dir<R: Read + Seek>(archive: &KarArchive<R>, path: &str) -> bool {
    matches!(archive.entries().get(path), Some(KarEntry::Directory))
}

/// List table names in a database. Returns an empty Vec for flat tables.
pub fn list_tables<R: Read + Seek>(archive: &KarArchive<R>) -> Result<Vec<String>> {
    match detect_kind(archive)? {
        VdbKind::Database => {
            let mut tables: Vec<String> = archive
                .list_dir("tbl")
                .into_iter()
                .filter(|p| matches!(archive.entries().get(*p), Some(KarEntry::Directory)))
                .filter_map(|p| p.strip_prefix("tbl/").map(str::to_owned))
                .collect();
            tables.sort();
            Ok(tables)
        }
        VdbKind::Table => Ok(Vec::new()),
    }
}

/// Pick a default table: SEQUENCE if present, else first sorted name, else
/// `None` (flat table or empty database).
pub fn default_table<R: Read + Seek>(archive: &KarArchive<R>) -> Result<Option<String>> {
    let tables = list_tables(archive)?;
    if tables.iter().any(|t| t == "SEQUENCE") {
        return Ok(Some("SEQUENCE".into()));
    }
    Ok(tables.into_iter().next())
}

/// List column names in a table. For databases, `table` selects which table
/// (defaulting to SEQUENCE / first). For flat tables, `table` must be `None`.
pub fn list_columns<R: Read + Seek>(
    archive: &KarArchive<R>,
    table: Option<&str>,
) -> Result<Vec<String>> {
    let col_base = column_base_path(archive, table)?;
    let prefix = format!("{col_base}/");
    let mut cols: Vec<String> = archive
        .list_dir(&col_base)
        .into_iter()
        .filter(|p| matches!(archive.entries().get(*p), Some(KarEntry::Directory)))
        .filter_map(|p| p.strip_prefix(&prefix).map(str::to_owned))
        .collect();
    cols.sort();
    Ok(cols)
}

/// Resolve the in-archive path to the `col/` directory of the chosen table.
pub(crate) fn column_base_path_public<R: Read + Seek>(
    archive: &KarArchive<R>,
    table: Option<&str>,
) -> Result<String> {
    column_base_path(archive, table)
}

fn column_base_path<R: Read + Seek>(
    archive: &KarArchive<R>,
    table: Option<&str>,
) -> Result<String> {
    match detect_kind(archive)? {
        VdbKind::Database => {
            let table_name = match table {
                Some(t) => t.to_string(),
                None => default_table(archive)?
                    .ok_or_else(|| Error::Vdb("database has no tables".into()))?,
            };
            let tbl_path = format!("tbl/{table_name}");
            if !has_dir(archive, &tbl_path) {
                return Err(Error::Vdb(format!("table not found: {table_name}")));
            }
            let col_path = format!("{tbl_path}/col");
            if !has_dir(archive, &col_path) {
                return Err(Error::Vdb(format!(
                    "table {table_name} has no col/ directory"
                )));
            }
            Ok(col_path)
        }
        VdbKind::Table => {
            if table.is_some() {
                return Err(Error::Vdb(
                    "this archive is a flat table; --table cannot be specified".into(),
                ));
            }
            Ok("col".into())
        }
    }
}

/// Per-column blob statistics useful for characterising unfamiliar archives
/// (e.g. inferring encoding and row-length from the first blob's header).
#[derive(Debug, Clone)]
pub struct ColumnStats {
    pub name: String,
    pub row_count: u64,
    pub blob_count: usize,
    pub first_row_id: i64,
    pub version: u32,
    pub data_eof: u64,
    pub page_size: u32,
    pub checksum_type: u8,
    /// Decoded first-blob fields; absent when the column is empty or the
    /// first blob fails to decode.
    pub first_blob: Option<FirstBlobStats>,
}

/// Summary of the first blob in a column — captures what the v1/v2 blob
/// header reveals about per-row layout without actually materialising row
/// values.
#[derive(Debug, Clone)]
pub struct FirstBlobStats {
    pub size: u32,
    pub id_range: u32,
    /// For v1 blobs: fixed-row element count per row (e.g. 302 bases/spot).
    pub row_length: Option<u64>,
    pub adjust: u8,
    pub big_endian: bool,
    /// Number of transform-header frames (v2 envelope stack).
    pub header_frames: usize,
    pub has_page_map: bool,
}

/// Collect per-column stats for every column in a table.
pub fn column_stats_all<R: Read + Seek>(
    archive: &mut KarArchive<R>,
    sra_path: &Path,
    table: Option<&str>,
) -> Result<Vec<ColumnStats>> {
    let cols = list_columns(archive, table)?;
    let col_base = column_base_path(archive, table)?;
    let mut out = Vec::with_capacity(cols.len());
    for col in cols {
        let full = format!("{col_base}/{col}");
        match ColumnReader::open(archive, &full, sra_path) {
            Ok(reader) => out.push(build_column_stats(col, &reader)),
            Err(e) => {
                tracing::debug!("column_stats: skipping {full}: {e}");
            }
        }
    }
    Ok(out)
}

fn build_column_stats(name: String, reader: &ColumnReader) -> ColumnStats {
    let meta = reader.meta();
    let first_row_id = reader.first_row_id().unwrap_or(0);
    let first_blob = reader.blobs().first().and_then(|blob| {
        let raw = reader.read_raw_blob_slice(blob.start_id).ok()?;
        let decoded = decode_blob(raw, meta.checksum_type, blob.id_range as u64, 0).ok()?;
        Some(FirstBlobStats {
            size: blob.size,
            id_range: blob.id_range,
            row_length: decoded.row_length,
            adjust: decoded.adjust,
            big_endian: decoded.big_endian,
            header_frames: decoded.headers.len(),
            has_page_map: decoded.page_map.is_some(),
        })
    });
    ColumnStats {
        name,
        row_count: reader.row_count(),
        blob_count: reader.blob_count(),
        first_row_id,
        version: meta.version,
        data_eof: meta.data_eof,
        page_size: meta.page_size,
        checksum_type: meta.checksum_type,
        first_blob,
    }
}

/// Open the chosen column and return `(first_row_id, row_count)`.
///
/// When `column` is `None`, picks the first column alphabetically — matching
/// `vdb-dump --id_range` behavior of using whatever column is available.
pub fn id_range<R: Read + Seek>(
    archive: &mut KarArchive<R>,
    sra_path: &Path,
    table: Option<&str>,
    column: Option<&str>,
) -> Result<(i64, u64)> {
    let col_base = column_base_path(archive, table)?;

    let column_name = match column {
        Some(c) => c.to_string(),
        None => list_columns(archive, table)?
            .into_iter()
            .next()
            .ok_or_else(|| Error::Vdb("no columns available to determine id range".into()))?,
    };

    let col_full = format!("{col_base}/{column_name}");
    let reader = ColumnReader::open(archive, &col_full, sra_path)?;
    let first = reader.first_row_id().unwrap_or(0);
    let count = reader.row_count();
    Ok((first, count))
}

/// Return the row count of the chosen table by opening its first column.
///
/// Mirrors how vdb-dump fills `SEQ` etc. in `--info` output.
pub fn table_row_count<R: Read + Seek>(
    archive: &mut KarArchive<R>,
    sra_path: &Path,
    table: Option<&str>,
) -> Result<u64> {
    Ok(id_range(archive, sra_path, table, None)?.1)
}

/// Read and parse the metadata tree for a given table (or for the root in a
/// flat-table archive). Returns `None` when no `md/cur` file is present.
pub fn read_table_metadata<R: Read + Seek>(
    archive: &mut KarArchive<R>,
    table: Option<&str>,
) -> Option<Vec<MetaNode>> {
    let md_path = match (detect_kind(archive).ok()?, table) {
        (VdbKind::Database, Some(t)) => format!("tbl/{t}/md/cur"),
        (VdbKind::Database, None) => {
            let t = default_table(archive).ok().flatten()?;
            format!("tbl/{t}/md/cur")
        }
        (VdbKind::Table, _) => "md/cur".into(),
    };
    let bytes = archive.read_file(&md_path).ok()?;
    let nodes = metadata::parse_md_cur(&bytes);
    if nodes.is_empty() { None } else { Some(nodes) }
}

/// Read and parse the database-level metadata (root `md/cur`), if present.
pub fn read_db_metadata<R: Read + Seek>(archive: &mut KarArchive<R>) -> Option<Vec<MetaNode>> {
    let bytes = archive.read_file("md/cur").ok()?;
    let nodes = metadata::parse_md_cur(&bytes);
    if nodes.is_empty() { None } else { Some(nodes) }
}

/// One line in a flattened metadata-tree listing.
#[derive(Debug, Clone)]
pub struct MetaNodeSummary {
    /// Path from the chosen root (e.g. `"STATS/TABLE/SPOT_COUNT"`).
    pub path: String,
    /// Number of bytes in the node's raw value (post-attrs, post-children).
    pub value_len: usize,
    /// Printable preview of the value bytes (non-printable → `.`). Truncated
    /// to at most 60 bytes.
    pub preview: String,
    /// Node attributes as `(name, string_preview)` pairs.
    pub attrs: Vec<(String, String)>,
    /// Count of direct children.
    pub child_count: usize,
}

/// Flatten a metadata tree into a depth-first list of nodes, optionally
/// rooted at a sub-path. When `sub_path` is empty, the whole tree is
/// walked. Descends no deeper than `max_depth` (None = unlimited).
pub fn flatten_metadata(
    nodes: &[MetaNode],
    sub_path: &str,
    max_depth: Option<usize>,
) -> Vec<MetaNodeSummary> {
    let roots: Vec<(&MetaNode, String)> = if sub_path.is_empty() {
        nodes.iter().map(|n| (n, n.name.clone())).collect()
    } else {
        match metadata::find_meta_node(nodes, sub_path) {
            Some(n) => vec![(n, sub_path.to_string())],
            None => Vec::new(),
        }
    };
    let mut out = Vec::new();
    for (root, path) in roots {
        walk_meta(root, &path, 0, max_depth, &mut out);
    }
    out
}

fn walk_meta(
    node: &MetaNode,
    path: &str,
    depth: usize,
    max_depth: Option<usize>,
    out: &mut Vec<MetaNodeSummary>,
) {
    out.push(summarise_meta(node, path));
    if let Some(limit) = max_depth
        && depth >= limit
    {
        return;
    }
    for child in &node.children {
        let child_path = format!("{path}/{}", child.name);
        walk_meta(child, &child_path, depth + 1, max_depth, out);
    }
}

fn summarise_meta(node: &MetaNode, path: &str) -> MetaNodeSummary {
    let preview = preview_bytes(&node.value, 60);
    let attrs = node
        .attrs
        .iter()
        .map(|(k, v)| (k.clone(), preview_bytes(v, 40)))
        .collect();
    MetaNodeSummary {
        path: path.to_string(),
        value_len: node.value.len(),
        preview,
        attrs,
        child_count: node.children.len(),
    }
}

fn preview_bytes(bytes: &[u8], max_len: usize) -> String {
    let mut out = String::with_capacity(max_len);
    for b in bytes.iter().take(max_len) {
        out.push(if (32..127).contains(b) {
            *b as char
        } else {
            '.'
        });
    }
    if bytes.len() > max_len {
        out.push('…');
    }
    out
}

/// All info needed to render `sracha vdb info`. Populated by [`gather_info`].
#[derive(Debug, Clone)]
pub struct InfoReport {
    pub kind: VdbKind,
    pub schema_name: Option<String>,
    pub platform: Option<String>,
    pub timestamp: Option<u64>,
    pub formatter: Option<SoftwareEvent>,
    pub loader: Option<SoftwareEvent>,
    pub update: Option<SoftwareEvent>,
    /// (table_name, row_count) pairs for each table in a database, or a single
    /// `("SEQUENCE", count)` style entry for a flat table.
    pub tables: Vec<(String, u64)>,
}

impl InfoReport {
    pub fn primary_row_count(&self) -> Option<u64> {
        self.tables
            .iter()
            .find(|(n, _)| n == "SEQUENCE")
            .or_else(|| self.tables.first())
            .map(|(_, c)| *c)
    }
}

/// Collect everything needed for the `info` command.
pub fn gather_info<R: Read + Seek>(
    archive: &mut KarArchive<R>,
    sra_path: &Path,
) -> Result<InfoReport> {
    let kind = detect_kind(archive)?;

    let table_md = read_table_metadata(archive, None);
    let db_md = read_db_metadata(archive);

    let schema_name = table_md
        .as_ref()
        .and_then(|n| metadata::find_meta_node(n, "schema"))
        .and_then(|n| {
            n.attrs
                .iter()
                .find(|(k, _)| k == "name")
                .map(|(_, v)| String::from_utf8_lossy(v).into_owned())
        })
        .or_else(|| {
            db_md
                .as_ref()
                .and_then(|n| metadata::find_meta_node(n, "schema"))
                .and_then(|n| {
                    n.attrs
                        .iter()
                        .find(|(k, _)| k == "name")
                        .map(|(_, v)| String::from_utf8_lossy(v).into_owned())
                })
        });

    let platform = table_md
        .as_ref()
        .and_then(|nodes| platform_from_nodes(nodes))
        .or_else(|| db_md.as_ref().and_then(|nodes| platform_from_nodes(nodes)));

    let timestamp = table_md
        .as_ref()
        .and_then(|n| metadata::load_timestamp(n))
        .or_else(|| db_md.as_ref().and_then(|n| metadata::load_timestamp(n)));

    let pick_event = |sub: &str| -> Option<SoftwareEvent> {
        table_md
            .as_ref()
            .and_then(|n| metadata::software_event(n, sub))
            .or_else(|| {
                db_md
                    .as_ref()
                    .and_then(|n| metadata::software_event(n, sub))
            })
    };

    let tables = match kind {
        VdbKind::Database => {
            let names = list_tables(archive)?;
            let mut out = Vec::with_capacity(names.len());
            for name in names {
                let count = table_row_count(archive, sra_path, Some(&name)).unwrap_or(0);
                out.push((name, count));
            }
            out
        }
        VdbKind::Table => {
            let count = table_row_count(archive, sra_path, None).unwrap_or(0);
            vec![("SEQUENCE".into(), count)]
        }
    };

    Ok(InfoReport {
        kind,
        schema_name,
        platform,
        timestamp,
        formatter: pick_event("formatter"),
        loader: pick_event("loader"),
        update: pick_event("update"),
        tables,
    })
}

fn platform_from_nodes(nodes: &[MetaNode]) -> Option<String> {
    let schema_node = nodes.iter().find(|n| n.name == "schema")?;
    for (_, attr_val) in &schema_node.attrs {
        let s = String::from_utf8_lossy(attr_val);
        if let Some(p) = metadata::detect_platform_from_schema(&s) {
            return Some(p);
        }
    }
    let s = String::from_utf8_lossy(&schema_node.value);
    metadata::detect_platform_from_schema(&s)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vdb::kar::test_helpers::{
        build_dir_node, build_empty_file_node, build_file_node, build_kar_archive,
    };
    use std::io::Cursor;

    fn make_db_kar() -> Vec<u8> {
        let read_idx1 = build_empty_file_node("idx1");
        let read_dir = build_dir_node("READ", &[&read_idx1]);
        let qual_idx1 = build_empty_file_node("idx1");
        let qual_dir = build_dir_node("QUALITY", &[&qual_idx1]);
        let col_dir = build_dir_node("col", &[&read_dir, &qual_dir]);
        let seq_dir = build_dir_node("SEQUENCE", &[&col_dir]);

        let pa_col_dir = build_dir_node("col", &[]);
        let pa_dir = build_dir_node("PRIMARY_ALIGNMENT", &[&pa_col_dir]);

        let tbl_dir = build_dir_node("tbl", &[&seq_dir, &pa_dir]);
        build_kar_archive(&[&tbl_dir], b"")
    }

    fn make_flat_kar() -> Vec<u8> {
        let read_idx1 = build_empty_file_node("idx1");
        let read_dir = build_dir_node("READ", &[&read_idx1]);
        let col_dir = build_dir_node("col", &[&read_dir]);
        build_kar_archive(&[&col_dir], b"")
    }

    fn make_invalid_kar() -> Vec<u8> {
        let other = build_file_node("readme.txt", 0, 0);
        build_kar_archive(&[&other], b"")
    }

    #[test]
    fn detect_kind_database() {
        let kar = KarArchive::open(Cursor::new(make_db_kar())).unwrap();
        assert_eq!(detect_kind(&kar).unwrap(), VdbKind::Database);
    }

    #[test]
    fn detect_kind_flat_table() {
        let kar = KarArchive::open(Cursor::new(make_flat_kar())).unwrap();
        assert_eq!(detect_kind(&kar).unwrap(), VdbKind::Table);
    }

    #[test]
    fn detect_kind_invalid() {
        let kar = KarArchive::open(Cursor::new(make_invalid_kar())).unwrap();
        assert!(detect_kind(&kar).is_err());
    }

    #[test]
    fn list_tables_db() {
        let kar = KarArchive::open(Cursor::new(make_db_kar())).unwrap();
        let tables = list_tables(&kar).unwrap();
        assert_eq!(tables, vec!["PRIMARY_ALIGNMENT", "SEQUENCE"]);
    }

    #[test]
    fn list_tables_flat() {
        let kar = KarArchive::open(Cursor::new(make_flat_kar())).unwrap();
        assert!(list_tables(&kar).unwrap().is_empty());
    }

    #[test]
    fn default_table_prefers_sequence() {
        let kar = KarArchive::open(Cursor::new(make_db_kar())).unwrap();
        assert_eq!(default_table(&kar).unwrap().as_deref(), Some("SEQUENCE"));
    }

    #[test]
    fn list_columns_db_default_table() {
        let kar = KarArchive::open(Cursor::new(make_db_kar())).unwrap();
        let cols = list_columns(&kar, None).unwrap();
        assert_eq!(cols, vec!["QUALITY", "READ"]);
    }

    #[test]
    fn list_columns_db_named_table() {
        let kar = KarArchive::open(Cursor::new(make_db_kar())).unwrap();
        let cols = list_columns(&kar, Some("PRIMARY_ALIGNMENT")).unwrap();
        assert!(cols.is_empty());
    }

    #[test]
    fn list_columns_flat() {
        let kar = KarArchive::open(Cursor::new(make_flat_kar())).unwrap();
        let cols = list_columns(&kar, None).unwrap();
        assert_eq!(cols, vec!["READ"]);
    }

    #[test]
    fn list_columns_flat_with_table_arg_errors() {
        let kar = KarArchive::open(Cursor::new(make_flat_kar())).unwrap();
        assert!(list_columns(&kar, Some("SEQUENCE")).is_err());
    }

    #[test]
    fn list_columns_unknown_table_errors() {
        let kar = KarArchive::open(Cursor::new(make_db_kar())).unwrap();
        assert!(list_columns(&kar, Some("NOPE")).is_err());
    }

    // -----------------------------------------------------------------------
    // flatten_metadata
    // -----------------------------------------------------------------------

    fn leaf(name: &str, value: &[u8]) -> MetaNode {
        MetaNode {
            name: name.into(),
            value: value.to_vec(),
            attrs: Vec::new(),
            children: Vec::new(),
        }
    }

    fn parent(name: &str, children: Vec<MetaNode>) -> MetaNode {
        MetaNode {
            name: name.into(),
            value: Vec::new(),
            attrs: Vec::new(),
            children,
        }
    }

    fn stats_tree() -> Vec<MetaNode> {
        vec![
            parent(
                "STATS",
                vec![parent(
                    "TABLE",
                    vec![leaf("SPOT_COUNT", b"\x01\x00\x00\x00\x00\x00\x00\x00")],
                )],
            ),
            leaf("schema", b"NCBI:align:tbl:seq#1.1"),
        ]
    }

    #[test]
    fn flatten_metadata_walks_whole_tree() {
        let rows = flatten_metadata(&stats_tree(), "", None);
        let paths: Vec<&str> = rows.iter().map(|r| r.path.as_str()).collect();
        assert_eq!(
            paths,
            vec!["STATS", "STATS/TABLE", "STATS/TABLE/SPOT_COUNT", "schema"]
        );
    }

    #[test]
    fn flatten_metadata_subpath_restricts() {
        let rows = flatten_metadata(&stats_tree(), "STATS/TABLE", None);
        let paths: Vec<&str> = rows.iter().map(|r| r.path.as_str()).collect();
        assert_eq!(paths, vec!["STATS/TABLE", "STATS/TABLE/SPOT_COUNT"]);
    }

    #[test]
    fn flatten_metadata_depth_limit_cuts_children() {
        let rows = flatten_metadata(&stats_tree(), "STATS", Some(0));
        let paths: Vec<&str> = rows.iter().map(|r| r.path.as_str()).collect();
        assert_eq!(paths, vec!["STATS"]);
    }

    #[test]
    fn flatten_metadata_missing_subpath_is_empty() {
        let rows = flatten_metadata(&stats_tree(), "not/here", None);
        assert!(rows.is_empty());
    }

    // -----------------------------------------------------------------------
    // column_stats_all
    // -----------------------------------------------------------------------

    /// Build a minimal single-column KAR with real idx1/idx0/data so that
    /// `ColumnReader::open` succeeds and the first-blob decoder populates
    /// `FirstBlobStats` fields.
    fn single_column_kar() -> (Vec<u8>, std::path::PathBuf) {
        use crate::vdb::kar::test_helpers::{build_dir_node, build_file_node, build_kar_archive};
        use crate::vdb::kdb::test_helpers::{build_blob_loc, build_idx1_v1};
        let col_data = b"ACGTN";
        let idx1 = build_idx1_v1(col_data.len() as u64, 1, 0);
        let idx0 = build_blob_loc(0, col_data.len() as u32, 1, 1);

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
        let seq_dir = build_dir_node("SEQUENCE", &[&col_dir]);
        let tbl_dir = build_dir_node("tbl", &[&seq_dir]);
        let archive = build_kar_archive(&[&tbl_dir], &data_section);

        let path = std::env::temp_dir().join(format!(
            "sracha-inspect-{}-{}.sra",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        std::fs::write(&path, &archive).unwrap();
        (archive, path)
    }

    #[test]
    fn column_stats_all_populates_first_blob() {
        let (bytes, sra_path) = single_column_kar();
        let mut kar = KarArchive::open(Cursor::new(bytes)).unwrap();
        let stats = column_stats_all(&mut kar, &sra_path, None).unwrap();
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].name, "READ");
        assert_eq!(stats[0].row_count, 1);
        assert_eq!(stats[0].blob_count, 1);
        assert_eq!(stats[0].first_row_id, 1);
        let fb = stats[0]
            .first_blob
            .as_ref()
            .expect("first blob should decode");
        assert_eq!(fb.id_range, 1);
        let _ = std::fs::remove_file(&sra_path);
    }
}
