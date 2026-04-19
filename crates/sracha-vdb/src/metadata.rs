//! Parse VDB table metadata (`md/cur`) to extract read structure.
//!
//! The `md/cur` file stores a PBSTree of KMDataNode entries. Each node has:
//! - 1 byte: bits  — `(name_len - 1) << 2 | has_children << 1 | has_attrs`
//! - `name_len` bytes: node name (NOT null-terminated)
//! - optional: attributes PBSTree (if has_attrs)
//! - optional: children PBSTree (if has_children)
//! - remaining bytes: node value
//!
//! For SRA-lite files that lack physical READ_LEN/NREADS columns, we extract
//! the read structure from:
//! 1. `READ_0`, `READ_1`, ... metadata nodes (pipe-delimited: `"B|151|"`)
//! 2. The embedded schema name (e.g. `NCBI:SRA:Illumina:...` → nreads=2)

use crate::kar;

/// Read descriptors extracted from metadata.
#[derive(Debug, Clone)]
pub struct ReadDescriptor {
    /// Read type: 'B' (biological) or 'T' (technical).
    pub read_type: u8,
    /// Fixed read length in bases (0 = compute as spot_len / nreads).
    pub read_len: u32,
}

/// Parse the metadata PBSTree (after the 8-byte KDBHdr) and extract
/// read structure info.
pub fn parse_read_structure(tree_data: &[u8]) -> Result<Vec<ReadDescriptor>, String> {
    let nodes = parse_meta_nodes(tree_data)?;

    // Strategy 1: READ_0, READ_1, ... top-level nodes with `B|151|`-style
    // values. Used by older SRA-lite files.
    let mut read_descs: Vec<(u32, ReadDescriptor)> = Vec::new();
    for node in &nodes {
        if let Some(idx) = node.name.strip_prefix("READ_")
            && let Ok(i) = idx.parse::<u32>()
        {
            let val = std::str::from_utf8(&node.value).unwrap_or("");
            if let Some(desc) = parse_read_desc_string(val) {
                read_descs.push((i, desc));
            }
        }
    }
    if !read_descs.is_empty() {
        read_descs.sort_by_key(|(i, _)| *i);
        return Ok(read_descs.into_iter().map(|(_, d)| d).collect());
    }

    // Strategy 2: static columns under `col/{READ_TYPE,READ_LEN}/row`.
    // Per-spot read structure is constant and stored once as a VDB
    // "static" column. Authoritative for both SRA-lite and
    // aligned-schema-labeled archives.
    if let Some(descs) = read_static_col_read_structure(&nodes) {
        return Ok(descs);
    }

    // Strategy 3: detect platform from embedded schema text.
    // The "schema" node contains the full VDB schema which starts with
    // the table type name (e.g. "NCBI:SRA:Illumina:tbl:phred:v2#1.0.4").
    for node in &nodes {
        if node.name == "schema" {
            // The table type name is typically stored as an attribute of
            // the schema node, not in the schema text value itself.
            for (_, attr_val) in &node.attrs {
                let attr_text = String::from_utf8_lossy(attr_val);
                if let Some(nreads) = infer_nreads_from_schema(&attr_text) {
                    let descs = (0..nreads)
                        .map(|_| ReadDescriptor {
                            read_type: b'B',
                            read_len: 0,
                        })
                        .collect();
                    return Ok(descs);
                }
            }
            // Also check the value text.
            let schema_text = String::from_utf8_lossy(&node.value);
            if let Some(nreads) = infer_nreads_from_schema(&schema_text) {
                let descs = (0..nreads)
                    .map(|_| ReadDescriptor {
                        read_type: b'B',
                        read_len: 0,
                    })
                    .collect();
                return Ok(descs);
            }
        }
    }

    // Log what we found for diagnosis.
    let node_names: Vec<&str> = nodes.iter().map(|n| n.name.as_str()).collect();
    Err(format!(
        "no read structure found in metadata ({} top-level nodes: {:?})",
        nodes.len(),
        node_names
    ))
}

/// Read per-read descriptors from `col/{READ_TYPE,READ_LEN}/row` static
/// columns. `READ_TYPE/row` stores one `u8` per read (low bit: 1 =
/// biological, 0 = technical); `READ_LEN/row` stores one little-endian
/// `u32` per read. `nreads` comes from `READ_TYPE/row` because it is
/// exactly one byte per read. When `READ_LEN/row` is absent or
/// mis-sized we emit `read_len=0` so callers fall back to `spot_len /
/// nreads`.
fn read_static_col_read_structure(nodes: &[MetaNode]) -> Option<Vec<ReadDescriptor>> {
    let col = nodes.iter().find(|n| n.name == "col")?;
    let read_type = find_meta_node(&col.children, "READ_TYPE/row")?;
    let nreads = read_type.value.len();
    if nreads == 0 {
        return None;
    }
    let lens_bytes = find_meta_node(&col.children, "READ_LEN/row")
        .map(|n| n.value.as_slice())
        .unwrap_or(&[]);
    let have_lens = lens_bytes.len() == 4 * nreads;

    let mut descs = Vec::with_capacity(nreads);
    for i in 0..nreads {
        let rtype = if read_type.value[i] & 1 != 0 {
            b'B'
        } else {
            b'T'
        };
        let read_len = if have_lens {
            u32::from_le_bytes(lens_bytes[i * 4..i * 4 + 4].try_into().ok()?)
        } else {
            0
        };
        descs.push(ReadDescriptor {
            read_type: rtype,
            read_len,
        });
    }
    Some(descs)
}

/// Return the schema node's attribute name (e.g.
/// `"NCBI:SRA:Illumina:tbl:phred:v2#1.0.4"` or
/// `"NCBI:align:db:alignment_sorted#1.3"`) if present in the table-level
/// metadata tree. Returns `None` when no `schema` node has an attribute.
pub fn schema_attr_name(tree_data: &[u8]) -> Option<String> {
    let nodes = parse_meta_nodes(tree_data).ok()?;
    for node in &nodes {
        if node.name == "schema" {
            for (_, attr_val) in &node.attrs {
                if !attr_val.is_empty() {
                    return Some(String::from_utf8_lossy(attr_val).into_owned());
                }
            }
        }
    }
    None
}

/// Returns true if a schema name indicates an alignment database (cSRA-like).
/// These use `NCBI:align:db:...` or `NCBI:align:tbl:seq` schemas where
/// `READ`/`READ_LEN`/`READ_TYPE` are logical columns synthesized by
/// ncbi-vdb's schema-aware cursor from the alignment representation.
/// sracha's physical-only cursor cannot reconstruct them, so we reject
/// up-front instead of mis-decoding or hanging on fallback heuristics.
pub fn is_aligned_database_schema(schema_name: &str) -> bool {
    schema_name.contains("align:db") || schema_name.contains("align:tbl")
}

/// Read the `STATS/TABLE/CMP_BASE_COUNT` node (if present) — the number of
/// bases stored as compressed reads (CMP_READ), i.e., bases whose recovery
/// requires `seq_restore_read` + the alignment/reference sibling tables.
///
/// Returns:
/// - `Some(0)` — aligned schema went through bam-load but
///   `ConvertDatabaseToUnmapped` (or similar) already moved every base out
///   of CMP_READ, so the physical READ column holds complete sequences and
///   sracha can decode normally.
/// - `Some(N)` with `N > 0` — N bases live only in CMP_READ; decoding them
///   needs the reference and a cross-table join sracha does not implement.
/// - `None` — the node is absent (most non-cSRA runs), no signal either
///   way.
pub fn read_cmp_base_count(tree_data: &[u8]) -> Option<u64> {
    let nodes = parse_meta_nodes(tree_data).ok()?;
    // STATS is a top-level node with nested children; we want STATS/TABLE/CMP_BASE_COUNT.
    fn find_child<'a>(parent: &'a [MetaNode], name: &str) -> Option<&'a MetaNode> {
        parent.iter().find(|n| n.name == name)
    }
    let stats = find_child(&nodes, "STATS")?;
    let table = find_child(&stats.children, "TABLE")?;
    let cmp = find_child(&table.children, "CMP_BASE_COUNT")?;
    if cmp.value.len() >= 8 {
        let mut b = [0u8; 8];
        b.copy_from_slice(&cmp.value[..8]);
        Some(u64::from_le_bytes(b))
    } else {
        None
    }
}

/// Whether the metadata has a top-level `unaligned` node.
///
/// bam-load's `ConvertDatabaseToUnmapped` stamps this marker when it
/// re-expands every row's compressed bases into the physical READ column
/// and removes the cross-table alignment dependency. Runs with this
/// marker decode normally even though the schema still claims to be
/// aligned; runs without it whose schema *is* aligned need
/// `seq_restore_read` and cannot be handled by sracha today.
pub fn has_unaligned_marker(tree_data: &[u8]) -> bool {
    let Ok(nodes) = parse_meta_nodes(tree_data) else {
        return false;
    };
    nodes.iter().any(|n| n.name == "unaligned")
}

/// Extract the sequencing platform from the schema table name.
///
/// NCBI VDB schema names follow the pattern `NCBI:SRA:<Platform>:tbl:...`.
/// Known platforms: Illumina, _454_, ABI (SOLiD), Helicos, PacBio, Nanopore, IonTorrent.
pub fn detect_platform_from_schema(schema_text: &str) -> Option<String> {
    let platforms = [
        ("Illumina", "ILLUMINA"),
        ("_454_", "LS454"),
        ("ABI", "ABI_SOLID"),
        ("Helicos", "HELICOS"),
        ("PacBio", "PACBIO_SMRT"),
        ("Nanopore", "OXFORD_NANOPORE"),
        ("IonTorrent", "ION_TORRENT"),
    ];
    for (schema_name, platform_name) in &platforms {
        if schema_text.contains(schema_name) {
            return Some(platform_name.to_string());
        }
    }
    None
}

/// Infer reads-per-spot from the schema table name.
fn infer_nreads_from_schema(schema_text: &str) -> Option<usize> {
    if schema_text.contains("Illumina") {
        tracing::debug!("metadata: schema indicates Illumina platform, assuming nreads=2");
        return Some(2);
    }
    None
}

/// Detect whether a run is an SRA-Lite (quality-stripped, synthesized-on-read)
/// submission.
///
/// SRA-Lite files are produced by NCBI's `delite` tool, which drops real
/// per-base quality scores and relies on the schema's `syn_quality` function
/// to synthesize a constant Phred score on read — Q30 for pass-filter reads,
/// Q3 for reject. Fasterq-dump honors this and emits the synthesized values,
/// while a naive reader sees the physically stored Q3 bytes and emits `$`
/// instead of `?`. The cleanest signal is the `SOFTWARE/delite` metadata
/// node stamped by the delite pipeline at conversion time.
pub fn detect_sra_lite(tree_data: &[u8]) -> bool {
    let Ok(nodes) = parse_meta_nodes(tree_data) else {
        return false;
    };
    for node in &nodes {
        if node.name == "SOFTWARE" {
            for child in &node.children {
                if child.name.eq_ignore_ascii_case("delite") {
                    return true;
                }
            }
        }
    }
    false
}

/// Detect the sequencing platform from VDB metadata.
///
/// Examines the schema node for platform-identifying strings.
/// Returns `None` if the platform cannot be determined.
pub fn detect_platform(tree_data: &[u8]) -> Option<String> {
    let nodes = parse_meta_nodes(tree_data).ok()?;
    for node in &nodes {
        if node.name == "schema" {
            for (_, attr_val) in &node.attrs {
                let attr_text = String::from_utf8_lossy(attr_val);
                if let Some(platform) = detect_platform_from_schema(&attr_text) {
                    return Some(platform);
                }
            }
            let schema_text = String::from_utf8_lossy(&node.value);
            if let Some(platform) = detect_platform_from_schema(&schema_text) {
                return Some(platform);
            }
        }
    }
    None
}

/// Parse a pipe-delimited read descriptor like "B|151|" or "T|50|".
fn parse_read_desc_string(s: &str) -> Option<ReadDescriptor> {
    let mut parts = s.split('|');
    let rtype_str = parts.next()?;
    let len_str = parts.next()?;
    let rtype = match rtype_str.chars().next()? {
        'B' => b'B',
        'T' => b'T',
        _ => return None,
    };
    let read_len = len_str.parse::<u32>().ok()?;
    Some(ReadDescriptor {
        read_type: rtype,
        read_len,
    })
}

// ---------------------------------------------------------------------------
// Metadata node parser (uses shared PBSTree from kar.rs)
// ---------------------------------------------------------------------------

pub struct MetaNode {
    pub name: String,
    pub value: Vec<u8>,
    /// Attribute key-value pairs (name → value bytes).
    pub attrs: Vec<(String, Vec<u8>)>,
    /// Child nodes (parsed lazily; empty when the node has none on disk).
    pub children: Vec<MetaNode>,
}

/// Parse a metadata node tree from a PBSTree buffer, recursively inflating
/// child PBSTrees so that nested paths like `LOAD/timestamp` and
/// `SOFTWARE/loader` can be resolved.
pub(crate) fn parse_meta_nodes(buf: &[u8]) -> Result<Vec<MetaNode>, String> {
    let slices = kar::parse_pbstree_slices(buf).map_err(|e| e.to_string())?;

    let mut nodes = Vec::with_capacity(slices.len());
    for data in &slices {
        if let Some(node) = parse_meta_node(data) {
            nodes.push(node);
        }
    }
    Ok(nodes)
}

fn parse_meta_node(data: &[u8]) -> Option<MetaNode> {
    if data.is_empty() {
        return None;
    }

    let bits = data[0];
    let name_len = ((bits >> 2) as usize) + 1;
    let has_attrs = bits & 1 != 0;
    let has_children = bits & 2 != 0;

    if 1 + name_len > data.len() {
        return None;
    }

    let name = String::from_utf8_lossy(&data[1..1 + name_len]).to_string();
    let mut pos = 1 + name_len;

    let attrs = if has_attrs {
        let attr_start = pos;
        let skip = kar::pbstree_byte_size(&data[pos..])?;
        let attr_nodes = parse_attr_nodes(&data[attr_start..attr_start + skip]);
        pos += skip;
        attr_nodes
    } else {
        Vec::new()
    };

    let children = if has_children {
        let child_start = pos;
        let skip = kar::pbstree_byte_size(&data[pos..])?;
        let child_buf = &data[child_start..child_start + skip];
        pos += skip;
        parse_meta_nodes(child_buf).unwrap_or_default()
    } else {
        Vec::new()
    };

    let value = data[pos..].to_vec();

    Some(MetaNode {
        name,
        value,
        attrs,
        children,
    })
}

/// Look up a node by `/`-delimited path within a parsed metadata tree.
pub fn find_meta_node<'a>(nodes: &'a [MetaNode], path: &str) -> Option<&'a MetaNode> {
    let mut parts = path.splitn(2, '/');
    let first = parts.next()?;
    let node = nodes.iter().find(|n| n.name == first)?;
    match parts.next() {
        None => Some(node),
        Some(rest) => find_meta_node(&node.children, rest),
    }
}

fn attr_value<'a>(node: &'a MetaNode, key: &str) -> Option<&'a [u8]> {
    node.attrs
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_slice())
}

fn attr_string(node: &MetaNode, key: &str) -> Option<String> {
    attr_value(node, key).map(|b| String::from_utf8_lossy(b).into_owned())
}

/// A single SOFTWARE/{formatter,loader,update} record, mirroring the
/// fields vdb-dump prints for FMT/LDR/UPD lines.
#[derive(Debug, Clone)]
pub struct SoftwareEvent {
    pub name: String,
    pub vers: String,
    pub run_date: String,
    pub tool_date: String,
}

/// Read one SOFTWARE event by sub-path (e.g. `formatter`, `loader`, `update`).
pub fn software_event(nodes: &[MetaNode], sub_path: &str) -> Option<SoftwareEvent> {
    let path = format!("SOFTWARE/{sub_path}");
    let node = find_meta_node(nodes, &path)?;
    let name = attr_string(node, "name")
        .or_else(|| attr_string(node, "tool"))
        .unwrap_or_default();
    if name.is_empty() {
        return None;
    }
    let vers = attr_string(node, "vers").unwrap_or_default();
    let run_date = attr_string(node, "run").unwrap_or_default();
    let tool_date = attr_string(node, "date")
        .or_else(|| attr_string(node, "build"))
        .unwrap_or_default();
    Some(SoftwareEvent {
        name,
        vers,
        run_date,
        tool_date,
    })
}

/// Read the `LOAD/timestamp` Unix-epoch value, if present.
///
/// Mirrors `KMDataNodeReadAsU64`: the value is 1/2/4/8 raw LE bytes depending
/// on the load tool's choice. Returns `None` for any other size.
pub fn load_timestamp(nodes: &[MetaNode]) -> Option<u64> {
    let node = find_meta_node(nodes, "LOAD/timestamp")?;
    match node.value.len() {
        1 => Some(node.value[0] as u64),
        2 => Some(u16::from_le_bytes(node.value[..2].try_into().ok()?) as u64),
        4 => Some(u32::from_le_bytes(node.value[..4].try_into().ok()?) as u64),
        8 => Some(u64::from_le_bytes(node.value[..8].try_into().ok()?)),
        _ => None,
    }
}

/// Return the raw bytes of the embedded schema text (`schema` node value).
pub fn schema_text(nodes: &[MetaNode]) -> Option<&[u8]> {
    let node = nodes.iter().find(|n| n.name == "schema")?;
    if node.value.is_empty() {
        None
    } else {
        Some(&node.value)
    }
}

/// Convenience: parse the metadata bytes of an `md/cur` file (after stripping
/// the 8-byte KDBHdr) into a node tree. Returns an empty Vec on failure.
pub fn parse_md_cur(md_bytes: &[u8]) -> Vec<MetaNode> {
    if md_bytes.len() < 8 {
        return Vec::new();
    }
    parse_meta_nodes(&md_bytes[8..]).unwrap_or_default()
}

/// Parse attribute nodes from a PBSTree. Attribute nodes store a
/// null-terminated name followed by the value bytes.
fn parse_attr_nodes(buf: &[u8]) -> Vec<(String, Vec<u8>)> {
    let slices = match kar::parse_pbstree_slices(buf) {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };

    let mut attrs = Vec::new();
    for node_data in &slices {
        if let Some(nul_pos) = node_data.iter().position(|&b| b == 0) {
            let name = String::from_utf8_lossy(&node_data[..nul_pos]).to_string();
            let value = node_data[nul_pos + 1..].to_vec();
            attrs.push((name, value));
        }
    }
    attrs
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // PBSTree builder helpers for synthetic test data
    // -----------------------------------------------------------------------

    /// Serialize a list of node byte slices into a PBSTree.
    pub(crate) fn build_pbstree(node_data: &[&[u8]]) -> Vec<u8> {
        if node_data.is_empty() {
            return vec![0, 0, 0, 0];
        }

        let mut data_section = Vec::new();
        let mut offsets = Vec::new();
        for nd in node_data {
            offsets.push(data_section.len());
            data_section.extend_from_slice(nd);
        }

        let data_size = data_section.len();
        let num_nodes = node_data.len();

        let mut buf = Vec::new();
        buf.extend_from_slice(&(num_nodes as u32).to_le_bytes());
        buf.extend_from_slice(&(data_size as u32).to_le_bytes());

        // Index entries: 1 byte each for data_size <= 256, 2 for <= 65536, else 4.
        if data_size <= 256 {
            for &off in &offsets {
                buf.push(off as u8);
            }
        } else if data_size <= 65536 {
            for &off in &offsets {
                buf.extend_from_slice(&(off as u16).to_le_bytes());
            }
        } else {
            for &off in &offsets {
                buf.extend_from_slice(&(off as u32).to_le_bytes());
            }
        }

        buf.extend_from_slice(&data_section);
        buf
    }

    /// Build a serialized metadata node.
    ///
    /// Encodes: `bits | name | [attrs_pbstree] | value`
    /// where `bits = (name_len - 1) << 2 | has_children << 1 | has_attrs`.
    pub(crate) fn build_meta_node(name: &str, value: &[u8], attrs: Option<&[u8]>) -> Vec<u8> {
        let name_bytes = name.as_bytes();
        let name_len = name_bytes.len();
        assert!(
            (1..=64).contains(&name_len),
            "name_len must be 1..=64, got {name_len}"
        );

        let has_attrs = attrs.is_some();
        let bits: u8 = ((name_len - 1) as u8) << 2 | u8::from(has_attrs);

        let mut buf = Vec::new();
        buf.push(bits);
        buf.extend_from_slice(name_bytes);
        if let Some(attr_data) = attrs {
            buf.extend_from_slice(attr_data);
        }
        buf.extend_from_slice(value);
        buf
    }

    /// Build an attribute PBSTree from `(name, value)` pairs.
    ///
    /// Each attribute node is serialized as `name\0value`.
    pub(crate) fn build_attrs_pbstree(attrs: &[(&str, &[u8])]) -> Vec<u8> {
        let nodes: Vec<Vec<u8>> = attrs
            .iter()
            .map(|(name, val)| {
                let mut node = Vec::new();
                node.extend_from_slice(name.as_bytes());
                node.push(0);
                node.extend_from_slice(val);
                node
            })
            .collect();
        let refs: Vec<&[u8]> = nodes.iter().map(|n| n.as_slice()).collect();
        build_pbstree(&refs)
    }

    // -----------------------------------------------------------------------
    // parse_read_desc_string
    // -----------------------------------------------------------------------

    #[test]
    fn read_desc_biological() {
        let d = parse_read_desc_string("B|151|").unwrap();
        assert_eq!(d.read_type, b'B');
        assert_eq!(d.read_len, 151);
    }

    #[test]
    fn read_desc_technical() {
        let d = parse_read_desc_string("T|50|").unwrap();
        assert_eq!(d.read_type, b'T');
        assert_eq!(d.read_len, 50);
    }

    #[test]
    fn read_desc_zero_length() {
        let d = parse_read_desc_string("B|0|").unwrap();
        assert_eq!(d.read_type, b'B');
        assert_eq!(d.read_len, 0);
    }

    #[test]
    fn read_desc_no_trailing_pipe() {
        let d = parse_read_desc_string("B|151").unwrap();
        assert_eq!(d.read_len, 151);
    }

    #[test]
    fn read_desc_empty_string() {
        assert!(parse_read_desc_string("").is_none());
    }

    #[test]
    fn read_desc_invalid_type() {
        assert!(parse_read_desc_string("X|100|").is_none());
    }

    #[test]
    fn read_desc_empty_type() {
        assert!(parse_read_desc_string("|151|").is_none());
    }

    #[test]
    fn read_desc_empty_length() {
        assert!(parse_read_desc_string("B||").is_none());
    }

    #[test]
    fn read_desc_non_numeric_length() {
        assert!(parse_read_desc_string("B|abc|").is_none());
    }

    #[test]
    fn read_desc_no_pipe() {
        assert!(parse_read_desc_string("B").is_none());
    }

    // -----------------------------------------------------------------------
    // detect_platform_from_schema
    // -----------------------------------------------------------------------

    #[test]
    fn platform_illumina() {
        assert_eq!(
            detect_platform_from_schema("NCBI:SRA:Illumina:tbl:phred:v2#1.0.4"),
            Some("ILLUMINA".into())
        );
    }

    #[test]
    fn platform_454() {
        assert_eq!(
            detect_platform_from_schema("NCBI:SRA:_454_:tbl:v2"),
            Some("LS454".into())
        );
    }

    #[test]
    fn platform_abi_solid() {
        assert_eq!(
            detect_platform_from_schema("NCBI:SRA:ABI:tbl:v2"),
            Some("ABI_SOLID".into())
        );
    }

    #[test]
    fn platform_helicos() {
        assert_eq!(
            detect_platform_from_schema("NCBI:SRA:Helicos:tbl:v2"),
            Some("HELICOS".into())
        );
    }

    #[test]
    fn platform_pacbio() {
        assert_eq!(
            detect_platform_from_schema("NCBI:SRA:PacBio:tbl:v2"),
            Some("PACBIO_SMRT".into())
        );
    }

    #[test]
    fn platform_nanopore() {
        assert_eq!(
            detect_platform_from_schema("NCBI:SRA:Nanopore:tbl:v2"),
            Some("OXFORD_NANOPORE".into())
        );
    }

    #[test]
    fn platform_iontorrent() {
        assert_eq!(
            detect_platform_from_schema("NCBI:SRA:IonTorrent:tbl:v2"),
            Some("ION_TORRENT".into())
        );
    }

    #[test]
    fn platform_unknown_schema() {
        assert_eq!(detect_platform_from_schema("NCBI:SRA:Unknown:tbl:v2"), None);
    }

    #[test]
    fn platform_empty_string() {
        assert_eq!(detect_platform_from_schema(""), None);
    }

    #[test]
    fn platform_case_sensitive() {
        assert_eq!(detect_platform_from_schema("illumina"), None);
    }

    // -----------------------------------------------------------------------
    // is_aligned_database_schema
    // -----------------------------------------------------------------------

    #[test]
    fn aligned_db_schemas_detected() {
        assert!(is_aligned_database_schema(
            "NCBI:align:db:alignment_sorted#1.3"
        ));
        assert!(is_aligned_database_schema(
            "NCBI:align:db:alignment_unsorted"
        ));
    }

    #[test]
    fn aligned_tbl_schemas_detected() {
        // Regression for iter-4 cSRA detection: DRR032766 carries the
        // `NCBI:align:tbl:seq` schema (table-level, not database-level)
        // but still needs seq_restore_read. The predicate must catch
        // both `align:db` and `align:tbl` forms.
        assert!(is_aligned_database_schema("NCBI:align:tbl:seq#1"));
        assert!(is_aligned_database_schema("NCBI:align:tbl:seq#1.2.1"));
    }

    #[test]
    fn plain_sequence_schemas_not_aligned() {
        assert!(!is_aligned_database_schema(
            "NCBI:SRA:Illumina:tbl:phred:v2#1.0.4"
        ));
        assert!(!is_aligned_database_schema("NCBI:SRA:PacBio:tbl:v2"));
        assert!(!is_aligned_database_schema(""));
    }

    // -----------------------------------------------------------------------
    // infer_nreads_from_schema
    // -----------------------------------------------------------------------

    #[test]
    fn infer_nreads_illumina() {
        assert_eq!(
            infer_nreads_from_schema("NCBI:SRA:Illumina:tbl:phred:v2"),
            Some(2)
        );
    }

    #[test]
    fn infer_nreads_non_illumina() {
        assert_eq!(infer_nreads_from_schema("NCBI:SRA:PacBio:tbl:v2"), None);
    }

    #[test]
    fn infer_nreads_empty() {
        assert_eq!(infer_nreads_from_schema(""), None);
    }

    // -----------------------------------------------------------------------
    // parse_meta_node
    // -----------------------------------------------------------------------

    #[test]
    fn meta_node_simple() {
        let data = build_meta_node("test", b"hello", None);
        let node = parse_meta_node(&data).unwrap();
        assert_eq!(node.name, "test");
        assert_eq!(node.value, b"hello");
        assert!(node.attrs.is_empty());
    }

    #[test]
    fn meta_node_empty_value() {
        let data = build_meta_node("x", b"", None);
        let node = parse_meta_node(&data).unwrap();
        assert_eq!(node.name, "x");
        assert!(node.value.is_empty());
    }

    #[test]
    fn meta_node_with_attrs() {
        let attrs = build_attrs_pbstree(&[("key", b"val")]);
        let data = build_meta_node("n", b"body", Some(&attrs));
        let node = parse_meta_node(&data).unwrap();
        assert_eq!(node.name, "n");
        assert_eq!(node.value, b"body");
        assert_eq!(node.attrs.len(), 1);
        assert_eq!(node.attrs[0].0, "key");
        assert_eq!(node.attrs[0].1, b"val");
    }

    #[test]
    fn meta_node_empty_buf() {
        assert!(parse_meta_node(&[]).is_none());
    }

    #[test]
    fn meta_node_truncated_name() {
        // bits says name_len = 4 but only 2 bytes follow
        assert!(parse_meta_node(&[0x0C, b'a', b'b']).is_none());
    }

    // -----------------------------------------------------------------------
    // parse_read_structure (end-to-end with synthetic PBSTree)
    // -----------------------------------------------------------------------

    #[test]
    fn read_structure_from_read_nodes() {
        let r0 = build_meta_node("READ_0", b"B|151|", None);
        let r1 = build_meta_node("READ_1", b"B|151|", None);
        let tree = build_pbstree(&[&r0, &r1]);

        let descs = parse_read_structure(&tree).unwrap();
        assert_eq!(descs.len(), 2);
        assert_eq!(descs[0].read_type, b'B');
        assert_eq!(descs[0].read_len, 151);
        assert_eq!(descs[1].read_type, b'B');
        assert_eq!(descs[1].read_len, 151);
    }

    #[test]
    fn read_structure_sorts_by_index() {
        // READ_1 appears before READ_0 in the tree — output should be sorted.
        let r1 = build_meta_node("READ_1", b"T|50|", None);
        let r0 = build_meta_node("READ_0", b"B|151|", None);
        let tree = build_pbstree(&[&r1, &r0]);

        let descs = parse_read_structure(&tree).unwrap();
        assert_eq!(descs.len(), 2);
        assert_eq!(descs[0].read_type, b'B');
        assert_eq!(descs[0].read_len, 151);
        assert_eq!(descs[1].read_type, b'T');
        assert_eq!(descs[1].read_len, 50);
    }

    #[test]
    fn read_structure_from_schema_attr() {
        let attrs = build_attrs_pbstree(&[("name", b"NCBI:SRA:Illumina:tbl:phred:v2#1.0.4")]);
        let schema = build_meta_node("schema", b"", Some(&attrs));
        let tree = build_pbstree(&[&schema]);

        let descs = parse_read_structure(&tree).unwrap();
        assert_eq!(descs.len(), 2);
        assert_eq!(descs[0].read_type, b'B');
        assert_eq!(descs[0].read_len, 0);
    }

    #[test]
    fn read_structure_from_schema_value() {
        let schema = build_meta_node("schema", b"NCBI:SRA:Illumina:tbl:phred:v2#1.0.4", None);
        let tree = build_pbstree(&[&schema]);

        let descs = parse_read_structure(&tree).unwrap();
        assert_eq!(descs.len(), 2);
    }

    #[test]
    fn read_structure_prefers_read_nodes_over_schema() {
        let r0 = build_meta_node("READ_0", b"B|100|", None);
        let attrs = build_attrs_pbstree(&[("name", b"NCBI:SRA:Illumina:tbl:phred:v2")]);
        let schema = build_meta_node("schema", b"", Some(&attrs));
        let tree = build_pbstree(&[&r0, &schema]);

        let descs = parse_read_structure(&tree).unwrap();
        // Strategy 1 (READ_ nodes) should win over Strategy 2 (schema).
        assert_eq!(descs.len(), 1);
        assert_eq!(descs[0].read_len, 100);
    }

    #[test]
    fn read_structure_no_info_errors() {
        let other = build_meta_node("other", b"stuff", None);
        let tree = build_pbstree(&[&other]);
        assert!(parse_read_structure(&tree).is_err());
    }

    #[test]
    fn read_structure_empty_tree_errors() {
        let tree = build_pbstree(&[]);
        assert!(parse_read_structure(&tree).is_err());
    }

    /// Build a meta-node with a children PBSTree (for `col/X/row` layout).
    pub(crate) fn build_meta_node_with_children(
        name: &str,
        value: &[u8],
        children: &[&[u8]],
        attrs: Option<&[u8]>,
    ) -> Vec<u8> {
        let name_bytes = name.as_bytes();
        let name_len = name_bytes.len();
        assert!((1..=64).contains(&name_len));
        let has_attrs = attrs.is_some();
        let has_children = !children.is_empty();
        let bits: u8 =
            (((name_len - 1) as u8) << 2) | (u8::from(has_children) << 1) | u8::from(has_attrs);
        let mut buf = Vec::new();
        buf.push(bits);
        buf.extend_from_slice(name_bytes);
        if let Some(attr_data) = attrs {
            buf.extend_from_slice(attr_data);
        }
        if has_children {
            buf.extend_from_slice(&build_pbstree(children));
        }
        buf.extend_from_slice(value);
        buf
    }

    fn static_col_tree(nreads: usize, types: &[u8], lens: &[u32]) -> Vec<u8> {
        assert_eq!(types.len(), nreads);
        let row_type = build_meta_node("row", types, None);
        let read_type = build_meta_node_with_children("READ_TYPE", b"", &[&row_type], None);
        let mut lens_bytes = Vec::with_capacity(lens.len() * 4);
        for &l in lens {
            lens_bytes.extend_from_slice(&l.to_le_bytes());
        }
        let row_len = build_meta_node("row", &lens_bytes, None);
        let read_len = build_meta_node_with_children("READ_LEN", b"", &[&row_len], None);
        let col = build_meta_node_with_children("col", b"", &[&read_type, &read_len], None);
        build_pbstree(&[&col])
    }

    #[test]
    fn read_structure_static_cols_paired_illumina() {
        let tree = static_col_tree(2, &[1, 1], &[151, 151]);
        let descs = parse_read_structure(&tree).unwrap();
        assert_eq!(descs.len(), 2);
        assert_eq!(descs[0].read_type, b'B');
        assert_eq!(descs[0].read_len, 151);
        assert_eq!(descs[1].read_type, b'B');
        assert_eq!(descs[1].read_len, 151);
    }

    #[test]
    fn read_structure_static_cols_technical_barcode() {
        // READ_TYPE low bit 0 => technical.
        let tree = static_col_tree(2, &[0, 1], &[10, 151]);
        let descs = parse_read_structure(&tree).unwrap();
        assert_eq!(descs[0].read_type, b'T');
        assert_eq!(descs[0].read_len, 10);
        assert_eq!(descs[1].read_type, b'B');
        assert_eq!(descs[1].read_len, 151);
    }

    #[test]
    fn read_structure_static_cols_missing_len_keeps_types() {
        // No READ_LEN/row — callers fall back to spot_len / nreads but
        // still get nreads and types.
        let row_type = build_meta_node("row", &[1, 1], None);
        let read_type = build_meta_node_with_children("READ_TYPE", b"", &[&row_type], None);
        let col = build_meta_node_with_children("col", b"", &[&read_type], None);
        let tree = build_pbstree(&[&col]);

        let descs = parse_read_structure(&tree).unwrap();
        assert_eq!(descs.len(), 2);
        assert!(descs.iter().all(|d| d.read_type == b'B'));
        assert!(descs.iter().all(|d| d.read_len == 0));
    }

    #[test]
    fn read_structure_static_cols_beats_schema_fallback() {
        // Even in the presence of an Illumina schema, static cols with
        // concrete lengths should be preferred (Strategy 2 before 3).
        let tree_with_col = static_col_tree(2, &[1, 1], &[75, 76]);
        let descs = parse_read_structure(&tree_with_col).unwrap();
        assert_eq!(descs[0].read_len, 75);
        assert_eq!(descs[1].read_len, 76);
    }

    // -----------------------------------------------------------------------
    // detect_platform (end-to-end with synthetic PBSTree)
    // -----------------------------------------------------------------------

    #[test]
    fn detect_platform_from_schema_attr() {
        let attrs = build_attrs_pbstree(&[("name", b"NCBI:SRA:Illumina:tbl:phred:v2#1.0.4")]);
        let schema = build_meta_node("schema", b"", Some(&attrs));
        let tree = build_pbstree(&[&schema]);
        assert_eq!(detect_platform(&tree), Some("ILLUMINA".into()));
    }

    #[test]
    fn detect_platform_from_schema_value() {
        let schema = build_meta_node("schema", b"NCBI:SRA:PacBio:tbl:v2", None);
        let tree = build_pbstree(&[&schema]);
        assert_eq!(detect_platform(&tree), Some("PACBIO_SMRT".into()));
    }

    #[test]
    fn detect_platform_no_schema_node() {
        let other = build_meta_node("other", b"data", None);
        let tree = build_pbstree(&[&other]);
        assert_eq!(detect_platform(&tree), None);
    }

    #[test]
    fn detect_platform_empty_tree() {
        let tree = build_pbstree(&[]);
        assert_eq!(detect_platform(&tree), None);
    }

    #[test]
    fn detect_platform_unknown_platform() {
        let schema = build_meta_node("schema", b"some_unknown_schema", None);
        let tree = build_pbstree(&[&schema]);
        assert_eq!(detect_platform(&tree), None);
    }

    // -----------------------------------------------------------------------
    // detect_sra_lite
    // -----------------------------------------------------------------------

    #[test]
    fn detect_sra_lite_with_delite_child() {
        // SOFTWARE node with a `delite` child — the marker `delite` stamps on
        // processed SRA-Lite runs (DRR035195).
        let delite = build_meta_node("delite", b"", None);
        let software = build_meta_node_with_children("SOFTWARE", b"", &[&delite], None);
        let tree = build_pbstree(&[&software]);
        assert!(detect_sra_lite(&tree));
    }

    #[test]
    fn detect_sra_lite_without_delite() {
        let loader = build_meta_node("loader", b"", None);
        let software = build_meta_node_with_children("SOFTWARE", b"", &[&loader], None);
        let tree = build_pbstree(&[&software]);
        assert!(!detect_sra_lite(&tree));
    }

    #[test]
    fn detect_sra_lite_empty_tree() {
        let tree = build_pbstree(&[]);
        assert!(!detect_sra_lite(&tree));
    }

    // -----------------------------------------------------------------------
    // read_cmp_base_count
    // -----------------------------------------------------------------------

    #[test]
    fn read_cmp_base_count_present() {
        // STATS/TABLE/CMP_BASE_COUNT holds a u64 LE — the "bases stored
        // compressed via alignment" signal we use to distinguish classic
        // cSRA from ConvertDatabaseToUnmapped-expanded archives.
        let cmp_bc_val = 12345u64.to_le_bytes();
        let cmp_bc = build_meta_node("CMP_BASE_COUNT", &cmp_bc_val, None);
        let table = build_meta_node_with_children("TABLE", b"", &[&cmp_bc], None);
        let stats = build_meta_node_with_children("STATS", b"", &[&table], None);
        let tree = build_pbstree(&[&stats]);
        assert_eq!(read_cmp_base_count(&tree), Some(12345));
    }

    #[test]
    fn read_cmp_base_count_absent() {
        let tree = build_pbstree(&[]);
        assert_eq!(read_cmp_base_count(&tree), None);
    }

    #[test]
    fn read_cmp_base_count_zero() {
        // Zero value should still parse (important: CMP_BASE_COUNT=0 means
        // aligned schema but no compressed bases — safe to decode).
        let cmp_bc_val = 0u64.to_le_bytes();
        let cmp_bc = build_meta_node("CMP_BASE_COUNT", &cmp_bc_val, None);
        let table = build_meta_node_with_children("TABLE", b"", &[&cmp_bc], None);
        let stats = build_meta_node_with_children("STATS", b"", &[&table], None);
        let tree = build_pbstree(&[&stats]);
        assert_eq!(read_cmp_base_count(&tree), Some(0));
    }

    // -----------------------------------------------------------------------
    // has_unaligned_marker
    // -----------------------------------------------------------------------

    #[test]
    fn has_unaligned_marker_present() {
        // `ConvertDatabaseToUnmapped` stamps a top-level `unaligned` node
        // after re-expanding every base back into physical READ. Its
        // presence means sracha can decode even if the schema claims to
        // be aligned. SRR10358300 fixture has this.
        let unaligned = build_meta_node("unaligned", b"", None);
        let tree = build_pbstree(&[&unaligned]);
        assert!(has_unaligned_marker(&tree));
    }

    #[test]
    fn has_unaligned_marker_absent() {
        let stats = build_meta_node("STATS", b"", None);
        let tree = build_pbstree(&[&stats]);
        assert!(!has_unaligned_marker(&tree));
    }
}
