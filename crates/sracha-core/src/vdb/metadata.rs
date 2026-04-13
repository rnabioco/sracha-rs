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

use byteorder::{ByteOrder, LittleEndian};

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
    let nodes = parse_meta_pbstree(tree_data)?;

    // Strategy 1: look for READ_0, READ_1, ... metadata nodes.
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

    // Strategy 2: detect platform from embedded schema text.
    // The "schema" node contains the full VDB schema which starts with
    // the table type name (e.g. "NCBI:SRA:Illumina:tbl:phred:v2#1.0.4").
    for node in &nodes {
        if node.name == "schema" {
            // The table type name is typically stored as an attribute of
            // the schema node, not in the schema text value itself.
            for (attr_name, attr_val) in &node.attrs {
                let attr_text = String::from_utf8_lossy(attr_val);
                tracing::debug!("schema attr: {attr_name}={attr_text:?}");
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

/// Infer reads-per-spot from the schema table name.
///
/// SRA-lite files embed the full schema in `md/cur`. The first line is the
/// table type, e.g. `"NCBI:SRA:Illumina:tbl:phred:v2#1.0.4version 1;..."`.
/// Illumina data is almost always paired-end (nreads=2).
fn infer_nreads_from_schema(schema_text: &str) -> Option<usize> {
    // Search the full schema text for Illumina table types.
    if schema_text.contains("Illumina") {
        tracing::info!("metadata: schema indicates Illumina platform, assuming nreads=2");
        return Some(2);
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
// Metadata PBSTree parser
// ---------------------------------------------------------------------------

struct MetaNode {
    name: String,
    value: Vec<u8>,
    #[allow(dead_code)]
    children: Vec<MetaNode>,
    /// Attribute key-value pairs (name → value bytes).
    attrs: Vec<(String, Vec<u8>)>,
}

fn parse_meta_pbstree(buf: &[u8]) -> Result<Vec<MetaNode>, String> {
    if buf.len() < 8 {
        return Ok(Vec::new());
    }

    let num_nodes = LittleEndian::read_u32(&buf[..4]) as usize;
    if num_nodes == 0 {
        return Ok(Vec::new());
    }

    let data_size = LittleEndian::read_u32(&buf[4..8]) as usize;
    let idx_elem_size = if data_size <= 256 {
        1usize
    } else if data_size <= 65536 {
        2
    } else {
        4
    };

    let idx_start = 8usize;
    let idx_bytes = num_nodes * idx_elem_size;
    let data_start = idx_start + idx_bytes;

    if data_start + data_size > buf.len() {
        return Err("metadata PBSTree truncated".into());
    }

    let idx_buf = &buf[idx_start..idx_start + idx_bytes];
    let data_buf = &buf[data_start..data_start + data_size];

    let mut nodes = Vec::with_capacity(num_nodes);
    for i in 0..num_nodes {
        let off = read_idx(idx_buf, i, idx_elem_size);
        let end = if i + 1 < num_nodes {
            read_idx(idx_buf, i + 1, idx_elem_size)
        } else {
            data_size
        };

        if off >= end || end > data_buf.len() {
            continue;
        }

        if let Some(node) = parse_meta_node(&data_buf[off..end]) {
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

    // Parse attributes PBSTree if present.
    // Attribute nodes are simpler: null-terminated name + remaining value.
    let attrs = if has_attrs {
        let attr_start = pos;
        let skip = skip_pbstree(&data[pos..])?;
        let attr_nodes = parse_attr_pbstree(&data[attr_start..attr_start + skip]);
        pos += skip;
        attr_nodes
    } else {
        Vec::new()
    };

    let children = if has_children {
        let child_start = pos;
        let skip = skip_pbstree(&data[pos..])?;
        let child_nodes =
            parse_meta_pbstree(&data[child_start..child_start + skip]).unwrap_or_default();
        pos += skip;
        child_nodes
    } else {
        Vec::new()
    };

    let value = data[pos..].to_vec();

    Some(MetaNode {
        name,
        value,
        children,
        attrs,
    })
}

fn skip_pbstree(buf: &[u8]) -> Option<usize> {
    if buf.len() < 4 {
        return None;
    }

    let num_nodes = LittleEndian::read_u32(&buf[..4]) as usize;
    if num_nodes == 0 {
        return Some(4);
    }

    if buf.len() < 8 {
        return None;
    }

    let data_size = LittleEndian::read_u32(&buf[4..8]) as usize;
    let idx_elem_size = if data_size <= 256 {
        1usize
    } else if data_size <= 65536 {
        2
    } else {
        4
    };

    let total = 8 + num_nodes * idx_elem_size + data_size;
    Some(total)
}

/// Parse attribute nodes from a PBSTree. Attribute nodes store a
/// null-terminated name followed by the value bytes.
fn parse_attr_pbstree(buf: &[u8]) -> Vec<(String, Vec<u8>)> {
    if buf.len() < 8 {
        return Vec::new();
    }

    let num_nodes = LittleEndian::read_u32(&buf[..4]) as usize;
    if num_nodes == 0 {
        return Vec::new();
    }

    let data_size = LittleEndian::read_u32(&buf[4..8]) as usize;
    let idx_elem_size = if data_size <= 256 {
        1usize
    } else if data_size <= 65536 {
        2
    } else {
        4
    };

    let idx_start = 8;
    let idx_bytes = num_nodes * idx_elem_size;
    let data_start = idx_start + idx_bytes;

    if data_start + data_size > buf.len() {
        return Vec::new();
    }

    let idx_buf = &buf[idx_start..idx_start + idx_bytes];
    let data_buf = &buf[data_start..data_start + data_size];

    let mut attrs = Vec::new();
    for i in 0..num_nodes {
        let off = read_idx(idx_buf, i, idx_elem_size);
        let end = if i + 1 < num_nodes {
            read_idx(idx_buf, i + 1, idx_elem_size)
        } else {
            data_size
        };

        if off >= end || end > data_buf.len() {
            continue;
        }

        let node_data = &data_buf[off..end];
        // Attribute: null-terminated name + remaining = value.
        if let Some(nul_pos) = node_data.iter().position(|&b| b == 0) {
            let name = String::from_utf8_lossy(&node_data[..nul_pos]).to_string();
            let value = node_data[nul_pos + 1..].to_vec();
            attrs.push((name, value));
        }
    }

    attrs
}

fn read_idx(idx_buf: &[u8], i: usize, elem_size: usize) -> usize {
    match elem_size {
        1 => idx_buf[i] as usize,
        2 => LittleEndian::read_u16(&idx_buf[i * 2..i * 2 + 2]) as usize,
        _ => LittleEndian::read_u32(&idx_buf[i * 4..i * 4 + 4]) as usize,
    }
}
