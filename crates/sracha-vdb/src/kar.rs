//! KAR (K Archive) parser for NCBI SRA `.sra` files.
//!
//! The KAR format is the container format used by NCBI's Sequence Read Archive.
//! It consists of a header, a table of contents (TOC) stored as a Persistent
//! Binary Search Tree (PBSTree), and a data section containing concatenated
//! file contents.
//!
//! # Binary Layout
//!
//! ```text
//! [Header: 24 bytes]
//!   bytes 0..4:   "NCBI" magic
//!   bytes 4..8:   ".sra"
//!   bytes 8..12:  byte_order tag (0x05031988 native, reversed for swap)
//!   bytes 12..16: version (u32, currently 1)
//!   bytes 16..24: file_offset (u64, start of data section)
//!
//! [TOC: from byte 24 to file_offset]
//!   PBSTree containing directory structure
//!
//! [Data section: from file_offset to EOF]
//!   Concatenated file contents, 4-byte aligned
//! ```

use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom};

use byteorder::{ByteOrder, LittleEndian, ReadBytesExt};

use crate::error::{Error, Result};

// KAR format constants
const MAGIC_NCBI: [u8; 4] = *b"NCBI";
const MAGIC_SRA: [u8; 4] = *b".sra";
const BYTE_ORDER_TAG: u32 = 0x05031988;
const BYTE_ORDER_REVERSED: u32 = 0x88190305;
const HEADER_SIZE: u64 = 24; // 4 + 4 + 4 + 4 + 8

// TOC entry type codes (from kar.c enum, starting at -1):
//   unknown=-1, notfound=0, dir=1, file=2, chunked=3,
//   softlink=4, hardlink=5, emptyfile=6, zombiefile=7
const TOC_TYPE_DIR: u8 = 1;
const TOC_TYPE_FILE: u8 = 2;
const TOC_TYPE_CHUNKED: u8 = 3;
const TOC_TYPE_SOFTLINK: u8 = 4;
const TOC_TYPE_EMPTYFILE: u8 = 6;

/// A parsed KAR archive that can read individual files.
#[derive(Debug)]
pub struct KarArchive<R: Read + Seek> {
    reader: R,
    header: KarHeader,
    entries: BTreeMap<String, KarEntry>,
}

/// KAR file header.
#[derive(Debug, Clone)]
pub struct KarHeader {
    /// Whether the archive byte order matches the native byte order.
    pub byte_order_native: bool,
    /// Archive format version (currently always 1).
    pub version: u32,
    /// Byte offset where the file data section begins.
    pub file_offset: u64,
}

/// An entry in the KAR table of contents.
#[derive(Debug, Clone)]
pub enum KarEntry {
    /// A file with data in the data section.
    File {
        /// Offset relative to the data section start.
        byte_offset: u64,
        /// Size in bytes.
        byte_size: u64,
    },
    /// A directory (no additional data).
    Directory,
    /// An empty file (zero bytes, no data section entry).
    EmptyFile,
    /// A symbolic link.
    Softlink {
        /// The link target path.
        target: String,
    },
}

// ---------------------------------------------------------------------------
// PBSTree parsing (shared by KAR TOC and VDB metadata)
// ---------------------------------------------------------------------------

/// Parse a PBSTree blob into a sequence of raw node data slices.
///
/// PBSTree binary format (from ncbi-vdb `P_BSTree`):
/// ```text
/// u32  num_nodes   — number of nodes in the tree
/// u32  data_size   — total size of all node data in bytes
/// idx[]            — array of num_nodes offsets into the data area
///                    (u8 if data_size <= 256, u16 if <= 65536, u32 otherwise)
/// data[]           — concatenated node payloads (data_size bytes total)
/// ```
///
/// Node `i` spans from `data[idx[i]]` to `data[idx[i+1]]` (or
/// `data[data_size]` for the last).
pub(crate) fn parse_pbstree_slices(buf: &[u8]) -> Result<Vec<&[u8]>> {
    if buf.len() < 4 {
        return Err(Error::InvalidKar("PBSTree too short for header".into()));
    }

    let num_nodes = LittleEndian::read_u32(&buf[..4]) as usize;
    if num_nodes == 0 {
        return Ok(Vec::new());
    }

    if buf.len() < 8 {
        return Err(Error::InvalidKar(
            "PBSTree too short for data_size field".into(),
        ));
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
        return Err(Error::InvalidKar(format!(
            "PBSTree: need {} bytes (idx={idx_bytes} + data={data_size}), have {} after header",
            idx_bytes + data_size,
            buf.len().saturating_sub(8),
        )));
    }

    let idx_buf = &buf[idx_start..idx_start + idx_bytes];
    let data_buf = &buf[data_start..data_start + data_size];

    let read_idx = |i: usize| -> usize {
        match idx_elem_size {
            1 => idx_buf[i] as usize,
            2 => LittleEndian::read_u16(&idx_buf[i * 2..i * 2 + 2]) as usize,
            _ => LittleEndian::read_u32(&idx_buf[i * 4..i * 4 + 4]) as usize,
        }
    };

    // The bounds check above ensures `num_nodes <= buf.len()` (each entry
    // occupies at least one index byte), so this is bounded — but a 1 GiB
    // KAR can otherwise drive `num_nodes * size_of::<&[u8]>() = 16 GiB` of
    // capacity. Reject through the same defense-in-depth ratio used by the
    // blob decoders.
    crate::blob::check_alloc_bytes(
        num_nodes.saturating_mul(std::mem::size_of::<&[u8]>()),
        buf.len(),
        "PBSTree node table",
    )?;
    let mut nodes = Vec::with_capacity(num_nodes);
    for i in 0..num_nodes {
        let off = read_idx(i);
        let end = if i + 1 < num_nodes {
            read_idx(i + 1)
        } else {
            data_size
        };

        if off > data_size || end > data_size || off > end {
            return Err(Error::InvalidKar(format!(
                "PBSTree node {i}: invalid range {off}..{end} (data_size={data_size})"
            )));
        }

        nodes.push(&data_buf[off..end]);
    }

    Ok(nodes)
}

/// Compute the total byte size of a serialized PBSTree without parsing nodes.
///
/// Returns `None` if the buffer is too short or malformed.
pub(crate) fn pbstree_byte_size(buf: &[u8]) -> Option<usize> {
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

    Some(8 + num_nodes * idx_elem_size + data_size)
}

// ---------------------------------------------------------------------------
// TOC node inflation
// ---------------------------------------------------------------------------

/// Read a `T`-sized little-endian value from `data` at `offset`, returning
/// the value and advancing `offset`.
fn toc_get<const N: usize>(data: &[u8], offset: &mut usize) -> Result<[u8; N]> {
    let end = *offset + N;
    if end > data.len() {
        return Err(Error::InvalidKar(format!(
            "TOC read out of bounds: need {end}, have {}",
            data.len()
        )));
    }
    let mut buf = [0u8; N];
    buf.copy_from_slice(&data[*offset..end]);
    *offset = end;
    Ok(buf)
}

/// Inflate a single PBSTree node into a `(name, KarEntry)` pair.
/// For directories, recursively parses the nested PBSTree.
fn inflate_node(data: &[u8], prefix: &str) -> Result<Vec<(String, KarEntry)>> {
    let mut offset = 0usize;

    // name_len (u16) + name bytes
    let name_len = LittleEndian::read_u16(&toc_get::<2>(data, &mut offset)?) as usize;
    if offset + name_len > data.len() {
        return Err(Error::InvalidKar("TOC node name extends past data".into()));
    }
    let name = std::str::from_utf8(&data[offset..offset + name_len])
        .map_err(|e| Error::InvalidKar(format!("invalid UTF-8 in TOC name: {e}")))?
        .to_owned();
    offset += name_len;

    // Skip mod_time (u64) + access_mode (u32) — not needed.
    if offset + 12 > data.len() {
        return Err(Error::InvalidKar(
            "TOC node truncated at mod_time/access_mode".into(),
        ));
    }
    offset += 12;

    // type_code (u8)
    let type_code = toc_get::<1>(data, &mut offset)?[0];

    let full_path = if prefix.is_empty() {
        name.clone()
    } else {
        format!("{prefix}/{name}")
    };

    let mut results = Vec::new();

    match type_code {
        TOC_TYPE_FILE | TOC_TYPE_CHUNKED => {
            let byte_offset = LittleEndian::read_u64(&toc_get::<8>(data, &mut offset)?);
            let byte_size = LittleEndian::read_u64(&toc_get::<8>(data, &mut offset)?);

            results.push((
                full_path,
                KarEntry::File {
                    byte_offset,
                    byte_size,
                },
            ));
        }

        TOC_TYPE_EMPTYFILE => {
            results.push((full_path, KarEntry::EmptyFile));
        }

        TOC_TYPE_DIR => {
            let subtree_data = &data[offset..];
            results.push((full_path.clone(), KarEntry::Directory));

            let child_nodes = parse_pbstree_slices(subtree_data)?;
            for child in &child_nodes {
                let mut children = inflate_node(child, &full_path)?;
                results.append(&mut children);
            }
        }

        TOC_TYPE_SOFTLINK => {
            let link_len = LittleEndian::read_u16(&toc_get::<2>(data, &mut offset)?) as usize;

            if offset + link_len > data.len() {
                return Err(Error::InvalidKar(
                    "softlink target extends past node data".into(),
                ));
            }
            let target = std::str::from_utf8(&data[offset..offset + link_len])
                .map_err(|e| Error::InvalidKar(format!("invalid UTF-8 in softlink: {e}")))?
                .to_owned();

            results.push((full_path, KarEntry::Softlink { target }));
        }

        // notfound(0), hardlink(5), zombiefile(7) — skip silently
        0 | 5 | 7 => {}

        other => {
            return Err(Error::InvalidKar(format!(
                "unknown TOC entry type code: {other}"
            )));
        }
    }

    Ok(results)
}

// ---------------------------------------------------------------------------
// KarArchive implementation
// ---------------------------------------------------------------------------

impl<R: Read + Seek> KarArchive<R> {
    /// Open a KAR archive and parse its header and table of contents.
    pub fn open(mut reader: R) -> Result<Self> {
        let header = Self::read_header(&mut reader)?;
        let entries = Self::read_toc(&mut reader, &header)?;

        Ok(Self {
            reader,
            header,
            entries,
        })
    }

    /// Return a reference to the parsed header.
    pub fn header(&self) -> &KarHeader {
        &self.header
    }

    /// Return a reference to all entries.
    pub fn entries(&self) -> &BTreeMap<String, KarEntry> {
        &self.entries
    }

    /// List all file paths (files + empty files) in the archive, sorted.
    pub fn list_files(&self) -> Vec<&str> {
        self.entries
            .iter()
            .filter_map(|(path, entry)| match entry {
                KarEntry::File { .. } | KarEntry::EmptyFile => Some(path.as_str()),
                _ => None,
            })
            .collect()
    }

    /// Read the full contents of a file in the archive.
    pub fn read_file(&mut self, path: &str) -> Result<Vec<u8>> {
        let entry = self
            .entries
            .get(path)
            .ok_or_else(|| Error::InvalidKar(format!("file not found in archive: {path}")))?;

        match entry {
            KarEntry::File {
                byte_offset,
                byte_size,
            } => {
                let abs_offset = self.header.file_offset + byte_offset;
                let size = *byte_size as usize;

                self.reader.seek(SeekFrom::Start(abs_offset))?;
                let mut buf = vec![0u8; size];
                self.reader.read_exact(&mut buf)?;
                Ok(buf)
            }
            KarEntry::EmptyFile => Ok(Vec::new()),
            KarEntry::Directory => Err(Error::InvalidKar(format!(
                "cannot read directory as file: {path}"
            ))),
            KarEntry::Softlink { .. } => Err(Error::InvalidKar(format!(
                "cannot read softlink as file: {path}"
            ))),
        }
    }

    /// Return the absolute byte offset and size of a file in the archive,
    /// without reading its contents.
    ///
    /// Returns `(absolute_offset, size)` for use with direct disk reads.
    /// Returns `None` if the path doesn't exist or isn't a regular file.
    pub fn file_location(&self, path: &str) -> Option<(u64, u64)> {
        match self.entries.get(path)? {
            KarEntry::File {
                byte_offset,
                byte_size,
            } => {
                let abs_offset = self.header.file_offset + byte_offset;
                Some((abs_offset, *byte_size))
            }
            KarEntry::EmptyFile => Some((0, 0)),
            _ => None,
        }
    }

    /// Return the size of a file, or `None` if the path doesn't exist or
    /// isn't a file.
    pub fn file_size(&self, path: &str) -> Option<u64> {
        match self.entries.get(path)? {
            KarEntry::File { byte_size, .. } => Some(*byte_size),
            KarEntry::EmptyFile => Some(0),
            _ => None,
        }
    }

    /// List immediate children of a directory path.
    ///
    /// The `path` should not have a trailing slash. Use an empty string for
    /// the root directory.
    pub fn list_dir(&self, path: &str) -> Vec<&str> {
        let prefix = if path.is_empty() {
            String::new()
        } else {
            format!("{path}/")
        };

        self.entries
            .keys()
            .filter_map(|p| {
                let rest = if prefix.is_empty() {
                    p.as_str()
                } else {
                    p.strip_prefix(&prefix)?
                };
                // Immediate child: no further '/' in the remainder
                if !rest.is_empty() && !rest.contains('/') {
                    Some(p.as_str())
                } else {
                    None
                }
            })
            .collect()
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    /// Parse the 24-byte KAR header.
    fn read_header(reader: &mut R) -> Result<KarHeader> {
        reader.seek(SeekFrom::Start(0))?;

        // Read magic "NCBI"
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        if magic != MAGIC_NCBI {
            return Err(Error::InvalidKar(format!(
                "bad magic: expected NCBI, got {magic:?}"
            )));
        }

        // Read ".sra"
        reader.read_exact(&mut magic)?;
        if magic != MAGIC_SRA {
            return Err(Error::InvalidKar(format!(
                "bad magic: expected .sra, got {magic:?}"
            )));
        }

        // Read byte order tag
        let byte_order_raw = reader.read_u32::<LittleEndian>()?;
        let byte_order_native = match byte_order_raw {
            BYTE_ORDER_TAG => true,
            BYTE_ORDER_REVERSED => false,
            other => {
                return Err(Error::InvalidKar(format!(
                    "invalid byte order tag: {other:#010x}"
                )));
            }
        };

        // Read version
        let version_raw = reader.read_u32::<LittleEndian>()?;
        let version = if byte_order_native {
            version_raw
        } else {
            version_raw.swap_bytes()
        };

        if version == 0 {
            return Err(Error::InvalidKar("version 0 is invalid".into()));
        }
        if version > 1 {
            return Err(Error::InvalidKar(format!("unsupported version: {version}")));
        }

        // Read file_offset (u64) — start of data section
        let file_offset_raw = reader.read_u64::<LittleEndian>()?;
        let file_offset = if byte_order_native {
            file_offset_raw
        } else {
            file_offset_raw.swap_bytes()
        };

        Ok(KarHeader {
            byte_order_native,
            version,
            file_offset,
        })
    }

    /// Read and parse the entire TOC (from end of header to file_offset).
    fn read_toc(reader: &mut R, header: &KarHeader) -> Result<BTreeMap<String, KarEntry>> {
        let toc_start = HEADER_SIZE;
        let toc_size = header.file_offset.checked_sub(toc_start).ok_or_else(|| {
            Error::InvalidKar(format!(
                "file_offset ({}) is less than header size ({HEADER_SIZE})",
                header.file_offset
            ))
        })? as usize;

        // Read the entire TOC into memory
        reader.seek(SeekFrom::Start(toc_start))?;
        let mut toc_buf = vec![0u8; toc_size];
        reader.read_exact(&mut toc_buf)?;

        // Parse the top-level PBSTree
        let nodes = parse_pbstree_slices(&toc_buf)?;

        // Inflate each node recursively
        let mut entries = BTreeMap::new();
        for node in &nodes {
            let inflated = inflate_node(node, "")?;
            for (path, entry) in inflated {
                entries.insert(path, entry);
            }
        }

        Ok(entries)
    }
}

// ---------------------------------------------------------------------------
// Helper: create a minimal in-memory KAR archive for testing
// ---------------------------------------------------------------------------

#[cfg(test)]
pub(crate) mod test_helpers {
    use byteorder::{LittleEndian, WriteBytesExt};
    use std::io::Write;

    use super::*;

    /// Build a PBSTree blob from a list of raw node payloads.
    ///
    /// Format: u32 num_nodes, u32 data_size, idx[num_nodes], data[data_size]
    /// Index element size depends on data_size (u8 if <=256, u16 if <=65536, u32 otherwise).
    pub fn build_pbstree(nodes: &[&[u8]]) -> Vec<u8> {
        let num_nodes = nodes.len() as u32;
        let data_size: usize = nodes.iter().map(|n| n.len()).sum();

        let mut buf = Vec::new();
        buf.write_u32::<LittleEndian>(num_nodes).unwrap();

        // Empty tree: just num_nodes=0 (per C code: "if (num_nodes == 0) size = 4")
        if num_nodes == 0 {
            return buf;
        }

        buf.write_u32::<LittleEndian>(data_size as u32).unwrap();

        // Write index array (offsets into data area).
        let mut offset = 0u32;
        for node in nodes {
            if data_size <= 256 {
                buf.write_u8(offset as u8).unwrap();
            } else if data_size <= 65536 {
                buf.write_u16::<LittleEndian>(offset as u16).unwrap();
            } else {
                buf.write_u32::<LittleEndian>(offset).unwrap();
            }
            offset += node.len() as u32;
        }

        // Write data area.
        for node in nodes {
            buf.write_all(node).unwrap();
        }
        buf
    }

    /// Build the common TOC entry header bytes for a node.
    pub fn build_toc_entry(name: &str, type_code: u8) -> Vec<u8> {
        let mut buf = Vec::new();
        // name_len (u16)
        buf.write_u16::<LittleEndian>(name.len() as u16).unwrap();
        // name bytes
        buf.write_all(name.as_bytes()).unwrap();
        // mod_time (u64) — zero
        buf.write_u64::<LittleEndian>(0).unwrap();
        // access_mode (u32) — 0o755
        buf.write_u32::<LittleEndian>(0o755).unwrap();
        // type_code (u8)
        buf.write_u8(type_code).unwrap();
        buf
    }

    /// Build a file TOC node.
    pub fn build_file_node(name: &str, byte_offset: u64, byte_size: u64) -> Vec<u8> {
        let mut buf = build_toc_entry(name, TOC_TYPE_FILE);
        buf.write_u64::<LittleEndian>(byte_offset).unwrap();
        buf.write_u64::<LittleEndian>(byte_size).unwrap();
        buf
    }

    /// Build an empty file TOC node.
    pub fn build_empty_file_node(name: &str) -> Vec<u8> {
        build_toc_entry(name, TOC_TYPE_EMPTYFILE)
    }

    /// Build a softlink TOC node.
    pub fn build_softlink_node(name: &str, target: &str) -> Vec<u8> {
        let mut buf = build_toc_entry(name, TOC_TYPE_SOFTLINK);
        buf.write_u16::<LittleEndian>(target.len() as u16).unwrap();
        buf.write_all(target.as_bytes()).unwrap();
        buf
    }

    /// Build a directory TOC node with the given child node payloads.
    pub fn build_dir_node(name: &str, children: &[&[u8]]) -> Vec<u8> {
        let mut buf = build_toc_entry(name, TOC_TYPE_DIR);
        let subtree = build_pbstree(children);
        buf.extend_from_slice(&subtree);
        buf
    }

    /// Build a full KAR archive from a list of top-level TOC node payloads
    /// and a data section.
    pub fn build_kar_archive(top_level_nodes: &[&[u8]], data: &[u8]) -> Vec<u8> {
        let toc = build_pbstree(top_level_nodes);

        // file_offset = header_size + toc_size, aligned to 4 bytes
        let raw_offset = HEADER_SIZE as usize + toc.len();
        let file_offset = (raw_offset + 3) & !3; // align to 4
        let padding = file_offset - raw_offset;

        let mut archive = Vec::new();
        // Header
        archive.extend_from_slice(&MAGIC_NCBI);
        archive.extend_from_slice(&MAGIC_SRA);
        archive.write_u32::<LittleEndian>(BYTE_ORDER_TAG).unwrap();
        archive.write_u32::<LittleEndian>(1).unwrap(); // version
        archive
            .write_u64::<LittleEndian>(file_offset as u64)
            .unwrap();

        // TOC
        archive.extend_from_slice(&toc);

        // Padding
        archive.extend(std::iter::repeat_n(0u8, padding));

        // Data section
        archive.extend_from_slice(data);

        archive
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::test_helpers::*;
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_header_parsing() {
        let archive = build_kar_archive(&[], b"");
        let mut cursor = Cursor::new(archive);
        let header = KarArchive::read_header(&mut cursor).unwrap();

        assert!(header.byte_order_native);
        assert_eq!(header.version, 1);
        // With 0 TOC nodes, PBSTree is just 4 bytes (u32 count=0).
        // Header is 24 bytes, TOC is 4 bytes => 28 bytes, aligned to 4 = 28.
        assert_eq!(header.file_offset, 28);
    }

    #[test]
    fn test_bad_magic_ncbi() {
        let mut archive = build_kar_archive(&[], b"");
        archive[0] = b'X'; // corrupt "NCBI"
        let result = KarArchive::open(Cursor::new(archive));
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("bad magic"), "unexpected error: {err}");
    }

    #[test]
    fn test_bad_magic_sra() {
        let mut archive = build_kar_archive(&[], b"");
        archive[4] = b'X'; // corrupt ".sra"
        let result = KarArchive::open(Cursor::new(archive));
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("bad magic"), "unexpected error: {err}");
    }

    #[test]
    fn test_bad_byte_order() {
        let mut archive = build_kar_archive(&[], b"");
        // Write garbage byte order
        archive[8..12].copy_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF]);
        let result = KarArchive::open(Cursor::new(archive));
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("byte order"), "unexpected error: {err}");
    }

    #[test]
    fn test_empty_archive() {
        let archive = build_kar_archive(&[], b"");
        let kar = KarArchive::open(Cursor::new(archive)).unwrap();
        assert!(kar.list_files().is_empty());
        assert!(kar.entries().is_empty());
    }

    #[test]
    fn test_single_file() {
        let file_data = b"Hello, KAR!";
        let file_node = build_file_node("test.txt", 0, file_data.len() as u64);
        let archive = build_kar_archive(&[&file_node], file_data);

        let mut kar = KarArchive::open(Cursor::new(archive)).unwrap();

        let files = kar.list_files();
        assert_eq!(files, vec!["test.txt"]);
        assert_eq!(kar.file_size("test.txt"), Some(file_data.len() as u64));

        let contents = kar.read_file("test.txt").unwrap();
        assert_eq!(contents, file_data);
    }

    #[test]
    fn test_empty_file() {
        let node = build_empty_file_node("empty.dat");
        let archive = build_kar_archive(&[&node], b"");

        let mut kar = KarArchive::open(Cursor::new(archive)).unwrap();

        let files = kar.list_files();
        assert_eq!(files, vec!["empty.dat"]);
        assert_eq!(kar.file_size("empty.dat"), Some(0));

        let contents = kar.read_file("empty.dat").unwrap();
        assert!(contents.is_empty());
    }

    #[test]
    fn test_softlink() {
        let link_node = build_softlink_node("link.txt", "target.txt");
        let archive = build_kar_archive(&[&link_node], b"");

        let kar = KarArchive::open(Cursor::new(archive)).unwrap();

        match kar.entries().get("link.txt").unwrap() {
            KarEntry::Softlink { target } => assert_eq!(target, "target.txt"),
            other => panic!("expected Softlink, got {other:?}"),
        }
    }

    #[test]
    fn test_directory_with_files() {
        let file1_data = b"file1 data here";
        let file2_data = b"file2 data here";

        let child1 = build_file_node("a.txt", 0, file1_data.len() as u64);
        let child2 = build_file_node("b.txt", file1_data.len() as u64, file2_data.len() as u64);

        let dir_node = build_dir_node("mydir", &[&child1, &child2]);
        let mut data = Vec::new();
        data.extend_from_slice(file1_data);
        data.extend_from_slice(file2_data);

        let archive = build_kar_archive(&[&dir_node], &data);

        let mut kar = KarArchive::open(Cursor::new(archive)).unwrap();

        // Check entries
        assert!(matches!(
            kar.entries().get("mydir"),
            Some(KarEntry::Directory)
        ));
        assert!(kar.entries().contains_key("mydir/a.txt"));
        assert!(kar.entries().contains_key("mydir/b.txt"));

        // list_files should include the two files but not the directory
        let files = kar.list_files();
        assert_eq!(files, vec!["mydir/a.txt", "mydir/b.txt"]);

        // list_dir for root
        let root_children = kar.list_dir("");
        assert_eq!(root_children, vec!["mydir"]);

        // list_dir for mydir
        let dir_children = kar.list_dir("mydir");
        assert_eq!(dir_children, vec!["mydir/a.txt", "mydir/b.txt"]);

        // Read files
        let contents1 = kar.read_file("mydir/a.txt").unwrap();
        assert_eq!(contents1, file1_data);

        let contents2 = kar.read_file("mydir/b.txt").unwrap();
        assert_eq!(contents2, file2_data);
    }

    #[test]
    fn test_nested_directories() {
        let file_data = b"nested content";
        let inner_file = build_file_node("deep.bin", 0, file_data.len() as u64);
        let inner_dir = build_dir_node("inner", &[&inner_file]);
        let outer_dir = build_dir_node("outer", &[&inner_dir]);

        let archive = build_kar_archive(&[&outer_dir], file_data);

        let mut kar = KarArchive::open(Cursor::new(archive)).unwrap();

        assert!(matches!(
            kar.entries().get("outer"),
            Some(KarEntry::Directory)
        ));
        assert!(matches!(
            kar.entries().get("outer/inner"),
            Some(KarEntry::Directory)
        ));

        let files = kar.list_files();
        assert_eq!(files, vec!["outer/inner/deep.bin"]);

        let contents = kar.read_file("outer/inner/deep.bin").unwrap();
        assert_eq!(contents, file_data);
    }

    #[test]
    fn test_multiple_top_level_entries() {
        let data_a = b"aaaa";
        let data_b = b"bbbbbb";

        let node_a = build_file_node("alpha.txt", 0, data_a.len() as u64);
        let node_b = build_file_node("beta.txt", data_a.len() as u64, data_b.len() as u64);

        let mut data = Vec::new();
        data.extend_from_slice(data_a);
        data.extend_from_slice(data_b);

        let archive = build_kar_archive(&[&node_a, &node_b], &data);

        let mut kar = KarArchive::open(Cursor::new(archive)).unwrap();

        let files = kar.list_files();
        assert_eq!(files, vec!["alpha.txt", "beta.txt"]);

        assert_eq!(kar.read_file("alpha.txt").unwrap(), data_a);
        assert_eq!(kar.read_file("beta.txt").unwrap(), data_b);
    }

    #[test]
    fn test_file_not_found() {
        let archive = build_kar_archive(&[], b"");
        let mut kar = KarArchive::open(Cursor::new(archive)).unwrap();

        let result = kar.read_file("nonexistent.txt");
        assert!(result.is_err());
    }

    #[test]
    fn test_file_size_missing() {
        let archive = build_kar_archive(&[], b"");
        let kar = KarArchive::open(Cursor::new(archive)).unwrap();

        assert_eq!(kar.file_size("nonexistent.txt"), None);
    }

    #[test]
    fn test_read_directory_as_file_fails() {
        let dir_node = build_dir_node("mydir", &[]);
        let archive = build_kar_archive(&[&dir_node], b"");

        let mut kar = KarArchive::open(Cursor::new(archive)).unwrap();

        let result = kar.read_file("mydir");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("directory"), "unexpected error: {err}");
    }

    #[test]
    fn test_mixed_entry_types() {
        let file_data = b"real content";
        let file_node = build_file_node("data.bin", 0, file_data.len() as u64);
        let empty_node = build_empty_file_node("placeholder");
        let link_node = build_softlink_node("shortcut", "data.bin");

        let archive = build_kar_archive(&[&file_node, &empty_node, &link_node], file_data);

        let mut kar = KarArchive::open(Cursor::new(archive)).unwrap();

        // Files include data.bin and placeholder
        let files = kar.list_files();
        assert_eq!(files, vec!["data.bin", "placeholder"]);

        assert_eq!(kar.file_size("data.bin"), Some(file_data.len() as u64));
        assert_eq!(kar.file_size("placeholder"), Some(0));
        assert_eq!(kar.file_size("shortcut"), None); // softlinks aren't files

        assert_eq!(kar.read_file("data.bin").unwrap(), file_data);
        assert!(kar.read_file("placeholder").unwrap().is_empty());

        match kar.entries().get("shortcut").unwrap() {
            KarEntry::Softlink { target } => assert_eq!(target, "data.bin"),
            other => panic!("expected Softlink, got {other:?}"),
        }
    }

    #[test]
    fn test_pbstree_parse_empty() {
        let buf = build_pbstree(&[]);
        let nodes = parse_pbstree_slices(&buf).unwrap();
        assert!(nodes.is_empty());
    }

    #[test]
    fn test_pbstree_parse_truncated() {
        // Just 2 bytes — not enough for node_count
        let buf = [0u8; 2];
        assert!(parse_pbstree_slices(&buf).is_err());
    }

    #[test]
    fn test_pbstree_parse_node_extends_past_buffer() {
        // node_count=1, data_size=100, but buffer too short for idx + data
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u32.to_le_bytes()); // num_nodes
        buf.extend_from_slice(&100u32.to_le_bytes()); // data_size
        // Need 1 byte idx + 100 bytes data = 101 bytes, but provide only 4
        buf.extend_from_slice(&[0u8; 4]);
        assert!(parse_pbstree_slices(&buf).is_err());
    }

    #[test]
    fn test_byte_order_reversed_header() {
        // Build a header with reversed byte order
        let mut archive = Vec::new();
        archive.extend_from_slice(&MAGIC_NCBI);
        archive.extend_from_slice(&MAGIC_SRA);
        archive.extend_from_slice(&BYTE_ORDER_REVERSED.to_le_bytes());
        // version = 1, but byte-swapped
        archive.extend_from_slice(&1u32.swap_bytes().to_le_bytes());
        // file_offset = 28, but byte-swapped
        archive.extend_from_slice(&28u64.swap_bytes().to_le_bytes());
        // Empty PBSTree (node_count = 0) — for swapped archives the TOC
        // integers would also be swapped. For this test, just verify header
        // parsing works.
        archive.extend_from_slice(&0u32.to_le_bytes());

        let header = KarArchive::read_header(&mut Cursor::new(&archive)).unwrap();
        assert!(!header.byte_order_native);
        assert_eq!(header.version, 1);
        assert_eq!(header.file_offset, 28);
    }
}
