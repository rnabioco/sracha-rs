//! KDB column reader for physical columns within a KAR archive.
//!
//! Each column in the KAR is a directory containing up to five files:
//!
//! - `idx`  — column metadata header (v2+): data_eof, page_size, etc.
//! - `idx1` — block locators (v2+) or full header + locators (v1)
//! - `idx2` — block index data: per-blob boundary information
//! - `idx0` — blob index: array of 24-byte [`BlobLoc`] entries (legacy)
//! - `data` — concatenated blob data (possibly zlib-compressed)
//!
//! The [`ColumnReader`] loads index files into memory and reads column data
//! lazily from disk, providing row-level access via
//! [`read_raw_blob_slice`](ColumnReader::read_raw_blob_slice).
//!
//! ## idx1/idx2 block index format
//!
//! For v2+ columns, `idx1` contains [`BlockLoc`] entries that describe blocks
//! of blobs. Each block locator encodes how the blob boundaries are stored in
//! `idx2` via `id_type` and `pg_type` fields (0=random, 1=uniform,
//! 2=magnitude, 3=predictable). The `parse_idx2_block` function decodes the
//! idx2 data for each block into individual [`BlobLoc`] entries.

use std::io::Seek;

use byteorder::{ByteOrder, LittleEndian};

use crate::error::{Error, Result};
use crate::kar::KarArchive;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Size of a single blob locator entry in idx0 (also used for v1 block locs
/// and v2+ block locs — they share the same 24-byte `KColLocDesc` layout).
const BLOB_LOC_SIZE: usize = 24;

/// Size of a single block locator entry in idx1 (same physical layout as
/// `BLOB_LOC_SIZE` but the gen field has different bit-packing).
const BLOCK_LOC_SIZE: usize = 24;

/// Expected endian magic in the idx1 header.
const KDB_ENDIAN_MAGIC: u32 = 0x05031988;

// ---------------------------------------------------------------------------
// Data types
// ---------------------------------------------------------------------------

/// Block type encoding for id and pg fields in [`BlockLoc`].
///
/// These correspond to the `btype*` enum in the C reference implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BlockType {
    /// Fully specified random access: each blob has explicit id/pg and range/size.
    Random = 0,
    /// Uniform: constant range/size stored once in the header.
    Uniform = 1,
    /// Magnitude: cumulative — sizes stored as deltas from a starting value.
    Magnitude = 2,
    /// Predictable: evenly spaced, can be computed from block-level info alone.
    Predictable = 3,
}

impl BlockType {
    fn from_u8(v: u8) -> Result<Self> {
        match v {
            0 => Ok(Self::Random),
            1 => Ok(Self::Uniform),
            2 => Ok(Self::Magnitude),
            3 => Ok(Self::Predictable),
            _ => Err(Error::Format(format!("invalid block type: {v}"))),
        }
    }
}

/// Metadata parsed from the column header (`idx1` for v1, or `idx` for v2+).
#[derive(Debug, Clone)]
pub struct ColumnMeta {
    /// Column header version (typically 1-3).
    pub version: u32,
    /// Size of the data file in bytes.
    pub data_eof: u64,
    /// End-of-file offset for the idx2 data.
    pub idx2_eof: u64,
    /// Page size for offset calculations.
    pub page_size: u32,
    /// Checksum type: 0 = none, 1 = CRC32, 2 = MD5.
    pub checksum_type: u8,
    /// Number of block entries in idx1.
    pub num_blocks: u32,
    /// Block locators parsed from idx1 (v1 legacy: used directly as BlobLocs).
    pub block_locs: Vec<BlobLoc>,
}

/// A single blob locator — the final output describing one blob's location.
///
/// Produced from `idx0` (legacy) or by decoding `idx2` block data via
/// `parse_idx2_block`.
#[derive(Debug, Clone)]
pub struct BlobLoc {
    /// Page offset into the data file (multiply by page_size for byte offset).
    pub pg: u64,
    /// Blob size in bytes.
    pub size: u32,
    /// Number of row IDs covered by this blob.
    pub id_range: u32,
    /// First row ID in this blob.
    pub start_id: i64,
}

/// A block locator from `idx1` (v2+).
///
/// Each entry describes a block of blobs whose detailed boundaries are stored
/// in `idx2`. The 32-bit `gen` field packs:
///   - bits 0..26:  `size` (27 bits) — byte size of this block's idx2 data
///   - bits 27..28: `id_type` (2 bits)
///   - bits 29..30: `pg_type` (2 bits)
///   - bit 31:      `compressed` flag
#[derive(Debug, Clone)]
pub struct BlockLoc {
    /// Offset into the idx2 file for this block's data.
    pub pg: u64,
    /// Size of idx2 data for this block (27 bits).
    pub size: u32,
    /// How row-ID boundaries are encoded (0-3).
    pub id_type: BlockType,
    /// How page/size boundaries are encoded (0-3).
    pub pg_type: BlockType,
    /// Whether the idx2 block data is compressed.
    pub compressed: bool,
    /// Number of row IDs covered by this block.
    pub id_range: u32,
    /// First row ID in this block.
    pub start_id: i64,
}

/// Where the column data bytes live.
///
/// `InMemory` holds the full `data` file in a `Vec<u8>` (used by
/// [`ColumnReader::from_parts`] for selective-fetch and test paths).
///
/// `OnDisk` stores the location of the data file within the SRA/KAR file
/// so that individual blobs can be read on demand without loading the
/// entire (potentially multi-GiB) data file into memory.
enum DataSource {
    /// Data loaded in memory (for `from_parts` / selective fetch).
    InMemory(Vec<u8>),
    /// Data memory-mapped from the SRA file.
    /// The mmap covers the column's data section within the KAR archive.
    /// Blob reads become simple slice operations — zero syscalls per blob.
    Mmap {
        /// Memory-mapped region covering the column data.
        mmap: memmap2::Mmap,
        /// Offset within the mmap where column data starts (always 0 since
        /// we map only the column's portion).
        _file_offset: u64,
    },
}

/// Reader for a single physical column within a KAR archive.
///
/// For the [`open`](Self::open) path the column data is read lazily from
/// disk (one blob at a time) to avoid loading multi-GiB data files into
/// memory. The [`from_parts`](Self::from_parts) path still accepts an
/// in-memory buffer for backwards compatibility.
pub struct ColumnReader {
    meta: ColumnMeta,
    blobs: Vec<BlobLoc>,
    data: DataSource,
}

// ---------------------------------------------------------------------------
// Parsing helpers
// ---------------------------------------------------------------------------

/// Parse the `idx1` column header into [`ColumnMeta`].
///
/// Layout (little-endian):
/// ```text
/// bytes 0-3:   endian magic (0x05031988 native, or byte-swapped)
/// bytes 4-7:   version (u32)
/// bytes 8-15:  data_eof (u64)
/// bytes 16-23: idx2_eof (u64)
/// bytes 24-27: num_blocks (u32)
/// bytes 28-31: page_size (u32)  — for v1; layout varies slightly by version
/// bytes 32-35: page_size (u32)  — for v2+
/// byte  36:    checksum type
/// ```
fn parse_idx1(buf: &[u8]) -> Result<ColumnMeta> {
    // Minimum header size: magic(4) + version(4) = 8 bytes (KDBHdr).
    // For v1, the full header follows (data_eof, idx2_eof, etc.).
    // For v2+, idx1 has only KDBHdr + block locators.
    if buf.len() < 8 {
        return Err(Error::Format(format!(
            "idx1 too short: {} bytes (need at least 8)",
            buf.len()
        )));
    }

    let magic = LittleEndian::read_u32(&buf[0..4]);
    let swapped = match magic {
        KDB_ENDIAN_MAGIC => false,
        _ if magic.swap_bytes() == KDB_ENDIAN_MAGIC => true,
        _ => {
            return Err(Error::Format(format!(
                "invalid idx1 endian magic: {magic:#010x}"
            )));
        }
    };

    let read_u32 = |offset: usize| -> u32 {
        let v = LittleEndian::read_u32(&buf[offset..offset + 4]);
        if swapped { v.swap_bytes() } else { v }
    };
    let read_u64 = |offset: usize| -> u64 {
        let v = LittleEndian::read_u64(&buf[offset..offset + 8]);
        if swapped { v.swap_bytes() } else { v }
    };

    let version = read_u32(4);

    // For v1: full KColumnHdr in idx1 file, block locators follow the header.
    //   Layout: KDBHdr(8) + data_eof(8) + idx2_eof(8) + num_blocks(4) + page_size(4) + checksum(1) + align(7) = 40 bytes
    //   Then num_blocks x 24-byte block locators
    //
    // For v2+: idx1 has only KDBHdr(8) + block locators. The full metadata
    //   (data_eof, page_size, etc.) is in the separate `idx` file.
    if version <= 1 {
        let data_eof = if buf.len() >= 16 { read_u64(8) } else { 0 };
        let idx2_eof = if buf.len() >= 24 { read_u64(16) } else { 0 };
        let num_blocks = if buf.len() >= 28 { read_u32(24) } else { 0 };
        let page_size = if buf.len() >= 32 { read_u32(28) } else { 1 };
        let checksum_type = if buf.len() >= 33 { buf[32] } else { 0 };
        let header_end = 40usize;

        let block_locs = parse_v1_block_locs_from(buf, header_end, num_blocks as usize);

        Ok(ColumnMeta {
            version,
            data_eof,
            idx2_eof,
            page_size: if page_size == 0 { 1 } else { page_size },
            checksum_type,
            num_blocks,
            block_locs,
        })
    } else {
        // v2+: block locators start at offset 8 (right after KDBHdr).
        // All metadata (data_eof, page_size, etc.) comes from the `idx` file.
        let header_end = 8usize;
        let num_blocks_in_idx1 = if buf.len() > header_end {
            (buf.len() - header_end) / BLOCK_LOC_SIZE
        } else {
            0
        };

        Ok(ColumnMeta {
            version,
            data_eof: 0,  // will be overridden from `idx` file
            idx2_eof: 0,  // will be overridden from `idx` file
            page_size: 1, // will be overridden from `idx` file if available
            checksum_type: 0,
            num_blocks: num_blocks_in_idx1 as u32,
            block_locs: Vec::new(), // v2+ uses BlockLoc + idx2 instead
        })
    }
}

/// Parse v1 block locator entries (treated as direct BlobLocs) from idx1.
///
/// In v1 format, each 24-byte entry is interpreted as a direct blob locator
/// with the gen field using bit 31 as a remove flag and bits 0..30 as size.
fn parse_v1_block_locs_from(buf: &[u8], start: usize, max_count: usize) -> Vec<BlobLoc> {
    let mut locs = Vec::new();
    if start >= buf.len() || max_count == 0 {
        return locs;
    }
    let loc_data = &buf[start..];
    let available = loc_data.len() / BLOB_LOC_SIZE;
    let count = max_count.min(available);
    for i in 0..count {
        let off = i * BLOB_LOC_SIZE;
        let entry = &loc_data[off..off + BLOB_LOC_SIZE];
        let pg = LittleEndian::read_u64(&entry[0..8]);
        let gen_field = LittleEndian::read_u32(&entry[8..12]);
        let id_range = LittleEndian::read_u32(&entry[12..16]);
        let start_id = LittleEndian::read_i64(&entry[16..24]);
        let size = gen_field & 0x7FFF_FFFF;
        locs.push(BlobLoc {
            pg,
            size,
            id_range,
            start_id,
        });
    }
    locs.sort_by_key(|b| b.start_id);
    locs
}

/// Parse v2+ block locator entries from idx1.
///
/// Each 24-byte `KColBlockLoc` entry has the gen field packed as:
///   - bits 0..26:  `size` (27 bits)
///   - bits 27..28: `id_type` (2 bits)
///   - bits 29..30: `pg_type` (2 bits)
///   - bit 31:      `compressed` flag
fn parse_block_locs_v2(buf: &[u8], start: usize, max_count: usize) -> Result<Vec<BlockLoc>> {
    let mut locs = Vec::new();
    if start >= buf.len() || max_count == 0 {
        return Ok(locs);
    }
    let loc_data = &buf[start..];
    let available = loc_data.len() / BLOCK_LOC_SIZE;
    let count = max_count.min(available);
    for i in 0..count {
        let off = i * BLOCK_LOC_SIZE;
        let entry = &loc_data[off..off + BLOCK_LOC_SIZE];
        let pg = LittleEndian::read_u64(&entry[0..8]);
        let gen_field = LittleEndian::read_u32(&entry[8..12]);
        let id_range = LittleEndian::read_u32(&entry[12..16]);
        let start_id = LittleEndian::read_i64(&entry[16..24]);

        // Unpack gen field (little-endian bit layout):
        //   bits  0..26 = size (27 bits)
        //   bits 27..28 = id_type (2 bits)
        //   bits 29..30 = pg_type (2 bits)
        //   bit  31     = compressed
        let size = gen_field & 0x07FF_FFFF; // lower 27 bits
        let id_type_raw = ((gen_field >> 27) & 0x3) as u8;
        let pg_type_raw = ((gen_field >> 29) & 0x3) as u8;
        let compressed = (gen_field >> 31) != 0;

        locs.push(BlockLoc {
            pg,
            size,
            id_type: BlockType::from_u8(id_type_raw)?,
            pg_type: BlockType::from_u8(pg_type_raw)?,
            compressed,
            id_range,
            start_id,
        });
    }
    Ok(locs)
}

/// Compute the number of blobs in a block from its size and type fields.
///
/// This mirrors `KColBlockLocEntryCount` from the C reference.
fn block_entry_count(block: &BlockLoc) -> u32 {
    // When both id and pg are predictable, size IS the count.
    if block.id_type == BlockType::Predictable && block.pg_type == BlockType::Predictable {
        return block.size;
    }

    let (id_hsz, id_dsz, id_ssz) = match block.id_type {
        BlockType::Random => (0, 8, 4),
        BlockType::Uniform => (4, 8, 0),
        BlockType::Magnitude => (8, 0, 4),
        BlockType::Predictable => (0, 0, 0),
    };

    let (pg_hsz, pg_dsz, pg_ssz) = match block.pg_type {
        BlockType::Random => (0, 8, 4),
        BlockType::Uniform => (4, 8, 0),
        BlockType::Magnitude => (8, 0, 4),
        BlockType::Predictable => (12, 0, 0),
    };

    let per_entry = id_dsz + id_ssz + pg_dsz + pg_ssz;
    if per_entry == 0 {
        // Should not happen (both predictable is handled above).
        return 0;
    }

    let header = id_hsz + pg_hsz;
    if (block.size as usize) <= header {
        return 0;
    }
    ((block.size as usize - header) / per_entry) as u32
}

/// Parse the idx2 block data for a single block, producing individual [`BlobLoc`] entries.
///
/// The `idx2_slice` should be exactly `block.size` bytes (the portion of the
/// idx2 file starting at `block.pg`).
///
/// The function decodes the two sections (id and pg) according to the block's
/// `id_type` and `pg_type`, then combines them into blob locators.
fn parse_idx2_block(idx2_slice: &[u8], block: &BlockLoc) -> Result<Vec<BlobLoc>> {
    let count = block_entry_count(block) as usize;
    if count == 0 {
        return Ok(Vec::new());
    }

    // --- Compute section sizes (mirrors KColIdxBlockInit) ---
    let (id_hsz, id_dsz, id_ssz): (usize, usize, usize) = match block.id_type {
        BlockType::Random => (0, 8, 4),
        BlockType::Uniform => (4, 8, 0),
        BlockType::Magnitude => (8, 0, 4),
        BlockType::Predictable => (0, 0, 0),
    };

    let (pg_hsz, pg_dsz, pg_ssz): (usize, usize, usize) = match block.pg_type {
        BlockType::Random => (0, 8, 4),
        BlockType::Uniform => (4, 8, 0),
        BlockType::Magnitude => (8, 0, 4),
        BlockType::Predictable => (12, 0, 0),
    };

    // Layout within idx2_slice:
    //   [id_header (id_hsz)]
    //   [pg_header (pg_hsz)]
    //   [id_d array (id_dsz * count)]  -- u64 values
    //   [pg_d array (pg_dsz * count)]  -- u64 values
    //   [id_s array (id_ssz * count)]  -- u32 values
    //   [pg_s array (pg_ssz * count)]  -- u32 values
    let id_h_off = 0;
    let pg_h_off = id_hsz;
    let id_d_off = pg_h_off + pg_hsz;
    let pg_d_off = id_d_off + id_dsz * count;
    let id_s_off = pg_d_off + pg_dsz * count;
    let pg_s_off = id_s_off + id_ssz * count;
    let expected_end = pg_s_off + pg_ssz * count;

    if idx2_slice.len() < expected_end {
        return Err(Error::Format(format!(
            "idx2 block too short: {} bytes, expected at least {expected_end}",
            idx2_slice.len()
        )));
    }

    let read_u32_at = |off: usize| -> u32 { LittleEndian::read_u32(&idx2_slice[off..off + 4]) };
    let read_u64_at = |off: usize| -> u64 { LittleEndian::read_u64(&idx2_slice[off..off + 8]) };

    // Build BlobLocs directly — fill id fields first, then pg fields.
    let mut blobs: Vec<BlobLoc> = Vec::with_capacity(count);

    // --- Fill start_id and id_range ---
    match block.id_type {
        BlockType::Random => {
            for i in 0..count {
                blobs.push(BlobLoc {
                    start_id: read_u64_at(id_d_off + i * 8) as i64,
                    id_range: read_u32_at(id_s_off + i * 4),
                    pg: 0,
                    size: 0,
                });
            }
        }
        BlockType::Uniform => {
            let uniform_range = read_u32_at(id_h_off);
            for i in 0..count {
                blobs.push(BlobLoc {
                    start_id: read_u64_at(id_d_off + i * 8) as i64,
                    id_range: uniform_range,
                    pg: 0,
                    size: 0,
                });
            }
        }
        BlockType::Magnitude => {
            let mut cur_id = read_u64_at(id_h_off) as i64;
            for i in 0..count {
                let rng = read_u32_at(id_s_off + i * 4);
                blobs.push(BlobLoc {
                    start_id: cur_id,
                    id_range: rng,
                    pg: 0,
                    size: 0,
                });
                cur_id += rng as i64;
            }
        }
        BlockType::Predictable => {
            let ids_per = if block.id_range as usize == count {
                1u32
            } else {
                block.id_range / count as u32
            };
            for i in 0..count {
                blobs.push(BlobLoc {
                    start_id: block.start_id + (i as u32 * ids_per) as i64,
                    id_range: ids_per,
                    pg: 0,
                    size: 0,
                });
            }
        }
    }

    // --- Fill pg and size ---
    match block.pg_type {
        BlockType::Random => {
            for (i, b) in blobs.iter_mut().enumerate() {
                b.pg = read_u64_at(pg_d_off + i * 8);
                b.size = read_u32_at(pg_s_off + i * 4);
            }
        }
        BlockType::Uniform => {
            let uniform_size = read_u32_at(pg_h_off);
            for (i, b) in blobs.iter_mut().enumerate() {
                b.pg = read_u64_at(pg_d_off + i * 8);
                b.size = uniform_size;
            }
        }
        BlockType::Magnitude => {
            let mut cur_pg = read_u64_at(pg_h_off);
            for (i, b) in blobs.iter_mut().enumerate() {
                let sz = read_u32_at(pg_s_off + i * 4);
                b.pg = cur_pg;
                b.size = sz;
                cur_pg += sz as u64;
            }
        }
        BlockType::Predictable => {
            let pred_pg = read_u64_at(pg_h_off);
            let pred_sz = read_u32_at(pg_h_off + 8);
            for (i, b) in blobs.iter_mut().enumerate() {
                b.pg = pred_pg + pred_sz as u64 * i as u64;
                b.size = pred_sz;
            }
        }
    }

    Ok(blobs)
}

/// Update column metadata from the separate `idx` file (used for v2+ columns).
///
/// The `idx` file for v2+ contains the full KColumnHdr with data_eof, page_size,
/// num_blocks, and checksum. The block locators themselves are in idx1.
fn update_meta_from_idx_file(meta: &mut ColumnMeta, buf: &[u8]) {
    if buf.len() < 8 {
        return;
    }
    let magic = LittleEndian::read_u32(&buf[0..4]);
    let swapped = magic != KDB_ENDIAN_MAGIC && magic.swap_bytes() == KDB_ENDIAN_MAGIC;
    if magic != KDB_ENDIAN_MAGIC && !swapped {
        return;
    }

    let read_u32 = |offset: usize| -> u32 {
        let v = LittleEndian::read_u32(&buf[offset..offset + 4]);
        if swapped { v.swap_bytes() } else { v }
    };
    let read_u64 = |offset: usize| -> u64 {
        let v = LittleEndian::read_u64(&buf[offset..offset + 8]);
        if swapped { v.swap_bytes() } else { v }
    };

    let version = read_u32(4);

    // v2/v3 layout in idx file:
    //   KDBHdr(8) + data_eof(8) + idx2_eof(8) + [idx0_count(4) for v3] + num_blocks(4) + page_size(4) + checksum(1)
    match version {
        2 if buf.len() >= 36 => {
            meta.data_eof = read_u64(8);
            meta.idx2_eof = read_u64(16);
            meta.num_blocks = read_u32(24);
            meta.page_size = read_u32(28);
            if meta.page_size == 0 {
                meta.page_size = 1;
            }
            meta.checksum_type = buf[32];
        }
        3 | 4 if buf.len() >= 40 => {
            meta.data_eof = read_u64(8);
            meta.idx2_eof = read_u64(16);
            // idx0_count at 24
            meta.num_blocks = read_u32(28);
            meta.page_size = read_u32(32);
            if meta.page_size == 0 {
                meta.page_size = 1;
            }
            meta.checksum_type = buf[36];
        }
        _ => {}
    }
}

fn parse_idx0(buf: &[u8]) -> Result<Vec<BlobLoc>> {
    if !buf.len().is_multiple_of(BLOB_LOC_SIZE) {
        return Err(Error::Format(format!(
            "idx0 size {} is not a multiple of {BLOB_LOC_SIZE}",
            buf.len()
        )));
    }

    let count = buf.len() / BLOB_LOC_SIZE;
    let mut blobs = Vec::with_capacity(count);

    for i in 0..count {
        let base = i * BLOB_LOC_SIZE;
        let pg = LittleEndian::read_u64(&buf[base..base + 8]);
        let gen_field = LittleEndian::read_u32(&buf[base + 8..base + 12]);
        let id_range = LittleEndian::read_u32(&buf[base + 12..base + 16]);
        let start_id = LittleEndian::read_i64(&buf[base + 16..base + 24]);

        // Extract size from lower 31 bits; bit 31 is the remove flag (skip those).
        let remove = (gen_field >> 31) != 0;
        if remove {
            continue;
        }
        let size = gen_field & 0x7FFF_FFFF;

        blobs.push(BlobLoc {
            pg,
            size,
            id_range,
            start_id,
        });
    }

    // Sort by start_id so binary search works correctly.
    blobs.sort_by_key(|b| b.start_id);

    Ok(blobs)
}

/// Create a synthetic single-blob locator covering all column data.
///
/// Used when no blob locators are found in the index files.
fn synthetic_single_blob(meta: &ColumnMeta, data_len: u64) -> BlobLoc {
    let data_size = if meta.data_eof > 0 {
        meta.data_eof.min(data_len) as u32
    } else {
        data_len as u32
    };
    tracing::debug!(
        "creating single-blob column: {data_size} bytes of data (v{})",
        meta.version,
    );
    BlobLoc {
        pg: 0,
        size: data_size,
        id_range: 0,
        start_id: 1,
    }
}

// ---------------------------------------------------------------------------
// ColumnReader implementation
// ---------------------------------------------------------------------------

impl ColumnReader {
    /// Construct a [`ColumnReader`] directly from raw column file bytes.
    ///
    /// This bypasses the KAR archive and is useful when column files have been
    /// fetched individually (e.g., via HTTP Range requests in the selective
    /// column fetch pipeline).
    ///
    /// - `idx1_bytes`: the raw `idx1` file (v1: full header + block locs; v2+: KDBHdr + block locs).
    /// - `idx0_bytes`: the raw `idx0` blob index file (legacy).
    /// - `idx_bytes`:  the raw `idx` metadata file (v2+).
    /// - `idx2_bytes`: the raw `idx2` block index data file.
    /// - `data_bytes`: the raw `data` file (concatenated blobs).
    pub fn from_parts(
        idx1_bytes: &[u8],
        idx0_bytes: &[u8],
        idx_bytes: &[u8],
        idx2_bytes: &[u8],
        data_bytes: Vec<u8>,
    ) -> Result<Self> {
        let mut meta = parse_idx1(idx1_bytes)?;
        let mut blobs = parse_idx0(idx0_bytes)?;

        // For v2+, update metadata from the `idx` file (which has the full header).
        if !idx_bytes.is_empty() && meta.version >= 2 {
            update_meta_from_idx_file(&mut meta, idx_bytes);
        }

        // For v2+ columns with idx2 data, parse block locators from idx1 and
        // decode idx2 to get individual blob locations.
        if blobs.is_empty() && meta.version >= 2 && !idx2_bytes.is_empty() {
            let header_end = 8usize; // KDBHdr size
            let block_locs = parse_block_locs_v2(idx1_bytes, header_end, meta.num_blocks as usize)?;

            tracing::debug!(
                "v{}: parsed {} block locators from idx1, decoding idx2 ({} bytes)",
                meta.version,
                block_locs.len(),
                idx2_bytes.len(),
            );

            for bloc in &block_locs {
                if bloc.compressed {
                    return Err(Error::Format(
                        "compressed idx2 blocks are not supported".into(),
                    ));
                }

                let start = bloc.pg as usize;
                let end = start + bloc.size as usize;

                // For predictable+predictable, the idx2 data is exactly 12 bytes
                // regardless of the size field (which holds the count).
                let slice_end = if bloc.id_type == BlockType::Predictable
                    && bloc.pg_type == BlockType::Predictable
                {
                    (start + 12).min(idx2_bytes.len())
                } else {
                    end.min(idx2_bytes.len())
                };

                if start > idx2_bytes.len() {
                    return Err(Error::Format(format!(
                        "idx2 block offset {start} exceeds idx2 size {}",
                        idx2_bytes.len()
                    )));
                }

                let idx2_slice = &idx2_bytes[start..slice_end];
                let block_blobs = parse_idx2_block(idx2_slice, bloc)?;
                blobs.extend(block_blobs);
            }

            blobs.sort_by_key(|b| b.start_id);
        }

        // For v1 columns with idx0, the blobs from parse_idx0 are correct.
        // For v1 columns without idx0, use the idx1 block_locs directly.
        if blobs.is_empty() && meta.version <= 1 && !meta.block_locs.is_empty() {
            tracing::debug!(
                "idx0 empty (v1); using {} block locators from idx1",
                meta.block_locs.len()
            );
            blobs = meta.block_locs.clone();
        }

        // When no blob locators are available at all, create a single
        // synthetic blob covering all data.
        if blobs.is_empty() && !data_bytes.is_empty() {
            blobs.push(synthetic_single_blob(&meta, data_bytes.len() as u64));
        }

        Ok(Self {
            meta,
            blobs,
            data: DataSource::InMemory(data_bytes),
        })
    }

    /// Open a column from the KAR archive by path.
    ///
    /// The `col_path` should be the directory containing `idx0`, `idx1`, `idx2`,
    /// and `data` files — for example `"SRR123456/tbl/SEQUENCE/col/READ"`.
    ///
    /// The `sra_path` is the path to the SRA file on disk. The column data
    /// file is NOT loaded into memory; instead its offset within the SRA
    /// file is recorded so that individual blobs can be read on demand.
    pub fn open<R: std::io::Read + Seek>(
        archive: &mut KarArchive<R>,
        col_path: &str,
        sra_path: &std::path::Path,
    ) -> Result<Self> {
        let idx1_path = format!("{col_path}/idx1");
        let idx0_path = format!("{col_path}/idx0");
        let idx_path = format!("{col_path}/idx");
        let idx2_path = format!("{col_path}/idx2");
        let data_path = format!("{col_path}/data");

        // Parse idx1 (required).
        let idx1_buf = archive
            .read_file(&idx1_path)
            .map_err(|_| Error::Format(format!("column header (idx1) not found at {idx1_path}")))?;

        // Parse idx0 (may be empty or missing — legacy format).
        let idx0_buf = archive.read_file(&idx0_path).unwrap_or_default();

        // Read the separate idx file (v2+ metadata).
        let idx_buf = archive.read_file(&idx_path).unwrap_or_default();

        // Read the idx2 block index data (v2+).
        let idx2_buf = archive.read_file(&idx2_path).unwrap_or_default();

        // Get the data file location without reading it into memory.
        let data_location = archive.file_location(&data_path);

        // Build the column using from_parts with an empty data buffer
        // (the index files are small; only the data file is large).
        let mut reader = Self::from_parts(&idx1_buf, &idx0_buf, &idx_buf, &idx2_buf, Vec::new())?;

        // Replace the InMemory(empty) data source with OnDisk if the data
        // file exists in the archive.
        if let Some((file_offset, file_size)) = data_location {
            // If no blob locators were found (from_parts skipped the
            // synthetic blob because data_bytes was empty), create one
            // now using the on-disk file size.
            if reader.blobs.is_empty() && file_size > 0 {
                reader
                    .blobs
                    .push(synthetic_single_blob(&reader.meta, file_size));
            }

            let file = std::fs::File::open(sra_path)?;
            let mmap = unsafe {
                memmap2::MmapOptions::new()
                    .offset(file_offset)
                    .len(file_size as usize)
                    .map(&file)
                    .map_err(|e| Error::Format(format!("mmap failed: {e}")))?
            };
            reader.data = DataSource::Mmap {
                mmap,
                _file_offset: file_offset,
            };
        }

        Ok(reader)
    }

    /// Read the raw (unprocessed) blob bytes for a given row ID.
    ///
    /// Does NOT attempt decompression. Use [`crate::blob::decode_blob`]
    /// for proper envelope parsing, CRC validation, and decompression.
    pub fn read_raw_blob_for_row(&self, row_id: i64) -> Result<Vec<u8>> {
        self.read_raw_blob_slice(row_id).map(|s| s.to_vec())
    }

    /// Zero-copy variant: returns a slice into the underlying data (mmap or
    /// in-memory buffer) without allocating.
    pub fn read_raw_blob_slice(&self, row_id: i64) -> Result<&[u8]> {
        let blob = self
            .find_blob(row_id)
            .ok_or_else(|| Error::Format(format!("no blob found for row_id {row_id}")))?;

        let blob_offset = self.blob_data_offset(blob);
        let size = blob.size as usize;

        match &self.data {
            DataSource::InMemory(data) => {
                if blob_offset + size > data.len() {
                    return Err(Error::Format(format!(
                        "blob data out of bounds: offset={blob_offset}, size={size}, data_len={}",
                        data.len()
                    )));
                }
                Ok(&data[blob_offset..blob_offset + size])
            }
            DataSource::Mmap { mmap, .. } => {
                if blob_offset + size > mmap.len() {
                    return Err(Error::Format(format!(
                        "blob data out of bounds: offset={blob_offset}, size={size}, mmap_len={}",
                        mmap.len()
                    )));
                }
                Ok(&mmap[blob_offset..blob_offset + size])
            }
        }
    }

    /// Find the blob that contains a given row ID.
    ///
    /// Uses binary search on `start_id`. A blob covers rows
    /// `[start_id, start_id + id_range)`.
    pub fn find_blob(&self, row_id: i64) -> Option<&BlobLoc> {
        // Binary search: find the last blob whose start_id <= row_id.
        let idx = match self.blobs.binary_search_by_key(&row_id, |b| b.start_id) {
            Ok(i) => i,
            Err(0) => return None,
            Err(i) => i - 1,
        };

        let blob = &self.blobs[idx];
        // id_range == 0 means "covers all rows" (synthetic single-blob column).
        if blob.id_range == 0 {
            return Some(blob);
        }
        let end_id = blob.start_id + blob.id_range as i64;
        if row_id >= blob.start_id && row_id < end_id {
            Some(blob)
        } else {
            None
        }
    }

    /// Number of blobs in this column.
    pub fn blob_count(&self) -> usize {
        self.blobs.len()
    }

    /// Iterate all blobs in order of `start_id`.
    pub fn blobs(&self) -> &[BlobLoc] {
        &self.blobs
    }

    /// Total row count (sum of all `id_range` values).
    pub fn row_count(&self) -> u64 {
        self.blobs.iter().map(|b| u64::from(b.id_range)).sum()
    }

    /// First row ID across all blobs, or `None` if there are no blobs.
    pub fn first_row_id(&self) -> Option<i64> {
        self.blobs.first().map(|b| b.start_id)
    }

    /// Return a reference to the column metadata.
    pub fn meta(&self) -> &ColumnMeta {
        &self.meta
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    /// Compute the byte offset into the data buffer for a given blob.
    fn blob_data_offset(&self, blob: &BlobLoc) -> usize {
        if self.meta.page_size <= 1 {
            blob.pg as usize
        } else {
            (blob.pg * u64::from(self.meta.page_size)) as usize
        }
    }
}

// ---------------------------------------------------------------------------
// Test helpers (shared with cursor tests)
// ---------------------------------------------------------------------------

#[cfg(test)]
pub(crate) mod test_helpers {
    use byteorder::{ByteOrder, LittleEndian};

    use super::BLOB_LOC_SIZE;
    use super::KDB_ENDIAN_MAGIC;

    /// Build a single 24-byte blob locator entry for idx0.
    pub fn build_blob_loc(pg: u64, size: u32, id_range: u32, start_id: i64) -> Vec<u8> {
        let mut buf = vec![0u8; BLOB_LOC_SIZE];
        LittleEndian::write_u64(&mut buf[0..8], pg);
        // gen field: lower 31 bits = size, bit 31 = 0 (not removed)
        LittleEndian::write_u32(&mut buf[8..12], size & 0x7FFF_FFFF);
        LittleEndian::write_u32(&mut buf[12..16], id_range);
        LittleEndian::write_i64(&mut buf[16..24], start_id);
        buf
    }

    /// Build a removed blob locator entry (bit 31 of gen set).
    pub fn build_removed_blob_loc(pg: u64, size: u32, id_range: u32, start_id: i64) -> Vec<u8> {
        let mut buf = vec![0u8; BLOB_LOC_SIZE];
        LittleEndian::write_u64(&mut buf[0..8], pg);
        LittleEndian::write_u32(&mut buf[8..12], (size & 0x7FFF_FFFF) | 0x8000_0000);
        LittleEndian::write_u32(&mut buf[12..16], id_range);
        LittleEndian::write_i64(&mut buf[16..24], start_id);
        buf
    }

    /// Build a minimal v1 idx1 header.
    pub fn build_idx1_v1(data_eof: u64, page_size: u32, checksum: u8) -> Vec<u8> {
        let mut buf = vec![0u8; 33];
        LittleEndian::write_u32(&mut buf[0..4], KDB_ENDIAN_MAGIC);
        LittleEndian::write_u32(&mut buf[4..8], 1); // version
        LittleEndian::write_u64(&mut buf[8..16], data_eof);
        LittleEndian::write_u64(&mut buf[16..24], 0); // idx2_eof
        LittleEndian::write_u32(&mut buf[24..28], 0); // num_blocks
        LittleEndian::write_u32(&mut buf[28..32], page_size);
        buf[32] = checksum;
        buf
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// Test-only convenience method: decompress + read in one call.
// Production code uses read_raw_blob_slice() + decode_blob() instead.
#[cfg(test)]
fn try_zlib_decompress(data: &[u8]) -> Option<Vec<u8>> {
    use crate::blob;

    if data.is_empty() {
        return None;
    }
    let estimated = data.len() * 4;
    if let Ok(out) = blob::zlib_decompress(data, estimated)
        && !out.is_empty()
    {
        return Some(out);
    }
    if let Ok(out) = blob::deflate_decompress(data, estimated)
        && !out.is_empty()
    {
        return Some(out);
    }
    None
}

#[cfg(test)]
impl ColumnReader {
    pub fn read_blob_for_row(&self, row_id: i64) -> Result<Vec<u8>> {
        let raw = self.read_raw_blob_for_row(row_id)?;
        Ok(try_zlib_decompress(&raw).unwrap_or(raw))
    }
}

#[cfg(test)]
mod tests {
    use super::test_helpers::*;
    use super::*;

    // -----------------------------------------------------------------------
    // idx0 parsing
    // -----------------------------------------------------------------------

    #[test]
    fn parse_idx0_empty() {
        let blobs = parse_idx0(&[]).unwrap();
        assert!(blobs.is_empty());
    }

    #[test]
    fn parse_idx0_single_blob() {
        let buf = build_blob_loc(0, 100, 10, 1);
        let blobs = parse_idx0(&buf).unwrap();
        assert_eq!(blobs.len(), 1);
        assert_eq!(blobs[0].pg, 0);
        assert_eq!(blobs[0].size, 100);
        assert_eq!(blobs[0].id_range, 10);
        assert_eq!(blobs[0].start_id, 1);
    }

    #[test]
    fn parse_idx0_multiple_blobs() {
        let mut buf = build_blob_loc(0, 50, 5, 1);
        buf.extend_from_slice(&build_blob_loc(50, 60, 5, 6));
        buf.extend_from_slice(&build_blob_loc(110, 40, 3, 11));

        let blobs = parse_idx0(&buf).unwrap();
        assert_eq!(blobs.len(), 3);
        assert_eq!(blobs[0].start_id, 1);
        assert_eq!(blobs[1].start_id, 6);
        assert_eq!(blobs[2].start_id, 11);
    }

    #[test]
    fn parse_idx0_removes_flagged_entries() {
        let mut buf = build_blob_loc(0, 50, 5, 1);
        buf.extend_from_slice(&build_removed_blob_loc(50, 60, 5, 6));
        buf.extend_from_slice(&build_blob_loc(110, 40, 3, 11));

        let blobs = parse_idx0(&buf).unwrap();
        assert_eq!(blobs.len(), 2);
        assert_eq!(blobs[0].start_id, 1);
        assert_eq!(blobs[1].start_id, 11);
    }

    #[test]
    fn parse_idx0_sorts_by_start_id() {
        // Insert in reverse order.
        let mut buf = build_blob_loc(110, 40, 3, 11);
        buf.extend_from_slice(&build_blob_loc(0, 50, 5, 1));
        buf.extend_from_slice(&build_blob_loc(50, 60, 5, 6));

        let blobs = parse_idx0(&buf).unwrap();
        assert_eq!(blobs[0].start_id, 1);
        assert_eq!(blobs[1].start_id, 6);
        assert_eq!(blobs[2].start_id, 11);
    }

    #[test]
    fn parse_idx0_invalid_size() {
        // 10 bytes is not a multiple of 24.
        let buf = vec![0u8; 10];
        assert!(parse_idx0(&buf).is_err());
    }

    // -----------------------------------------------------------------------
    // idx1 parsing
    // -----------------------------------------------------------------------

    #[test]
    fn parse_idx1_v1_basic() {
        let buf = build_idx1_v1(4096, 1, 0);
        let meta = parse_idx1(&buf).unwrap();
        assert_eq!(meta.version, 1);
        assert_eq!(meta.data_eof, 4096);
        assert_eq!(meta.page_size, 1);
        assert_eq!(meta.checksum_type, 0);
    }

    #[test]
    fn parse_idx1_v1_with_page_size() {
        let buf = build_idx1_v1(8192, 4096, 1);
        let meta = parse_idx1(&buf).unwrap();
        assert_eq!(meta.page_size, 4096);
        assert_eq!(meta.checksum_type, 1);
    }

    #[test]
    fn parse_idx1_zero_page_size_becomes_one() {
        let buf = build_idx1_v1(100, 0, 0);
        let meta = parse_idx1(&buf).unwrap();
        assert_eq!(meta.page_size, 1);
    }

    #[test]
    fn parse_idx1_bad_magic() {
        let mut buf = build_idx1_v1(100, 1, 0);
        LittleEndian::write_u32(&mut buf[0..4], 0xDEADBEEF);
        assert!(parse_idx1(&buf).is_err());
    }

    #[test]
    fn parse_idx1_too_short() {
        let buf = vec![0u8; 8];
        assert!(parse_idx1(&buf).is_err());
    }

    #[test]
    fn parse_idx1_byte_swapped() {
        let mut buf = vec![0u8; 33];
        LittleEndian::write_u32(&mut buf[0..4], KDB_ENDIAN_MAGIC.swap_bytes());
        LittleEndian::write_u32(&mut buf[4..8], 1u32.swap_bytes());
        LittleEndian::write_u64(&mut buf[8..16], 512u64.swap_bytes());
        LittleEndian::write_u64(&mut buf[16..24], 0u64);
        LittleEndian::write_u32(&mut buf[24..28], 0u32);
        LittleEndian::write_u32(&mut buf[28..32], 1u32.swap_bytes());
        buf[32] = 0;

        let meta = parse_idx1(&buf).unwrap();
        assert_eq!(meta.version, 1);
        assert_eq!(meta.data_eof, 512);
        assert_eq!(meta.page_size, 1);
    }

    // -----------------------------------------------------------------------
    // find_blob
    // -----------------------------------------------------------------------

    #[test]
    fn find_blob_exact_start() {
        let blobs = vec![
            BlobLoc {
                pg: 0,
                size: 50,
                id_range: 5,
                start_id: 1,
            },
            BlobLoc {
                pg: 50,
                size: 60,
                id_range: 5,
                start_id: 6,
            },
        ];
        let reader = ColumnReader {
            meta: ColumnMeta {
                version: 1,
                data_eof: 110,
                idx2_eof: 0,
                page_size: 1,
                checksum_type: 0,
                num_blocks: 0,
                block_locs: vec![],
            },
            blobs,
            data: DataSource::InMemory(vec![0u8; 110]),
        };

        let b = reader.find_blob(1).unwrap();
        assert_eq!(b.start_id, 1);

        let b = reader.find_blob(6).unwrap();
        assert_eq!(b.start_id, 6);
    }

    #[test]
    fn find_blob_within_range() {
        let blobs = vec![
            BlobLoc {
                pg: 0,
                size: 50,
                id_range: 5,
                start_id: 1,
            },
            BlobLoc {
                pg: 50,
                size: 60,
                id_range: 5,
                start_id: 6,
            },
        ];
        let reader = ColumnReader {
            meta: ColumnMeta {
                version: 1,
                data_eof: 110,
                idx2_eof: 0,
                page_size: 1,
                checksum_type: 0,
                num_blocks: 0,
                block_locs: vec![],
            },
            blobs,
            data: DataSource::InMemory(vec![0u8; 110]),
        };

        let b = reader.find_blob(3).unwrap();
        assert_eq!(b.start_id, 1);

        let b = reader.find_blob(10).unwrap();
        assert_eq!(b.start_id, 6);
    }

    #[test]
    fn find_blob_out_of_range() {
        let blobs = vec![BlobLoc {
            pg: 0,
            size: 50,
            id_range: 5,
            start_id: 1,
        }];
        let reader = ColumnReader {
            meta: ColumnMeta {
                version: 1,
                data_eof: 50,
                idx2_eof: 0,
                page_size: 1,
                checksum_type: 0,
                num_blocks: 0,
                block_locs: vec![],
            },
            blobs,
            data: DataSource::InMemory(vec![0u8; 50]),
        };

        // Before first blob.
        assert!(reader.find_blob(0).is_none());
        // After last blob.
        assert!(reader.find_blob(6).is_none());
    }

    #[test]
    fn find_blob_empty() {
        let reader = ColumnReader {
            meta: ColumnMeta {
                version: 1,
                data_eof: 0,
                idx2_eof: 0,
                page_size: 1,
                checksum_type: 0,
                num_blocks: 0,
                block_locs: vec![],
            },
            blobs: Vec::new(),
            data: DataSource::InMemory(Vec::new()),
        };
        assert!(reader.find_blob(1).is_none());
    }

    // -----------------------------------------------------------------------
    // row_count / first_row_id / blob_count
    // -----------------------------------------------------------------------

    #[test]
    fn row_count_sums_id_ranges() {
        let blobs = vec![
            BlobLoc {
                pg: 0,
                size: 10,
                id_range: 5,
                start_id: 1,
            },
            BlobLoc {
                pg: 10,
                size: 20,
                id_range: 8,
                start_id: 6,
            },
        ];
        let reader = ColumnReader {
            meta: ColumnMeta {
                version: 1,
                data_eof: 30,
                idx2_eof: 0,
                page_size: 1,
                checksum_type: 0,
                num_blocks: 0,
                block_locs: vec![],
            },
            blobs,
            data: DataSource::InMemory(vec![0u8; 30]),
        };
        assert_eq!(reader.row_count(), 13);
    }

    #[test]
    fn first_row_id_returns_min() {
        let blobs = vec![
            BlobLoc {
                pg: 0,
                size: 10,
                id_range: 5,
                start_id: 1,
            },
            BlobLoc {
                pg: 10,
                size: 20,
                id_range: 8,
                start_id: 6,
            },
        ];
        let reader = ColumnReader {
            meta: ColumnMeta {
                version: 1,
                data_eof: 30,
                idx2_eof: 0,
                page_size: 1,
                checksum_type: 0,
                num_blocks: 0,
                block_locs: vec![],
            },
            blobs,
            data: DataSource::InMemory(vec![0u8; 30]),
        };
        assert_eq!(reader.first_row_id(), Some(1));
    }

    #[test]
    fn first_row_id_none_for_empty() {
        let reader = ColumnReader {
            meta: ColumnMeta {
                version: 1,
                data_eof: 0,
                idx2_eof: 0,
                page_size: 1,
                checksum_type: 0,
                num_blocks: 0,
                block_locs: vec![],
            },
            blobs: Vec::new(),
            data: DataSource::InMemory(Vec::new()),
        };
        assert_eq!(reader.first_row_id(), None);
    }

    #[test]
    fn blob_count_is_correct() {
        let blobs = vec![
            BlobLoc {
                pg: 0,
                size: 10,
                id_range: 5,
                start_id: 1,
            },
            BlobLoc {
                pg: 10,
                size: 20,
                id_range: 8,
                start_id: 6,
            },
            BlobLoc {
                pg: 30,
                size: 15,
                id_range: 3,
                start_id: 14,
            },
        ];
        let reader = ColumnReader {
            meta: ColumnMeta {
                version: 1,
                data_eof: 45,
                idx2_eof: 0,
                page_size: 1,
                checksum_type: 0,
                num_blocks: 0,
                block_locs: vec![],
            },
            blobs,
            data: DataSource::InMemory(vec![0u8; 45]),
        };
        assert_eq!(reader.blob_count(), 3);
    }

    // -----------------------------------------------------------------------
    // blob_data_offset with page_size
    // -----------------------------------------------------------------------

    #[test]
    fn blob_data_offset_page_size_one() {
        let reader = ColumnReader {
            meta: ColumnMeta {
                version: 1,
                data_eof: 200,
                idx2_eof: 0,
                page_size: 1,
                checksum_type: 0,
                num_blocks: 0,
                block_locs: vec![],
            },
            blobs: Vec::new(),
            data: DataSource::InMemory(vec![0u8; 200]),
        };
        let blob = BlobLoc {
            pg: 42,
            size: 10,
            id_range: 1,
            start_id: 1,
        };
        assert_eq!(reader.blob_data_offset(&blob), 42);
    }

    #[test]
    fn blob_data_offset_page_size_4096() {
        let reader = ColumnReader {
            meta: ColumnMeta {
                version: 1,
                data_eof: 200,
                idx2_eof: 0,
                page_size: 4096,
                checksum_type: 0,
                num_blocks: 0,
                block_locs: vec![],
            },
            blobs: Vec::new(),
            data: DataSource::InMemory(vec![0u8; 200]),
        };
        let blob = BlobLoc {
            pg: 3,
            size: 10,
            id_range: 1,
            start_id: 1,
        };
        assert_eq!(reader.blob_data_offset(&blob), 3 * 4096);
    }

    // -----------------------------------------------------------------------
    // read_blob_for_row with decompression
    // -----------------------------------------------------------------------

    #[test]
    fn read_blob_for_row_raw_data() {
        let data = b"Hello, world!";
        let blobs = vec![BlobLoc {
            pg: 0,
            size: data.len() as u32,
            id_range: 1,
            start_id: 1,
        }];
        let reader = ColumnReader {
            meta: ColumnMeta {
                version: 1,
                data_eof: data.len() as u64,
                idx2_eof: 0,
                page_size: 1,
                checksum_type: 0,
                num_blocks: 0,
                block_locs: vec![],
            },
            blobs,
            data: DataSource::InMemory(data.to_vec()),
        };

        let result = reader.read_blob_for_row(1).unwrap();
        assert_eq!(result, b"Hello, world!");
    }

    #[test]
    fn read_blob_for_row_zlib_compressed() {
        use flate2::Compression;
        use flate2::write::ZlibEncoder;
        use std::io::Write;

        let original = b"ACGTACGTACGTACGTACGTACGT";
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(original).unwrap();
        let compressed = encoder.finish().unwrap();

        let blobs = vec![BlobLoc {
            pg: 0,
            size: compressed.len() as u32,
            id_range: 1,
            start_id: 1,
        }];
        let reader = ColumnReader {
            meta: ColumnMeta {
                version: 1,
                data_eof: compressed.len() as u64,
                idx2_eof: 0,
                page_size: 1,
                checksum_type: 0,
                num_blocks: 0,
                block_locs: vec![],
            },
            blobs,
            data: DataSource::InMemory(compressed),
        };

        let result = reader.read_blob_for_row(1).unwrap();
        assert_eq!(result, original);
    }

    #[test]
    fn read_blob_for_row_missing_row() {
        let reader = ColumnReader {
            meta: ColumnMeta {
                version: 1,
                data_eof: 0,
                idx2_eof: 0,
                page_size: 1,
                checksum_type: 0,
                num_blocks: 0,
                block_locs: vec![],
            },
            blobs: Vec::new(),
            data: DataSource::InMemory(Vec::new()),
        };
        assert!(reader.read_blob_for_row(1).is_err());
    }

    #[test]
    fn read_blob_for_row_out_of_bounds_data() {
        let blobs = vec![BlobLoc {
            pg: 0,
            size: 100,
            id_range: 1,
            start_id: 1,
        }];
        let reader = ColumnReader {
            meta: ColumnMeta {
                version: 1,
                data_eof: 100,
                idx2_eof: 0,
                page_size: 1,
                checksum_type: 0,
                num_blocks: 0,
                block_locs: vec![],
            },
            blobs,
            data: DataSource::InMemory(vec![0u8; 10]), // data is too small
        };
        assert!(reader.read_blob_for_row(1).is_err());
    }

    // -----------------------------------------------------------------------
    // Integration: open() with KarArchive test helpers
    // -----------------------------------------------------------------------

    #[test]
    fn open_from_kar_archive() {
        use crate::kar::test_helpers::*;
        use std::io::Cursor;

        // Build idx1 (v1 header)
        let idx1 = build_idx1_v1(5, 1, 0);

        // Build idx0 with one blob: pg=0, size=5, id_range=1, start_id=1
        let idx0 = build_blob_loc(0, 5, 1, 1);

        // Build data: 5 bytes
        let col_data = b"ACGTN";

        // Build the KAR directory structure:
        // SRR/tbl/SEQUENCE/col/READ/idx1
        // SRR/tbl/SEQUENCE/col/READ/idx0
        // SRR/tbl/SEQUENCE/col/READ/data
        //
        // We need to compute offsets carefully. All three files are in the data section.
        // idx1 is first, then idx0, then col_data.
        let total_data_size = idx1.len() + idx0.len() + col_data.len();
        let mut data_section = Vec::with_capacity(total_data_size);
        let idx1_offset = 0u64;
        data_section.extend_from_slice(&idx1);
        let idx0_offset = data_section.len() as u64;
        data_section.extend_from_slice(&idx0);
        let col_data_offset = data_section.len() as u64;
        data_section.extend_from_slice(col_data);

        let idx1_node = build_file_node("idx1", idx1_offset, idx1.len() as u64);
        let idx0_node = build_file_node("idx0", idx0_offset, idx0.len() as u64);
        let data_node = build_file_node("data", col_data_offset, col_data.len() as u64);

        let read_dir = build_dir_node("READ", &[&data_node, &idx0_node, &idx1_node]);
        let col_dir = build_dir_node("col", &[&read_dir]);
        let seq_dir = build_dir_node("SEQUENCE", &[&col_dir]);
        let tbl_dir = build_dir_node("tbl", &[&seq_dir]);
        let root_dir = build_dir_node("SRR", &[&tbl_dir]);

        let archive_bytes = build_kar_archive(&[&root_dir], &data_section);

        // Write to a temp file so OnDisk reads work.
        let sra_path =
            std::env::temp_dir().join(format!("sracha-kdb-test-{}.sra", std::process::id(),));
        std::fs::write(&sra_path, &archive_bytes).unwrap();

        let mut archive = KarArchive::open(Cursor::new(archive_bytes)).unwrap();

        let reader =
            ColumnReader::open(&mut archive, "SRR/tbl/SEQUENCE/col/READ", &sra_path).unwrap();
        assert_eq!(reader.blob_count(), 1);
        assert_eq!(reader.row_count(), 1);
        assert_eq!(reader.first_row_id(), Some(1));

        let blob_data = reader.read_blob_for_row(1).unwrap();
        assert_eq!(blob_data, b"ACGTN");

        let _ = std::fs::remove_file(&sra_path);
    }

    // -----------------------------------------------------------------------
    // BlockLoc parsing (v2+ idx1)
    // -----------------------------------------------------------------------

    /// Build a 24-byte v2+ block locator entry for idx1.
    ///
    /// The gen field packs: size(27) | id_type(2) | pg_type(2) | compressed(1).
    fn build_block_loc_entry(
        pg: u64,
        size: u32,
        id_type: u8,
        pg_type: u8,
        compressed: bool,
        id_range: u32,
        start_id: i64,
    ) -> Vec<u8> {
        let mut buf = vec![0u8; BLOCK_LOC_SIZE];
        LittleEndian::write_u64(&mut buf[0..8], pg);
        let gen_field = (size & 0x07FF_FFFF)
            | ((id_type as u32 & 0x3) << 27)
            | ((pg_type as u32 & 0x3) << 29)
            | ((compressed as u32) << 31);
        LittleEndian::write_u32(&mut buf[8..12], gen_field);
        LittleEndian::write_u32(&mut buf[12..16], id_range);
        LittleEndian::write_i64(&mut buf[16..24], start_id);
        buf
    }

    /// Build a v2+ idx1 file: KDBHdr(8) + block locator entries.
    fn build_idx1_v2(block_entries: &[Vec<u8>]) -> Vec<u8> {
        let mut buf = vec![0u8; 8];
        LittleEndian::write_u32(&mut buf[0..4], KDB_ENDIAN_MAGIC);
        LittleEndian::write_u32(&mut buf[4..8], 3); // version 3
        for entry in block_entries {
            buf.extend_from_slice(entry);
        }
        buf
    }

    #[test]
    fn parse_block_locs_v2_basic() {
        let entry = build_block_loc_entry(0, 924, 3, 2, false, 468992, 1);
        let idx1 = build_idx1_v2(&[entry]);
        let locs = parse_block_locs_v2(&idx1, 8, 1).unwrap();
        assert_eq!(locs.len(), 1);
        assert_eq!(locs[0].pg, 0);
        assert_eq!(locs[0].size, 924);
        assert_eq!(locs[0].id_type, BlockType::Predictable);
        assert_eq!(locs[0].pg_type, BlockType::Magnitude);
        assert!(!locs[0].compressed);
        assert_eq!(locs[0].id_range, 468992);
        assert_eq!(locs[0].start_id, 1);
    }

    #[test]
    fn parse_block_locs_v2_all_types() {
        // Test all four type combinations parse correctly.
        for id_t in 0..4u8 {
            for pg_t in 0..4u8 {
                let entry = build_block_loc_entry(100, 500, id_t, pg_t, false, 1000, 42);
                let idx1 = build_idx1_v2(&[entry]);
                let locs = parse_block_locs_v2(&idx1, 8, 1).unwrap();
                assert_eq!(locs.len(), 1);
                assert_eq!(locs[0].id_type as u8, id_t);
                assert_eq!(locs[0].pg_type as u8, pg_t);
            }
        }
    }

    #[test]
    fn parse_block_locs_v2_compressed_flag() {
        let entry = build_block_loc_entry(0, 100, 0, 0, true, 10, 1);
        let idx1 = build_idx1_v2(&[entry]);
        let locs = parse_block_locs_v2(&idx1, 8, 1).unwrap();
        assert!(locs[0].compressed);
    }

    // -----------------------------------------------------------------------
    // block_entry_count
    // -----------------------------------------------------------------------

    #[test]
    fn block_entry_count_predictable_predictable() {
        // Both predictable: size IS the count.
        let bloc = BlockLoc {
            pg: 0,
            size: 229,
            id_type: BlockType::Predictable,
            pg_type: BlockType::Predictable,
            compressed: false,
            id_range: 468992,
            start_id: 1,
        };
        assert_eq!(block_entry_count(&bloc), 229);
    }

    #[test]
    fn block_entry_count_predictable_magnitude() {
        // SRR000001-like: id_type=3(predictable), pg_type=2(magnitude).
        // id: hsz=0, dsz=0, ssz=0; pg: hsz=8, dsz=0, ssz=4.
        // count = (924 - 0 - 8) / (0 + 0 + 0 + 4) = 916 / 4 = 229.
        let bloc = BlockLoc {
            pg: 0,
            size: 924,
            id_type: BlockType::Predictable,
            pg_type: BlockType::Magnitude,
            compressed: false,
            id_range: 468992,
            start_id: 1,
        };
        assert_eq!(block_entry_count(&bloc), 229);
    }

    #[test]
    fn block_entry_count_random_random() {
        // id: hsz=0, dsz=8, ssz=4; pg: hsz=0, dsz=8, ssz=4.
        // per_entry = 8+4+8+4 = 24. header = 0.
        // count = 240 / 24 = 10.
        let bloc = BlockLoc {
            pg: 0,
            size: 240,
            id_type: BlockType::Random,
            pg_type: BlockType::Random,
            compressed: false,
            id_range: 100,
            start_id: 1,
        };
        assert_eq!(block_entry_count(&bloc), 10);
    }

    #[test]
    fn block_entry_count_uniform_uniform() {
        // id: hsz=4, dsz=8, ssz=0; pg: hsz=4, dsz=8, ssz=0.
        // per_entry = 8+0+8+0 = 16. header = 4+4 = 8.
        // count = (168 - 8) / 16 = 160 / 16 = 10.
        let bloc = BlockLoc {
            pg: 0,
            size: 168,
            id_type: BlockType::Uniform,
            pg_type: BlockType::Uniform,
            compressed: false,
            id_range: 100,
            start_id: 1,
        };
        assert_eq!(block_entry_count(&bloc), 10);
    }

    // -----------------------------------------------------------------------
    // parse_idx2_block — all type combinations
    // -----------------------------------------------------------------------

    /// Build idx2 data for pg_type=Predictable (type 3): h.pred.pg(u64) + h.pred.sz(u32).
    fn build_idx2_pg_predictable(pg: u64, sz: u32) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&pg.to_le_bytes());
        buf.extend_from_slice(&sz.to_le_bytes());
        buf
    }

    /// Combine id and pg sections into a complete idx2 block.
    ///
    /// Layout: [id_header][pg_header][id_d][pg_d][id_s][pg_s]
    ///
    /// This helper handles the correct interleaving for all type combinations.
    #[allow(clippy::too_many_arguments)]
    fn build_idx2_data(
        id_type: BlockType,
        pg_type: BlockType,
        count: usize,
        id_header: &[u8], // id_hsz bytes
        pg_header: &[u8], // pg_hsz bytes
        id_d: &[u8],      // id_dsz * count bytes (u64 array)
        pg_d: &[u8],      // pg_dsz * count bytes (u64 array)
        id_s: &[u8],      // id_ssz * count bytes (u32 array)
        pg_s: &[u8],      // pg_ssz * count bytes (u32 array)
    ) -> Vec<u8> {
        let _ = (id_type, pg_type, count); // used only for documentation
        let mut buf = Vec::new();
        buf.extend_from_slice(id_header);
        buf.extend_from_slice(pg_header);
        buf.extend_from_slice(id_d);
        buf.extend_from_slice(pg_d);
        buf.extend_from_slice(id_s);
        buf.extend_from_slice(pg_s);
        buf
    }

    #[test]
    fn idx2_predictable_magnitude_srr000001_like() {
        // SRR000001 block 0: id_type=3(predictable), pg_type=2(magnitude).
        // 229 blobs, id_range=468992, start_id=1.
        // ids_per = 468992 / 229 = 2048.
        //
        // idx2 layout: [pg_header: first_pg(u64)] [pg_s: 229 x u32 sizes]
        // No id data in idx2 (both id header and data are 0 bytes).
        let count = 229usize;
        let ids_per = 2048u32;
        let first_pg = 0u64;

        // Generate realistic blob sizes (varying around 4000 bytes).
        let mut sizes = Vec::with_capacity(count);
        for i in 0..count {
            sizes.push(3800 + (i as u32 % 400));
        }

        // Build the idx2 data: [first_pg(u64)] [sizes: 229 x u32].
        // id_hsz=0, pg_hsz=8, id_dsz=0, pg_dsz=0, id_ssz=0, pg_ssz=4.
        let idx2 = build_idx2_data(
            BlockType::Predictable,
            BlockType::Magnitude,
            count,
            &[],                     // id_header (0 bytes)
            &first_pg.to_le_bytes(), // pg_header (8 bytes)
            &[],                     // id_d (0)
            &[],                     // pg_d (0)
            &[],                     // id_s (0)
            &sizes
                .iter()
                .flat_map(|s| s.to_le_bytes())
                .collect::<Vec<_>>(), // pg_s
        );

        assert_eq!(idx2.len(), 8 + 229 * 4); // = 924 bytes

        let bloc = BlockLoc {
            pg: 0,
            size: 924,
            id_type: BlockType::Predictable,
            pg_type: BlockType::Magnitude,
            compressed: false,
            id_range: 468992,
            start_id: 1,
        };

        assert_eq!(block_entry_count(&bloc), 229);

        let blobs = parse_idx2_block(&idx2, &bloc).unwrap();
        assert_eq!(blobs.len(), 229);

        // Check first blob.
        assert_eq!(blobs[0].start_id, 1);
        assert_eq!(blobs[0].id_range, ids_per);
        assert_eq!(blobs[0].pg, 0);
        assert_eq!(blobs[0].size, sizes[0]);

        // Check second blob: pg should be cumulative.
        assert_eq!(blobs[1].start_id, 1 + ids_per as i64);
        assert_eq!(blobs[1].id_range, ids_per);
        assert_eq!(blobs[1].pg, sizes[0] as u64);
        assert_eq!(blobs[1].size, sizes[1]);

        // Check that pages are cumulative.
        let mut expected_pg = 0u64;
        for (i, blob) in blobs.iter().enumerate() {
            assert_eq!(blob.pg, expected_pg, "blob {i} pg mismatch");
            assert_eq!(blob.start_id, 1 + (i as u32 * ids_per) as i64);
            assert_eq!(blob.id_range, ids_per);
            assert_eq!(blob.size, sizes[i]);
            expected_pg += sizes[i] as u64;
        }

        // Last blob.
        let last = &blobs[228];
        assert_eq!(last.start_id, 1 + 228 * 2048);
    }

    #[test]
    fn idx2_random_random() {
        // 3 blobs, id_type=0(random), pg_type=0(random).
        // Layout: [id_d: 3xu64] [pg_d: 3xu64] [id_s: 3xu32] [pg_s: 3xu32]
        let start_ids: Vec<u64> = vec![1, 100, 200];
        let id_ranges: Vec<u32> = vec![10, 20, 30];
        let pgs: Vec<u64> = vec![0, 500, 1500];
        let pg_sizes: Vec<u32> = vec![500, 1000, 800];

        let idx2 = build_idx2_data(
            BlockType::Random,
            BlockType::Random,
            3,
            &[],
            &[],
            &start_ids
                .iter()
                .flat_map(|v| v.to_le_bytes())
                .collect::<Vec<_>>(),
            &pgs.iter().flat_map(|v| v.to_le_bytes()).collect::<Vec<_>>(),
            &id_ranges
                .iter()
                .flat_map(|v| v.to_le_bytes())
                .collect::<Vec<_>>(),
            &pg_sizes
                .iter()
                .flat_map(|v| v.to_le_bytes())
                .collect::<Vec<_>>(),
        );

        // size = (8+4+8+4)*3 = 72
        let bloc = BlockLoc {
            pg: 0,
            size: idx2.len() as u32,
            id_type: BlockType::Random,
            pg_type: BlockType::Random,
            compressed: false,
            id_range: 250,
            start_id: 1,
        };

        assert_eq!(block_entry_count(&bloc), 3);
        let blobs = parse_idx2_block(&idx2, &bloc).unwrap();
        assert_eq!(blobs.len(), 3);

        assert_eq!(blobs[0].start_id, 1);
        assert_eq!(blobs[0].id_range, 10);
        assert_eq!(blobs[0].pg, 0);
        assert_eq!(blobs[0].size, 500);

        assert_eq!(blobs[1].start_id, 100);
        assert_eq!(blobs[1].id_range, 20);
        assert_eq!(blobs[1].pg, 500);
        assert_eq!(blobs[1].size, 1000);

        assert_eq!(blobs[2].start_id, 200);
        assert_eq!(blobs[2].id_range, 30);
        assert_eq!(blobs[2].pg, 1500);
        assert_eq!(blobs[2].size, 800);
    }

    #[test]
    fn idx2_uniform_uniform() {
        // 3 blobs, id_type=1(uniform), pg_type=1(uniform).
        // id: h.span(u32=10) + d[3](u64 start_ids)
        // pg: h.span(u32=256) + d[3](u64 pgs)
        let uniform_range = 10u32;
        let start_ids: Vec<u64> = vec![1, 11, 21];
        let uniform_size = 256u32;
        let pgs: Vec<u64> = vec![0, 256, 512];

        let idx2 = build_idx2_data(
            BlockType::Uniform,
            BlockType::Uniform,
            3,
            &uniform_range.to_le_bytes(), // id header
            &uniform_size.to_le_bytes(),  // pg header
            &start_ids
                .iter()
                .flat_map(|v| v.to_le_bytes())
                .collect::<Vec<_>>(),
            &pgs.iter().flat_map(|v| v.to_le_bytes()).collect::<Vec<_>>(),
            &[],
            &[],
        );

        // size = 4+4 + 3*8 + 3*8 = 56
        let bloc = BlockLoc {
            pg: 0,
            size: idx2.len() as u32,
            id_type: BlockType::Uniform,
            pg_type: BlockType::Uniform,
            compressed: false,
            id_range: 30,
            start_id: 1,
        };

        assert_eq!(block_entry_count(&bloc), 3);
        let blobs = parse_idx2_block(&idx2, &bloc).unwrap();
        assert_eq!(blobs.len(), 3);

        for (i, blob) in blobs.iter().enumerate() {
            assert_eq!(blob.start_id, 1 + (i as i64 * 10));
            assert_eq!(blob.id_range, 10);
            assert_eq!(blob.pg, (i as u64) * 256);
            assert_eq!(blob.size, 256);
        }
    }

    #[test]
    fn idx2_magnitude_magnitude() {
        // 3 blobs, id_type=2(magnitude), pg_type=2(magnitude).
        // id: h.first(u64=1) + s[3](u32 id_ranges)
        // pg: h.first(u64=0) + s[3](u32 pg_sizes)
        let first_id = 1u64;
        let id_ranges: Vec<u32> = vec![5, 10, 15];
        let first_pg = 0u64;
        let pg_sizes: Vec<u32> = vec![100, 200, 300];

        let idx2 = build_idx2_data(
            BlockType::Magnitude,
            BlockType::Magnitude,
            3,
            &first_id.to_le_bytes(), // id header (8 bytes)
            &first_pg.to_le_bytes(), // pg header (8 bytes)
            &[],
            &[],
            &id_ranges
                .iter()
                .flat_map(|v| v.to_le_bytes())
                .collect::<Vec<_>>(),
            &pg_sizes
                .iter()
                .flat_map(|v| v.to_le_bytes())
                .collect::<Vec<_>>(),
        );

        // size = 8+8 + 3*4 + 3*4 = 40
        let bloc = BlockLoc {
            pg: 0,
            size: idx2.len() as u32,
            id_type: BlockType::Magnitude,
            pg_type: BlockType::Magnitude,
            compressed: false,
            id_range: 30,
            start_id: 1,
        };

        assert_eq!(block_entry_count(&bloc), 3);
        let blobs = parse_idx2_block(&idx2, &bloc).unwrap();
        assert_eq!(blobs.len(), 3);

        // IDs cumulative from first_id=1: 1, 1+5=6, 6+10=16.
        assert_eq!(blobs[0].start_id, 1);
        assert_eq!(blobs[0].id_range, 5);
        assert_eq!(blobs[1].start_id, 6);
        assert_eq!(blobs[1].id_range, 10);
        assert_eq!(blobs[2].start_id, 16);
        assert_eq!(blobs[2].id_range, 15);

        // Pages cumulative from first_pg=0: 0, 100, 300.
        assert_eq!(blobs[0].pg, 0);
        assert_eq!(blobs[0].size, 100);
        assert_eq!(blobs[1].pg, 100);
        assert_eq!(blobs[1].size, 200);
        assert_eq!(blobs[2].pg, 300);
        assert_eq!(blobs[2].size, 300);
    }

    #[test]
    fn idx2_predictable_predictable() {
        // Both predictable: idx2 data is just h.pred = { pg: u64, sz: u32 } = 12 bytes.
        // size field = count (not byte size).
        let pred_pg = 1000u64;
        let pred_sz = 50u32;
        let count = 5u32;

        let idx2 = build_idx2_pg_predictable(pred_pg, pred_sz);
        assert_eq!(idx2.len(), 12);

        let bloc = BlockLoc {
            pg: 0,
            size: count, // size = count for pred+pred
            id_type: BlockType::Predictable,
            pg_type: BlockType::Predictable,
            compressed: false,
            id_range: 100,
            start_id: 1,
        };

        assert_eq!(block_entry_count(&bloc), 5);
        let blobs = parse_idx2_block(&idx2, &bloc).unwrap();
        assert_eq!(blobs.len(), 5);

        for (i, blob) in blobs.iter().enumerate() {
            // ids_per = 100 / 5 = 20
            assert_eq!(blob.start_id, 1 + (i as i64 * 20));
            assert_eq!(blob.id_range, 20);
            assert_eq!(blob.pg, 1000 + (i as u64 * 50));
            assert_eq!(blob.size, 50);
        }
    }

    #[test]
    fn idx2_random_predictable() {
        // id_type=0(random), pg_type=3(predictable).
        // id: hsz=0, dsz=8, ssz=4. pg: hsz=12, dsz=0, ssz=0.
        // Layout: [pg_header:12] [id_d: 3xu64] [id_s: 3xu32]
        let start_ids: Vec<u64> = vec![1, 50, 200];
        let id_ranges: Vec<u32> = vec![10, 20, 30];
        let pred_pg = 0u64;
        let pred_sz = 100u32;

        let mut pg_header = Vec::new();
        pg_header.extend_from_slice(&pred_pg.to_le_bytes());
        pg_header.extend_from_slice(&pred_sz.to_le_bytes());

        let idx2 = build_idx2_data(
            BlockType::Random,
            BlockType::Predictable,
            3,
            &[],        // id header (0)
            &pg_header, // pg header (12)
            &start_ids
                .iter()
                .flat_map(|v| v.to_le_bytes())
                .collect::<Vec<_>>(),
            &[], // pg_d (0)
            &id_ranges
                .iter()
                .flat_map(|v| v.to_le_bytes())
                .collect::<Vec<_>>(),
            &[], // pg_s (0)
        );

        // size = 0+12 + 3*8 + 3*4 = 48
        let bloc = BlockLoc {
            pg: 0,
            size: idx2.len() as u32,
            id_type: BlockType::Random,
            pg_type: BlockType::Predictable,
            compressed: false,
            id_range: 260,
            start_id: 1,
        };

        // count = (48 - 0 - 12) / (8 + 4) = 36 / 12 = 3
        assert_eq!(block_entry_count(&bloc), 3);
        let blobs = parse_idx2_block(&idx2, &bloc).unwrap();
        assert_eq!(blobs.len(), 3);

        assert_eq!(blobs[0].start_id, 1);
        assert_eq!(blobs[0].id_range, 10);
        assert_eq!(blobs[0].pg, 0);
        assert_eq!(blobs[0].size, 100);

        assert_eq!(blobs[1].start_id, 50);
        assert_eq!(blobs[1].pg, 100);
        assert_eq!(blobs[1].size, 100);

        assert_eq!(blobs[2].start_id, 200);
        assert_eq!(blobs[2].pg, 200);
        assert_eq!(blobs[2].size, 100);
    }

    // -----------------------------------------------------------------------
    // from_parts with idx2 (v2+ integration)
    // -----------------------------------------------------------------------

    #[test]
    fn from_parts_v2_with_idx2() {
        // Build a v2+ column using idx1/idx/idx2 (no idx0).
        // Block: id_type=3(predictable), pg_type=2(magnitude), 3 blobs.
        let count = 3usize;
        let ids_per = 10u32;
        let id_range = count as u32 * ids_per;
        let first_pg = 0u64;
        let blob_sizes: Vec<u32> = vec![100, 200, 150];

        // Build idx2 data.
        let idx2 = build_idx2_data(
            BlockType::Predictable,
            BlockType::Magnitude,
            count,
            &[],
            &first_pg.to_le_bytes(),
            &[],
            &[],
            &[],
            &blob_sizes
                .iter()
                .flat_map(|v| v.to_le_bytes())
                .collect::<Vec<_>>(),
        );
        // idx2 size = 8 + 3*4 = 20

        // Build idx1: v3 header + 1 block locator.
        let bloc_entry = build_block_loc_entry(
            0,                 // pg in idx2
            idx2.len() as u32, // size of idx2 data
            3,                 // id_type = predictable
            2,                 // pg_type = magnitude
            false,
            id_range,
            1, // start_id
        );
        let idx1 = build_idx1_v2(&[bloc_entry]);

        // Build idx file (v3 format).
        let total_data = blob_sizes.iter().sum::<u32>() as u64;
        let mut idx = vec![0u8; 40];
        LittleEndian::write_u32(&mut idx[0..4], KDB_ENDIAN_MAGIC);
        LittleEndian::write_u32(&mut idx[4..8], 3); // version 3
        LittleEndian::write_u64(&mut idx[8..16], total_data); // data_eof
        LittleEndian::write_u64(&mut idx[16..24], idx2.len() as u64); // idx2_eof
        LittleEndian::write_u32(&mut idx[24..28], 0); // idx0_count
        LittleEndian::write_u32(&mut idx[28..32], 1); // num_blocks
        LittleEndian::write_u32(&mut idx[32..36], 1); // page_size
        idx[36] = 0; // checksum = none

        // Build fake data.
        let data = vec![0xABu8; total_data as usize];

        let reader = ColumnReader::from_parts(&idx1, &[], &idx, &idx2, data).unwrap();

        assert_eq!(reader.blob_count(), 3);
        assert_eq!(reader.row_count(), id_range as u64);
        assert_eq!(reader.first_row_id(), Some(1));

        // Verify individual blobs.
        let b0 = reader.find_blob(1).unwrap();
        assert_eq!(b0.start_id, 1);
        assert_eq!(b0.id_range, 10);
        assert_eq!(b0.pg, 0);
        assert_eq!(b0.size, 100);

        let b1 = reader.find_blob(11).unwrap();
        assert_eq!(b1.start_id, 11);
        assert_eq!(b1.pg, 100);
        assert_eq!(b1.size, 200);

        let b2 = reader.find_blob(21).unwrap();
        assert_eq!(b2.start_id, 21);
        assert_eq!(b2.pg, 300);
        assert_eq!(b2.size, 150);
    }

    #[test]
    fn from_parts_v2_no_idx2_falls_back_to_synthetic() {
        // v2+ without idx2: should create a single synthetic blob.
        let idx1 = build_idx1_v2(&[]);
        let mut idx = vec![0u8; 40];
        LittleEndian::write_u32(&mut idx[0..4], KDB_ENDIAN_MAGIC);
        LittleEndian::write_u32(&mut idx[4..8], 3);
        LittleEndian::write_u64(&mut idx[8..16], 100); // data_eof
        LittleEndian::write_u64(&mut idx[16..24], 0); // idx2_eof
        LittleEndian::write_u32(&mut idx[28..32], 0); // num_blocks
        LittleEndian::write_u32(&mut idx[32..36], 1); // page_size

        let data = vec![0u8; 100];
        let reader = ColumnReader::from_parts(&idx1, &[], &idx, &[], data).unwrap();
        assert_eq!(reader.blob_count(), 1);
        assert_eq!(reader.blobs()[0].size, 100);
    }

    #[test]
    fn from_parts_v1_still_works() {
        // v1 column with idx0 should still work (backward compatibility).
        let idx1 = build_idx1_v1(50, 1, 0);
        let idx0 = build_blob_loc(0, 50, 10, 1);
        let data = vec![0u8; 50];

        let reader = ColumnReader::from_parts(&idx1, &idx0, &[], &[], data).unwrap();
        assert_eq!(reader.blob_count(), 1);
        assert_eq!(reader.blobs()[0].size, 50);
        assert_eq!(reader.blobs()[0].id_range, 10);
    }
}
