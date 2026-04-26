//! Pure-Rust walker for NCBI's persistent prefix trie (P_Trie) image
//! used inside `idx/skey` to store Illumina name templates.
//!
//! The on-disk format is documented in `ext/ncbi-vdb/libs/klib/ptrie.c`
//! (see `PTrieMakeInt`, `PTrieInitNode`, the `PTrieEncodeNodeId{0..7}`
//! family) and `ext/ncbi-vdb/libs/klib/pbstree-impl.c`
//! (`PBSTreeImplGetNodeData8/16/32`).
//!
//! The flat offset-indexed string-table fast path in `cursor.rs` covers
//! single-transition tries (`num_trans <= 1`). For real multi-transition
//! tries (`num_trans > 1`), templates are split: branch transitions hold
//! shared prefix bytes and per-branch PBSTrees only hold the suffix
//! unique to that branch. This module reconstructs the full templates
//! by walking the trie top-down and emitting `(node_id, prefix||suffix)`
//! at every leaf.

/// Skey magic at byte 0 — common to all versions we handle.
const SKEY_MAGIC: u32 = 0x05031988;

/// P_Trie image offset for v3/v4 skey headers (40-byte header).
/// v2 archives would use 0x20 (32-byte header) but the existing
/// offset-table fast path is also v3/v4-only, so we keep parity.
const PTRIE_OFFSET_V3V4: usize = 0x28;

/// Bail if a putative `num_trans` exceeds this. Real archives are
/// at most a few thousand transitions.
const MAX_NUM_TRANS: u32 = 1_000_000;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum Stride {
    U8,
    U16,
    U32,
}

impl Stride {
    fn bytes(self) -> usize {
        match self {
            Stride::U8 => 1,
            Stride::U16 => 2,
            Stride::U32 => 4,
        }
    }
}

/// Stride rule shared by `trans_off`, PBSTree `data_idx`, and the
/// dad/child arrays — all keyed off the size of the addressed region.
fn stride_for_data_size(data_size: u64) -> Stride {
    if data_size <= 256 * 4 {
        Stride::U8
    } else if data_size <= 65536 * 4 {
        Stride::U16
    } else {
        Stride::U32
    }
}

/// Stride for dad/child pointers — keyed off `num_trans` directly
/// (not multiplied by 4).
fn stride_for_num_trans(num_trans: u32) -> Stride {
    if num_trans <= 256 {
        Stride::U8
    } else if num_trans <= 65536 {
        Stride::U16
    } else {
        Stride::U32
    }
}

fn align_up(pos: usize, stride: usize) -> usize {
    debug_assert!(stride.is_power_of_two());
    (pos + stride - 1) & !(stride - 1)
}

fn read_u8(buf: &[u8], pos: usize) -> Option<u8> {
    buf.get(pos).copied()
}

fn read_u16(buf: &[u8], pos: usize) -> Option<u16> {
    Some(u16::from_le_bytes(buf.get(pos..pos + 2)?.try_into().ok()?))
}

fn read_u32(buf: &[u8], pos: usize) -> Option<u32> {
    Some(u32::from_le_bytes(buf.get(pos..pos + 4)?.try_into().ok()?))
}

fn read_strided(buf: &[u8], pos: usize, stride: Stride) -> Option<u32> {
    match stride {
        Stride::U8 => read_u8(buf, pos).map(|v| v as u32),
        Stride::U16 => read_u16(buf, pos).map(|v| v as u32),
        Stride::U32 => read_u32(buf, pos),
    }
}

#[derive(Debug)]
struct PTrieHeader {
    num_trans: u32,
    num_nodes: u32,
    data_size: u64,
    id_coding: u8,
    backtrace: bool,
    width: u16,
    /// `rmap[i]` is the codepoint (typically a printable ASCII byte) for
    /// the 1-based index `i + 1`. Index 0 is "unmapped" and never produces
    /// an edge.
    rmap: Vec<u32>,
    /// trans_off[tid - 1] is the byte offset (already multiplied by 4)
    /// into the `data` block where transition `tid`'s P_TTrans node starts.
    trans_off: Vec<u64>,
    /// Absolute offset within the original skey buffer where the P_TTrans
    /// data block starts (right after `trans_off[]`).
    data_base: usize,
}

/// Detect the skey wrapper version and return the offset where the
/// P_Trie image begins. Returns `None` for unknown layouts so the
/// caller can fall through to byte-scan.
fn ptrie_offset(skey: &[u8]) -> Option<usize> {
    if skey.len() < 8 {
        return None;
    }
    let magic = read_u32(skey, 0)?;
    if magic != SKEY_MAGIC {
        return None;
    }
    let version = read_u32(skey, 4)?;
    if version == 3 || version == 4 {
        Some(PTRIE_OFFSET_V3V4)
    } else {
        // v2 archives need a different offset and aren't covered by
        // the existing fast path either; let byte-scan handle them.
        None
    }
}

fn parse_header(skey: &[u8]) -> Option<PTrieHeader> {
    let base = ptrie_offset(skey)?;
    if skey.len() < base + 0x10 {
        return None;
    }
    let num_trans = read_u32(skey, base)?;
    let num_nodes = read_u32(skey, base + 4)?;
    let data_size_lo = read_u32(skey, base + 8)?;
    let keys = read_u8(skey, base + 0x0C)?;
    let ext_data_size = read_u8(skey, base + 0x0D)?;
    let width = read_u16(skey, base + 0x0E)?;

    let _ext_keys = (keys & 1) != 0;
    let backtrace = (keys & 2) != 0;
    let id_coding = (keys >> 2) & 7;
    // EXTENDED_PTRIE: combine ext_data_size byte into data_size unconditionally.
    // Modern archives reserve byte 0x0D for the extension; non-extended writers
    // leave it at zero, so the unconditional combine is a no-op for them.
    let data_size = (data_size_lo as u64) | ((ext_data_size as u64) << 32);

    if num_trans == 0 || num_trans > MAX_NUM_TRANS {
        return None;
    }
    if width == 0 || width as usize > 256 {
        return None;
    }

    // rmap[width] follows the header.
    let rmap_start = base + 0x10;
    let rmap_end = rmap_start + (width as usize) * 4;
    if rmap_end > skey.len() {
        return None;
    }
    let mut rmap = Vec::with_capacity(width as usize);
    for i in 0..width as usize {
        rmap.push(read_u32(skey, rmap_start + i * 4)?);
    }

    // trans_off[] follows rmap. The persisted region is padded to a u32
    // boundary regardless of stride: trans_off_len counts u32 words, and
    // total bytes = trans_off_len * 4 (per ptrie.c:1308-1309 `min_size +=
    // trans_off_len << 2`).
    let off_stride = stride_for_data_size(data_size);
    let trans_off_len = match off_stride {
        Stride::U8 => num_trans.div_ceil(4) as usize,
        Stride::U16 => num_trans.div_ceil(2) as usize,
        Stride::U32 => num_trans as usize,
    };
    let off_bytes = trans_off_len * 4;
    let off_start = rmap_end;
    let off_end = off_start + off_bytes;
    if off_end > skey.len() {
        return None;
    }

    let mut trans_off = Vec::with_capacity(num_trans as usize);
    for i in 0..num_trans as usize {
        let v = read_strided(skey, off_start + i * off_stride.bytes(), off_stride)?;
        // Persisted offsets are stored as units of 4 bytes (entries are
        // 4-byte aligned, see ptrie.c:1287-1291). Multiply once here.
        trans_off.push((v as u64) * 4);
    }

    let data_base = off_end;
    if data_base + data_size as usize > skey.len() {
        return None;
    }

    Some(PTrieHeader {
        num_trans,
        num_nodes,
        data_size,
        id_coding,
        backtrace,
        width,
        rmap,
        trans_off,
        data_base,
    })
}

/// Output of `parse_trans_node`: everything needed to enumerate a
/// transition's edges and its leaf PBSTree.
#[derive(Debug)]
struct TransNode<'a> {
    /// Number of children. Sourced from `pttHdrChildCnt` (idx[3]) — the
    /// authoritative child[] array length the C code uses.
    tcnt: u32,
    /// Length of the `child_seq_type` bitstream in slots.
    slen: u32,
    /// `(slen + 7) >> 3` bytes; bit i selects single-char vs range.
    child_seq_type: &'a [u8],
    /// Slice from `pttFirstIdx` through the end of `child_seq_type` — the
    /// edge-iteration loop reads codepoint codes from here at strided
    /// positions, and for ranges may read PAST the official icnt boundary
    /// into the bitstream bytes (mirrors the C code's overlap).
    idx_buf: &'a [u8],
    /// Stride for `idx_buf` (u8 if width ≤ 256, else u16).
    idx_stride: Stride,
    /// Child transition ids (1-based), `tcnt` entries.
    children: Vec<u32>,
    /// Slice of the trailing PBSTree for this transition's leaves.
    pbstree: &'a [u8],
}

fn parse_trans_node<'a>(
    data: &'a [u8],
    off: u64,
    next_off: u64,
    width: u16,
    num_trans: u32,
    backtrace: bool,
) -> Option<TransNode<'a>> {
    let off = off as usize;
    let end = next_off as usize;
    if off >= data.len() || end > data.len() || end <= off {
        return None;
    }
    let buf = &data[off..end];

    let idx_stride = if width <= 256 {
        Stride::U8
    } else {
        Stride::U16
    };
    let s = idx_stride.bytes();

    // Real archives are written with both `RECORD_HDR_IDX` and
    // `RECORD_HDR_DEPTH` enabled, so the per-node header is **6** idx
    // entries (pttFirstIdx = 6), not the 4 the priv.h enum suggests at
    // first glance. The verified layout (per pbstree-priv.h:189-223 with
    // both #ifs taken):
    //   idx[0] = pttHdrIdx       — codepoint code of the edge from parent
    //                              (0 for root or unmapped)
    //   idx[1] = pttHdrDepth     — depth from root
    //   idx[2] = pttHdrTransCnt  — non-terminal child count
    //   idx[3] = pttHdrIdxCnt    — icnt: post-header idx[] entries
    //   idx[4] = pttHdrSeqLen    — slen: bitstream-slot count
    //                              (= pttHdrNullEnd when icnt == 0;
    //                               idx[4..] are absent in that case)
    //   idx[5] = pttHdrChildCnt  — child[] array length (the LOCAL tcnt
    //                              the C code uses)
    let _trans_idx = read_strided(buf, 0, idx_stride)?;
    let _depth = read_strided(buf, s, idx_stride)?;
    let _trans_cnt = read_strided(buf, 2 * s, idx_stride)?;
    let icnt = read_strided(buf, 3 * s, idx_stride)?;

    let slen: u32;
    let child_cnt: u32;
    let child_seq_type: &[u8];
    let idx_buf: &[u8];
    let mut cursor: usize;

    const PTT_FIRST_IDX: usize = 6;

    if icnt == 0 {
        // Leaf-only transition: only the first 4 header entries are
        // present (up to pttHdrNullEnd). The dad position starts here.
        slen = 0;
        child_cnt = 0;
        child_seq_type = &[];
        idx_buf = &[];
        cursor = 4 * s;
    } else {
        slen = read_strided(buf, 4 * s, idx_stride)?;
        child_cnt = read_strided(buf, 5 * s, idx_stride)?;

        // The post-header idx[] entries and the child_seq_type bitstream
        // are layout-adjacent. The bitstream-iteration loop in
        // ptrie.c:435-487 advances `k` past the official icnt boundary
        // when the current slot is a range, with the upper-bound entry
        // overlapping into the child_seq_type bytes. Hand enumerate_edges
        // a slice covering both regions so it can read past icnt.
        let idx_start = PTT_FIRST_IDX * s;
        let idx_bytes = (icnt as usize) * s;
        if idx_start + idx_bytes > buf.len() {
            return None;
        }
        let bits_bytes = ((slen + 7) >> 3) as usize;
        let bits_start = idx_start + idx_bytes;
        if bits_start + bits_bytes > buf.len() {
            return None;
        }
        child_seq_type = &buf[bits_start..bits_start + bits_bytes];
        idx_buf = &buf[idx_start..bits_start + bits_bytes];
        cursor = bits_start + bits_bytes;
    }

    // Dad pointer (only when backtrace): aligned to its own stride,
    // skipped (we don't need the value for top-down DFS).
    let trans_stride = stride_for_num_trans(num_trans);
    if backtrace {
        cursor = align_up(cursor, trans_stride.bytes());
        if cursor + trans_stride.bytes() > buf.len() {
            return None;
        }
        cursor += trans_stride.bytes();
    }

    // Children: aligned to trans_stride, length child_cnt. Persisted
    // child values are 0-based; the C code (`PTTransGetChild + 1`,
    // ptrie.c:468) converts them to 1-based tids on read.
    let mut children = Vec::with_capacity(child_cnt as usize);
    if child_cnt > 0 {
        cursor = align_up(cursor, trans_stride.bytes());
        let need = (child_cnt as usize) * trans_stride.bytes();
        if cursor + need > buf.len() {
            return None;
        }
        for i in 0..child_cnt as usize {
            let v = read_strided(buf, cursor + i * trans_stride.bytes(), trans_stride)?;
            children.push(v.checked_add(1)?);
        }
        cursor += need;
    }

    // Trailing PBSTree alignment, per ptrie.c:1557-1568. If the post-children
    // position is already u32-aligned, the PBSTree starts immediately. If not,
    // a single `has_vals` boolean byte precedes the alignment padding: when
    // zero, this transition has no leaves; otherwise the PBSTree starts at
    // the next u32 boundary.
    let pbstree: &[u8] = if cursor >= buf.len() {
        &[]
    } else if cursor.is_multiple_of(4) {
        &buf[cursor..]
    } else {
        let has_vals = buf[cursor];
        if has_vals == 0 {
            &[]
        } else {
            cursor = align_up(cursor + 1, 4);
            if cursor >= buf.len() {
                &[]
            } else {
                &buf[cursor..]
            }
        }
    };

    Some(TransNode {
        tcnt: child_cnt,
        slen,
        child_seq_type,
        idx_buf,
        idx_stride,
        children,
        pbstree,
    })
}

/// `(edge_byte, child_tid)` pairs, one per child transition. Mirrors the
/// `PTTransForEach` enumeration in `ext/ncbi-vdb/libs/klib/ptrie.c:435-487`.
/// `k` is the strided index into `idx_buf`; for ranges the upper bound is
/// read at `++k`, which may overlap into the `child_seq_type` bytes
/// (the C code permits this overlap).
fn enumerate_edges(node: &TransNode, rmap: &[u32]) -> Option<Vec<(u8, u32)>> {
    if node.tcnt == 0 {
        return Some(Vec::new());
    }
    let s = node.idx_stride.bytes();
    let read_idx = |k: usize| -> Option<u32> {
        let pos = k * s;
        read_strided(node.idx_buf, pos, node.idx_stride)
    };

    let mut edges = Vec::with_capacity(node.tcnt as usize);
    // `k` is the strided index into idx_buf; corresponds to the C code's
    // `k - pttFirstIdx` (the slice already starts at pttFirstIdx).
    let mut k = 0usize;
    let mut j = 0usize; // child[] cursor

    for i in 0..node.slen as usize {
        let left = read_idx(k)?;
        let mut right = left;
        let bit = (node.child_seq_type[i >> 3] >> (i & 7)) & 1;
        if bit != 0 {
            k += 1;
            right = read_idx(k)?;
        }
        if right < left {
            return None;
        }
        // Codepoint codes are 0-based indexes into rmap (PTrieGetRMap
        // returns rmap[idx] directly; ptrie.c:987-989).
        let mut cp_code = left;
        while cp_code <= right {
            let child = *node.children.get(j)?;
            j += 1;
            let cp = *rmap.get(cp_code as usize)?;
            if cp != 0 && cp <= 0xFF {
                edges.push((cp as u8, child));
            }
            // cp == 0 marks the "unmapped" sentinel; the C code emits '?'
            // when it shows up during key reconstruction. We just skip the
            // edge (the child slot was still consumed above).
            cp_code += 1;
        }
        k += 1;
    }
    Some(edges)
}

/// PBSTree leaf decoder. Returns one suffix per `btid - 1`. NULs are
/// stripped (mirrors `parse_skey_offset_table`'s NUL handling).
fn parse_pbstree(buf: &[u8]) -> Option<Vec<Vec<u8>>> {
    if buf.len() < 8 {
        // Empty PBSTrees show up on transitions whose children all carry
        // their values down further. Treat as zero leaves.
        return Some(Vec::new());
    }
    let num_nodes = read_u32(buf, 0)?;
    let data_size = read_u32(buf, 4)? as u64;
    if num_nodes == 0 {
        return Some(Vec::new());
    }
    let stride = stride_for_data_size(data_size);
    let idx_start = 8;
    let idx_bytes = (num_nodes as usize) * stride.bytes();
    if idx_start + idx_bytes + data_size as usize > buf.len() {
        return None;
    }
    let payload_start = idx_start + idx_bytes;
    let payload_end = payload_start + data_size as usize;
    let payload = &buf[payload_start..payload_end];

    // data_idx entries are RAW byte offsets into the payload — pbstree-impl.c
    // PBSTreeImplGetNodeData8/16/32 use them directly without the *4 scaling
    // that trans_off needs. The data_idx region is also NOT padded to u32:
    // payload starts immediately at `idx_start + num_nodes * stride.bytes()`.
    let mut offsets = Vec::with_capacity(num_nodes as usize + 1);
    for i in 0..num_nodes as usize {
        let v = read_strided(buf, idx_start + i * stride.bytes(), stride)?;
        offsets.push(v as usize);
    }
    offsets.push(data_size as usize);

    let mut out = Vec::with_capacity(num_nodes as usize);
    for i in 0..num_nodes as usize {
        let s = offsets[i];
        let e = offsets[i + 1].min(data_size as usize);
        if e < s || e > payload.len() {
            return None;
        }
        let bytes = &payload[s..e];
        let nul = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
        out.push(bytes[..nul].to_vec());
    }
    Some(out)
}

fn encode_node_id(tid: u32, btid: u32, id_coding: u8) -> Option<u32> {
    if tid == 0 || btid == 0 {
        return None;
    }
    if id_coding > 6 {
        // Variant 7 is binary-search-based; out of scope.
        return None;
    }
    let shift_bits = 8 + 2 * (id_coding as u32);
    let max_btid_minus_1: u32 = (1u32 << shift_bits) - 1;
    if btid - 1 > max_btid_minus_1 {
        return None;
    }
    let max_tid_minus_1 = u32::MAX >> shift_bits;
    if tid - 1 > max_tid_minus_1 {
        return None;
    }
    let raw = ((tid - 1) << shift_bits) | (btid - 1);
    raw.checked_add(1)
}

#[allow(clippy::too_many_arguments)]
fn dfs(
    edges: &[Vec<(u8, u32)>],
    leaves: &[Vec<Vec<u8>>],
    id_coding: u8,
    tid: u32,
    prefix: &mut Vec<u8>,
    out: &mut Vec<(u32, Vec<u8>)>,
    visited: &mut [bool],
) -> Option<()> {
    let i = (tid - 1) as usize;
    if visited.get(i).copied().unwrap_or(true) {
        // Cycle or out-of-range — bail rather than recurse forever.
        return None;
    }
    visited[i] = true;

    for (btid_minus_1, suffix) in leaves[i].iter().enumerate() {
        let btid = (btid_minus_1 as u32) + 1;
        let node_id = encode_node_id(tid, btid, id_coding)?;
        let mut full = Vec::with_capacity(prefix.len() + suffix.len());
        full.extend_from_slice(prefix);
        full.extend_from_slice(suffix);
        out.push((node_id, full));
    }

    for &(byte, child) in &edges[i] {
        if child == 0 || child as usize > edges.len() {
            return None;
        }
        prefix.push(byte);
        dfs(edges, leaves, id_coding, child, prefix, out, visited)?;
        prefix.pop();
    }
    Some(())
}

/// Walk the P_Trie at offset 0x28 of `skey_data` and reconstruct a
/// dense `Vec<Vec<u8>>` indexed by `node_id - 1`. Returns `None` when
/// the layout doesn't validate (caller falls back to byte-scan).
pub(crate) fn parse_ptrie_templates(skey_data: &[u8]) -> Option<Vec<Vec<u8>>> {
    let header = parse_header(skey_data)?;
    if header.num_trans <= 1 {
        // Single-transition PTries are handled by the offset-table fast
        // path; refusing here means the walker can be unit-tested in
        // isolation without surprising flat-layout fixtures.
        return None;
    }
    if header.id_coding > 6 {
        tracing::warn!(
            "skey ptrie: id_coding={} (binary-search variant 7) not supported; \
             falling back to byte-scan",
            header.id_coding
        );
        return None;
    }

    let data = &skey_data[header.data_base..header.data_base + header.data_size as usize];

    // Parse every transition.
    let mut nodes: Vec<TransNode> = Vec::with_capacity(header.num_trans as usize);
    let n = header.num_trans as usize;
    for tid in 1..=n {
        let off = header.trans_off[tid - 1];
        let next_off = if tid < n {
            header.trans_off[tid]
        } else {
            header.data_size
        };
        let node = parse_trans_node(
            data,
            off,
            next_off,
            header.width,
            header.num_trans,
            header.backtrace,
        )?;
        nodes.push(node);
    }

    // Decode edges and leaves for each transition.
    let mut edges: Vec<Vec<(u8, u32)>> = Vec::with_capacity(n);
    let mut leaves: Vec<Vec<Vec<u8>>> = Vec::with_capacity(n);
    for node in &nodes {
        edges.push(enumerate_edges(node, &header.rmap)?);
        leaves.push(parse_pbstree(node.pbstree)?);
    }

    // Top-down DFS from the root (tid=1) to assemble (node_id, template).
    let mut out: Vec<(u32, Vec<u8>)> = Vec::with_capacity(header.num_nodes as usize);
    let mut prefix: Vec<u8> = Vec::with_capacity(64);
    let mut visited = vec![false; n];
    dfs(
        &edges,
        &leaves,
        header.id_coding,
        1,
        &mut prefix,
        &mut out,
        &mut visited,
    )?;

    if out.is_empty() {
        return None;
    }

    // Sanity check encoded node_ids. For id_coding N the encoder packs
    // (tid-1) << shift_bits | (btid-1) + 1, so the natural upper bound is
    // num_trans * (1 << shift_bits). Cap the resulting allocation at 100M
    // slots to prevent runaway memory if a malformed header sneaks past.
    let max_node_id = out.iter().map(|(id, _)| *id).max().unwrap_or(0);
    let shift_bits: u32 = 8 + 2 * (header.id_coding as u32);
    let max_allowed = (header.num_trans as u64).saturating_mul(1u64 << shift_bits);
    const ALLOC_CAP: u64 = 100_000_000;
    if max_node_id == 0 || (max_node_id as u64) > max_allowed || (max_node_id as u64) > ALLOC_CAP {
        return None;
    }

    // Place each template at its node_id - 1 slot.
    let mut templates = vec![Vec::<u8>::new(); max_node_id as usize];
    for (id, t) in out {
        let i = (id - 1) as usize;
        if !templates[i].is_empty() && templates[i] != t {
            // Two leaves disagree on the same node_id — corrupt.
            return None;
        }
        templates[i] = t;
    }

    // Validation: at least 50% of populated entries must contain `$X`.
    // Mirrors the offset-table parser's guard so we fall through to
    // byte-scan rather than emit garbage on a misparse.
    let populated: Vec<&Vec<u8>> = templates.iter().filter(|t| !t.is_empty()).collect();
    if populated.is_empty() {
        return None;
    }
    let with_placeholder = populated
        .iter()
        .filter(|t| t.windows(2).any(|w| w == b"$X"))
        .count();
    if with_placeholder * 2 < populated.len() {
        return None;
    }

    Some(templates)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stride_thresholds() {
        assert_eq!(stride_for_data_size(0), Stride::U8);
        assert_eq!(stride_for_data_size(1024), Stride::U8);
        assert_eq!(stride_for_data_size(1025), Stride::U16);
        assert_eq!(stride_for_data_size(262144), Stride::U16);
        assert_eq!(stride_for_data_size(262145), Stride::U32);

        assert_eq!(stride_for_num_trans(0), Stride::U8);
        assert_eq!(stride_for_num_trans(256), Stride::U8);
        assert_eq!(stride_for_num_trans(257), Stride::U16);
        assert_eq!(stride_for_num_trans(65536), Stride::U16);
        assert_eq!(stride_for_num_trans(65537), Stride::U32);
    }

    #[test]
    fn align_up_basic() {
        assert_eq!(align_up(0, 4), 0);
        assert_eq!(align_up(1, 4), 4);
        assert_eq!(align_up(4, 4), 4);
        assert_eq!(align_up(5, 4), 8);
        assert_eq!(align_up(7, 1), 7);
        assert_eq!(align_up(7, 2), 8);
    }

    #[test]
    fn encode_node_id_drr032228() {
        // Issue #29 reference: ord2node[0] = 12289 with id_coding=1 must
        // round-trip to (tid=13, btid=1).
        assert_eq!(encode_node_id(13, 1, 1), Some(12289));
    }

    #[test]
    fn encode_node_id_id_coding_zero() {
        // 8-bit btid: tid=2, btid=3 → ((2-1) << 8) | (3-1) + 1 = 256 + 2 + 1 = 259
        assert_eq!(encode_node_id(2, 3, 0), Some(259));
    }

    #[test]
    fn encode_node_id_rejects_variant_7() {
        assert_eq!(encode_node_id(1, 1, 7), None);
    }

    #[test]
    fn pbstree_two_leaves() {
        // Hand-built PBSTree with two suffixes. num_nodes=2, data_size=8,
        // then data_idx[2] at u8 stride (data_size <= 1024), then 8 bytes
        // of payload. data_idx entries are RAW byte offsets per
        // pbstree-impl.c PBSTreeImplGetNodeData8.
        let mut blob = Vec::new();
        blob.extend_from_slice(&2u32.to_le_bytes()); // num_nodes
        blob.extend_from_slice(&8u32.to_le_bytes()); // data_size
        blob.push(0); // data_idx[0] = byte offset 0
        blob.push(2); // data_idx[1] = byte offset 2
        blob.extend_from_slice(b"ABCD\0\0\0\0"); // payload, 8 bytes

        let leaves = parse_pbstree(&blob).expect("parse");
        assert_eq!(leaves.len(), 2);
        assert_eq!(leaves[0], b"AB");
        assert_eq!(leaves[1], b"CD");
    }

    #[test]
    fn pbstree_empty() {
        let mut blob = Vec::new();
        blob.extend_from_slice(&0u32.to_le_bytes());
        blob.extend_from_slice(&0u32.to_le_bytes());
        let leaves = parse_pbstree(&blob).expect("parse");
        assert!(leaves.is_empty());
    }

    /// Build a minimal but realistic skey blob exercising the full walker:
    /// root transition with one edge 'A' → leaf transition holding the
    /// embedded key suffix "$X". Final template at the encoded node_id
    /// should be "A$X" (root prefix "" via 'A' edge ‖ leaf suffix "$X").
    ///
    /// Mirrors the on-disk format real archives use: 6-entry header per
    /// transition (RECORD_HDR_IDX + RECORD_HDR_DEPTH enabled in writer),
    /// 4-entry header for `icnt == 0` leaf-only nodes (pttHdrNullEnd=4).
    fn build_minimal_skey() -> Vec<u8> {
        // ---- skey header (40 bytes; magic+version drive ptrie_offset). ----
        let mut blob = vec![0u8; 0x28];
        blob[0..4].copy_from_slice(&SKEY_MAGIC.to_le_bytes());
        blob[4..8].copy_from_slice(&3u32.to_le_bytes());

        // ---- P_Trie header @ 0x28 ----
        // num_trans=2, num_nodes=1, data_size=28, keys=0 (id_coding=0,
        // backtrace=false), ext_data_size=0, width=1, rmap=['A'].
        blob.extend_from_slice(&2u32.to_le_bytes()); // num_trans
        blob.extend_from_slice(&1u32.to_le_bytes()); // num_nodes
        blob.extend_from_slice(&28u32.to_le_bytes()); // data_size
        blob.push(0); // keys
        blob.push(0); // ext_data_size
        blob.extend_from_slice(&1u16.to_le_bytes()); // width
        blob.extend_from_slice(&(b'A' as u32).to_le_bytes()); // rmap[0] = 'A'

        // trans_off (u8 stride; trans_off_len=1 word=4 bytes).
        // trans_off[0] = 0 (root at byte 0 of data)
        // trans_off[1] = 3 (tid=2 at byte 12 of data: 3 * 4)
        blob.push(0);
        blob.push(3);
        blob.push(0);
        blob.push(0);

        let data_start = blob.len();

        // ---- tid=1 (root): non-leaf, 6-entry header, 1 edge 'A' → tid=2 ----
        // idx[0]=0 (pttHdrIdx — root has no parent edge)
        // idx[1]=0 (pttHdrDepth = 0)
        // idx[2]=1 (pttHdrTransCnt = 1)
        // idx[3]=1 (pttHdrIdxCnt = 1 post-header entry)
        // idx[4]=1 (pttHdrSeqLen = 1 bitstream slot)
        // idx[5]=1 (pttHdrChildCnt = 1)
        blob.extend_from_slice(&[0, 0, 1, 1, 1, 1]);
        // idx[6] = 0 (post-header — codepoint code 0 = rmap[0] = 'A')
        blob.push(0);
        // child_seq_type[0] = 0 (bit 0 == 0 → single)
        blob.push(0);
        // No dad (backtrace=false).
        // Children: align to trans_stride=1 (no-op), 1 byte = raw child id 1
        // (PTTransGetChild returns +1 to give tid=2).
        blob.push(1);
        // cursor=9 here, unaligned. tid=1 has no leaves; PTAlign with
        // first_byte=0 writes (3 bytes "00 00 00") to reach byte 12.
        blob.extend_from_slice(&[0, 0, 0]);
        debug_assert_eq!(blob.len() - data_start, 12);

        // ---- tid=2 (leaf-only): icnt=0, 4-entry header, then PBSTree ----
        // idx[0]=0 (pttHdrIdx — code 0 = 'A' from parent)
        // idx[1]=1 (pttHdrDepth = 1)
        // idx[2]=0 (pttHdrTransCnt = 0)
        // idx[3]=0 (pttHdrIdxCnt = 0 → leaf-only)
        blob.extend_from_slice(&[0, 1, 0, 0]);
        // No dad (backtrace=false). No children (tcnt=0).
        // cursor=4, aligned to 4 → PBSTree starts directly here.

        // PBSTree with 1 leaf; data is "$X\0" (NUL-terminated key suffix).
        // num_nodes=1, data_size=3, data_idx[0]=0, payload="$X\0".
        blob.extend_from_slice(&1u32.to_le_bytes());
        blob.extend_from_slice(&3u32.to_le_bytes());
        blob.push(0); // data_idx[0] = byte offset 0 (u8 stride: data_size <= 1024)
        blob.extend_from_slice(b"$X\0"); // payload

        // tid=2 = 4 (header) + 4 + 4 + 1 + 3 = 16 bytes.
        debug_assert_eq!(blob.len() - data_start, 28);

        blob
    }

    #[test]
    fn parse_ptrie_templates_minimal_blob() {
        let skey = build_minimal_skey();

        // node_id for (tid=2, btid=1, id_coding=0) = ((2-1)<<8) | 0 + 1 = 257.
        let templates = parse_ptrie_templates(&skey).expect("walker should succeed");
        assert_eq!(templates.len(), 257);
        assert_eq!(templates[256], b"A$X");
        // All other slots empty.
        assert!(templates[..256].iter().all(|t| t.is_empty()));
    }

    #[test]
    fn parse_header_minimal_blob_extracts_fields() {
        let skey = build_minimal_skey();
        let h = parse_header(&skey).expect("header parse");
        assert_eq!(h.num_trans, 2);
        assert_eq!(h.num_nodes, 1);
        assert_eq!(h.data_size, 28);
        assert_eq!(h.id_coding, 0);
        assert!(!h.backtrace);
        assert_eq!(h.width, 1);
        assert_eq!(h.rmap, vec![b'A' as u32]);
        assert_eq!(h.trans_off, vec![0, 12]); // raw bytes after *4
    }

    /// Snapshot test against a real DRR032228 skey blob — the archive
    /// the issue documents in detail. Verifies the walker reconstructs
    /// the issue-specified template at the issue-specified node_id.
    #[test]
    fn drr032228_fixture_reconstructs_issue_29_template() {
        let skey = include_bytes!("../tests/fixtures/skey_DRR032228.bin");
        let templates = parse_ptrie_templates(skey).expect("walker should succeed");

        // ord2node[0] = 12289 → (tid=13, btid=1, id_coding=1) per the
        // issue. The leaf at that position must reconstruct to the
        // fasterq-dump reference template at spot 1.
        let expected: &[u8] = b"HWI-D00619:29:C53F3ACXX:3:1101:$X:$Y";
        assert!(templates.len() >= 12289);
        assert_eq!(
            templates[12288],
            expected,
            "node_id=12289 should reconstruct as {:?}, got {:?}",
            std::str::from_utf8(expected).unwrap(),
            std::str::from_utf8(&templates[12288]).unwrap_or("<non-utf8>")
        );

        // Spot-check that DFS emitted ~num_nodes = 768 populated entries.
        let populated = templates.iter().filter(|t| !t.is_empty()).count();
        assert_eq!(populated, 768);
    }

    #[test]
    fn ptrie_offset_v3v4_only() {
        let mut blob = vec![0u8; 16];
        blob[0..4].copy_from_slice(&SKEY_MAGIC.to_le_bytes());
        blob[4..8].copy_from_slice(&3u32.to_le_bytes());
        assert_eq!(ptrie_offset(&blob), Some(0x28));
        blob[4..8].copy_from_slice(&4u32.to_le_bytes());
        assert_eq!(ptrie_offset(&blob), Some(0x28));
        blob[4..8].copy_from_slice(&2u32.to_le_bytes());
        assert_eq!(ptrie_offset(&blob), None);
        blob[0..4].copy_from_slice(&0xDEADBEEFu32.to_le_bytes());
        assert_eq!(ptrie_offset(&blob), None);
    }
}
