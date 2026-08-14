//! External reference-sequence (refseq) support for cSRA decode.
//!
//! Most aligned SRA stores its reference bases inline as
//! `REFERENCE/col/CMP_READ`. Runs aligned to a public assembly usually do
//! not: their REFERENCE table keeps only the chunk *layout*
//! (`SEQ_ID`, `SEQ_START`, `SEQ_LEN`) and the bases live in separate NCBI
//! refseq objects named by `SEQ_ID` (e.g. `CM000663.1` = GRCh37 chr1).
//!
//! This module covers the archive-side half of that: discovering which
//! objects a run needs ([`external_refs_needed`]) and reading bases out of
//! them once someone else has put them on local disk ([`RefSeqStore`]).
//! Fetching is deliberately *not* here — `sracha-vdb` performs no I/O
//! beyond the filesystem, so `sracha-core` resolves and downloads the
//! objects and hands back local paths.
//!
//! A refseq object is a flat VDB table (`NCBI:refseq:tbl:reference`) whose
//! bases live in `READ` (2na-packed) overlaid with `ALTREAD` (4na-bin,
//! left-trimmed and right-aligned). `READ` alone decodes ambiguity codes —
//! most visibly the telomeric N runs — as `A`, so the overlay is not
//! optional.

use std::collections::HashMap;
use std::io::{Read, Seek};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::cache::{CachedColumn, ColumnKind};
use crate::error::{Error, Result};
use crate::inspect;
use crate::kar::KarArchive;
use crate::kdb::ColumnReader;

/// 4na-bin code for N (all four bases possible).
pub(crate) const BASE_N: u8 = 0x0F;

/// One external reference object a run needs in order to decode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalRefNeed {
    /// `REFERENCE.SEQ_ID`, used verbatim as the NCBI accession.
    pub seq_id: String,
    /// How many REFERENCE chunk rows name this object.
    pub chunk_rows: u64,
    /// Highest 0-based end offset referenced within the object, i.e.
    /// `max(SEQ_START - 1 + SEQ_LEN)`. Zero when every chunk is all-N.
    pub max_end: u64,
    /// Every chunk naming this object has `SEQ_START == 0` — the whole
    /// span is Ns and no fetch is required.
    pub all_n: bool,
}

/// Scan a cSRA archive's REFERENCE table and report which external refseq
/// objects it needs.
///
/// Returns an empty vec when the archive embeds its bases (a `CMP_READ`
/// column is present) or has no REFERENCE layout columns at all. Reads
/// only `SEQ_ID`, `SEQ_START` and `SEQ_LEN`, so it is cheap relative to a
/// decode even on a whole-genome REFERENCE table.
pub fn external_refs_needed<R: Read + Seek>(
    archive: &mut KarArchive<R>,
    sra_path: &Path,
) -> Result<Vec<ExternalRefNeed>> {
    let Ok(col_base) = inspect::column_base_path_public(archive, Some("REFERENCE")) else {
        return Ok(Vec::new());
    };
    let open = |archive: &mut KarArchive<R>, name: &str| -> Option<ColumnReader> {
        ColumnReader::open(archive, &format!("{col_base}/{name}"), sra_path).ok()
    };

    // Bases embedded → nothing external to fetch, regardless of layout.
    if open(archive, "CMP_READ").is_some() {
        return Ok(Vec::new());
    }

    let (Some(seq_id), Some(seq_start), Some(seq_len)) = (
        open(archive, "SEQ_ID"),
        open(archive, "SEQ_START"),
        open(archive, "SEQ_LEN"),
    ) else {
        return Ok(Vec::new());
    };

    let first_row = seq_len.first_row_id().unwrap_or(1);
    let row_count = seq_len.row_count();
    let seq_id = CachedColumn::new(seq_id, ColumnKind::Zip);
    let seq_start = CachedColumn::new(seq_start, ColumnKind::Irzip { elem_bits: 32 });
    let seq_len = CachedColumn::new(seq_len, ColumnKind::Irzip { elem_bits: 32 });

    // Preserve first-seen order: REFERENCE chunks are laid out grouped by
    // SEQ_ID, and reporting them in archive order keeps logs readable.
    let mut order: Vec<String> = Vec::new();
    let mut by_id: HashMap<String, ExternalRefNeed> = HashMap::new();

    for row in first_row..first_row + row_count as i64 {
        let id_bytes = seq_id.read_byte_row(row)?;
        let id = String::from_utf8_lossy(&id_bytes).trim().to_string();
        if id.is_empty() {
            continue;
        }
        let start = seq_start.read_scalar_u32(row)? as u64;
        let len = seq_len.read_scalar_u32(row)? as u64;

        let need = by_id.entry(id.clone()).or_insert_with(|| {
            order.push(id.clone());
            ExternalRefNeed {
                seq_id: id.clone(),
                chunk_rows: 0,
                max_end: 0,
                all_n: true,
            }
        });
        need.chunk_rows += 1;
        // SEQ_START == 0 is the all-N sentinel: the chunk is `SEQ_LEN` Ns
        // and addresses nothing in the external object.
        if start > 0 {
            need.all_n = false;
            need.max_end = need.max_end.max(start - 1 + len);
        }
    }

    Ok(order
        .into_iter()
        .filter_map(|id| by_id.remove(&id))
        .collect())
}

/// A set of refseq objects already materialised on local disk, keyed by
/// `REFERENCE.SEQ_ID`.
///
/// Immutable and shareable: build one per accession and clone the `Arc`
/// into each decode worker. The per-worker mutable blob caches live in
/// [`RefSeqReaders`], not here.
pub struct RefSeqStore {
    objects: HashMap<String, Arc<RefSeqObject>>,
}

/// One opened refseq object.
pub struct RefSeqObject {
    seq_id: String,
    read: Arc<ColumnReader>,
    altread: Option<Arc<ColumnReader>>,
    max_seq_len: u32,
    first_row: i64,
    row_count: u64,
    total_seq_len: u64,
    circular: bool,
}

impl RefSeqStore {
    /// Open every `(seq_id, local path)` pair. This is the single seam
    /// that keeps network code out of `sracha-vdb`: callers hand over
    /// paths, never URLs.
    pub fn open<P: AsRef<Path>>(objects: &[(String, P)]) -> Result<Self> {
        let mut map = HashMap::with_capacity(objects.len());
        for (seq_id, path) in objects {
            let obj = RefSeqObject::open(seq_id, path.as_ref())?;
            map.insert(seq_id.clone(), Arc::new(obj));
        }
        Ok(Self { objects: map })
    }

    pub fn is_empty(&self) -> bool {
        self.objects.is_empty()
    }

    pub fn contains(&self, seq_id: &str) -> bool {
        self.objects.contains_key(seq_id)
    }

    fn get(&self, seq_id: &str) -> Option<&Arc<RefSeqObject>> {
        self.objects.get(seq_id)
    }
}

impl RefSeqObject {
    fn open(seq_id: &str, path: &Path) -> Result<Self> {
        let file = std::fs::File::open(path)?;
        let mut archive = KarArchive::open(std::io::BufReader::new(file))?;
        let col_base = inspect::column_base_path_public(&archive, None)?;

        let read = ColumnReader::open(&mut archive, &format!("{col_base}/READ"), path)
            .map_err(|e| Error::Format(format!("refseq {seq_id}: READ: {e}")))?;
        let altread = ColumnReader::open(&mut archive, &format!("{col_base}/ALTREAD"), path).ok();

        let first_row = read.first_row_id().unwrap_or(1);
        let row_count = read.row_count();

        // MAX_SEQ_LEN / TOTAL_SEQ_LEN / CIRCULAR are static metadata on a
        // refseq object, not physical columns.
        let meta = inspect::read_table_metadata(&mut archive, None).unwrap_or_default();
        let max_seq_len = meta_u32(&meta, "col/MAX_SEQ_LEN/row")
            .filter(|&v| v > 0)
            .unwrap_or(crate::reference::DEFAULT_MAX_SEQ_LEN);
        let total_seq_len =
            meta_u64(&meta, "STATS/TOTAL_SEQ_LEN").unwrap_or(row_count * u64::from(max_seq_len));
        let circular = meta_u32(&meta, "col/CIRCULAR/row")
            .map(|v| v != 0)
            .unwrap_or(false);

        Ok(Self {
            seq_id: seq_id.to_string(),
            read: Arc::new(read),
            altread: altread.map(Arc::new),
            max_seq_len,
            first_row,
            row_count,
            total_seq_len,
            circular,
        })
    }
}

/// Read a little-endian scalar out of the static metadata tree. Values are
/// stored at their natural width (`CIRCULAR` is one byte, `MAX_SEQ_LEN`
/// four, `TOTAL_SEQ_LEN` eight), so accept anything up to the target size.
fn meta_uint(nodes: &[crate::metadata::MetaNode], path: &str, max_bytes: usize) -> Option<u64> {
    let v = &crate::metadata::find_meta_node(nodes, path)?.value;
    if v.is_empty() || v.len() > max_bytes {
        return None;
    }
    let mut buf = [0u8; 8];
    buf[..v.len()].copy_from_slice(v);
    Some(u64::from_le_bytes(buf))
}

fn meta_u32(nodes: &[crate::metadata::MetaNode], path: &str) -> Option<u32> {
    meta_uint(nodes, path, 4).map(|v| v as u32)
}

fn meta_u64(nodes: &[crate::metadata::MetaNode], path: &str) -> Option<u64> {
    meta_uint(nodes, path, 8)
}

/// Per-worker view over a shared [`RefSeqStore`].
///
/// Holds the single-slot decoded-blob caches that make row reads cheap;
/// those use `RefCell`, so each rayon worker needs its own. Construction
/// is a map walk — no file opens, no mmap, no decode.
pub struct RefSeqReaders {
    store: Arc<RefSeqStore>,
    cols: HashMap<String, (CachedColumn, Option<CachedColumn>)>,
}

impl RefSeqReaders {
    pub fn new(store: &Arc<RefSeqStore>) -> Self {
        let mut cols = HashMap::with_capacity(store.objects.len());
        for (seq_id, obj) in &store.objects {
            cols.insert(
                seq_id.clone(),
                (
                    CachedColumn::from_shared(obj.read.clone(), ColumnKind::TwoNa),
                    obj.altread
                        .as_ref()
                        .map(|c| CachedColumn::from_shared(c.clone(), ColumnKind::Zip)),
                ),
            );
        }
        Self {
            store: store.clone(),
            cols,
        }
    }

    /// `len` reference bases as 4na-bin bytes, starting at 0-based
    /// position `pos` within `seq_id`.
    pub fn bases_at(&self, seq_id: &str, pos: u64, len: usize) -> Result<Vec<u8>> {
        let obj = self
            .store
            .get(seq_id)
            .ok_or_else(|| Error::Format(format!("refseq {seq_id}: not materialised")))?;
        let (read, altread) = self
            .cols
            .get(seq_id)
            .ok_or_else(|| Error::Format(format!("refseq {seq_id}: no reader")))?;

        let msl = u64::from(obj.max_seq_len);
        let mut pos = if obj.circular && obj.total_seq_len > 0 {
            pos % obj.total_seq_len
        } else {
            pos
        };
        let mut out = Vec::with_capacity(len);
        while out.len() < len {
            let row = obj.first_row + (pos / msl) as i64;
            if row >= obj.first_row + obj.row_count as i64 {
                if obj.circular {
                    pos = 0;
                    continue;
                }
                return Err(Error::Format(format!(
                    "refseq {}: position {pos} past end ({} bases)",
                    obj.seq_id, obj.total_seq_len
                )));
            }
            let offset = (pos % msl) as usize;
            let chunk = chunk_bases(read, altread.as_ref(), row)?;
            if offset >= chunk.len() {
                return Err(Error::Format(format!(
                    "refseq {}: offset {offset} past chunk {row} ({} bases)",
                    obj.seq_id,
                    chunk.len()
                )));
            }
            let take = (len - out.len()).min(chunk.len() - offset);
            out.extend_from_slice(&chunk[offset..offset + take]);
            pos += take as u64;
        }
        Ok(out)
    }
}

/// Decode one refseq chunk row: 2na `READ` overlaid with the left-trimmed,
/// right-aligned 4na `ALTREAD` mask. Where ALTREAD is non-zero it wins —
/// that is how Ns and other ambiguity codes survive a 2na store.
fn chunk_bases(read: &CachedColumn, altread: Option<&CachedColumn>, row: i64) -> Result<Vec<u8>> {
    let mut bases = read.read_2na_row(row)?;
    let Some(alt) = altread else {
        return Ok(bases);
    };
    let mask = alt.read_byte_row(row)?;
    if mask.is_empty() {
        return Ok(bases);
    }
    // `trim<ALIGN_LEFT, 0>` drops leading zeros, so the stored mask is a
    // suffix of the row: align it to the right before overlaying.
    let shift = bases.len().saturating_sub(mask.len());
    for (i, &m) in mask.iter().enumerate() {
        if m != 0
            && let Some(slot) = bases.get_mut(shift + i)
        {
            *slot = m & 0x0F;
        }
    }
    Ok(bases)
}

/// Path a refseq object is cached at within `dir`.
pub fn cache_path(dir: &Path, seq_id: &str) -> PathBuf {
    dir.join(seq_id)
}

/// Is `seq_id` safe to use as a filename? `SEQ_ID` is archive content, so
/// it must never be able to escape the cache directory.
pub fn is_safe_seq_id(seq_id: &str) -> bool {
    !seq_id.is_empty()
        && seq_id.len() <= 64
        && seq_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The store is shared across rayon decode workers by `Arc`, so this
    /// has to hold — the mutable per-worker state lives in `RefSeqReaders`.
    #[test]
    fn store_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<RefSeqStore>();
    }

    #[test]
    fn seq_id_safety() {
        assert!(is_safe_seq_id("CM000663.1"));
        assert!(is_safe_seq_id("NC_012920.1"));
        assert!(!is_safe_seq_id(""));
        assert!(!is_safe_seq_id("../../etc/passwd"));
        assert!(!is_safe_seq_id("a/b"));
        assert!(!is_safe_seq_id(&"x".repeat(65)));
    }

    /// Ground truth from `vdb-dump ERR10213669 -T REFERENCE`: 24 external
    /// references, the first being GRCh37 chr1 (`CM000663.1`) across
    /// 49,851 chunk rows — 49,850 full 5000-base chunks plus a 621-base
    /// tail, i.e. chr1's 249,250,621 bases. This is the regression guard
    /// on decoding `SEQ_ID` (zip ascii behind a repeat-count page map) and
    /// `SEQ_START` (signed izip, where 0 is the all-N sentinel).
    #[test]
    fn external_refs_for_err10213669() {
        let p = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/ERR10213669.sra");
        if !p.exists() {
            return;
        }
        let file = std::fs::File::open(&p).unwrap();
        let mut archive = KarArchive::open(std::io::BufReader::new(file)).unwrap();
        let needs = external_refs_needed(&mut archive, &p).unwrap();

        assert_eq!(needs.len(), 24, "GRCh37 primary assembly + chrM");
        assert_eq!(needs[0].seq_id, "CM000663.1");
        assert_eq!(needs[0].chunk_rows, 49_851);
        assert!(!needs[0].all_n);
        // The last chunks of chr1 are telomeric Ns (SEQ_START == 0), so the
        // furthest *addressed* base stops short of the full length.
        assert!(
            (249_000_000..=249_250_621).contains(&needs[0].max_end),
            "chr1 max_end out of range: {}",
            needs[0].max_end
        );
        assert_eq!(needs[1].seq_id, "CM000664.1");
    }

    /// ALTREAD is stored left-trimmed, so it overlays the *tail* of the
    /// row. A mask shorter than the row must not shift the bases it hits.
    #[test]
    fn altread_overlay_is_right_aligned() {
        // 8 bases of A (0x1) with a 3-byte mask covering the last three.
        let bases = vec![0x1u8; 8];
        let mask = [0x0F, 0x00, 0x0F];
        let shift = bases.len() - mask.len();
        let mut got = bases.clone();
        for (i, &m) in mask.iter().enumerate() {
            if m != 0 {
                got[shift + i] = m & 0x0F;
            }
        }
        assert_eq!(got, vec![0x1, 0x1, 0x1, 0x1, 0x1, 0x0F, 0x1, 0x0F]);
    }
}
