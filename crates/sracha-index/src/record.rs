//! Per-accession record produced by the extractor.
//!
//! Captures everything `sracha get` and `sracha info` need to know
//! about an accession WITHOUT reading the full .sra file. Sources:
//! - HEAD response (file size, MD5 via S3 ETag if multipart).
//! - KAR header (24 bytes, gives data section offset).
//! - KAR TOC (tens of KB, lists all archive entries with offsets/sizes).
//! - idx files in the archive (small, contain blob locator tables).
//! - Selected metadata files for SRA-lite / cSRA / platform detection.

use serde::{Deserialize, Serialize};

/// Per-accession record. One per SRA accession in the catalog.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessionRecord {
    /// Canonical accession id, e.g. "SRR2584863".
    pub accession: String,

    /// File size in bytes (HEAD Content-Length).
    pub file_size: u64,

    /// MD5 of the .sra file. May be `None` if SDL didn't supply it
    /// (S3 multipart ETags are not bare MD5 — fall back to per-blob
    /// hashing for verification when this is absent).
    pub md5: Option<[u8; 16]>,

    /// Total spot count from RunInfo.
    pub spots: Option<u64>,

    /// Read layout: SINGLE / PAIRED.
    pub layout: Layout,

    /// Sequencing platform.
    pub platform: Platform,

    /// Per-read length pattern, e.g. `[150, 150]` for paired 2x150.
    pub read_lengths: Vec<u32>,

    /// VDB schema fingerprint (sha256 over the column-codec layout).
    /// Used to dedup the `schemas` table at write time.
    pub schema_fingerprint: [u8; 32],

    /// Where the data section starts in the .sra file (KAR header
    /// `file_offset`). Streaming consumers need this to plan column
    /// data byte-range fetches without re-parsing the header.
    pub kar_data_offset: u64,

    /// Column blob locators. Sorted by `(column_id, blob_idx)`.
    /// Populated for the SEQUENCE table only (the only table sracha
    /// emits FASTQ from).
    pub blobs: Vec<BlobLocator>,

    /// Schema entry. May be replaced with a `schema_id` foreign key
    /// at write time once the writer's dedup table is consulted.
    pub schema: SchemaEntry,

    /// Optional Illumina name-format templates (skey table). Many
    /// accessions share these; the writer dedups via `name_fmt_id`.
    pub name_fmt: Option<NameFmtEntry>,

    /// Wall-clock seconds the extractor spent on this accession.
    /// Diagnostic only; not persisted to the index.
    #[serde(skip)]
    pub extract_secs: f32,

    /// Bytes the extractor pulled from S3 for this accession.
    /// Diagnostic only; not persisted.
    #[serde(skip)]
    pub bytes_fetched: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Layout {
    Single,
    Paired,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Platform {
    Illumina,
    PacBio,
    OxfordNanopore,
    IonTorrent,
    Other,
}

/// One row of the `blobs` table — locates a single blob within its
/// column's data slab.
///
/// Compact form: `blob_idx` and `pg` are derivable and not stored.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobLocator {
    /// Index into the schema's column list.
    pub column_id: u8,
    /// First row id in this blob.
    pub start_id: i64,
    /// Number of rows. Typically 8192 or similar small set of values.
    pub id_range: u32,
    /// Absolute byte offset in the .sra file.
    pub blob_offset: u64,
    /// Compressed size in bytes. Used by extractor + writer; the
    /// index does NOT persist this per-blob — sracha refetches the
    /// (tiny) idx files for exact boundaries when decoding. For
    /// streaming-download planning the writer derives a per-(acc,
    /// col) approximation.
    pub blob_size: u32,
}

/// Schema fingerprint payload. Distinct schemas are deduped via
/// `schema_fingerprint`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaEntry {
    pub fingerprint: [u8; 32],
    pub columns: Vec<ColumnMetaEntry>,
    pub is_csra: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMetaEntry {
    pub name: String,
    /// VDB schema version (1 / 2+).
    pub version: u32,
    /// Codec / checksum_type id (see `sracha-vdb::kdb::ColumnMeta`).
    pub codec: u32,
    /// Page size in bytes; 0/1 means byte-addressed (no paging).
    pub page_size: u32,
}

/// Illumina skey templates. Stored in a separate, narrow table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NameFmtEntry {
    pub fingerprint: [u8; 32],
    pub templates: Vec<Vec<u8>>,
    pub spot_starts: Vec<i64>,
}

impl AccessionRecord {
    /// Per-column first-`blobs_per_col` `(blob_offset, blob_size)` pairs
    /// for download priority hinting. Groups `blobs` by `column_id`
    /// first, since the on-disk catalog is sorted by `(column_id,
    /// blob_idx)` but a defensive group-by makes the helper robust to
    /// future reorderings.
    ///
    /// Used by `sracha get --catalog --stream` to seed the chunk
    /// dispatch queue before download starts, instead of waiting for
    /// the KAR header to land and be parsed.
    pub fn priority_byte_ranges(&self, blobs_per_col: usize) -> Vec<(u64, u64)> {
        use std::collections::BTreeMap;
        let mut by_col: BTreeMap<u8, Vec<&BlobLocator>> = BTreeMap::new();
        for b in &self.blobs {
            by_col.entry(b.column_id).or_default().push(b);
        }
        let mut out = Vec::with_capacity(by_col.len() * blobs_per_col);
        for blobs in by_col.values() {
            let take = blobs_per_col.min(blobs.len());
            for b in &blobs[..take] {
                if b.blob_size == 0 {
                    continue;
                }
                out.push((b.blob_offset, u64::from(b.blob_size)));
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(blobs: Vec<BlobLocator>) -> AccessionRecord {
        AccessionRecord {
            accession: "X".into(),
            file_size: 0,
            md5: None,
            spots: None,
            layout: Layout::Unknown,
            platform: Platform::Other,
            read_lengths: vec![],
            schema_fingerprint: [0; 32],
            kar_data_offset: 0,
            blobs,
            schema: SchemaEntry {
                fingerprint: [0; 32],
                columns: vec![],
                is_csra: false,
            },
            name_fmt: None,
            extract_secs: 0.0,
            bytes_fetched: 0,
        }
    }

    fn b(column_id: u8, offset: u64, size: u32) -> BlobLocator {
        BlobLocator {
            column_id,
            start_id: 0,
            id_range: 0,
            blob_offset: offset,
            blob_size: size,
        }
    }

    #[test]
    fn priority_byte_ranges_groups_by_column_and_caps() {
        let r = rec(vec![
            b(0, 100, 10),
            b(0, 200, 10),
            b(0, 300, 10),
            b(1, 1000, 20),
            b(1, 2000, 20),
        ]);
        let mut got = r.priority_byte_ranges(2);
        got.sort();
        assert_eq!(got, vec![(100, 10), (200, 10), (1000, 20), (2000, 20)]);
    }

    #[test]
    fn priority_byte_ranges_skips_zero_size_blobs() {
        let r = rec(vec![b(0, 100, 0), b(0, 200, 10)]);
        assert_eq!(r.priority_byte_ranges(2), vec![(200, 10)]);
    }
}
