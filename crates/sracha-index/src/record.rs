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
/// Compact form: `blob_idx` and `pg` are NOT stored — both are
/// derivable. `blob_idx` = row-position within the table for a given
/// (accession, column) run. `pg` = `blob_offset / page_size` (page
/// size lives in the schemas table). Reader recomputes both on
/// load.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobLocator {
    /// Index into the schema's column list (compact int, dictionary-encoded).
    pub column_id: u8,
    /// First row id in this blob.
    pub start_id: i64,
    /// Number of rows. Typically 8192 or similar small set of values
    /// — dictionary-encodes well.
    pub id_range: u32,
    /// Absolute byte offset in the .sra file. Monotonic per
    /// (accession, column) so delta-encodes to small values.
    pub blob_offset: u64,
    /// Compressed size in bytes.
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
