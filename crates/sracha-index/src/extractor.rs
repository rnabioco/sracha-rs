//! Async metadata extractor: given an accession, fetch JUST the
//! KAR header + TOC + idx files from S3 and emit an [`AccessionRecord`].
//! Never downloads the full .sra.

use crate::record::AccessionRecord;
use crate::{Error, Result};

/// Extract metadata for one accession. Network-only; uses HTTP Range
/// to fetch only the bytes needed (typically <500 KB total).
pub async fn extract(_accession: &str) -> Result<AccessionRecord> {
    Err(Error::Extractor("not yet implemented".into()))
}
