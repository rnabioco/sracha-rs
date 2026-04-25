//! Vortex shard reader / query API.
//!
//! v0 stub. Reader implementation iterates after we have a real
//! populated shard to test against — the writer + build path comes
//! first so we can validate the index format on a few-thousand-
//! accession corpus.

use crate::record::AccessionRecord;
use crate::{Error, Result};

pub struct CatalogReader;

impl CatalogReader {
    pub fn open_local(_path: &std::path::Path) -> Result<Self> {
        Err(Error::Reader("not yet implemented".into()))
    }

    pub fn lookup(&self, _accession: &str) -> Result<Option<AccessionRecord>> {
        Err(Error::Reader("not yet implemented".into()))
    }
}
