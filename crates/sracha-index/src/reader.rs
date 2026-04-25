//! Vortex shard reader / query API.
//!
//! Opens a shard (or set of shards via a manifest) and supports
//! point-lookup by accession id plus simple range filters. Designed
//! for HTTP Range queries against a remote-hosted Vortex file.

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
