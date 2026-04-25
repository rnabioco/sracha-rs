//! Vortex shard writer.
//!
//! Takes a stream of [`AccessionRecord`]s, deduplicates schemas and
//! name-fmts on the fly, and writes a single Vortex shard file
//! containing all four tables (`accessions`, `blobs`, `schemas`,
//! `name_fmts`).

use crate::record::AccessionRecord;
use crate::{Error, Result};

pub struct ShardWriter;

impl ShardWriter {
    pub fn create(_path: &std::path::Path) -> Result<Self> {
        Err(Error::Writer("not yet implemented".into()))
    }

    pub fn append(&mut self, _record: AccessionRecord) -> Result<()> {
        Err(Error::Writer("not yet implemented".into()))
    }

    pub fn finish(self) -> Result<()> {
        Err(Error::Writer("not yet implemented".into()))
    }
}
