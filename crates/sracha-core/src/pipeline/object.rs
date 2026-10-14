//! Download of one resolved SRA object: the archive and, when the run has
//! one, its `.vdbcache` companion. Shared by `sracha get` and `sracha fetch`.

use std::path::{Path, PathBuf};

use crate::download::{DownloadConfig, DownloadResult, download_file};
use crate::error::{Error, Result};
use crate::sdl::{ResolvedAccession, ResolvedFile};

/// KAR magic ("NCBI" + ".sra"). Guards the cached-file skip path when SDL
/// supplied no MD5, so a corrupt file from a crashed prior run can't feed
/// garbage to the VDB decoder.
const KAR_MAGIC: &[u8] = b"NCBI.sra";

/// One transferred file.
#[derive(Debug, Clone)]
pub struct ObjectTransfer {
    pub path: PathBuf,
    pub size: u64,
    /// MD5 computed or verified during the download.
    pub md5: Option<String>,
    /// Bytes moved over the network this session; 0 on a cache hit.
    pub bytes_transferred: u64,
    /// Service label of the mirror used (e.g. "s3").
    pub service: String,
}

/// Archive plus optional vdbcache, as downloaded.
#[derive(Debug, Clone)]
pub struct DownloadOutcome {
    pub archive: ObjectTransfer,
    /// Present when the resolved run has a `.vdbcache`; saved next to the
    /// archive as `<archive>.vdbcache`, the layout the cSRA decoder probes.
    pub vdbcache: Option<ObjectTransfer>,
}

/// Where the vdbcache for `archive_path` lives.
pub fn vdbcache_path_for(archive_path: &Path) -> PathBuf {
    crate::vdb::csra::vdbcache_sidecar_path(archive_path)
}

async fn download_one(
    accession: &str,
    file: &ResolvedFile,
    what: &str,
    dest: &Path,
    cfg: &DownloadConfig,
) -> Result<ObjectTransfer> {
    let mirror = file.preferred_mirror().ok_or_else(|| Error::Download {
        accession: accession.to_string(),
        message: format!("no {what} download mirrors available"),
    })?;
    tracing::info!(
        "{accession}: downloading {what} {} from [{}] to {}",
        crate::util::format_size(file.size),
        mirror.service,
        dest.display(),
    );
    let cfg = DownloadConfig {
        expected_prefix: Some(KAR_MAGIC.to_vec()),
        ..cfg.clone()
    };
    let DownloadResult {
        path,
        size,
        md5,
        bytes_transferred,
    } = download_file(
        std::slice::from_ref(&mirror.url),
        file.size,
        file.md5.as_deref(),
        dest,
        &cfg,
    )
    .await?;
    Ok(ObjectTransfer {
        path,
        size,
        md5,
        bytes_transferred,
        service: mirror.service.clone(),
    })
}

/// Download the archive of `resolved` to `archive_path`, plus its vdbcache
/// (if any) to `<archive_path>.vdbcache`. Mirror choice is
/// [`ResolvedFile::preferred_mirror`]; `cfg.expected_prefix` is overridden
/// with the KAR magic for both files.
pub async fn download_sra_object(
    resolved: &ResolvedAccession,
    archive_path: &Path,
    cfg: &DownloadConfig,
) -> Result<DownloadOutcome> {
    let acc = &resolved.accession;
    let archive = download_one(acc, &resolved.sra_file, "archive", archive_path, cfg).await?;
    let vdbcache = match &resolved.vdbcache_file {
        Some(vdb) => {
            Some(download_one(acc, vdb, "vdbcache", &vdbcache_path_for(archive_path), cfg).await?)
        }
        None => None,
    };
    Ok(DownloadOutcome { archive, vdbcache })
}
