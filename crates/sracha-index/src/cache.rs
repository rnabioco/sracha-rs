//! User-cache directory resolution for the hosted catalog.
//!
//! Resolution order:
//!   1. `$SRACHA_CATALOG_DIR` if set and non-empty.
//!   2. `$XDG_CACHE_HOME/sracha/catalog` if `XDG_CACHE_HOME` is set.
//!   3. `$HOME/.cache/sracha/catalog`.
//!
//! The directory is *not* created here — callers decide whether the
//! absence is an error (`status`, `path`) or something to repair
//! (`update`).

use std::path::PathBuf;

use crate::reader::Manifest;
use crate::{Error, Result};

/// Default base URL for the hosted catalog. Override with the
/// `SRACHA_INDEX_URL` env var or the `--index-url` flag on
/// `sracha index update`.
pub const DEFAULT_INDEX_URL: &str = "https://sracha-catalog.s3.amazonaws.com/v1";

/// Resolve the cache directory for the catalog without touching disk.
pub fn resolve_cache_dir() -> Result<PathBuf> {
    if let Some(p) = non_empty_env("SRACHA_CATALOG_DIR") {
        return Ok(PathBuf::from(p));
    }
    if let Some(xdg) = non_empty_env("XDG_CACHE_HOME") {
        return Ok(PathBuf::from(xdg).join("sracha").join("catalog"));
    }
    let home = non_empty_env("HOME").ok_or_else(|| {
        Error::Reader("cannot resolve catalog cache dir: $HOME is not set".into())
    })?;
    Ok(PathBuf::from(home)
        .join(".cache")
        .join("sracha")
        .join("catalog"))
}

/// Resolve the index download base URL: `SRACHA_INDEX_URL` overrides
/// the compiled-in default.
pub fn resolve_index_url(override_url: Option<&str>) -> String {
    if let Some(u) = override_url {
        return u.trim_end_matches('/').to_string();
    }
    if let Some(env) = non_empty_env("SRACHA_INDEX_URL") {
        return env.trim_end_matches('/').to_string();
    }
    DEFAULT_INDEX_URL.to_string()
}

/// Read the local manifest if one exists; returns `None` for a fresh
/// cache dir or a missing manifest.
pub fn local_manifest(cache_dir: &std::path::Path) -> Result<Option<Manifest>> {
    let path = cache_dir.join("manifest.json");
    if !path.exists() {
        return Ok(None);
    }
    let bytes = std::fs::read(&path)
        .map_err(|e| Error::Reader(format!("read local manifest {}: {e}", path.display())))?;
    let m: Manifest = serde_json::from_slice(&bytes)?;
    Ok(Some(m))
}

fn non_empty_env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
}
