//! Direct S3 resolution for the NCBI SRA Open Data Program bucket.
//!
//! SRA run data is publicly available on S3 at a predictable path:
//! `https://sra-pub-run-odp.s3.amazonaws.com/sra/{accession}/{accession}`
//!
//! This module constructs URLs directly from accessions and probes them
//! with HEAD requests, skipping the SDL API round-trip for the common case.
//! Falls back to SDL when the direct URL doesn't exist (old/non-public data).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::sync::Semaphore;

use crate::sdl::{ResolvedAccession, ResolvedFile, ResolvedMirror};

const ODP_BASE: &str = "https://sra-pub-run-odp.s3.amazonaws.com/sra";

/// Maximum concurrent HEAD probes.
const PROBE_CONCURRENCY: usize = 16;

/// Timeout for a single HEAD probe.
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

fn sra_url(accession: &str) -> String {
    format!("{ODP_BASE}/{accession}/{accession}")
}

fn vdbcache_url(accession: &str) -> String {
    format!("{ODP_BASE}/{accession}/{accession}.vdbcache")
}

/// Extract an MD5 hex digest from an S3 ETag header value.
///
/// S3 ETags for non-multipart objects are the MD5 in double quotes:
/// `"d41d8cd98f00b204e9800998ecf8427e"`. Multipart uploads produce
/// ETags like `"abc123...-3"` which are not MD5s.
fn extract_md5_from_etag(etag: &str) -> Option<String> {
    let stripped = etag.trim_matches('"');
    if stripped.len() == 32 && stripped.bytes().all(|b| b.is_ascii_hexdigit()) {
        Some(stripped.to_lowercase())
    } else {
        None
    }
}

/// Probe an S3 URL with a HEAD request.
///
/// Returns `(content_length, optional_md5)` on success.
async fn probe(client: &reqwest::Client, url: &str) -> Result<(u64, Option<String>)> {
    let resp = client
        .head(url)
        .timeout(PROBE_TIMEOUT)
        .send()
        .await
        .with_context(|| format!("HEAD {url}"))?;

    if !resp.status().is_success() {
        anyhow::bail!("HEAD {url} returned {}", resp.status());
    }

    let size = resp
        .headers()
        .get(reqwest::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .with_context(|| format!("missing Content-Length for {url}"))?;

    let md5 = resp
        .headers()
        .get(reqwest::header::ETAG)
        .and_then(|v| v.to_str().ok())
        .and_then(extract_md5_from_etag);

    Ok((size, md5))
}

/// Resolve a single accession via direct S3 HEAD probe.
///
/// On success, returns a `ResolvedAccession` with a single S3 mirror.
/// `run_info` is always `None` — the caller should fetch it separately
/// via `SdlClient::fetch_run_info_batch` if needed.
pub async fn resolve_direct(
    client: &reqwest::Client,
    accession: &str,
) -> Result<ResolvedAccession> {
    let url = sra_url(accession);
    let (size, md5) = probe(client, &url).await?;

    // Optionally probe vdbcache (non-fatal).
    let vdbcache_file = match probe(client, &vdbcache_url(accession)).await {
        Ok((vdb_size, vdb_md5)) => Some(ResolvedFile {
            mirrors: vec![ResolvedMirror {
                url: vdbcache_url(accession),
                service: "s3-direct".into(),
            }],
            size: vdb_size,
            md5: vdb_md5,
            is_lite: false,
        }),
        Err(_) => None,
    };

    Ok(ResolvedAccession {
        accession: accession.to_string(),
        sra_file: ResolvedFile {
            mirrors: vec![ResolvedMirror {
                url,
                service: "s3-direct".into(),
            }],
            size,
            md5,
            is_lite: false,
        },
        vdbcache_file,
        run_info: None,
    })
}

/// Resolve multiple accessions concurrently via direct S3 HEAD probes.
///
/// Returns a map from accession to result. Failed probes appear as `Err`.
pub async fn resolve_direct_many(
    client: &reqwest::Client,
    accessions: &[String],
) -> HashMap<String, Result<ResolvedAccession>> {
    let semaphore = Arc::new(Semaphore::new(PROBE_CONCURRENCY));
    let mut handles = Vec::with_capacity(accessions.len());

    for acc in accessions {
        let client = client.clone();
        let acc = acc.clone();
        let sem = semaphore.clone();
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            let result = resolve_direct(&client, &acc).await;
            (acc, result)
        }));
    }

    let mut results = HashMap::with_capacity(accessions.len());
    for handle in handles {
        let (acc, result) = handle.await.unwrap();
        results.insert(acc, result);
    }
    results
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sra_url() {
        assert_eq!(
            sra_url("SRR000001"),
            "https://sra-pub-run-odp.s3.amazonaws.com/sra/SRR000001/SRR000001"
        );
    }

    #[test]
    fn test_vdbcache_url() {
        assert_eq!(
            vdbcache_url("SRR000001"),
            "https://sra-pub-run-odp.s3.amazonaws.com/sra/SRR000001/SRR000001.vdbcache"
        );
    }

    #[test]
    fn test_extract_md5_plain() {
        let etag = "\"d41d8cd98f00b204e9800998ecf8427e\"";
        assert_eq!(
            extract_md5_from_etag(etag),
            Some("d41d8cd98f00b204e9800998ecf8427e".into())
        );
    }

    #[test]
    fn test_extract_md5_multipart() {
        let etag = "\"d41d8cd98f00b204e9800998ecf8427e-3\"";
        assert_eq!(extract_md5_from_etag(etag), None);
    }

    #[test]
    fn test_extract_md5_empty() {
        assert_eq!(extract_md5_from_etag(""), None);
        assert_eq!(extract_md5_from_etag("\"\""), None);
    }

    #[test]
    fn test_extract_md5_uppercase() {
        let etag = "\"D41D8CD98F00B204E9800998ECF8427E\"";
        assert_eq!(
            extract_md5_from_etag(etag),
            Some("d41d8cd98f00b204e9800998ecf8427e".into())
        );
    }
}
