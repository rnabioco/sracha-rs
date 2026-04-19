//! ENA (European Nucleotide Archive) Filereport API client.
//!
//! ENA mirrors all INSDC data and serves archive-generated `fastq.gz` files
//! directly over HTTP. Using these files lets `sracha get` skip the SRA
//! download + VDB decode + gzip compression stages entirely.
//!
//! The single endpoint used here — `filereport?result=read_run` — accepts
//! both run accessions (SRR/ERR/DRR) and project accessions
//! (PRJEB/PRJNA/ERP/SRP/DRP). Projects return one JSON row per run.

use std::sync::Arc;

use serde::Deserialize;
use tokio::sync::Semaphore;

use crate::error::{Error, Result};
use crate::fastq::OutputSlot;

const ENA_FILEREPORT_URL: &str = "https://www.ebi.ac.uk/ena/portal/api/filereport";

/// Maximum retry attempts for HTTP 429 / 5xx responses.
const MAX_API_RETRIES: u32 = 3;

/// Concurrency cap for batch queries. ENA allows 50 req/s; 20 in-flight
/// gives headroom for other API calls sharing the same process.
const ENA_BATCH_CONCURRENCY: usize = 20;

/// Fields requested from the filereport API. Order matters only for humans —
/// the API returns a JSON object keyed by field name either way.
const ENA_FIELDS: &str = "run_accession,fastq_ftp,fastq_md5,fastq_bytes";

/// One downloadable FASTQ file from ENA.
#[derive(Debug, Clone)]
pub struct EnaFastqFile {
    /// HTTP URL (the filereport returns `ftp://` paths; we rewrite to `http://`
    /// because `ftp.sra.ebi.ac.uk` serves the same paths over HTTP without
    /// authentication).
    pub url: String,
    /// MD5 hex digest for integrity verification post-download.
    pub md5: String,
    /// File size in bytes.
    pub size: u64,
    /// Which output slot this file maps to (Read1, Read2, Single, Unpaired).
    pub slot: OutputSlot,
}

/// Resolved ENA download information for one run accession.
#[derive(Debug, Clone)]
pub struct EnaResolved {
    /// Run accession (SRR/ERR/DRR).
    pub accession: String,
    /// All downloadable files for this run.
    pub fastq_files: Vec<EnaFastqFile>,
    /// Sum of `fastq_files[i].size`.
    pub total_size: u64,
}

/// Raw JSON row from the filereport API.
#[derive(Debug, Deserialize)]
struct FilereportRow {
    #[serde(default)]
    run_accession: String,
    #[serde(default)]
    fastq_ftp: String,
    #[serde(default)]
    fastq_md5: String,
    #[serde(default)]
    fastq_bytes: String,
}

/// HTTP GET with automatic retry on 429 and 5xx errors.
///
/// Mirrors `sdl::http_get_with_retry` — exponential backoff (1s, 2s, 4s) plus
/// 0-500ms jitter to avoid thundering-herd under concurrent ENA batch queries.
async fn http_get_with_retry(
    client: &reqwest::Client,
    url: &str,
) -> std::result::Result<reqwest::Response, reqwest::Error> {
    for attempt in 0..=MAX_API_RETRIES {
        let resp = client.get(url).send().await?;
        let status = resp.status();

        if (status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error())
            && attempt < MAX_API_RETRIES
        {
            let base = std::time::Duration::from_secs(1 << attempt);
            let jitter = std::time::Duration::from_millis(rand_jitter_ms());
            let delay = base + jitter;
            tracing::info!(
                "HTTP {status} from ENA {url}, retry {}/{MAX_API_RETRIES} in {delay:?}",
                attempt + 1
            );
            tokio::time::sleep(delay).await;
            continue;
        }

        return Ok(resp);
    }
    unreachable!()
}

fn rand_jitter_ms() -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    (nanos % 500) as u64
}

/// Build the filereport URL for a given accession (run or project).
fn build_url(accession: &str) -> String {
    // `url::Url` would percent-encode `accession=SRR000001` identically; keep
    // it simple since accessions are ASCII alphanumeric.
    format!(
        "{ENA_FILEREPORT_URL}?accession={accession}&result=read_run&fields={ENA_FIELDS}&format=json"
    )
}

/// Fetch + parse raw filereport rows for an accession.
async fn fetch_rows(client: &reqwest::Client, accession: &str) -> Result<Vec<FilereportRow>> {
    let url = build_url(accession);
    tracing::debug!("ENA request: {url}");

    let resp = http_get_with_retry(client, &url)
        .await
        .map_err(|e| Error::Ena {
            message: e.to_string(),
        })?;

    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(Vec::new());
    }
    if !resp.status().is_success() {
        return Err(Error::Ena {
            message: format!("HTTP {} for accession {}", resp.status(), accession),
        });
    }

    let body = resp.text().await.map_err(|e| Error::Ena {
        message: e.to_string(),
    })?;
    tracing::debug!("ENA response for {accession}: {body}");

    // ENA returns `[]` for unknown accessions with format=json.
    if body.trim().is_empty() || body.trim() == "[]" {
        return Ok(Vec::new());
    }

    let rows: Vec<FilereportRow> = serde_json::from_str(&body).map_err(|e| Error::Ena {
        message: format!("parse filereport for {accession}: {e}"),
    })?;
    Ok(rows)
}

/// Convert a single filereport row into `EnaResolved`, or `None` if the row
/// has no FASTQ payload (some runs list only BAM/submitted files).
fn row_to_resolved(row: FilereportRow) -> Option<EnaResolved> {
    let accession = row.run_accession;
    if accession.is_empty() {
        return None;
    }

    let ftps: Vec<&str> = split_or_empty(&row.fastq_ftp);
    if ftps.is_empty() {
        return None;
    }
    let md5s: Vec<&str> = split_or_empty(&row.fastq_md5);
    let bytes: Vec<&str> = split_or_empty(&row.fastq_bytes);

    // ENA guarantees equal field counts when fastq_ftp is populated, but be
    // defensive: missing parallel data disqualifies the row from the fast
    // path (we need size + MD5 to use download_file safely).
    if md5s.len() != ftps.len() || bytes.len() != ftps.len() {
        tracing::warn!(
            "ENA filereport for {accession}: mismatched field counts \
             (ftp={}, md5={}, bytes={}); skipping",
            ftps.len(),
            md5s.len(),
            bytes.len(),
        );
        return None;
    }

    let slots = assign_slots(&ftps);
    let mut files = Vec::with_capacity(ftps.len());
    let mut total: u64 = 0;
    for (i, ftp) in ftps.iter().enumerate() {
        let size: u64 = bytes[i].parse().unwrap_or(0);
        if size == 0 || md5s[i].is_empty() {
            // A row without size/md5 is unusable for the fast path.
            return None;
        }
        files.push(EnaFastqFile {
            url: ftp_to_http(ftp),
            md5: md5s[i].to_string(),
            size,
            slot: slots[i],
        });
        total += size;
    }

    Some(EnaResolved {
        accession,
        fastq_files: files,
        total_size: total,
    })
}

fn split_or_empty(s: &str) -> Vec<&str> {
    if s.is_empty() {
        Vec::new()
    } else {
        s.split(';').collect()
    }
}

/// Normalize a `fastq_ftp` value from ENA to an `http://` URL.
///
/// ENA's filereport returns values in one of three forms:
/// `ftp://ftp.sra.ebi.ac.uk/...`, `ftp.sra.ebi.ac.uk/...` (scheme-less,
/// the common case today), or `http://ftp.sra.ebi.ac.uk/...`. ENA serves
/// the same paths over HTTP on the same host, so we normalize all three
/// to `http://...`. HTTP avoids FTP client baggage (PASV, separate
/// control/data connections) and lets the existing parallel chunked
/// downloader reuse HTTP connection pools.
fn ftp_to_http(url: &str) -> String {
    if let Some(rest) = url.strip_prefix("ftp://") {
        format!("http://{rest}")
    } else if url.starts_with("http://") || url.starts_with("https://") {
        url.to_string()
    } else {
        format!("http://{url}")
    }
}

/// Assign `OutputSlot` to each file based on its filename suffix.
///
/// Naming conventions ENA uses:
/// - `{ACC}.fastq.gz` → `Single` (single-end) or `Unpaired` (orphan in paired runs)
/// - `{ACC}_1.fastq.gz` → `Read1`
/// - `{ACC}_2.fastq.gz` → `Read2`
///
/// When a paired run also has unpaired reads, ENA returns three files; the
/// bare-name file is the orphan and maps to `Unpaired` (same filename slot as
/// fasterq-dump's split-3 orphan). Single-end runs return one bare-name file
/// which maps to `Single`.
fn assign_slots(ftps: &[&str]) -> Vec<OutputSlot> {
    let has_r1 = ftps.iter().any(|u| has_suffix(u, "_1.fastq.gz"));
    let has_r2 = ftps.iter().any(|u| has_suffix(u, "_2.fastq.gz"));
    let paired = has_r1 && has_r2;

    ftps.iter()
        .map(|u| {
            if has_suffix(u, "_1.fastq.gz") {
                OutputSlot::Read1
            } else if has_suffix(u, "_2.fastq.gz") {
                OutputSlot::Read2
            } else if paired {
                OutputSlot::Unpaired
            } else {
                OutputSlot::Single
            }
        })
        .collect()
}

fn has_suffix(url: &str, suffix: &str) -> bool {
    // Match against the filename portion, ignoring query strings.
    let path = url.split('?').next().unwrap_or(url);
    path.ends_with(suffix)
}

/// Resolve a single run accession to its ENA FASTQ files.
///
/// Returns `Ok(None)` when ENA has no FASTQ for this accession (the caller
/// should then fall back to the NCBI path). Returns `Err` only for API /
/// network failures.
pub async fn resolve_ena(client: &reqwest::Client, accession: &str) -> Result<Option<EnaResolved>> {
    let rows = fetch_rows(client, accession).await?;
    // For a run accession, filereport returns 0 or 1 row.
    Ok(rows.into_iter().find_map(row_to_resolved))
}

/// Resolve a batch of run accessions concurrently (up to 20 in flight).
///
/// Preserves input order. Each result is `(accession, Option<EnaResolved>)`
/// where `None` means "no FASTQ on ENA" OR "ENA API failed for this one"; the
/// specific failure is logged via `tracing::warn!` but does not abort the
/// batch, matching the forgiving behavior of SDL batch resolution.
pub async fn resolve_ena_many(
    client: &reqwest::Client,
    accessions: &[String],
) -> Vec<(String, Option<EnaResolved>)> {
    let sem = Arc::new(Semaphore::new(ENA_BATCH_CONCURRENCY));
    let mut handles = Vec::with_capacity(accessions.len());
    for acc in accessions {
        let client = client.clone();
        let sem = sem.clone();
        let acc = acc.clone();
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire_owned().await.expect("semaphore closed");
            let result = resolve_ena(&client, &acc).await;
            match result {
                Ok(r) => (acc, r),
                Err(e) => {
                    tracing::warn!("ENA resolve failed for {acc}: {e}; treating as no FASTQ");
                    (acc, None)
                }
            }
        }));
    }

    let mut out = Vec::with_capacity(handles.len());
    for h in handles {
        match h.await {
            Ok(pair) => out.push(pair),
            Err(join_err) => {
                tracing::warn!("ENA resolve task panicked: {join_err}");
            }
        }
    }
    out
}

/// Resolve a project/study accession to all its runs via the same filereport
/// endpoint. Accepts PRJEB/PRJNA/ERP/SRP/DRP.
///
/// Returns an empty vec when the project has no runs with FASTQ payloads
/// (caller should fall back to NCBI EUtils).
pub async fn resolve_ena_project(
    client: &reqwest::Client,
    project: &str,
) -> Result<Vec<EnaResolved>> {
    let rows = fetch_rows(client, project).await?;
    Ok(rows.into_iter().filter_map(row_to_resolved).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ftp_to_http_rewrites_scheme() {
        assert_eq!(
            ftp_to_http("ftp://ftp.sra.ebi.ac.uk/vol1/fastq/SRR000/SRR000001/SRR000001_1.fastq.gz"),
            "http://ftp.sra.ebi.ac.uk/vol1/fastq/SRR000/SRR000001/SRR000001_1.fastq.gz"
        );
    }

    #[test]
    fn ftp_to_http_passes_http_through() {
        assert_eq!(
            ftp_to_http("http://example.com/x.fastq.gz"),
            "http://example.com/x.fastq.gz"
        );
    }

    #[test]
    fn ftp_to_http_passes_https_through() {
        assert_eq!(
            ftp_to_http("https://example.com/x.fastq.gz"),
            "https://example.com/x.fastq.gz"
        );
    }

    #[test]
    fn ftp_to_http_prepends_when_schemeless() {
        // ENA's filereport often returns schemeless paths like this.
        assert_eq!(
            ftp_to_http("ftp.sra.ebi.ac.uk/vol1/fastq/SRR000/SRR000001/SRR000001_1.fastq.gz"),
            "http://ftp.sra.ebi.ac.uk/vol1/fastq/SRR000/SRR000001/SRR000001_1.fastq.gz"
        );
    }

    #[test]
    fn slots_single_end() {
        let ftps = vec!["ftp://host/SRR1.fastq.gz"];
        assert_eq!(assign_slots(&ftps), vec![OutputSlot::Single]);
    }

    #[test]
    fn slots_paired_end() {
        let ftps = vec!["ftp://host/SRR1_1.fastq.gz", "ftp://host/SRR1_2.fastq.gz"];
        assert_eq!(
            assign_slots(&ftps),
            vec![OutputSlot::Read1, OutputSlot::Read2]
        );
    }

    #[test]
    fn slots_paired_plus_unpaired() {
        // Three-file case: paired mates plus orphan bare-name file.
        let ftps = vec![
            "ftp://host/SRR1.fastq.gz",
            "ftp://host/SRR1_1.fastq.gz",
            "ftp://host/SRR1_2.fastq.gz",
        ];
        assert_eq!(
            assign_slots(&ftps),
            vec![OutputSlot::Unpaired, OutputSlot::Read1, OutputSlot::Read2]
        );
    }

    #[test]
    fn row_to_resolved_single_end() {
        let row = FilereportRow {
            run_accession: "SRR000001".into(),
            fastq_ftp: "ftp://ftp.sra.ebi.ac.uk/vol1/fastq/SRR000/SRR000001/SRR000001.fastq.gz"
                .into(),
            fastq_md5: "abc123".into(),
            fastq_bytes: "12345".into(),
        };
        let resolved = row_to_resolved(row).expect("should resolve");
        assert_eq!(resolved.accession, "SRR000001");
        assert_eq!(resolved.fastq_files.len(), 1);
        assert_eq!(resolved.fastq_files[0].slot, OutputSlot::Single);
        assert_eq!(resolved.fastq_files[0].md5, "abc123");
        assert_eq!(resolved.fastq_files[0].size, 12345);
        assert_eq!(resolved.total_size, 12345);
        assert!(resolved.fastq_files[0].url.starts_with("http://"));
    }

    #[test]
    fn row_to_resolved_paired() {
        let row = FilereportRow {
            run_accession: "SRR123".into(),
            fastq_ftp: "ftp://host/SRR123_1.fastq.gz;ftp://host/SRR123_2.fastq.gz".into(),
            fastq_md5: "aaa;bbb".into(),
            fastq_bytes: "100;200".into(),
        };
        let r = row_to_resolved(row).expect("should resolve");
        assert_eq!(r.fastq_files.len(), 2);
        assert_eq!(r.fastq_files[0].slot, OutputSlot::Read1);
        assert_eq!(r.fastq_files[1].slot, OutputSlot::Read2);
        assert_eq!(r.total_size, 300);
    }

    #[test]
    fn row_to_resolved_empty_ftp_is_none() {
        let row = FilereportRow {
            run_accession: "SRR999".into(),
            fastq_ftp: "".into(),
            fastq_md5: "".into(),
            fastq_bytes: "".into(),
        };
        assert!(row_to_resolved(row).is_none());
    }

    #[test]
    fn row_to_resolved_mismatched_counts_is_none() {
        let row = FilereportRow {
            run_accession: "SRR1".into(),
            fastq_ftp: "ftp://a;ftp://b".into(),
            fastq_md5: "aaa".into(), // only one MD5 for two files
            fastq_bytes: "1;2".into(),
        };
        assert!(row_to_resolved(row).is_none());
    }

    #[test]
    fn row_to_resolved_missing_size_or_md5_is_none() {
        let row = FilereportRow {
            run_accession: "SRR1".into(),
            fastq_ftp: "ftp://host/SRR1.fastq.gz".into(),
            fastq_md5: "".into(),
            fastq_bytes: "1".into(),
        };
        assert!(row_to_resolved(row).is_none());
    }

    #[test]
    fn build_url_shape() {
        let url = build_url("SRR000001");
        assert!(url.contains("accession=SRR000001"));
        assert!(url.contains("result=read_run"));
        assert!(url.contains("format=json"));
        assert!(url.contains("run_accession"));
        assert!(url.contains("fastq_ftp"));
        assert!(url.contains("fastq_md5"));
        assert!(url.contains("fastq_bytes"));
    }
}
