mod response;

pub use response::*;

use std::collections::HashMap;

use crate::accession::{ProjectAccession, ProjectKind};
use crate::error::{Error, Result};

const SDL_URL: &str = "https://locate.ncbi.nlm.nih.gov/sdl/2/retrieve";

/// NCBI EUtils ESearch endpoint (returns JSON).
const EUTILS_ESEARCH_URL: &str = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi";

/// NCBI EUtils RunInfo endpoint (returns CSV).
const EUTILS_EFETCH_URL: &str = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi";

/// Maximum UIDs per EFetch request to stay within URL length limits.
const EFETCH_BATCH_SIZE: usize = 500;

/// Maximum retry attempts for HTTP 429 / 5xx responses.
const MAX_API_RETRIES: u32 = 3;

/// HTTP GET with automatic retry on 429 (Too Many Requests) and 5xx errors.
///
/// Uses exponential backoff (1s, 2s, 4s) with random jitter (0-500ms) to
/// avoid thundering-herd effects when NCBI rate-limits concurrent requests.
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
            // `info!` not `warn!`: a single 429/5xx that recovers under the
            // 3-attempt backoff isn't an error the user needs to see — the
            // exhausted-retry case will surface as a real Error to the caller.
            tracing::info!(
                "HTTP {status} from {url}, retry {}/{MAX_API_RETRIES} in {delay:?}",
                attempt + 1
            );
            tokio::time::sleep(delay).await;
            continue;
        }

        return Ok(resp);
    }
    unreachable!()
}

/// Cheap pseudo-random jitter (0-500ms) without pulling in the `rand` crate.
fn rand_jitter_ms() -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    (nanos % 500) as u64
}

/// Format preference for SRA downloads.
///
/// Controls the `capability` query parameter sent to the SDL API:
/// - `Sra` (default): no capability parameter, returns SRA Normalized from S3 ODP.
/// - `Sralite`: sends `capability=zqa:z`, returns SRA-lite from NCBI servers.
///
/// SRA-lite files are 2-10x smaller but use simplified quality scores
/// (Q30 pass-filter, Q3 reject).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FormatPreference {
    /// SRA Normalized (full quality scores). Default.
    #[default]
    Sra,
    /// SRA-lite (simplified quality scores, smaller files).
    Sralite,
}

/// A single download mirror for a resolved file.
#[derive(Debug, Clone)]
pub struct ResolvedMirror {
    /// Direct download URL.
    pub url: String,
    /// Service name (e.g. "ncbi", "s3", "gs").
    pub service: String,
}

/// A resolved SRA file with download URLs and metadata.
#[derive(Debug, Clone)]
pub struct ResolvedFile {
    /// Download mirrors with service labels, filtered to freely accessible locations.
    pub mirrors: Vec<ResolvedMirror>,
    /// File size in bytes.
    pub size: u64,
    /// MD5 hex digest, if provided.
    pub md5: Option<String>,
    /// Whether this is an SRA-lite file (quality scores stripped).
    pub is_lite: bool,
}

/// Read structure metadata from NCBI EUtils.
///
/// Queried from the EFetch RunInfo CSV endpoint, this provides authoritative
/// read structure information that is more reliable than parsing VDB metadata.
#[derive(Debug, Clone)]
pub struct RunInfo {
    /// Number of reads per spot (1 for SINGLE, 2 for PAIRED).
    pub nreads: usize,
    /// Per-read average lengths (e.g. `[151, 151]` for paired 151bp).
    pub avg_read_len: Vec<u32>,
    /// Total bases per spot (sum of `avg_read_len`).
    pub spot_len: u32,
    /// Sequencing platform (e.g. "ILLUMINA", "LS454", "OXFORD_NANOPORE").
    pub platform: Option<String>,
    /// Total spot count reported by EUtils RunInfo (None if missing/unparseable).
    pub spots: Option<u64>,
}

/// Resolved download information for a single accession.
#[derive(Debug, Clone)]
pub struct ResolvedAccession {
    /// The accession string.
    pub accession: String,
    /// The primary SRA data file.
    pub sra_file: ResolvedFile,
    /// Optional vdbcache companion file.
    pub vdbcache_file: Option<ResolvedFile>,
    /// Read structure from NCBI EUtils (may be `None` if the API call failed).
    pub run_info: Option<RunInfo>,
}

/// Client for the NCBI SDL (Service Discovery Layer) API.
pub struct SdlClient {
    http: reqwest::Client,
}

impl SdlClient {
    pub fn new() -> Self {
        Self::with_client(crate::http::default_client())
    }

    /// Construct a `SdlClient` that reuses an existing `reqwest::Client`.
    /// Prefer this when orchestrating multi-accession runs so the same
    /// connection pool (and TLS sessions) are reused across SDL, S3, and
    /// SRA downloads.
    pub fn with_client(http: reqwest::Client) -> Self {
        Self { http }
    }

    /// Access the inner reqwest client (for pipeline reuse).
    pub fn http_client(&self) -> &reqwest::Client {
        &self.http
    }

    /// Resolve one or more accessions to download locations via the SDL API.
    ///
    /// Uses GET with query parameters: `?acc=SRR000001&acc=SRR000002`
    /// When `format` is `Sralite`, appends `capability=zqa:z` to request
    /// SRA-lite files from the SDL API.
    pub async fn resolve(
        &self,
        accessions: &[String],
        format: FormatPreference,
    ) -> Result<SdlResponse> {
        let mut url = reqwest::Url::parse(SDL_URL).expect("invalid SDL URL");
        for acc in accessions {
            url.query_pairs_mut().append_pair("acc", acc);
        }
        if format == FormatPreference::Sralite {
            url.query_pairs_mut().append_pair("capability", "zqa:z");
        }

        tracing::debug!("SDL request: {url}");

        let resp = http_get_with_retry(&self.http, url.as_str())
            .await
            .map_err(|e| Error::Sdl {
                message: e.to_string(),
            })?;

        if !resp.status().is_success() {
            return Err(Error::Sdl {
                message: format!("HTTP {}", resp.status()),
            });
        }

        let body = resp.text().await.map_err(|e| Error::Sdl {
            message: e.to_string(),
        })?;
        tracing::debug!("SDL response: {body}");

        let parsed: SdlResponse = serde_json::from_str(&body)?;
        Ok(parsed)
    }

    /// Resolve a single accession and return a clean summary with URLs, size, MD5, etc.
    ///
    /// Also queries the NCBI EUtils API for read structure metadata (nreads,
    /// per-read lengths). This is used as the authoritative source for
    /// splitting paired-end reads when the SRA file lacks a `READ_LEN` column.
    pub async fn resolve_one(
        &self,
        accession: &str,
        format: FormatPreference,
    ) -> Result<ResolvedAccession> {
        let response = self.resolve(&[accession.to_string()], format).await?;

        let result = response
            .find_result(accession)
            .ok_or_else(|| Error::NotFound(accession.to_string()))?;

        if let Some(status) = result.status
            && status != 200
        {
            let msg = result.message.as_deref().unwrap_or("unknown error");
            return Err(Error::Sdl {
                message: format!("{accession}: SDL status {status} — {msg}"),
            });
        }

        let sra_sdl = result.find_sra_file().ok_or_else(|| Error::Sdl {
            message: format!("no SRA file found for {accession}"),
        })?;

        let sra_file = resolved_file_from_sdl(sra_sdl)?;

        let vdbcache_file = result
            .find_vdbcache_file()
            .and_then(|f| resolved_file_from_sdl(f).ok());

        // Fetch read structure from EUtils (non-fatal on failure).
        let run_info = self.fetch_run_info(accession).await;

        Ok(ResolvedAccession {
            accession: accession.to_string(),
            sra_file,
            vdbcache_file,
            run_info,
        })
    }

    /// Resolve multiple accessions in batch (one SDL call + one EUtils call).
    ///
    /// The outer `Result` fails if the SDL batch request itself fails.
    /// Individual per-accession failures (404, missing SRA file) are returned
    /// as `Err` entries in the inner `Vec`.
    pub async fn resolve_many(
        &self,
        accessions: &[String],
        format: FormatPreference,
    ) -> Result<Vec<Result<ResolvedAccession>>> {
        if accessions.is_empty() {
            return Ok(Vec::new());
        }

        // Single batched SDL call.
        let sdl_response = self.resolve(accessions, format).await?;

        // Single batched EUtils RunInfo call (non-fatal).
        let run_info_map = self.fetch_run_info_batch(accessions).await;

        // Assemble per-accession results.
        let results = accessions
            .iter()
            .map(|acc| {
                let result = sdl_response
                    .find_result(acc)
                    .ok_or_else(|| Error::NotFound(acc.clone()))?;

                if let Some(status) = result.status
                    && status != 200
                {
                    let msg = result.message.as_deref().unwrap_or("unknown error");
                    return Err(Error::Sdl {
                        message: format!("{acc}: SDL status {status} — {msg}"),
                    });
                }

                let sra_sdl = result.find_sra_file().ok_or_else(|| Error::Sdl {
                    message: format!("no SRA file found for {acc}"),
                })?;

                let sra_file = resolved_file_from_sdl(sra_sdl)?;

                let vdbcache_file = result
                    .find_vdbcache_file()
                    .and_then(|f| resolved_file_from_sdl(f).ok());

                let run_info = run_info_map.get(acc).cloned();

                Ok(ResolvedAccession {
                    accession: acc.clone(),
                    sra_file,
                    vdbcache_file,
                    run_info,
                })
            })
            .collect();

        Ok(results)
    }

    /// Query NCBI EUtils EFetch RunInfo CSV for read structure metadata.
    ///
    /// Returns `None` on any failure (network, parse, unexpected format) —
    /// this is a best-effort enhancement, not a hard requirement.
    async fn fetch_run_info(&self, accession: &str) -> Option<RunInfo> {
        let map = self.fetch_run_info_batch(&[accession.to_string()]).await;
        map.into_values().next()
    }

    /// Batch-fetch RunInfo for multiple accessions in chunks of [`EFETCH_BATCH_SIZE`].
    ///
    /// Non-fatal: accessions that fail to resolve simply won't appear in the map.
    pub async fn fetch_run_info_batch(&self, accessions: &[String]) -> HashMap<String, RunInfo> {
        let mut result = HashMap::new();

        for chunk in accessions.chunks(EFETCH_BATCH_SIZE) {
            let ids = chunk.join(",");
            let url = format!("{EUTILS_EFETCH_URL}?db=sra&id={ids}&rettype=runinfo&retmode=text");

            tracing::debug!("EUtils RunInfo batch request ({} accessions)", chunk.len());

            let resp = match http_get_with_retry(&self.http, &url).await {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!("EUtils RunInfo batch request failed: {e}");
                    continue;
                }
            };

            if !resp.status().is_success() {
                tracing::warn!("EUtils RunInfo batch HTTP {}", resp.status());
                continue;
            }

            let body = match resp.text().await {
                Ok(b) => b,
                Err(e) => {
                    tracing::warn!("EUtils RunInfo batch body read failed: {e}");
                    continue;
                }
            };

            tracing::debug!("EUtils RunInfo batch response: {body}");

            result.extend(parse_run_info_csv_multi(&body));
        }

        result
    }

    /// Resolve a project/study accession to a list of run accession strings.
    ///
    /// Uses ESearch to find SRA UIDs for the project, then EFetch RunInfo CSV
    /// to extract the individual run accessions (SRR/ERR/DRR).
    pub async fn resolve_project(&self, project: &ProjectAccession) -> Result<Vec<String>> {
        let accession = project.to_string();

        // Build the ESearch query term based on accession type.
        let term = match project.kind {
            ProjectKind::Study => format!("{accession}[accn]"),
            ProjectKind::BioProject => format!("{accession}[bioproject]"),
        };

        tracing::info!("{accession}: resolving project to run accessions via EUtils");

        // Step 1: ESearch to get SRA UIDs.
        let search_url =
            format!("{EUTILS_ESEARCH_URL}?db=sra&term={term}&retmax=10000&retmode=json");
        tracing::debug!("ESearch request: {search_url}");

        let resp = http_get_with_retry(&self.http, &search_url)
            .await
            .map_err(|e| Error::Sdl {
                message: format!("{accession}: ESearch request failed: {e}"),
            })?;

        if !resp.status().is_success() {
            return Err(Error::Sdl {
                message: format!("{accession}: ESearch HTTP {}", resp.status()),
            });
        }

        let body: serde_json::Value = resp.json().await.map_err(|e| Error::Sdl {
            message: format!("{accession}: ESearch JSON parse failed: {e}"),
        })?;

        tracing::debug!("ESearch response: {body}");

        let id_list = body["esearchresult"]["idlist"]
            .as_array()
            .ok_or_else(|| Error::Sdl {
                message: format!("{accession}: ESearch returned no idlist"),
            })?;

        if id_list.is_empty() {
            return Err(Error::NotFound(format!(
                "{accession} — verify the accession exists at https://www.ncbi.nlm.nih.gov/sra/?term={accession}"
            )));
        }

        let uids: Vec<String> = id_list
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();

        tracing::debug!("{accession}: ESearch found {} SRA UIDs", uids.len());

        // Step 2: EFetch RunInfo CSV in batches to get run accessions.
        let mut run_accessions = Vec::new();

        for chunk in uids.chunks(EFETCH_BATCH_SIZE) {
            let ids = chunk.join(",");
            let fetch_url =
                format!("{EUTILS_EFETCH_URL}?db=sra&id={ids}&rettype=runinfo&retmode=text");

            tracing::debug!("EFetch RunInfo batch request ({} UIDs)", chunk.len());

            let resp = http_get_with_retry(&self.http, &fetch_url)
                .await
                .map_err(|e| Error::Sdl {
                    message: format!("{accession}: EFetch request failed: {e}"),
                })?;

            if !resp.status().is_success() {
                return Err(Error::Sdl {
                    message: format!("{accession}: EFetch HTTP {}", resp.status()),
                });
            }

            let csv_body = resp.text().await.map_err(|e| Error::Sdl {
                message: format!("{accession}: EFetch body read failed: {e}"),
            })?;

            let runs = parse_run_accessions_from_csv(&csv_body);
            run_accessions.extend(runs);
        }

        if run_accessions.is_empty() {
            return Err(Error::NotFound(format!(
                "{accession} — project exists but contains no run accessions"
            )));
        }

        run_accessions.sort();
        run_accessions.dedup();

        tracing::info!(
            "{accession}: resolved to {} run accession(s)",
            run_accessions.len()
        );

        Ok(run_accessions)
    }
}

/// Parse an EFetch RunInfo CSV with multiple data rows into a map of accession → RunInfo.
///
/// The CSV has a header row followed by one data row per run. We need the `Run`
/// column as key, and `LibraryLayout` + `avgLength` to build each `RunInfo`.
/// Rows that fail to parse are silently skipped.
fn parse_run_info_csv_multi(body: &str) -> HashMap<String, RunInfo> {
    let mut result = HashMap::new();
    let mut lines = body.lines().filter(|l| !l.trim().is_empty());

    let Some(header) = lines.next() else {
        return result;
    };

    let headers: Vec<&str> = header.split(',').collect();
    let col = |name: &str| headers.iter().position(|h| *h == name);

    let Some(layout_idx) = col("LibraryLayout") else {
        return result;
    };
    let Some(avg_len_idx) = col("avgLength") else {
        return result;
    };
    let run_idx = col("Run");
    let platform_idx = col("Platform");
    let spots_idx = col("spots");

    for data in lines {
        let values: Vec<&str> = data.split(',').collect();
        if values.len() != headers.len() {
            continue;
        }

        let accession = run_idx
            .and_then(|i| values.get(i).copied())
            .unwrap_or("")
            .to_string();
        if accession.is_empty() {
            continue;
        }

        let Some(layout) = values.get(layout_idx).copied() else {
            continue;
        };
        let Some(avg_length) = values.get(avg_len_idx).and_then(|v| v.parse::<u32>().ok()) else {
            continue;
        };

        let nreads = match layout {
            "PAIRED" => 2usize,
            "SINGLE" => 1,
            other => {
                tracing::warn!("{accession}: unknown LibraryLayout '{other}', defaulting to 1");
                1
            }
        };

        let per_read = avg_length / nreads as u32;
        if per_read == 0 {
            tracing::warn!("{accession}: computed per_read_len=0 from avgLength={avg_length}");
            continue;
        }

        let mut avg_read_len = vec![per_read; nreads];
        let used = per_read * nreads as u32;
        if used < avg_length
            && let Some(last) = avg_read_len.last_mut()
        {
            *last += avg_length - used;
        }

        let platform = platform_idx
            .and_then(|i| values.get(i).copied())
            .filter(|s| !s.is_empty())
            .map(String::from);

        let spots = spots_idx.and_then(|i| values.get(i).and_then(|v| v.parse::<u64>().ok()));

        tracing::debug!(
            "{accession}: EUtils RunInfo: layout={layout}, avgLength={avg_length}, \
             nreads={nreads}, per_read_len={avg_read_len:?}, platform={platform:?}, spots={spots:?}",
        );

        result.insert(
            accession,
            RunInfo {
                nreads,
                avg_read_len,
                spot_len: avg_length,
                platform,
                spots,
            },
        );
    }

    result
}

/// Parse the EFetch RunInfo CSV to extract read structure for a single accession.
///
/// Thin wrapper around [`parse_run_info_csv_multi`] that looks up the given accession.
#[cfg(test)]
fn parse_run_info_csv(body: &str, accession: &str) -> Option<RunInfo> {
    let mut map = parse_run_info_csv_multi(body);
    map.remove(accession)
}

/// Parse run accessions from an EFetch RunInfo CSV body.
///
/// Extracts the `Run` column from each data row. Rows with empty or missing
/// `Run` values are silently skipped.
fn parse_run_accessions_from_csv(body: &str) -> Vec<String> {
    let mut lines = body.lines().filter(|l| !l.trim().is_empty());
    let Some(header) = lines.next() else {
        return Vec::new();
    };

    let headers: Vec<&str> = header.split(',').collect();
    let run_idx = match headers.iter().position(|h| *h == "Run") {
        Some(i) => i,
        None => return Vec::new(),
    };

    lines
        .filter_map(|line| {
            let fields: Vec<&str> = line.split(',').collect();
            let run = fields.get(run_idx).copied().unwrap_or("");
            if run.is_empty() {
                None
            } else {
                Some(run.to_string())
            }
        })
        .collect()
}

/// Extract a [`ResolvedFile`] from an [`SdlFile`] response entry.
fn resolved_file_from_sdl(sdl_file: &SdlFile) -> Result<ResolvedFile> {
    let mirrors: Vec<ResolvedMirror> = sdl_file
        .locations
        .iter()
        .filter(|loc| !loc.ce_required && !loc.pay_required)
        .map(|loc| ResolvedMirror {
            url: loc.link.clone(),
            service: loc.service.clone().unwrap_or_else(|| "unknown".into()),
        })
        .collect();

    let size = sdl_file.size_bytes().unwrap_or(0);
    let md5 = sdl_file.md5.clone();
    let is_lite = sdl_file.noqual;

    Ok(ResolvedFile {
        mirrors,
        size,
        md5,
        is_lite,
    })
}

impl Default for SdlClient {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_sdl_file(
        file_type: &str,
        noqual: bool,
        size: Option<u64>,
        locations: Vec<SdlLocation>,
    ) -> SdlFile {
        SdlFile {
            file_type: file_type.to_string(),
            name: None,
            accession: None,
            object: None,
            size,
            md5: None,
            format: None,
            modification_date: None,
            noqual,
            locations,
        }
    }

    fn make_location(service: &str, ce: bool, pay: bool) -> SdlLocation {
        SdlLocation {
            link: format!("https://{service}.example.com/file"),
            service: Some(service.to_string()),
            region: None,
            expiration_date: None,
            ce_required: ce,
            pay_required: pay,
        }
    }

    #[test]
    fn resolved_file_filters_paid_mirrors() {
        let sdl = make_sdl_file(
            "sra",
            false,
            Some(1000),
            vec![
                make_location("s3", false, false),
                make_location("gs", true, false),   // CE required
                make_location("ncbi", false, true), // pay required
            ],
        );
        let resolved = resolved_file_from_sdl(&sdl).unwrap();
        assert_eq!(resolved.mirrors.len(), 1);
        assert_eq!(resolved.mirrors[0].service, "s3");
    }

    #[test]
    fn resolved_file_keeps_free_mirrors() {
        let sdl = make_sdl_file(
            "sra",
            false,
            Some(5000),
            vec![
                make_location("s3", false, false),
                make_location("ncbi", false, false),
            ],
        );
        let resolved = resolved_file_from_sdl(&sdl).unwrap();
        assert_eq!(resolved.mirrors.len(), 2);
        assert_eq!(resolved.size, 5000);
    }

    #[test]
    fn resolved_file_lite_flag() {
        let sdl = make_sdl_file("sra", true, Some(1000), vec![]);
        let resolved = resolved_file_from_sdl(&sdl).unwrap();
        assert!(resolved.is_lite);
    }

    // -----------------------------------------------------------------------
    // RunInfo CSV parsing
    // -----------------------------------------------------------------------

    #[test]
    fn parse_run_info_paired() {
        let csv = "Run,spots,bases,avgLength,LibraryLayout\n\
                   SRR17778092,506446387,152946808874,302,PAIRED\n";
        let ri = parse_run_info_csv(csv, "SRR17778092").unwrap();
        assert_eq!(ri.nreads, 2);
        assert_eq!(ri.spot_len, 302);
        assert_eq!(ri.avg_read_len, vec![151, 151]);
        assert_eq!(ri.spots, Some(506_446_387));
    }

    #[test]
    fn parse_run_info_missing_spots_column() {
        let csv = "Run,avgLength,LibraryLayout\nTEST,150,SINGLE\n";
        let ri = parse_run_info_csv(csv, "TEST").unwrap();
        assert_eq!(ri.spots, None);
    }

    #[test]
    fn parse_run_info_single() {
        let csv = "Run,avgLength,LibraryLayout\n\
                   SRR000001,150,SINGLE\n";
        let ri = parse_run_info_csv(csv, "SRR000001").unwrap();
        assert_eq!(ri.nreads, 1);
        assert_eq!(ri.spot_len, 150);
        assert_eq!(ri.avg_read_len, vec![150]);
    }

    #[test]
    fn parse_run_info_odd_avg_length() {
        // avgLength=301 for PAIRED → 150 + 151
        let csv = "Run,avgLength,LibraryLayout\nTEST,301,PAIRED\n";
        let ri = parse_run_info_csv(csv, "TEST").unwrap();
        assert_eq!(ri.nreads, 2);
        assert_eq!(ri.avg_read_len, vec![150, 151]);
        assert_eq!(ri.spot_len, 301);
    }

    #[test]
    fn parse_run_info_missing_layout() {
        let csv = "Run,avgLength\nTEST,302\n";
        assert!(parse_run_info_csv(csv, "TEST").is_none());
    }

    #[test]
    fn parse_run_info_empty_body() {
        assert!(parse_run_info_csv("", "test").is_none());
    }

    #[test]
    fn parse_run_info_header_only() {
        let csv = "Run,avgLength,LibraryLayout\n";
        assert!(parse_run_info_csv(csv, "test").is_none());
    }

    #[test]
    fn parse_run_info_multi_rows() {
        let csv = "Run,spots,bases,avgLength,LibraryLayout\n\
                   SRR111,100,200,302,PAIRED\n\
                   SRR222,200,400,150,SINGLE\n";
        let map = parse_run_info_csv_multi(csv);
        assert_eq!(map.len(), 2);
        let r1 = map.get("SRR111").unwrap();
        assert_eq!(r1.nreads, 2);
        assert_eq!(r1.spot_len, 302);
        assert_eq!(r1.avg_read_len, vec![151, 151]);
        let r2 = map.get("SRR222").unwrap();
        assert_eq!(r2.nreads, 1);
        assert_eq!(r2.spot_len, 150);
        assert_eq!(r2.avg_read_len, vec![150]);
    }

    // -----------------------------------------------------------------------
    // Run accession extraction from RunInfo CSV
    // -----------------------------------------------------------------------

    #[test]
    fn parse_run_accessions_multiple_rows() {
        let csv = "Run,spots,bases,avgLength,LibraryLayout\n\
                   SRR1234567,100,200,150,PAIRED\n\
                   SRR1234568,200,400,150,PAIRED\n\
                   SRR1234569,300,600,150,SINGLE\n";
        let runs = parse_run_accessions_from_csv(csv);
        assert_eq!(runs, vec!["SRR1234567", "SRR1234568", "SRR1234569"]);
    }

    #[test]
    fn parse_run_accessions_empty_csv() {
        assert!(parse_run_accessions_from_csv("").is_empty());
    }

    #[test]
    fn parse_run_accessions_header_only() {
        let csv = "Run,spots\n";
        assert!(parse_run_accessions_from_csv(csv).is_empty());
    }

    #[test]
    fn parse_run_accessions_no_run_column() {
        let csv = "spots,bases\n100,200\n";
        assert!(parse_run_accessions_from_csv(csv).is_empty());
    }
}
