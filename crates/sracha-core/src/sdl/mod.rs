mod response;

pub use response::*;

use crate::accession::{ProjectAccession, ProjectKind};
use crate::error::{Error, Result};

const SDL_URL: &str = "https://locate.ncbi.nlm.nih.gov/sdl/2/retrieve";

/// NCBI EUtils ESearch endpoint (returns JSON).
const EUTILS_ESEARCH_URL: &str = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi";

/// NCBI EUtils RunInfo endpoint (returns CSV).
const EUTILS_EFETCH_URL: &str = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi";

/// Maximum UIDs per EFetch request to stay within URL length limits.
const EFETCH_BATCH_SIZE: usize = 500;

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
        Self {
            http: reqwest::Client::builder()
                .user_agent(format!("sracha/{}", env!("CARGO_PKG_VERSION")))
                .build()
                .expect("failed to build HTTP client"),
        }
    }

    /// Access the inner reqwest client (for pipeline reuse).
    pub fn http_client(&self) -> &reqwest::Client {
        &self.http
    }

    /// Resolve one or more accessions to download locations via the SDL API.
    ///
    /// Uses GET with query parameters: `?acc=SRR000001&acc=SRR000002`
    pub async fn resolve(&self, accessions: &[String]) -> Result<SdlResponse> {
        let mut url = reqwest::Url::parse(SDL_URL).expect("invalid SDL URL");
        for acc in accessions {
            url.query_pairs_mut().append_pair("acc", acc);
        }

        tracing::debug!("SDL request: {url}");

        let resp = self.http.get(url).send().await.map_err(|e| Error::Sdl {
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
    pub async fn resolve_one(&self, accession: &str) -> Result<ResolvedAccession> {
        let response = self.resolve(&[accession.to_string()]).await?;

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

    /// Query NCBI EUtils EFetch RunInfo CSV for read structure metadata.
    ///
    /// Returns `None` on any failure (network, parse, unexpected format) —
    /// this is a best-effort enhancement, not a hard requirement.
    async fn fetch_run_info(&self, accession: &str) -> Option<RunInfo> {
        let url = format!("{EUTILS_EFETCH_URL}?db=sra&id={accession}&rettype=runinfo&retmode=text");

        tracing::debug!("EUtils RunInfo request: {url}");

        let resp = match self.http.get(&url).send().await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("{accession}: EUtils RunInfo request failed: {e}");
                return None;
            }
        };

        if !resp.status().is_success() {
            tracing::warn!("{accession}: EUtils RunInfo HTTP {}", resp.status());
            return None;
        }

        let body = match resp.text().await {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!("{accession}: EUtils RunInfo body read failed: {e}");
                return None;
            }
        };

        tracing::debug!("EUtils RunInfo response: {body}");

        parse_run_info_csv(&body, accession)
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

        let resp = self
            .http
            .get(&search_url)
            .send()
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
                "no SRA runs found for {accession}"
            )));
        }

        let uids: Vec<String> = id_list
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();

        tracing::info!("{accession}: ESearch found {} SRA UIDs", uids.len());

        // Step 2: EFetch RunInfo CSV in batches to get run accessions.
        let mut run_accessions = Vec::new();

        for chunk in uids.chunks(EFETCH_BATCH_SIZE) {
            let ids = chunk.join(",");
            let fetch_url =
                format!("{EUTILS_EFETCH_URL}?db=sra&id={ids}&rettype=runinfo&retmode=text");

            tracing::debug!("EFetch RunInfo batch request ({} UIDs)", chunk.len());

            let resp = self
                .http
                .get(&fetch_url)
                .send()
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
                "EFetch returned no run accessions for {accession}"
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

/// Parse the EFetch RunInfo CSV to extract read structure.
///
/// The CSV has a header row and one data row. We need `LibraryLayout`
/// (PAIRED/SINGLE) and `avgLength` (total bases per spot).
fn parse_run_info_csv(body: &str, accession: &str) -> Option<RunInfo> {
    let mut lines = body.lines().filter(|l| !l.trim().is_empty());
    let header = lines.next()?;
    let data = lines.next()?;

    let headers: Vec<&str> = header.split(',').collect();
    let values: Vec<&str> = data.split(',').collect();

    if headers.len() != values.len() {
        tracing::warn!(
            "{accession}: RunInfo CSV header/data column mismatch ({} vs {})",
            headers.len(),
            values.len()
        );
        return None;
    }

    let get_field = |name: &str| -> Option<&str> {
        headers
            .iter()
            .position(|h| *h == name)
            .and_then(|i| values.get(i).copied())
    };

    let layout = get_field("LibraryLayout")?;
    let avg_length: u32 = get_field("avgLength")?.parse().ok()?;

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
        return None;
    }

    // Build per-read lengths. Last read gets any remainder from integer division.
    let mut avg_read_len = vec![per_read; nreads];
    let used = per_read * nreads as u32;
    if used < avg_length
        && let Some(last) = avg_read_len.last_mut()
    {
        *last += avg_length - used;
    }

    tracing::info!(
        "{accession}: EUtils RunInfo: layout={layout}, avgLength={avg_length}, \
         nreads={nreads}, per_read_len={avg_read_len:?}",
    );

    Some(RunInfo {
        nreads,
        avg_read_len,
        spot_len: avg_length,
    })
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
        let csv = "avgLength,LibraryLayout\n301,PAIRED\n";
        let ri = parse_run_info_csv(csv, "test").unwrap();
        assert_eq!(ri.nreads, 2);
        assert_eq!(ri.avg_read_len, vec![150, 151]);
        assert_eq!(ri.spot_len, 301);
    }

    #[test]
    fn parse_run_info_missing_layout() {
        let csv = "avgLength\n302\n";
        assert!(parse_run_info_csv(csv, "test").is_none());
    }

    #[test]
    fn parse_run_info_empty_body() {
        assert!(parse_run_info_csv("", "test").is_none());
    }

    #[test]
    fn parse_run_info_header_only() {
        let csv = "avgLength,LibraryLayout\n";
        assert!(parse_run_info_csv(csv, "test").is_none());
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
