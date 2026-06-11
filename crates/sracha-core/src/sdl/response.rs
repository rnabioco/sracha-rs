use serde::Deserialize;
use serde_json::Value;

/// Top-level response from the NCBI SDL API.
#[derive(Debug, Default, Deserialize)]
pub struct SdlResponse {
    /// API version string (e.g., "2").
    pub version: Option<String>,
    /// Status code (integer in JSON, e.g., 200 or 500).
    #[serde(default, deserialize_with = "deserialize_status")]
    pub status: Option<i64>,
    pub message: Option<String>,
    #[serde(default, rename = "nextToken")]
    pub next_token: Option<String>,
    #[serde(default, rename = "result")]
    pub results: Vec<SdlResult>,
}

impl SdlResponse {
    /// Find the result entry matching a given accession string.
    pub fn find_result(&self, accession: &str) -> Option<&SdlResult> {
        self.results.iter().find(|r| {
            r.query.as_deref() == Some(accession) || r.bundle.as_deref() == Some(accession)
        })
    }
}

/// Per-accession result from SDL.
#[derive(Debug, Deserialize)]
pub struct SdlResult {
    /// The accession that was queried.
    pub query: Option<String>,
    /// Alternate field name used in some responses.
    pub bundle: Option<String>,
    /// Status code (integer, e.g., 200).
    #[serde(default, deserialize_with = "deserialize_status")]
    pub status: Option<i64>,
    #[serde(default, rename = "msg")]
    pub message: Option<String>,
    #[serde(default)]
    pub files: Vec<SdlFile>,
}

impl SdlResult {
    /// Get the accession string from whichever field is populated.
    pub fn accession(&self) -> &str {
        self.query
            .as_deref()
            .or(self.bundle.as_deref())
            .unwrap_or("unknown")
    }

    /// Find the first file entry with type "sra".
    pub fn find_sra_file(&self) -> Option<&SdlFile> {
        self.files.iter().find(|f| f.file_type == "sra")
    }

    /// Find the first file entry with type "vdbcache".
    pub fn find_vdbcache_file(&self) -> Option<&SdlFile> {
        self.files.iter().find(|f| f.file_type == "vdbcache")
    }

    /// Whether this result indicates success.
    pub fn is_ok(&self) -> bool {
        self.status == Some(200)
    }
}

/// A downloadable file for an accession.
#[derive(Debug, Deserialize)]
pub struct SdlFile {
    /// File type: "sra", "vdbcache", etc.
    #[serde(rename = "type")]
    pub file_type: String,
    /// File name.
    pub name: Option<String>,
    /// Accession (sometimes present).
    pub accession: Option<String>,
    /// Object identifier.
    pub object: Option<String>,
    /// File size in bytes (integer in JSON).
    #[serde(default, deserialize_with = "deserialize_size")]
    pub size: Option<u64>,
    /// MD5 hex digest.
    pub md5: Option<String>,
    /// File format.
    pub format: Option<String>,
    /// Modification date.
    #[serde(rename = "modificationDate")]
    pub modification_date: Option<String>,
    /// Whether quality scores are absent (SRA-lite).
    #[serde(default)]
    pub noqual: bool,
    /// Download locations.
    #[serde(default)]
    pub locations: Vec<SdlLocation>,
}

impl SdlFile {
    /// File size in bytes.
    pub fn size_bytes(&self) -> Option<u64> {
        self.size
    }

    /// Whether this is an SRA (not vdbcache or other) file.
    pub fn is_sra(&self) -> bool {
        self.file_type == "sra"
    }
}

/// A specific download location/mirror for a file.
#[derive(Debug, Deserialize)]
pub struct SdlLocation {
    /// Direct download URL.
    pub link: String,
    /// Service name: "ncbi", "s3", "gs".
    pub service: Option<String>,
    /// Geographic region: "be-md", "us-east-1", etc.
    pub region: Option<String>,
    /// URL expiration date (for cloud presigned URLs).
    #[serde(rename = "expirationDate")]
    pub expiration_date: Option<String>,
    /// Whether Compute Environment token is required.
    #[serde(default, rename = "ceRequired")]
    pub ce_required: bool,
    /// Whether payment is required.
    #[serde(default, rename = "payRequired")]
    pub pay_required: bool,
}

// ---------------------------------------------------------------------------
// Flexible deserializers (SDL returns integers but some fields could be strings)
// ---------------------------------------------------------------------------

/// Deserialize a status field that may be an integer or a string.
fn deserialize_status<'de, D>(deserializer: D) -> std::result::Result<Option<i64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let v = Option::<Value>::deserialize(deserializer)?;
    match v {
        None => Ok(None),
        Some(Value::Number(n)) => Ok(n.as_i64()),
        Some(Value::String(s)) => Ok(s.parse().ok()),
        _ => Ok(None),
    }
}

/// Deserialize a size field that may be an integer or a string.
fn deserialize_size<'de, D>(deserializer: D) -> std::result::Result<Option<u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let v = Option::<Value>::deserialize(deserializer)?;
    match v {
        None => Ok(None),
        Some(Value::Number(n)) => Ok(n.as_u64()),
        Some(Value::String(s)) => Ok(s.parse().ok()),
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_full_response() {
        let json = r#"{
            "version": "2",
            "status": 200,
            "result": [{
                "query": "SRR000001",
                "status": 200,
                "files": [{
                    "type": "sra",
                    "name": "SRR000001",
                    "size": 5747798,
                    "md5": "abc123def456",
                    "noqual": false,
                    "locations": [{
                        "link": "https://example.com/SRR000001",
                        "service": "s3",
                        "region": "us-east-1",
                        "ceRequired": false,
                        "payRequired": false
                    }]
                }]
            }]
        }"#;
        let resp: SdlResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.version.as_deref(), Some("2"));
        assert_eq!(resp.status, Some(200));
        assert_eq!(resp.results.len(), 1);
        let result = &resp.results[0];
        assert_eq!(result.query.as_deref(), Some("SRR000001"));
        assert!(result.is_ok());
        let sra = result.find_sra_file().unwrap();
        assert_eq!(sra.size, Some(5747798));
        assert_eq!(sra.md5.as_deref(), Some("abc123def456"));
        assert!(!sra.noqual);
        assert_eq!(sra.locations.len(), 1);
        assert_eq!(sra.locations[0].service.as_deref(), Some("s3"));
    }

    #[test]
    fn parse_response_with_bundle_field() {
        let json = r#"{
            "result": [{
                "bundle": "SRR000001",
                "status": 200,
                "files": []
            }]
        }"#;
        let resp: SdlResponse = serde_json::from_str(json).unwrap();
        let result = resp.find_result("SRR000001").unwrap();
        assert_eq!(result.accession(), "SRR000001");
    }

    #[test]
    fn parse_response_with_msg_field() {
        let json = r#"{
            "result": [{
                "query": "SRR999999",
                "status": 404,
                "msg": "not found"
            }]
        }"#;
        let resp: SdlResponse = serde_json::from_str(json).unwrap();
        let result = &resp.results[0];
        assert_eq!(result.message.as_deref(), Some("not found"));
        assert!(!result.is_ok());
    }

    #[test]
    fn parse_status_as_string() {
        let json = r#"{
            "status": "200",
            "result": [{ "query": "SRR000001", "status": "200", "files": [] }]
        }"#;
        let resp: SdlResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.status, Some(200));
        assert!(resp.results[0].is_ok());
    }

    #[test]
    fn parse_size_as_string() {
        let json = r#"{
            "result": [{
                "query": "SRR000001",
                "files": [{ "type": "sra", "size": "12345", "locations": [] }]
            }]
        }"#;
        let resp: SdlResponse = serde_json::from_str(json).unwrap();
        let sra = resp.results[0].find_sra_file().unwrap();
        assert_eq!(sra.size, Some(12345));
    }

    #[test]
    fn parse_noqual_lite() {
        let json = r#"{
            "result": [{
                "query": "SRR000001",
                "files": [{ "type": "sra", "noqual": true, "locations": [] }]
            }]
        }"#;
        let resp: SdlResponse = serde_json::from_str(json).unwrap();
        let sra = resp.results[0].find_sra_file().unwrap();
        assert!(sra.noqual);
        assert!(sra.is_sra());
    }

    #[test]
    fn find_result_by_accession() {
        let json = r#"{
            "result": [
                { "query": "SRR000001", "files": [] },
                { "query": "SRR000002", "files": [] }
            ]
        }"#;
        let resp: SdlResponse = serde_json::from_str(json).unwrap();
        assert!(resp.find_result("SRR000001").is_some());
        assert!(resp.find_result("SRR000002").is_some());
        assert!(resp.find_result("SRR999999").is_none());
    }

    #[test]
    fn find_sra_and_vdbcache_files() {
        let json = r#"{
            "result": [{
                "query": "SRR000001",
                "files": [
                    { "type": "sra", "locations": [] },
                    { "type": "vdbcache", "locations": [] }
                ]
            }]
        }"#;
        let resp: SdlResponse = serde_json::from_str(json).unwrap();
        let result = &resp.results[0];
        assert!(result.find_sra_file().is_some());
        assert!(result.find_vdbcache_file().is_some());
    }

    #[test]
    fn parse_empty_results() {
        let json = r#"{ "result": [] }"#;
        let resp: SdlResponse = serde_json::from_str(json).unwrap();
        assert!(resp.results.is_empty());
        assert!(resp.find_result("SRR000001").is_none());
    }

    #[test]
    fn parse_missing_optional_fields() {
        let json = r#"{
            "result": [{
                "query": "SRR000001",
                "files": [{ "type": "sra", "locations": [] }]
            }]
        }"#;
        let resp: SdlResponse = serde_json::from_str(json).unwrap();
        assert!(resp.version.is_none());
        assert!(resp.status.is_none());
        assert!(resp.message.is_none());
        let sra = resp.results[0].find_sra_file().unwrap();
        assert!(sra.size.is_none());
        assert!(sra.md5.is_none());
        assert!(sra.name.is_none());
        assert!(!sra.noqual);
    }
}
