//! Descriptive metadata from NCBI EFetch's SRA experiment-package XML.
//!
//! The RunInfo CSV carries the technical fields (layout, platform, spot
//! counts) but none of the human-readable ones: the experiment/sample titles,
//! the study title, the design description. Those only exist in the
//! `db=sra` XML, so `sracha info` fetches it in one batched request and
//! parses out the handful of fields worth showing.
//!
//! Everything here is best-effort: a failed or slow fetch yields no
//! descriptive fields and never fails the command.

use std::collections::HashMap;

use super::{EFETCH_BATCH_SIZE, EUTILS_EFETCH_URL, send_with_retry};

/// Packages carry a submitter block and sample attributes, so they are far
/// bigger than a RunInfo CSV row; keep each response modest.
const XML_BATCH_SIZE: usize = EFETCH_BATCH_SIZE / 2;

/// Overall cap on the descriptive fetch so it can never stall `info`.
const FETCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Descriptive fields for one run's experiment package. Absent elements are
/// `None`; not every run has a sample title, and some have neither title.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExperimentInfo {
    /// `EXPERIMENT/TITLE`.
    pub experiment_title: Option<String>,
    /// `SAMPLE/TITLE`.
    pub sample_title: Option<String>,
    /// `STUDY/DESCRIPTOR/STUDY_TITLE`.
    pub study_title: Option<String>,
    /// `EXPERIMENT/DESIGN/DESIGN_DESCRIPTION`.
    pub design_description: Option<String>,
    /// `LIBRARY_DESCRIPTOR/LIBRARY_CONSTRUCTION_PROTOCOL`.
    pub library_construction_protocol: Option<String>,
}

impl ExperimentInfo {
    /// The best single human-readable name: the sample title when there is
    /// one, otherwise the experiment title.
    pub fn title(&self) -> Option<&str> {
        self.sample_title
            .as_deref()
            .or(self.experiment_title.as_deref())
    }
}

/// Parse an EFetch `db=sra` XML document into per-run [`ExperimentInfo`],
/// keyed by run accession. A package listing several runs maps each of them
/// to the same info.
pub fn parse_experiment_packages(xml: &str) -> Result<HashMap<String, ExperimentInfo>, String> {
    let doc = roxmltree::Document::parse(xml).map_err(|e| format!("invalid EFetch XML: {e}"))?;
    let mut out = HashMap::new();

    for pkg in doc
        .descendants()
        .filter(|n| n.has_tag_name("EXPERIMENT_PACKAGE"))
    {
        let experiment = child(pkg, "EXPERIMENT");
        let design = experiment.and_then(|e| child(e, "DESIGN"));
        let info = ExperimentInfo {
            experiment_title: experiment.and_then(|e| text_of(e, &["TITLE"])),
            sample_title: child(pkg, "SAMPLE").and_then(|s| text_of(s, &["TITLE"])),
            study_title: child(pkg, "STUDY")
                .and_then(|s| text_of(s, &["DESCRIPTOR", "STUDY_TITLE"])),
            design_description: design.and_then(|d| text_of(d, &["DESIGN_DESCRIPTION"])),
            library_construction_protocol: design
                .and_then(|d| text_of(d, &["LIBRARY_DESCRIPTOR", "LIBRARY_CONSTRUCTION_PROTOCOL"])),
        };

        if let Some(run_set) = child(pkg, "RUN_SET") {
            for run in run_set.children().filter(|n| n.has_tag_name("RUN")) {
                if let Some(acc) = run.attribute("accession") {
                    out.insert(acc.to_string(), info.clone());
                }
            }
        }
    }
    Ok(out)
}

fn child<'a, 'i>(node: roxmltree::Node<'a, 'i>, name: &str) -> Option<roxmltree::Node<'a, 'i>> {
    node.children().find(|n| n.has_tag_name(name))
}

/// Text at `path` below `node`, with whitespace runs (including embedded
/// newlines) collapsed to single spaces. Empty text is `None`.
fn text_of(node: roxmltree::Node<'_, '_>, path: &[&str]) -> Option<String> {
    let mut cur = node;
    for name in path {
        cur = child(cur, name)?;
    }
    let text = cur.text()?.split_whitespace().collect::<Vec<_>>().join(" ");
    (!text.is_empty()).then_some(text)
}

/// Fetch descriptive metadata for `runs` with batched EFetch XML requests.
///
/// Never fails: a request, status or parse error is logged at debug level
/// and the affected runs are simply absent from the result.
pub async fn fetch_experiment_info(
    client: &reqwest::Client,
    runs: &[String],
) -> HashMap<String, ExperimentInfo> {
    fetch_experiment_info_from(EUTILS_EFETCH_URL, client, runs).await
}

async fn fetch_experiment_info_from(
    url: &str,
    client: &reqwest::Client,
    runs: &[String],
) -> HashMap<String, ExperimentInfo> {
    let work = async {
        let mut out = HashMap::new();
        for chunk in runs.chunks(XML_BATCH_SIZE) {
            let ids = chunk.join(",");
            let form = [("db", "sra"), ("id", ids.as_str()), ("retmode", "xml")];
            let resp =
                send_with_retry("EUtils EFetch XML (POST)", || client.post(url).form(&form)).await;
            let body = match resp {
                Ok(r) if r.status().is_success() => r.text().await.ok(),
                Ok(r) => {
                    tracing::debug!("EFetch XML returned HTTP {}", r.status());
                    None
                }
                Err(e) => {
                    tracing::debug!("EFetch XML request failed: {e}");
                    None
                }
            };
            let Some(body) = body else { continue };
            match parse_experiment_packages(&body) {
                Ok(map) => out.extend(map),
                Err(e) => tracing::debug!("{e}"),
            }
        }
        out
    };
    match tokio::time::timeout(FETCH_TIMEOUT, work).await {
        Ok(out) => out,
        Err(_) => {
            tracing::debug!("EFetch XML timed out after {FETCH_TIMEOUT:?}");
            HashMap::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURE: &str = include_str!("testdata/efetch_packages.xml");

    #[test]
    fn experiment_title_only_is_the_fallback_title() {
        let map = parse_experiment_packages(FIXTURE).unwrap();
        let info = &map["SRR18918001"];
        assert_eq!(info.sample_title, None);
        assert_eq!(
            info.experiment_title.as_deref(),
            Some("scATAC-seq of Drosophila melanogaster: third-instar larval third leg disc")
        );
        assert_eq!(info.title(), info.experiment_title.as_deref());
        assert_eq!(
            info.design_description.as_deref(),
            Some("10X Genomics scATAC-seq v.1")
        );
        assert!(
            info.study_title
                .as_deref()
                .unwrap()
                .starts_with("Single-Cell Atlas")
        );
    }

    #[test]
    fn sample_title_wins_and_whitespace_collapses() {
        let map = parse_experiment_packages(FIXTURE).unwrap();
        let info = &map["SRR000002"];
        assert_eq!(info.sample_title.as_deref(), Some("HeLa rep 1"));
        assert_eq!(
            info.experiment_title.as_deref(),
            Some("RNA-seq of HeLa cells")
        );
        assert_eq!(info.title(), Some("HeLa rep 1"));
        assert_eq!(
            info.library_construction_protocol.as_deref(),
            Some("Poly-A selected, stranded.")
        );
    }

    #[test]
    fn multi_run_package_maps_every_run() {
        let map = parse_experiment_packages(FIXTURE).unwrap();
        assert_eq!(map["SRR000002"], map["SRR000003"]);
    }

    #[test]
    fn package_with_no_titles_yields_none() {
        let map = parse_experiment_packages(FIXTURE).unwrap();
        let info = &map["SRR000004"];
        assert_eq!(info.title(), None);
        assert_eq!(*info, ExperimentInfo::default());
    }

    #[test]
    fn malformed_xml_is_an_error_not_a_panic() {
        assert!(parse_experiment_packages("<EXPERIMENT_PACKAGE_SET><oops").is_err());
    }

    #[tokio::test]
    async fn empty_run_list_makes_no_request() {
        let client = reqwest::Client::new();
        assert!(fetch_experiment_info(&client, &[]).await.is_empty());
    }

    #[tokio::test]
    async fn runs_are_fetched_in_batches() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/efetch.fcgi"))
            .respond_with(ResponseTemplate::new(200).set_body_string(FIXTURE))
            .expect(2)
            .mount(&server)
            .await;

        // One more run than fits in a single request forces a second POST.
        let runs: Vec<String> = (0..=XML_BATCH_SIZE).map(|i| format!("SRR{i}")).collect();
        let url = format!("{}/efetch.fcgi", server.uri());
        let out = fetch_experiment_info_from(&url, &reqwest::Client::new(), &runs).await;
        assert!(out["SRR18918001"].title().is_some());
        // `expect(2)` is verified when the server drops.
    }

    #[tokio::test]
    async fn http_error_and_garbage_bodies_are_non_fatal() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let bad = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(403))
            .mount(&bad)
            .await;
        let garbage = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_string("not xml <"))
            .mount(&garbage)
            .await;

        let runs = vec!["SRR1".to_string()];
        let client = reqwest::Client::new();
        for server in [&bad, &garbage] {
            let out = fetch_experiment_info_from(&server.uri(), &client, &runs).await;
            assert!(out.is_empty());
        }
    }
}
