//! Run-metadata sidecar writer for `sracha get`.
//!
//! Emits a per-run TSV/JSON sidecar with BioSample, library, instrument and
//! sequencing metrics resolved from NCBI EUtils RunInfo plus the download
//! mirror chosen at fetch time. Intended for downstream pipelines that need
//! to link FASTQ files to sample-level metadata without re-querying NCBI.

use std::io::Write;
use std::path::Path;

use serde::Serialize;

use crate::delimited::{join_list, write_row};
use crate::error::Result;
use crate::sdl::ResolvedAccession;

/// Sidecar output format selected by the `--metadata` CLI flag.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetadataFormat {
    /// Tab-separated values (one header row + one row per run).
    Tsv,
    /// JSON array of run-metadata objects, pretty-printed.
    Json,
    /// Both `.metadata.tsv` and `.metadata.json` side by side.
    Both,
}

/// One row of run metadata — the full set of fields written to the sidecar.
#[derive(Clone, Debug, Default, Serialize)]
pub struct RunMetadata {
    pub accession: String,
    pub url: Option<String>,
    pub size_bytes: Option<u64>,
    pub md5: Option<String>,
    pub source_service: Option<String>,
    pub spots: Option<u64>,
    pub spot_len: Option<u32>,
    pub nreads: Option<usize>,
    pub avg_read_len: Vec<u32>,
    pub platform: Option<String>,
    pub instrument_model: Option<String>,
    pub library_strategy: Option<String>,
    pub library_source: Option<String>,
    pub library_selection: Option<String>,
    pub library_layout: Option<String>,
    pub library_name: Option<String>,
    pub experiment: Option<String>,
    pub study: Option<String>,
    pub bioproject: Option<String>,
    pub sample: Option<String>,
    pub biosample: Option<String>,
    pub scientific_name: Option<String>,
    pub tax_id: Option<u32>,
    pub bases: Option<u64>,
    pub size_mb: Option<u64>,
    pub release_date: Option<String>,
    pub load_date: Option<String>,
}

impl RunMetadata {
    /// Build a `RunMetadata` row from a [`ResolvedAccession`], copying the
    /// primary mirror's URL/service plus everything we know from EUtils
    /// RunInfo. Missing fields stay `None`.
    pub fn from_resolved(r: &ResolvedAccession) -> Self {
        let primary = r.sra_file.preferred_mirror();
        let ri = r.run_info.as_ref();
        Self {
            accession: r.accession.clone(),
            url: primary.map(|m| m.url.clone()),
            size_bytes: Some(r.sra_file.size),
            md5: r.sra_file.md5.clone(),
            source_service: primary.map(|m| m.service.clone()),
            spots: ri.and_then(|x| x.spots),
            spot_len: ri.map(|x| x.spot_len),
            nreads: ri.map(|x| x.nreads),
            avg_read_len: ri.map(|x| x.avg_read_len.clone()).unwrap_or_default(),
            platform: ri.and_then(|x| x.platform.clone()),
            instrument_model: ri.and_then(|x| x.instrument_model.clone()),
            library_strategy: ri.and_then(|x| x.library_strategy.clone()),
            library_source: ri.and_then(|x| x.library_source.clone()),
            library_selection: ri.and_then(|x| x.library_selection.clone()),
            library_layout: ri.and_then(|x| x.library_layout.clone()),
            library_name: ri.and_then(|x| x.library_name.clone()),
            experiment: ri.and_then(|x| x.experiment.clone()),
            study: ri.and_then(|x| x.study.clone()),
            bioproject: ri.and_then(|x| x.bioproject.clone()),
            sample: ri.and_then(|x| x.sample.clone()),
            biosample: ri.and_then(|x| x.biosample.clone()),
            scientific_name: ri.and_then(|x| x.scientific_name.clone()),
            tax_id: ri.and_then(|x| x.tax_id),
            bases: ri.and_then(|x| x.bases),
            size_mb: ri.and_then(|x| x.size_mb),
            release_date: ri.and_then(|x| x.release_date.clone()),
            load_date: ri.and_then(|x| x.load_date.clone()),
        }
    }
}

/// Sidecar column names, in output order. [`RunMetadata::cells`] yields values
/// in this same order.
pub const COLUMNS: [&str; 27] = [
    "accession",
    "url",
    "size_bytes",
    "md5",
    "source_service",
    "spots",
    "spot_len",
    "nreads",
    "avg_read_len",
    "platform",
    "instrument_model",
    "library_strategy",
    "library_source",
    "library_selection",
    "library_layout",
    "library_name",
    "experiment",
    "study",
    "bioproject",
    "sample",
    "biosample",
    "scientific_name",
    "tax_id",
    "bases",
    "size_mb",
    "release_date",
    "load_date",
];

fn opt_str(v: &Option<String>) -> String {
    v.clone().unwrap_or_default()
}

fn opt_num<T: std::fmt::Display>(v: &Option<T>) -> String {
    v.as_ref().map(|x| x.to_string()).unwrap_or_default()
}

impl RunMetadata {
    /// Every column rendered as text, in [`COLUMNS`] order. The single
    /// source for the TSV sidecar and the `--dry-run` table. List-valued
    /// columns use [`crate::delimited::LIST_SEP`].
    pub fn cells(&self) -> [String; 27] {
        [
            self.accession.clone(),
            opt_str(&self.url),
            opt_num(&self.size_bytes),
            opt_str(&self.md5),
            opt_str(&self.source_service),
            opt_num(&self.spots),
            opt_num(&self.spot_len),
            opt_num(&self.nreads),
            join_list(&self.avg_read_len),
            opt_str(&self.platform),
            opt_str(&self.instrument_model),
            opt_str(&self.library_strategy),
            opt_str(&self.library_source),
            opt_str(&self.library_selection),
            opt_str(&self.library_layout),
            opt_str(&self.library_name),
            opt_str(&self.experiment),
            opt_str(&self.study),
            opt_str(&self.bioproject),
            opt_str(&self.sample),
            opt_str(&self.biosample),
            opt_str(&self.scientific_name),
            opt_num(&self.tax_id),
            opt_num(&self.bases),
            opt_num(&self.size_mb),
            opt_str(&self.release_date),
            opt_str(&self.load_date),
        ]
    }
}

/// Write a TSV sidecar to `path`, overwriting if present.
pub fn write_tsv(path: &Path, rows: &[RunMetadata]) -> Result<()> {
    let mut f = std::io::BufWriter::new(std::fs::File::create(path)?);
    write_row(&mut f, &COLUMNS, b'\t')?;
    for r in rows {
        let cells = r.cells();
        let refs: Vec<&str> = cells.iter().map(String::as_str).collect();
        write_row(&mut f, &refs, b'\t')?;
    }
    f.flush()?;
    Ok(())
}

/// Write a JSON sidecar to `path` (array of objects, pretty-printed),
/// overwriting if present.
pub fn write_json(path: &Path, rows: &[RunMetadata]) -> Result<()> {
    let f = std::fs::File::create(path)?;
    serde_json::to_writer_pretty(f, rows)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sdl::{ResolvedFile, ResolvedMirror, RunInfo};

    fn sample_resolved() -> ResolvedAccession {
        ResolvedAccession {
            accession: "SRR111222".into(),
            sra_file: ResolvedFile {
                mirrors: vec![ResolvedMirror {
                    url: "https://s3.example.com/SRR111222".into(),
                    service: "s3".into(),
                }],
                size: 9_876_543,
                md5: Some("deadbeef".into()),
                is_lite: false,
            },
            vdbcache_file: None,
            run_info: Some(RunInfo {
                nreads: 2,
                avg_read_len: vec![151, 151],
                spot_len: 302,
                platform: Some("ILLUMINA".into()),
                spots: Some(1_000),
                library_strategy: Some("RNA-Seq".into()),
                library_source: Some("TRANSCRIPTOMIC".into()),
                library_selection: Some("cDNA".into()),
                library_layout: Some("PAIRED".into()),
                library_name: Some("lib_X".into()),
                instrument_model: Some("Illumina NovaSeq 6000".into()),
                experiment: Some("SRX111".into()),
                study: Some("SRP222".into()),
                bioproject: Some("PRJNA333".into()),
                sample: Some("SRS444".into()),
                biosample: Some("SAMN555".into()),
                scientific_name: Some("Homo sapiens".into()),
                tax_id: Some(9606),
                bases: Some(302_000),
                size_mb: Some(10),
                release_date: Some("2024-01-01".into()),
                load_date: Some("2024-01-02".into()),
            }),
        }
    }

    #[test]
    fn from_resolved_copies_all_fields() {
        let r = sample_resolved();
        let m = RunMetadata::from_resolved(&r);
        assert_eq!(m.accession, "SRR111222");
        assert_eq!(m.url.as_deref(), Some("https://s3.example.com/SRR111222"));
        assert_eq!(m.size_bytes, Some(9_876_543));
        assert_eq!(m.md5.as_deref(), Some("deadbeef"));
        assert_eq!(m.source_service.as_deref(), Some("s3"));
        assert_eq!(m.spots, Some(1_000));
        assert_eq!(m.spot_len, Some(302));
        assert_eq!(m.nreads, Some(2));
        assert_eq!(m.avg_read_len, vec![151, 151]);
        assert_eq!(m.platform.as_deref(), Some("ILLUMINA"));
        assert_eq!(m.instrument_model.as_deref(), Some("Illumina NovaSeq 6000"));
        assert_eq!(m.library_strategy.as_deref(), Some("RNA-Seq"));
        assert_eq!(m.library_source.as_deref(), Some("TRANSCRIPTOMIC"));
        assert_eq!(m.library_selection.as_deref(), Some("cDNA"));
        assert_eq!(m.library_layout.as_deref(), Some("PAIRED"));
        assert_eq!(m.library_name.as_deref(), Some("lib_X"));
        assert_eq!(m.experiment.as_deref(), Some("SRX111"));
        assert_eq!(m.study.as_deref(), Some("SRP222"));
        assert_eq!(m.bioproject.as_deref(), Some("PRJNA333"));
        assert_eq!(m.sample.as_deref(), Some("SRS444"));
        assert_eq!(m.biosample.as_deref(), Some("SAMN555"));
        assert_eq!(m.scientific_name.as_deref(), Some("Homo sapiens"));
        assert_eq!(m.tax_id, Some(9606));
        assert_eq!(m.bases, Some(302_000));
        assert_eq!(m.size_mb, Some(10));
        assert_eq!(m.release_date.as_deref(), Some("2024-01-01"));
        assert_eq!(m.load_date.as_deref(), Some("2024-01-02"));
    }

    #[test]
    fn from_resolved_without_run_info_leaves_optional_fields_empty() {
        let mut r = sample_resolved();
        r.run_info = None;
        let m = RunMetadata::from_resolved(&r);
        // URL + size still come from the SDL response.
        assert_eq!(m.url.as_deref(), Some("https://s3.example.com/SRR111222"));
        assert_eq!(m.size_bytes, Some(9_876_543));
        // RunInfo-derived fields default to None / empty.
        assert!(m.spots.is_none());
        assert!(m.platform.is_none());
        assert!(m.biosample.is_none());
        assert!(m.avg_read_len.is_empty());
    }

    #[test]
    fn write_tsv_emits_header_and_row() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("SRR111222.metadata.tsv");
        let row = RunMetadata::from_resolved(&sample_resolved());
        write_tsv(&path, &[row]).unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        let mut lines = content.lines();
        assert_eq!(lines.next(), Some(COLUMNS.join("\t").as_str()));
        let data = lines.next().expect("data row present");
        assert!(lines.next().is_none(), "exactly one data row");

        // Spot-check selected columns appear in the expected order.
        let cols: Vec<&str> = data.split('\t').collect();
        assert_eq!(cols.len(), 27, "27 columns per row");
        assert_eq!(cols[0], "SRR111222");
        assert_eq!(cols[1], "https://s3.example.com/SRR111222");
        assert_eq!(cols[2], "9876543");
        assert_eq!(cols[3], "deadbeef");
        assert_eq!(cols[4], "s3");
        assert_eq!(cols[8], "151;151"); // avg_read_len joined by `;`
        assert_eq!(cols[9], "ILLUMINA");
        assert_eq!(cols[20], "SAMN555");
    }

    #[test]
    fn sidecar_row_matches_from_resolved() {
        // Exercises the production path: the row `from_resolved` builds is
        // exactly what lands in the sidecar, free text escaped.
        let mut r = sample_resolved();
        r.run_info.as_mut().unwrap().library_name = Some("lib\tA\nB".into());
        let row = RunMetadata::from_resolved(&r);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("SRR111222.metadata.tsv");
        write_tsv(&path, std::slice::from_ref(&row)).unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(
            lines.len(),
            2,
            "tabs/newlines in free text must not split rows"
        );
        let cols: Vec<&str> = lines[1].split('\t').collect();
        assert_eq!(cols.len(), COLUMNS.len());
        for (i, name) in COLUMNS.iter().enumerate() {
            assert_eq!(cols[i], row.cells()[i].replace(['\t', '\n'], " "), "{name}");
        }
        let col = |n: &str| cols[COLUMNS.iter().position(|c| *c == n).unwrap()];
        assert_eq!(col("library_name"), "lib A B");
        assert_eq!(col("url"), "https://s3.example.com/SRR111222");
        assert_eq!(col("source_service"), "s3");
        assert_eq!(col("md5"), "deadbeef");
        assert_eq!(col("size_bytes"), "9876543");
        assert_eq!(col("load_date"), "2024-01-02");
    }

    #[test]
    fn write_json_emits_array_of_one_object() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("SRR111222.metadata.json");
        let row = RunMetadata::from_resolved(&sample_resolved());
        write_json(&path, &[row]).unwrap();

        let raw = std::fs::read_to_string(&path).unwrap();
        let value: serde_json::Value = serde_json::from_str(&raw).unwrap();
        let array = value.as_array().expect("array at the top level");
        assert_eq!(array.len(), 1);
        let obj = array[0].as_object().expect("first element is object");
        assert_eq!(obj["accession"], "SRR111222");
        assert_eq!(obj["biosample"], "SAMN555");
        assert_eq!(obj["tax_id"], 9606);
        assert!(obj.contains_key("library_strategy"));
        assert!(obj.contains_key("avg_read_len"));
    }
}
