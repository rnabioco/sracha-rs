//! Pipeline-friendly output for `sracha info`.
//!
//! Every renderer (table, per-accession detail, TSV/CSV) consumes the same
//! [`InfoRecord`], built either from a resolved remote run or from a local
//! archive, so the formats cannot drift apart. The human-readable table view
//! (with styling and the `tabled` crate) stays in the CLI binary.

use std::io::Write;
use std::path::Path;

use crate::sdl::{ExperimentInfo, ResolvedAccession};

/// Everything `sracha info` knows about one accession, independent of how it
/// is rendered or where it came from.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct InfoRecord {
    pub accession: String,
    /// Local file path; `None` for remote runs.
    pub path: Option<String>,
    /// `"SRA"` or `"cSRA"`. One meaning for both origins — reference-compressed
    /// or not — detected differently: remote runs by the presence of a
    /// `.vdbcache` sidecar (legacy monolithic cSRA reads as `"SRA"`), local
    /// archives by opening them.
    pub archive_type: &'static str,
    /// Biological read count (`nreads` column).
    pub nreads: Option<usize>,
    /// Per-read lengths, in read order. Includes technical reads when the
    /// source knows them (the archive's own metadata).
    pub read_lengths: Vec<u32>,
    /// `true` when `read_lengths` is a guess from NCBI RunInfo's average
    /// length rather than the archive's stored structure.
    pub lengths_estimated: bool,
    /// `true` = biological, per entry of `read_lengths`. Empty when unknown.
    pub biological: Vec<bool>,
    pub spots: Option<u64>,
    pub size_bytes: Option<u64>,
    pub platform: Option<String>,
    pub md5: Option<String>,
    /// Preferred download mirror service (remote only).
    pub source: Option<String>,
    /// Preferred download mirror URL (remote only).
    pub source_url: Option<String>,
    /// Services of the other mirrors, best first (remote only).
    pub alt_mirrors: Vec<String>,
    /// Total bases: NCBI's reported count when present, else an estimate
    /// (`bases_estimated`) from spots x spot length.
    pub bases: Option<u64>,
    pub bases_estimated: bool,
    /// Size of the `.vdbcache` sidecar, when the run has one (remote only).
    pub vdbcache_size: Option<u64>,
    /// SRA-lite (simplified quality) rather than normalized (remote only).
    pub is_lite: bool,
    /// Local archive details.
    pub blobs: Option<usize>,
    pub columns: Vec<String>,
    /// Descriptive fields from EFetch XML (remote runs only; best effort).
    pub experiment: Option<ExperimentInfo>,
}

impl InfoRecord {
    /// Build a record from a resolved remote run.
    pub fn from_resolved(r: &ResolvedAccession) -> Self {
        let ri = r.run_info.as_ref();
        let preferred = r.sra_file.preferred_mirror();
        // NCBI's own base count when it reported one; otherwise spots x the
        // average spot length, flagged as an estimate.
        let (bases, bases_estimated) = match ri {
            Some(ri) => match (ri.bases, ri.spots) {
                (Some(b), _) => (Some(b), false),
                (None, Some(s)) if ri.spot_len > 0 => (Some(s * u64::from(ri.spot_len)), true),
                _ => (None, false),
            },
            None => (None, false),
        };
        Self {
            accession: r.accession.clone(),
            path: None,
            archive_type: if r.vdbcache_file.is_some() {
                "cSRA"
            } else {
                "SRA"
            },
            nreads: ri.map(|ri| ri.nreads),
            read_lengths: ri.map(|ri| ri.avg_read_len.clone()).unwrap_or_default(),
            lengths_estimated: ri.is_some_and(|ri| !ri.avg_read_len.is_empty()),
            biological: Vec::new(),
            spots: ri.and_then(|ri| ri.spots),
            size_bytes: Some(r.sra_file.size),
            platform: ri.and_then(|ri| ri.platform.clone()),
            md5: r.sra_file.md5.clone(),
            source: preferred.map(|m| m.service.clone()),
            source_url: preferred.map(|m| m.url.clone()),
            alt_mirrors: r
                .sra_file
                .alternate_mirrors()
                .iter()
                .map(|m| m.service.clone())
                .collect(),
            bases,
            bases_estimated,
            vdbcache_size: r.vdbcache_file.as_ref().map(|f| f.size),
            is_lite: r.sra_file.is_lite,
            blobs: None,
            columns: Vec::new(),
            experiment: None,
        }
    }

    /// Build a record by opening a local `.sra` archive.
    pub fn from_local_archive(path: &Path) -> Result<Self, String> {
        use crate::vdb::cursor::VdbCursor;
        use crate::vdb::kar::KarArchive;

        let accession = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown")
            .to_string();
        let size_bytes = std::fs::metadata(path).map(|m| m.len()).ok();

        let file = std::fs::File::open(path).map_err(|e| format!("open failed: {e}"))?;
        let mut archive = KarArchive::open(std::io::BufReader::new(file))
            .map_err(|e| format!("KAR parse failed: {e}"))?;

        let mut rec = InfoRecord {
            accession,
            path: Some(path.display().to_string()),
            archive_type: "SRA",
            size_bytes,
            ..Default::default()
        };

        // cSRA archives don't open as a plain VdbCursor (no physical READ
        // column); describe them via the cSRA cursor instead. A sibling
        // `.sra.vdbcache` is honoured so split-archive cSRA is recognised.
        let vdbcache = crate::vdb::csra::vdbcache_sidecar_path(path);
        let vdbcache_opt = vdbcache.exists().then_some(vdbcache.as_path());
        if crate::vdb::csra::looks_like_decodable_csra(path, vdbcache_opt).unwrap_or(false) {
            let mut cache_archive = match vdbcache_opt {
                Some(p) => {
                    let f =
                        std::fs::File::open(p).map_err(|e| format!("vdbcache open failed: {e}"))?;
                    Some(
                        KarArchive::open(std::io::BufReader::new(f))
                            .map_err(|e| format!("vdbcache parse failed: {e}"))?,
                    )
                }
                None => None,
            };
            let csra = crate::vdb::csra::CsraCursor::open_any(
                &mut archive,
                path,
                cache_archive.as_mut().zip(vdbcache_opt),
            )
            .map_err(|e| format!("cSRA cursor failed: {e}"))?;
            rec.archive_type = "cSRA";
            rec.platform = Some("reference-compressed cSRA".into());
            rec.spots = Some(csra.row_count());
            rec.columns = [
                "CMP_READ",
                "PRIMARY_ALIGNMENT_ID",
                "READ_LEN",
                "READ_TYPE",
                "QUALITY",
            ]
            .map(String::from)
            .to_vec();
            return Ok(rec);
        }

        let cursor =
            VdbCursor::open(&mut archive, path).map_err(|e| format!("cursor open failed: {e}"))?;
        rec.platform = Some(cursor.platform().unwrap_or("unknown").to_string());
        rec.blobs = Some(cursor.read_col().blob_count());
        rec.spots = Some(
            cursor
                .read_col()
                .blobs()
                .iter()
                .map(|b| b.id_range as u64)
                .sum(),
        );
        rec.read_lengths = cursor.metadata_read_lengths().unwrap_or_default();
        // INSDC xread_type: 1 = biological, anything else technical.
        if let Some(types) = cursor.metadata_read_types()
            && types.len() == rec.read_lengths.len()
        {
            rec.biological = types.iter().map(|&t| t == 1).collect();
        }
        rec.nreads = Some(rec.biological_count());

        rec.columns.push("READ".into());
        if cursor.quality_col().is_some() {
            rec.columns.push("QUALITY".into());
        }
        if cursor.read_len_col().is_some() {
            rec.columns.push("READ_LEN".into());
        }
        if cursor.name_col().is_some() {
            rec.columns.push("NAME".into());
        }
        Ok(rec)
    }

    /// Human-readable name: the sample title, else the experiment title.
    pub fn title(&self) -> Option<&str> {
        self.experiment.as_ref().and_then(|e| e.title())
    }

    /// Replace the estimated read lengths with the archive's real structure.
    pub fn apply_read_structure(&mut self, rs: ReadStructure) {
        self.read_lengths = rs.lengths;
        self.biological = rs.biological;
        self.lengths_estimated = false;
        self.nreads = Some(self.biological_count());
    }

    /// Number of biological reads; every read when types are unknown.
    fn biological_count(&self) -> usize {
        if self.biological.is_empty() {
            self.read_lengths.iter().filter(|&&l| l > 0).count()
        } else {
            self.biological
                .iter()
                .zip(&self.read_lengths)
                .filter(|&(&b, &l)| b && l > 0)
                .count()
        }
    }

    /// `SINGLE` / `PAIRED` / `N-read`, or empty when the read count is unknown.
    pub fn layout(&self) -> String {
        match self.nreads {
            None | Some(0) => String::new(),
            Some(1) => "SINGLE".into(),
            Some(2) => "PAIRED".into(),
            Some(n) => format!("{n}-read"),
        }
    }

    /// Human-readable read structure: `2×154bp`, `61bp+62bp`, or
    /// `8bp(T)+50bp+16bp(T)+49bp` when technical reads are known. `-` when no
    /// lengths are available.
    pub fn reads_label(&self) -> String {
        let l = &self.read_lengths;
        if l.is_empty() {
            return "-".into();
        }
        if self.lengths_estimated {
            return format!("~{}", self.reads_label_exact());
        }
        self.reads_label_exact()
    }

    fn reads_label_exact(&self) -> String {
        let l = &self.read_lengths;
        let has_tech = self.biological.iter().any(|&b| !b);
        if !has_tech && l.iter().all(|&x| x == l[0]) {
            return format!("{}×{}bp", l.len(), l[0]);
        }
        l.iter()
            .enumerate()
            .map(|(i, len)| {
                if self.biological.get(i) == Some(&false) {
                    format!("{len}bp(T)")
                } else {
                    format!("{len}bp")
                }
            })
            .collect::<Vec<_>>()
            .join("+")
    }

    /// `;`-joined read lengths for the `read_lengths` column.
    fn read_lengths_field(&self) -> String {
        join_semi(self.read_lengths.iter().map(|l| l.to_string()))
    }

    /// `;`-joined `B`/`T` flags for the `read_types` column.
    fn read_types_field(&self) -> String {
        join_semi(
            self.biological
                .iter()
                .map(|&b| if b { "B" } else { "T" }.to_string()),
        )
    }
}

fn join_semi(it: impl Iterator<Item = String>) -> String {
    it.collect::<Vec<_>>().join(";")
}

/// The archive's stored per-read structure, read from VDB metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadStructure {
    pub lengths: Vec<u32>,
    pub biological: Vec<bool>,
}

/// Read the true per-read lengths and types of a remote archive using range
/// requests (metadata only, tens of KB). Blocking: call from `spawn_blocking`.
pub fn fetch_read_structure(url: &str) -> Result<ReadStructure, String> {
    use crate::remote::HttpRangeReader;
    use crate::vdb::kar::KarArchive;

    let reader = HttpRangeReader::open(url).map_err(|e| format!("range open failed: {e}"))?;
    let mut archive = KarArchive::open(reader).map_err(|e| format!("KAR parse failed: {e}"))?;
    let descs = crate::vdb::cursor::archive_read_structure(&mut archive, "SEQUENCE")
        .ok_or_else(|| "no static read structure in archive metadata".to_string())?;
    Ok(ReadStructure {
        lengths: descs.iter().map(|&(_, l)| l).collect(),
        biological: descs.iter().map(|&(t, _)| t == b'B').collect(),
    })
}

/// Fetch read structures for many URLs concurrently (at most `concurrency`
/// in flight). A failed fetch yields `None` so callers fall back to the
/// RunInfo estimate instead of failing `info`.
pub async fn fetch_read_structures(
    urls: Vec<Option<String>>,
    concurrency: usize,
) -> Vec<Option<ReadStructure>> {
    use std::sync::Arc;
    let sem = Arc::new(tokio::sync::Semaphore::new(concurrency.max(1)));
    let tasks: Vec<_> = urls
        .into_iter()
        .map(|url| {
            let sem = sem.clone();
            tokio::spawn(async move {
                let url = url?;
                let _permit = sem.acquire_owned().await.ok()?;
                let u = url.clone();
                match tokio::task::spawn_blocking(move || fetch_read_structure(&u)).await {
                    Ok(Ok(rs)) => Some(rs),
                    Ok(Err(e)) => {
                        tracing::debug!("{url}: read structure unavailable: {e}");
                        None
                    }
                    Err(_) => None,
                }
            })
        })
        .collect();
    let mut out = Vec::with_capacity(tasks.len());
    for t in tasks {
        out.push(t.await.ok().flatten());
    }
    out
}

/// One row's worth of `sracha info` state: either a fully built record
/// (remote run or local archive) or an error captured during resolution so
/// it can still be rendered as a row. `Ok` is the bare resolved form, used
/// where no archive read structure was fetched.
pub enum InfoEntry<'a> {
    Ok(&'a ResolvedAccession),
    Record(Box<InfoRecord>),
    Error { accession: String, message: String },
}

const COLUMNS: &[&str] = &[
    "accession",
    "archive_type",
    "layout",
    "nreads",
    "spots",
    "size_bytes",
    "platform",
    "md5",
    "read_lengths",
    "read_types",
    "read_lengths_source",
    "title",
];

/// Emit one header row + one record per entry in TSV (`delim = b'\t'`) or CSV
/// (`delim = b','`) for pipeline consumption. Errored entries keep their
/// accession column populated; other columns are left empty.
pub fn write_delim<W: Write>(w: &mut W, entries: &[InfoEntry<'_>], delim: u8) {
    write_row(w, COLUMNS, delim);

    for entry in entries {
        let rec = match entry {
            InfoEntry::Ok(r) => InfoRecord::from_resolved(r),
            InfoEntry::Record(rec) => (**rec).clone(),
            InfoEntry::Error { accession, .. } => {
                let mut row = vec![""; COLUMNS.len()];
                row[0] = accession.as_str();
                write_row(w, &row, delim);
                continue;
            }
        };
        write_record(w, &rec, delim);
    }
    let _ = w.flush();
}

fn write_record<W: Write>(w: &mut W, rec: &InfoRecord, delim: u8) {
    let layout = rec.layout();
    let nreads = rec.nreads.map(|n| n.to_string()).unwrap_or_default();
    let spots = rec.spots.map(|s| s.to_string()).unwrap_or_default();
    let size_bytes = rec.size_bytes.map(|s| s.to_string()).unwrap_or_default();
    let read_lengths = rec.read_lengths_field();
    let read_types = rec.read_types_field();
    let source = match (rec.read_lengths.is_empty(), rec.lengths_estimated) {
        (true, _) => "",
        (false, true) => "runinfo",
        (false, false) => "archive",
    };
    write_row(
        w,
        &[
            rec.accession.as_str(),
            rec.archive_type,
            layout.as_str(),
            nreads.as_str(),
            spots.as_str(),
            size_bytes.as_str(),
            rec.platform.as_deref().unwrap_or(""),
            rec.md5.as_deref().unwrap_or(""),
            read_lengths.as_str(),
            read_types.as_str(),
            source,
            rec.title().unwrap_or(""),
        ],
        delim,
    );
}

fn write_row<W: Write>(w: &mut W, fields: &[&str], delim: u8) {
    for (i, field) in fields.iter().enumerate() {
        if i > 0 {
            let _ = w.write_all(&[delim]);
        }
        if delim == b',' {
            write_csv_field(w, field);
        } else {
            // TSV: collapse any embedded tab/newline to a single space so each
            // record stays on one line and column alignment is preserved.
            for ch in field.chars() {
                let out = match ch {
                    '\t' | '\n' | '\r' => ' ',
                    c => c,
                };
                let mut buf = [0u8; 4];
                let _ = w.write_all(out.encode_utf8(&mut buf).as_bytes());
            }
        }
    }
    let _ = writeln!(w);
}

fn write_csv_field<W: Write>(w: &mut W, field: &str) {
    let needs_quote = field
        .bytes()
        .any(|b| b == b',' || b == b'"' || b == b'\n' || b == b'\r');
    if !needs_quote {
        let _ = w.write_all(field.as_bytes());
        return;
    }
    let _ = w.write_all(b"\"");
    for ch in field.chars() {
        if ch == '"' {
            let _ = w.write_all(b"\"\"");
        } else {
            let mut buf = [0u8; 4];
            let _ = w.write_all(ch.encode_utf8(&mut buf).as_bytes());
        }
    }
    let _ = w.write_all(b"\"");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn local_record() -> InfoRecord {
        InfoRecord {
            accession: "SRR18918001".into(),
            path: Some("SRR18918001.sra".into()),
            archive_type: "SRA",
            nreads: Some(2),
            read_lengths: vec![8, 50, 16, 49],
            biological: vec![false, true, false, true],
            spots: Some(240_732_238),
            size_bytes: Some(9_828_828_653),
            platform: Some("ILLUMINA".into()),
            ..Default::default()
        }
    }

    fn render(entries: &[InfoEntry<'_>], delim: u8) -> Vec<String> {
        let mut buf = Vec::new();
        write_delim(&mut buf, entries, delim);
        String::from_utf8(buf)
            .unwrap()
            .lines()
            .map(String::from)
            .collect()
    }

    #[test]
    fn csv_field_quotes_when_needed() {
        let mut buf = Vec::new();
        write_csv_field(&mut buf, "plain");
        assert_eq!(buf, b"plain");

        buf.clear();
        write_csv_field(&mut buf, "has,comma");
        assert_eq!(buf, b"\"has,comma\"");

        buf.clear();
        write_csv_field(&mut buf, "has\"quote");
        assert_eq!(buf, b"\"has\"\"quote\"");
    }

    #[test]
    fn tsv_row_collapses_tabs_and_newlines() {
        let mut buf = Vec::new();
        write_row(&mut buf, &["a\tb", "c\nd", "ok"], b'\t');
        let s = String::from_utf8(buf).unwrap();
        assert_eq!(s, "a b\tc d\tok\n");
    }

    #[test]
    fn tsv_includes_read_lengths_column() {
        let lines = render(&[InfoEntry::Record(Box::new(local_record()))], b'\t');
        assert_eq!(
            lines[0],
            "accession\tarchive_type\tlayout\tnreads\tspots\tsize_bytes\tplatform\tmd5\tread_lengths\tread_types\tread_lengths_source\ttitle"
        );
        assert_eq!(
            lines[1],
            "SRR18918001\tSRA\tPAIRED\t2\t240732238\t9828828653\tILLUMINA\t\t8;50;16;49\tT;B;T;B\tarchive\t"
        );
    }

    #[test]
    fn tsv_local_record_row() {
        let mut rec = local_record();
        rec.read_lengths = vec![100];
        rec.biological.clear();
        rec.nreads = Some(1);
        let lines = render(&[InfoEntry::Record(Box::new(rec))], b'\t');
        assert!(
            lines[1].starts_with("SRR18918001\tSRA\tSINGLE\t1\t"),
            "{}",
            lines[1]
        );
        assert!(lines[1].ends_with("\t100\t\tarchive\t"), "{}", lines[1]);
    }

    #[test]
    fn csv_row_has_no_quoting_for_semicolon_lists() {
        let lines = render(&[InfoEntry::Record(Box::new(local_record()))], b',');
        assert!(
            lines[1].ends_with(",8;50;16;49,T;B;T;B,archive,"),
            "{}",
            lines[1]
        );
    }

    #[test]
    fn error_entry_keeps_accession() {
        let lines = render(
            &[InfoEntry::Error {
                accession: "SRR0".into(),
                message: "boom".into(),
            }],
            b'\t',
        );
        assert_eq!(lines[1], "SRR0\t\t\t\t\t\t\t\t\t\t\t");
    }

    #[test]
    fn reads_label_marks_technical_reads() {
        assert_eq!(local_record().reads_label(), "8bp(T)+50bp+16bp(T)+49bp");
    }

    #[test]
    fn reads_label_collapses_uniform_reads() {
        let rec = InfoRecord {
            read_lengths: vec![154, 154],
            ..Default::default()
        };
        assert_eq!(rec.reads_label(), "2×154bp");
        assert_eq!(InfoRecord::default().reads_label(), "-");
    }

    #[test]
    fn estimated_lengths_are_marked() {
        let rec = InfoRecord {
            accession: "SRR1".into(),
            archive_type: "SRA",
            nreads: Some(2),
            read_lengths: vec![61, 62],
            lengths_estimated: true,
            ..Default::default()
        };
        assert_eq!(rec.reads_label(), "~61bp+62bp");
        let lines = render(&[InfoEntry::Record(Box::new(rec))], b'\t');
        assert!(lines[1].ends_with("\t61;62\t\truninfo\t"), "{}", lines[1]);
    }

    #[test]
    fn apply_read_structure_replaces_estimate() {
        let mut rec = InfoRecord {
            accession: "SRR18918001".into(),
            archive_type: "SRA",
            nreads: Some(2),
            read_lengths: vec![61, 62],
            lengths_estimated: true,
            ..Default::default()
        };
        rec.apply_read_structure(ReadStructure {
            lengths: vec![8, 50, 16, 49],
            biological: vec![false, true, false, true],
        });
        assert!(!rec.lengths_estimated);
        assert_eq!(rec.nreads, Some(2));
        assert_eq!(rec.reads_label(), "8bp(T)+50bp+16bp(T)+49bp");
    }

    #[tokio::test]
    async fn unreachable_url_falls_back_to_none() {
        let out =
            fetch_read_structures(vec![None, Some("http://127.0.0.1:1/x.sra".into())], 2).await;
        assert_eq!(out, vec![None, None]);
    }

    #[test]
    fn title_column_prefers_sample_title_and_quotes_commas_in_csv() {
        let mut rec = local_record();
        rec.experiment = Some(ExperimentInfo {
            experiment_title: Some("exp, title".into()),
            ..Default::default()
        });
        assert_eq!(rec.title(), Some("exp, title"));
        let lines = render(&[InfoEntry::Record(Box::new(rec.clone()))], b',');
        assert!(
            lines[1].ends_with(",archive,\"exp, title\""),
            "{}",
            lines[1]
        );

        rec.experiment.as_mut().unwrap().sample_title = Some("sample".into());
        assert_eq!(rec.title(), Some("sample"));
        let lines = render(&[InfoEntry::Record(Box::new(rec))], b'\t');
        assert!(lines[1].ends_with("\tarchive\tsample"), "{}", lines[1]);
    }

    fn resolved(
        mirrors: &[(&str, &str)],
        spots: Option<u64>,
        spot_len: u32,
        bases: Option<u64>,
    ) -> ResolvedAccession {
        use crate::sdl::{ResolvedFile, ResolvedMirror, RunInfo};
        ResolvedAccession {
            accession: "SRR1".into(),
            sra_file: ResolvedFile {
                mirrors: mirrors
                    .iter()
                    .map(|&(service, url)| ResolvedMirror {
                        url: url.into(),
                        service: service.into(),
                    })
                    .collect(),
                size: 10,
                md5: None,
                is_lite: true,
            },
            vdbcache_file: None,
            run_info: Some(RunInfo {
                spots,
                spot_len,
                bases,
                ..Default::default()
            }),
        }
    }

    #[test]
    fn bases_prefers_runinfo_bases() {
        let r = resolved(&[], Some(100), 50, Some(4_999));
        let rec = InfoRecord::from_resolved(&r);
        assert_eq!(rec.bases, Some(4_999));
        assert!(!rec.bases_estimated);
    }

    #[test]
    fn bases_falls_back_to_marked_estimate() {
        let r = resolved(&[], Some(100), 50, None);
        let rec = InfoRecord::from_resolved(&r);
        assert_eq!(rec.bases, Some(5_000));
        assert!(rec.bases_estimated);

        let none = InfoRecord::from_resolved(&resolved(&[], None, 50, None));
        assert_eq!(none.bases, None);
    }

    #[test]
    fn record_reports_the_mirror_the_download_uses() {
        // SDL order puts the on-prem mirror first; the record must still
        // report the preferred cloud mirror and list the rest as alternates.
        let r = resolved(
            &[
                ("ncbi", "https://ncbi/x"),
                ("s3", "https://s3/x"),
                ("gs", "https://gs/x"),
            ],
            None,
            0,
            None,
        );
        let rec = InfoRecord::from_resolved(&r);
        assert_eq!(rec.source.as_deref(), Some("s3"));
        assert_eq!(rec.source_url.as_deref(), Some("https://s3/x"));
        assert_eq!(rec.alt_mirrors, ["gs", "ncbi"]);
        assert!(rec.is_lite);
    }
}
