//! Pipeline-friendly output for `sracha info`.
//!
//! Every renderer (table, per-accession detail, TSV/CSV) consumes the same
//! [`InfoRecord`], built either from a resolved remote run or from a local
//! archive, so the formats cannot drift apart. The human-readable table view
//! (with styling and the `tabled` crate) stays in the CLI binary.

use std::io::Write;
use std::path::Path;

use crate::sdl::ResolvedAccession;

/// Everything `sracha info` knows about one accession, independent of how it
/// is rendered or where it came from.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct InfoRecord {
    pub accession: String,
    /// Local file path; `None` for remote runs.
    pub path: Option<String>,
    /// `"SRA"` or `"cSRA"`.
    pub archive_type: &'static str,
    /// Biological read count (`nreads` column).
    pub nreads: Option<usize>,
    /// Per-read lengths, in read order. Includes technical reads when the
    /// source knows them (local archives).
    pub read_lengths: Vec<u32>,
    /// `true` = biological, per entry of `read_lengths`. Empty when unknown.
    pub biological: Vec<bool>,
    pub spots: Option<u64>,
    pub size_bytes: Option<u64>,
    pub platform: Option<String>,
    pub md5: Option<String>,
    /// Download mirror service (remote only).
    pub source: Option<String>,
    /// Local archive details.
    pub blobs: Option<usize>,
    pub columns: Vec<String>,
}

impl InfoRecord {
    /// Build a record from a resolved remote run.
    pub fn from_resolved(r: &ResolvedAccession) -> Self {
        let ri = r.run_info.as_ref();
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
            biological: Vec::new(),
            spots: ri.and_then(|ri| ri.spots),
            size_bytes: Some(r.sra_file.size),
            platform: ri.and_then(|ri| ri.platform.clone()),
            md5: r.sra_file.md5.clone(),
            source: r.sra_file.mirrors.first().map(|m| m.service.clone()),
            blobs: None,
            columns: Vec::new(),
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

/// One row's worth of `sracha info` state: either a fully resolved record
/// (from SDL/S3), a local archive record, or an error captured during
/// resolution so it can still be rendered as a row.
pub enum InfoEntry<'a> {
    Ok(&'a ResolvedAccession),
    Local(Box<InfoRecord>),
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
];

/// Emit one header row + one record per entry in TSV (`delim = b'\t'`) or CSV
/// (`delim = b','`) for pipeline consumption. Errored entries keep their
/// accession column populated; other columns are left empty.
pub fn write_delim<W: Write>(w: &mut W, entries: &[InfoEntry<'_>], delim: u8) {
    write_row(w, COLUMNS, delim);

    for entry in entries {
        let rec = match entry {
            InfoEntry::Ok(r) => InfoRecord::from_resolved(r),
            InfoEntry::Local(rec) => (**rec).clone(),
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
        let lines = render(&[InfoEntry::Local(Box::new(local_record()))], b'\t');
        assert_eq!(
            lines[0],
            "accession\tarchive_type\tlayout\tnreads\tspots\tsize_bytes\tplatform\tmd5\tread_lengths\tread_types"
        );
        assert_eq!(
            lines[1],
            "SRR18918001\tSRA\tPAIRED\t2\t240732238\t9828828653\tILLUMINA\t\t8;50;16;49\tT;B;T;B"
        );
    }

    #[test]
    fn tsv_local_record_row() {
        let mut rec = local_record();
        rec.read_lengths = vec![100];
        rec.biological.clear();
        rec.nreads = Some(1);
        let lines = render(&[InfoEntry::Local(Box::new(rec))], b'\t');
        assert!(
            lines[1].starts_with("SRR18918001\tSRA\tSINGLE\t1\t"),
            "{}",
            lines[1]
        );
        assert!(lines[1].ends_with("\t100\t"), "{}", lines[1]);
    }

    #[test]
    fn csv_row_has_no_quoting_for_semicolon_lists() {
        let lines = render(&[InfoEntry::Local(Box::new(local_record()))], b',');
        assert!(lines[1].ends_with(",8;50;16;49,T;B;T;B"), "{}", lines[1]);
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
        assert_eq!(lines[1], "SRR0\t\t\t\t\t\t\t\t\t");
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
}
