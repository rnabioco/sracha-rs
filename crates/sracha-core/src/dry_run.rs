use std::io::Write;

use serde::Serialize;

use crate::delimited::write_row;
use crate::error::Result;
use crate::metadata::{COLUMNS, RunMetadata};
use crate::sdl::ResolvedAccession;

/// Dry-run column names paired with the [`RunMetadata`] column each is read
/// from, in output order.
const DRY_RUN_COLUMNS: [(&str, &str); 10] = [
    ("accession", "accession"),
    ("url", "url"),
    ("service", "source_service"),
    ("size_bytes", "size_bytes"),
    ("md5", "md5"),
    ("nreads", "nreads"),
    ("spot_len", "spot_len"),
    ("avg_read_len", "avg_read_len"),
    ("platform", "platform"),
    ("spots", "spots"),
];

/// The JSON shape of a dry-run row: a subset of [`RunMetadata`].
#[derive(Debug, Serialize)]
pub struct DryRunRow {
    pub accession: String,
    pub url: Option<String>,
    pub service: Option<String>,
    pub size_bytes: u64,
    pub md5: Option<String>,
    pub nreads: Option<usize>,
    pub spot_len: Option<u32>,
    pub avg_read_len: Vec<u32>,
    pub platform: Option<String>,
    pub spots: Option<u64>,
}

impl From<RunMetadata> for DryRunRow {
    fn from(m: RunMetadata) -> Self {
        Self {
            accession: m.accession,
            url: m.url,
            service: m.source_service,
            size_bytes: m.size_bytes.unwrap_or_default(),
            md5: m.md5,
            nreads: m.nreads,
            spot_len: m.spot_len,
            avg_read_len: m.avg_read_len,
            platform: m.platform,
            spots: m.spots,
        }
    }
}

pub fn write_tsv<W: Write>(mut w: W, resolved: &[ResolvedAccession]) -> Result<()> {
    let header: Vec<&str> = DRY_RUN_COLUMNS.iter().map(|(name, _)| *name).collect();
    write_row(&mut w, &header, b'\t')?;
    let idx: Vec<usize> = DRY_RUN_COLUMNS
        .iter()
        .map(|(_, src)| {
            COLUMNS
                .iter()
                .position(|c| c == src)
                .expect("dry-run column exists in metadata")
        })
        .collect();
    for r in resolved {
        let cells = RunMetadata::from_resolved(r).cells();
        let fields: Vec<&str> = idx.iter().map(|&i| cells[i].as_str()).collect();
        write_row(&mut w, &fields, b'\t')?;
    }
    Ok(())
}

pub fn write_json<W: Write>(w: W, resolved: &[ResolvedAccession]) -> Result<()> {
    let rows: Vec<DryRunRow> = resolved
        .iter()
        .map(|r| DryRunRow::from(RunMetadata::from_resolved(r)))
        .collect();
    serde_json::to_writer_pretty(w, &rows)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sdl::{ResolvedAccession, ResolvedFile, ResolvedMirror, RunInfo};

    fn sample() -> ResolvedAccession {
        ResolvedAccession {
            accession: "SRR123".into(),
            sra_file: ResolvedFile {
                size: 1024,
                md5: Some("abcd".into()),
                mirrors: vec![ResolvedMirror {
                    service: "s3".into(),
                    url: "https://example/SRR123".into(),
                }],
                is_lite: false,
            },
            vdbcache_file: None,
            run_info: Some(RunInfo {
                nreads: 2,
                avg_read_len: vec![100, 100],
                spot_len: 200,
                platform: Some("ILLUMINA".into()),
                spots: Some(42),
                ..Default::default()
            }),
        }
    }

    #[test]
    fn tsv_writes_header_and_row() {
        let mut buf = Vec::new();
        write_tsv(&mut buf, &[sample()]).unwrap();
        let s = String::from_utf8(buf).unwrap();
        let mut lines = s.lines();
        assert!(lines.next().unwrap().starts_with("accession\turl"));
        let row = lines.next().unwrap();
        assert!(row.starts_with(
            "SRR123\thttps://example/SRR123\ts3\t1024\tabcd\t2\t200\t100;100\tILLUMINA\t42"
        ));
        assert!(lines.next().is_none());
    }

    #[test]
    fn row_is_subset_of_metadata() {
        let r = sample();
        let meta = RunMetadata::from_resolved(&r);
        let cells = meta.cells();
        let mut buf = Vec::new();
        write_tsv(&mut buf, &[r]).unwrap();
        let s = String::from_utf8(buf).unwrap();
        let mut lines = s.lines();
        let header: Vec<&str> = lines.next().unwrap().split('\t').collect();
        let row: Vec<&str> = lines.next().unwrap().split('\t').collect();
        assert_eq!(header.len(), DRY_RUN_COLUMNS.len());
        for ((name, src), value) in DRY_RUN_COLUMNS.iter().zip(&row) {
            let i = COLUMNS.iter().position(|c| c == src).unwrap();
            assert_eq!(*value, cells[i], "{name}");
        }
        let json = DryRunRow::from(meta);
        assert_eq!(json.service.as_deref(), Some("s3"));
        assert_eq!(json.size_bytes, 1024);
    }

    #[test]
    fn json_writes_array() {
        let mut buf = Vec::new();
        write_json(&mut buf, &[sample()]).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        let arr = v.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        let o = arr[0].as_object().unwrap();
        assert_eq!(o["accession"], "SRR123");
        assert_eq!(o["url"], "https://example/SRR123");
        assert_eq!(o["size_bytes"], 1024);
    }
}
