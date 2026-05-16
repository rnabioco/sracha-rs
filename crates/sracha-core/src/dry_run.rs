use std::io::Write;

use serde::Serialize;

use crate::error::Result;
use crate::sdl::ResolvedAccession;

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

impl DryRunRow {
    pub fn from_resolved(r: &ResolvedAccession) -> Self {
        let primary = r.sra_file.mirrors.first();
        let ri = r.run_info.as_ref();
        Self {
            accession: r.accession.clone(),
            url: primary.map(|m| m.url.clone()),
            service: primary.map(|m| m.service.clone()),
            size_bytes: r.sra_file.size,
            md5: r.sra_file.md5.clone(),
            nreads: ri.map(|x| x.nreads),
            spot_len: ri.map(|x| x.spot_len),
            avg_read_len: ri.map(|x| x.avg_read_len.clone()).unwrap_or_default(),
            platform: ri.and_then(|x| x.platform.clone()),
            spots: ri.and_then(|x| x.spots),
        }
    }
}

pub fn write_tsv<W: Write>(mut w: W, resolved: &[ResolvedAccession]) -> Result<()> {
    writeln!(
        w,
        "accession\turl\tservice\tsize_bytes\tmd5\tnreads\tspot_len\tavg_read_len\tplatform\tspots"
    )?;
    for r in resolved {
        let row = DryRunRow::from_resolved(r);
        writeln!(
            w,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            row.accession,
            row.url.as_deref().unwrap_or(""),
            row.service.as_deref().unwrap_or(""),
            row.size_bytes,
            row.md5.as_deref().unwrap_or(""),
            row.nreads.map(|n| n.to_string()).unwrap_or_default(),
            row.spot_len.map(|n| n.to_string()).unwrap_or_default(),
            row.avg_read_len
                .iter()
                .map(|n| n.to_string())
                .collect::<Vec<_>>()
                .join(","),
            row.platform.as_deref().unwrap_or(""),
            row.spots.map(|n| n.to_string()).unwrap_or_default(),
        )?;
    }
    Ok(())
}

pub fn write_json<W: Write>(w: W, resolved: &[ResolvedAccession]) -> Result<()> {
    let rows: Vec<DryRunRow> = resolved.iter().map(DryRunRow::from_resolved).collect();
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
            "SRR123\thttps://example/SRR123\ts3\t1024\tabcd\t2\t200\t100,100\tILLUMINA\t42"
        ));
        assert!(lines.next().is_none());
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
