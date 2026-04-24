//! Pipeline-friendly output for `sracha info`.
//!
//! This module renders one accession per row in TSV/CSV form. The
//! human-readable table view (with styling and the `tabled` crate)
//! stays in the CLI binary.

use std::io::Write;

use crate::sdl::ResolvedAccession;

/// One row's worth of `sracha info` state: either a fully resolved record
/// (from SDL/S3) or an error captured during resolution so it can still be
/// rendered as a row.
pub enum InfoEntry<'a> {
    Ok(&'a ResolvedAccession),
    Error { accession: String, message: String },
}

/// Emit one header row + one record per entry in TSV (`delim = b'\t'`) or CSV
/// (`delim = b','`) for pipeline consumption. Errored entries keep their
/// accession column populated; other columns are left empty.
pub fn write_delim<W: Write>(w: &mut W, entries: &[InfoEntry<'_>], delim: u8) {
    const COLUMNS: &[&str] = &[
        "accession",
        "archive_type",
        "layout",
        "nreads",
        "spots",
        "size_bytes",
        "platform",
        "md5",
    ];
    write_row(w, COLUMNS, delim);

    for entry in entries {
        match entry {
            InfoEntry::Ok(r) => {
                let archive_type = if r.vdbcache_file.is_some() {
                    "cSRA"
                } else {
                    "SRA"
                };
                let (layout, nreads) = r
                    .run_info
                    .as_ref()
                    .map(|ri| {
                        let layout = match ri.nreads {
                            1 => "SINGLE".to_string(),
                            2 => "PAIRED".to_string(),
                            n => format!("{n}-read"),
                        };
                        (layout, ri.nreads.to_string())
                    })
                    .unwrap_or_default();
                let spots = r
                    .run_info
                    .as_ref()
                    .and_then(|ri| ri.spots)
                    .map(|s| s.to_string())
                    .unwrap_or_default();
                let size_bytes = r.sra_file.size.to_string();
                let platform = r
                    .run_info
                    .as_ref()
                    .and_then(|ri| ri.platform.clone())
                    .unwrap_or_default();
                let md5 = r.sra_file.md5.clone().unwrap_or_default();

                write_row(
                    w,
                    &[
                        r.accession.as_str(),
                        archive_type,
                        layout.as_str(),
                        nreads.as_str(),
                        spots.as_str(),
                        size_bytes.as_str(),
                        platform.as_str(),
                        md5.as_str(),
                    ],
                    delim,
                );
            }
            InfoEntry::Error { accession, .. } => {
                write_row(w, &[accession.as_str(), "", "", "", "", "", "", ""], delim);
            }
        }
    }
    let _ = w.flush();
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
}
