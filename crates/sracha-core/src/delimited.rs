//! Shared delimited-text (TSV/CSV) row writer used by `info`, `--dry-run` and
//! the run-metadata sidecar.
//!
//! Conventions, identical for every writer:
//! - TSV: embedded tab/CR/LF in a field collapse to a single space so each
//!   record stays on one line.
//! - CSV: fields containing `,`, `"` or a line break are quoted, `"` doubled.
//! - List-valued columns (read lengths and the like) join with [`LIST_SEP`],
//!   which is neither a TSV nor a CSV delimiter, so lists never need quoting.

use std::io::{self, Write};

/// Separator for list-valued columns.
pub const LIST_SEP: &str = ";";

/// Join numbers with [`LIST_SEP`].
pub fn join_list<T: std::fmt::Display>(items: &[T]) -> String {
    items
        .iter()
        .map(|n| n.to_string())
        .collect::<Vec<_>>()
        .join(LIST_SEP)
}

/// Write one record (fields separated by `delim`, newline-terminated).
pub fn write_row<W: Write>(w: &mut W, fields: &[&str], delim: u8) -> io::Result<()> {
    for (i, field) in fields.iter().enumerate() {
        if i > 0 {
            w.write_all(&[delim])?;
        }
        if delim == b',' {
            write_csv_field(w, field)?;
        } else {
            let mut buf = [0u8; 4];
            for ch in field.chars() {
                let out = match ch {
                    '\t' | '\n' | '\r' => ' ',
                    c => c,
                };
                w.write_all(out.encode_utf8(&mut buf).as_bytes())?;
            }
        }
    }
    writeln!(w)
}

/// Write one CSV field, quoting only when needed.
pub fn write_csv_field<W: Write>(w: &mut W, field: &str) -> io::Result<()> {
    let needs_quote = field
        .bytes()
        .any(|b| b == b',' || b == b'"' || b == b'\n' || b == b'\r');
    if !needs_quote {
        return w.write_all(field.as_bytes());
    }
    w.write_all(b"\"")?;
    let mut buf = [0u8; 4];
    for ch in field.chars() {
        if ch == '"' {
            w.write_all(b"\"\"")?;
        } else {
            w.write_all(ch.encode_utf8(&mut buf).as_bytes())?;
        }
    }
    w.write_all(b"\"")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_tabs() {
        let mut buf = Vec::new();
        write_row(&mut buf, &["a\tb", "c\nd", "ok"], b'\t').unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "a b\tc d\tok\n");
    }

    #[test]
    fn quotes_csv() {
        let mut buf = Vec::new();
        write_csv_field(&mut buf, "plain").unwrap();
        assert_eq!(buf, b"plain");

        buf.clear();
        write_csv_field(&mut buf, "has,comma").unwrap();
        assert_eq!(buf, b"\"has,comma\"");

        buf.clear();
        write_csv_field(&mut buf, "has\"quote").unwrap();
        assert_eq!(buf, b"\"has\"\"quote\"");
    }

    #[test]
    fn lists_join_with_semicolon() {
        assert_eq!(join_list(&[151u32, 151]), "151;151");
        assert_eq!(join_list::<u32>(&[]), "");
    }
}
