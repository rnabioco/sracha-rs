//! Custom FASTQ/FASTA defline templates (`--seq-defline`).
//!
//! Mirrors the `--seq-defline` option of NCBI's `fasterq-dump`: a template
//! string with `$`-prefixed variables that are substituted per read. Templates
//! are compiled once (at CLI-parse time) into a flat token list so the decode
//! hot path only iterates pre-resolved literals/variables — no re-parsing per
//! record.
//!
//! Supported variables (the common subset of `fasterq-dump`'s set):
//!
//! | var   | meaning      |
//! |-------|--------------|
//! | `$ac` | accession    |
//! | `$si` | spot id      |
//! | `$ri` | read id      |
//! | `$sn` | spot name    |
//! | `$rl` | read length  |
//!
//! `$sn` resolves to the run's `NAME`/`SPOT_NAME` column. For Illumina that is
//! the reconstructed instrument name (`M05881:542:...`); for long-read
//! platforms (PacBio, Oxford Nanopore) it is the platform-native read
//! identifier as submitted — e.g. PacBio `m64012_.../ccs` or an ONT
//! `<uuid> runid=.. read=.. ch=.. start_time=..` string. The channel /
//! start-time / ZMW fields ONT and PacBio embed there are substrings of that
//! one name, not separate VDB columns, so they are exposed only as part of
//! `$sn` (which is also the default header's description field) rather than as
//! standalone variables.
//!
//! `$$` emits a literal `$`. A leading `@`/`>` is accepted (so `fasterq-dump`
//! templates like `@$ac.$si.$ri` paste in unchanged) but stripped — the record
//! prefix is added by the formatter (`@` for FASTQ, `>` for FASTA). `$sg`
//! (spot-group) is intentionally rejected; it would need SPOT_GROUP column
//! decoding that this path does not (yet) perform.

use super::IntegrityDiag;

/// A variable slot in a compiled [`DeflineTemplate`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DeflineVar {
    /// `$ac` — run accession.
    Accession,
    /// `$si` — 1-based spot (row) id.
    SpotId,
    /// `$ri` — 1-based read id within the spot.
    ReadId,
    /// `$sn` — original spot name (empty when absent).
    SpotName,
    /// `$rl` — length of this read in bases.
    ReadLen,
}

/// A single compiled token: either literal bytes or a variable to substitute.
#[derive(Clone, Debug, PartialEq, Eq)]
enum DeflineToken {
    Literal(Vec<u8>),
    Var(DeflineVar),
}

/// A compiled defline template, ready for per-record substitution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeflineTemplate {
    tokens: Vec<DeflineToken>,
}

impl DeflineTemplate {
    /// Compile a `--seq-defline` template string.
    ///
    /// Returns `Err(String)` on an unknown or unsupported variable so it can be
    /// wired directly as a clap `value_parser` (errors surface at argument-parse
    /// time). A single leading `@` or `>` is stripped.
    pub fn parse(s: &str) -> Result<Self, String> {
        // Strip one record-prefix char; the formatter re-adds the right one.
        let body = s
            .strip_prefix('@')
            .or_else(|| s.strip_prefix('>'))
            .unwrap_or(s);
        let bytes = body.as_bytes();

        let mut tokens: Vec<DeflineToken> = Vec::new();
        let mut lit: Vec<u8> = Vec::new();
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] != b'$' {
                lit.push(bytes[i]);
                i += 1;
                continue;
            }
            // `$$` -> literal '$'.
            if bytes.get(i + 1) == Some(&b'$') {
                lit.push(b'$');
                i += 2;
                continue;
            }
            // A two-letter variable code follows the `$`.
            let code = bytes.get(i + 1..i + 3).ok_or_else(|| {
                format!("incomplete variable at end of defline template '{s}'; {SUPPORTED}")
            })?;
            let var = match code {
                b"ac" => DeflineVar::Accession,
                b"si" => DeflineVar::SpotId,
                b"ri" => DeflineVar::ReadId,
                b"sn" => DeflineVar::SpotName,
                b"rl" => DeflineVar::ReadLen,
                b"sg" => {
                    return Err(format!(
                        "spot-group ($sg) is not supported in --seq-defline; {SUPPORTED}"
                    ));
                }
                other => {
                    let other = String::from_utf8_lossy(other);
                    return Err(format!("unknown defline variable '${other}'; {SUPPORTED}"));
                }
            };
            if !lit.is_empty() {
                tokens.push(DeflineToken::Literal(std::mem::take(&mut lit)));
            }
            tokens.push(DeflineToken::Var(var));
            i += 3;
        }
        if !lit.is_empty() {
            tokens.push(DeflineToken::Literal(lit));
        }
        Ok(DeflineTemplate { tokens })
    }

    /// Append the substituted defline *body* (no `@`/`>` prefix, no newline).
    fn append_body(
        &self,
        out: &mut Vec<u8>,
        run_name: &str,
        spot_id: &[u8],
        read_id: u32,
        spot_name: Option<&[u8]>,
        read_len: usize,
    ) {
        let mut itoa_buf = itoa::Buffer::new();
        for tok in &self.tokens {
            match tok {
                DeflineToken::Literal(b) => out.extend_from_slice(b),
                DeflineToken::Var(DeflineVar::Accession) => {
                    out.extend_from_slice(run_name.as_bytes())
                }
                DeflineToken::Var(DeflineVar::SpotId) => out.extend_from_slice(spot_id),
                DeflineToken::Var(DeflineVar::ReadId) => {
                    out.extend_from_slice(itoa_buf.format(read_id).as_bytes())
                }
                DeflineToken::Var(DeflineVar::SpotName) => {
                    if let Some(name) = spot_name {
                        out.extend_from_slice(name);
                    }
                }
                DeflineToken::Var(DeflineVar::ReadLen) => {
                    out.extend_from_slice(itoa_buf.format(read_len).as_bytes())
                }
            }
        }
    }
}

const SUPPORTED: &str = "supported variables: $ac (accession) $si (spot id) \
                         $ri (read id) $sn (spot name) $rl (read length), \
                         and $$ for a literal '$'";

/// Append one read as a FASTQ (or FASTA) record using a compiled template.
///
/// Builds the defline body once and copies it to the `+` line via an intra-Vec
/// memcpy — same trick as the default fast path in [`super::append_fastq_record`].
/// When `fasta` is set, the `+`/quality lines are omitted and the record is
/// prefixed with `>`.
#[allow(clippy::too_many_arguments)]
pub fn append_record_templated(
    out: &mut Vec<u8>,
    template: &DeflineTemplate,
    fasta: bool,
    run_name: &str,
    spot_id: &[u8],
    read_id: u32,
    spot_name: Option<&[u8]>,
    sequence: &[u8],
    quality: &[u8],
    diag: Option<&IntegrityDiag>,
) {
    let len = sequence.len();

    out.push(if fasta { b'>' } else { b'@' });
    let body_start = out.len();
    template.append_body(out, run_name, spot_id, read_id, spot_name, len);
    let body_end = out.len();
    out.push(b'\n');

    out.extend_from_slice(sequence);
    out.push(b'\n');

    if fasta {
        return;
    }

    // `+` line repeats the defline body (sracha convention). Single memcpy.
    out.push(b'+');
    out.extend_from_within(body_start..body_end);
    out.push(b'\n');

    let quality = super::repair_quality(quality, len, diag);
    out.extend_from_slice(&quality);
    out.push(b'\n');
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body(t: &DeflineTemplate, name: Option<&[u8]>) -> String {
        let mut out = Vec::new();
        t.append_body(&mut out, "SRR000001", b"42", 1, name, 4);
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn parses_common_template_and_strips_at() {
        let t = DeflineTemplate::parse("@$ac.$si.$ri").unwrap();
        assert_eq!(body(&t, None), "SRR000001.42.1");
    }

    #[test]
    fn substitutes_all_variables() {
        let t = DeflineTemplate::parse("$ac/$si/$ri/$sn/$rl").unwrap();
        assert_eq!(body(&t, Some(b"NAME")), "SRR000001/42/1/NAME/4");
    }

    #[test]
    fn spot_name_carries_long_read_native_id() {
        // For PacBio/ONT the NAME column holds the platform-native read id;
        // $sn must pass it through verbatim (spaces, slashes, key=value all).
        let t = DeflineTemplate::parse("@$ac.$si $sn").unwrap();
        assert_eq!(
            body(&t, Some(b"m64012_200723_165033/ccs")),
            "SRR000001.42 m64012_200723_165033/ccs"
        );
        let ont = DeflineTemplate::parse("$sn").unwrap();
        assert_eq!(
            body(&ont, Some(b"abc-uuid runid=r1 read=123 ch=42")),
            "abc-uuid runid=r1 read=123 ch=42"
        );
    }

    #[test]
    fn spot_name_absent_emits_empty() {
        let t = DeflineTemplate::parse("$si:$sn:end").unwrap();
        assert_eq!(body(&t, None), "42::end");
    }

    #[test]
    fn literal_dollar_via_double() {
        let t = DeflineTemplate::parse("cost=$$$si").unwrap();
        assert_eq!(body(&t, None), "cost=$42");
    }

    #[test]
    fn rejects_spot_group() {
        let err = DeflineTemplate::parse("@$ac.$sg").unwrap_err();
        assert!(err.contains("spot-group"), "{err}");
    }

    #[test]
    fn rejects_unknown_variable() {
        let err = DeflineTemplate::parse("@$ac.$zz").unwrap_err();
        assert!(err.contains("unknown defline variable '$zz'"), "{err}");
    }

    #[test]
    fn rejects_incomplete_variable() {
        assert!(DeflineTemplate::parse("$ac.$r").is_err());
    }

    #[test]
    fn templated_fastq_record_mirrors_plus_line() {
        let t = DeflineTemplate::parse("@$ac.$si.$ri").unwrap();
        let mut out = Vec::new();
        append_record_templated(
            &mut out, &t, false, "SRR1", b"7", 2, None, b"ACGT", b"IIII", None,
        );
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "@SRR1.7.2\nACGT\n+SRR1.7.2\nIIII\n"
        );
    }

    #[test]
    fn templated_fasta_record_has_no_quality() {
        let t = DeflineTemplate::parse("@$ac.$si.$ri").unwrap();
        let mut out = Vec::new();
        append_record_templated(
            &mut out, &t, true, "SRR1", b"7", 2, None, b"ACGT", b"", None,
        );
        assert_eq!(String::from_utf8(out).unwrap(), ">SRR1.7.2\nACGT\n");
    }
}
