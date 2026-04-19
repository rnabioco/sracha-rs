//! Parser for the `-R` row-range argument of `sracha vdb dump`.
//!
//! Accepts comma-separated segments: `5`, `5-20`, `100-`, `-50`, mixed.
//! All bounds are inclusive and 1-indexed (matching `vdb-dump`).
//! Segments may overlap or appear out of order — iteration materialises
//! them in input order after clamping to the column's actual row range.

use crate::error::{Error, Result};

/// One parsed segment of a row range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Segment {
    /// A single row id.
    Single(i64),
    /// `lo..=hi` with both bounds explicit.
    Closed(i64, i64),
    /// `lo..=column_last` (open upper bound, e.g. `500-`).
    FromStart(i64),
    /// `column_first..=hi` (open lower bound, e.g. `-50`).
    ToEnd(i64),
}

/// Parsed `-R` argument.
#[derive(Debug, Clone, Default)]
pub struct RowRanges {
    segments: Vec<Segment>,
}

impl RowRanges {
    /// Parse a comma-separated range string. An empty string returns an
    /// empty `RowRanges` (caller should interpret that as "all rows").
    pub fn parse(s: &str) -> Result<Self> {
        let s = s.trim();
        if s.is_empty() {
            return Ok(Self::default());
        }
        let mut segments = Vec::new();
        for raw in s.split(',') {
            let part = raw.trim();
            if part.is_empty() {
                continue;
            }
            segments.push(parse_segment(part)?);
        }
        Ok(Self { segments })
    }

    /// `true` when no segments were parsed (caller: dump every row).
    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    /// Raw segments in input order.
    pub fn segments(&self) -> &[Segment] {
        &self.segments
    }

    /// Iterate every row id covered by any segment, clamped to
    /// `[first_row, first_row + row_count)`. Preserves segment order;
    /// does not deduplicate across segments.
    pub fn iter_row_ids(&self, first_row: i64, row_count: u64) -> impl Iterator<Item = i64> + '_ {
        let last_row = first_row.saturating_add(row_count.saturating_sub(1) as i64);
        let span_empty = row_count == 0;
        self.segments.iter().flat_map(move |seg| {
            let (lo, hi) = match *seg {
                Segment::Single(id) => (id, id),
                Segment::Closed(a, b) => (a.min(b), a.max(b)),
                Segment::FromStart(a) => (a, last_row),
                Segment::ToEnd(b) => (first_row, b),
            };
            let lo = lo.max(first_row);
            let hi = hi.min(last_row);
            let empty = span_empty || lo > hi;
            let start = if empty { 1 } else { lo };
            let end = if empty { 0 } else { hi };
            start..=end
        })
    }
}

fn parse_segment(s: &str) -> Result<Segment> {
    let bad = |msg: &str| Error::Format(format!("row-range: {msg}: {s:?}"));
    if let Some(idx) = s.find('-') {
        let (lhs, rhs) = (&s[..idx], &s[idx + 1..]);
        let lhs = lhs.trim();
        let rhs = rhs.trim();
        match (lhs.is_empty(), rhs.is_empty()) {
            (true, true) => Err(bad("empty range")),
            (false, true) => {
                let a = parse_i64(lhs).ok_or_else(|| bad("not a number"))?;
                Ok(Segment::FromStart(a))
            }
            (true, false) => {
                let b = parse_i64(rhs).ok_or_else(|| bad("not a number"))?;
                Ok(Segment::ToEnd(b))
            }
            (false, false) => {
                let a = parse_i64(lhs).ok_or_else(|| bad("not a number"))?;
                let b = parse_i64(rhs).ok_or_else(|| bad("not a number"))?;
                Ok(Segment::Closed(a, b))
            }
        }
    } else {
        let a = parse_i64(s).ok_or_else(|| bad("not a number"))?;
        Ok(Segment::Single(a))
    }
}

fn parse_i64(s: &str) -> Option<i64> {
    s.parse::<i64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_string() {
        let r = RowRanges::parse("").unwrap();
        assert!(r.is_empty());
        let ids: Vec<i64> = r.iter_row_ids(1, 10).collect();
        assert!(ids.is_empty());
    }

    #[test]
    fn single_row() {
        let r = RowRanges::parse("5").unwrap();
        assert_eq!(r.segments(), &[Segment::Single(5)]);
        let ids: Vec<i64> = r.iter_row_ids(1, 10).collect();
        assert_eq!(ids, vec![5]);
    }

    #[test]
    fn closed_range() {
        let r = RowRanges::parse("3-5").unwrap();
        let ids: Vec<i64> = r.iter_row_ids(1, 10).collect();
        assert_eq!(ids, vec![3, 4, 5]);
    }

    #[test]
    fn from_start_open_ended() {
        let r = RowRanges::parse("8-").unwrap();
        let ids: Vec<i64> = r.iter_row_ids(1, 10).collect();
        assert_eq!(ids, vec![8, 9, 10]);
    }

    #[test]
    fn to_end_open_ended() {
        let r = RowRanges::parse("-3").unwrap();
        let ids: Vec<i64> = r.iter_row_ids(1, 10).collect();
        assert_eq!(ids, vec![1, 2, 3]);
    }

    #[test]
    fn mixed_segments_preserve_input_order() {
        let r = RowRanges::parse("5, 1-2, 7").unwrap();
        let ids: Vec<i64> = r.iter_row_ids(1, 10).collect();
        assert_eq!(ids, vec![5, 1, 2, 7]);
    }

    #[test]
    fn overlapping_segments_are_not_deduped() {
        let r = RowRanges::parse("1-3,2-4").unwrap();
        let ids: Vec<i64> = r.iter_row_ids(1, 10).collect();
        assert_eq!(ids, vec![1, 2, 3, 2, 3, 4]);
    }

    #[test]
    fn reversed_bounds_are_normalised() {
        let r = RowRanges::parse("5-3").unwrap();
        let ids: Vec<i64> = r.iter_row_ids(1, 10).collect();
        assert_eq!(ids, vec![3, 4, 5]);
    }

    #[test]
    fn clamps_to_column_range() {
        let r = RowRanges::parse("0-100").unwrap();
        let ids: Vec<i64> = r.iter_row_ids(5, 3).collect();
        assert_eq!(ids, vec![5, 6, 7]);
    }

    #[test]
    fn segment_fully_outside_is_empty() {
        let r = RowRanges::parse("100-200").unwrap();
        let ids: Vec<i64> = r.iter_row_ids(1, 10).collect();
        assert!(ids.is_empty());
    }

    #[test]
    fn empty_column_emits_nothing() {
        let r = RowRanges::parse("1-10").unwrap();
        let ids: Vec<i64> = r.iter_row_ids(1, 0).collect();
        assert!(ids.is_empty());
    }

    #[test]
    fn bad_input_not_a_number() {
        assert!(RowRanges::parse("abc").is_err());
        assert!(RowRanges::parse("1-abc").is_err());
        assert!(RowRanges::parse("abc-5").is_err());
    }

    #[test]
    fn bad_input_empty_dash() {
        assert!(RowRanges::parse("-").is_err());
    }

    #[test]
    fn trims_whitespace() {
        let r = RowRanges::parse(" 1 - 3 , 5 ").unwrap();
        let ids: Vec<i64> = r.iter_row_ids(1, 10).collect();
        assert_eq!(ids, vec![1, 2, 3, 5]);
    }

    #[test]
    fn skips_empty_csv_fields() {
        let r = RowRanges::parse("1,,3").unwrap();
        let ids: Vec<i64> = r.iter_row_ids(1, 10).collect();
        assert_eq!(ids, vec![1, 3]);
    }
}
