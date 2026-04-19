//! `sracha vdb dump` — row-level data dump for SRA SEQUENCE-style columns.
//!
//! Phase 2a scope per issue #12: heuristic-typed decode of the columns
//! that matter for SRA runs (READ / QUALITY / NAME / SPOT_GROUP /
//! READ_LEN / READ_TYPE / X / Y, plus CMP_READ, ALTREAD, READ_START,
//! READ_FILTER). Unknown columns render as hex.
//!
//! Formats (default / csv / tab / json) all go through a single row-by-row
//! driver that pulls one cell at a time from per-column blob caches. No
//! schema engine — the heuristic in [`infer_kind`] covers the columns that
//! real SRA pipelines actually query.

use std::collections::HashSet;
use std::io::{Read, Seek, Write};
use std::path::Path;

use serde_json::{Value, json};

use crate::blob::{DecodedBlob, PageMap};
use crate::blob_codecs::{
    decode_irzip_column, decode_quality_encoding, decode_raw, decode_zip_encoding,
};
use crate::encoding::{phred_to_ascii, unpack_2na, unpack_4na};
use crate::error::{Error, Result};
use crate::inspect::{column_base_path_public, detect_kind, list_columns};
use crate::kar::KarArchive;
use crate::kdb::ColumnReader;
use crate::row_range::RowRanges;

// ---------------------------------------------------------------------------
// Column heuristic
// ---------------------------------------------------------------------------

/// How to interpret the raw bytes of one physical column.
///
/// Inferred from the column name alone — the VDB schema engine is out of
/// scope for Phase 2a. Unknown columns fall back to [`CellKind::HexRaw`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CellKind {
    /// 2na packed DNA (READ, CMP_READ) — one byte per base after unpacking.
    Dna2na,
    /// 4na ambiguity mask stored low-nibble per base (ALTREAD `INSDC:4na:bin`).
    Dna4naBin,
    /// Raw Phred quality, printed as Phred+33 ASCII (QUALITY, ORIGINAL_QUALITY).
    QualityPhred33,
    /// Plain ASCII text (NAME, SPOT_NAME, SPOT_GROUP).
    AsciiBytes,
    /// One u32 per element; each row has `reads_per_spot` elements.
    U32Array,
    /// One u8 per element; each row has `reads_per_spot` elements.
    U8Array,
    /// One i32/u32 per row (X, Y coordinates).
    I32Scalar,
    /// Fallback — render raw blob bytes as hex.
    HexRaw,
}

impl CellKind {
    /// Bytes per logical element after decode (1 for ASCII-style, 4 for u32/i32).
    pub fn elem_bytes(self) -> usize {
        match self {
            Self::Dna2na
            | Self::Dna4naBin
            | Self::QualityPhred33
            | Self::AsciiBytes
            | Self::U8Array
            | Self::HexRaw => 1,
            Self::U32Array | Self::I32Scalar => 4,
        }
    }
}

/// Guess the [`CellKind`] for a SEQUENCE-style column by name.
pub fn infer_kind(col_name: &str) -> CellKind {
    match col_name {
        "READ" | "CMP_READ" => CellKind::Dna2na,
        "ALTREAD" => CellKind::Dna4naBin,
        "QUALITY" | "ORIGINAL_QUALITY" | "CMP_QUALITY" => CellKind::QualityPhred33,
        "NAME" | "SPOT_NAME" | "SPOT_GROUP" | "LABEL" => CellKind::AsciiBytes,
        "READ_LEN" | "READ_START" => CellKind::U32Array,
        "READ_TYPE" | "READ_FILTER" => CellKind::U8Array,
        "X" | "Y" => CellKind::I32Scalar,
        _ => CellKind::HexRaw,
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Output format for `dump`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DumpFormat {
    /// vdb-dump-style `row N: COL="val", COL=[1,2], ...` per row.
    Default,
    /// Comma-separated, strings quoted, arrays joined with `;`.
    Csv,
    /// Tab-separated, same escaping rules as CSV.
    Tab,
    /// Newline-delimited JSON — one JSON object per row.
    Json,
}

/// User-supplied options to [`DumpRunner::new`].
pub struct DumpSpec {
    /// Columns requested by the user. Empty means "all columns in the table".
    pub columns: Vec<String>,
    /// Columns to exclude (applied after `columns`).
    pub exclude: Vec<String>,
    /// Row range. Empty means "all rows".
    pub rows: RowRanges,
    /// Output format.
    pub format: DumpFormat,
}

/// Per-column state held by the runner: the reader plus a one-blob cache of
/// decoded row bytes.
struct OpenedColumn {
    name: String,
    kind: CellKind,
    reader: ColumnReader,
    cache: Option<BlobCache>,
}

/// Decoded bytes + per-row byte ranges for a single blob.
struct BlobCache {
    start_id: i64,
    row_count: u64,
    /// Flat byte buffer with one logical row after another.
    /// For `elem_bytes == 1` kinds this is one byte per base/char. For
    /// u32/i32 kinds it is little-endian u32 bytes.
    bytes: Vec<u8>,
    /// Per-row byte offsets into `bytes`, length `row_count + 1`.
    offsets: Vec<usize>,
}

/// Top-level entry point.
pub struct DumpRunner<R: Read + Seek> {
    cols: Vec<OpenedColumn>,
    first_row: i64,
    row_count: u64,
    format: DumpFormat,
    rows: RowRanges,
    _archive: std::marker::PhantomData<R>,
}

impl<R: Read + Seek> DumpRunner<R> {
    /// Open the requested columns in the archive and prepare to iterate.
    pub fn new(
        archive: &mut KarArchive<R>,
        sra_path: &Path,
        table: Option<&str>,
        spec: DumpSpec,
    ) -> Result<Self> {
        let _ = detect_kind(archive)?;
        let col_base = column_base_path_public(archive, table)?;
        let available = list_columns(archive, table)?;

        let requested = resolve_column_selection(&available, &spec.columns, &spec.exclude);
        if requested.is_empty() {
            return Err(Error::Format(
                "dump: no columns selected after applying -C/-x filters".into(),
            ));
        }

        let mut cols = Vec::with_capacity(requested.len());
        for name in &requested {
            let full = format!("{col_base}/{name}");
            match ColumnReader::open(archive, &full, sra_path) {
                Ok(reader) => cols.push(OpenedColumn {
                    name: name.clone(),
                    kind: infer_kind(name),
                    reader,
                    cache: None,
                }),
                Err(e) => {
                    tracing::warn!("dump: skipping column {name}: {e}");
                }
            }
        }
        if cols.is_empty() {
            return Err(Error::Format(
                "dump: every selected column failed to open".into(),
            ));
        }

        let first_row = cols
            .iter()
            .filter_map(|c| c.reader.first_row_id())
            .min()
            .unwrap_or(1);
        let row_count = cols.iter().map(|c| c.reader.row_count()).max().unwrap_or(0);

        Ok(Self {
            cols,
            first_row,
            row_count,
            format: spec.format,
            rows: spec.rows,
            _archive: std::marker::PhantomData,
        })
    }

    /// Emit every requested row to `w`.
    pub fn run<W: Write>(&mut self, w: &mut W) -> Result<()> {
        if self.rows.is_empty() {
            let first = self.first_row;
            let count = self.row_count;
            let mut id = first;
            let last = first.saturating_add(count.saturating_sub(1) as i64);
            while id <= last && count > 0 {
                self.emit_row(w, id)?;
                id += 1;
            }
        } else {
            // Materialise first so we can drop the borrow on `self.rows`
            // before the &mut-borrow in `emit_row`.
            let ids: Vec<i64> = self
                .rows
                .iter_row_ids(self.first_row, self.row_count)
                .collect();
            for id in ids {
                self.emit_row(w, id)?;
            }
        }
        Ok(())
    }

    fn emit_row<W: Write>(&mut self, w: &mut W, row_id: i64) -> Result<()> {
        // Refresh every column's blob cache to cover this row.
        for col in &mut self.cols {
            ensure_blob(col, row_id)?;
        }

        match self.format {
            DumpFormat::Default => write_default(w, row_id, &self.cols)?,
            DumpFormat::Csv => write_delimited(w, row_id, &self.cols, b',')?,
            DumpFormat::Tab => write_delimited(w, row_id, &self.cols, b'\t')?,
            DumpFormat::Json => write_json(w, row_id, &self.cols)?,
        }
        Ok(())
    }
}

fn resolve_column_selection(
    available: &[String],
    requested: &[String],
    exclude: &[String],
) -> Vec<String> {
    let excl: HashSet<&str> = exclude.iter().map(String::as_str).collect();
    let base: Vec<String> = if requested.is_empty() {
        available.to_vec()
    } else {
        requested.to_vec()
    };
    base.into_iter()
        .filter(|n| !excl.contains(n.as_str()))
        .collect()
}

// ---------------------------------------------------------------------------
// Per-blob decode + caching
// ---------------------------------------------------------------------------

fn ensure_blob(col: &mut OpenedColumn, row_id: i64) -> Result<()> {
    if let Some(cache) = &col.cache {
        let end = cache.start_id + cache.row_count as i64;
        if row_id >= cache.start_id && row_id < end {
            return Ok(());
        }
    }
    let blob = col.reader.find_blob(row_id).ok_or_else(|| {
        Error::Format(format!("dump: no blob covers row {row_id} in {}", col.name))
    })?;
    let start_id = blob.start_id;
    let id_range = blob.id_range as u64;
    let raw = col.reader.read_raw_blob_slice(row_id)?.to_vec();
    let cs = col.reader.meta().checksum_type;
    let decoded = decode_raw(&raw, cs, id_range)?;
    let (bytes, offsets) = materialize_blob(col.kind, &decoded, id_range)?;
    if offsets.len() as u64 != id_range + 1 {
        return Err(Error::Format(format!(
            "dump: column {} blob decoded {} rows, expected {id_range}",
            col.name,
            offsets.len().saturating_sub(1),
        )));
    }
    col.cache = Some(BlobCache {
        start_id,
        row_count: id_range,
        bytes,
        offsets,
    });
    Ok(())
}

/// Decode a blob according to its [`CellKind`] and return:
/// * `bytes` — one logical row after another.
/// * `offsets` — per-row byte offsets (length = row_count + 1).
fn materialize_blob(
    kind: CellKind,
    decoded: &DecodedBlob<'_>,
    row_count: u64,
) -> Result<(Vec<u8>, Vec<usize>)> {
    match kind {
        CellKind::Dna2na => materialize_dna2na(decoded, row_count),
        CellKind::Dna4naBin => materialize_dna4na_bin(decoded, row_count),
        CellKind::QualityPhred33 => materialize_quality(decoded, row_count),
        CellKind::AsciiBytes => materialize_ascii(decoded, row_count),
        CellKind::U32Array | CellKind::I32Scalar => materialize_u32(decoded, row_count),
        CellKind::U8Array => materialize_u8_array(decoded, row_count),
        CellKind::HexRaw => materialize_hex_fallback(decoded, row_count),
    }
}

fn materialize_dna2na(decoded: &DecodedBlob<'_>, row_count: u64) -> Result<(Vec<u8>, Vec<usize>)> {
    let packed = decode_zip_encoding(decoded)?;
    let actual_bases = elem_count(decoded, &packed, 2);
    let bases = unpack_2na(&packed, actual_bases);
    split_by_rows(decoded, bases, row_count, 1)
}

fn materialize_dna4na_bin(
    decoded: &DecodedBlob<'_>,
    row_count: u64,
) -> Result<(Vec<u8>, Vec<usize>)> {
    // INSDC:4na:bin stores one byte per base (low nibble carries the code).
    // INSDC:4na:packed (rare) would be 2 bases per byte — detect via page_map
    // element counts vs payload length.
    let raw = decode_zip_encoding(decoded)?;
    let offsets = split_offsets(decoded, row_count, 1)?;
    let expected = offsets.last().copied().unwrap_or(0);
    if raw.len() == expected {
        let out: Vec<u8> = raw.iter().map(|b| decode_4na_nibble(b & 0x0F)).collect();
        return Ok((out, offsets));
    }
    if raw.len() * 2 == expected {
        let bases = unpack_4na(&raw, expected);
        return split_by_rows(decoded, bases, row_count, 1);
    }
    Err(Error::Format(format!(
        "dump: ALTREAD payload size {} doesn't match expected {} (4na:bin) or {} (4na:packed)",
        raw.len(),
        expected,
        expected / 2,
    )))
}

fn decode_4na_nibble(n: u8) -> u8 {
    const LUT: [u8; 16] = *b"NACMGRSVTWYHKDBN";
    LUT[(n & 0x0F) as usize]
}

fn materialize_quality(decoded: &DecodedBlob<'_>, row_count: u64) -> Result<(Vec<u8>, Vec<usize>)> {
    let raw = decode_quality_encoding(decoded)?;
    let ascii = phred_to_ascii(&raw);
    split_by_rows(decoded, ascii, row_count, 1)
}

fn materialize_ascii(decoded: &DecodedBlob<'_>, row_count: u64) -> Result<(Vec<u8>, Vec<usize>)> {
    let raw = decode_zip_encoding(decoded)?;
    let raw = if let Some(pm) = decoded.page_map.as_ref() {
        if pm.data_runs.is_empty() {
            raw
        } else {
            pm.expand_variable_data_runs(&raw)?
        }
    } else {
        raw
    };
    split_by_rows_already_expanded(decoded, raw, row_count, 1)
}

fn materialize_u32(decoded: &DecodedBlob<'_>, row_count: u64) -> Result<(Vec<u8>, Vec<usize>)> {
    let raw = decode_irzip_column(decoded)?;
    // `decode_irzip_column` already runs the page_map data_runs expansion,
    // so `raw` is flat row bytes at `entry_bytes = row_length * 4`.
    split_by_rows_already_expanded(decoded, raw, row_count, 4)
}

fn materialize_u8_array(
    decoded: &DecodedBlob<'_>,
    row_count: u64,
) -> Result<(Vec<u8>, Vec<usize>)> {
    let raw = decode_zip_encoding(decoded)?;
    let raw = expand_runs_if_any(decoded, raw, 1)?;
    split_by_rows_already_expanded(decoded, raw, row_count, 1)
}

fn materialize_hex_fallback(
    decoded: &DecodedBlob<'_>,
    row_count: u64,
) -> Result<(Vec<u8>, Vec<usize>)> {
    let raw = decode_zip_encoding(decoded).unwrap_or_else(|_| decoded.data.to_vec());
    // No structural info → treat as one monolithic row when row_count == 1,
    // otherwise split by equal shares if divisible.
    if row_count <= 1 {
        let offsets = vec![0, raw.len()];
        return Ok((raw, offsets));
    }
    let per_row = raw.len() / row_count as usize;
    if per_row * row_count as usize != raw.len() {
        return Err(Error::Format(format!(
            "dump: hex-fallback can't split {} bytes evenly across {row_count} rows",
            raw.len(),
        )));
    }
    let offsets: Vec<usize> = (0..=row_count as usize).map(|i| i * per_row).collect();
    Ok((raw, offsets))
}

/// Compute the element count for a bit-packed blob.
///
/// `bits_per_elem` is 2 for 2na, 4 for 4na, 8 for Phred/ASCII. `adjust`
/// from the envelope trims padding bits off the last byte of the blob.
fn elem_count(decoded: &DecodedBlob<'_>, data: &[u8], bits_per_elem: u32) -> usize {
    let total_bits = data.len() as u64 * 8;
    let adjust = decoded.adjust as u64;
    ((total_bits.saturating_sub(adjust)) / bits_per_elem as u64) as usize
}

/// Build per-row byte offsets from page_map lengths or fixed row_length.
fn split_offsets(
    decoded: &DecodedBlob<'_>,
    row_count: u64,
    elem_bytes: usize,
) -> Result<Vec<usize>> {
    if let Some(pm) = decoded.page_map.as_ref() {
        let lens = expand_leng_runs(pm);
        if lens.len() as u64 != row_count {
            return Err(Error::Format(format!(
                "dump: page_map expanded to {} rows, expected {row_count}",
                lens.len(),
            )));
        }
        let mut offsets = Vec::with_capacity(lens.len() + 1);
        let mut cur = 0usize;
        offsets.push(cur);
        for l in lens {
            cur += l as usize * elem_bytes;
            offsets.push(cur);
        }
        return Ok(offsets);
    }
    if let Some(rl) = decoded.row_length {
        let n = rl as usize * elem_bytes;
        let offsets: Vec<usize> = (0..=row_count as usize).map(|i| i * n).collect();
        return Ok(offsets);
    }
    Err(Error::Format(
        "dump: blob has neither page_map nor row_length — cannot split rows".into(),
    ))
}

/// `lengths[i]` repeated `leng_runs[i]` times → one entry per logical row.
fn expand_leng_runs(pm: &PageMap) -> Vec<u32> {
    let mut out = Vec::with_capacity(pm.total_rows() as usize);
    for (l, r) in pm.lengths.iter().zip(pm.leng_runs.iter()) {
        for _ in 0..*r {
            out.push(*l);
        }
    }
    out
}

/// For fixed-size element columns (u32, u8), run `expand_data_runs_bytes` if
/// the page_map carries data_runs — otherwise return as-is.
fn expand_runs_if_any(
    decoded: &DecodedBlob<'_>,
    data: Vec<u8>,
    elem_bytes: usize,
) -> Result<Vec<u8>> {
    if let Some(pm) = decoded.page_map.as_ref()
        && !pm.data_runs.is_empty()
    {
        return pm.expand_data_runs_bytes(&data, elem_bytes);
    }
    Ok(data)
}

/// After elements have been unpacked/expanded to one ASCII byte per elem
/// (2na → base, 4na → base, Phred+33 → char), slice by per-row element
/// counts with multiplier `mult`.
fn split_by_rows(
    decoded: &DecodedBlob<'_>,
    data: Vec<u8>,
    row_count: u64,
    elem_bytes: usize,
) -> Result<(Vec<u8>, Vec<usize>)> {
    let offsets = split_offsets(decoded, row_count, elem_bytes)?;
    let expected = offsets.last().copied().unwrap_or(0);
    if data.len() != expected {
        return Err(Error::Format(format!(
            "dump: decoded {} bytes but split plan needs {expected}",
            data.len(),
        )));
    }
    Ok((data, offsets))
}

/// Like [`split_by_rows`] but for byte streams that already include
/// data_runs expansion (e.g. `decode_irzip_column` result).
fn split_by_rows_already_expanded(
    decoded: &DecodedBlob<'_>,
    data: Vec<u8>,
    row_count: u64,
    elem_bytes: usize,
) -> Result<(Vec<u8>, Vec<usize>)> {
    split_by_rows(decoded, data, row_count, elem_bytes)
}

// ---------------------------------------------------------------------------
// Output formatting
// ---------------------------------------------------------------------------

fn cell_bytes(col: &OpenedColumn, row_id: i64) -> &[u8] {
    let cache = col.cache.as_ref().expect("cache populated by ensure_blob");
    let i = (row_id - cache.start_id) as usize;
    let lo = cache.offsets[i];
    let hi = cache.offsets[i + 1];
    &cache.bytes[lo..hi]
}

fn write_default<W: Write>(w: &mut W, row_id: i64, cols: &[OpenedColumn]) -> Result<()> {
    write!(w, "{row_id}:")?;
    for (i, col) in cols.iter().enumerate() {
        let sep = if i == 0 { " " } else { ", " };
        let bytes = cell_bytes(col, row_id);
        write!(w, "{sep}{}=", col.name)?;
        write_cell_default(w, col.kind, bytes)?;
    }
    writeln!(w)?;
    Ok(())
}

fn write_delimited<W: Write>(
    w: &mut W,
    row_id: i64,
    cols: &[OpenedColumn],
    delim: u8,
) -> Result<()> {
    write!(w, "{row_id}")?;
    for col in cols {
        w.write_all(&[delim])?;
        let bytes = cell_bytes(col, row_id);
        write_cell_delimited(w, col.kind, bytes, delim)?;
    }
    writeln!(w)?;
    Ok(())
}

fn write_json<W: Write>(w: &mut W, row_id: i64, cols: &[OpenedColumn]) -> Result<()> {
    let mut obj = serde_json::Map::with_capacity(cols.len() + 1);
    obj.insert("row_id".into(), json!(row_id));
    for col in cols {
        let bytes = cell_bytes(col, row_id);
        obj.insert(col.name.clone(), cell_to_json(col.kind, bytes));
    }
    serde_json::to_writer(&mut *w, &Value::Object(obj))
        .map_err(|e| Error::Format(e.to_string()))?;
    writeln!(w)?;
    Ok(())
}

fn write_cell_default<W: Write>(w: &mut W, kind: CellKind, bytes: &[u8]) -> Result<()> {
    match kind {
        CellKind::Dna2na
        | CellKind::Dna4naBin
        | CellKind::QualityPhred33
        | CellKind::AsciiBytes => {
            w.write_all(b"\"")?;
            w.write_all(bytes)?;
            w.write_all(b"\"")?;
        }
        CellKind::U32Array => {
            write_u32_array(w, bytes, b'[', b']', b", ")?;
        }
        CellKind::U8Array => {
            write_u8_array(w, bytes, b'[', b']', b", ")?;
        }
        CellKind::I32Scalar => {
            write_i32_scalar(w, bytes)?;
        }
        CellKind::HexRaw => {
            write_hex(w, bytes)?;
        }
    }
    Ok(())
}

fn write_cell_delimited<W: Write>(
    w: &mut W,
    kind: CellKind,
    bytes: &[u8],
    delim: u8,
) -> Result<()> {
    match kind {
        CellKind::Dna2na
        | CellKind::Dna4naBin
        | CellKind::QualityPhred33
        | CellKind::AsciiBytes => {
            write_quoted(w, bytes, delim)?;
        }
        CellKind::U32Array => {
            write_u32_array(w, bytes, 0, 0, b";")?;
        }
        CellKind::U8Array => {
            write_u8_array(w, bytes, 0, 0, b";")?;
        }
        CellKind::I32Scalar => {
            write_i32_scalar(w, bytes)?;
        }
        CellKind::HexRaw => {
            write_hex(w, bytes)?;
        }
    }
    Ok(())
}

fn cell_to_json(kind: CellKind, bytes: &[u8]) -> Value {
    match kind {
        CellKind::Dna2na
        | CellKind::Dna4naBin
        | CellKind::QualityPhred33
        | CellKind::AsciiBytes => Value::String(String::from_utf8_lossy(bytes).into_owned()),
        CellKind::U32Array => {
            let vals: Vec<Value> = bytes
                .chunks_exact(4)
                .map(|c| json!(u32::from_le_bytes(c.try_into().unwrap())))
                .collect();
            Value::Array(vals)
        }
        CellKind::U8Array => Value::Array(bytes.iter().map(|&b| json!(b)).collect()),
        CellKind::I32Scalar => {
            if bytes.len() >= 4 {
                let v = i32::from_le_bytes(bytes[..4].try_into().unwrap());
                json!(v)
            } else {
                Value::Null
            }
        }
        CellKind::HexRaw => {
            let mut s = String::with_capacity(bytes.len() * 2);
            for b in bytes {
                s.push_str(&format!("{b:02x}"));
            }
            Value::String(s)
        }
    }
}

fn write_u32_array<W: Write>(
    w: &mut W,
    bytes: &[u8],
    lead: u8,
    trail: u8,
    sep: &[u8],
) -> Result<()> {
    if lead != 0 {
        w.write_all(&[lead])?;
    }
    for (i, chunk) in bytes.chunks_exact(4).enumerate() {
        if i > 0 {
            w.write_all(sep)?;
        }
        let v = u32::from_le_bytes(chunk.try_into().unwrap());
        write!(w, "{v}")?;
    }
    if trail != 0 {
        w.write_all(&[trail])?;
    }
    Ok(())
}

fn write_u8_array<W: Write>(
    w: &mut W,
    bytes: &[u8],
    lead: u8,
    trail: u8,
    sep: &[u8],
) -> Result<()> {
    if lead != 0 {
        w.write_all(&[lead])?;
    }
    for (i, b) in bytes.iter().enumerate() {
        if i > 0 {
            w.write_all(sep)?;
        }
        write!(w, "{b}")?;
    }
    if trail != 0 {
        w.write_all(&[trail])?;
    }
    Ok(())
}

fn write_i32_scalar<W: Write>(w: &mut W, bytes: &[u8]) -> Result<()> {
    if bytes.len() >= 4 {
        let v = i32::from_le_bytes(bytes[..4].try_into().unwrap());
        write!(w, "{v}")?;
    } else {
        write!(w, "NA")?;
    }
    Ok(())
}

fn write_quoted<W: Write>(w: &mut W, bytes: &[u8], delim: u8) -> Result<()> {
    let needs_quote = bytes.contains(&delim) || bytes.contains(&b'"') || bytes.contains(&b'\n');
    if !needs_quote {
        w.write_all(bytes)?;
        return Ok(());
    }
    w.write_all(b"\"")?;
    for &b in bytes {
        if b == b'"' {
            w.write_all(b"\"\"")?;
        } else {
            w.write_all(&[b])?;
        }
    }
    w.write_all(b"\"")?;
    Ok(())
}

fn write_hex<W: Write>(w: &mut W, bytes: &[u8]) -> Result<()> {
    for b in bytes {
        write!(w, "{b:02x}")?;
    }
    Ok(())
}

/// Convenience for tests: run a dump to an in-memory `Vec<u8>`.
#[cfg(test)]
pub fn dump_to_vec<R: Read + Seek>(
    archive: &mut KarArchive<R>,
    sra_path: &Path,
    table: Option<&str>,
    spec: DumpSpec,
) -> Result<Vec<u8>> {
    let mut runner = DumpRunner::new(archive, sra_path, table, spec)?;
    let mut buf = Vec::new();
    runner.run(&mut buf)?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn infer_kind_table_matches_spec() {
        assert_eq!(infer_kind("READ"), CellKind::Dna2na);
        assert_eq!(infer_kind("CMP_READ"), CellKind::Dna2na);
        assert_eq!(infer_kind("ALTREAD"), CellKind::Dna4naBin);
        assert_eq!(infer_kind("QUALITY"), CellKind::QualityPhred33);
        assert_eq!(infer_kind("ORIGINAL_QUALITY"), CellKind::QualityPhred33);
        assert_eq!(infer_kind("NAME"), CellKind::AsciiBytes);
        assert_eq!(infer_kind("SPOT_NAME"), CellKind::AsciiBytes);
        assert_eq!(infer_kind("SPOT_GROUP"), CellKind::AsciiBytes);
        assert_eq!(infer_kind("READ_LEN"), CellKind::U32Array);
        assert_eq!(infer_kind("READ_START"), CellKind::U32Array);
        assert_eq!(infer_kind("READ_TYPE"), CellKind::U8Array);
        assert_eq!(infer_kind("READ_FILTER"), CellKind::U8Array);
        assert_eq!(infer_kind("X"), CellKind::I32Scalar);
        assert_eq!(infer_kind("Y"), CellKind::I32Scalar);
        assert_eq!(infer_kind("PRIMARY_ALIGNMENT_ID"), CellKind::HexRaw);
    }

    #[test]
    fn elem_bytes_per_kind() {
        assert_eq!(CellKind::Dna2na.elem_bytes(), 1);
        assert_eq!(CellKind::QualityPhred33.elem_bytes(), 1);
        assert_eq!(CellKind::AsciiBytes.elem_bytes(), 1);
        assert_eq!(CellKind::U32Array.elem_bytes(), 4);
        assert_eq!(CellKind::I32Scalar.elem_bytes(), 4);
    }

    #[test]
    fn resolve_column_selection_defaults_to_all() {
        let all = vec![
            "READ".to_string(),
            "QUALITY".to_string(),
            "NAME".to_string(),
        ];
        let picked = resolve_column_selection(&all, &[], &[]);
        assert_eq!(picked, all);
    }

    #[test]
    fn resolve_column_selection_respects_include() {
        let all = vec![
            "READ".to_string(),
            "QUALITY".to_string(),
            "NAME".to_string(),
        ];
        let req = vec!["READ".to_string(), "NAME".to_string()];
        let picked = resolve_column_selection(&all, &req, &[]);
        assert_eq!(picked, vec!["READ".to_string(), "NAME".to_string()]);
    }

    #[test]
    fn resolve_column_selection_applies_exclude() {
        let all = vec![
            "READ".to_string(),
            "QUALITY".to_string(),
            "NAME".to_string(),
        ];
        let excl = vec!["QUALITY".to_string()];
        let picked = resolve_column_selection(&all, &[], &excl);
        assert_eq!(picked, vec!["READ".to_string(), "NAME".to_string()]);
    }

    #[test]
    fn write_quoted_does_nothing_when_safe() {
        let mut buf = Vec::new();
        write_quoted(&mut buf, b"ACGT", b',').unwrap();
        assert_eq!(buf, b"ACGT");
    }

    #[test]
    fn write_quoted_wraps_when_delim_present() {
        let mut buf = Vec::new();
        write_quoted(&mut buf, b"A,C", b',').unwrap();
        assert_eq!(buf, b"\"A,C\"");
    }

    #[test]
    fn write_quoted_escapes_interior_quotes() {
        let mut buf = Vec::new();
        write_quoted(&mut buf, b"a\"b", b',').unwrap();
        assert_eq!(buf, b"\"a\"\"b\"");
    }

    #[test]
    fn cell_to_json_u32_array_is_numeric_array() {
        let bytes = [1u32.to_le_bytes(), 151u32.to_le_bytes()].concat();
        let v = cell_to_json(CellKind::U32Array, &bytes);
        assert_eq!(v, json!([1, 151]));
    }

    #[test]
    fn cell_to_json_dna_is_string() {
        let v = cell_to_json(CellKind::Dna2na, b"ACGT");
        assert_eq!(v, json!("ACGT"));
    }

    #[test]
    fn cell_to_json_hex_encodes_bytes() {
        let v = cell_to_json(CellKind::HexRaw, &[0x00, 0xff, 0xab]);
        assert_eq!(v, json!("00ffab"));
    }
}
