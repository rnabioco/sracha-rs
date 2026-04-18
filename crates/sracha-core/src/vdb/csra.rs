//! High-level cSRA cursor — the user-facing API for reference-compressed
//! aligned SRA archives.
//!
//! Combines [`AlignmentCursor`] (PRIMARY_ALIGNMENT), [`ReferenceCursor`]
//! (REFERENCE), and a SEQUENCE-side column set (CMP_READ,
//! PRIMARY_ALIGNMENT_ID, READ_LEN, READ_TYPE, QUALITY) into one object
//! that can decode a spot's full bases + quality with a single call.
//! This is the building block Phase 3 wires into the FASTQ pipeline.

use std::io::{Read, Seek};
use std::path::Path;

use crate::error::{Error, Result};
use crate::vdb::alignment::AlignmentCursor;
use crate::vdb::blob::{self, DecodedBlob};
use crate::vdb::inspect;
use crate::vdb::kar::KarArchive;
use crate::vdb::kdb::ColumnReader;
use crate::vdb::reference::ReferenceCursor;
use crate::vdb::restore::{align_restore_read, seq_restore_read};

/// Inspect the KAR archive at `sra_path` and return true if it looks like
/// a reference-compressed cSRA we can decode via `CsraCursor`: SEQUENCE
/// has a physical CMP_READ column, PRIMARY_ALIGNMENT table is present,
/// and REFERENCE table is present. Archives that fail any of these
/// checks fall through to the regular VDB decode path.
pub fn looks_like_decodable_csra(sra_path: &Path) -> Result<bool> {
    let file = std::fs::File::open(sra_path)?;
    let archive = KarArchive::open(std::io::BufReader::new(file))?;
    let keys = archive.entries().keys();
    let mut has_seq_cmp_read = false;
    let mut has_primary = false;
    let mut has_reference = false;
    for k in keys {
        if k.ends_with("tbl/SEQUENCE/col/CMP_READ") || k.contains("/tbl/SEQUENCE/col/CMP_READ/") {
            has_seq_cmp_read = true;
        }
        if k == "tbl/PRIMARY_ALIGNMENT" || k.ends_with("/tbl/PRIMARY_ALIGNMENT") {
            has_primary = true;
        }
        if k == "tbl/REFERENCE" || k.ends_with("/tbl/REFERENCE") {
            has_reference = true;
        }
    }
    Ok(has_seq_cmp_read && has_primary && has_reference)
}

pub struct CsraCursor {
    // SEQUENCE-side columns
    cmp_read: ColumnReader,
    primary_alignment_id: ColumnReader,
    read_len: ColumnReader,
    read_type: ColumnReader,
    quality: ColumnReader,

    alignment: AlignmentCursor,
    reference: ReferenceCursor,

    row_count: u64,
    first_row: i64,
}

/// Summary stats for [`CsraCursor::write_fastq`].
#[derive(Debug, Clone, Copy)]
pub struct FastqStats {
    pub spots: u64,
}

/// Per-spot decoded values.
#[derive(Debug, Clone)]
pub struct SpotRead {
    /// Reconstructed bases in 4na-bin (A=1, C=2, G=4, T=8, N=15). Length
    /// equals `read_lens.iter().sum::<u32>()`.
    pub bases: Vec<u8>,
    /// Phred quality bytes, one per base. Same length as `bases`.
    pub quality: Vec<u8>,
    /// Per-read length (same as SEQUENCE.READ_LEN for this spot).
    pub read_lens: Vec<u32>,
    /// Per-read type bitfield (SEQUENCE.READ_TYPE).
    pub read_types: Vec<u8>,
}

impl CsraCursor {
    pub fn open<R: Read + Seek>(archive: &mut KarArchive<R>, sra_path: &Path) -> Result<Self> {
        let col_base = inspect::column_base_path_public(archive, Some("SEQUENCE"))?;
        let open_col = |archive: &mut KarArchive<R>, name: &str| -> Result<ColumnReader> {
            ColumnReader::open(archive, &format!("{col_base}/{name}"), sra_path)
                .map_err(|e| Error::Vdb(format!("SEQUENCE/{name}: {e}")))
        };
        let cmp_read = open_col(archive, "CMP_READ")?;
        let primary_alignment_id = open_col(archive, "PRIMARY_ALIGNMENT_ID")?;
        let read_len = open_col(archive, "READ_LEN")?;
        let read_type = open_col(archive, "READ_TYPE")?;
        let quality = open_col(archive, "QUALITY")?;

        let alignment = AlignmentCursor::open(archive, sra_path)?;
        let reference = ReferenceCursor::open(archive, sra_path)?;

        let first_row = cmp_read.first_row_id().unwrap_or(1);
        let row_count = cmp_read.row_count();

        Ok(Self {
            cmp_read,
            primary_alignment_id,
            read_len,
            read_type,
            quality,
            alignment,
            reference,
            row_count,
            first_row,
        })
    }

    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    pub fn first_row(&self) -> i64 {
        self.first_row
    }

    /// Decode the archive and write a single FASTQ file named
    /// `{accession}.fastq` into `output_dir`. Returns the output path and
    /// the number of spots written. For v1 this ignores split / compression
    /// / stdout config and always writes one uncompressed file; richer
    /// options land when cSRA moves to the batched pipeline.
    pub fn write_fastq_to_dir(
        &self,
        accession: &str,
        output_dir: &Path,
    ) -> Result<(std::path::PathBuf, FastqStats)> {
        std::fs::create_dir_all(output_dir).map_err(|e| {
            Error::Vdb(format!("cSRA output: create {}: {e}", output_dir.display()))
        })?;
        let out_path = output_dir.join(format!("{accession}.fastq"));
        let out_file = std::fs::File::create(&out_path)
            .map_err(|e| Error::Vdb(format!("cSRA output: create {}: {e}", out_path.display())))?;
        let buf_writer = std::io::BufWriter::new(out_file);
        let stats = self.write_fastq(accession, buf_writer)?;
        Ok((out_path, stats))
    }

    /// Write a minimal FASTQ rendering of every spot in the archive to
    /// `writer`, matching `fasterq-dump --split-files`'s single-file
    /// default format:
    ///
    /// ```text
    /// @{accession}.{spot_id} {spot_id} length={total_len}
    /// {bases}
    /// +{accession}.{spot_id} {spot_id} length={total_len}
    /// {phred+33 quality}
    /// ```
    ///
    /// This intentionally bypasses the existing FASTQ pipeline for the
    /// first end-to-end cSRA integration so we can validate byte-parity
    /// with sra-tools before plumbing into the split / compression /
    /// naming subsystems in Phase 3c.
    pub fn write_fastq<W: std::io::Write>(
        &self,
        accession: &str,
        mut writer: W,
    ) -> Result<FastqStats> {
        use crate::vdb::restore::fourna_to_ascii;
        let mut spots = 0u64;
        for row_id in self.first_row..(self.first_row + self.row_count as i64) {
            let spot = self.read_spot(row_id)?;
            let ascii = fourna_to_ascii(&spot.bases);
            let total: u32 = spot.read_lens.iter().sum();
            let qual: Vec<u8> = spot.quality.iter().map(|q| q.wrapping_add(33)).collect();

            writeln!(writer, "@{accession}.{row_id} {row_id} length={total}")
                .map_err(|e| Error::Vdb(format!("fastq write: {e}")))?;
            writer
                .write_all(&ascii)
                .map_err(|e| Error::Vdb(format!("fastq write: {e}")))?;
            writer
                .write_all(b"\n")
                .map_err(|e| Error::Vdb(format!("fastq write: {e}")))?;
            writeln!(writer, "+{accession}.{row_id} {row_id} length={total}")
                .map_err(|e| Error::Vdb(format!("fastq write: {e}")))?;
            writer
                .write_all(&qual)
                .map_err(|e| Error::Vdb(format!("fastq write: {e}")))?;
            writer
                .write_all(b"\n")
                .map_err(|e| Error::Vdb(format!("fastq write: {e}")))?;
            spots += 1;
        }
        Ok(FastqStats { spots })
    }

    /// Decode one SEQUENCE row's full bases + quality.
    pub fn read_spot(&self, row_id: i64) -> Result<SpotRead> {
        let align_ids = read_i64_row(&self.primary_alignment_id, row_id)?;
        let read_lens = read_u32_row(&self.read_len, row_id)?;
        let read_types = read_byte_row(&self.read_type, row_id)?;
        let cmp_read_2na = read_2na_row(&self.cmp_read, row_id)?;
        let quality = read_byte_row(&self.quality, row_id)?;

        if read_lens.len() != align_ids.len() || read_types.len() != align_ids.len() {
            return Err(Error::Vdb(format!(
                "csra row {row_id}: inconsistent per-read array lengths \
                 (align_ids={}, read_lens={}, read_types={})",
                align_ids.len(),
                read_lens.len(),
                read_types.len(),
            )));
        }

        // Splice via seq_restore_read. fetch_aligned resolves an alignment
        // row id to its reference-oriented bases via align_restore_read.
        let bases = seq_restore_read(
            &cmp_read_2na,
            &align_ids,
            &read_lens,
            &read_types,
            |alignment_id| {
                let row = self.alignment.read_row(alignment_id)?;
                let ref_read = self
                    .reference
                    .fetch_span(row.global_ref_start, row.ref_len)?;
                align_restore_read(
                    &ref_read,
                    &row.has_mismatch,
                    &row.mismatch,
                    &row.has_ref_offset,
                    &row.ref_offset,
                    row.has_mismatch.len(),
                )
            },
        )?;

        Ok(SpotRead {
            bases,
            quality,
            read_lens,
            read_types,
        })
    }
}

// ---------------------------------------------------------------------------
// Per-row decoders for SEQUENCE columns. Mirror `alignment.rs`'s helpers but
// at the element widths the SEQUENCE side uses.
// ---------------------------------------------------------------------------

fn read_u32_row(col: &ColumnReader, row_id: i64) -> Result<Vec<u32>> {
    let (bytes, pm, row_offset) = read_variable_payload(col, row_id, VarEncoding::IrzipBits(32))?;
    let record_lens = pm.data_record_lengths();
    let rec_idx = resolve_record_idx(&pm, row_offset, row_id)?;
    let start: usize = record_lens
        .iter()
        .take(rec_idx)
        .map(|&n| n as usize * 4)
        .sum();
    let len_elems = record_lens[rec_idx] as usize;
    let end = start + len_elems * 4;
    if end > bytes.len() {
        return Err(Error::Vdb(format!(
            "u32 row {row_id}: slice [{start}..{end}] past payload {}",
            bytes.len()
        )));
    }
    Ok(bytes[start..end]
        .chunks_exact(4)
        .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect())
}

fn read_i64_row(col: &ColumnReader, row_id: i64) -> Result<Vec<i64>> {
    let (bytes, pm, row_offset) = read_variable_payload(col, row_id, VarEncoding::IrzipBits(64))?;
    let record_lens = pm.data_record_lengths();
    let rec_idx = resolve_record_idx(&pm, row_offset, row_id)?;
    let start: usize = record_lens
        .iter()
        .take(rec_idx)
        .map(|&n| n as usize * 8)
        .sum();
    let len_elems = record_lens[rec_idx] as usize;
    let end = start + len_elems * 8;
    if end > bytes.len() {
        return Err(Error::Vdb(format!(
            "i64 row {row_id}: slice [{start}..{end}] past payload {}",
            bytes.len()
        )));
    }
    Ok(bytes[start..end]
        .chunks_exact(8)
        .map(|c| i64::from_le_bytes([c[0], c[1], c[2], c[3], c[4], c[5], c[6], c[7]]))
        .collect())
}

fn read_byte_row(col: &ColumnReader, row_id: i64) -> Result<Vec<u8>> {
    let (bytes, pm, row_offset) = read_variable_payload(col, row_id, VarEncoding::Zip)?;
    let record_lens = pm.data_record_lengths();
    let rec_idx = resolve_record_idx(&pm, row_offset, row_id)?;
    let start: usize = record_lens.iter().take(rec_idx).map(|&n| n as usize).sum();
    let len = record_lens[rec_idx] as usize;
    let end = start + len;
    if end > bytes.len() {
        return Err(Error::Vdb(format!(
            "byte row {row_id}: slice [{start}..{end}] past payload {}",
            bytes.len()
        )));
    }
    Ok(bytes[start..end].to_vec())
}

/// Read one SEQUENCE.CMP_READ row's bases as 4na-bin. The underlying column
/// is 2na-packed MSB-first (same encoding as REFERENCE.CMP_READ). The
/// page_map record lengths are in bases (nucleotides), not bits.
fn read_2na_row(col: &ColumnReader, row_id: i64) -> Result<Vec<u8>> {
    let blob = col
        .find_blob(row_id)
        .ok_or_else(|| Error::Vdb(format!("2na row: no blob for row {row_id}")))?;
    let raw = col.read_raw_blob_slice(row_id)?;
    let decoded = blob::decode_blob(raw, col.meta().checksum_type, u64::from(blob.id_range), 2)?;
    let pm = decoded
        .page_map
        .as_ref()
        .ok_or_else(|| Error::Vdb("SEQUENCE.CMP_READ: page_map required".into()))?;
    let record_lens = pm.data_record_lengths();
    let row_offset = (row_id - blob.start_id) as usize;
    let rec_idx = resolve_record_idx(pm, row_offset, row_id)?;
    let len_bases = record_lens[rec_idx] as usize;
    if len_bases == 0 {
        return Ok(Vec::new());
    }
    let start_bits: usize = record_lens
        .iter()
        .take(rec_idx)
        .map(|&n| n as usize * 2)
        .sum();

    const LUT_2NA_TO_4NA: [u8; 4] = [0x1, 0x2, 0x4, 0x8]; // A C G T
    let mut out = Vec::with_capacity(len_bases);
    for i in 0..len_bases {
        let bit_idx = start_bits + i * 2;
        let byte = bit_idx / 8;
        let shift = 6 - (bit_idx % 8);
        let b = decoded.data.get(byte).copied().ok_or_else(|| {
            Error::Vdb(format!(
                "SEQUENCE.CMP_READ row {row_id}: bit {bit_idx} past payload"
            ))
        })?;
        let code = (b >> shift) & 0x03;
        out.push(LUT_2NA_TO_4NA[code as usize]);
    }
    Ok(out)
}

#[derive(Clone, Copy)]
enum VarEncoding {
    Zip,
    IrzipBits(u32),
}

fn read_variable_payload(
    col: &ColumnReader,
    row_id: i64,
    enc: VarEncoding,
) -> Result<(Vec<u8>, blob::PageMap, usize)> {
    let blob = col
        .find_blob(row_id)
        .ok_or_else(|| Error::Vdb(format!("no blob for row {row_id}")))?;
    let raw = col.read_raw_blob_slice(row_id)?;
    let decoded = blob::decode_blob(raw, col.meta().checksum_type, u64::from(blob.id_range), 8)?;
    let pm = decoded
        .page_map
        .clone()
        .ok_or_else(|| Error::Vdb("variable column: page_map required".into()))?;
    let bytes = match enc {
        VarEncoding::Zip => decode_bytes_payload(&decoded)?,
        VarEncoding::IrzipBits(bits) => decode_integer_bytes(&decoded, bits)?,
    };
    Ok((bytes, pm, (row_id - blob.start_id) as usize))
}

fn decode_bytes_payload(decoded: &DecodedBlob<'_>) -> Result<Vec<u8>> {
    let hdr = decoded.headers.first();
    let osize = hdr.map(|h| h.osize as usize).unwrap_or(decoded.data.len());
    if decoded.data.len() == osize {
        return Ok(decoded.data.to_vec());
    }
    if let Ok(out) = blob::deflate_decompress(&decoded.data, osize)
        && out.len() == osize
    {
        return Ok(out);
    }
    Err(Error::Vdb(format!(
        "byte column: no decoder succeeded (data.len={}, osize={osize})",
        decoded.data.len()
    )))
}

fn decode_integer_bytes(decoded: &DecodedBlob<'_>, elem_bits: u32) -> Result<Vec<u8>> {
    let hdr = decoded.headers.first();
    let osize = hdr.map(|h| h.osize as usize).unwrap_or(decoded.data.len());
    if let Some(h) = hdr
        && !h.ops.is_empty()
    {
        let planes = h.ops[0];
        let min = h.args.first().copied().unwrap_or(0);
        let slope = h.args.get(1).copied().unwrap_or(0);
        let num_elems = (osize as u32) / (elem_bits / 8);
        let series2 = h
            .args
            .get(2)
            .and_then(|&m2| h.args.get(3).map(|&s2| (m2, s2)));
        return blob::irzip_decode(
            &decoded.data,
            elem_bits,
            num_elems,
            min,
            slope,
            planes,
            series2,
        );
    }
    if decoded.data.len() == osize {
        return Ok(decoded.data.to_vec());
    }
    if let Ok(out) = blob::deflate_decompress(&decoded.data, osize)
        && out.len() == osize
    {
        return Ok(out);
    }
    Err(Error::Vdb(format!(
        "integer column: no decoder succeeded (elem_bits={elem_bits}, data.len={}, osize={osize})",
        decoded.data.len()
    )))
}

fn resolve_record_idx(pm: &blob::PageMap, logical_offset: usize, row_id: i64) -> Result<usize> {
    if pm.data_runs.is_empty() {
        return Ok(logical_offset);
    }
    let mut seen = 0usize;
    for (i, &repeat) in pm.data_runs.iter().enumerate() {
        let end = seen + repeat as usize;
        if logical_offset < end {
            return Ok(i);
        }
        seen = end;
    }
    Err(Error::Vdb(format!(
        "row {row_id}: logical offset {logical_offset} outside data_runs"
    )))
}
