//! VDB blob → per-read rows. Format-agnostic.
//!
//! Each output format (`crate::parquet`, `crate::vortex`) pulls rows out of
//! the [`DecodedBlob`]s produced here via [`iter_spots`] / [`iter_reads`]
//! and pushes them into format-specific builders.
//!
//! Also hosts the length-mode resolution (including the data-sniff
//! fallback) because it's shared by both format writers.

use crate::convert::encoding::{is_pure_acgt, pack_2na, pack_4na};
use crate::convert::schema::{DnaPacking, LengthMode, LengthModeChoice};
use crate::error::{Error, Result};
use crate::pipeline::{decode_irzip_column, decode_raw, decode_zip_encoding};
use crate::vdb::cursor::VdbCursor;

// ---------------------------------------------------------------------------
// Length-mode resolution
// ---------------------------------------------------------------------------

pub fn resolve_length_mode(cursor: &VdbCursor, choice: LengthModeChoice) -> Result<LengthMode> {
    let detected = detect_length_mode(cursor);
    match (choice, detected) {
        (LengthModeChoice::Auto, mode) => Ok(mode),
        (LengthModeChoice::Fixed, LengthMode::Fixed { read_len }) => {
            Ok(LengthMode::Fixed { read_len })
        }
        (LengthModeChoice::Fixed, LengthMode::Variable) => Err(Error::Vdb(
            "--length-mode fixed requested but data has variable read lengths".into(),
        )),
        (LengthModeChoice::Variable, _) => Ok(LengthMode::Variable),
    }
}

fn detect_length_mode(cursor: &VdbCursor) -> LengthMode {
    // 1. Trust explicit metadata when all read lengths are uniform.
    if let Some(lengths) = cursor.metadata_read_lengths()
        && !lengths.is_empty()
        && lengths.iter().all(|&l| l == lengths[0])
    {
        return LengthMode::Fixed {
            read_len: lengths[0],
        };
    }
    // 2. Fall back to a data sniff. When the file has no READ_LEN column and
    //    no metadata read_lengths, it's often because every read is the same
    //    length (common case: Illumina paired-end). Check that every blob has
    //    the same (bases_per_blob / id_range / reads_per_spot) ratio and that
    //    the ratio divides evenly. If so, treat as Fixed.
    if cursor.read_len_col().is_some() {
        return LengthMode::Variable;
    }
    let rps = cursor.metadata_reads_per_spot().unwrap_or(1).max(1) as u64;
    let blobs = cursor.read_col().blobs();
    if blobs.is_empty() {
        return LengthMode::Variable;
    }

    let mut inferred_read_len: Option<u32> = None;
    for blob in blobs {
        if blob.id_range == 0 {
            continue;
        }
        let Ok(raw) = cursor.read_col().read_raw_blob_slice(blob.start_id) else {
            return LengthMode::Variable;
        };
        let Ok(decoded) = decode_raw(
            raw,
            cursor.read_col().meta().checksum_type,
            blob.id_range as u64,
        ) else {
            return LengthMode::Variable;
        };
        let total_bits = decoded.data.len() * 8;
        let adjust = decoded.adjust as usize;
        let actual_bases = total_bits.saturating_sub(adjust) / 2;
        let denom = (blob.id_range as u64) * rps;
        if denom == 0 || !(actual_bases as u64).is_multiple_of(denom) {
            return LengthMode::Variable;
        }
        let read_len = (actual_bases as u64 / denom) as u32;
        match inferred_read_len {
            None => inferred_read_len = Some(read_len),
            Some(existing) if existing == read_len => {}
            _ => return LengthMode::Variable,
        }
    }

    match inferred_read_len {
        Some(read_len) if read_len > 0 => LengthMode::Fixed { read_len },
        _ => LengthMode::Variable,
    }
}

// ---------------------------------------------------------------------------
// Per-blob decode (minimal: READ, QUALITY, READ_LEN, NAME)
// ---------------------------------------------------------------------------

pub struct DecodedBlob {
    /// Concatenated bases for all spots in the blob (ASCII).
    pub bases: Vec<u8>,
    /// Concatenated quality (Phred+33 ASCII). Empty if QUALITY column absent.
    pub quality: Vec<u8>,
    /// Per-read lengths, flat, length = total reads in blob.
    pub read_lengths: Vec<u32>,
    /// Reads per spot (uniform across the blob).
    pub reads_per_spot: usize,
    /// Per-spot names, length = spot count. Empty placeholder if NAME absent.
    pub names: Vec<Vec<u8>>,
}

impl DecodedBlob {
    pub fn spot_count(&self) -> usize {
        self.read_lengths.len() / self.reads_per_spot.max(1)
    }

    pub fn iter_spots(&self) -> SpotIter<'_> {
        SpotIter {
            blob: self,
            spot_idx: 0,
            base_offset: 0,
        }
    }
}

pub struct SpotIter<'a> {
    blob: &'a DecodedBlob,
    spot_idx: usize,
    base_offset: usize,
}

pub struct SpotView<'a> {
    pub name: &'a [u8],
    bases: &'a [u8],
    quality: &'a [u8],
    read_lengths: &'a [u32],
}

pub struct ReadView<'a> {
    pub sequence: &'a [u8],
    pub quality: &'a [u8],
}

impl<'a> Iterator for SpotIter<'a> {
    type Item = SpotView<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.spot_idx >= self.blob.spot_count() {
            return None;
        }
        let rps = self.blob.reads_per_spot.max(1);
        let lens = &self.blob.read_lengths[self.spot_idx * rps..(self.spot_idx + 1) * rps];
        let spot_len: usize = lens.iter().map(|&l| l as usize).sum();

        let bases_end = self.base_offset + spot_len;
        let bases = if bases_end <= self.blob.bases.len() {
            &self.blob.bases[self.base_offset..bases_end]
        } else {
            &[]
        };
        let quality = if !self.blob.quality.is_empty() && bases_end <= self.blob.quality.len() {
            &self.blob.quality[self.base_offset..bases_end]
        } else {
            &[]
        };
        let name: &[u8] = self
            .blob
            .names
            .get(self.spot_idx)
            .map(|v| v.as_slice())
            .unwrap_or(&[]);

        self.spot_idx += 1;
        self.base_offset = bases_end;
        Some(SpotView {
            name,
            bases,
            quality,
            read_lengths: lens,
        })
    }
}

impl<'a> SpotView<'a> {
    pub fn iter_reads(&self) -> ReadIter<'a> {
        ReadIter {
            spot: SpotView {
                name: self.name,
                bases: self.bases,
                quality: self.quality,
                read_lengths: self.read_lengths,
            },
            read_idx: 0,
            base_offset: 0,
        }
    }
}

pub struct ReadIter<'a> {
    spot: SpotView<'a>,
    read_idx: usize,
    base_offset: usize,
}

impl<'a> Iterator for ReadIter<'a> {
    type Item = ReadView<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.read_idx >= self.spot.read_lengths.len() {
            return None;
        }
        let len = self.spot.read_lengths[self.read_idx] as usize;
        let end = self.base_offset + len;
        let sequence = if end <= self.spot.bases.len() {
            &self.spot.bases[self.base_offset..end]
        } else {
            &[]
        };
        let quality = if !self.spot.quality.is_empty() && end <= self.spot.quality.len() {
            &self.spot.quality[self.base_offset..end]
        } else {
            &[]
        };
        self.read_idx += 1;
        self.base_offset = end;
        Some(ReadView { sequence, quality })
    }
}

#[allow(clippy::too_many_arguments)]
pub fn decode_one_blob(
    read_raw: &[u8],
    read_cs: u8,
    id_range: u64,
    quality_raw: &[u8],
    quality_cs: u8,
    read_len_raw: &[u8],
    read_len_cs: u8,
    name_raw: &[u8],
    name_cs: u8,
    metadata_reads_per_spot: Option<usize>,
) -> Result<DecodedBlob> {
    // READ
    let read_decoded = decode_raw(read_raw, read_cs, id_range)?;
    let total_bits = read_decoded.data.len() * 8;
    let adjust = read_decoded.adjust as usize;
    let actual_bases = total_bits.saturating_sub(adjust) / 2;
    let bases = crate::vdb::encoding::unpack_2na(&read_decoded.data, actual_bases);

    // QUALITY
    let quality: Vec<u8> = if !quality_raw.is_empty() {
        let qdecoded = decode_raw(quality_raw, quality_cs, id_range)?;
        let qpage_map = qdecoded.page_map.clone();
        let mut qdata = decode_zip_encoding(&qdecoded);
        if let Some(ref pm) = qpage_map
            && !pm.data_runs.is_empty()
        {
            qdata = pm.expand_variable_data_runs(&qdata);
        }
        let all_valid_ascii =
            qdata.len() == bases.len() && qdata.iter().all(|&b| (33..=126).contains(&b));
        if all_valid_ascii {
            qdata
        } else {
            crate::vdb::encoding::phred_to_ascii(&qdata)
        }
    } else {
        Vec::new()
    };

    // READ_LEN
    let (read_lengths, reads_per_spot): (Vec<u32>, usize) = if !read_len_raw.is_empty() {
        let rldecoded = decode_raw(read_len_raw, read_len_cs, id_range)?;
        let rps = rldecoded
            .page_map
            .as_ref()
            .and_then(|pm| pm.lengths.first().copied())
            .unwrap_or(1) as usize;
        let rl_bytes = decode_irzip_column(&rldecoded);
        let lengths: Vec<u32> = rl_bytes
            .chunks_exact(4)
            .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
            .collect();
        (lengths, rps.max(1))
    } else {
        // No READ_LEN column. The blob's `id_range` is the authoritative
        // spot count; derive spot_len by dividing total bases by that, then
        // split each spot into `reads_per_spot` equal-size reads when
        // metadata tells us there's more than one read per spot.
        let rps = metadata_reads_per_spot.unwrap_or(1).max(1);
        let n_spots = id_range as usize;
        let spot_len = bases.len().checked_div(n_spots).unwrap_or(bases.len()) as u32;
        let read_len = (spot_len / rps as u32).max(1);
        let mut lengths = Vec::with_capacity(n_spots * rps);
        for _ in 0..n_spots {
            for _ in 0..rps {
                lengths.push(read_len);
            }
        }
        (lengths, rps)
    };

    // NAME (from page_map: variable-width strings)
    let names: Vec<Vec<u8>> = if !name_raw.is_empty() {
        let ndecoded = decode_raw(name_raw, name_cs, id_range)?;
        let name_bytes = decode_zip_encoding(&ndecoded);
        let mut out = Vec::new();
        if let Some(ref pm) = ndecoded.page_map {
            let mut offset = 0usize;
            for (len, run) in pm.lengths.iter().zip(pm.leng_runs.iter()) {
                let nlen = *len as usize;
                for _ in 0..*run {
                    if offset + nlen <= name_bytes.len() {
                        out.push(name_bytes[offset..offset + nlen].to_vec());
                        offset += nlen;
                    }
                }
            }
        }
        out
    } else {
        Vec::new()
    };

    Ok(DecodedBlob {
        bases,
        quality,
        read_lengths,
        reads_per_spot,
        names,
    })
}

// ---------------------------------------------------------------------------
// DNA sequence packing router
// ---------------------------------------------------------------------------

/// Pack ASCII DNA bases according to the chosen packing, using `pack_2na`
/// when the input is pure-ACGT and falling back to `pack_4na` when it
/// contains IUPAC ambiguity codes.
pub fn pack_sequence(ascii: &[u8], packing: DnaPacking) -> Vec<u8> {
    match packing {
        DnaPacking::Ascii => ascii.to_vec(),
        DnaPacking::TwoNa => {
            if is_pure_acgt(ascii) {
                pack_2na(ascii)
            } else {
                pack_4na(ascii)
            }
        }
        DnaPacking::FourNa => pack_4na(ascii),
    }
}
