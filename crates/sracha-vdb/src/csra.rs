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

use crate::alignment::AlignmentCursor;
use crate::cache::{CachedColumn, ColumnKind};
use crate::error::{Error, Result};
use crate::inspect;
use crate::kar::KarArchive;
use crate::kdb::ColumnReader;
use crate::reference::ReferenceCursor;
use crate::restore::{align_restore_read, seq_restore_read};

/// Given a `.sra` path, return the canonical `.sra.vdbcache` sibling path
/// (NCBI's default layout — `sracha fetch` saves the vdbcache with that
/// suffix when the SDL response lists one). The file may or may not
/// exist; callers are expected to gate on `.exists()`.
pub fn vdbcache_sidecar_path(sra_path: &Path) -> std::path::PathBuf {
    let mut p = sra_path.as_os_str().to_owned();
    p.push(".vdbcache");
    std::path::PathBuf::from(p)
}

/// Does the KAR TOC contain `tbl/{table}` as a directory entry (either at
/// the archive root or nested under a single accession prefix)?
pub(crate) fn archive_has_table<R: Read + Seek>(archive: &KarArchive<R>, table: &str) -> bool {
    let suffix = format!("/tbl/{table}");
    let exact = format!("tbl/{table}");
    archive
        .entries()
        .keys()
        .any(|k| k == &exact || k.ends_with(&suffix))
}

fn archive_has_seq_cmp_read<R: Read + Seek>(archive: &KarArchive<R>) -> bool {
    let exact = "tbl/SEQUENCE/col/CMP_READ";
    let nested = "/tbl/SEQUENCE/col/CMP_READ";
    archive.entries().keys().any(|k| {
        k == exact
            || k.ends_with(nested)
            || k.starts_with(&format!("{exact}/"))
            || k.contains(&format!("{nested}/"))
    })
}

/// True when the archive has a `REFERENCE/col/CMP_READ` column — i.e.
/// reference bases are embedded in the archive rather than fetched from
/// an external refseq service. The detector is a cheap TOC scan so the
/// caller can surface an actionable error without attempting to open
/// the column.
fn archive_has_reference_cmp_read<R: Read + Seek>(archive: &KarArchive<R>) -> bool {
    let exact = "tbl/REFERENCE/col/CMP_READ";
    let nested = "/tbl/REFERENCE/col/CMP_READ";
    archive.entries().keys().any(|k| {
        k == exact
            || k.ends_with(nested)
            || k.starts_with(&format!("{exact}/"))
            || k.contains(&format!("{nested}/"))
    })
}

/// Inspect the KAR archive at `sra_path` (and optional `.vdbcache`
/// sidecar) and return true if the pair looks like a reference-
/// compressed cSRA decodable by `CsraCursor`. `SEQUENCE/col/CMP_READ`
/// must live in the main archive (the SEQUENCE half is never
/// sidecar'd). `PRIMARY_ALIGNMENT` and `REFERENCE` may live in either
/// archive — older monolithic cSRA (VDB-3418) keeps them in main,
/// modern NCBI uploads split them into `.sra.vdbcache`.
///
/// Pure TOC scan — no column reads — so safe to call up-front from
/// `pipeline::run_fastq`.
pub fn looks_like_decodable_csra(sra_path: &Path, vdbcache_path: Option<&Path>) -> Result<bool> {
    let main = open_kar(sra_path)?;
    let cache = match vdbcache_path {
        Some(p) if p.exists() => Some(open_kar(p)?),
        _ => None,
    };

    let has_seq_cmp_read = archive_has_seq_cmp_read(&main);
    let has_primary = archive_has_table(&main, "PRIMARY_ALIGNMENT")
        || cache
            .as_ref()
            .map(|c| archive_has_table(c, "PRIMARY_ALIGNMENT"))
            .unwrap_or(false);
    let has_reference = archive_has_table(&main, "REFERENCE")
        || cache
            .as_ref()
            .map(|c| archive_has_table(c, "REFERENCE"))
            .unwrap_or(false);

    Ok(has_seq_cmp_read && has_primary && has_reference)
}

fn open_kar(path: &Path) -> Result<KarArchive<std::io::BufReader<std::fs::File>>> {
    let file = std::fs::File::open(path)?;
    KarArchive::open(std::io::BufReader::new(file))
}

/// Build the user-facing error for archives whose REFERENCE table has
/// no embedded `CMP_READ` — i.e. reference bases are fetched from an
/// external NCBI refseq service (common on modern SRR-prefix cSRA).
/// sracha doesn't yet implement the external fetcher; returning an
/// actionable message beats the opaque "idx1 not found" that bubbles
/// up from `ColumnReader::open`.
fn external_refseq_error() -> Error {
    Error::Format(
        "cSRA: REFERENCE table has no embedded CMP_READ column — reference \
         bases are stored externally (fetched from NCBI refseq by SEQ_ID). \
         sracha does not yet implement external refseq fetch; decode this \
         archive with `fasterq-dump` for now."
            .into(),
    )
}

/// Build the user-facing error for archives whose SEQUENCE table omits
/// a physical READ_LEN column because every spot has the same
/// fixed-length layout (values live in `tbl/SEQUENCE/md/cur/col/
/// READ_LEN/row` as static metadata). sracha's CsraCursor currently
/// requires physical READ_LEN; the static-metadata fallback is tracked
/// as a follow-up.
fn fixed_length_readlen_error() -> Error {
    Error::Format(
        "cSRA: SEQUENCE.READ_LEN is not a physical column — this archive \
         encodes a fixed spot layout in static metadata, which sracha does \
         not yet synthesize. Decode this archive with `fasterq-dump` for now."
            .into(),
    )
}

/// Does `col_base/{col_name}` exist as a directory under the SEQUENCE
/// table? Used to check for physical columns without opening them.
fn archive_has_seq_column<R: Read + Seek>(
    archive: &KarArchive<R>,
    col_base: &str,
    col_name: &str,
) -> bool {
    let exact = format!("{col_base}/{col_name}");
    let prefix = format!("{exact}/");
    archive
        .entries()
        .keys()
        .any(|k| k == &exact || k.starts_with(&prefix))
}

pub struct CsraCursor {
    // SEQUENCE-side columns
    cmp_read: CachedColumn,
    primary_alignment_id: CachedColumn,
    read_len: CachedColumn,
    read_type: CachedColumn,
    quality: CachedColumn,

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
        Self::open_any::<R>(archive, sra_path, None)
    }

    /// vdbcache-aware open: SEQUENCE columns come from `main`, but
    /// `PRIMARY_ALIGNMENT` / `REFERENCE` are routed to whichever archive
    /// carries them (modern NCBI cSRA keeps them in the `.sra.vdbcache`
    /// sidecar). Pass `None` for monolithic archives like VDB-3418 — the
    /// legacy behaviour.
    pub fn open_any<R: Read + Seek>(
        main: &mut KarArchive<R>,
        main_path: &Path,
        vdbcache: Option<(&mut KarArchive<R>, &Path)>,
    ) -> Result<Self> {
        // SEQUENCE-side columns always live in the main archive — the
        // vdbcache only carries the alignment + reference halves.
        let col_base = inspect::column_base_path_public(main, Some("SEQUENCE"))?;
        let open_col = |archive: &mut KarArchive<R>, name: &str| -> Result<ColumnReader> {
            ColumnReader::open(archive, &format!("{col_base}/{name}"), main_path)
                .map_err(|e| Error::Format(format!("SEQUENCE/{name}: {e}")))
        };
        // Pre-flight: surface actionable errors for known-unsupported
        // shapes before we start opening columns. These checks are cheap
        // TOC scans so the error fires with useful guidance rather than
        // an opaque "idx1 not found" deep in the decoder.
        if !archive_has_seq_column(main, &col_base, "READ_LEN") {
            return Err(fixed_length_readlen_error());
        }
        let main_has_ref_cmp_read = archive_has_reference_cmp_read(main);
        let cache_has_ref_cmp_read =
            matches!(&vdbcache, Some((c, _)) if archive_has_reference_cmp_read(c));
        if !main_has_ref_cmp_read && !cache_has_ref_cmp_read {
            return Err(external_refseq_error());
        }

        let cmp_read = open_col(main, "CMP_READ")?;
        let primary_alignment_id = open_col(main, "PRIMARY_ALIGNMENT_ID")?;
        let read_len = open_col(main, "READ_LEN")?;
        let read_type = open_col(main, "READ_TYPE")?;
        let quality = open_col(main, "QUALITY")?;

        let primary_in_main = archive_has_table(main, "PRIMARY_ALIGNMENT");
        let reference_in_main = archive_has_table(main, "REFERENCE");

        // Destructure vdbcache once so we can reborrow the inner refs
        // per sub-cursor; each `AlignmentCursor::open` /
        // `ReferenceCursor::open` call only borrows the archive for the
        // duration of its scope.
        let (alignment, reference) = match vdbcache {
            None => {
                if !archive_has_reference_cmp_read(main) {
                    return Err(external_refseq_error());
                }
                let alignment = AlignmentCursor::open(main, main_path)?;
                let reference = ReferenceCursor::open(main, main_path)?;
                (alignment, reference)
            }
            Some((cache, cache_path)) => {
                let alignment = if primary_in_main {
                    AlignmentCursor::open(main, main_path)?
                } else if archive_has_table(cache, "PRIMARY_ALIGNMENT") {
                    AlignmentCursor::open(cache, cache_path)?
                } else {
                    return Err(Error::Format(
                        "cSRA: PRIMARY_ALIGNMENT table not found in main archive or vdbcache"
                            .into(),
                    ));
                };
                let reference = if reference_in_main {
                    if !archive_has_reference_cmp_read(main) {
                        return Err(external_refseq_error());
                    }
                    ReferenceCursor::open(main, main_path)?
                } else if archive_has_table(cache, "REFERENCE") {
                    if !archive_has_reference_cmp_read(cache) {
                        return Err(external_refseq_error());
                    }
                    ReferenceCursor::open(cache, cache_path)?
                } else {
                    return Err(Error::Format(
                        "cSRA: REFERENCE table not found in main archive or vdbcache".into(),
                    ));
                };
                (alignment, reference)
            }
        };

        let first_row = cmp_read.first_row_id().unwrap_or(1);
        let row_count = cmp_read.row_count();

        Ok(Self {
            cmp_read: CachedColumn::new(cmp_read, ColumnKind::TwoNa),
            primary_alignment_id: CachedColumn::new(
                primary_alignment_id,
                ColumnKind::Irzip { elem_bits: 64 },
            ),
            read_len: CachedColumn::new(read_len, ColumnKind::Irzip { elem_bits: 32 }),
            read_type: CachedColumn::new(read_type, ColumnKind::Zip),
            quality: CachedColumn::new(quality, ColumnKind::Zip),
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
            Error::Format(format!("cSRA output: create {}: {e}", output_dir.display()))
        })?;
        let out_path = output_dir.join(format!("{accession}.fastq"));
        let out_file = std::fs::File::create(&out_path).map_err(|e| {
            Error::Format(format!("cSRA output: create {}: {e}", out_path.display()))
        })?;
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
        use crate::restore::fourna_to_ascii;
        let mut spots = 0u64;
        for row_id in self.first_row..(self.first_row + self.row_count as i64) {
            let spot = self.read_spot(row_id)?;
            let ascii = fourna_to_ascii(&spot.bases);
            let total: u32 = spot.read_lens.iter().sum();
            let qual: Vec<u8> = spot.quality.iter().map(|q| q.wrapping_add(33)).collect();

            writeln!(writer, "@{accession}.{row_id} {row_id} length={total}")
                .map_err(|e| Error::Format(format!("fastq write: {e}")))?;
            writer
                .write_all(&ascii)
                .map_err(|e| Error::Format(format!("fastq write: {e}")))?;
            writer
                .write_all(b"\n")
                .map_err(|e| Error::Format(format!("fastq write: {e}")))?;
            writeln!(writer, "+{accession}.{row_id} {row_id} length={total}")
                .map_err(|e| Error::Format(format!("fastq write: {e}")))?;
            writer
                .write_all(&qual)
                .map_err(|e| Error::Format(format!("fastq write: {e}")))?;
            writer
                .write_all(b"\n")
                .map_err(|e| Error::Format(format!("fastq write: {e}")))?;
            spots += 1;
        }
        Ok(FastqStats { spots })
    }

    /// Decode one SEQUENCE row's full bases + quality.
    pub fn read_spot(&self, row_id: i64) -> Result<SpotRead> {
        let align_ids = self.primary_alignment_id.read_i64_row(row_id)?;
        let read_lens = self.read_len.read_u32_row(row_id)?;
        let read_types = self.read_type.read_byte_row(row_id)?;
        let cmp_read_2na = self.cmp_read.read_2na_row(row_id)?;
        let quality = self.quality.read_byte_row(row_id)?;

        if read_lens.len() != align_ids.len() || read_types.len() != align_ids.len() {
            return Err(Error::Format(format!(
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

#[cfg(test)]
mod looks_like_tests {
    use super::looks_like_decodable_csra;
    use std::path::Path;

    #[test]
    fn real_csra_detected() {
        // VDB-3418 is the reference-compressed cSRA in our fixtures —
        // SEQUENCE/col/CMP_READ + PRIMARY_ALIGNMENT + REFERENCE all
        // present. Skip silently when the fixture isn't materialised
        // (CI may not have run the download).
        let p = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/VDB-3418.sra");
        if !p.exists() {
            return;
        }
        assert!(
            looks_like_decodable_csra(&p, None).unwrap(),
            "VDB-3418 should pass the cSRA detector"
        );
    }

    #[test]
    fn non_csra_not_detected() {
        // DRR045255 is a flat-SEQUENCE bam-load-residue archive that
        // decodes via the plain VdbCursor path, not CsraCursor — the
        // detector must NOT claim it.
        let p = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/DRR045255.sra");
        if !p.exists() {
            return;
        }
        assert!(
            !looks_like_decodable_csra(&p, None).unwrap(),
            "DRR045255 should not be routed through CsraCursor"
        );
    }

    #[test]
    fn missing_file_returns_io_error() {
        let p = Path::new("/this/path/does/not/exist.sra");
        assert!(looks_like_decodable_csra(p, None).is_err());
    }
}
