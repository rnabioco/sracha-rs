//! `sracha validate` subcommand implementation — opens an SRA file,
//! decodes every blob to verify CRC32/MD5 integrity, and reports any
//! errors. Extracted from `pipeline/mod.rs` as part of the pipeline
//! refactor (no behavior change).

use rayon::prelude::*;

use crate::error::Error;
use crate::vdb::cursor::VdbCursor;
use crate::vdb::kar::KarArchive;

use super::{decode_raw, decode_zip_encoding, make_styled_pb};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Result of validating an SRA file.
pub struct ValidationResult {
    /// Label for the file that was validated.
    pub label: String,
    /// Whether the file is valid (no errors).
    pub valid: bool,
    /// Number of spots decoded during validation.
    pub spots_validated: u64,
    /// Number of blobs validated.
    pub blobs_validated: usize,
    /// Columns found in the SEQUENCE table.
    pub columns_found: Vec<String>,
    /// Errors encountered during validation.
    pub errors: Vec<String>,
    /// MD5 hex digest of the entire SRA file.
    pub md5: Option<String>,
    /// True if any error originated from [`Error::BlobIntegrity`] (per-blob
    /// CRC32/MD5 failure during decode). Callers can use this to show the
    /// shared [`crate::error::BLOB_INTEGRITY_GUIDANCE`] text once.
    pub any_blob_integrity_error: bool,
}

/// Validate an SRA file by opening as KAR archive, parsing the SEQUENCE
/// table, and decoding all blobs. No output files are produced.
pub fn run_validate(
    sra_path: &std::path::Path,
    threads: usize,
    progress: bool,
) -> ValidationResult {
    let label = sra_path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown")
        .to_string();

    let mut errors = Vec::new();
    let mut columns_found = Vec::new();

    // Step 1: Open KAR archive.
    let file = match std::fs::File::open(sra_path) {
        Ok(f) => f,
        Err(e) => {
            errors.push(format!("cannot open file: {e}"));
            return ValidationResult {
                label,
                valid: false,
                spots_validated: 0,
                blobs_validated: 0,
                columns_found,
                errors,
                md5: None,
                any_blob_integrity_error: false,
            };
        }
    };

    let mut archive = match KarArchive::open(std::io::BufReader::new(file)) {
        Ok(a) => a,
        Err(e) => {
            errors.push(format!("invalid KAR archive: {e}"));
            return ValidationResult {
                label,
                valid: false,
                spots_validated: 0,
                blobs_validated: 0,
                columns_found,
                errors,
                md5: None,
                any_blob_integrity_error: false,
            };
        }
    };

    // Step 2: Open VDB cursor.
    let cursor = match VdbCursor::open(&mut archive, sra_path) {
        Ok(c) => c,
        Err(e) => {
            errors.push(format!("cannot open VDB cursor: {e}"));
            return ValidationResult {
                label,
                valid: false,
                spots_validated: 0,
                blobs_validated: 0,
                columns_found,
                errors,
                md5: None,
                any_blob_integrity_error: false,
            };
        }
    };

    // Record found columns.
    columns_found.push("READ".into());
    if cursor.has_quality() {
        columns_found.push("QUALITY".into());
    }
    if cursor.read_len_col().is_some() {
        columns_found.push("READ_LEN".into());
    }
    if cursor.read_type_col().is_some() {
        columns_found.push("READ_TYPE".into());
    }
    if cursor.name_col().is_some() {
        columns_found.push("NAME".into());
    }

    let expected_spots = cursor.spot_count();
    let num_blobs = cursor.read_col().blob_count();
    let read_cs = cursor.read_col().meta().checksum_type;
    let has_quality = cursor.quality_col().is_some();
    let quality_blob_count = cursor.quality_col().map_or(0, |c| c.blob_count());
    let quality_cs = cursor.quality_col().map_or(0, |c| c.meta().checksum_type);
    let has_read_len = cursor.read_len_col().is_some();
    let read_len_blob_count = cursor.read_len_col().map_or(0, |c| c.blob_count());
    let read_len_cs = cursor.read_len_col().map_or(0, |c| c.meta().checksum_type);

    // Step 3: Decode all blobs in parallel.
    let pool = match rayon::ThreadPoolBuilder::new().num_threads(threads).build() {
        Ok(p) => p,
        Err(e) => {
            errors.push(format!("failed to build thread pool: {e}"));
            return ValidationResult {
                label,
                valid: false,
                spots_validated: 0,
                blobs_validated: 0,
                columns_found,
                errors,
                md5: None,
                any_blob_integrity_error: false,
            };
        }
    };

    let decode_pb = if progress {
        Some(make_styled_pb(
            num_blobs as u64,
            "  {elapsed_precise} [{bar:40.cyan}] {pos}/{len} blobs  {per_sec}  eta {eta}",
        ))
    } else {
        None
    };

    let total_spots = std::sync::atomic::AtomicU64::new(0);
    let mut blobs_validated: usize = 0;
    let mut any_blob_integrity_error = false;

    const BATCH_SIZE: usize = 1024;
    let mut blob_idx: usize = 0;

    // Helper: capture (msg, is_blob_integrity) from a decode Error.
    fn fmt_err(tag: &str, bi: usize, e: Error) -> (String, bool) {
        let is_integrity = matches!(e, Error::BlobIntegrity { .. });
        (format!("{tag} blob {bi} decode: {e}"), is_integrity)
    }

    while blob_idx < num_blobs {
        let batch_end = (blob_idx + BATCH_SIZE).min(num_blobs);

        let batch_errors: Vec<(usize, String, bool)> = pool.install(|| {
            (blob_idx..batch_end)
                .into_par_iter()
                .filter_map(|bi| {
                    // Decode READ blob.
                    let read_blob = &cursor.read_col().blobs()[bi];
                    let read_raw = match cursor.read_col().read_raw_blob_slice(read_blob.start_id) {
                        Ok(r) => r,
                        Err(e) => return Some((bi, format!("READ blob {bi}: {e}"), false)),
                    };
                    let id_range = read_blob.id_range as u64;
                    if let Err(e) = decode_raw(read_raw, read_cs, id_range) {
                        let (msg, is_integ) = fmt_err("READ", bi, e);
                        return Some((bi, msg, is_integ));
                    }

                    // Count spots from this blob.
                    total_spots.fetch_add(id_range, std::sync::atomic::Ordering::Relaxed);

                    // Decode QUALITY blob.
                    if has_quality && bi < quality_blob_count {
                        let qcol = cursor.quality_col().unwrap();
                        let qblob = &qcol.blobs()[bi];
                        match qcol.read_raw_blob_slice(qblob.start_id) {
                            Ok(q_raw) => {
                                let q_id = qblob.id_range as u64;
                                match decode_raw(q_raw, quality_cs, q_id) {
                                    Ok(qd) => {
                                        if let Err(e) = decode_zip_encoding(&qd) {
                                            return Some((
                                                bi,
                                                format!("QUALITY blob {bi} unzip: {e}"),
                                                false,
                                            ));
                                        }
                                    }
                                    Err(e) => {
                                        let (msg, is_integ) = fmt_err("QUALITY", bi, e);
                                        return Some((bi, msg, is_integ));
                                    }
                                }
                            }
                            Err(e) => {
                                return Some((bi, format!("QUALITY blob {bi}: {e}"), false));
                            }
                        }
                    }

                    // Decode READ_LEN blob.
                    if has_read_len && bi < read_len_blob_count {
                        let rlcol = cursor.read_len_col().unwrap();
                        let rlblob = &rlcol.blobs()[bi];
                        match rlcol.read_raw_blob_slice(rlblob.start_id) {
                            Ok(rl_raw) => {
                                let rl_id = rlblob.id_range as u64;
                                if let Err(e) = decode_raw(rl_raw, read_len_cs, rl_id) {
                                    let (msg, is_integ) = fmt_err("READ_LEN", bi, e);
                                    return Some((bi, msg, is_integ));
                                }
                            }
                            Err(e) => {
                                return Some((bi, format!("READ_LEN blob {bi}: {e}"), false));
                            }
                        }
                    }

                    None
                })
                .collect()
        });

        for (bi, msg, is_integ) in &batch_errors {
            errors.push(msg.clone());
            if *is_integ {
                any_blob_integrity_error = true;
            }
            tracing::error!("validation error at blob {bi}: {msg}");
        }

        blobs_validated += batch_end - blob_idx;

        if let Some(ref pb) = decode_pb {
            pb.inc((batch_end - blob_idx) as u64);
        }

        blob_idx = batch_end;
    }

    if let Some(pb) = decode_pb {
        pb.finish_and_clear();
    }

    let decoded_spots = total_spots.load(std::sync::atomic::Ordering::Relaxed);
    if expected_spots > 0 && decoded_spots != expected_spots {
        errors.push(format!(
            "spot count mismatch: metadata says {expected_spots}, decoded {decoded_spots}",
        ));
    }

    let md5 = compute_file_md5(sra_path)
        .map_err(|e| errors.push(format!("MD5 compute: {e}")))
        .ok();

    ValidationResult {
        valid: errors.is_empty(),
        label,
        spots_validated: decoded_spots,
        blobs_validated,
        columns_found,
        errors,
        md5,
        any_blob_integrity_error,
    }
}

fn compute_file_md5(path: &std::path::Path) -> std::io::Result<String> {
    use md5::{Digest, Md5};
    use std::io::Read;
    let mut file = std::fs::File::open(path)?;
    let mut hasher = Md5::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    let digest = hasher.finalize();
    Ok(digest.iter().map(|b| format!("{b:02x}")).collect())
}
