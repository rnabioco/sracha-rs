//! Async metadata extractor: given an accession, fetch JUST the
//! KAR header + TOC + idx files from S3 and emit an [`AccessionRecord`].
//!
//! Never downloads the full .sra. Total network bytes per accession
//! are typically <500 KB (header+TOC ~tens of KB; each idx file
//! 1-50 KB; ~5-15 idx files per accession).

use std::collections::BTreeMap;
use std::io::Cursor;
use std::time::Instant;

use sha2::{Digest, Sha256};
use sracha_core::s3 as s3core;
use sracha_vdb::kar::KarArchive;
use sracha_vdb::kdb::ColumnReader;
use sracha_vdb::metadata as vdb_md;

use crate::record::{
    AccessionRecord, BlobLocator, ColumnMetaEntry, Layout, NameFmtEntry, Platform, SchemaEntry,
};
use crate::{Error, Result};

/// Initial speculative fetch size — covers KAR header + most TOCs in
/// one round trip. Empirically TOCs for typical accessions are
/// 10-100 KB; this catches the common case in one Range request.
const INITIAL_FETCH: u64 = 256 * 1024;

/// Cap on a single idx file fetch — anything larger almost certainly
/// signals a corrupt/malicious size value. Chosen to be generous for
/// real-world idx2 (block index) files which can hit a few MB on
/// large accessions.
const MAX_IDX_BYTES: u64 = 64 * 1024 * 1024;

/// Extract metadata for one accession. Network-only; uses HTTP Range
/// to fetch only the bytes needed.
pub async fn extract(accession: &str) -> Result<AccessionRecord> {
    let started = Instant::now();
    let mut bytes_fetched: u64 = 0;

    // 1. Resolve to a canonical S3 URL + file size + (optional) MD5.
    let client = reqwest::Client::builder()
        .pool_idle_timeout(std::time::Duration::from_secs(60))
        .build()
        .map_err(Error::Http)?;
    let resolved = s3core::resolve_direct(&client, accession)
        .await
        .map_err(|e| Error::Extractor(format!("resolve {accession}: {e}")))?;
    let url = resolved
        .sra_file
        .mirrors
        .first()
        .ok_or_else(|| Error::Extractor("no mirrors".into()))?
        .url
        .clone();
    let file_size = resolved.sra_file.size;
    let md5 = resolved.sra_file.md5.as_deref().and_then(parse_md5_hex);

    // 2. Speculative Range-fetch [0..min(INITIAL_FETCH, file_size)].
    //    Most archives' TOC fits inside this single request.
    let initial_end = INITIAL_FETCH.min(file_size).saturating_sub(1);
    let initial = range_fetch(&client, &url, 0, initial_end).await?;
    bytes_fetched += initial.len() as u64;

    if initial.len() < 24 {
        return Err(Error::Extractor(format!(
            "{accession}: response too short for KAR header ({} bytes)",
            initial.len()
        )));
    }

    // Peek at the header to learn `file_offset` (start of the data
    // section = end of the TOC).
    let file_offset = read_u64_le(&initial[16..24]);
    if file_offset == 0 || file_offset > file_size {
        return Err(Error::Extractor(format!(
            "{accession}: implausible kar file_offset={file_offset} (file_size={file_size})"
        )));
    }

    // 3. Make sure we have header + TOC bytes [0..file_offset]. If
    //    the speculative fetch already covered it, reuse; otherwise
    //    fetch the remainder.
    let header_toc: Vec<u8> = if (initial.len() as u64) >= file_offset {
        initial[..file_offset as usize].to_vec()
    } else {
        let extra = range_fetch(&client, &url, initial.len() as u64, file_offset - 1).await?;
        bytes_fetched += extra.len() as u64;
        let mut combined = Vec::with_capacity(file_offset as usize);
        combined.extend_from_slice(&initial);
        combined.extend_from_slice(&extra);
        combined
    };

    // 4. Open the archive against an in-memory cursor of just the
    //    header + TOC. KarArchive::open only reads those bytes, so
    //    file_location() works for every entry but read_file() would
    //    fail for entries pointing into the data section we haven't
    //    fetched.
    let archive = KarArchive::open(Cursor::new(header_toc.clone())).map_err(Error::Vdb)?;

    // 5. Identify SEQUENCE-table columns by looking for `*/idx1`
    //    entries under the standard prefix layouts.
    let columns = find_seq_columns(&archive);
    if columns.is_empty() {
        return Err(Error::Extractor(format!(
            "{accession}: no SEQUENCE-table columns found in archive"
        )));
    }

    // 6. Plan all idx-file fetches up-front, then issue them in
    //    parallel. One HTTP request per idx file; tokio::spawn each
    //    so we get true parallelism on the connection pool.
    let mut idx_fetch_plan: Vec<(String, u64, u64)> = Vec::new();
    for col in &columns {
        for sidecar in ["idx1", "idx0", "idx", "idx2"] {
            let path = format!("{col}/{sidecar}");
            if let Some((off, sz)) = archive.file_location(&path) {
                if sz == 0 {
                    continue;
                }
                if sz > MAX_IDX_BYTES {
                    return Err(Error::Extractor(format!(
                        "{accession}: idx file {path} too large ({sz} bytes)"
                    )));
                }
                idx_fetch_plan.push((path, off, sz));
            }
        }
    }

    // 6b. Also queue the `md/cur` files. These carry read structure
    //     (per-spot read types + lengths) and platform — needed to
    //     populate AccessionRecord.{platform, layout, read_lengths}.
    //     Try the table-level path first, then fall back to the
    //     database-level path; mirroring sracha-vdb's
    //     detect_metadata.
    let mut md_plan: Vec<(String, u64, u64)> = Vec::new();
    for path in ["tbl/SEQUENCE/md/cur", "md/cur"] {
        if let Some((off, sz)) = archive.file_location(path) {
            if sz == 0 || sz > MAX_IDX_BYTES {
                continue;
            }
            md_plan.push((path.to_string(), off, sz));
        }
    }
    let combined_plan: Vec<(String, u64, u64)> = idx_fetch_plan
        .into_iter()
        .chain(md_plan.into_iter())
        .collect();

    let mut idx_buffers: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    let fetched: Vec<(String, Vec<u8>)> =
        futures_join_all_ranges(&client, &url, combined_plan).await?;
    for (path, buf) in fetched {
        bytes_fetched += buf.len() as u64;
        idx_buffers.insert(path, buf);
    }

    // Parse md/cur (8-byte KDBHdr prefix, then PBSTree). Try
    // table-level first.
    let (read_descs, platform_str): (Option<Vec<vdb_md::ReadDescriptor>>, Option<String>) = {
        let mut rps: Option<Vec<vdb_md::ReadDescriptor>> = None;
        let mut platform: Option<String> = None;
        for path in ["tbl/SEQUENCE/md/cur", "md/cur"] {
            let Some(buf) = idx_buffers.get(path) else {
                continue;
            };
            if buf.len() < 8 {
                continue;
            }
            let tree = &buf[8..];
            if rps.is_none()
                && let Ok(d) = vdb_md::parse_read_structure(tree)
            {
                rps = Some(d);
            }
            if platform.is_none() {
                platform = vdb_md::detect_platform(tree);
            }
        }
        (rps, platform)
    };

    // 7. For each column, build a ColumnReader from its idx buffers.
    //    Extract blob locators and column metadata. Empty data is
    //    fine here: blob byte ranges live in idx0/idx1/idx2, not in
    //    the data slab.
    let mut column_meta: Vec<ColumnMetaEntry> = Vec::with_capacity(columns.len());
    let mut blobs_out: Vec<BlobLocator> = Vec::new();
    // Spots = total row count of the READ column (sum of blob
    // id_ranges). Set the first time we successfully parse READ.
    let mut spots_from_read: Option<u64> = None;

    for (column_id_u8, col) in columns.iter().enumerate() {
        let column_id =
            u8::try_from(column_id_u8).map_err(|_| Error::Extractor("too many columns".into()))?;
        let idx1 = idx_buffers
            .get(&format!("{col}/idx1"))
            .cloned()
            .unwrap_or_default();
        let idx0 = idx_buffers
            .get(&format!("{col}/idx0"))
            .cloned()
            .unwrap_or_default();
        let idx = idx_buffers
            .get(&format!("{col}/idx"))
            .cloned()
            .unwrap_or_default();
        let idx2 = idx_buffers
            .get(&format!("{col}/idx2"))
            .cloned()
            .unwrap_or_default();

        if idx1.is_empty() {
            continue;
        }

        // Some columns hit `idx0 size N is not a multiple of 24` on
        // newer VDB schema variants (the idx0 layout differs from the
        // legacy fixed-24-byte BlobLoc table). Skip those columns
        // rather than fail the whole accession — we still emit
        // partial blob locators for the columns that did parse, and
        // sracha-core's full ColumnReader::open path handles these
        // accessions correctly so the index data gap is recoverable
        // at decode time. Tracked as a sracha-vdb hardening TODO.
        let reader = match ColumnReader::from_parts(&idx1, &idx0, &idx, &idx2, Vec::new()) {
            Ok(r) => r,
            Err(e) => {
                tracing::debug!("{accession}: column {col} from_parts failed: {e} (skipping)");
                continue;
            }
        };

        // Resolve the absolute byte offset of THIS column's data slab.
        // For accessions where the column has no `data` entry (e.g.
        // empty SPOT_GROUP), data_off = 0; consumers should treat that
        // as "no data slab" by checking blob_size.
        let data_path = format!("{col}/data");
        let data_off = archive
            .file_location(&data_path)
            .map(|(o, _)| o)
            .unwrap_or(0);

        // Capture per-column metadata for the schema fingerprint.
        let meta_view = reader.meta();
        let column_name = col.rsplit('/').next().unwrap_or(col.as_str()).to_string();

        // First time we see the READ column: derive `spots` from
        // its blob id_ranges. Every column has the same row count
        // for SEQUENCE-table data, so READ is a fine canonical
        // source.
        if column_name == "READ" && spots_from_read.is_none() {
            let total: u64 = reader.blobs().iter().map(|b| u64::from(b.id_range)).sum();
            if total > 0 {
                spots_from_read = Some(total);
            }
        }

        column_meta.push(ColumnMetaEntry {
            name: column_name,
            version: meta_view.version,
            codec: u32::from(meta_view.checksum_type),
            page_size: meta_view.page_size,
        });

        // Walk blobs and emit BlobLocator rows. blob_idx and pg
        // are dropped — derivable at read time from row order and
        // (blob_offset / page_size) respectively.
        for blob in reader.blobs() {
            // Reconstruct absolute file offset: data_off +
            // blob_offset_within_slab. ColumnReader's
            // blob_data_offset is `pg * page_size` (or just `pg` if
            // page_size <= 1). Derive from blob.pg + meta.page_size.
            let off_within = if meta_view.page_size <= 1 {
                blob.pg
            } else {
                blob.pg * u64::from(meta_view.page_size)
            };
            let blob_offset_abs = data_off + off_within;

            blobs_out.push(BlobLocator {
                column_id,
                start_id: blob.start_id,
                id_range: blob.id_range,
                blob_offset: blob_offset_abs,
                blob_size: blob.size,
            });
        }
    }

    // 8. Schema fingerprint: hash the (sorted) column metadata list
    //    so equivalent schemas dedup.
    let mut fp_hasher = Sha256::new();
    for cm in &column_meta {
        fp_hasher.update(cm.name.as_bytes());
        fp_hasher.update(cm.version.to_le_bytes());
        fp_hasher.update(cm.codec.to_le_bytes());
        fp_hasher.update(cm.page_size.to_le_bytes());
    }
    let schema_fingerprint: [u8; 32] = fp_hasher.finalize().into();

    let kar_data_offset = file_offset;

    // Map md/cur read structure to AccessionRecord fields.
    // Biological reads (type='B') determine layout + per-mate
    // length pattern; technical reads (e.g. barcodes) are filtered
    // out for the user-facing read_lengths.
    let bio_lens: Vec<u32> = read_descs
        .as_ref()
        .map(|descs| {
            descs
                .iter()
                .filter(|d| d.read_type == b'B')
                .map(|d| d.read_len)
                .collect()
        })
        .unwrap_or_default();
    let layout = match bio_lens.len() {
        1 => Layout::Single,
        2 => Layout::Paired,
        _ => Layout::Unknown,
    };
    let platform = platform_str
        .as_deref()
        .map(str::to_uppercase)
        .as_deref()
        .map(classify_platform)
        .unwrap_or(Platform::Other);

    let record = AccessionRecord {
        accession: accession.to_string(),
        file_size,
        md5,
        spots: spots_from_read,
        layout,
        platform,
        read_lengths: bio_lens,
        schema_fingerprint,
        kar_data_offset,
        blobs: blobs_out,
        schema: SchemaEntry {
            fingerprint: schema_fingerprint,
            columns: column_meta,
            is_csra: archive
                .list_files()
                .iter()
                .any(|p| p.contains("PRIMARY_ALIGNMENT")),
        },
        name_fmt: extract_name_fmt(&archive, &header_toc, &idx_buffers)
            .ok()
            .flatten(),
        extract_secs: started.elapsed().as_secs_f32(),
        bytes_fetched,
    };

    Ok(record)
}

/// Identify SEQUENCE-table column subtrees by walking the TOC for
/// `*/idx1` entries. Returns the column directory paths (e.g.
/// `tbl/SEQUENCE/col/READ`).
///
/// Handles both database (`tbl/SEQUENCE/col/*`) and bare-table
/// (`col/*`) layouts — see `cursor::find_sequence_col_base` for the
/// canonical version. We don't import that helper because it's
/// private; this function makes its own pass over `list_files`.
fn find_seq_columns<R: std::io::Read + std::io::Seek>(archive: &KarArchive<R>) -> Vec<String> {
    let mut cols: Vec<String> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for path in archive.list_files() {
        let Some(stripped) = path.strip_suffix("/idx1") else {
            continue;
        };
        // Filter to SEQUENCE-table columns only. cSRA databases
        // contain other tables (PRIMARY_ALIGNMENT, REFERENCE) we
        // don't extract here in v0.
        let in_seq = stripped.starts_with("tbl/SEQUENCE/col/")
            || stripped.starts_with("col/")
            || stripped.contains("/tbl/SEQUENCE/col/");
        if in_seq && seen.insert(stripped.to_string()) {
            cols.push(stripped.to_string());
        }
    }
    cols.sort();
    cols
}

/// Try to extract the Illumina skey templates table (lives at
/// `tbl/SEQUENCE/idx/skey` or similar). Returns `None` if the
/// archive has no skey file.
fn extract_name_fmt<R: std::io::Read + std::io::Seek>(
    _archive: &KarArchive<R>,
    _header_toc: &[u8],
    _idx_buffers: &BTreeMap<String, Vec<u8>>,
) -> Result<Option<NameFmtEntry>> {
    // v0: not yet implemented. Cursor::load_name_templates reads
    // straight from a KarArchive. For the index extractor we'd need
    // to pull the skey file via Range fetch (it's small) and parse it.
    // Wire up after the basic builder lands.
    Ok(None)
}

/// Issue HTTP Range requests in parallel for a planned list of
/// `(path, abs_offset, size)` tuples. Returns `(path, body)` pairs
/// in input order.
async fn futures_join_all_ranges(
    client: &reqwest::Client,
    url: &str,
    plan: Vec<(String, u64, u64)>,
) -> Result<Vec<(String, Vec<u8>)>> {
    let mut handles = Vec::with_capacity(plan.len());
    for (path, off, sz) in plan {
        let client = client.clone();
        let url = url.to_string();
        handles.push(tokio::spawn(async move {
            let bytes = range_fetch(&client, &url, off, off + sz - 1).await?;
            Ok::<_, Error>((path, bytes))
        }));
    }
    let mut out = Vec::with_capacity(handles.len());
    for h in handles {
        out.push(
            h.await
                .map_err(|e| Error::Extractor(format!("join: {e}")))??,
        );
    }
    Ok(out)
}

/// Single HTTP Range request: fetch `[start..=end]` bytes inclusive.
async fn range_fetch(
    client: &reqwest::Client,
    url: &str,
    start: u64,
    end_inclusive: u64,
) -> Result<Vec<u8>> {
    let resp = client
        .get(url)
        .header("Range", format!("bytes={start}-{end_inclusive}"))
        .send()
        .await?;
    if !resp.status().is_success() {
        return Err(Error::Extractor(format!(
            "range fetch {start}-{end_inclusive} {url}: HTTP {}",
            resp.status()
        )));
    }
    let bytes = resp.bytes().await?.to_vec();
    Ok(bytes)
}

fn read_u64_le(b: &[u8]) -> u64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(&b[..8]);
    u64::from_le_bytes(a)
}

fn parse_md5_hex(s: &str) -> Option<[u8; 16]> {
    if s.len() != 32 {
        return None;
    }
    let mut out = [0u8; 16];
    for i in 0..16 {
        out[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

/// Classify a VDB metadata `PLATFORM/SRA_PLATFORM_*` string into the
/// catalog's compact `Platform` enum. Input is assumed uppercased.
fn classify_platform(p: &str) -> Platform {
    if p.contains("ILLUMINA") {
        Platform::Illumina
    } else if p.contains("PACBIO") {
        Platform::PacBio
    } else if p.contains("NANOPORE") || p.contains("OXFORD") {
        Platform::OxfordNanopore
    } else if p.contains("ION_TORRENT") || p.contains("ION TORRENT") || p.contains("IONTORRENT") {
        Platform::IonTorrent
    } else {
        Platform::Other
    }
}
