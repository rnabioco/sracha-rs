//! Pipeline orchestration for the `sracha get` command.
//!
//! Implements a full-download approach:
//!
//! 1. **Phase 1 -- Download**: Use the parallel chunked downloader to fetch the
//!    full SRA file to a temporary location.
//! 2. **Phase 2 -- Parse + Output**: Open the downloaded file as a KAR archive,
//!    create a VdbCursor to read SEQUENCE table columns, decode VDB blobs, and
//!    write FASTQ output.
//! 3. **Phase 3 -- Cleanup + Report**: Delete the temp file and print stats.

use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;

use flate2::write::GzEncoder;
use crate::download::{DownloadConfig, download_file};
use crate::error::{Error, Result};
use crate::fastq::{FastqConfig, OutputSlot, SplitMode, SpotRecord, format_spot, output_filename};
use crate::sdl::ResolvedAccession;
use crate::vdb::blob;
use crate::vdb::cursor::VdbCursor;
use crate::vdb::kar::KarArchive;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the get pipeline.
pub struct PipelineConfig {
    /// Directory for output files.
    pub output_dir: PathBuf,
    /// How to split reads across output files.
    pub split_mode: SplitMode,
    /// Whether to gzip-compress output.
    pub gzip: bool,
    /// Gzip compression level (1-9).
    pub gzip_level: u32,
    /// Number of threads for compression (None = all cores).
    pub threads: Option<usize>,
    /// Number of parallel HTTP connections for downloading.
    pub connections: usize,
    /// Skip technical reads.
    pub skip_technical: bool,
    /// Minimum read length filter.
    pub min_read_len: Option<u32>,
    /// Overwrite existing output files.
    pub force: bool,
    /// Show progress indicators.
    pub progress: bool,
}

// ---------------------------------------------------------------------------
// Statistics
// ---------------------------------------------------------------------------

/// Statistics from a completed pipeline run.
pub struct PipelineStats {
    /// The accession that was processed.
    pub accession: String,
    /// Number of spots (rows) read from the SRA file.
    pub spots_read: u64,
    /// Number of FASTQ reads written (after filtering).
    pub reads_written: u64,
    /// Total bytes downloaded via HTTP Range requests.
    pub bytes_downloaded: u64,
    /// Total size of the full SRA file on the server.
    pub total_sra_size: u64,
    /// Paths of all output files created.
    pub output_files: Vec<PathBuf>,
}

// ---------------------------------------------------------------------------
// Output writer
// ---------------------------------------------------------------------------

/// An output writer that handles both gzip-compressed and plain FASTQ output.
enum OutputWriter {
    Gz(GzEncoder<std::io::BufWriter<std::fs::File>>),
    Plain(std::io::BufWriter<std::fs::File>),
}

impl OutputWriter {
    fn write_all(&mut self, data: &[u8]) -> std::io::Result<()> {
        match self {
            OutputWriter::Gz(w) => w.write_all(data),
            OutputWriter::Plain(w) => w.write_all(data),
        }
    }

    fn finish(self) -> std::io::Result<()> {
        match self {
            OutputWriter::Gz(w) => {
                w.finish()?;
                Ok(())
            }
            OutputWriter::Plain(mut w) => {
                w.flush()?;
                Ok(())
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Mirror selection
// ---------------------------------------------------------------------------

/// Select the best mirror URL for downloading.
///
/// Prefers cloud mirrors (s3, gs) over NCBI on-premises servers because
/// cloud CDNs are typically much faster for parallel chunked downloads.
fn select_mirror(resolved: &ResolvedAccession) -> Result<String> {
    let mirrors = &resolved.sra_file.mirrors;
    if mirrors.is_empty() {
        return Err(Error::Download {
            accession: resolved.accession.clone(),
            message: "no download mirrors available".into(),
        });
    }

    // Prefer cloud mirrors — much faster for parallel downloads.
    // Priority: s3 > gs > sra-ncbi > ncbi > any
    let priority = |s: &str| -> u8 {
        match s {
            "s3" => 0,
            "gs" => 1,
            s if s.contains("sra-ncbi") => 2,
            "ncbi" => 3,
            _ => 4,
        }
    };

    let best = mirrors
        .iter()
        .min_by_key(|m| priority(m.service.as_str()))
        .unwrap();

    tracing::info!(
        "selected mirror: [{}] {}",
        best.service,
        &best.url[..best.url.len().min(80)],
    );

    Ok(best.url.clone())
}

// ---------------------------------------------------------------------------
// Phase 2: Parse VDB + output FASTQ
// ---------------------------------------------------------------------------

/// Decode VDB columns from a local SRA file, format FASTQ, and write to
/// output files.
///
/// This opens the SRA file as a KAR archive, creates a VdbCursor for the
/// SEQUENCE table, bulk-decompresses each column, and iterates through spots
/// to produce FASTQ output.
fn decode_and_write(
    sra_path: &std::path::Path,
    accession: &str,
    config: &PipelineConfig,
    is_lite: bool,
) -> Result<(u64, u64, Vec<PathBuf>)> {
    let file = std::fs::File::open(sra_path)?;
    let mut archive = KarArchive::open(std::io::BufReader::new(file))?;
    let cursor = VdbCursor::open(&mut archive)?;

    // ------------------------------------------------------------------
    // Streaming blob-by-blob decode and FASTQ output.
    //
    // Instead of loading all blobs into memory at once, process one blob
    // index at a time: decode READ, QUALITY, and READ_LEN for that blob,
    // split into reads, write FASTQ, then free the data before moving on.
    //
    // Physical encodings:
    //   READ:     INSDC:2na:packed (2 bits/base, 4 bases/byte)
    //   QUALITY:  zip_encoding of phred scores (deflate-compressed u8 array)
    //   READ_LEN: izip_encoding of u32 values
    //   READ_TYPE: zip_encoding of u8 values
    // ------------------------------------------------------------------

    /// Decode a raw blob, stripping envelope/headers/page_map.
    fn decode_raw(raw: &[u8], checksum_type: u8, row_count: u64) -> Result<blob::DecodedBlob> {
        let cs_size: usize = match checksum_type {
            1 => 4,  // CRC32
            2 => 16, // MD5
            _ => 0,
        };
        let effective = if raw.len() > cs_size {
            &raw[..raw.len() - cs_size]
        } else {
            raw
        };
        blob::decode_blob(effective, 0, row_count, 8)
    }

    let fastq_config = FastqConfig {
        split_mode: config.split_mode,
        skip_technical: config.skip_technical,
        min_read_len: config.min_read_len,
    };

    // Create output directory.
    std::fs::create_dir_all(&config.output_dir)?;

    // Lazily create output writers as we encounter different output slots.
    let mut writers: HashMap<OutputSlot, OutputWriter> = HashMap::new();
    let mut output_files: Vec<PathBuf> = Vec::new();

    let mut spots_read: u64 = 0;
    let mut reads_written: u64 = 0;

    // Use the READ column as the primary blob source.
    let read_cs = cursor.read_col().meta().checksum_type;
    let num_blobs = cursor.read_col().blob_count();

    tracing::info!(
        "{accession}: streaming decode of {num_blobs} blobs",
    );

    for blob_idx in 0..num_blobs {
        // --------------------------------------------------------------
        // Decode READ blob → 2na → ASCII bases.
        // --------------------------------------------------------------
        let read_blob = &cursor.read_col().blobs()[blob_idx];
        let read_raw = cursor.read_col().read_raw_blob_for_row(read_blob.start_id)?;
        let read_decoded = decode_raw(&read_raw, read_cs, read_blob.id_range as u64)?;
        let total_bits = read_decoded.data.len() * 8;
        let adjust = read_decoded.adjust as usize;
        let actual_bases = (total_bits.saturating_sub(adjust)) / 2;
        let read_data = crate::vdb::encoding::unpack_2na(&read_decoded.data, actual_bases);
        // Save page map for per-row splitting when READ_LEN is absent.
        let read_page_map = read_decoded.page_map;
        if blob_idx == 0 {
            tracing::info!("blob 0: page_map={}, read_len_col={}", read_page_map.is_some(), cursor.read_len_col().is_some());
            if let Some(ref pm) = read_page_map {
                tracing::info!(
                    "blob 0 page_map: data_recs={}, lengths={:?}, leng_runs={:?}, data_runs_len={}",
                    pm.data_recs,
                    &pm.lengths[..pm.lengths.len().min(5)],
                    &pm.leng_runs[..pm.leng_runs.len().min(5)],
                    pm.data_runs.len(),
                );
            } else {
                tracing::info!("blob 0: no page map");
            }
        }
        drop(read_raw);

        // --------------------------------------------------------------
        // Decode QUALITY blob (same index).
        // Try deflate decompression, fall back to raw bytes.
        // --------------------------------------------------------------
        let quality_data: Vec<u8> = if let Some(qcol) = cursor.quality_col() {
            if blob_idx < qcol.blob_count() {
                let qblob = &qcol.blobs()[blob_idx];
                let qcs = qcol.meta().checksum_type;
                let qraw = qcol.read_raw_blob_for_row(qblob.start_id)?;
                let qdecoded = decode_raw(&qraw, qcs, qblob.id_range as u64)?;
                drop(qraw);

                use std::io::Read as _;
                let mut dec = flate2::read::DeflateDecoder::new(qdecoded.data.as_slice());
                let mut out = Vec::new();
                if dec.read_to_end(&mut out).is_ok() && !out.is_empty() {
                    out
                } else {
                    qdecoded.data
                }
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        // Convert quality to phred+33 ASCII if needed.
        let quality_all: Vec<u8> = if !quality_data.is_empty() {
            if quality_data.len() == read_data.len() {
                if quality_data.iter().all(|&b| b >= 33) {
                    quality_data
                } else {
                    crate::vdb::encoding::phred_to_ascii(&quality_data)
                }
            } else {
                crate::vdb::encoding::phred_to_ascii(&quality_data)
            }
        } else {
            // SRA-lite: synthetic quality.
            crate::vdb::encoding::sra_lite_quality(read_data.len(), !is_lite)
        };

        // --------------------------------------------------------------
        // Decode READ_LEN blob (same index) → irzip/izip → u32 lengths.
        // --------------------------------------------------------------
        let read_lengths: Vec<u32> = if let Some(rlcol) = cursor.read_len_col() {
            if blob_idx < rlcol.blob_count() {
                let rlblob = &rlcol.blobs()[blob_idx];
                let rlcs = rlcol.meta().checksum_type;
                let rlraw = rlcol.read_raw_blob_for_row(rlblob.start_id)?;
                let rldecoded = decode_raw(&rlraw, rlcs, rlblob.id_range as u64)?;
                drop(rlraw);

                let hdr_version = rldecoded.headers.first().map(|h| h.version).unwrap_or(0);

                let decoded_ints = if hdr_version >= 1 {
                    let hdr = &rldecoded.headers[0];
                    let planes = hdr.ops.first().copied().unwrap_or(0xFF);
                    let min = hdr.args.first().copied().unwrap_or(0);
                    let slope = hdr.args.get(1).copied().unwrap_or(0);
                    let num_elems = (hdr.osize as u32) / 4;
                    blob::irzip_decode(&rldecoded.data, 32, num_elems, min, slope, planes)
                        .unwrap_or_default()
                } else {
                    let num_elems = rldecoded.row_length
                        .unwrap_or_else(|| (rldecoded.data.len() as u64 * 8) / 32)
                        as u32;
                    blob::izip_decode(&rldecoded.data, 32, num_elems).unwrap_or_default()
                };

                // Expand via page map data_runs if present.
                let rl_bytes = if let Some(ref pm) = rldecoded.page_map {
                    pm.expand_data_runs_bytes(&decoded_ints, 4)
                } else {
                    decoded_ints
                };

                rl_bytes
                    .chunks_exact(4)
                    .map(|chunk| u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
                    .collect()
            } else {
                // Fewer READ_LEN blobs than READ blobs: treat as one read.
                vec![read_data.len() as u32]
            }
        } else if let Some(ref pm) = read_page_map {
            // No READ_LEN column: use the READ blob's page map for row lengths.
            if blob_idx == 0 {
                tracing::info!(
                    "using page map: {} lengths, {} leng_runs, data_recs={}, total_bases={}, lengths[20..30]={:?}, leng_runs[20..30]={:?}",
                    pm.lengths.len(),
                    pm.leng_runs.len(),
                    pm.data_recs,
                    read_data.len(),
                    &pm.lengths[20..pm.lengths.len().min(30)],
                    &pm.leng_runs[20..pm.leng_runs.len().min(30)],
                );
            }
            let mut row_lengths = Vec::new();
            for (len, run) in pm.lengths.iter().zip(pm.leng_runs.iter()) {
                for _ in 0..*run {
                    // Sanity check: individual reads shouldn't exceed 100KB
                    if *len > 100_000 {
                        tracing::warn!(
                            "blob {blob_idx}: page map length {} exceeds 100KB, skipping (likely deserialization error)",
                            len
                        );
                        continue;
                    }
                    row_lengths.push(*len);
                }
            }
            if blob_idx == 0 {
                let bad_count = row_lengths.iter().filter(|&&l| l > 10000).count();
                let skipped = pm.lengths.iter().zip(pm.leng_runs.iter())
                    .flat_map(|(l, r)| std::iter::repeat(*l).take(*r as usize))
                    .filter(|&l| l > 100_000)
                    .count();
                tracing::info!(
                    "blob 0: {} row_lengths from page map (skipped {} > 100KB), {} > 10KB, pm.lengths has {} entries",
                    row_lengths.len(), skipped, bad_count, pm.lengths.len(),
                );
                // Show some of the bad values
                let bad_vals: Vec<_> = pm.lengths.iter().filter(|&&l| l > 10000).take(5).collect();
                if !bad_vals.is_empty() {
                    tracing::warn!("bad page map lengths: {bad_vals:?}");
                }
            }
            row_lengths
        } else {
            // No READ_LEN, no page map: treat entire blob as one read.
            vec![read_data.len() as u32]
        };

        // TODO: decode READ_TYPE and READ_FILTER from their physical encodings.
        // For now, assume all reads are biological and pass filter.

        // --------------------------------------------------------------
        // Iterate reads in THIS blob and write FASTQ immediately.
        // --------------------------------------------------------------
        let mut seq_offset: usize = 0;
        let mut qual_offset: usize = 0;

        for (local_read_idx, &rlen) in read_lengths.iter().enumerate() {
            let rlen = rlen as usize;

            // Extract sequence slice for this read.
            let seq_end = seq_offset + rlen;
            if seq_end > read_data.len() {
                tracing::warn!(
                    "blob {blob_idx}, read {local_read_idx}: sequence overrun at offset \
                     {seq_offset} + {rlen} > {}; stopping blob",
                    read_data.len(),
                );
                break;
            }
            let sequence = &read_data[seq_offset..seq_end];
            seq_offset = seq_end;

            // Extract quality slice for this read.
            let qual_end = qual_offset + rlen;
            if qual_end > quality_all.len() {
                tracing::warn!(
                    "blob {blob_idx}, read {local_read_idx}: quality overrun at offset \
                     {qual_offset} + {rlen} > {}; stopping blob",
                    quality_all.len(),
                );
                break;
            }
            let quality = &quality_all[qual_offset..qual_end];
            qual_offset = qual_end;

            // Global read index for naming.
            let global_read_idx = spots_read as usize + local_read_idx;

            // Synthetic name: accession.row_number
            let name: Vec<u8> = format!("{accession}.{}", global_read_idx + 1).into_bytes();

            // Build a SpotRecord for this single read.
            let spot = SpotRecord {
                name,
                sequence: sequence.to_vec(),
                quality: quality.to_vec(),
                read_lengths: vec![rlen as u32],
                read_types: vec![0u8],  // 0 = biological
                read_filter: vec![0u8], // 0 = pass
                spot_group: Vec::new(),
            };

            let records = format_spot(&spot, accession, &fastq_config);

            for (slot, record) in &records {
                let writer = writers.entry(*slot).or_insert_with(|| {
                    let filename = output_filename(accession, *slot, config.gzip);
                    let path = config.output_dir.join(&filename);
                    output_files.push(path.clone());

                    let file =
                        std::fs::File::create(&path).expect("failed to create output file");
                    let buf = std::io::BufWriter::with_capacity(256 * 1024, file);

                    if config.gzip {
                        OutputWriter::Gz(GzEncoder::new(
                            buf,
                            flate2::Compression::new(config.gzip_level),
                        ))
                    } else {
                        OutputWriter::Plain(buf)
                    }
                });

                writer.write_all(&record.data).map_err(Error::Io)?;
                reads_written += 1;
            }
        }

        spots_read += read_lengths.len() as u64;

        // Log progress every 50 blobs.
        if (blob_idx + 1) % 50 == 0 || blob_idx + 1 == num_blobs {
            tracing::info!(
                "{accession}: decoded {}/{} blobs, {} reads so far",
                blob_idx + 1,
                num_blobs,
                spots_read,
            );
        }

        // Blob data (read_data, quality_all, read_lengths) is dropped here,
        // freeing memory before processing the next blob.
    }

    tracing::info!(
        "{accession}: streaming decode complete -- {} reads, {} written",
        spots_read,
        reads_written,
    );

    // Finish all writers.
    for (_, writer) in writers {
        writer.finish().map_err(Error::Io)?;
    }

    Ok((spots_read, reads_written, output_files))
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Statistics from a completed fastq conversion (no download).
pub struct FastqStats {
    /// The accession/label used in FASTQ deflines.
    pub accession: String,
    /// Number of spots (rows) read from the SRA file.
    pub spots_read: u64,
    /// Number of FASTQ reads written (after filtering).
    pub reads_written: u64,
    /// Paths of all output files created.
    pub output_files: Vec<PathBuf>,
}

/// Convert a local SRA file to FASTQ without downloading.
///
/// Opens the SRA file as a KAR archive, creates a VdbCursor, decodes VDB
/// blobs, and writes FASTQ output. The `accession` is used for FASTQ
/// defline naming; if `None`, the filename stem is used.
pub fn run_fastq(
    sra_path: &std::path::Path,
    accession: Option<&str>,
    config: &PipelineConfig,
) -> Result<FastqStats> {
    let acc = accession.map(String::from).unwrap_or_else(|| {
        sra_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown")
            .to_string()
    });

    // Detect SRA-lite by checking if the quality column is absent.
    // We pass `false` initially; decode_and_write will handle quality
    // absence gracefully via sra_lite_quality fallback.
    let is_lite = false;

    let (spots_read, reads_written, output_files) =
        decode_and_write(sra_path, &acc, config, is_lite)?;

    Ok(FastqStats {
        accession: acc,
        spots_read,
        reads_written,
        output_files,
    })
}

/// Run the full get pipeline for a single accession.
///
/// This is the full-download approach:
/// 1. Download the complete SRA file to a temporary location
/// 2. Open as KAR archive, parse SEQUENCE table columns
/// 3. Decode VDB blobs, format FASTQ, compress output
/// 4. Delete temporary SRA file
pub async fn run_get(
    resolved: &ResolvedAccession,
    config: &PipelineConfig,
) -> Result<PipelineStats> {
    let accession = &resolved.accession;
    let total_sra_size = resolved.sra_file.size;
    let url = select_mirror(resolved)?;
    let urls = vec![url.clone()];

    tracing::info!("{accession}: starting full download from {url}");

    // --- Phase 1: Download the full SRA file ---
    let temp_filename = format!(".sracha-tmp-{accession}.sra");
    let temp_path = config.output_dir.join(&temp_filename);

    // Ensure output directory exists before downloading.
    tokio::fs::create_dir_all(&config.output_dir).await?;

    let dl_config = DownloadConfig {
        connections: config.connections,
        chunk_size: 0, // adaptive
        force: true,   // always overwrite temp file
        validate: false,
        progress: config.progress,
    };

    tracing::info!(
        "{accession}: downloading {} to {}",
        crate::util::format_size(total_sra_size),
        temp_path.display(),
    );

    let dl_result = download_file(
        &urls,
        total_sra_size,
        resolved.sra_file.md5.as_deref(),
        &temp_path,
        &dl_config,
    )
    .await?;

    let bytes_downloaded = dl_result.size;

    tracing::info!(
        "{accession}: download complete ({})",
        crate::util::format_size(bytes_downloaded),
    );

    // --- Phase 2: Parse VDB + output FASTQ ---
    let is_lite = resolved.sra_file.is_lite;
    let accession_owned = accession.clone();
    let temp_path_for_blocking = temp_path.clone();

    let config_for_blocking = PipelineConfig {
        output_dir: config.output_dir.clone(),
        split_mode: config.split_mode,
        gzip: config.gzip,
        gzip_level: config.gzip_level,
        threads: config.threads,
        connections: config.connections,
        skip_technical: config.skip_technical,
        min_read_len: config.min_read_len,
        force: config.force,
        progress: config.progress,
    };

    let (spots_read, reads_written, output_files) = tokio::task::spawn_blocking(move || {
        decode_and_write(
            &temp_path_for_blocking,
            &accession_owned,
            &config_for_blocking,
            is_lite,
        )
    })
    .await
    .map_err(|e| Error::Vdb(format!("decode task panicked: {e}")))??;

    // --- Phase 3: Cleanup ---
    if let Err(e) = tokio::fs::remove_file(&temp_path).await {
        tracing::warn!(
            "{accession}: failed to remove temp file {}: {e}",
            temp_path.display(),
        );
    }

    tracing::info!(
        "{accession}: done -- {spots_read} spots, {reads_written} reads written, \
         {} downloaded",
        crate::util::format_size(bytes_downloaded),
    );

    Ok(PipelineStats {
        accession: accession.clone(),
        spots_read,
        reads_written,
        bytes_downloaded,
        total_sra_size,
        output_files,
    })
}
