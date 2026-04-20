mod cli;
mod progress;
mod style;
mod vdb_cmd;

use std::path::Path;

use anyhow::{Context, Result};
use clap::Parser;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::format::{self, FormatEvent, FormatFields};
use tracing_subscriber::registry::LookupSpan;

struct SrachaFormatter;

impl<S, N> FormatEvent<S, N> for SrachaFormatter
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &tracing_subscriber::fmt::FmtContext<'_, S, N>,
        mut writer: format::Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> std::fmt::Result {
        let now = chrono::Local::now();
        write!(writer, "{}", now.format("%Y-%m-%d %H:%M:%S"))?;

        write!(writer, " {:>5}", event.metadata().level())?;

        let target = event.metadata().target();
        if let Some(module) = target.strip_prefix("sracha_core::") {
            write!(writer, " [{module}]")?;
        } else if target != "sracha" && target != "sracha_core" {
            write!(writer, " [{target}]")?;
        }

        write!(writer, " ")?;
        ctx.field_format().format_fields(writer.by_ref(), event)?;
        writeln!(writer)
    }
}

use cli::{Cli, Command};
use sracha_core::accession::{self, InputAccession};
use sracha_core::sdl::{ResolvedAccession, SdlClient};
use sracha_core::util::format_size;

#[tokio::main]
async fn main() -> Result<()> {
    // Reset SIGPIPE to default so piping to `head` etc. exits cleanly
    // instead of printing a BrokenPipe error.
    #[cfg(unix)]
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_DFL);
    }

    let cli = Cli::parse();

    // Set up tracing
    let filter = match (cli.quiet, cli.verbose) {
        (true, _) => "error",
        (_, 0) => "warn",
        (_, 1) => "info",
        (_, 2) => "debug",
        _ => "trace",
    };
    tracing_subscriber::fmt()
        .event_format(SrachaFormatter)
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(filter)),
        )
        .with_writer(std::io::stderr)
        .init();

    match cli.command {
        Command::Fetch(args) => {
            let raw = collect_accessions(&args.accessions, args.accession_list.as_deref())?;
            let http_client = sracha_core::http::default_client();
            let client = SdlClient::with_client(http_client.clone());
            let (run_accessions, has_projects) = if args.prefer_ena {
                resolve_to_runs_via_ena(&raw, &client).await?
            } else {
                resolve_to_runs(&raw, &client).await?
            };

            // ENA fast path for fetch: when --prefer-ena is set, download ENA
            // FASTQ.gz files directly instead of the SRA binary. We query ENA
            // before SDL so we can skip the SRA resolve entirely for hits.
            let ena_results: Vec<Option<sracha_core::ena::EnaResolved>> = if args.prefer_ena {
                let sp = progress::Spinner::start(format!(
                    "Querying ENA filereport for {} accession(s)",
                    style::count(run_accessions.len()),
                ));
                let pairs = sracha_core::ena::resolve_ena_many(&http_client, &run_accessions).await;
                let hits = pairs.iter().filter(|(_, v)| v.is_some()).count();
                sp.finish(format!(
                    "ENA serves {} of {} accession(s); rest will use NCBI",
                    style::count(hits),
                    style::count(run_accessions.len()),
                ));
                pairs.into_iter().map(|(_, v)| v).collect()
            } else {
                vec![None; run_accessions.len()]
            };

            tokio::fs::create_dir_all(&args.output_dir).await?;

            // Handle ENA hits first (pure HTTP, no SDL involvement).
            for (i, acc) in run_accessions.iter().enumerate() {
                let Some(ena) = &ena_results[i] else {
                    continue;
                };
                let dl_config = sracha_core::download::DownloadConfig {
                    connections: args.connections,
                    force: args.force,
                    validate: !args.no_validate,
                    progress: !args.no_progress,
                    resume: !args.no_resume,
                    client: Some(http_client.clone()),
                    ..Default::default()
                };
                for file in &ena.fastq_files {
                    let name = ena_fetch_filename(acc, file.slot);
                    let out = args.output_dir.join(&name);
                    tracing::info!(
                        "{acc}: downloading ENA FASTQ {} ({}) to {}",
                        file.url,
                        format_size(file.size),
                        out.display(),
                    );
                    sracha_core::download::download_file(
                        std::slice::from_ref(&file.url),
                        file.size,
                        Some(&file.md5),
                        &out,
                        &dl_config,
                    )
                    .await?;
                    eprintln!(
                        "{}: {} downloaded from [{}]",
                        style::header(acc),
                        style::value(format_size(file.size)),
                        style::value("ena"),
                    );
                    eprintln!("  wrote {}", style::path(out.display()));
                }
            }

            // Resolve the remaining accessions (non-ENA) via SDL as before.
            let ncbi_accessions: Vec<String> = run_accessions
                .iter()
                .zip(ena_results.iter())
                .filter(|(_, ena)| ena.is_none())
                .map(|(acc, _)| acc.clone())
                .collect();

            if ncbi_accessions.is_empty() {
                return Ok(());
            }

            let sp = progress::Spinner::start(format!(
                "Resolving {} accession(s)",
                style::count(ncbi_accessions.len()),
            ));
            let resolved_all = resolve_accessions(
                &ncbi_accessions,
                &client,
                args.prefer_sdl,
                false,
                args.format.into(),
            )
            .await?;
            sp.finish(format!(
                "Resolved {} accession(s)",
                style::count(resolved_all.len()),
            ));
            check_download_confirmation(&resolved_all, args.yes, has_projects)?;
            check_disk_space(&resolved_all, &args.output_dir)?;

            for resolved in &resolved_all {
                let acc = &resolved.accession;
                let mirror = resolved
                    .sra_file
                    .mirrors
                    .iter()
                    .min_by_key(|m| match m.service.as_str() {
                        "s3" | "s3-direct" => 0u8,
                        "gs" => 1,
                        _ if m.service.contains("sra-ncbi") => 2,
                        "ncbi" => 3,
                        _ => 4,
                    })
                    .ok_or_else(|| anyhow::anyhow!("no mirrors for {acc}"))?;
                let url = mirror.url.clone();
                let output_path = args.output_dir.join(format!("{acc}.sra"));
                let dl_config = sracha_core::download::DownloadConfig {
                    connections: args.connections,
                    force: args.force,
                    validate: !args.no_validate,
                    progress: !args.no_progress,
                    resume: !args.no_resume,
                    client: Some(http_client.clone()),
                    ..Default::default()
                };
                tracing::info!(
                    "{acc}: downloading {} from [{}] to {}",
                    format_size(resolved.sra_file.size),
                    mirror.service,
                    output_path.display()
                );
                sracha_core::download::download_file(
                    &[url],
                    resolved.sra_file.size,
                    resolved.sra_file.md5.as_deref(),
                    &output_path,
                    &dl_config,
                )
                .await?;
                eprintln!(
                    "{}: {} downloaded from [{}]",
                    style::header(acc),
                    style::value(format_size(resolved.sra_file.size)),
                    style::value(&mirror.service),
                );
                eprintln!("  wrote {}", style::path(output_path.display()));

                // If SDL returned a vdbcache sidecar (modern cSRA
                // archives split the REFERENCE / extra alignment
                // columns out of the main KAR), download it next to
                // the .sra so `sracha fastq` can find it via the
                // `.sra.vdbcache` convention.
                if let Some(ref vdb) = resolved.vdbcache_file {
                    let vdb_mirror = vdb
                        .mirrors
                        .iter()
                        .min_by_key(|m| match m.service.as_str() {
                            "s3" | "s3-direct" => 0u8,
                            "gs" => 1,
                            _ if m.service.contains("sra-ncbi") => 2,
                            "ncbi" => 3,
                            _ => 4,
                        })
                        .ok_or_else(|| anyhow::anyhow!("no vdbcache mirrors for {acc}"))?;
                    let vdb_url = vdb_mirror.url.clone();
                    let vdb_path = args.output_dir.join(format!("{acc}.sra.vdbcache"));
                    tracing::info!(
                        "{acc}: downloading vdbcache {} from [{}] to {}",
                        format_size(vdb.size),
                        vdb_mirror.service,
                        vdb_path.display()
                    );
                    sracha_core::download::download_file(
                        &[vdb_url],
                        vdb.size,
                        vdb.md5.as_deref(),
                        &vdb_path,
                        &dl_config,
                    )
                    .await?;
                    eprintln!(
                        "  vdbcache {} wrote {}",
                        style::value(format_size(vdb.size)),
                        style::path(vdb_path.display())
                    );
                }
            }
            Ok(())
        }
        Command::Fastq(args) => {
            let format_label = if args.fasta { "FASTA" } else { "FASTQ" };
            tracing::info!(
                "converting {} input(s) to {format_label}",
                args.inputs.len()
            );

            let split_mode =
                cli::resolve_split_mode(args.split, args.stdout).map_err(|e| anyhow::anyhow!(e))?;
            let compression = cli::resolve_compression(
                args.stdout,
                args.zstd,
                args.zstd_level,
                args.threads,
                args.no_gzip,
                args.gzip_level,
            );

            for input in &args.inputs {
                let sra_path = std::path::Path::new(input);
                if !sra_path.exists() {
                    eprintln!(
                        "{} file not found: {}",
                        style::error_label("error:"),
                        style::path(input)
                    );
                    if accession::parse_input(input).is_ok() {
                        eprintln!(
                            "  {} {} looks like an accession — use `sracha get {}` to download and convert",
                            style::label("hint:"),
                            input,
                            input,
                        );
                    }
                    continue;
                }

                let pipeline_config = sracha_core::pipeline::PipelineConfig {
                    output_dir: args.output_dir.clone(),
                    split_mode,
                    compression,
                    threads: args.threads,
                    connections: 1,
                    skip_technical: !args.include_technical,
                    min_read_len: args.min_read_len,
                    force: args.force,
                    progress: !args.no_progress && !args.stdout,
                    run_info: None,
                    fasta: args.fasta,
                    resume: true,
                    stdout: args.stdout,
                    cancelled: None,
                    http_client: None,
                    strict: args.strict,
                    keep_sra: false,
                    progress_parent: None,
                };

                let stats = sracha_core::pipeline::run_fastq(sra_path, None, &pipeline_config)?;

                eprintln!(
                    "{}: {} spots, {} reads written",
                    style::header(&stats.accession),
                    style::count(stats.spots_read),
                    style::count(stats.reads_written),
                );
                if !args.stdout {
                    for path in &stats.output_files {
                        eprintln!("  wrote {}", style::path(path.display()));
                    }
                }
            }

            Ok(())
        }
        Command::Get(args) => {
            let split_mode =
                cli::resolve_split_mode(args.split, args.stdout).map_err(|e| anyhow::anyhow!(e))?;

            let raw = collect_accessions(&args.accessions, args.accession_list.as_deref())?;
            let http_client = sracha_core::http::default_client();
            let sdl_client = SdlClient::with_client(http_client.clone());

            let compression = cli::resolve_compression(
                args.stdout,
                args.zstd,
                args.zstd_level,
                args.threads,
                args.no_gzip,
                args.gzip_level,
            );

            // Phase 1 ENA fast path is only valid for gzip output, split-3 or
            // split-files, no fasta, no stdout. Anything else falls back to
            // the NCBI pipeline even if `--prefer-ena` is set.
            let ena_compatible = args.prefer_ena
                && matches!(
                    compression,
                    sracha_core::fastq::CompressionMode::Gzip { .. }
                )
                && matches!(
                    split_mode,
                    sracha_core::fastq::SplitMode::Split3
                        | sracha_core::fastq::SplitMode::SplitFiles
                )
                && !args.fasta
                && !args.stdout;

            if args.prefer_ena && !ena_compatible {
                tracing::info!(
                    "--prefer-ena requested but output config is incompatible \
                     (needs gzip + split-3/split-files + no-fasta + no-stdout); \
                     falling back to NCBI pipeline"
                );
            }

            let (run_accessions, has_projects) = if ena_compatible {
                resolve_to_runs_via_ena(&raw, &sdl_client).await?
            } else {
                resolve_to_runs(&raw, &sdl_client).await?
            };
            let sp = progress::Spinner::start(format!(
                "Resolving {} accession(s)",
                style::count(run_accessions.len()),
            ));
            let resolved_all = resolve_accessions(
                &run_accessions,
                &sdl_client,
                args.prefer_sdl,
                !args.no_runinfo,
                args.format.into(),
            )
            .await?;
            sp.finish(format!(
                "Resolved {} accession(s)",
                style::count(resolved_all.len()),
            ));
            check_download_confirmation(&resolved_all, args.yes, has_projects)?;
            check_disk_space(&resolved_all, &args.output_dir)?;

            let format_label = if args.fasta { "FASTA" } else { "FASTQ" };
            tracing::info!(
                "get {} run accession(s) -> {format_label}",
                resolved_all.len()
            );

            let progress = !args.no_progress && !args.stdout;

            // If ENA is in play, resolve filereport metadata for every run up
            // front. Accessions ENA can't serve (empty/None) silently fall back
            // to the NCBI pipeline per-accession below.
            let ena_results: Vec<Option<sracha_core::ena::EnaResolved>> = if ena_compatible {
                let sp = progress::Spinner::start(format!(
                    "Querying ENA filereport for {} accession(s)",
                    style::count(resolved_all.len()),
                ));
                let accs: Vec<String> = resolved_all.iter().map(|r| r.accession.clone()).collect();
                let pairs = sracha_core::ena::resolve_ena_many(&http_client, &accs).await;
                let ena_hits = pairs.iter().filter(|(_, v)| v.is_some()).count();
                sp.finish(format!(
                    "ENA serves {} of {} accession(s); rest will use NCBI",
                    style::count(ena_hits),
                    style::count(resolved_all.len()),
                ));
                pairs.into_iter().map(|(_, v)| v).collect()
            } else {
                vec![None; resolved_all.len()]
            };

            // Check platform — reject legacy platforms with complex read structures.
            // Only applies to accessions that will go through VDB decode; ENA
            // serves pre-decoded FASTQ so the platform is irrelevant there.
            for (resolved, ena) in resolved_all.iter().zip(ena_results.iter()) {
                if ena.is_some() {
                    continue;
                }
                if let Some(ref ri) = resolved.run_info
                    && let Some(ref platform) = ri.platform
                    && sracha_core::pipeline::is_unsupported_platform(platform)
                {
                    anyhow::bail!(
                        "{}: unsupported platform '{}' — sracha does not support legacy sequencing platforms (use --prefer-ena to bypass VDB decode)",
                        resolved.accession,
                        platform
                    );
                }
            }

            // -- Ctrl-C signal handling --
            use std::sync::Arc;
            use std::sync::atomic::{AtomicBool, Ordering};

            let cancelled = Arc::new(AtomicBool::new(false));
            {
                let cancelled = cancelled.clone();
                tokio::spawn(async move {
                    tokio::signal::ctrl_c().await.ok();
                    eprintln!(
                        "\nInterrupted -- shutting down gracefully. \
                         Press Ctrl-C again to force quit."
                    );
                    cancelled.store(true, Ordering::SeqCst);

                    tokio::signal::ctrl_c().await.ok();
                    eprintln!("\nForce quit.");
                    std::process::exit(1);
                });
            }

            let mut completed_accessions: Vec<String> = Vec::new();
            let mut interrupted_accession: Option<String> = None;

            // Process accessions with an N-deep download prefetch queue:
            // while decoding accession i, keep the next `prefetch_depth`
            // downloads in flight so slow networks don't stall decode.
            // Depth 1 matches the old behavior; depth 2+ costs one extra
            // temp file on disk per step but hides network latency.
            let prefetch_depth = args.prefetch_depth.max(1);
            let mut pending_downloads: std::collections::VecDeque<
                tokio::task::JoinHandle<
                    sracha_core::error::Result<sracha_core::pipeline::DownloadedSra>,
                >,
            > = std::collections::VecDeque::new();

            let make_config = {
                let http_client = http_client.clone();
                let cancelled = cancelled.clone();
                move |resolved: &ResolvedAccession| sracha_core::pipeline::PipelineConfig {
                    output_dir: args.output_dir.clone(),
                    split_mode,
                    compression,
                    threads: args.threads,
                    connections: args.connections,
                    skip_technical: !args.include_technical,
                    min_read_len: args.min_read_len,
                    force: args.force,
                    progress,
                    run_info: resolved.run_info.clone(),
                    fasta: args.fasta,
                    resume: !args.no_resume,
                    stdout: args.stdout,
                    cancelled: Some(cancelled.clone()),
                    strict: args.strict,
                    http_client: Some(http_client.clone()),
                    keep_sra: args.keep_sra,
                    // run_get_streaming auto-creates a MultiProgress if
                    // progress is on and this is None — which is what we
                    // want for the single-accession streaming fast path.
                    progress_parent: None,
                }
            };

            // ----------------------------------------------------------
            // Phase 3e: streaming fast path for single-NCBI-accession Get.
            //
            // For a single accession the prefetch_depth queue gives no
            // win (there's no N+1 to download while N decodes), but
            // run_get_streaming overlaps the SAME accession's download
            // and decode for ~10-15% e2e wall-clock improvement on
            // medium files (~25%+ on multi-GB files where decode is a
            // bigger share of e2e). Multi-accession runs stay on the
            // prefetch_depth path below; cross-accession prefetch
            // dominates when N is large and decode << download.
            if resolved_all.len() == 1 && ena_results[0].is_none() {
                let resolved = &resolved_all[0];
                let pipeline_config = make_config(resolved);
                let source = resolved
                    .sra_file
                    .mirrors
                    .first()
                    .map(|m| m.service.clone())
                    .unwrap_or_else(|| "unknown".into());

                match sracha_core::pipeline::run_get_streaming(resolved, &pipeline_config).await {
                    Ok(stats) => {
                        if stats.spots_read == 0 && stats.bytes_transferred == 0 {
                            eprintln!(
                                "{}: outputs already exist, skipped (use --force to re-process)",
                                style::header(&stats.accession),
                            );
                        } else if stats.bytes_transferred == 0 {
                            eprintln!(
                                "{}: {} spots, {} reads written (cached, no download needed)",
                                style::header(&stats.accession),
                                style::count(stats.spots_read),
                                style::count(stats.reads_written),
                            );
                        } else if stats.bytes_transferred < stats.total_sra_size {
                            eprintln!(
                                "{}: {} spots, {} reads written, {} of {} transferred from [{}] (resumed)",
                                style::header(&stats.accession),
                                style::count(stats.spots_read),
                                style::count(stats.reads_written),
                                style::value(format_size(stats.bytes_transferred)),
                                style::value(format_size(stats.total_sra_size)),
                                style::value(&source),
                            );
                        } else {
                            eprintln!(
                                "{}: {} spots, {} reads written, {} downloaded from [{}]",
                                style::header(&stats.accession),
                                style::count(stats.spots_read),
                                style::count(stats.reads_written),
                                style::value(format_size(stats.total_sra_size)),
                                style::value(&source),
                            );
                        }
                        if !args.stdout {
                            for path in &stats.output_files {
                                eprintln!("  wrote {}", style::path(path.display()));
                            }
                        }
                    }
                    Err(sracha_core::error::Error::Cancelled { .. }) => {
                        if args.stdout {
                            eprintln!(
                                "Interrupted -- cleaned up partial files for {}.",
                                style::header(&resolved.accession),
                            );
                        } else {
                            eprintln!(
                                "Interrupted -- cleaned up partial output for {} \
                                 (temp SRA kept, next run will skip download).",
                                style::header(&resolved.accession),
                            );
                        }
                        std::process::exit(130);
                    }
                    Err(e) => return Err(e.into()),
                }
                return Ok(());
            }
            // ----------------------------------------------------------

            // Spawn a download task for resolved_all[idx]. Only used for the
            // NCBI path; ENA accessions are handled inline (no decode to
            // overlap with, so prefetching buys nothing).
            let spawn_download = |idx: usize| {
                let resolved = resolved_all[idx].clone();
                let config = make_config(&resolved);
                tokio::spawn(async move {
                    sracha_core::pipeline::download_sra(&resolved, &config).await
                })
            };

            // Seed the prefetch queue with the first `prefetch_depth` indices
            // that will go through the NCBI path. Skip ENA indices — they
            // don't need prefetching.
            let mut seeded = 0usize;
            for (j, ena) in ena_results.iter().enumerate() {
                if seeded >= prefetch_depth {
                    break;
                }
                if ena.is_none() {
                    pending_downloads.push_back(spawn_download(j));
                    seeded += 1;
                }
            }

            // Cursor for topping up the prefetch queue: the next index past
            // the already-seeded set that still needs an NCBI download.
            let mut next_prefetch_idx = {
                let mut n = 0usize;
                let mut count = 0usize;
                while count < prefetch_depth && n < resolved_all.len() {
                    if ena_results[n].is_none() {
                        count += 1;
                    }
                    n += 1;
                }
                n
            };

            for (i, resolved) in resolved_all.iter().enumerate() {
                if cancelled.load(Ordering::Relaxed) {
                    break;
                }

                let pipeline_config = make_config(resolved);

                // ---- ENA fast path ------------------------------------------------
                if let Some(ena) = &ena_results[i] {
                    let ena_future =
                        sracha_core::pipeline::download_ena_fastq(ena, &pipeline_config);
                    let stats = match ena_future.await {
                        Ok(s) => s,
                        Err(sracha_core::error::Error::Cancelled { .. }) => {
                            interrupted_accession = Some(resolved.accession.clone());
                            break;
                        }
                        Err(e) => return Err(e.into()),
                    };
                    let source = "ena";
                    if stats.bytes_transferred == 0 {
                        eprintln!(
                            "{}: {} of {} cached (no download needed) from [{}]",
                            style::header(&stats.accession),
                            style::value(format_size(stats.total_sra_size)),
                            style::value(format_size(stats.total_sra_size)),
                            style::value(source),
                        );
                    } else if stats.bytes_transferred < stats.total_sra_size {
                        eprintln!(
                            "{}: {} of {} transferred from [{}] (resumed)",
                            style::header(&stats.accession),
                            style::value(format_size(stats.bytes_transferred)),
                            style::value(format_size(stats.total_sra_size)),
                            style::value(source),
                        );
                    } else {
                        eprintln!(
                            "{}: {} downloaded from [{}]",
                            style::header(&stats.accession),
                            style::value(format_size(stats.total_sra_size)),
                            style::value(source),
                        );
                    }
                    for path in &stats.output_files {
                        eprintln!("  wrote {}", style::path(path.display()));
                    }
                    completed_accessions.push(resolved.accession.clone());
                    continue;
                }

                // ---- NCBI path ----------------------------------------------------
                let handle = pending_downloads
                    .pop_front()
                    .expect("queue is pre-seeded and topped up per iteration");
                let downloaded = match handle.await {
                    Ok(Ok(d)) => d,
                    Ok(Err(sracha_core::error::Error::Cancelled { .. })) => {
                        interrupted_accession = Some(resolved.accession.clone());
                        break;
                    }
                    Ok(Err(e)) => return Err(e.into()),
                    Err(join_err) => {
                        if cancelled.load(Ordering::Relaxed) {
                            interrupted_accession = Some(resolved.accession.clone());
                            break;
                        }
                        return Err(anyhow::anyhow!("download task panicked: {join_err}"));
                    }
                };

                // Top up the queue: find the next NCBI-bound index and spawn it.
                while next_prefetch_idx < resolved_all.len()
                    && ena_results[next_prefetch_idx].is_some()
                {
                    next_prefetch_idx += 1;
                }
                if next_prefetch_idx < resolved_all.len() && !cancelled.load(Ordering::Relaxed) {
                    pending_downloads.push_back(spawn_download(next_prefetch_idx));
                    next_prefetch_idx += 1;
                }
                // Skip trailing ENA indices after the one we just spawned.
                let _ = i;

                // Decode (CPU-bound) while the next download runs in the background.
                let source = resolved
                    .sra_file
                    .mirrors
                    .first()
                    .map(|m| m.service.clone())
                    .unwrap_or_else(|| "unknown".into());
                match tokio::task::block_in_place(|| {
                    sracha_core::pipeline::decode_sra(&downloaded, &pipeline_config)
                }) {
                    Ok(stats) => {
                        if stats.spots_read == 0 && stats.bytes_transferred == 0 {
                            eprintln!(
                                "{}: outputs already exist, skipped (use --force to re-process)",
                                style::header(&stats.accession),
                            );
                        } else if stats.bytes_transferred == 0 {
                            eprintln!(
                                "{}: {} spots, {} reads written (cached, no download needed)",
                                style::header(&stats.accession),
                                style::count(stats.spots_read),
                                style::count(stats.reads_written),
                            );
                        } else if stats.bytes_transferred < stats.total_sra_size {
                            eprintln!(
                                "{}: {} spots, {} reads written, {} of {} transferred from [{}] (resumed)",
                                style::header(&stats.accession),
                                style::count(stats.spots_read),
                                style::count(stats.reads_written),
                                style::value(format_size(stats.bytes_transferred)),
                                style::value(format_size(stats.total_sra_size)),
                                style::value(&source),
                            );
                        } else {
                            eprintln!(
                                "{}: {} spots, {} reads written, {} downloaded from [{}]",
                                style::header(&stats.accession),
                                style::count(stats.spots_read),
                                style::count(stats.reads_written),
                                style::value(format_size(stats.total_sra_size)),
                                style::value(&source),
                            );
                        }
                        if !args.stdout {
                            for path in &stats.output_files {
                                eprintln!("  wrote {}", style::path(path.display()));
                            }
                        }
                        completed_accessions.push(resolved.accession.clone());
                    }
                    Err(sracha_core::error::Error::Cancelled { .. }) => {
                        interrupted_accession = Some(resolved.accession.clone());
                        break;
                    }
                    Err(e) => return Err(e.into()),
                }
            }

            // Let any in-flight prefetch downloads detect the cancellation
            // flag and clean up their temp files gracefully (~100 ms each).
            while let Some(handle) = pending_downloads.pop_front() {
                let _ = handle.await;
            }

            // Print summary and exit if interrupted.
            if cancelled.load(Ordering::Relaxed) {
                let interrupted = interrupted_accession.as_deref().unwrap_or("unknown");
                let suffix = if completed_accessions.is_empty() {
                    String::new()
                } else {
                    format!(" Completed: {}", completed_accessions.join(", "))
                };
                if args.stdout {
                    eprintln!(
                        "Interrupted -- cleaned up partial files for {}.{suffix}",
                        style::header(interrupted),
                    );
                } else {
                    eprintln!(
                        "Interrupted -- cleaned up partial output for {} \
                         (temp SRA kept, next run will skip download).{suffix}",
                        style::header(interrupted),
                    );
                }
                std::process::exit(130);
            }

            Ok(())
        }
        Command::Info(args) => {
            let raw = collect_accessions(&args.accessions, args.accession_list.as_deref())?;

            // Split local file paths from accession strings. A bare file path
            // goes straight to the local-file path; anything else is looked up
            // via SDL/EUtils. `~/` is expanded so users can pass shell paths.
            let (paths, accessions): (Vec<_>, Vec<_>) = raw
                .into_iter()
                .map(|s| expand_tilde(&s))
                .partition(|s| std::path::Path::new(s).is_file());

            for p in &paths {
                print_local_file_info(std::path::Path::new(p));
            }

            if accessions.is_empty() {
                return Ok(());
            }

            let http_client = sracha_core::http::default_client();
            let client = SdlClient::with_client(http_client.clone());
            let (run_accessions, _has_projects) = if args.prefer_ena {
                resolve_to_runs_via_ena(&accessions, &client).await?
            } else {
                resolve_to_runs(&accessions, &client).await?
            };

            let sp = progress::Spinner::start(format!(
                "Resolving {} accession(s)",
                style::count(run_accessions.len()),
            ));
            // Use the same resolver as `get`/`fetch` so info reports the
            // actual download source (s3-direct vs sdl-s3/ncbi/gs)
            // that sracha will pick up when downloading. Failures in the
            // S3 probe fall through to SDL inside `resolve_accessions`,
            // so per-accession errors are surfaced as a single bail.
            let resolved = match resolve_accessions(
                &run_accessions,
                &client,
                false, // prefer_sdl
                true,  // need_run_info
                sracha_core::sdl::FormatPreference::Sra,
            )
            .await
            {
                Ok(r) => r,
                Err(e) => {
                    sp.finish(format!(
                        "Resolved {} accession(s) with errors",
                        style::count(run_accessions.len()),
                    ));
                    eprintln!("{} {e}", style::error_label("error:"));
                    return Ok(());
                }
            };
            sp.finish(format!(
                "Resolved {} accession(s)",
                style::count(resolved.len()),
            ));

            let entries: Vec<InfoEntry> = resolved.iter().map(InfoEntry::Ok).collect();

            if entries.len() > 1 {
                // Project/multi-accession: summary table.
                print_info_table(&entries);
            } else if let Some(InfoEntry::Ok(r)) = entries.first() {
                print_resolved(r);
            }

            if args.prefer_ena {
                let accs: Vec<String> = resolved.iter().map(|r| r.accession.clone()).collect();
                let ena = sracha_core::ena::resolve_ena_many(&http_client, &accs).await;
                for (acc, r) in &ena {
                    print_ena_section(acc, r.as_ref());
                }
            }
            Ok(())
        }
        Command::Validate(args) => {
            use sracha_core::accession;
            use sracha_core::sdl::FormatPreference;
            use tabled::builder::Builder;
            use tabled::settings::object::{Columns, Rows};
            use tabled::settings::{Alignment, Color, Modify, Style};

            let mut all_valid = true;

            struct ValidateRow {
                label: String,
                valid: bool,
                spots: String,
                blobs: String,
                columns: String,
                md5_status: String,
                errors: Vec<String>,
                any_blob_integrity_error: bool,
            }

            let mut rows: Vec<ValidateRow> = Vec::new();

            let sdl = if args.md5.is_none() && !args.offline {
                Some(SdlClient::with_client(sracha_core::http::default_client()))
            } else {
                None
            };

            for input in &args.inputs {
                let sra_path = std::path::Path::new(input);
                if !sra_path.exists() {
                    eprintln!(
                        "{} file not found: {}",
                        style::error_label("error:"),
                        style::path(input)
                    );
                    all_valid = false;
                    continue;
                }

                let mut result =
                    sracha_core::pipeline::run_validate(sra_path, args.threads, !args.no_progress);

                // Determine expected MD5: --md5 > SDL lookup > none.
                let expected_md5: Option<String> = if let Some(hash) = &args.md5 {
                    Some(hash.to_lowercase())
                } else if let Some(client) = &sdl {
                    let stem = sra_path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
                    if accession::parse(stem).is_ok() {
                        match client.resolve_one(stem, FormatPreference::Sra).await {
                            Ok(r) => r.sra_file.md5.map(|s| s.to_lowercase()),
                            Err(e) => {
                                tracing::warn!("{stem}: SDL lookup failed: {e}");
                                None
                            }
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };

                let md5_status = match (result.md5.as_deref(), expected_md5.as_deref()) {
                    (Some(got), Some(exp)) => {
                        if got.eq_ignore_ascii_case(exp) {
                            format!("{} ok", &got[..12])
                        } else {
                            result
                                .errors
                                .push(format!("MD5 mismatch: expected {exp}, got {got}"));
                            result.valid = false;
                            format!("MISMATCH ({})", &got[..12])
                        }
                    }
                    (Some(got), None) => format!("{} (no ref)", &got[..12]),
                    (None, _) => String::from("-"),
                };

                if !result.valid {
                    all_valid = false;
                }

                rows.push(ValidateRow {
                    label: result.label,
                    valid: result.valid,
                    spots: result.spots_validated.to_string(),
                    blobs: result.blobs_validated.to_string(),
                    columns: result.columns_found.join(", "),
                    md5_status,
                    errors: result.errors,
                    any_blob_integrity_error: result.any_blob_integrity_error,
                });
            }

            if !rows.is_empty() {
                let mut builder = Builder::new();
                builder.push_record(["File", "Status", "Spots", "Blobs", "Columns", "MD5"]);

                for row in &rows {
                    let status = if row.valid {
                        style::value("ok")
                    } else {
                        style::error_label("FAILED")
                    };
                    builder.push_record([
                        row.label.clone(),
                        status,
                        row.spots.clone(),
                        row.blobs.clone(),
                        row.columns.clone(),
                        row.md5_status.clone(),
                    ]);
                }

                let mut table = builder.build();
                table
                    .with(Style::rounded())
                    .with(Modify::new(Rows::first()).with(Color::new("\x1b[1m", "\x1b[22m")))
                    .with(Modify::new(Columns::new(2..=2)).with(Alignment::right()))
                    .with(Modify::new(Columns::new(3..=3)).with(Alignment::right()));

                eprintln!("{table}");

                // Print error details for failed files below the table.
                for row in &rows {
                    if !row.errors.is_empty() {
                        eprintln!(
                            "\n{}: {} error(s)",
                            style::header(&row.label),
                            style::count(row.errors.len()),
                        );
                        for err in &row.errors {
                            eprintln!("  {err}");
                        }
                    }
                }

                if rows.iter().any(|r| r.any_blob_integrity_error) {
                    eprintln!(
                        "\n{} {}",
                        style::header("hint:"),
                        sracha_core::error::BLOB_INTEGRITY_GUIDANCE,
                    );
                }
            }

            if !all_valid {
                std::process::exit(1);
            }

            Ok(())
        }
        Command::Vdb(args) => vdb_cmd::run(args.cmd),
    }
}

/// Collect accessions from positional arguments and an optional file.
///
/// Lines in the accession list file are trimmed; blank lines and lines
/// starting with `#` are skipped.
fn expand_tilde(s: &str) -> String {
    if let Some(rest) = s.strip_prefix("~/")
        && let Ok(home) = std::env::var("HOME")
    {
        return format!("{home}/{rest}");
    }
    s.to_string()
}

fn collect_accessions(positional: &[String], list_file: Option<&Path>) -> Result<Vec<String>> {
    let mut accessions: Vec<String> = positional.to_vec();

    if let Some(path) = list_file {
        let contents =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        for line in contents.lines() {
            let trimmed = line.trim();
            if !trimmed.is_empty() && !trimmed.starts_with('#') {
                accessions.push(trimmed.to_string());
            }
        }
    }

    if accessions.is_empty() {
        anyhow::bail!("no accessions provided (use positional arguments or --accession-list)");
    }

    Ok(accessions)
}

/// Parse each input string and resolve project/study accessions to run accessions.
///
/// Run accessions (SRR/ERR/DRR) pass through unchanged.
/// Study (SRP/ERP/DRP) and BioProject (PRJNA/PRJEB/PRJDB) accessions are
/// resolved to their constituent runs via the NCBI EUtils API.
async fn resolve_to_runs(inputs: &[String], client: &SdlClient) -> Result<(Vec<String>, bool)> {
    resolve_to_runs_inner(inputs, client, false).await
}

/// Output filename for `fetch --prefer-ena`. ENA always serves gzip'd FASTQ,
/// so the extension is fixed; the slot determines the suffix.
fn ena_fetch_filename(accession: &str, slot: sracha_core::fastq::OutputSlot) -> String {
    use sracha_core::fastq::OutputSlot;
    match slot {
        OutputSlot::Single | OutputSlot::Unpaired => format!("{accession}.fastq.gz"),
        OutputSlot::Read1 => format!("{accession}_1.fastq.gz"),
        OutputSlot::Read2 => format!("{accession}_2.fastq.gz"),
        OutputSlot::ReadN(n) => format!("{accession}_{}.fastq.gz", n + 1),
    }
}

/// Like [`resolve_to_runs`] but routes project resolution through ENA's
/// filereport API when possible. Falls back to NCBI EUtils if ENA returns
/// no runs for a project (rare — almost all INSDC projects are mirrored).
///
/// Reuses the HTTP client from the provided [`SdlClient`] so connection
/// pools are shared between SDL and ENA calls.
async fn resolve_to_runs_via_ena(
    inputs: &[String],
    client: &SdlClient,
) -> Result<(Vec<String>, bool)> {
    resolve_to_runs_inner(inputs, client, true).await
}

async fn resolve_to_runs_inner(
    inputs: &[String],
    client: &SdlClient,
    via_ena: bool,
) -> Result<(Vec<String>, bool)> {
    let mut run_accessions = Vec::new();
    let mut has_projects = false;

    for input in inputs {
        let parsed = accession::parse_input(input)?;
        match parsed {
            InputAccession::Run(acc) => {
                run_accessions.push(acc.to_string());
            }
            InputAccession::Project(proj) => {
                has_projects = true;
                let sp = progress::Spinner::start(format!(
                    "{}: resolving project to run accessions",
                    style::header(&proj),
                ));

                let runs = if via_ena {
                    match sracha_core::ena::resolve_ena_project(
                        client.http_client(),
                        &proj.to_string(),
                    )
                    .await
                    {
                        Ok(resolved) if !resolved.is_empty() => {
                            resolved.into_iter().map(|r| r.accession).collect()
                        }
                        Ok(_) => {
                            tracing::info!(
                                "{proj}: ENA returned no runs, falling back to NCBI EUtils",
                            );
                            client.resolve_project(&proj).await?
                        }
                        Err(e) => {
                            tracing::warn!(
                                "{proj}: ENA project resolve failed ({e}), falling back to NCBI EUtils",
                            );
                            client.resolve_project(&proj).await?
                        }
                    }
                } else {
                    client.resolve_project(&proj).await?
                };

                sp.finish(format!(
                    "{}: found {} run(s)",
                    style::header(&proj),
                    style::count(runs.len()),
                ));
                run_accessions.extend(runs);
            }
        }
    }

    Ok((run_accessions, has_projects))
}

fn print_local_file_info(path: &std::path::Path) {
    use sracha_core::vdb::cursor::VdbCursor;
    use sracha_core::vdb::kar::KarArchive;

    let label = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown");

    let size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);

    println!("{}", style::header(label));
    println!(
        "  {}    {}",
        style::label("Path:"),
        style::path(path.display())
    );
    println!(
        "  {}    {}",
        style::label("Size:"),
        style::value(format_size(size))
    );

    let file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("  {} open failed: {e}", style::error_label("error:"));
            return;
        }
    };
    let mut archive = match KarArchive::open(std::io::BufReader::new(file)) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("  {} KAR parse failed: {e}", style::error_label("error:"));
            return;
        }
    };
    // cSRA dispatch: archives we can decode via CsraCursor don't open as
    // a plain VdbCursor (no physical READ column). Render a cSRA-flavoured
    // info block instead of surfacing the reject-if-csra error. When a
    // `.sra.vdbcache` sidecar exists alongside the .sra, check both so
    // modern NCBI split-archive cSRA is recognised.
    let vdbcache = sracha_core::vdb::csra::vdbcache_sidecar_path(path);
    let vdbcache_opt = if vdbcache.exists() {
        Some(vdbcache.as_path())
    } else {
        None
    };
    if sracha_core::vdb::csra::looks_like_decodable_csra(path, vdbcache_opt).unwrap_or(false) {
        print_local_csra_info(path, &mut archive);
        return;
    }

    let cursor = match VdbCursor::open(&mut archive, path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("  {} cursor open failed: {e}", style::error_label("error:"));
            return;
        }
    };

    let platform = cursor.platform().unwrap_or("unknown").to_string();
    let blob_count = cursor.read_col().blob_count();
    let spot_count: u64 = cursor
        .read_col()
        .blobs()
        .iter()
        .map(|b| b.id_range as u64)
        .sum();
    let read_lengths = cursor.metadata_read_lengths().unwrap_or_default();
    let layout = match read_lengths.len() {
        0 => "unknown".to_string(),
        1 => format!("SINGLE ({}bp)", read_lengths[0]),
        n => format!("{n} reads × {read_lengths:?}bp"),
    };

    let mut columns: Vec<&str> = Vec::new();
    columns.push("READ");
    if cursor.quality_col().is_some() {
        columns.push("QUALITY");
    }
    if cursor.read_len_col().is_some() {
        columns.push("READ_LEN");
    }
    if cursor.name_col().is_some() {
        columns.push("NAME");
    }

    println!("  {} {}", style::label("Platform:"), style::value(platform));
    println!("  {}  {}", style::label("Layout:"), style::value(layout));
    println!(
        "  {}   {}",
        style::label("Spots:"),
        style::count(spot_count)
    );
    println!(
        "  {}   {}",
        style::label("Blobs:"),
        style::count(blob_count)
    );
    println!(
        "  {} {}",
        style::label("Columns:"),
        style::value(columns.join(", "))
    );
}

fn print_local_csra_info<R: std::io::Read + std::io::Seek>(
    path: &std::path::Path,
    archive: &mut sracha_core::vdb::kar::KarArchive<R>,
) {
    use sracha_core::vdb::csra::CsraCursor;

    let csra = match CsraCursor::open(archive, path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("  {} cSRA cursor failed: {e}", style::error_label("error:"));
            return;
        }
    };

    println!(
        "  {} {}",
        style::label("Platform:"),
        style::value("reference-compressed cSRA")
    );
    println!(
        "  {}   {}",
        style::label("Spots:"),
        style::count(csra.row_count())
    );
    println!(
        "  {} {}",
        style::label("Columns:"),
        style::value(
            "CMP_READ, PRIMARY_ALIGNMENT_ID, READ_LEN, READ_TYPE, QUALITY (+ PRIMARY_ALIGNMENT + REFERENCE)"
        )
    );
    println!(
        "  {} {}",
        style::label("Note:"),
        style::value("decode via `sracha fastq` (not supported by plain VDB cursor)")
    );
}

fn print_resolved(resolved: &ResolvedAccession) {
    let f = &resolved.sra_file;

    println!("{}", style::header(&resolved.accession));

    // Size and format flavor on one line.
    let format_tag = if f.is_lite {
        "SRA-lite"
    } else {
        "SRA normalized"
    };
    println!(
        "  {}     {}  ({})",
        style::label("Size:"),
        style::value(format_size(f.size)),
        format_tag,
    );

    // Source: whichever mirror sracha will actually use for download.
    // `s3-direct` means the bucket HEAD probe succeeded; anything else
    // (s3/ncbi/gs) came from the SDL locate API.
    if let Some(primary) = f.mirrors.first() {
        println!(
            "  {}   [{}] {}",
            style::label("Source:"),
            style::value(&primary.service),
            style::path(&primary.url),
        );
    }

    println!(
        "  {}      {}",
        style::label("MD5:"),
        style::value(f.md5.as_deref().unwrap_or("not provided")),
    );

    if let Some(ref ri) = resolved.run_info {
        let layout = match ri.nreads {
            1 => "SINGLE".to_string(),
            2 => "PAIRED".to_string(),
            n => format!("{n}-read"),
        };
        let read_desc = if !ri.avg_read_len.is_empty()
            && ri.avg_read_len.iter().all(|&l| l == ri.avg_read_len[0])
        {
            format!("{}×{}bp", ri.nreads, ri.avg_read_len[0])
        } else {
            ri.avg_read_len
                .iter()
                .map(|l| format!("{l}bp"))
                .collect::<Vec<_>>()
                .join("+")
        };
        let spots = ri
            .spots
            .map(|s| format!(", {} spots", style::count(thousands(s))))
            .unwrap_or_default();
        let bases = ri
            .spots
            .map(|s| format!(", {}", style::value(format_bases(s * ri.spot_len as u64))))
            .unwrap_or_default();
        println!(
            "  {}   {} {}{}{}",
            style::label("Layout:"),
            style::value(layout),
            style::value(read_desc),
            spots,
            bases,
        );
        if let Some(ref plat) = ri.platform {
            println!("  {} {}", style::label("Platform:"), style::value(plat),);
        }
    }

    if let Some(ref vdb) = resolved.vdbcache_file {
        println!(
            "  {} yes  ({})",
            style::label("VDBcache:"),
            style::value(format_size(vdb.size)),
        );
    }

    // Alternate mirrors, if any — primary already rendered above.
    if f.mirrors.len() > 1 {
        let alts: Vec<&str> = f
            .mirrors
            .iter()
            .skip(1)
            .map(|m| m.service.as_str())
            .collect();
        println!(
            "  {}  {} alternate(s) [{}]",
            style::label("Mirrors:"),
            style::count(alts.len()),
            alts.join(", "),
        );
    }
}

/// Print the ENA FASTQ section for a single accession — shown under
/// `sracha info --prefer-ena`. Renders URL, size, and MD5 per file, or
/// a single "no ENA FASTQ available" line when `resolved` is None.
fn print_ena_section(accession: &str, resolved: Option<&sracha_core::ena::EnaResolved>) {
    println!();
    println!("{} — ENA FASTQ", style::header(accession));
    let Some(r) = resolved else {
        println!("  {} no ENA FASTQ available", style::label("status:"));
        return;
    };
    println!(
        "  {}    {} across {} file(s)",
        style::label("Total:"),
        style::value(format_size(r.total_size)),
        style::count(r.fastq_files.len()),
    );
    for f in &r.fastq_files {
        let slot_tag = match f.slot {
            sracha_core::fastq::OutputSlot::Single => "single",
            sracha_core::fastq::OutputSlot::Read1 => "read1",
            sracha_core::fastq::OutputSlot::Read2 => "read2",
            sracha_core::fastq::OutputSlot::Unpaired => "unpaired",
            sracha_core::fastq::OutputSlot::ReadN(_) => "readN",
        };
        println!(
            "  {} [{}] {}",
            style::label("File:"),
            style::value(slot_tag),
            style::path(&f.url),
        );
        println!(
            "    {}  {}   {}  {}",
            style::label("size:"),
            style::value(format_size(f.size)),
            style::label("md5:"),
            style::value(&f.md5),
        );
    }
}

/// Insert thousands separators into an integer (e.g. 1234567 → "1,234,567").
fn thousands<T: Into<u64>>(n: T) -> String {
    let s = n.into().to_string();
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, b) in bytes.iter().enumerate() {
        if i > 0 && (bytes.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(*b as char);
    }
    out
}

/// Format a base count as Mbp / Gbp for readability.
fn format_bases(b: u64) -> String {
    const M: u64 = 1_000_000;
    const G: u64 = 1_000_000_000;
    if b >= G {
        format!("{:.2} Gbp", b as f64 / G as f64)
    } else if b >= M {
        format!("{:.2} Mbp", b as f64 / M as f64)
    } else if b >= 1_000 {
        format!("{:.1} Kbp", b as f64 / 1_000.0)
    } else {
        format!("{b} bp")
    }
}

/// One row's worth of `sracha info` state: either a fully resolved record
/// (from SDL/S3) or an error captured during resolution so it can still be
/// rendered as a table row.
pub enum InfoEntry<'a> {
    Ok(&'a ResolvedAccession),
    Error { accession: String, message: String },
}

/// Print a compact table for multiple accessions. Errored entries appear
/// as rows with an `error` status (other columns dashed) and the error
/// message is printed beneath the table.
fn print_info_table(entries: &[InfoEntry<'_>]) {
    use tabled::builder::Builder;
    use tabled::settings::object::{Columns, Rows};
    use tabled::settings::style::HorizontalLine;
    use tabled::settings::{Alignment, Color, Modify, Panel, Style};

    let mut total_size: u64 = 0;
    let mut total_spots: u64 = 0;
    let mut ok_count = 0usize;
    let mut builder = Builder::new();

    builder.push_record([
        "Accession",
        "Size",
        "Layout",
        "Reads",
        "Platform",
        "Type",
        "Spots",
        "Source",
    ]);

    for entry in entries {
        match entry {
            InfoEntry::Ok(r) => {
                let layout = r
                    .run_info
                    .as_ref()
                    .map(|ri| match ri.nreads {
                        1 => "SINGLE".to_string(),
                        2 => "PAIRED".to_string(),
                        n => format!("{n}-read"),
                    })
                    .unwrap_or_else(|| "?".into());
                let reads = r
                    .run_info
                    .as_ref()
                    .map(|ri| {
                        if !ri.avg_read_len.is_empty()
                            && ri.avg_read_len.iter().all(|&l| l == ri.avg_read_len[0])
                        {
                            format!("{}×{}bp", ri.nreads, ri.avg_read_len[0])
                        } else {
                            ri.avg_read_len
                                .iter()
                                .map(|l| format!("{l}bp"))
                                .collect::<Vec<_>>()
                                .join("+")
                        }
                    })
                    .unwrap_or_else(|| "-".into());
                let platform = r
                    .run_info
                    .as_ref()
                    .and_then(|ri| ri.platform.clone())
                    .unwrap_or_else(|| "-".into());
                let spots = r
                    .run_info
                    .as_ref()
                    .and_then(|ri| ri.spots)
                    .map(|s| {
                        total_spots += s;
                        thousands(s)
                    })
                    .unwrap_or_else(|| "-".into());
                let source = r
                    .sra_file
                    .mirrors
                    .first()
                    .map(|m| m.service.clone())
                    .unwrap_or_else(|| "-".into());
                // SDL ships modern reference-compressed cSRA with a
                // `.sra.vdbcache` sidecar carrying PRIMARY_ALIGNMENT /
                // REFERENCE. Its presence is the strongest remote signal
                // short of opening the archive. Legacy monolithic cSRA
                // (pre-split like VDB-3418) has no sidecar and will read
                // as "SRA" here — a known blind spot.
                let archive_type = if r.vdbcache_file.is_some() {
                    "cSRA"
                } else {
                    "SRA"
                };
                total_size += r.sra_file.size;
                ok_count += 1;

                builder.push_record([
                    r.accession.clone(),
                    format_size(r.sra_file.size),
                    layout,
                    reads,
                    platform,
                    archive_type.into(),
                    spots,
                    source,
                ]);
            }
            InfoEntry::Error { accession, .. } => {
                builder.push_record([
                    accession.clone(),
                    "-".into(),
                    "-".into(),
                    "-".into(),
                    "-".into(),
                    "-".into(),
                    "-".into(),
                    style::error_label("error"),
                ]);
            }
        }
    }

    let err_count = entries.len() - ok_count;
    let summary = if err_count == 0 {
        format!(
            "Total: {} across {} run(s), {} spots",
            style::value(format_size(total_size)),
            style::count(ok_count),
            style::count(thousands(total_spots)),
        )
    } else {
        format!(
            "Total: {} across {} ok, {} error",
            style::value(format_size(total_size)),
            style::count(ok_count),
            style::count(err_count),
        )
    };

    let footer_line = entries.len() + 1;
    let mut table = builder.build();
    table
        .with(Panel::footer(summary))
        // `intersection_bottom('─')` flattens the outer bottom border —
        // the footer panel has no column separators, so the default `┴`
        // ticks were sticking up at nothing. The body/footer separator
        // keeps its `┴` (set via horizontals below) since the body row
        // above it does have columns.
        .with(Style::rounded().intersection_bottom('─').horizontals([
            (1, HorizontalLine::full('─', '┼', '├', '┤')),
            (footer_line, HorizontalLine::full('─', '┴', '├', '┤')),
        ]))
        .with(Modify::new(Rows::first()).with(Color::new("\x1b[1m", "\x1b[22m")))
        // Right-align numeric columns: Size (1), Spots (6).
        .with(Modify::new(Columns::new(1..=1)).with(Alignment::right()))
        .with(Modify::new(Columns::new(6..=6)).with(Alignment::right()));

    println!("{table}");

    // Error details below the table so the user can act on each failure.
    for entry in entries {
        if let InfoEntry::Error { accession, message } = entry {
            eprintln!("  {}: {message}", style::header(accession));
        }
    }
}

/// Size threshold (in bytes) above which downloads require `--yes` confirmation.
const LARGE_DOWNLOAD_THRESHOLD: u64 = 100 * 1024 * 1024 * 1024; // 100 GiB

/// Resolve accessions with direct S3 probing, falling back to SDL per-accession.
///
/// When `prefer_sdl` is `true`, skips S3 and uses SDL directly (current behavior).
/// When `need_run_info` is `true`, fetches read structure metadata via EUtils
/// for FASTQ conversion (needed by the `get` command).
/// When `format` is `Sralite`, skips S3 (SRA-lite files are not on the ODP bucket).
async fn resolve_accessions(
    run_accessions: &[String],
    client: &SdlClient,
    prefer_sdl: bool,
    need_run_info: bool,
    format: sracha_core::sdl::FormatPreference,
) -> Result<Vec<ResolvedAccession>> {
    // SRA-lite files are not on the free S3 ODP bucket, so we must use SDL.
    if prefer_sdl || format == sracha_core::sdl::FormatPreference::Sralite {
        if prefer_sdl {
            tracing::info!("using SDL for all accessions (--prefer-sdl)");
        } else {
            tracing::info!("using SDL for all accessions (--format sralite)");
        }
        let resolved: Vec<ResolvedAccession> = client
            .resolve_many(run_accessions, format)
            .await?
            .into_iter()
            .collect::<std::result::Result<Vec<_>, _>>()?;
        return Ok(resolved);
    }

    // Phase 1 + Phase 3 in parallel: the S3 HEAD storm and the EUtils
    // RunInfo EFetch are independent. Kicking them off together saves one
    // round-trip of startup latency (visible mostly on multi-accession
    // `get` runs).
    tracing::info!(
        "probing direct S3 for {} accession(s)...",
        run_accessions.len()
    );
    let s3_future = sracha_core::s3::resolve_direct_many(client.http_client(), run_accessions);
    let run_info_future = async {
        if need_run_info {
            Some(client.fetch_run_info_batch(run_accessions).await)
        } else {
            None
        }
    };
    let (s3_results, run_info_early) = tokio::join!(s3_future, run_info_future);

    let mut resolved: Vec<Option<ResolvedAccession>> = vec![None; run_accessions.len()];
    let mut sdl_needed: Vec<(usize, String)> = Vec::new();

    for (i, acc) in run_accessions.iter().enumerate() {
        match s3_results.get(acc) {
            Some(Ok(r)) => {
                tracing::info!(
                    "{acc}: resolved via direct S3 ({})",
                    format_size(r.sra_file.size)
                );
                resolved[i] = Some(r.clone());
            }
            Some(Err(e)) => {
                tracing::debug!("{acc}: direct S3 probe failed: {e}");
                sdl_needed.push((i, acc.clone()));
            }
            None => {
                sdl_needed.push((i, acc.clone()));
            }
        }
    }

    // Phase 2: Fall back to SDL for accessions that failed direct S3.
    if !sdl_needed.is_empty() {
        let sdl_accs: Vec<String> = sdl_needed.iter().map(|(_, a)| a.clone()).collect();
        tracing::info!("falling back to SDL for {} accession(s)", sdl_accs.len());
        let sdl_results = client.resolve_many(&sdl_accs, format).await?;

        for ((i, acc), result) in sdl_needed.into_iter().zip(sdl_results) {
            match result {
                Ok(r) => {
                    tracing::info!("{acc}: resolved via SDL ({})", format_size(r.sra_file.size));
                    resolved[i] = Some(r);
                }
                Err(e) => {
                    anyhow::bail!("{acc}: failed to resolve via both S3 and SDL: {e}");
                }
            }
        }
    }

    let mut resolved: Vec<ResolvedAccession> = resolved.into_iter().flatten().collect();

    // Phase 2.5: Supplement S3-resolved accessions missing MD5 via SDL.
    // Large SRA files use S3 multipart uploads whose ETags are not MD5s.
    // We keep the fast S3 download URL but get the authoritative MD5 from SDL.
    let md5_needed: Vec<String> = resolved
        .iter()
        .filter(|r| r.sra_file.md5.is_none())
        .map(|r| r.accession.clone())
        .collect();
    if !md5_needed.is_empty() {
        tracing::info!(
            "fetching MD5 from SDL for {} accession(s) (S3 multipart ETag)",
            md5_needed.len()
        );
        let sdl_results = client.resolve_many(&md5_needed, format).await?;
        let md5_map: std::collections::HashMap<String, String> = sdl_results
            .into_iter()
            .filter_map(|r| r.ok())
            .filter_map(|r| {
                let md5 = r.sra_file.md5.clone()?;
                Some((r.accession.clone(), md5))
            })
            .collect();
        for r in &mut resolved {
            if r.sra_file.md5.is_none()
                && let Some(md5) = md5_map.get(&r.accession)
            {
                tracing::debug!("{}: supplemented MD5 from SDL: {md5}", r.accession);
                r.sra_file.md5 = Some(md5.clone());
            }
        }
    }

    // Phase 3: apply RunInfo (already fetched in parallel with Phase 1).
    if let Some(run_info_map) = run_info_early {
        for r in &mut resolved {
            if r.run_info.is_none() {
                r.run_info = run_info_map.get(&r.accession).cloned();
            }
        }
    }

    Ok(resolved)
}

/// Check whether confirmation is needed before downloading.
///
/// Project downloads (SRP/PRJNA/etc.) always require `--yes` confirmation.
/// Non-project downloads require `--yes` only when the total exceeds 500 GiB.
fn check_download_confirmation(
    resolved: &[ResolvedAccession],
    yes: bool,
    has_projects: bool,
) -> Result<()> {
    let total_size: u64 = resolved.iter().map(|r| r.sra_file.size).sum();
    let entries: Vec<InfoEntry> = resolved.iter().map(InfoEntry::Ok).collect();

    if has_projects && !yes {
        eprintln!();
        print_info_table(&entries);
        eprintln!();
        anyhow::bail!(
            "project downloads require confirmation -- rerun with --yes / -y to proceed ({})",
            format_size(total_size),
        );
    }

    if total_size > LARGE_DOWNLOAD_THRESHOLD && !yes {
        eprintln!();
        print_info_table(&entries);
        eprintln!();
        anyhow::bail!(
            "total download size {} exceeds 100 GiB -- rerun with --yes / -y to confirm",
            format_size(total_size),
        );
    }

    // For confirmed project downloads, still show the table for visibility.
    if has_projects {
        eprintln!();
        print_info_table(&entries);
        eprintln!();
    }

    Ok(())
}

/// Check that the target directory has enough free disk space for the download.
///
/// Uses `statvfs` to query available space. Falls back silently if the check
/// fails (e.g. the directory doesn't exist yet or the filesystem doesn't
/// support `statvfs`).
fn check_disk_space(resolved: &[ResolvedAccession], output_dir: &Path) -> Result<()> {
    let total_size: u64 = resolved.iter().map(|r| r.sra_file.size).sum();

    // Find the nearest existing ancestor to stat (output_dir may not exist yet).
    let stat_path = {
        let mut p = output_dir.to_path_buf();
        while !p.exists() {
            if !p.pop() {
                // Can't find any existing ancestor — skip the check.
                return Ok(());
            }
        }
        p
    };

    let available = available_space(&stat_path);
    let Some(available) = available else {
        return Ok(());
    };

    if total_size > available {
        anyhow::bail!(
            "not enough disk space: download requires {} but only {} available in {}",
            format_size(total_size),
            format_size(available),
            output_dir.display(),
        );
    }

    Ok(())
}

/// Query available disk space via `statvfs`.
#[cfg(unix)]
fn available_space(path: &Path) -> Option<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_path = CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    let ret = unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) };
    if ret != 0 {
        return None;
    }
    #[allow(clippy::unnecessary_cast)]
    Some(stat.f_bavail as u64 * stat.f_frsize as u64)
}

#[cfg(not(unix))]
fn available_space(_path: &Path) -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use sracha_core::sdl::{ResolvedFile, ResolvedMirror};
    use std::io::Write;

    fn make_resolved_acc(accession: &str, size: u64) -> ResolvedAccession {
        ResolvedAccession {
            accession: accession.into(),
            sra_file: ResolvedFile {
                mirrors: vec![ResolvedMirror {
                    url: "https://example.com/f".into(),
                    service: "s3".into(),
                }],
                size,
                md5: None,
                is_lite: false,
            },
            vdbcache_file: None,
            run_info: None,
        }
    }

    // -----------------------------------------------------------------------
    // collect_accessions
    // -----------------------------------------------------------------------

    #[test]
    fn collect_from_positional_only() {
        let args = vec!["SRR000001".to_string(), "SRR000002".to_string()];
        let result = collect_accessions(&args, None).unwrap();
        assert_eq!(result, vec!["SRR000001", "SRR000002"]);
    }

    #[test]
    fn collect_from_file() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("accessions.txt");
        let mut f = std::fs::File::create(&file_path).unwrap();
        writeln!(f, "SRR000001").unwrap();
        writeln!(f, "  SRR000002  ").unwrap();
        writeln!(f, "# comment").unwrap();
        writeln!(f).unwrap();
        writeln!(f, "SRR000003").unwrap();

        let result = collect_accessions(&[], Some(&file_path)).unwrap();
        assert_eq!(result, vec!["SRR000001", "SRR000002", "SRR000003"]);
    }

    #[test]
    fn collect_merges_positional_and_file() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("list.txt");
        std::fs::write(&file_path, "SRR000002\n").unwrap();

        let args = vec!["SRR000001".to_string()];
        let result = collect_accessions(&args, Some(&file_path)).unwrap();
        assert_eq!(result, vec!["SRR000001", "SRR000002"]);
    }

    #[test]
    fn collect_empty_errors() {
        let result = collect_accessions(&[], None);
        assert!(result.is_err());
    }

    #[test]
    fn collect_file_all_comments_errors() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("empty.txt");
        std::fs::write(&file_path, "# comment\n\n").unwrap();

        let result = collect_accessions(&[], Some(&file_path));
        assert!(result.is_err());
    }

    #[test]
    fn collect_nonexistent_file_errors() {
        let result = collect_accessions(&[], Some(Path::new("/nonexistent/list.txt")));
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // check_download_confirmation
    // -----------------------------------------------------------------------

    #[test]
    fn confirmation_ok_with_yes_flag() {
        let resolved = vec![make_resolved_acc("SRR1", 200 * 1024 * 1024 * 1024)];
        assert!(check_download_confirmation(&resolved, true, true).is_ok());
    }

    #[test]
    fn confirmation_required_for_projects() {
        let resolved = vec![make_resolved_acc("SRR1", 1000)];
        assert!(check_download_confirmation(&resolved, false, true).is_err());
    }

    #[test]
    fn confirmation_required_for_large_downloads() {
        let resolved = vec![make_resolved_acc("SRR1", 200 * 1024 * 1024 * 1024)];
        assert!(check_download_confirmation(&resolved, false, false).is_err());
    }

    #[test]
    fn confirmation_ok_for_small_non_project() {
        let resolved = vec![make_resolved_acc("SRR1", 1000)];
        assert!(check_download_confirmation(&resolved, false, false).is_ok());
    }

    // -----------------------------------------------------------------------
    // check_disk_space
    // -----------------------------------------------------------------------

    #[test]
    fn disk_space_ok_when_sufficient() {
        let resolved = vec![make_resolved_acc("SRR1", 1024)];
        let dir = tempfile::tempdir().unwrap();
        // Real directory — should have some space available.
        assert!(check_disk_space(&resolved, dir.path()).is_ok());
    }

    #[test]
    fn disk_space_walks_to_existing_ancestor() {
        let resolved = vec![make_resolved_acc("SRR1", 1024)];
        let dir = tempfile::tempdir().unwrap();
        let deep_path = dir.path().join("a/b/c/d/e");
        // The directory doesn't exist, but check_disk_space should walk up.
        assert!(check_disk_space(&resolved, &deep_path).is_ok());
    }

    // -----------------------------------------------------------------------
    // available_space
    // -----------------------------------------------------------------------

    #[test]
    fn available_space_returns_some_for_real_path() {
        let space = available_space(Path::new("/tmp"));
        assert!(space.is_some());
        assert!(space.unwrap() > 0);
    }

    #[test]
    fn available_space_returns_none_for_nonexistent() {
        let space = available_space(Path::new("/nonexistent/path/that/does/not/exist"));
        assert!(space.is_none());
    }
}
