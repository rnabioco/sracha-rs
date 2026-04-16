mod cli;
mod style;

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
        (_, 0) => "info",
        (_, 1) => "debug",
        (_, 2) => "trace",
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
            let client = SdlClient::new();
            let (run_accessions, has_projects) = resolve_to_runs(&raw, &client).await?;
            eprintln!(
                "Resolving {} accession(s)...",
                style::count(run_accessions.len()),
            );
            let resolved_all = resolve_accessions(
                &run_accessions,
                &client,
                args.prefer_sdl,
                false,
                args.format.into(),
            )
            .await?;
            check_download_confirmation(&resolved_all, args.yes, has_projects)?;
            check_disk_space(&resolved_all, &args.output_dir)?;

            tokio::fs::create_dir_all(&args.output_dir).await?;
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
                    validate: args.validate,
                    progress: !args.no_progress,
                    resume: !args.no_resume,
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
                tracing::info!("{acc}: saved to {}", output_path.display());
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
                        eprintln!("  -> {}", style::path(path.display()));
                    }
                }
            }

            Ok(())
        }
        Command::Get(args) => {
            let split_mode =
                cli::resolve_split_mode(args.split, args.stdout).map_err(|e| anyhow::anyhow!(e))?;

            let raw = collect_accessions(&args.accessions, args.accession_list.as_deref())?;
            let sdl_client = SdlClient::new();
            let (run_accessions, has_projects) = resolve_to_runs(&raw, &sdl_client).await?;
            eprintln!(
                "Resolving {} accession(s)...",
                style::count(run_accessions.len()),
            );
            let resolved_all = resolve_accessions(
                &run_accessions,
                &sdl_client,
                args.prefer_sdl,
                !args.no_runinfo,
                args.format.into(),
            )
            .await?;
            check_download_confirmation(&resolved_all, args.yes, has_projects)?;
            check_disk_space(&resolved_all, &args.output_dir)?;

            // Check platform — reject legacy platforms with complex read structures.
            for resolved in &resolved_all {
                if let Some(ref ri) = resolved.run_info
                    && let Some(ref platform) = ri.platform
                    && sracha_core::pipeline::is_unsupported_platform(platform)
                {
                    anyhow::bail!(
                        "{}: unsupported platform '{}' — sracha does not support legacy sequencing platforms",
                        resolved.accession,
                        platform
                    );
                }
            }

            let format_label = if args.fasta { "FASTA" } else { "FASTQ" };
            tracing::info!(
                "get {} run accession(s) -> {format_label}",
                resolved_all.len()
            );

            let compression = cli::resolve_compression(
                args.stdout,
                args.zstd,
                args.zstd_level,
                args.threads,
                args.no_gzip,
                args.gzip_level,
            );

            let progress = !args.no_progress && !args.stdout;

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

            // Process accessions with one-ahead download prefetch: while
            // decoding accession N, start downloading accession N+1 so that
            // network and CPU overlap.
            let mut pending_download: Option<
                tokio::task::JoinHandle<
                    sracha_core::error::Result<sracha_core::pipeline::DownloadedSra>,
                >,
            > = None;

            let make_config =
                |resolved: &ResolvedAccession| sracha_core::pipeline::PipelineConfig {
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
                };

            for (i, resolved) in resolved_all.iter().enumerate() {
                if cancelled.load(Ordering::Relaxed) {
                    break;
                }

                let pipeline_config = make_config(resolved);

                // Await this accession's download (prefetched or fresh).
                let downloaded = if let Some(handle) = pending_download.take() {
                    match handle.await {
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
                    }
                } else {
                    match sracha_core::pipeline::download_sra(resolved, &pipeline_config).await {
                        Ok(d) => d,
                        Err(sracha_core::error::Error::Cancelled { .. }) => {
                            interrupted_accession = Some(resolved.accession.clone());
                            break;
                        }
                        Err(e) => return Err(e.into()),
                    }
                };

                // Start prefetching the next accession's download.
                if i + 1 < resolved_all.len() && !cancelled.load(Ordering::Relaxed) {
                    let next_resolved = resolved_all[i + 1].clone();
                    let next_config = make_config(&next_resolved);
                    pending_download = Some(tokio::spawn(async move {
                        sracha_core::pipeline::download_sra(&next_resolved, &next_config).await
                    }));
                }

                // Decode (CPU-bound) while the next download runs in the background.
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
                                "{}: {} spots, {} reads written, {} of {} transferred (resumed)",
                                style::header(&stats.accession),
                                style::count(stats.spots_read),
                                style::count(stats.reads_written),
                                style::value(format_size(stats.bytes_transferred)),
                                style::value(format_size(stats.total_sra_size)),
                            );
                        } else {
                            eprintln!(
                                "{}: {} spots, {} reads written, {} downloaded",
                                style::header(&stats.accession),
                                style::count(stats.spots_read),
                                style::count(stats.reads_written),
                                style::value(format_size(stats.total_sra_size)),
                            );
                        }
                        if !args.stdout {
                            for path in &stats.output_files {
                                eprintln!("  -> {}", style::path(path.display()));
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

            // Let any in-flight prefetch download detect the cancellation flag
            // and clean up its temp file gracefully (~100ms).
            if let Some(handle) = pending_download.take() {
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
            let client = SdlClient::new();
            let (run_accessions, _has_projects) = resolve_to_runs(&raw, &client).await?;

            let mut resolved_all = Vec::new();
            for result in client
                .resolve_many(&run_accessions, Default::default())
                .await?
            {
                match result {
                    Ok(resolved) => resolved_all.push(resolved),
                    Err(e) => eprintln!("{} {e}", style::error_label("error:")),
                }
            }

            if resolved_all.len() > 1 {
                // Project/multi-accession: print summary table then total.
                print_info_table(&resolved_all);
            } else {
                // Single accession: detailed view.
                for resolved in &resolved_all {
                    print_resolved(resolved);
                }
            }
            Ok(())
        }
        Command::Validate(args) => {
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
                errors: Vec<String>,
            }

            let mut rows: Vec<ValidateRow> = Vec::new();

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

                let result =
                    sracha_core::pipeline::run_validate(sra_path, args.threads, !args.no_progress);

                if !result.valid {
                    all_valid = false;
                }

                rows.push(ValidateRow {
                    label: result.label,
                    valid: result.valid,
                    spots: result.spots_validated.to_string(),
                    blobs: result.blobs_validated.to_string(),
                    columns: result.columns_found.join(", "),
                    errors: result.errors,
                });
            }

            if !rows.is_empty() {
                let mut builder = Builder::new();
                builder.push_record(["File", "Status", "Spots", "Blobs", "Columns"]);

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
            }

            if !all_valid {
                std::process::exit(1);
            }

            Ok(())
        }
    }
}

/// Collect accessions from positional arguments and an optional file.
///
/// Lines in the accession list file are trimmed; blank lines and lines
/// starting with `#` are skipped.
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
                eprintln!(
                    "{}: resolving project to run accessions...",
                    style::header(&proj),
                );
                let runs = client.resolve_project(&proj).await?;
                eprintln!(
                    "{}: found {} run(s)",
                    style::header(&proj),
                    style::count(runs.len()),
                );
                run_accessions.extend(runs);
            }
        }
    }

    Ok((run_accessions, has_projects))
}

fn print_resolved(resolved: &ResolvedAccession) {
    let f = &resolved.sra_file;

    println!("{}", style::header(&resolved.accession));
    println!(
        "  {}    {}",
        style::label("Size:"),
        style::value(format_size(f.size))
    );
    println!(
        "  {}     {}",
        style::label("MD5:"),
        style::value(f.md5.as_deref().unwrap_or("not provided"))
    );
    println!(
        "  {}    {}",
        style::label("Lite:"),
        style::value(if f.is_lite { "yes" } else { "no" })
    );
    println!(
        "  {} {}",
        style::label("Mirrors:"),
        style::count(f.mirrors.len())
    );
    for m in &f.mirrors {
        println!("    [{}] {}", style::value(&m.service), style::path(&m.url));
    }

    if let Some(ref vdb) = resolved.vdbcache_file {
        println!(
            "  {} yes ({}, {} mirrors)",
            style::label("VDBcache:"),
            style::value(format_size(vdb.size)),
            style::count(vdb.mirrors.len())
        );
    }

    if let Some(ref ri) = resolved.run_info {
        let layout = if ri.nreads == 2 { "PAIRED" } else { "SINGLE" };
        println!(
            "  {}  {} ({}, read lengths: {:?})",
            style::label("Layout:"),
            style::value(layout),
            style::value(format!("{}bp spot", ri.spot_len)),
            ri.avg_read_len
        );
    }
}

/// Print a compact table for multiple resolved accessions (project view).
fn print_info_table(resolved: &[ResolvedAccession]) {
    use tabled::builder::Builder;
    use tabled::settings::object::{Columns, Rows};
    use tabled::settings::style::HorizontalLine;
    use tabled::settings::{Alignment, Color, Modify, Panel, Style};

    let mut total_size: u64 = 0;
    let mut builder = Builder::new();

    builder.push_record(["Accession", "Size", "Layout", "Lite"]);

    for r in resolved {
        let layout = r
            .run_info
            .as_ref()
            .map(|ri| if ri.nreads == 2 { "PAIRED" } else { "SINGLE" })
            .unwrap_or("?");
        let lite = if r.sra_file.is_lite { "yes" } else { "no" };
        total_size += r.sra_file.size;

        builder.push_record([
            r.accession.clone(),
            format_size(r.sra_file.size),
            layout.to_string(),
            lite.to_string(),
        ]);
    }

    let summary = format!(
        "Total: {} across {} run(s)",
        style::value(format_size(total_size)),
        style::count(resolved.len()),
    );

    let footer_line = resolved.len() + 1;
    let mut table = builder.build();
    table
        .with(Panel::footer(summary))
        .with(Style::rounded().horizontals([
            (1, HorizontalLine::full('─', '┼', '├', '┤')),
            (footer_line, HorizontalLine::full('─', '┴', '├', '┤')),
        ]))
        .with(Modify::new(Rows::first()).with(Color::new("\x1b[1m", "\x1b[22m")))
        .with(Modify::new(Columns::new(1..=1)).with(Alignment::right()));

    println!("{table}");
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

    // Phase 1: Try direct S3 for all accessions.
    tracing::info!(
        "probing direct S3 for {} accession(s)...",
        run_accessions.len()
    );
    let s3_results =
        sracha_core::s3::resolve_direct_many(client.http_client(), run_accessions).await;

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

    // Phase 3: Fetch run_info if needed (for FASTQ conversion).
    if need_run_info {
        let all_accs: Vec<String> = resolved.iter().map(|r| r.accession.clone()).collect();
        let run_info_map = client.fetch_run_info_batch(&all_accs).await;
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

    if has_projects && !yes {
        eprintln!();
        print_info_table(resolved);
        eprintln!();
        anyhow::bail!(
            "project downloads require confirmation -- rerun with --yes / -y to proceed ({})",
            format_size(total_size),
        );
    }

    if total_size > LARGE_DOWNLOAD_THRESHOLD && !yes {
        eprintln!();
        print_info_table(resolved);
        eprintln!();
        anyhow::bail!(
            "total download size {} exceeds 100 GiB -- rerun with --yes / -y to confirm",
            format_size(total_size),
        );
    }

    // For confirmed project downloads, still show the table for visibility.
    if has_projects {
        eprintln!();
        print_info_table(resolved);
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
