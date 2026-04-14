mod cli;
mod style;

use std::path::Path;

use anyhow::{Context, Result};
use clap::Parser;
use tracing_subscriber::EnvFilter;

use cli::{Cli, Command};
use sracha_core::accession::{self, InputAccession};
use sracha_core::fastq::CompressionMode;
use sracha_core::sdl::{ResolvedAccession, SdlClient};
use sracha_core::util::format_size;

#[tokio::main]
async fn main() -> Result<()> {
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
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(filter)),
        )
        .with_writer(std::io::stderr)
        .init();

    match cli.command {
        Command::Fetch(args) => {
            let raw = collect_accessions(&args.accessions, args.accession_list.as_deref())?;
            let client = SdlClient::new();
            let run_accessions = resolve_to_runs(&raw, &client).await?;
            let resolved_all = resolve_and_check_size(&run_accessions, &client, args.yes).await?;

            tokio::fs::create_dir_all(&args.output_dir).await?;
            for resolved in &resolved_all {
                let acc = &resolved.accession;
                let mirror = resolved
                    .sra_file
                    .mirrors
                    .iter()
                    .min_by_key(|m| match m.service.as_str() {
                        "s3" => 0u8,
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

            let split_mode = match args.split {
                cli::SplitMode::Split3 => sracha_core::fastq::SplitMode::Split3,
                cli::SplitMode::SplitFiles => sracha_core::fastq::SplitMode::SplitFiles,
                cli::SplitMode::SplitSpot => sracha_core::fastq::SplitMode::SplitSpot,
                cli::SplitMode::Interleaved => sracha_core::fastq::SplitMode::Interleaved,
            };

            let compression = if args.zstd {
                CompressionMode::Zstd {
                    level: args.zstd_level,
                    threads: args.threads as u32,
                }
            } else if args.no_gzip {
                CompressionMode::None
            } else {
                CompressionMode::Gzip {
                    level: args.gzip_level,
                }
            };

            for input in &args.inputs {
                let sra_path = std::path::Path::new(input);
                if !sra_path.exists() {
                    eprintln!(
                        "{} file not found: {}",
                        style::error_label("error:"),
                        style::path(input)
                    );
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
                    progress: !args.no_progress,
                    run_info: None,
                    fasta: args.fasta,
                };

                let stats = sracha_core::pipeline::run_fastq(sra_path, None, &pipeline_config)?;

                eprintln!(
                    "{}: {} spots, {} reads written",
                    style::header(&stats.accession),
                    style::count(stats.spots_read),
                    style::count(stats.reads_written),
                );
                for path in &stats.output_files {
                    eprintln!("  -> {}", style::path(path.display()));
                }
            }

            Ok(())
        }
        Command::Get(args) => {
            let raw = collect_accessions(&args.accessions, args.accession_list.as_deref())?;
            let sdl_client = SdlClient::new();
            let run_accessions = resolve_to_runs(&raw, &sdl_client).await?;
            let resolved_all =
                resolve_and_check_size(&run_accessions, &sdl_client, args.yes).await?;

            let format_label = if args.fasta { "FASTA" } else { "FASTQ" };
            tracing::info!(
                "get {} run accession(s) -> {format_label}",
                resolved_all.len()
            );

            let split_mode = match args.split {
                cli::SplitMode::Split3 => sracha_core::fastq::SplitMode::Split3,
                cli::SplitMode::SplitFiles => sracha_core::fastq::SplitMode::SplitFiles,
                cli::SplitMode::SplitSpot => sracha_core::fastq::SplitMode::SplitSpot,
                cli::SplitMode::Interleaved => sracha_core::fastq::SplitMode::Interleaved,
            };

            let compression = if args.zstd {
                CompressionMode::Zstd {
                    level: args.zstd_level,
                    threads: args.threads as u32,
                }
            } else if args.no_gzip {
                CompressionMode::None
            } else {
                CompressionMode::Gzip {
                    level: args.gzip_level,
                }
            };

            for resolved in &resolved_all {
                let pipeline_config = sracha_core::pipeline::PipelineConfig {
                    output_dir: args.output_dir.clone(),
                    split_mode,
                    compression,
                    threads: args.threads,
                    connections: args.connections,
                    skip_technical: !args.include_technical,
                    min_read_len: args.min_read_len,
                    force: args.force,
                    progress: !args.no_progress,
                    run_info: resolved.run_info.clone(),
                    fasta: args.fasta,
                };

                let stats = sracha_core::pipeline::run_get(resolved, &pipeline_config).await?;

                let pct = if stats.total_sra_size > 0 {
                    stats.bytes_downloaded as f64 / stats.total_sra_size as f64 * 100.0
                } else {
                    100.0
                };
                eprintln!(
                    "{}: {} spots, {} reads written, {} downloaded ({} of {})",
                    style::header(&stats.accession),
                    style::count(stats.spots_read),
                    style::count(stats.reads_written),
                    style::value(format_size(stats.bytes_downloaded)),
                    style::percentage(format!("{pct:.1}%")),
                    style::value(format_size(stats.total_sra_size)),
                );
                for path in &stats.output_files {
                    eprintln!("  -> {}", style::path(path.display()));
                }
            }

            Ok(())
        }
        Command::Info(args) => {
            let raw = collect_accessions(&args.accessions, args.accession_list.as_deref())?;
            let client = SdlClient::new();
            let run_accessions = resolve_to_runs(&raw, &client).await?;

            let mut resolved_all = Vec::new();
            for result in client.resolve_many(&run_accessions).await? {
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
            let mut all_valid = true;

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

                if result.valid {
                    eprintln!(
                        "{}: {} -- {} spots, {} blobs, columns: [{}]",
                        style::header(&result.label),
                        style::value("ok"),
                        style::count(result.spots_validated),
                        style::count(result.blobs_validated),
                        result.columns_found.join(", "),
                    );
                } else {
                    all_valid = false;
                    eprintln!(
                        "{}: {} -- {} error(s)",
                        style::header(&result.label),
                        style::error_label("FAILED"),
                        style::count(result.errors.len()),
                    );
                    for err in &result.errors {
                        eprintln!("  {err}");
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
async fn resolve_to_runs(inputs: &[String], client: &SdlClient) -> Result<Vec<String>> {
    let mut run_accessions = Vec::new();

    for input in inputs {
        let parsed = accession::parse_input(input)?;
        match parsed {
            InputAccession::Run(acc) => {
                run_accessions.push(acc.to_string());
            }
            InputAccession::Project(proj) => {
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

    Ok(run_accessions)
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
    // Header
    println!(
        "  {:<14} {:>10}  {:>6}  {}",
        style::label("Accession"),
        style::label("Size"),
        style::label("Layout"),
        style::label("Lite"),
    );
    println!("  {}", "-".repeat(50));

    let mut total_size: u64 = 0;

    for r in resolved {
        let layout = r
            .run_info
            .as_ref()
            .map(|ri| if ri.nreads == 2 { "PAIRED" } else { "SINGLE" })
            .unwrap_or("?");
        let lite = if r.sra_file.is_lite { "yes" } else { "no" };
        total_size += r.sra_file.size;

        println!(
            "  {:<14} {:>10}  {:>6}  {}",
            r.accession,
            format_size(r.sra_file.size),
            layout,
            lite,
        );
    }

    println!("  {}", "-".repeat(50));
    println!(
        "  {} {} across {} run(s)",
        style::label("Total:"),
        style::value(format_size(total_size)),
        style::count(resolved.len()),
    );
}

/// Size threshold (in bytes) above which downloads require `--yes` confirmation.
const LARGE_DOWNLOAD_THRESHOLD: u64 = 500 * 1024 * 1024 * 1024; // 500 GiB

/// Resolve all accessions and check total download size.
///
/// If the total exceeds [`LARGE_DOWNLOAD_THRESHOLD`] and `yes` is `false`,
/// prints the summary and exits with an error asking the user to confirm.
async fn resolve_and_check_size(
    run_accessions: &[String],
    client: &SdlClient,
    yes: bool,
) -> Result<Vec<ResolvedAccession>> {
    let resolved: Vec<ResolvedAccession> = client
        .resolve_many(run_accessions)
        .await?
        .into_iter()
        .collect::<std::result::Result<Vec<_>, _>>()?;

    let total_size: u64 = resolved.iter().map(|r| r.sra_file.size).sum();

    if total_size > LARGE_DOWNLOAD_THRESHOLD && !yes {
        eprintln!();
        print_info_table(&resolved);
        eprintln!();
        anyhow::bail!(
            "total download size {} exceeds 500 GiB -- rerun with --yes / -y to confirm",
            format_size(total_size),
        );
    }

    Ok(resolved)
}
