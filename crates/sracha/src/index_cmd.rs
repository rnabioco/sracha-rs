//! Dispatcher for `sracha index <subcommand>`.
//!
//! Wraps `sracha-index` cache + fetch helpers with the user-facing
//! styling and progress conventions used elsewhere in the CLI.

use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result, anyhow};
use indicatif::{ProgressBar, ProgressStyle};
use sracha_core::util::format_size;
use sracha_index::cache;
use sracha_index::fetch::{self, FetchProgress};

use crate::cli::IndexCmd;
use crate::progress::Spinner;
use crate::style;

pub async fn run(cmd: IndexCmd) -> Result<()> {
    match cmd {
        IndexCmd::Update {
            index_url,
            cache_dir,
            force,
            no_progress,
        } => run_update(index_url.as_deref(), cache_dir, force, no_progress).await,
        IndexCmd::Status { cache_dir } => run_status(cache_dir),
        IndexCmd::Path { cache_dir } => run_path(cache_dir),
        IndexCmd::Clear { cache_dir, dry_run } => run_clear(cache_dir, dry_run),
    }
}

fn resolve_cache_dir(override_dir: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(dir) = override_dir {
        return Ok(dir);
    }
    cache::resolve_cache_dir().map_err(|e| anyhow!(e))
}

async fn run_update(
    index_url: Option<&str>,
    cache_dir_override: Option<PathBuf>,
    force: bool,
    no_progress: bool,
) -> Result<()> {
    let cache_dir = resolve_cache_dir(cache_dir_override)?;
    let url = cache::resolve_index_url(index_url);

    eprintln!(
        "{} catalog from {} into {}",
        style::label("Updating"),
        style::value(&url),
        style::path(cache_dir.display()),
    );

    let client = sracha_core::http::default_client();
    let pb = if no_progress || !std::io::IsTerminal::is_terminal(&std::io::stderr()) {
        None
    } else {
        let bar = ProgressBar::new(0);
        bar.set_style(
            ProgressStyle::with_template(
                "{spinner:.cyan} {msg}\n  [{bar:30.cyan/blue}] {bytes}/{total_bytes}",
            )
            .expect("valid progress template")
            .progress_chars("=> "),
        );
        bar.set_message("manifest.json");
        Some(bar)
    };

    let started = Instant::now();
    let summary = {
        let pb_ref = pb.as_ref();
        fetch::update_catalog(&client, &url, &cache_dir, force, |p: FetchProgress| {
            if let Some(bar) = pb_ref {
                let label = if p.shard.is_empty() {
                    p.file.clone()
                } else {
                    format!("{}/{}", p.shard, p.file)
                };
                bar.set_message(label);
                if let Some(total) = p.bytes_total {
                    bar.set_length(total);
                }
                bar.set_position(p.bytes_done);
            }
        })
        .await
        .context("downloading catalog")?
    };
    if let Some(bar) = pb {
        bar.finish_and_clear();
    }
    let elapsed = started.elapsed();

    eprintln!(
        "  {} {} new shard(s), {} skipped, {} in {:.1}s",
        style::label("→"),
        style::count(summary.shards_fetched.len()),
        style::count(summary.shards_skipped.len()),
        style::value(format_size(summary.bytes_fetched)),
        elapsed.as_secs_f64(),
    );
    if !summary.shards_fetched.is_empty() {
        eprintln!("  fetched: {}", summary.shards_fetched.join(", "),);
    }
    eprintln!(
        "  manifest: {}",
        style::path(summary.manifest_path.display()),
    );
    Ok(())
}

fn run_status(cache_dir_override: Option<PathBuf>) -> Result<()> {
    let cache_dir = resolve_cache_dir(cache_dir_override)?;
    if !cache_dir.exists() {
        eprintln!(
            "{} no local catalog at {}",
            style::warn_label("warning:"),
            style::path(cache_dir.display()),
        );
        eprintln!("  run `sracha index update` to download one");
        return Ok(());
    }
    let manifest = match cache::local_manifest(&cache_dir).map_err(|e| anyhow!(e))? {
        Some(m) => m,
        None => {
            eprintln!(
                "{} cache dir exists but has no manifest.json",
                style::warn_label("warning:"),
            );
            eprintln!("  run `sracha index update` to populate it");
            return Ok(());
        }
    };

    let on_disk = directory_size(&cache_dir).unwrap_or(0);
    let total_acc: u64 = manifest.shards.iter().map(|s| s.n_accessions).sum();
    let newest = manifest
        .shards
        .iter()
        .map(|s| s.built_at.as_str())
        .max()
        .unwrap_or("(none)");

    eprintln!(
        "{}: {}",
        style::label("path"),
        style::path(cache_dir.display())
    );
    eprintln!(
        "{}: {} (manifest version {})",
        style::label("shards"),
        style::count(manifest.shards.len()),
        manifest.version,
    );
    eprintln!(
        "{}: {} accessions across all shards",
        style::label("rows"),
        style::count(total_acc),
    );
    eprintln!(
        "{}: {}",
        style::label("newest shard built"),
        style::value(newest),
    );
    eprintln!(
        "{}: {}",
        style::label("on-disk size"),
        style::value(format_size(on_disk)),
    );
    if !manifest.shards.is_empty() {
        eprintln!("{}:", style::label("shards"));
        for s in &manifest.shards {
            eprintln!(
                "  {} ({} accessions, built {})",
                style::value(&s.name),
                s.n_accessions,
                s.built_at,
            );
        }
    }
    Ok(())
}

fn run_path(cache_dir_override: Option<PathBuf>) -> Result<()> {
    let cache_dir = resolve_cache_dir(cache_dir_override)?;
    println!("{}", cache_dir.display());
    Ok(())
}

fn run_clear(cache_dir_override: Option<PathBuf>, dry_run: bool) -> Result<()> {
    let cache_dir = resolve_cache_dir(cache_dir_override)?;
    if !cache_dir.exists() {
        eprintln!(
            "{} {} does not exist; nothing to clear",
            style::label("note:"),
            style::path(cache_dir.display()),
        );
        return Ok(());
    }
    let bytes = directory_size(&cache_dir).unwrap_or(0);
    if dry_run {
        eprintln!(
            "{} would delete {} ({})",
            style::label("dry-run:"),
            style::path(cache_dir.display()),
            style::value(format_size(bytes)),
        );
        return Ok(());
    }
    let sp = Spinner::start(format!(
        "Removing {} ({})",
        cache_dir.display(),
        format_size(bytes),
    ));
    std::fs::remove_dir_all(&cache_dir)
        .with_context(|| format!("removing {}", cache_dir.display()))?;
    sp.finish(format!("Removed {}", cache_dir.display()));
    Ok(())
}

fn directory_size(path: &Path) -> std::io::Result<u64> {
    let mut total = 0u64;
    for entry in walkdir(path)? {
        if let Ok(meta) = entry.metadata()
            && meta.is_file()
        {
            total += meta.len();
        }
    }
    Ok(total)
}

fn walkdir(path: &Path) -> std::io::Result<Vec<std::fs::DirEntry>> {
    let mut stack = vec![path.to_path_buf()];
    let mut out = Vec::new();
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let ty = entry.file_type()?;
            if ty.is_dir() {
                stack.push(entry.path());
            }
            out.push(entry);
        }
    }
    Ok(out)
}
