//! `sracha vdb` — archive inspection, local or remote.
//!
//! Every subcommand accepts a local `.sra` path, an `https://` URL, or a
//! bare run accession. For the latter two the archive is read in place over
//! HTTP range requests: the KAR table of contents is a file prefix and the
//! column index files are kilobytes, so answering "what shape is this run?"
//! costs a few round trips instead of a multi-gigabyte download.

use std::fs::File;
use std::io::{BufReader, Read, Seek, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use sracha_core::remote::{HttpRangeReader, TransferStats};
use sracha_core::vdb::dump::{self, DumpSpec};
use sracha_core::vdb::inspect::{self, ColumnStats, InfoReport, VdbKind};
use sracha_core::vdb::kar::KarArchive;
use sracha_core::vdb::kdb::ColumnData;
use sracha_core::vdb::metadata::{self, SoftwareEvent};
use sracha_core::vdb::row_range::RowRanges;

use crate::cli::{DumpFormat, VdbCmd};
use crate::style;

/// A resolved `vdb` subcommand target.
struct Target {
    /// What the user typed, echoed back as the `acc` field of `info`.
    input: String,
    location: Location,
}

enum Location {
    Local(PathBuf),
    Remote(String),
}

pub async fn run(cmd: VdbCmd) -> Result<()> {
    let target = resolve_target(source_of(&cmd)).await?;
    // The inspection path is blocking (mmap, or a blocking range reader),
    // so keep it off the runtime's worker threads.
    tokio::task::spawn_blocking(move || execute(target, cmd))
        .await
        .context("vdb inspection task panicked")?
}

/// The source argument, which every subcommand carries.
fn source_of(cmd: &VdbCmd) -> &str {
    match cmd {
        VdbCmd::Info { source, .. }
        | VdbCmd::Tables { source }
        | VdbCmd::Columns { source, .. }
        | VdbCmd::Meta { source, .. }
        | VdbCmd::Schema { source }
        | VdbCmd::IdRange { source, .. }
        | VdbCmd::Dump { source, .. } => source,
    }
}

/// Decide whether the input names a local file, a URL, or an accession to
/// resolve. A local path wins over accession lookup so a file literally
/// named `SRR000001.sra` is never silently fetched from the network.
async fn resolve_target(input: &str) -> Result<Target> {
    if input.starts_with("http://") || input.starts_with("https://") {
        return Ok(Target {
            input: input.to_string(),
            location: Location::Remote(input.to_string()),
        });
    }

    let path = Path::new(input);
    if path.exists() {
        return Ok(Target {
            input: input.to_string(),
            location: Location::Local(path.to_path_buf()),
        });
    }

    let acc = sracha_core::accession::parse(input)
        .with_context(|| format!("{input} is not an existing file, a URL, or a run accession"))?;
    let acc = acc.to_string();

    let client = sracha_core::http::default_client();
    let resolved = match sracha_core::s3::resolve_direct(&client, &acc).await {
        Ok(r) => r,
        Err(e) => {
            tracing::debug!("S3 probe for {acc} failed ({e}); falling back to SDL");
            sracha_core::sdl::SdlClient::with_client(client)
                .resolve_one(&acc, sracha_core::sdl::FormatPreference::Sra)
                .await
                .with_context(|| format!("resolving {acc}"))?
        }
    };

    let url = resolved
        .sra_file
        .mirrors
        .first()
        .map(|m| m.url.clone())
        .ok_or_else(|| anyhow::anyhow!("no download URL found for {acc}"))?;

    tracing::info!("{acc} resolved to {url}");
    Ok(Target {
        input: acc,
        location: Location::Remote(url),
    })
}

fn execute(target: Target, cmd: VdbCmd) -> Result<()> {
    match target.location {
        Location::Local(path) => {
            let f = File::open(&path).with_context(|| format!("opening {}", path.display()))?;
            let mut kar = KarArchive::open(BufReader::new(f))
                .with_context(|| format!("parsing KAR archive at {}", path.display()))?;
            let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            let where_ = path.display().to_string();
            dispatch(
                &mut kar,
                ColumnData::Local(&path),
                &target.input,
                &where_,
                size,
                cmd,
            )
        }
        Location::Remote(url) => {
            let stats = TransferStats::default();
            let reader = HttpRangeReader::with_stats(&url, stats.clone())
                .with_context(|| format!("opening {url} for range reads"))?;
            let size = reader.len();
            let mut kar = KarArchive::open(reader)
                .with_context(|| format!("parsing KAR table of contents from {url}"))?;
            let result = dispatch(&mut kar, ColumnData::Ranged, &target.input, &url, size, cmd);
            tracing::info!(
                "transferred {} bytes in {} requests ({} archive bytes)",
                stats.bytes_fetched(),
                stats.requests(),
                size,
            );
            result
        }
    }
}

fn dispatch<R: Read + Seek>(
    kar: &mut KarArchive<R>,
    data: ColumnData<'_>,
    acc: &str,
    where_: &str,
    size: u64,
    cmd: VdbCmd,
) -> Result<()> {
    match cmd {
        VdbCmd::Info { json, .. } => cmd_info(kar, data, acc, where_, size, json),
        VdbCmd::Tables { .. } => cmd_tables(kar),
        VdbCmd::Columns { table, stats, .. } => cmd_columns(kar, data, table.as_deref(), stats),
        VdbCmd::Meta {
            table,
            path,
            depth,
            db,
            ..
        } => cmd_meta(kar, table.as_deref(), path.as_deref(), depth, db),
        VdbCmd::Schema { .. } => cmd_schema(kar),
        VdbCmd::IdRange { table, column, .. } => {
            cmd_id_range(kar, data, table.as_deref(), column.as_deref())
        }
        VdbCmd::Dump {
            table,
            columns,
            exclude,
            rows,
            format,
            raw,
            ..
        } => cmd_dump(
            kar,
            data,
            where_,
            table.as_deref(),
            columns,
            exclude,
            rows.as_deref(),
            format,
            raw,
        ),
    }
}

fn cmd_info<R: Read + Seek>(
    kar: &mut KarArchive<R>,
    data: ColumnData<'_>,
    acc: &str,
    where_: &str,
    file_size: u64,
    as_json: bool,
) -> Result<()> {
    let report = inspect::gather_info(kar, data)?;

    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    if as_json {
        write_info_json(&mut out, acc, where_, file_size, &report)?;
    } else {
        write_info_text(&mut out, acc, where_, file_size, &report)?;
    }
    Ok(())
}

fn write_info_text<W: Write>(
    w: &mut W,
    acc: &str,
    where_: &str,
    file_size: u64,
    r: &InfoReport,
) -> Result<()> {
    writeln!(w, "acc    : {acc}")?;
    writeln!(w, "path   : {where_}")?;
    if file_size != 0 {
        writeln!(w, "size   : {}", thousands(file_size))?;
    }
    writeln!(w, "type   : {}", r.kind.as_str())?;
    if let Some(p) = &r.platform {
        writeln!(w, "platf  : SRA_PLATFORM_{p}")?;
    }
    for (name, count) in &r.tables {
        let label = match name.as_str() {
            "SEQUENCE" => "SEQ    ",
            "REFERENCE" => "REF    ",
            "PRIMARY_ALIGNMENT" => "PRIM   ",
            "SECONDARY_ALIGNMENT" => "SEC    ",
            "EVIDENCE_ALIGNMENT" => "EVID   ",
            "EVIDENCE_INTERVAL" => "EVINT  ",
            "CONSENSUS" => "CONS   ",
            "PASSES" => "PASS   ",
            "METRICS" => "METR   ",
            _ => "",
        };
        if !label.is_empty() && *count != 0 {
            writeln!(w, "{label}: {}", thousands(*count))?;
        }
    }
    if let Some(s) = &r.schema_name {
        writeln!(w, "SCHEMA : {s}")?;
    }
    if let Some(ts) = r.timestamp {
        writeln!(w, "TIME   : 0x{ts:016x} ({})", format_c_time(ts))?;
    }
    write_event_text(w, "FMT", r.formatter.as_ref())?;
    write_event_text(w, "LDR", r.loader.as_ref())?;
    write_event_text(w, "UPD", r.update.as_ref())?;
    Ok(())
}

fn write_event_text<W: Write>(w: &mut W, prefix: &str, ev: Option<&SoftwareEvent>) -> Result<()> {
    let Some(ev) = ev else {
        return Ok(());
    };
    if !ev.name.is_empty() {
        writeln!(w, "{prefix:<6} : {}", ev.name)?;
    }
    if !ev.vers.is_empty() {
        writeln!(w, "{prefix}VER : {}", ev.vers)?;
    }
    if !ev.tool_date.is_empty() {
        writeln!(w, "{prefix}DATE: {}", ev.tool_date)?;
    }
    if !ev.run_date.is_empty() {
        writeln!(w, "{prefix}RUN : {}", ev.run_date)?;
    }
    Ok(())
}

fn write_info_json<W: Write>(
    w: &mut W,
    acc: &str,
    where_: &str,
    file_size: u64,
    r: &InfoReport,
) -> Result<()> {
    use serde_json::{Map, Value, json};
    let mut obj = Map::new();
    obj.insert("acc".into(), json!(acc));
    obj.insert("path".into(), json!(where_));
    if file_size != 0 {
        obj.insert("size".into(), json!(file_size));
    }
    obj.insert("type".into(), json!(r.kind.as_str()));
    if let Some(p) = &r.platform {
        obj.insert("platform".into(), json!(format!("SRA_PLATFORM_{p}")));
    }
    let mut tables = Map::new();
    for (name, count) in &r.tables {
        tables.insert(name.clone(), json!(count));
    }
    obj.insert("tables".into(), Value::Object(tables));
    if let Some(s) = &r.schema_name {
        obj.insert("schema".into(), json!(s));
    }
    if let Some(ts) = r.timestamp {
        obj.insert("timestamp".into(), json!(ts));
        obj.insert("time".into(), json!(format_iso_time(ts)));
    }
    let mut events = Map::new();
    for (key, ev) in [
        ("formatter", r.formatter.as_ref()),
        ("loader", r.loader.as_ref()),
        ("update", r.update.as_ref()),
    ] {
        if let Some(ev) = ev {
            events.insert(
                key.into(),
                json!({
                    "name": ev.name,
                    "vers": ev.vers,
                    "date": ev.tool_date,
                    "run":  ev.run_date,
                }),
            );
        }
    }
    if !events.is_empty() {
        obj.insert("software".into(), Value::Object(events));
    }
    serde_json::to_writer_pretty(&mut *w, &Value::Object(obj))?;
    writeln!(w)?;
    Ok(())
}

fn cmd_tables<R: Read + Seek>(kar: &KarArchive<R>) -> Result<()> {
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    match inspect::detect_kind(kar)? {
        VdbKind::Database => {
            for t in inspect::list_tables(kar)? {
                writeln!(out, "{t}")?;
            }
        }
        VdbKind::Table => {
            eprintln!(
                "{} this archive is a flat Table; no inner tables to list",
                style::header("note:")
            );
        }
    }
    Ok(())
}

fn cmd_columns<R: Read + Seek>(
    kar: &mut KarArchive<R>,
    data: ColumnData<'_>,
    table: Option<&str>,
    stats: bool,
) -> Result<()> {
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    if stats {
        let rows = inspect::column_stats_all(kar, data, table)?;
        write_column_stats(&mut out, &rows)?;
    } else {
        let cols = inspect::list_columns(kar, table)?;
        for c in cols {
            writeln!(out, "{c}")?;
        }
    }
    Ok(())
}

fn write_column_stats<W: Write>(w: &mut W, rows: &[ColumnStats]) -> Result<()> {
    writeln!(
        w,
        "{:<20} {:>12} {:>7} {:>8} {:>3} {:>12} {:>4} {:>4} {:>10} {:>5} {:>5} {:>5}",
        "column",
        "rows",
        "blobs",
        "first",
        "ver",
        "data_eof",
        "page",
        "csum",
        "b0_size",
        "range",
        "rowlen",
        "adj",
    )?;
    for s in rows {
        write!(
            w,
            "{:<20} {:>12} {:>7} {:>8} {:>3} {:>12} {:>4} {:>4}",
            s.name,
            s.row_count,
            s.blob_count,
            s.first_row_id,
            s.version,
            s.data_eof,
            s.page_size,
            s.checksum_type,
        )?;
        if let Some(fb) = &s.first_blob {
            write!(
                w,
                " {:>10} {:>5} {:>5} {:>5}",
                fb.size,
                fb.id_range,
                fb.row_length
                    .map(|n| n.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                fb.adjust,
            )?;
            if fb.header_frames > 0 || fb.has_page_map || fb.big_endian {
                write!(
                    w,
                    "  [frames={} page_map={} be={}]",
                    fb.header_frames, fb.has_page_map, fb.big_endian
                )?;
            }
        }
        writeln!(w)?;
    }
    Ok(())
}

fn cmd_meta<R: Read + Seek>(
    kar: &mut KarArchive<R>,
    table: Option<&str>,
    sub_path: Option<&str>,
    depth: Option<usize>,
    db: bool,
) -> Result<()> {
    let nodes = if db {
        inspect::read_db_metadata(kar)
            .ok_or_else(|| anyhow::anyhow!("no database-level md/cur in archive"))?
    } else {
        inspect::read_table_metadata(kar, table).ok_or_else(|| {
            anyhow::anyhow!(
                "no table metadata for {} in archive",
                table.unwrap_or("SEQUENCE/first table")
            )
        })?
    };
    let rows = inspect::flatten_metadata(&nodes, sub_path.unwrap_or(""), depth);
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    if rows.is_empty() {
        let target = sub_path.unwrap_or("<root>");
        writeln!(
            out,
            "{} no metadata nodes under {target}",
            style::header("note:")
        )?;
        return Ok(());
    }
    for r in rows {
        write!(
            out,
            "{:<48} len={:<6} kids={}",
            r.path, r.value_len, r.child_count
        )?;
        if r.value_len > 0 {
            write!(out, "  val={:?}", r.preview)?;
        }
        for (k, v) in &r.attrs {
            write!(out, "  {k}={v:?}")?;
        }
        writeln!(out)?;
    }
    Ok(())
}

fn cmd_schema<R: Read + Seek>(kar: &mut KarArchive<R>) -> Result<()> {
    let nodes = inspect::read_table_metadata(kar, None)
        .or_else(|| inspect::read_db_metadata(kar))
        .ok_or_else(|| anyhow::anyhow!("no metadata (md/cur) found in archive"))?;
    let text = metadata::schema_text(&nodes)
        .ok_or_else(|| anyhow::anyhow!("no schema node found in metadata"))?;
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    out.write_all(text)?;
    if !text.ends_with(b"\n") {
        writeln!(out)?;
    }
    Ok(())
}

fn cmd_id_range<R: Read + Seek>(
    kar: &mut KarArchive<R>,
    data: ColumnData<'_>,
    table: Option<&str>,
    column: Option<&str>,
) -> Result<()> {
    let (first, count) = inspect::id_range(kar, data, table, column)?;
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    writeln!(
        out,
        "id-range: first-row = {first}, row-count = {}",
        thousands(count)
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn cmd_dump<R: Read + Seek>(
    kar: &mut KarArchive<R>,
    data: ColumnData<'_>,
    where_: &str,
    table: Option<&str>,
    columns: Vec<String>,
    exclude: Vec<String>,
    rows: Option<&str>,
    format: DumpFormat,
    raw: bool,
) -> Result<()> {
    let rows = match rows {
        Some(s) => RowRanges::parse(s).context("parsing --rows / -R argument")?,
        None => RowRanges::default(),
    };
    if matches!(data, ColumnData::Ranged) && rows.is_empty() {
        eprintln!(
            "{} dumping every row of a remote archive transfers each selected \
             column in full; pass -R to fetch a range instead",
            style::header("note:")
        );
    }
    let spec = DumpSpec {
        columns,
        exclude,
        rows,
        format: format.into(),
        raw,
    };
    let mut runner = dump::DumpRunner::new(kar, data, table, spec)
        .with_context(|| format!("preparing vdb dump for {where_}"))?;
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    runner
        .run(&mut out)
        .with_context(|| format!("dumping rows from {where_}"))?;
    Ok(())
}

fn thousands(n: u64) -> String {
    let s = n.to_string();
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, b) in bytes.iter().enumerate() {
        if i != 0 && (bytes.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(*b as char);
    }
    out
}

fn format_c_time(ts: u64) -> String {
    use chrono::TimeZone;
    match chrono::Local.timestamp_opt(ts as i64, 0) {
        chrono::LocalResult::Single(dt) => dt.format("%m/%d/%Y %H:%M").to_string(),
        _ => format!("ts={ts}"),
    }
}

fn format_iso_time(ts: u64) -> String {
    use chrono::TimeZone;
    match chrono::Local.timestamp_opt(ts as i64, 0) {
        chrono::LocalResult::Single(dt) => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
        _ => format!("ts={ts}"),
    }
}
