use std::fs::File;
use std::io::{BufReader, Write};
use std::path::Path;

use anyhow::{Context, Result};
use sracha_core::vdb::dump::{self, DumpSpec};
use sracha_core::vdb::inspect::{self, ColumnStats, InfoReport, VdbKind};
use sracha_core::vdb::kar::KarArchive;
use sracha_core::vdb::metadata::{self, SoftwareEvent};
use sracha_core::vdb::row_range::RowRanges;

use crate::cli::{DumpFormat, VdbCmd};
use crate::style;

pub fn run(cmd: VdbCmd) -> Result<()> {
    match cmd {
        VdbCmd::Info { file, json } => cmd_info(&file, json),
        VdbCmd::Tables { file } => cmd_tables(&file),
        VdbCmd::Columns { file, table, stats } => cmd_columns(&file, table.as_deref(), stats),
        VdbCmd::Meta {
            file,
            table,
            path,
            depth,
            db,
        } => cmd_meta(&file, table.as_deref(), path.as_deref(), depth, db),
        VdbCmd::Schema { file } => cmd_schema(&file),
        VdbCmd::IdRange {
            file,
            table,
            column,
        } => cmd_id_range(&file, table.as_deref(), column.as_deref()),
        VdbCmd::Dump {
            file,
            table,
            columns,
            exclude,
            rows,
            format,
        } => cmd_dump(
            &file,
            table.as_deref(),
            columns,
            exclude,
            rows.as_deref(),
            format,
        ),
    }
}

fn open_kar(path: &Path) -> Result<KarArchive<BufReader<File>>> {
    let f = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    KarArchive::open(BufReader::new(f))
        .with_context(|| format!("parsing KAR archive at {}", path.display()))
}

fn cmd_info(path: &Path, as_json: bool) -> Result<()> {
    let mut kar = open_kar(path)?;
    let report = inspect::gather_info(&mut kar, path)?;
    let file_size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    let acc = path.display().to_string();

    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    if as_json {
        write_info_json(&mut out, &acc, path, file_size, &report)?;
    } else {
        write_info_text(&mut out, &acc, path, file_size, &report)?;
    }
    Ok(())
}

fn write_info_text<W: Write>(
    w: &mut W,
    acc: &str,
    path: &Path,
    file_size: u64,
    r: &InfoReport,
) -> Result<()> {
    writeln!(w, "acc    : {acc}")?;
    writeln!(w, "path   : {}", path.display())?;
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
    path: &Path,
    file_size: u64,
    r: &InfoReport,
) -> Result<()> {
    use serde_json::{Map, Value, json};
    let mut obj = Map::new();
    obj.insert("acc".into(), json!(acc));
    obj.insert("path".into(), json!(path.display().to_string()));
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

fn cmd_tables(path: &Path) -> Result<()> {
    let kar = open_kar(path)?;
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    match inspect::detect_kind(&kar)? {
        VdbKind::Database => {
            for t in inspect::list_tables(&kar)? {
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

fn cmd_columns(path: &Path, table: Option<&str>, stats: bool) -> Result<()> {
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    if stats {
        let mut kar = open_kar(path)?;
        let rows = inspect::column_stats_all(&mut kar, path, table)?;
        write_column_stats(&mut out, &rows)?;
    } else {
        let kar = open_kar(path)?;
        let cols = inspect::list_columns(&kar, table)?;
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

fn cmd_meta(
    path: &Path,
    table: Option<&str>,
    sub_path: Option<&str>,
    depth: Option<usize>,
    db: bool,
) -> Result<()> {
    let mut kar = open_kar(path)?;
    let nodes = if db {
        inspect::read_db_metadata(&mut kar)
            .ok_or_else(|| anyhow::anyhow!("no database-level md/cur in archive"))?
    } else {
        inspect::read_table_metadata(&mut kar, table).ok_or_else(|| {
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

fn cmd_schema(path: &Path) -> Result<()> {
    let mut kar = open_kar(path)?;
    let nodes = inspect::read_table_metadata(&mut kar, None)
        .or_else(|| inspect::read_db_metadata(&mut kar))
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

fn cmd_id_range(path: &Path, table: Option<&str>, column: Option<&str>) -> Result<()> {
    let mut kar = open_kar(path)?;
    let (first, count) = inspect::id_range(&mut kar, path, table, column)?;
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    writeln!(
        out,
        "id-range: first-row = {first}, row-count = {}",
        thousands(count)
    )?;
    Ok(())
}

fn cmd_dump(
    path: &Path,
    table: Option<&str>,
    columns: Vec<String>,
    exclude: Vec<String>,
    rows: Option<&str>,
    format: DumpFormat,
) -> Result<()> {
    let mut kar = open_kar(path)?;
    let rows = match rows {
        Some(s) => RowRanges::parse(s).context("parsing --rows / -R argument")?,
        None => RowRanges::default(),
    };
    let spec = DumpSpec {
        columns,
        exclude,
        rows,
        format: format.into(),
    };
    let mut runner = dump::DumpRunner::new(&mut kar, path, table, spec)
        .with_context(|| format!("preparing vdb dump for {}", path.display()))?;
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    runner
        .run(&mut out)
        .with_context(|| format!("dumping rows from {}", path.display()))?;
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
