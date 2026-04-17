//! Full-scan read of a Parquet file; prints row count.
//!
//! Usage: `cargo run --release --example read_parquet -- <path.parquet>`
//!
//! Pair with `sracha convert --format parquet` for decode-time benchmarks
//! (Issue #9).

use std::env;
use std::fs::File;
use std::process;
use std::time::Instant;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("usage: read_parquet <path.parquet>");
        process::exit(2);
    }
    let path = &args[1];
    let file = File::open(path).expect("open parquet");
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("parquet builder");
    let reader = builder.build().expect("parquet reader");

    let t0 = Instant::now();
    let mut rows: u64 = 0;
    let mut batches: u64 = 0;
    for batch in reader {
        let batch = batch.expect("read batch");
        rows += batch.num_rows() as u64;
        batches += 1;
    }
    let elapsed = t0.elapsed().as_secs_f64();
    println!("{path}: {rows} rows in {batches} batches, {elapsed:.3} s");
}
