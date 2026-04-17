//! Full-scan read of a Vortex file; prints row count.
//!
//! Usage: `cargo run --release --example read_vortex -- <path.vortex>`
//!
//! Pair with `sracha convert --format vortex` for decode-time benchmarks
//! (Issue #9).

use std::env;
use std::process;
use std::time::Instant;

use futures::StreamExt;
use tokio::runtime::Builder as RuntimeBuilder;

use vortex::VortexSessionDefault;
use vortex::file::OpenOptionsSessionExt;
use vortex::session::VortexSession;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("usage: read_vortex <path.vortex>");
        process::exit(2);
    }
    let path = args[1].clone();

    let runtime = RuntimeBuilder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let (rows, chunks, elapsed) = runtime.block_on(async move {
        let session = VortexSession::default();
        let file = session
            .open_options()
            .open_path(&path)
            .await
            .expect("open vortex");
        let mut stream = file
            .scan()
            .expect("scan")
            .into_array_stream()
            .expect("into_array_stream");
        let t0 = Instant::now();
        let mut rows: u64 = 0;
        let mut chunks: u64 = 0;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.expect("read chunk");
            rows += chunk.len() as u64;
            chunks += 1;
        }
        (rows, chunks, t0.elapsed().as_secs_f64())
    });
    println!(
        "{}: {rows} rows in {chunks} chunks, {elapsed:.3} s",
        args[1]
    );
}
