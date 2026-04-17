//! Print the layout tree of a `.vortex` file with per-segment byte sizes.
//!
//! Usage: `cargo run --release --example vortex_inspect -- <path.vortex>`

use std::env;
use std::process;

use tokio::runtime::Builder as RuntimeBuilder;

use vortex::VortexSessionDefault;
use vortex::file::OpenOptionsSessionExt;
use vortex::session::VortexSession;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("usage: vortex_inspect <path.vortex>");
        process::exit(2);
    }
    let path = args[1].clone();

    let runtime = RuntimeBuilder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    runtime.block_on(async move {
        let session = VortexSession::default();
        let file = session
            .open_options()
            .open_path(&path)
            .await
            .expect("open vortex");
        let layout = file.footer().layout().clone();
        let source = file.segment_source();
        let tree = layout
            .display_tree_with_segments(source)
            .await
            .expect("display tree");
        let total = std::fs::metadata(&path).unwrap().len();
        println!(
            "{path}: {} bytes ({:.1} MiB)",
            total,
            total as f64 / 1048576.0
        );
        println!("{tree}");
    });
}
