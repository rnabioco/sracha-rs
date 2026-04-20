//! Decode benchmark for the FASTQ-generation hot path.
//!
//! Runs `run_fastq` against local SRA fixtures in `tests/fixtures/` and
//! measures wall-clock throughput. Each fixture missing from disk is
//! skipped with a stderr notice (so the bench works on fresh clones).
//!
//! Typical invocation (release build, profile symbols retained):
//!
//! ```sh
//! srun -p normal -c 16 cargo bench -p sracha-core --bench decode
//! ```
//!
//! For flamegraphs, wrap the bench run with samply:
//!
//! ```sh
//! samply record cargo bench -p sracha-core --bench decode -- --bench <case>
//! ```

use std::path::PathBuf;
use std::time::Duration;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use sracha_core::fastq::{CompressionMode, SplitMode};
use sracha_core::pipeline::{PipelineConfig, run_fastq};

/// Fixture resolution: look under `tests/fixtures/<name>.sra`. Missing
/// fixtures are *not* downloaded here — this mirrors the gated behavior
/// in `tests/pipeline.rs` (which only fetches on `--ignored` test runs).
fn fixture(name: &str) -> Option<PathBuf> {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(format!("{name}.sra"));
    p.exists().then_some(p)
}

fn config(out: &std::path::Path, threads: usize, compression: CompressionMode) -> PipelineConfig {
    PipelineConfig {
        output_dir: out.to_path_buf(),
        split_mode: SplitMode::SplitSpot,
        compression,
        threads,
        connections: 1,
        skip_technical: true,
        min_read_len: None,
        force: true,
        progress: false,
        run_info: None,
        fasta: false,
        resume: true,
        stdout: false,
        cancelled: None,
        strict: false,
        http_client: None,
        keep_sra: false,
        progress_parent: None,
    }
}

/// One bench case: `(fixture accession, compression mode, label suffix)`.
fn cases() -> Vec<(&'static str, CompressionMode, &'static str)> {
    vec![
        ("SRR28588231", CompressionMode::None, "plain"),
        ("SRR28588231", CompressionMode::Gzip { level: 6 }, "gzip6"),
        ("SRR000001", CompressionMode::None, "plain"),
        ("SRR2584863", CompressionMode::None, "plain"),
    ]
}

fn bench_decode(c: &mut Criterion) {
    let threads = std::env::var("SRACHA_BENCH_THREADS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8usize);

    let mut group = c.benchmark_group("decode");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(60));
    group.warm_up_time(Duration::from_secs(3));

    for (name, compression, label) in cases() {
        let Some(sra) = fixture(name) else {
            eprintln!("skipping {name} ({label}): fixture not present");
            continue;
        };
        let size = std::fs::metadata(&sra).map(|m| m.len()).unwrap_or(0);
        group.throughput(Throughput::Bytes(size));

        let id = BenchmarkId::from_parameter(format!("{name}-{label}"));
        group.bench_function(id, |b| {
            b.iter_batched(
                || tempfile::tempdir().expect("tempdir"),
                |tmp| {
                    let cfg = config(tmp.path(), threads, compression);
                    run_fastq(&sra, Some(name), &cfg).expect("decode");
                },
                BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

criterion_group!(benches, bench_decode);
criterion_main!(benches);
