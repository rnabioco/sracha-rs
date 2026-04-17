//! Probe: can we get FSST-class compression on packed-binary data in
//! Vortex 0.68?
//!
//! Finding: **no, not without upstream changes.**
//!
//! Two walls:
//!   1. `Canonical::VarBinView(Binary)` is hardcoded to bypass every
//!      compression scheme
//!      (`vortex-compressor-0.68/src/compressor.rs:225-234`).
//!   2. `Canonical::VarBinView(Utf8)` validates UTF-8 on every view
//!      (`vortex-array-0.68/src/arrays/varbinview/array.rs:300-302`),
//!      including inlined and outlined bytes — runs in debug builds and
//!      in `try_new`.
//!
//! Running this example hits wall #2:
//!
//!     [Debug Assertion]: Invalid `VarBinViewArray` parameters:
//!       Invalid argument error: view at index 0: outlined bytes fails
//!       utf-8 validation
//!
//! Extension dtypes over Utf8 storage don't help — the compressor's
//! extension fallback (`compressor.rs:236-253`) compresses the storage
//! array, which re-canonicalizes to Utf8 and hits the same validator.
//!
//! The ascii baseline does work, and shows FSST's ceiling for DNA:
//!
//!     ascii  Utf8+FSST : rows=100000  file=5003284B  raw=15000000B  ratio=0.334
//!
//! So on 150 bp reads, FSST compresses ACGT text to ~50 bytes/read. Our
//! current `List<u8>` 2na path (packed bytes through the primitive
//! cascade) gets ~28 bytes/read on the same data — already denser than
//! FSST-on-ASCII. The marginal value of a "FSST for primitive u8" scheme
//! is therefore smaller than it first looked, and the path is blocked
//! anyway.
//!
//! Real fixes need upstream Vortex changes:
//!   - Remove the `VarBinView(Binary)` bypass (let Binary data go through
//!     the string cascade).
//!   - Or expose scheme selection at the List level so a scheme can see
//!     both elements and offsets together.
//!
//! Until then, keep quality as `Utf8` (FSST's string cascade) and packed
//! sequence as `List<u8>` (primitive cascade). See
//! `crates/sracha-core/src/vortex/builder.rs` for the live schema.

use std::path::PathBuf;

use futures::StreamExt;
use tokio::runtime::Builder as RuntimeBuilder;

use vortex::VortexSessionDefault;
use vortex::array::arrays::StructArray;
use vortex::array::arrays::struct_::StructArrayExt;
use vortex::array::builders::{ArrayBuilder, VarBinViewBuilder};
use vortex::array::{ArrayRef, IntoArray};
use vortex::compressor::BtrBlocksCompressorBuilder;
use vortex::dtype::{DType, Nullability};
use vortex::file::{OpenOptionsSessionExt, WriteOptionsSessionExt};
use vortex::layout::LayoutStrategy;
use vortex::layout::layouts::buffered::BufferedStrategy;
use vortex::layout::layouts::chunked::writer::ChunkedLayoutStrategy;
use vortex::layout::layouts::collect::CollectStrategy;
use vortex::layout::layouts::compressed::{CompressingStrategy, CompressorPlugin};
use vortex::layout::layouts::dict::writer::{DictLayoutOptions, DictStrategy};
use vortex::layout::layouts::flat::writer::FlatLayoutStrategy;
use vortex::layout::layouts::repartition::{RepartitionStrategy, RepartitionWriterOptions};
use vortex::layout::layouts::table::TableStrategy;
use vortex::layout::layouts::zoned::writer::{ZonedLayoutOptions, ZonedStrategy};
use vortex::session::VortexSession;

use std::sync::Arc;

const NUM_ROWS: usize = 100_000;
const READ_LEN_BP: usize = 150;

fn pack_2na(ascii: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(ascii.len().div_ceil(4));
    let mut acc: u8 = 0;
    let mut shift = 6;
    for &b in ascii {
        let bits = match b {
            b'A' => 0b00,
            b'C' => 0b01,
            b'G' => 0b10,
            b'T' => 0b11,
            _ => 0b00,
        };
        acc |= bits << shift;
        if shift == 0 {
            out.push(acc);
            acc = 0;
            shift = 6;
        } else {
            shift -= 2;
        }
    }
    if shift != 6 {
        out.push(acc);
    }
    out
}

fn make_random_dna(len: usize, seed: u64) -> Vec<u8> {
    // Trivial xorshift → pseudorandom ACGT.
    let mut s = seed.wrapping_mul(2862933555777941757).wrapping_add(3037000493);
    let alphabet = b"ACGT";
    (0..len)
        .map(|_| {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            alphabet[(s as usize) & 0b11]
        })
        .collect()
}

fn build_local_write_strategy() -> Arc<dyn LayoutStrategy> {
    let flat: Arc<dyn LayoutStrategy> = Arc::new(FlatLayoutStrategy::default());
    let chunked = ChunkedLayoutStrategy::new(Arc::clone(&flat));
    let buffered = BufferedStrategy::new(chunked, 2 * (1 << 20));
    let data_compressor: Arc<dyn CompressorPlugin> = Arc::new(
        BtrBlocksCompressorBuilder::default()
            .with_compact()
            .build(),
    );
    let compressing = CompressingStrategy::new(buffered, Arc::clone(&data_compressor));
    let coalescing = RepartitionStrategy::new(
        compressing,
        RepartitionWriterOptions {
            block_size_minimum: 16 * (1 << 20),
            block_len_multiple: 524_288,
            block_size_target: Some(16 * (1 << 20)),
            canonicalize: true,
        },
    );
    let stats_compressor: Arc<dyn CompressorPlugin> =
        Arc::new(BtrBlocksCompressorBuilder::default().with_compact().build());
    let compress_then_flat = CompressingStrategy::new(Arc::clone(&flat), stats_compressor);
    let dict = DictStrategy::new(
        coalescing.clone(),
        compress_then_flat.clone(),
        coalescing,
        DictLayoutOptions::default(),
    );
    let stats = ZonedStrategy::new(
        dict,
        compress_then_flat.clone(),
        ZonedLayoutOptions {
            block_size: 524_288,
            ..Default::default()
        },
    );
    let repartition = RepartitionStrategy::new(
        stats,
        RepartitionWriterOptions {
            block_size_minimum: 0,
            block_len_multiple: 524_288,
            block_size_target: None,
            canonicalize: false,
        },
    );
    let validity = CollectStrategy::new(compress_then_flat);
    let table = TableStrategy::new(Arc::new(validity), Arc::new(repartition));
    Arc::new(table)
}

fn build_column(utf8_mode: bool) -> ArrayRef {
    let dtype = DType::Utf8(Nullability::NonNullable);
    let mut builder = VarBinViewBuilder::with_capacity(dtype, NUM_ROWS);
    for i in 0..NUM_ROWS {
        let ascii = make_random_dna(READ_LEN_BP, i as u64);
        if utf8_mode {
            builder.append_value(&ascii);
        } else {
            let packed = pack_2na(&ascii);
            // ← Deliberately shove non-UTF-8 bytes into a Utf8-labeled
            //   VarBinView to test the FSST path.
            builder.append_value(&packed);
        }
    }
    let fields: Vec<(Arc<str>, ArrayRef)> = vec![(Arc::from("sequence"), builder.finish())];
    StructArray::try_from_iter(fields).unwrap().into_array()
}

async fn write_and_read(path: &std::path::Path, array: ArrayRef) -> usize {
    let session = VortexSession::default();
    let strategy = build_local_write_strategy();
    let mut file = tokio::fs::File::create(path).await.unwrap();
    session
        .write_options()
        .with_strategy(strategy)
        .write(&mut file, array.to_array_stream())
        .await
        .unwrap();
    drop(file);

    // Read back and spot-check row 0's bytes.
    let file = session.open_options().open_path(path).await.unwrap();
    let mut stream = file.scan().unwrap().into_array_stream().unwrap();
    let mut total = 0usize;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.unwrap();
        total += chunk.len();
        let s = chunk.to_canonical().unwrap().into_struct();
        let seq = s.unmasked_field_by_name("sequence").unwrap();
        let v = seq.to_canonical().unwrap().into_varbinview();
        // Touch row 0 to ensure canonicalization works (FSST decode).
        let _ = v.bytes_at(0);
    }
    total
}

fn main() {
    let tmp = tempfile::tempdir().unwrap();
    let runtime = RuntimeBuilder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let raw_utf8_bytes = NUM_ROWS * READ_LEN_BP;
    let raw_packed_bytes = NUM_ROWS * READ_LEN_BP.div_ceil(4);

    // Baseline: real ASCII DNA via Utf8 + FSST.
    let p1: PathBuf = tmp.path().join("ascii.vortex");
    let rows1 = runtime.block_on(write_and_read(&p1, build_column(true)));
    let sz1 = std::fs::metadata(&p1).unwrap().len();
    println!(
        "ascii  Utf8+FSST : rows={rows1}  file={sz1}B  raw={raw_utf8_bytes}B  ratio={:.3}",
        sz1 as f64 / raw_utf8_bytes as f64
    );

    // Test: packed 2na bytes stuffed into Utf8 + FSST.
    let p2 = tmp.path().join("packed.vortex");
    let rows2 = runtime.block_on(write_and_read(&p2, build_column(false)));
    let sz2 = std::fs::metadata(&p2).unwrap().len();
    println!(
        "packed Utf8+FSST : rows={rows2}  file={sz2}B  raw={raw_packed_bytes}B  ratio={:.3}",
        sz2 as f64 / raw_packed_bytes as f64
    );

    println!("Done. No panic from FSST-on-invalid-UTF-8 means the approach is viable.");
}
