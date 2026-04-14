//! Pigz-style parallel gzip compression.
//!
//! Splits input into fixed-size blocks, compresses each block independently
//! on a rayon thread pool, and concatenates the resulting gzip members.
//! Multiple concatenated gzip members form a valid gzip stream per RFC 1952.

use std::collections::BTreeMap;
use std::io::{self, Write};
use std::sync::Arc;

use crossbeam_channel::{Receiver, Sender, bounded};

/// Default block size: 256 KiB.
pub const DEFAULT_BLOCK_SIZE: usize = 256 * 1024;

/// A parallel gzip writer that compresses data using multiple threads.
///
/// Data is buffered into blocks of `block_size` bytes. When a block is full,
/// it is submitted to the rayon thread pool for compression. Compressed blocks
/// are written to the inner writer in order.
///
/// # Ordering
///
/// Each block is tagged with a monotonic sequence number. A dedicated drain
/// loop collects compressed blocks from a crossbeam channel and writes them
/// to the inner writer strictly in sequence-number order, so the output is
/// deterministic regardless of which rayon worker finishes first.
pub struct ParGzWriter<W: Write + Send> {
    /// Underlying writer receiving the concatenated gzip stream.
    inner: Option<W>,
    /// Current uncompressed data waiting to fill a block.
    buf: Vec<u8>,
    /// Bytes per uncompressed block.
    block_size: usize,
    /// Gzip compression level (1–9).
    level: u32,
    /// Dedicated thread pool for compression (never the global rayon pool).
    pool: Arc<rayon::ThreadPool>,
    /// Monotonically increasing block counter.
    next_seq: u64,
    /// Sender half — rayon workers send `(seq, compressed_bytes)` here.
    tx: Sender<(u64, Vec<u8>)>,
    /// Receiver half — we drain this when writing output.
    rx: Receiver<(u64, Vec<u8>)>,
    /// Number of blocks that have been dispatched but not yet drained.
    pending: u64,
    /// Sequence number of the next block we expect to write out.
    drain_seq: u64,
    /// Out-of-order blocks waiting for earlier ones to arrive.
    reorder_buf: BTreeMap<u64, Vec<u8>>,
    /// Tracks whether an I/O error occurred during draining, so we can
    /// propagate it on the next `write` or `finish` call.
    drain_error: Option<io::Error>,
}

impl<W: Write + Send> ParGzWriter<W> {
    /// Create a new parallel gzip writer.
    ///
    /// - `inner`: the underlying writer (file, stdout, etc.)
    /// - `level`: gzip compression level (1–9)
    /// - `block_size`: bytes per uncompressed block (e.g. [`DEFAULT_BLOCK_SIZE`])
    /// - `pool`: dedicated rayon thread pool for compression work
    pub fn new(inner: W, level: u32, block_size: usize, pool: Arc<rayon::ThreadPool>) -> Self {
        assert!(block_size > 0, "block_size must be > 0");
        // Bound the channel to roughly 2× the pool thread count so that
        // compression can run ahead of the drain loop without unbounded
        // memory growth.
        let bound = pool.current_num_threads() * 2;
        let (tx, rx) = bounded(bound);
        Self {
            inner: Some(inner),
            buf: Vec::with_capacity(block_size),
            block_size,
            level,
            pool,
            next_seq: 0,
            tx,
            rx,
            pending: 0,
            drain_seq: 0,
            reorder_buf: BTreeMap::new(),
            drain_error: None,
        }
    }

    /// Flush any buffered data, compress the remaining block, and finalize.
    ///
    /// Returns the inner writer on success.
    pub fn finish(mut self) -> io::Result<W> {
        // Compress any leftover data in the buffer.
        if !self.buf.is_empty() {
            let block = std::mem::take(&mut self.buf);
            self.dispatch_block(block);
        }

        // Drain every outstanding block.
        self.drain_all()?;

        // Check for deferred errors.
        if let Some(e) = self.drain_error.take() {
            return Err(e);
        }

        Ok(self.inner.take().expect("inner writer already consumed"))
    }

    // -- internal helpers ---------------------------------------------------

    /// Submit `block` for compression on the dedicated pool.
    fn dispatch_block(&mut self, block: Vec<u8>) {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.pending += 1;
        let level = self.level;
        let tx = self.tx.clone();

        self.pool.spawn(move || {
            let compressed = compress_block(&block, level);
            // If the receiver is dropped the send will fail, which is fine —
            // it means the writer was dropped without calling finish().
            let _ = tx.send((seq, compressed));
        });
    }

    /// Try to drain as many in-order completed blocks as possible without
    /// blocking. This is called from `write()` to keep memory bounded.
    fn drain_ready(&mut self) {
        while self.pending > 0 {
            match self.rx.try_recv() {
                Ok((seq, data)) => {
                    self.pending -= 1;
                    self.insert_and_flush(seq, data);
                }
                Err(_) => break,
            }
        }
    }

    /// Block until every pending compressed block has been received and
    /// written to `inner`.
    fn drain_all(&mut self) -> io::Result<()> {
        while self.pending > 0 {
            match self.rx.recv() {
                Ok((seq, data)) => {
                    self.pending -= 1;
                    self.insert_and_flush(seq, data);
                }
                Err(_) => {
                    // Channel disconnected unexpectedly.
                    return Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "compression channel closed unexpectedly",
                    ));
                }
            }
        }

        if let Some(e) = self.drain_error.take() {
            return Err(e);
        }
        Ok(())
    }

    /// Insert a compressed block into the reorder buffer and flush any
    /// consecutive blocks that are ready.
    fn insert_and_flush(&mut self, seq: u64, data: Vec<u8>) {
        if seq == self.drain_seq {
            // Fast path — this is exactly the block we need next.
            self.write_compressed(&data);
            self.drain_seq += 1;
            // Flush any buffered blocks that are now consecutive.
            self.flush_reorder_buf();
        } else {
            // Out-of-order — stash for later.
            self.reorder_buf.insert(seq, data);
        }
    }

    /// Flush consecutive blocks from the reorder buffer.
    fn flush_reorder_buf(&mut self) {
        while let Some(entry) = self.reorder_buf.first_entry() {
            if *entry.key() != self.drain_seq {
                break;
            }
            let data = entry.remove();
            self.write_compressed(&data);
            self.drain_seq += 1;
        }
    }

    /// Write a compressed block to the inner writer, capturing errors.
    fn write_compressed(&mut self, data: &[u8]) {
        if self.drain_error.is_some() {
            return; // Already have an error; skip further writes.
        }
        if let Some(ref mut w) = self.inner
            && let Err(e) = w.write_all(data)
        {
            self.drain_error = Some(e);
        }
    }
}

impl<W: Write + Send> Write for ParGzWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if let Some(e) = self.drain_error.take() {
            return Err(e);
        }

        let mut written = 0;
        while written < buf.len() {
            let remaining_cap = self.block_size - self.buf.len();
            let chunk = &buf[written..buf.len().min(written + remaining_cap)];
            self.buf.extend_from_slice(chunk);
            written += chunk.len();

            if self.buf.len() >= self.block_size {
                let block = std::mem::take(&mut self.buf);
                self.buf = Vec::with_capacity(self.block_size);
                self.dispatch_block(block);
                // Opportunistically drain completed blocks so memory stays
                // bounded even when the caller writes far more data than the
                // channel bound × block_size.
                self.drain_ready();
            }
        }

        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        // Flush means "ensure all data written so far reaches the writer."
        // Compress whatever is in the buffer, then drain everything.
        if !self.buf.is_empty() {
            let block = std::mem::take(&mut self.buf);
            self.buf = Vec::with_capacity(self.block_size);
            self.dispatch_block(block);
        }
        self.drain_all()?;
        if let Some(ref mut w) = self.inner {
            w.flush()?;
        }
        Ok(())
    }
}

// --------------------------------------------------------------------------
// Free function: compress a single block into a standalone gzip member.
// --------------------------------------------------------------------------

fn compress_block(data: &[u8], level: u32) -> Vec<u8> {
    use std::cell::RefCell;

    use libdeflater::{CompressionLvl, Compressor};

    // Reuse the Compressor and output buffer across calls on the same rayon
    // worker thread.  Compressor allocates ~300 KiB of internal hash tables;
    // reusing it avoids thousands of malloc/free cycles per file.
    thread_local! {
        static TL: RefCell<Option<(i32, Compressor, Vec<u8>)>> = const { RefCell::new(None) };
    }

    let lvl_i32 = level.clamp(1, 12) as i32;

    TL.with(|cell| {
        let mut slot = cell.borrow_mut();

        // Recreate if this is the first call or the level changed.
        if slot.as_ref().map_or(true, |(cached_lvl, _, _)| *cached_lvl != lvl_i32) {
            let lvl = CompressionLvl::new(lvl_i32).expect("valid compression level");
            *slot = Some((lvl_i32, Compressor::new(lvl), Vec::new()));
        }

        let (_, compressor, out_buf) = slot.as_mut().unwrap();

        let max_sz = compressor.gzip_compress_bound(data.len());
        out_buf.resize(max_sz, 0);
        let actual = compressor
            .gzip_compress(data, out_buf)
            .expect("gzip compression should not fail");
        out_buf[..actual].to_vec()
    })
}

// --------------------------------------------------------------------------
// Tests
// --------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::Read as _;

    use flate2::read::MultiGzDecoder;

    fn test_pool() -> Arc<rayon::ThreadPool> {
        Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(4)
                .build()
                .unwrap(),
        )
    }

    /// Helper: compress `input` via `ParGzWriter` and return the raw
    /// compressed bytes.
    fn compress_via_par(input: &[u8], level: u32, block_size: usize) -> Vec<u8> {
        let mut writer = ParGzWriter::new(Vec::new(), level, block_size, test_pool());
        writer.write_all(input).unwrap();
        writer.finish().unwrap()
    }

    /// Helper: decompress gzip (potentially multi-member) data.
    fn decompress(compressed: &[u8]) -> Vec<u8> {
        let mut decoder = MultiGzDecoder::new(compressed);
        let mut out = Vec::new();
        decoder.read_to_end(&mut out).unwrap();
        out
    }

    #[test]
    fn small_data_less_than_one_block() {
        let input = b"hello, world!";
        let compressed = compress_via_par(input, 6, DEFAULT_BLOCK_SIZE);
        let decompressed = decompress(&compressed);
        assert_eq!(decompressed, input);
    }

    #[test]
    fn exactly_one_block() {
        let input: Vec<u8> = (0u8..=255).cycle().take(DEFAULT_BLOCK_SIZE).collect();
        let compressed = compress_via_par(&input, 6, DEFAULT_BLOCK_SIZE);
        let decompressed = decompress(&compressed);
        assert_eq!(decompressed, input);
    }

    #[test]
    fn multiple_blocks() {
        // 3.5 blocks worth of data → 4 gzip members.
        let size = DEFAULT_BLOCK_SIZE * 3 + DEFAULT_BLOCK_SIZE / 2;
        let input: Vec<u8> = (0u8..=255).cycle().take(size).collect();
        let compressed = compress_via_par(&input, 6, DEFAULT_BLOCK_SIZE);
        let decompressed = decompress(&compressed);
        assert_eq!(decompressed, input);
    }

    #[test]
    fn empty_input() {
        let compressed = compress_via_par(b"", 6, DEFAULT_BLOCK_SIZE);
        // No data → no gzip members → empty output.
        assert!(compressed.is_empty());
    }

    #[test]
    fn different_compression_levels() {
        let input: Vec<u8> = (0u8..=255).cycle().take(DEFAULT_BLOCK_SIZE * 2).collect();
        for level in [1, 6, 9] {
            let compressed = compress_via_par(&input, level, DEFAULT_BLOCK_SIZE);
            let decompressed = decompress(&compressed);
            assert_eq!(decompressed, input, "failed at level {level}");
        }
    }

    #[test]
    fn finish_returns_inner_writer() {
        let writer = ParGzWriter::new(Vec::<u8>::new(), 6, DEFAULT_BLOCK_SIZE, test_pool());
        let inner = writer.finish().unwrap();
        // The inner Vec should be accessible and empty (no data written).
        assert!(inner.is_empty());
    }

    #[test]
    fn small_block_size() {
        // Use a tiny block size to exercise the reordering logic with many
        // blocks.
        let block_size = 64;
        let input: Vec<u8> = (0u8..=255).cycle().take(block_size * 20).collect();
        let compressed = compress_via_par(&input, 6, block_size);
        let decompressed = decompress(&compressed);
        assert_eq!(decompressed, input);
    }

    #[test]
    fn write_one_byte_at_a_time() {
        let input = b"abcdefghijklmnopqrstuvwxyz";
        let mut writer = ParGzWriter::new(Vec::new(), 6, 8, test_pool());
        for &byte in input.iter() {
            writer.write_all(&[byte]).unwrap();
        }
        let compressed = writer.finish().unwrap();
        let decompressed = decompress(&compressed);
        assert_eq!(decompressed, input);
    }

    #[test]
    fn flush_mid_stream() {
        let mut writer = ParGzWriter::new(Vec::new(), 6, DEFAULT_BLOCK_SIZE, test_pool());
        writer.write_all(b"part one").unwrap();
        writer.flush().unwrap();
        writer.write_all(b"part two").unwrap();
        let compressed = writer.finish().unwrap();
        let decompressed = decompress(&compressed);
        assert_eq!(decompressed, b"part onepart two");
    }
}
