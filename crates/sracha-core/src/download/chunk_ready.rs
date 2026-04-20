//! Per-chunk completion tracking for streaming decode.
//!
//! `ChunkReadyTracker` is a lock-free presence map shared between the
//! parallel chunked downloader and a streaming consumer (the decoder).
//! As chunks complete, the downloader calls [`ChunkReadyTracker::mark_done`];
//! the decoder polls or awaits readiness via [`ChunkReadyTracker::is_range_ready`]
//! / [`ChunkReadyTracker::await_range`].
//!
//! ## Streaming-decode safety
//!
//! When download writes parallel chunks to a file via `pwrite`, unwritten
//! pages remain sparse — and reading them through a mmap returns **zeros**,
//! not an error. A streaming decoder that reads bytes before the covering
//! chunk is marked ready will silently produce wrong output. Every read
//! the decoder issues against the not-yet-complete file MUST be preceded
//! by `is_range_ready` (debug-asserted) or gated by `await_range`.
//!
//! ## Memory ordering
//!
//! `mark_done` uses `Release`, the readers use `Acquire`. Combined with
//! `tokio::sync::Notify`, this guarantees a waiter that wakes up will see
//! the bit it was waiting on (and any earlier writes from the producer).

use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::Notify;

/// Lock-free chunk presence map + async wake.
pub struct ChunkReadyTracker {
    /// One slot per chunk index. `true` means the chunk's bytes are
    /// fully written to disk and safe to read.
    ready: Vec<AtomicBool>,
    /// Wakes any awaiter whenever a new chunk is marked done. Awaiters
    /// must re-check their condition after notification (spurious wakes
    /// are normal — `mark_done(idx)` notifies *all* waiters, not just
    /// those interested in `idx`).
    notify: Notify,
    /// Bytes per chunk. Last chunk may be shorter; only `chunk_size`
    /// matters for byte→index translation since chunks are uniform
    /// except possibly the tail.
    chunk_size: u64,
    /// Total file size in bytes.
    file_size: u64,
}

impl ChunkReadyTracker {
    /// Create a tracker for a download with `total_chunks` chunks of
    /// `chunk_size` bytes each (last chunk may be shorter to fit
    /// `file_size`). All chunks start as not-ready.
    pub fn new(total_chunks: usize, chunk_size: u64, file_size: u64) -> Self {
        assert!(chunk_size > 0, "chunk_size must be > 0");
        assert!(
            (total_chunks as u64).saturating_mul(chunk_size) >= file_size,
            "total_chunks * chunk_size ({} * {}) must cover file_size ({})",
            total_chunks,
            chunk_size,
            file_size,
        );
        let mut ready = Vec::with_capacity(total_chunks);
        for _ in 0..total_chunks {
            ready.push(AtomicBool::new(false));
        }
        Self {
            ready,
            notify: Notify::new(),
            chunk_size,
            file_size,
        }
    }

    /// Number of chunks this tracker covers.
    pub fn total_chunks(&self) -> usize {
        self.ready.len()
    }

    /// Total file size this tracker covers.
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// Mark a chunk as fully downloaded. Idempotent — double-marking is
    /// harmless. Panics if `chunk_idx >= total_chunks`.
    pub fn mark_done(&self, chunk_idx: usize) {
        self.ready[chunk_idx].store(true, Ordering::Release);
        // notify_waiters wakes ALL current waiters; each re-checks its
        // condition. This is correct (avoids missed wakeups when waiters
        // are interested in different chunks) at the cost of some spurious
        // re-checks. Cheap with our small waiter count.
        self.notify.notify_waiters();
    }

    /// Pre-mark all chunks as done (e.g., when resuming and the entire
    /// file is already on disk). Equivalent to `mark_done(i)` for every
    /// `i`, plus a single notification.
    pub fn mark_all_done(&self) {
        for slot in &self.ready {
            slot.store(true, Ordering::Release);
        }
        self.notify.notify_waiters();
    }

    /// Non-blocking: is this chunk's bytes ready to read?
    pub fn is_chunk_ready(&self, chunk_idx: usize) -> bool {
        self.ready[chunk_idx].load(Ordering::Acquire)
    }

    /// Non-blocking: are all chunks covering the byte range
    /// `[byte_start, byte_end)` ready? `byte_end` is exclusive.
    /// Returns `true` for an empty range.
    pub fn is_range_ready(&self, byte_start: u64, byte_end: u64) -> bool {
        if byte_end <= byte_start {
            return true;
        }
        for idx in self.byte_range_to_chunk_indices(byte_start, byte_end) {
            if !self.is_chunk_ready(idx) {
                return false;
            }
        }
        true
    }

    /// Async: wait until `chunk_idx` is marked ready. Returns
    /// immediately if already ready.
    pub async fn await_chunk(&self, chunk_idx: usize) {
        loop {
            // Register interest BEFORE checking the bit. If we checked
            // first and the producer set the bit + notified between the
            // check and the await, we'd miss the wakeup.
            let notified = self.notify.notified();
            if self.is_chunk_ready(chunk_idx) {
                return;
            }
            notified.await;
        }
    }

    /// Async: wait until every chunk covering `[byte_start, byte_end)`
    /// is ready. Returns immediately for an empty range.
    pub async fn await_range(&self, byte_start: u64, byte_end: u64) {
        if byte_end <= byte_start {
            return;
        }
        loop {
            let notified = self.notify.notified();
            if self.is_range_ready(byte_start, byte_end) {
                return;
            }
            notified.await;
        }
    }

    /// Async: wait until *every* chunk is ready (download fully
    /// complete). Useful as a fallback for code paths that haven't been
    /// converted to streaming yet.
    pub async fn await_all(&self) {
        self.await_range(0, self.file_size).await;
    }

    /// Sync wrapper around [`Self::await_range`] for callers that
    /// already hold a Tokio runtime handle but execute in a synchronous
    /// stack frame (e.g. inside `tokio::task::block_in_place`). Returns
    /// immediately on the fast path (range already ready); blocks the
    /// current thread otherwise.
    ///
    /// **Must** be called from within a Tokio runtime context. Panics
    /// from `Handle::current()` otherwise — surface that as a clearer
    /// error at the call site if you can't guarantee the context.
    pub fn wait_range(&self, byte_start: u64, byte_end: u64) {
        if self.is_range_ready(byte_start, byte_end) {
            return;
        }
        tokio::runtime::Handle::current().block_on(self.await_range(byte_start, byte_end));
    }

    /// Sync convenience: block until every chunk is ready.
    pub fn wait_all(&self) {
        self.wait_range(0, self.file_size);
    }

    /// Translate a byte range to the inclusive sequence of chunk
    /// indices covering it. `byte_end` is exclusive; an empty input
    /// yields an empty range.
    fn byte_range_to_chunk_indices(
        &self,
        byte_start: u64,
        byte_end: u64,
    ) -> std::ops::Range<usize> {
        if byte_end <= byte_start {
            return 0..0;
        }
        let first = (byte_start / self.chunk_size) as usize;
        // byte_end is exclusive, so the last covered chunk is
        // ((byte_end - 1) / chunk_size).
        let last = ((byte_end - 1) / self.chunk_size) as usize;
        let cap = self.ready.len();
        // Clamp in case the caller asks for bytes past file_size.
        let first = first.min(cap);
        let last_exclusive = (last + 1).min(cap);
        first..last_exclusive
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn t(chunks: usize, chunk_size: u64) -> ChunkReadyTracker {
        ChunkReadyTracker::new(chunks, chunk_size, chunks as u64 * chunk_size)
    }

    #[test]
    fn new_starts_with_no_chunks_ready() {
        let t = t(4, 1024);
        assert_eq!(t.total_chunks(), 4);
        for i in 0..4 {
            assert!(!t.is_chunk_ready(i));
        }
    }

    #[test]
    fn mark_done_makes_chunk_ready() {
        let t = t(4, 1024);
        t.mark_done(2);
        assert!(t.is_chunk_ready(2));
        for i in [0, 1, 3] {
            assert!(!t.is_chunk_ready(i));
        }
    }

    #[test]
    fn mark_done_is_idempotent() {
        let t = t(2, 1024);
        t.mark_done(0);
        t.mark_done(0);
        assert!(t.is_chunk_ready(0));
    }

    #[test]
    fn mark_all_done_marks_all() {
        let t = t(8, 1024);
        t.mark_all_done();
        for i in 0..8 {
            assert!(t.is_chunk_ready(i));
        }
    }

    #[test]
    fn empty_range_is_always_ready() {
        let t = t(4, 1024);
        assert!(t.is_range_ready(100, 100));
        assert!(t.is_range_ready(500, 200));
    }

    #[test]
    fn range_ready_requires_all_covering_chunks() {
        let t = t(4, 100);
        // Range [50, 250) covers chunks 0, 1, 2.
        assert!(!t.is_range_ready(50, 250));
        t.mark_done(0);
        assert!(!t.is_range_ready(50, 250));
        t.mark_done(1);
        assert!(!t.is_range_ready(50, 250));
        t.mark_done(2);
        assert!(t.is_range_ready(50, 250));
        // Chunk 3 (bytes 300..400) wasn't required and is still not ready.
        assert!(!t.is_chunk_ready(3));
    }

    #[test]
    fn byte_range_translation_handles_chunk_boundaries() {
        let t = t(4, 100);
        // Exact boundary [0, 100): just chunk 0.
        assert_eq!(t.byte_range_to_chunk_indices(0, 100), 0..1);
        // [100, 200): just chunk 1.
        assert_eq!(t.byte_range_to_chunk_indices(100, 200), 1..2);
        // [99, 101): chunks 0 and 1.
        assert_eq!(t.byte_range_to_chunk_indices(99, 101), 0..2);
        // [0, 400): all four chunks.
        assert_eq!(t.byte_range_to_chunk_indices(0, 400), 0..4);
        // [350, 1000): clamps to chunk 3 only (file is 400 bytes).
        assert_eq!(t.byte_range_to_chunk_indices(350, 1000), 3..4);
    }

    #[tokio::test]
    async fn await_chunk_returns_immediately_when_ready() {
        let t = t(4, 1024);
        t.mark_done(1);
        // Should not block.
        tokio::time::timeout(std::time::Duration::from_millis(50), t.await_chunk(1))
            .await
            .expect("await_chunk on already-ready chunk should not block");
    }

    #[tokio::test]
    async fn await_chunk_wakes_on_mark_done() {
        let t = Arc::new(t(4, 1024));
        let t_clone = t.clone();
        let waiter = tokio::spawn(async move { t_clone.await_chunk(2).await });
        // Give the waiter time to register on Notify.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        t.mark_done(2);
        tokio::time::timeout(std::time::Duration::from_millis(100), waiter)
            .await
            .expect("waiter should wake within 100ms")
            .expect("task should not panic");
    }

    #[tokio::test]
    async fn await_range_wakes_when_last_chunk_arrives() {
        let t = Arc::new(t(4, 100));
        // Range [50, 250) needs chunks 0, 1, 2.
        let t_clone = t.clone();
        let waiter = tokio::spawn(async move { t_clone.await_range(50, 250).await });
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        t.mark_done(2);
        // Still missing 0 and 1 — waiter must NOT be done yet.
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(20),
                &mut Box::pin(async {})
            )
            .await
            .is_ok()
        );
        t.mark_done(0);
        t.mark_done(1);
        tokio::time::timeout(std::time::Duration::from_millis(100), waiter)
            .await
            .expect("waiter should wake within 100ms after final chunk")
            .expect("task should not panic");
    }

    #[tokio::test]
    async fn multiple_waiters_all_wake() {
        let t = Arc::new(t(4, 1024));
        let mut handles = Vec::new();
        for idx in 0..4 {
            let tc = t.clone();
            handles.push(tokio::spawn(async move { tc.await_chunk(idx).await }));
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        t.mark_all_done();
        for h in handles {
            tokio::time::timeout(std::time::Duration::from_millis(100), h)
                .await
                .expect("all waiters should wake")
                .expect("no task panics");
        }
    }
}
