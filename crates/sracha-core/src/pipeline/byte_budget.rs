//! [`ByteBudget`] — byte-bounded backpressure for the decode → write pipeline.
//!
//! Decode runs blob-by-blob in parallel; each decoded blob holds its formatted
//! FASTQ output (`Vec<u8>`) in memory until the single writer thread drains it.
//! The amount queued ahead of the writer is the dominant source of resident
//! memory on large runs (issue #54: a 1024-blob × capacity-4 *count*-bounded
//! channel buffered ~19 GiB before the writer saw the first byte).
//!
//! Bounding by blob *count* is the wrong unit — formatted bytes per blob vary
//! by ~1000× across runs (short Illumina vs. long-read PacBio/ONT, full vs.
//! SRA-lite). [`ByteBudget`] bounds the queue by *bytes* instead, so the
//! pipeline depth self-tunes: many small blobs queue deeply (max throughput),
//! few huge blobs queue shallowly (memory safe), under one predictable cap.
//!
//! The producer [`acquire`](ByteBudget::acquire)s a batch's byte size before
//! handing it off and blocks while the queue is over budget; the writer
//! [`release`](ByteBudget::release)s after draining each batch.

use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// Tracks bytes currently queued from the decode producer to the writer and
/// applies backpressure when that exceeds `cap`.
pub(crate) struct ByteBudget {
    /// Bytes handed to the writer but not yet drained.
    queued: Mutex<u64>,
    /// Signalled when the writer releases bytes or the budget is closed.
    cv: Condvar,
    /// Soft ceiling on `queued`. A batch larger than `cap` is still admitted
    /// when the queue is empty, so a single oversized batch never deadlocks.
    cap: u64,
    /// Set when the writer exits (normal or error) so a parked producer wakes.
    closed: AtomicBool,
}

impl ByteBudget {
    pub(crate) fn new(cap: u64) -> Self {
        Self {
            queued: Mutex::new(0),
            cv: Condvar::new(),
            cap: cap.max(1),
            closed: AtomicBool::new(false),
        }
    }

    /// Reserve `n` bytes of queue, blocking while the queue is over `cap`.
    ///
    /// Returns `true` once the reservation is made; `false` if the writer has
    /// [`close`](Self::close)d the budget or `cancelled` fired — in which case
    /// the producer should stop without sending. Polls with a short timeout so
    /// it stays responsive to cancellation even if no `release` arrives.
    ///
    /// Admits a batch larger than `cap` when the queue is empty (the `*q == 0`
    /// arm), which is what guarantees forward progress on a single blob whose
    /// output exceeds the whole budget.
    pub(crate) fn acquire(&self, n: u64, cancelled: Option<&AtomicBool>) -> bool {
        let mut q = self.queued.lock().unwrap();
        loop {
            if self.closed.load(Ordering::Relaxed) {
                return false;
            }
            if cancelled.is_some_and(|c| c.load(Ordering::Relaxed)) {
                return false;
            }
            if *q == 0 || *q + n <= self.cap {
                *q += n;
                return true;
            }
            (q, _) = self.cv.wait_timeout(q, Duration::from_millis(100)).unwrap();
        }
    }

    /// Return `n` bytes to the budget after the writer has drained them and
    /// wake any producer parked in [`acquire`](Self::acquire).
    pub(crate) fn release(&self, n: u64) {
        let mut q = self.queued.lock().unwrap();
        *q = q.saturating_sub(n);
        drop(q);
        self.cv.notify_all();
    }

    /// Mark the budget closed (writer is gone) and wake every parked producer.
    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
        self.cv.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;

    #[test]
    fn acquire_under_cap_succeeds_immediately() {
        let b = ByteBudget::new(1000);
        assert!(b.acquire(400, None));
        assert!(b.acquire(400, None));
        assert_eq!(*b.queued.lock().unwrap(), 800);
    }

    #[test]
    fn oversized_batch_admitted_when_empty() {
        // A single batch larger than the whole cap must pass when the queue is
        // empty, or decode of a giant blob would deadlock forever.
        let b = ByteBudget::new(100);
        assert!(b.acquire(10_000, None));
        assert_eq!(*b.queued.lock().unwrap(), 10_000);
    }

    #[test]
    fn release_lets_a_blocked_producer_proceed() {
        let b = Arc::new(ByteBudget::new(1000));
        assert!(b.acquire(800, None)); // queue now 800

        let b2 = Arc::clone(&b);
        let done = Arc::new(AtomicBool::new(false));
        let done2 = Arc::clone(&done);
        // 800 + 400 > 1000 and queue is non-empty, so this blocks.
        let h = std::thread::spawn(move || {
            assert!(b2.acquire(400, None));
            done2.store(true, Ordering::SeqCst);
        });

        // Give the producer a moment to park, then confirm it's still blocked.
        std::thread::sleep(Duration::from_millis(50));
        assert!(!done.load(Ordering::SeqCst), "should still be blocked");

        b.release(800); // drops queue to 0; the 400 now fits
        h.join().unwrap();
        assert!(done.load(Ordering::SeqCst));
        assert_eq!(*b.queued.lock().unwrap(), 400);
    }

    #[test]
    fn close_unblocks_producer_with_false() {
        let b = Arc::new(ByteBudget::new(1000));
        assert!(b.acquire(900, None));

        let b2 = Arc::clone(&b);
        let h = std::thread::spawn(move || b2.acquire(900, None));
        std::thread::sleep(Duration::from_millis(50));
        b.close();
        assert!(!h.join().unwrap(), "acquire must return false after close");
    }

    #[test]
    fn cancel_flag_unblocks_producer_with_false() {
        let b = Arc::new(ByteBudget::new(1000));
        assert!(b.acquire(900, None));
        let cancel = Arc::new(AtomicBool::new(false));

        let b2 = Arc::clone(&b);
        let cancel2 = Arc::clone(&cancel);
        let h = std::thread::spawn(move || b2.acquire(900, Some(&cancel2)));
        std::thread::sleep(Duration::from_millis(50));
        cancel.store(true, Ordering::SeqCst);
        assert!(!h.join().unwrap(), "acquire must return false after cancel");
    }

    #[test]
    fn release_saturates_at_zero() {
        // Defensive: a release larger than queued must not underflow.
        let b = ByteBudget::new(1000);
        assert!(b.acquire(100, None));
        b.release(999_999);
        assert_eq!(*b.queued.lock().unwrap(), 0);
    }

    #[test]
    fn concurrent_producers_never_exceed_cap_beyond_one_batch() {
        // With cap C and uniform batch size S (S <= C), queued should never
        // exceed C while batches are outstanding (each producer acquires
        // before "sending"). One consumer drains in a loop.
        let cap = 1000u64;
        let b = Arc::new(ByteBudget::new(cap));
        let peak = Arc::new(AtomicU64::new(0));
        let batch = 200u64;

        let mut producers = Vec::new();
        for _ in 0..4 {
            let b = Arc::clone(&b);
            let peak = Arc::clone(&peak);
            producers.push(std::thread::spawn(move || {
                for _ in 0..50 {
                    assert!(b.acquire(batch, None));
                    let cur = *b.queued.lock().unwrap();
                    peak.fetch_max(cur, Ordering::Relaxed);
                    // Simulate immediate hand-off + drain.
                    b.release(batch);
                }
            }));
        }
        for p in producers {
            p.join().unwrap();
        }
        // Each acquire admits only when *q == 0 or *q + batch <= cap, so the
        // observed queue never exceeds cap (batch divides cap here).
        assert!(peak.load(Ordering::Relaxed) <= cap);
    }
}
