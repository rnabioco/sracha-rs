//! `Read + Seek` over an HTTP resource, backed by range requests.
//!
//! This exists so `sracha vdb` can inspect an archive that lives on NCBI's
//! S3 mirror without downloading it. A KAR archive is laid out so that the
//! answers to every inspection question — the TOC, then a handful of small
//! column index files — sit in a few kilobytes scattered through a file
//! that may be gigabytes long, which is exactly the access pattern range
//! requests are for.
//!
//! Reads are served from a block cache: a miss fetches the aligned block(s)
//! covering the request in a single `Range` GET, so the sequential
//! `seek` + `read_exact` pattern `KarArchive` uses costs one round trip per
//! new region rather than one per read.
//!
//! Deliberately blocking. [`sracha_vdb::kar::KarArchive`] is generic over
//! `Read + Seek`, and the inspection paths are latency-bound rather than
//! throughput-bound, so an async reader would buy nothing and force a
//! parallel set of decode entry points. The client owns a private runtime
//! on its own thread (`reqwest::blocking`), so constructing one inside an
//! async context is safe — call it from `spawn_blocking` to avoid parking
//! a runtime worker.

use std::collections::HashMap;
use std::io::{self, Read, Seek, SeekFrom};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Cache block size. Sized so that the cluster of `idx`/`idx1`/`idx2`
/// files belonging to one column usually lands in a single fetch, while
/// still being small enough that a metadata-only command stays in the tens
/// of kilobytes.
const BLOCK_SIZE: u64 = 64 * 1024;

/// Upper bound on cached blocks (~32 MiB). Blocks past this are evicted in
/// insertion order, which suits the forward-ish walk of a TOC scan.
const MAX_CACHED_BLOCKS: usize = 512;

/// Matches `download::MAX_RETRIES` — transient 5xx and connection resets
/// on NCBI's mirror are common enough to be worth retrying, rare enough
/// that three attempts is plenty.
const MAX_RETRIES: u32 = 3;

/// Counts bytes actually pulled over the wire.
///
/// Shared with the caller so a test can assert that an inspection command
/// stays proportional to the metadata it reads rather than to the archive
/// size — the regression this whole module exists to prevent.
#[derive(Debug, Clone, Default)]
pub struct TransferStats(Arc<Inner>);

#[derive(Debug, Default)]
struct Inner {
    bytes: AtomicU64,
    requests: AtomicU64,
}

impl TransferStats {
    /// Total response-body bytes received.
    pub fn bytes_fetched(&self) -> u64 {
        self.0.bytes.load(Ordering::Relaxed)
    }

    /// Number of HTTP requests issued, including the initial HEAD.
    pub fn requests(&self) -> u64 {
        self.0.requests.load(Ordering::Relaxed)
    }
}

/// A seekable reader over an HTTP resource that supports byte ranges.
pub struct HttpRangeReader {
    client: reqwest::blocking::Client,
    url: String,
    len: u64,
    pos: u64,
    blocks: HashMap<u64, Arc<[u8]>>,
    /// Block indices in insertion order, for eviction.
    order: std::collections::VecDeque<u64>,
    stats: TransferStats,
}

impl HttpRangeReader {
    /// Open `url`, learning its length from a HEAD request.
    ///
    /// Fails if the server does not advertise `Accept-Ranges: bytes`;
    /// without ranges every read would pull the whole object and the
    /// caller is better off downloading it properly.
    pub fn open(url: &str) -> io::Result<Self> {
        Self::with_stats(url, TransferStats::default())
    }

    /// Same as [`open`](Self::open), but reporting transfer volume into a
    /// caller-owned [`TransferStats`].
    pub fn with_stats(url: &str, stats: TransferStats) -> io::Result<Self> {
        let client = reqwest::blocking::Client::builder()
            .user_agent(format!("sracha/{}", env!("CARGO_PKG_VERSION")))
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(60))
            .build()
            .map_err(io::Error::other)?;

        stats.0.requests.fetch_add(1, Ordering::Relaxed);
        let resp = client.head(url).send().map_err(io::Error::other)?;
        if !resp.status().is_success() {
            return Err(io::Error::other(format!(
                "HEAD {url} returned HTTP {}",
                resp.status()
            )));
        }

        let supports_range = resp
            .headers()
            .get(reqwest::header::ACCEPT_RANGES)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.contains("bytes"));
        if !supports_range {
            return Err(io::Error::other(format!(
                "{url} does not advertise Accept-Ranges: bytes; \
                 remote inspection needs range support"
            )));
        }

        let len = resp
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .ok_or_else(|| io::Error::other(format!("missing Content-Length for {url}")))?;

        tracing::debug!("remote archive {url}: {len} bytes, range requests supported");

        Ok(Self {
            client,
            url: url.to_string(),
            len,
            pos: 0,
            blocks: HashMap::new(),
            order: std::collections::VecDeque::new(),
            stats,
        })
    }

    /// Total size of the remote object.
    pub fn len(&self) -> u64 {
        self.len
    }

    /// Whether the remote object is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Transfer counters for this reader.
    pub fn stats(&self) -> &TransferStats {
        &self.stats
    }

    /// Fetch `[start, start + len)` with retries and exponential backoff.
    fn fetch(&self, start: u64, len: u64) -> io::Result<Vec<u8>> {
        let end = start + len - 1;
        let range = format!("bytes={start}-{end}");
        let mut last: Option<io::Error> = None;

        for attempt in 0..MAX_RETRIES {
            if attempt > 0 {
                let backoff = Duration::from_millis(200 << (attempt - 1));
                tracing::debug!(
                    "retrying range {range} of {} (attempt {}/{MAX_RETRIES}), backoff {backoff:?}",
                    self.url,
                    attempt + 1,
                );
                std::thread::sleep(backoff);
            }

            self.stats.0.requests.fetch_add(1, Ordering::Relaxed);
            match self.try_fetch(&range) {
                Ok(bytes) => {
                    self.stats
                        .0
                        .bytes
                        .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                    return Ok(bytes);
                }
                Err(e) => last = Some(e),
            }
        }

        Err(last.unwrap_or_else(|| io::Error::other("range request failed")))
    }

    fn try_fetch(&self, range: &str) -> io::Result<Vec<u8>> {
        let resp = self
            .client
            .get(&self.url)
            .header(reqwest::header::RANGE, range)
            .send()
            .map_err(io::Error::other)?;

        // Require 206. A server that ignored the Range header and replied
        // 200 would hand us the whole archive, which we would then splice
        // in at the wrong offset — silent corruption, and the transfer we
        // were trying to avoid.
        if resp.status() != reqwest::StatusCode::PARTIAL_CONTENT {
            return Err(io::Error::other(format!(
                "range request {range} returned HTTP {} (expected 206)",
                resp.status()
            )));
        }

        resp.bytes()
            .map(|b| b.to_vec())
            .map_err(|e| io::Error::other(format!("reading range {range}: {e}")))
    }

    /// Return the cached block `idx`, fetching it (with any adjacent
    /// missing blocks up to `through`) if absent.
    fn block(&mut self, idx: u64, through: u64) -> io::Result<Arc<[u8]>> {
        if let Some(b) = self.blocks.get(&idx) {
            return Ok(b.clone());
        }

        // Extend the fetch over the contiguous run of missing blocks the
        // caller is about to ask for, so one read_exact spanning several
        // blocks costs one request rather than one per block.
        let mut last = idx;
        while last < through && !self.blocks.contains_key(&(last + 1)) {
            last += 1;
        }

        let start = idx * BLOCK_SIZE;
        let end = ((last + 1) * BLOCK_SIZE).min(self.len);
        let bytes = self.fetch(start, end - start)?;

        let mut wanted = None;
        for (i, chunk) in bytes.chunks(BLOCK_SIZE as usize).enumerate() {
            let block_idx = idx + i as u64;
            let block: Arc<[u8]> = Arc::from(chunk);
            if block_idx == idx {
                wanted = Some(block.clone());
            }
            if self.blocks.insert(block_idx, block).is_none() {
                self.order.push_back(block_idx);
            }
        }
        while self.order.len() > MAX_CACHED_BLOCKS {
            if let Some(evict) = self.order.pop_front() {
                self.blocks.remove(&evict);
            }
        }

        wanted.ok_or_else(|| io::Error::other("short range response"))
    }
}

impl Read for HttpRangeReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.pos >= self.len || buf.is_empty() {
            return Ok(0);
        }
        let want = (buf.len() as u64).min(self.len - self.pos);
        let idx = self.pos / BLOCK_SIZE;
        let last = (self.pos + want - 1) / BLOCK_SIZE;

        let block = self.block(idx, last)?;
        let off = (self.pos - idx * BLOCK_SIZE) as usize;
        // One block per call; `read_exact` loops for the rest, and the
        // read-ahead in `block` means those iterations are cache hits.
        let n = (block.len() - off).min(want as usize);
        buf[..n].copy_from_slice(&block[off..off + n]);
        self.pos += n as u64;
        Ok(n)
    }
}

impl Seek for HttpRangeReader {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let new = match pos {
            SeekFrom::Start(n) => n as i64,
            SeekFrom::Current(d) => self.pos as i64 + d,
            SeekFrom::End(d) => self.len as i64 + d,
        };
        if new < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek to a negative position",
            ));
        }
        self.pos = new as u64;
        Ok(self.pos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Write};
    use std::net::{TcpListener, TcpStream};

    /// Minimal HTTP/1.1 server that understands HEAD and single-range GET.
    ///
    /// Hand-rolled rather than wiremock because the reader under test is
    /// blocking and wiremock is async — driving one from the other in a
    /// test buys more complexity than the ~40 lines here.
    fn serve(body: Vec<u8>) -> (String, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { break };
                if !handle_one(stream, &body) {
                    break;
                }
            }
        });
        (format!("http://{addr}/archive.sra"), handle)
    }

    /// Returns false when the client asked us to stop.
    fn handle_one(mut stream: TcpStream, body: &[u8]) -> bool {
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut request_line = String::new();
        if reader.read_line(&mut request_line).is_err() || request_line.is_empty() {
            return true;
        }
        let mut range = None;
        loop {
            let mut line = String::new();
            if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
                break;
            }
            if let Some(v) = line.to_ascii_lowercase().strip_prefix("range: bytes=") {
                let v = v.trim().to_string();
                let (a, b) = v.split_once('-').unwrap();
                range = Some((
                    a.parse::<usize>().unwrap(),
                    b.parse::<usize>().unwrap_or(body.len() - 1),
                ));
            }
        }

        if request_line.starts_with("SHUTDOWN") {
            return false;
        }

        if request_line.starts_with("HEAD") {
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(head.as_bytes());
            return true;
        }

        let (start, end) = range.unwrap_or((0, body.len() - 1));
        let end = end.min(body.len() - 1);
        let slice = &body[start..=end];
        let head = format!(
            "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\n\
             Content-Range: bytes {start}-{end}/{}\r\nConnection: close\r\n\r\n",
            slice.len(),
            body.len()
        );
        let _ = stream.write_all(head.as_bytes());
        let _ = stream.write_all(slice);
        true
    }

    fn shutdown(url: &str) {
        let addr = url.trim_start_matches("http://");
        let addr = addr.split('/').next().unwrap();
        if let Ok(mut s) = TcpStream::connect(addr) {
            let _ = s.write_all(b"SHUTDOWN / HTTP/1.1\r\n\r\n");
        }
    }

    fn body(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    #[test]
    fn reads_a_slice_from_the_middle() {
        let data = body(300_000);
        let (url, handle) = serve(data.clone());

        let mut r = HttpRangeReader::open(&url).unwrap();
        assert_eq!(r.len(), 300_000);
        r.seek(SeekFrom::Start(100_000)).unwrap();
        let mut buf = vec![0u8; 4096];
        r.read_exact(&mut buf).unwrap();
        assert_eq!(buf, data[100_000..104_096]);

        shutdown(&url);
        handle.join().unwrap();
    }

    #[test]
    fn a_small_read_transfers_one_block_not_the_file() {
        let data = body(4 * 1024 * 1024);
        let (url, handle) = serve(data.clone());

        let mut r = HttpRangeReader::open(&url).unwrap();
        let mut buf = [0u8; 24];
        r.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, &data[..24]);
        assert_eq!(r.stats().bytes_fetched(), BLOCK_SIZE);

        shutdown(&url);
        handle.join().unwrap();
    }

    #[test]
    fn repeat_reads_of_a_cached_region_cost_nothing() {
        let data = body(300_000);
        let (url, handle) = serve(data.clone());

        let mut r = HttpRangeReader::open(&url).unwrap();
        let mut buf = vec![0u8; 1000];
        r.read_exact(&mut buf).unwrap();
        let after_first = r.stats().bytes_fetched();

        for _ in 0..5 {
            r.seek(SeekFrom::Start(0)).unwrap();
            r.read_exact(&mut buf).unwrap();
        }
        assert_eq!(r.stats().bytes_fetched(), after_first);

        shutdown(&url);
        handle.join().unwrap();
    }

    #[test]
    fn a_read_spanning_blocks_is_one_request() {
        let data = body(1024 * 1024);
        let (url, handle) = serve(data.clone());

        let mut r = HttpRangeReader::open(&url).unwrap();
        let before = r.stats().requests();
        let mut buf = vec![0u8; (BLOCK_SIZE * 3) as usize];
        r.read_exact(&mut buf).unwrap();
        assert_eq!(buf, data[..buf.len()]);
        assert_eq!(r.stats().requests() - before, 1);

        shutdown(&url);
        handle.join().unwrap();
    }

    #[test]
    fn reads_clamp_at_end_of_file() {
        let data = body(1000);
        let (url, handle) = serve(data.clone());

        let mut r = HttpRangeReader::open(&url).unwrap();
        r.seek(SeekFrom::End(-10)).unwrap();
        let mut buf = Vec::new();
        r.read_to_end(&mut buf).unwrap();
        assert_eq!(buf, data[990..]);

        shutdown(&url);
        handle.join().unwrap();
    }
}
