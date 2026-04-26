//! Anonymous in-memory backing for the .sra stream.
//!
//! Returns a `BackingStore` containing both an open `File` and a
//! `PathBuf` that aliases that file. The download writer pwrites to
//! the File; the cursor mmaps via the path. No bytes hit a real
//! filesystem on Linux.
//!
//! Platform behavior:
//! - **Linux**: `memfd_create(2)` for an unnamed in-RAM file +
//!   `/proc/self/fd/<fd>` as the alias path. Auto-released when the
//!   `File` is dropped.
//! - **Other (macOS, etc.)**: `NamedTempFile::new_in($TMPDIR)`. Path
//!   is real and disk-backed unless `$TMPDIR` is on tmpfs. Set
//!   `TMPDIR=/dev/shm` (Linux) or any tmpfs mount for RAM behavior.
//!
//! The Drop impl on the returned struct releases both the fd and
//! the temp file.

use std::fs::File;
use std::path::PathBuf;

pub struct BackingStore {
    /// Owning handle. Drop closes the fd / removes the file.
    pub file: File,
    /// Filesystem-visible path that aliases `file`. On Linux this
    /// is `/proc/self/fd/<fd>` — valid only while the fd is open.
    pub path: PathBuf,
    /// Owned NamedTempFile for the non-Linux fallback. Held to keep
    /// the temp path alive until this struct drops.
    #[allow(dead_code)]
    _tempfile: Option<tempfile::NamedTempFile>,
}

impl BackingStore {
    /// Open an anonymous in-memory file sized to `size` bytes.
    pub fn open(size: u64) -> std::io::Result<Self> {
        let store = open_inner()?;
        store.file.set_len(size)?;
        Ok(store)
    }
}

#[cfg(target_os = "linux")]
fn open_inner() -> std::io::Result<BackingStore> {
    use std::ffi::CString;
    use std::os::fd::{AsRawFd, FromRawFd, RawFd};

    const MFD_CLOEXEC: libc::c_uint = 0x0001;
    let name = CString::new("sracha-stream").unwrap();
    // SAFETY: name is null-terminated; libc::memfd_create exists on
    // Linux >= 3.17.
    let fd: RawFd = unsafe { libc::memfd_create(name.as_ptr(), MFD_CLOEXEC) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: fd is freshly returned by memfd_create.
    let file = unsafe { File::from_raw_fd(fd) };
    let path = PathBuf::from(format!("/proc/self/fd/{}", file.as_raw_fd()));
    Ok(BackingStore {
        file,
        path,
        _tempfile: None,
    })
}

#[cfg(not(target_os = "linux"))]
fn open_inner() -> std::io::Result<BackingStore> {
    let nt = tempfile::NamedTempFile::new()?;
    let path = nt.path().to_path_buf();
    let file = nt.reopen()?;
    Ok(BackingStore {
        file,
        path,
        _tempfile: Some(nt),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Seek, SeekFrom};

    #[cfg(unix)]
    fn pwrite(file: &File, offset: u64, buf: &[u8]) -> std::io::Result<()> {
        use std::os::unix::fs::FileExt;
        file.write_all_at(buf, offset)
    }

    /// Cross-platform exercise of the BackingStore: open at `size`,
    /// pwrite two non-contiguous byte runs, mmap-read via the alias
    /// path, and verify both runs round-trip. Validates the Linux
    /// memfd path and the non-Linux NamedTempFile path together.
    #[cfg(unix)]
    #[test]
    fn backing_store_roundtrip() {
        let size: u64 = 4096;
        let store = BackingStore::open(size).expect("open");

        assert_eq!(store.file.metadata().unwrap().len(), size);
        assert!(
            store.path.exists() || cfg!(target_os = "linux"),
            "alias path {} must exist on non-Linux",
            store.path.display()
        );

        let head = b"sracha-head";
        let tail = b"sracha-tail";
        pwrite(&store.file, 0, head).unwrap();
        pwrite(&store.file, size - tail.len() as u64, tail).unwrap();

        let mut reader = std::fs::File::open(&store.path).expect("reopen via alias path");
        let mut got_head = vec![0u8; head.len()];
        reader.read_exact(&mut got_head).unwrap();
        assert_eq!(&got_head, head);

        reader
            .seek(SeekFrom::Start(size - tail.len() as u64))
            .unwrap();
        let mut got_tail = vec![0u8; tail.len()];
        reader.read_exact(&mut got_tail).unwrap();
        assert_eq!(&got_tail, tail);
    }
}
