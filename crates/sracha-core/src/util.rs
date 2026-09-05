/// Format a byte count as a human-readable string (e.g., "276.15 MiB").
pub fn format_size(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * KIB;
    const GIB: u64 = 1024 * MIB;
    const TIB: u64 = 1024 * GIB;

    if bytes >= TIB {
        format!("{:.2} TiB", bytes as f64 / TIB as f64)
    } else if bytes >= GIB {
        format!("{:.2} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.2} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.2} KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{bytes} B")
    }
}

/// Insert thousands separators into an integer (e.g. `1234567` → `"1,234,567"`).
pub fn thousands<T: Into<u64>>(n: T) -> String {
    let s = n.into().to_string();
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, b) in bytes.iter().enumerate() {
        if i > 0 && (bytes.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(*b as char);
    }
    out
}

/// Format a base-pair count as Kbp / Mbp / Gbp for readability.
pub fn format_bases(b: u64) -> String {
    const M: u64 = 1_000_000;
    const G: u64 = 1_000_000_000;
    if b >= G {
        format!("{:.2} Gbp", b as f64 / G as f64)
    } else if b >= M {
        format!("{:.2} Mbp", b as f64 / M as f64)
    } else if b >= 1_000 {
        format!("{:.1} Kbp", b as f64 / 1_000.0)
    } else {
        format!("{b} bp")
    }
}

/// Return a pseudo-random jitter in `0..max` milliseconds.
///
/// Derives entropy from the current time's sub-second nanoseconds — good
/// enough to de-synchronize concurrent retry backoffs (avoiding a thundering
/// herd against a struggling host) without pulling in a `rand` dependency.
/// `max == 0` yields `0`.
pub fn jitter_ms(max: u64) -> u64 {
    if max == 0 {
        return 0;
    }
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u64;
    nanos % max
}

/// Write `buf` in full at absolute byte `offset`, without relying on the
/// file's cursor.
///
/// The parallel chunked downloader has many threads writing disjoint ranges of
/// one shared handle at once, so every write must name its own offset. Unix
/// gets that from `pwrite`; Windows from `WriteFile` with an explicit
/// `OVERLAPPED` offset. Both are genuinely positional, so concurrent calls
/// against a shared handle stay correct.
///
/// Windows' `seek_write` does move the handle's cursor as a side effect. That
/// is harmless for the caller that matters: the download handle is opened
/// write-only and touched exclusively through this function, so nothing reads
/// the cursor back.
#[cfg(unix)]
pub fn write_all_at(file: &std::fs::File, buf: &[u8], offset: u64) -> std::io::Result<()> {
    std::os::unix::fs::FileExt::write_all_at(file, buf, offset)
}

#[cfg(windows)]
pub fn write_all_at(file: &std::fs::File, mut buf: &[u8], mut offset: u64) -> std::io::Result<()> {
    // Windows has no `write_all_at`, and a short write is not an error there,
    // so drive the loop the way std's Unix implementation does.
    use std::os::windows::fs::FileExt;
    while !buf.is_empty() {
        match file.seek_write(buf, offset) {
            Ok(0) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "failed to write whole buffer",
                ));
            }
            Ok(n) => {
                buf = &buf[n..];
                offset += n as u64;
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// Fill `buf` from absolute byte `offset`, without relying on the file's
/// cursor. The read counterpart of [`write_all_at`].
#[cfg(unix)]
pub fn read_exact_at(file: &std::fs::File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    std::os::unix::fs::FileExt::read_exact_at(file, buf, offset)
}

#[cfg(windows)]
pub fn read_exact_at(
    file: &std::fs::File,
    mut buf: &mut [u8],
    mut offset: u64,
) -> std::io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !buf.is_empty() {
        match file.seek_read(buf, offset) {
            Ok(0) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "failed to fill whole buffer",
                ));
            }
            Ok(n) => {
                buf = &mut buf[n..];
                offset += n as u64;
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jitter_ms_is_bounded() {
        for max in [1u64, 7, 500, 1000] {
            for _ in 0..64 {
                assert!(jitter_ms(max) < max);
            }
        }
    }

    #[test]
    fn jitter_ms_zero_max() {
        assert_eq!(jitter_ms(0), 0);
    }

    #[test]
    fn format_size_bytes() {
        assert_eq!(format_size(0), "0 B");
        assert_eq!(format_size(512), "512 B");
        assert_eq!(format_size(1023), "1023 B");
    }

    #[test]
    fn format_size_kib() {
        assert_eq!(format_size(1024), "1.00 KiB");
        assert_eq!(format_size(1536), "1.50 KiB");
    }

    #[test]
    fn format_size_mib() {
        assert_eq!(format_size(1024 * 1024), "1.00 MiB");
        assert_eq!(format_size(276 * 1024 * 1024 + 153_600), "276.15 MiB");
    }

    #[test]
    fn format_size_gib() {
        assert_eq!(format_size(1024 * 1024 * 1024), "1.00 GiB");
    }

    #[test]
    fn format_size_tib() {
        assert_eq!(format_size(1024 * 1024 * 1024 * 1024), "1.00 TiB");
    }

    #[test]
    fn thousands_basics() {
        assert_eq!(thousands(0u64), "0");
        assert_eq!(thousands(42u64), "42");
        assert_eq!(thousands(1_000u64), "1,000");
        assert_eq!(thousands(1_234_567u64), "1,234,567");
        assert_eq!(thousands(1_000_000_000u64), "1,000,000,000");
    }

    #[test]
    fn format_bases_scales() {
        assert_eq!(format_bases(0), "0 bp");
        assert_eq!(format_bases(999), "999 bp");
        assert_eq!(format_bases(1_500), "1.5 Kbp");
        assert_eq!(format_bases(2_500_000), "2.50 Mbp");
        assert_eq!(format_bases(3_000_000_000), "3.00 Gbp");
    }

    // positional I/O shim

    #[test]
    fn positional_io_round_trip() {
        use std::io::Write;

        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.write_all(&[0u8; 64]).unwrap();
        tmp.flush().unwrap();

        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(tmp.path())
            .unwrap();

        // Write two disjoint ranges out of order, as the chunked downloader does.
        write_all_at(&file, b"world", 32).unwrap();
        write_all_at(&file, b"hello", 8).unwrap();

        let mut buf = [0u8; 5];
        read_exact_at(&file, &mut buf, 8).unwrap();
        assert_eq!(&buf, b"hello");
        read_exact_at(&file, &mut buf, 32).unwrap();
        assert_eq!(&buf, b"world");

        // Untouched bytes stay zero: nothing was written at the cursor.
        let mut head = [0xffu8; 8];
        read_exact_at(&file, &mut head, 0).unwrap();
        assert_eq!(head, [0u8; 8]);
    }

    #[test]
    fn read_exact_at_past_eof_errors() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let file = std::fs::File::open(tmp.path()).unwrap();
        let mut buf = [0u8; 4];
        let err = read_exact_at(&file, &mut buf, 0).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }
}
