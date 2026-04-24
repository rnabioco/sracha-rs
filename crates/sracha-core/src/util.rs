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

#[cfg(test)]
mod tests {
    use super::*;

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
}
