//! Format-agnostic schema enums: DNA packing choice and length mode.
//!
//! These describe *semantics* of the output, not any specific on-disk
//! layout — the Parquet-specific Arrow schema builder lives in
//! `crate::parquet::schema`.

/// How DNA bases are stored in the `sequence` column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DnaPacking {
    /// One byte per base (`A`, `C`, `G`, `T`, `N`, IUPAC).
    Ascii,
    /// 2 bits per base (4 bases per byte). Lossy for non-ACGT — caller must
    /// verify with [`crate::convert::encoding::is_pure_acgt`] before
    /// choosing this.
    TwoNa,
    /// 4 bits per base (2 bases per byte). Preserves IUPAC ambiguity codes.
    FourNa,
}

impl DnaPacking {
    /// Bytes required to encode `n_bases` bases under this packing.
    pub fn packed_len(self, n_bases: u32) -> u32 {
        match self {
            DnaPacking::Ascii => n_bases,
            DnaPacking::TwoNa => n_bases.div_ceil(4),
            DnaPacking::FourNa => n_bases.div_ceil(2),
        }
    }
}

/// Whether read lengths are uniform across the run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LengthMode {
    /// All reads have the same length. Allows fixed-size columns.
    Fixed { read_len: u32 },
    /// Read lengths vary; emit a per-row `read_len` column.
    Variable,
}

/// User-facing length-mode selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LengthModeChoice {
    /// Detect from data: fixed if all reads share a length, else variable.
    Auto,
    /// Force fixed-length even if detection is ambiguous (errors on mismatch).
    Fixed,
    /// Force variable-length even if reads are uniform.
    Variable,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packed_len_rounds_up() {
        assert_eq!(DnaPacking::Ascii.packed_len(150), 150);
        assert_eq!(DnaPacking::TwoNa.packed_len(150), 38);
        assert_eq!(DnaPacking::TwoNa.packed_len(151), 38);
        assert_eq!(DnaPacking::TwoNa.packed_len(152), 38);
        assert_eq!(DnaPacking::TwoNa.packed_len(153), 39);
        assert_eq!(DnaPacking::FourNa.packed_len(150), 75);
        assert_eq!(DnaPacking::FourNa.packed_len(151), 76);
    }
}
