//! Centralized styling for CLI output.
//!
//! These color unconditionally. They are called from both `println!` (the
//! `info` report tables) and `eprintln!` (progress and diagnostics), so there
//! is no single stream to test here. `enable_windows_ansi` in `main.rs` makes
//! sure the escapes render rather than print raw on a Windows console.

use owo_colors::OwoColorize;
use std::fmt::Display;

/// Style for accession names and section headers
pub fn header<T: Display>(s: T) -> String {
    format!("{}", s.bold())
}

/// Style for labels like "Size:", "MD5:", "Mirrors:"
pub fn label<T: Display>(s: T) -> String {
    format!("{}", s.bold())
}

/// Style for important counts/numbers (spots, reads, etc.)
pub fn count<T: Display>(n: T) -> String {
    format!("{}", n.green())
}

/// Style for values in key-value pairs (sizes, hashes, etc.)
pub fn value<T: Display>(s: T) -> String {
    format!("{}", s.cyan())
}

/// Style for file paths and URLs
pub fn path<T: Display>(s: T) -> String {
    format!("{}", s.cyan())
}

/// Style for error prefix "error:"
pub fn error_label<T: Display>(s: T) -> String {
    format!("{}", s.red().bold())
}

/// Style for warning prefix "warning:"
pub fn warn_label<T: Display>(s: T) -> String {
    format!("{}", s.yellow().bold())
}
