//! Centralized styling for CLI output.

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
