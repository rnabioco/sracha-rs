use std::fmt;

use crate::error::{Error, Result};

/// A validated SRA run accession identifier (SRR/ERR/DRR + 6-9 digits).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Accession {
    pub prefix: AccessionPrefix,
    pub number: String,
    raw: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AccessionPrefix {
    /// NCBI Sequence Read Archive
    Srr,
    /// EBI European Nucleotide Archive
    Err,
    /// DDBJ
    Drr,
}

/// A project or study accession that must be resolved to individual runs.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProjectAccession {
    pub kind: ProjectKind,
    raw: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProjectKind {
    /// SRP/ERP/DRP study accessions.
    Study,
    /// PRJNA/PRJEB/PRJDB BioProject accessions.
    BioProject,
}

/// Any accession that can be given as CLI input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InputAccession {
    /// A run accession (SRR/ERR/DRR) — directly downloadable.
    Run(Accession),
    /// A study or BioProject accession — must be resolved to runs first.
    Project(ProjectAccession),
}

impl fmt::Display for Accession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.raw)
    }
}

impl fmt::Display for AccessionPrefix {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Srr => write!(f, "SRR"),
            Self::Err => write!(f, "ERR"),
            Self::Drr => write!(f, "DRR"),
        }
    }
}

impl fmt::Display for ProjectAccession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.raw)
    }
}

impl fmt::Display for InputAccession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Run(acc) => write!(f, "{acc}"),
            Self::Project(proj) => write!(f, "{proj}"),
        }
    }
}

/// Parse and validate an SRA run accession string.
pub fn parse(input: &str) -> Result<Accession> {
    let input = input.trim();
    if input.len() < 9 {
        return Err(Error::InvalidAccession(format!(
            "'{input}' is too short for an SRA run accession"
        )));
    }

    let prefix_str = &input[..3];
    let prefix = match prefix_str.to_uppercase().as_str() {
        "SRR" => AccessionPrefix::Srr,
        "ERR" => AccessionPrefix::Err,
        "DRR" => AccessionPrefix::Drr,
        _ => {
            return Err(Error::InvalidAccession(format!(
                "'{input}' has unrecognized prefix '{prefix_str}' (expected SRR/ERR/DRR)"
            )));
        }
    };

    let number = &input[3..];
    if number.len() < 6 || number.len() > 9 {
        return Err(Error::InvalidAccession(format!(
            "'{input}' has {}-digit number (expected 6-9)",
            number.len()
        )));
    }
    if !number.chars().all(|c| c.is_ascii_digit()) {
        return Err(Error::InvalidAccession(format!(
            "'{input}' contains non-digit characters after prefix"
        )));
    }

    Ok(Accession {
        prefix,
        number: number.to_string(),
        raw: format!("{prefix}{number}"),
    })
}

/// Parse any accession type: run (SRR/ERR/DRR), study (SRP/ERP/DRP),
/// or BioProject (PRJNA/PRJEB/PRJDB).
pub fn parse_input(input: &str) -> Result<InputAccession> {
    let trimmed = input.trim();
    let upper = trimmed.to_uppercase();

    // BioProject: PRJNA/PRJEB/PRJDB + digits (5-char prefix, variable-length number)
    for prefix in &["PRJNA", "PRJEB", "PRJDB"] {
        if upper.starts_with(prefix) {
            let number = &trimmed[prefix.len()..];
            if number.is_empty() || !number.chars().all(|c| c.is_ascii_digit()) {
                return Err(Error::InvalidAccession(format!(
                    "'{trimmed}' contains non-digit characters after {prefix} prefix"
                )));
            }
            return Ok(InputAccession::Project(ProjectAccession {
                kind: ProjectKind::BioProject,
                raw: format!("{prefix}{number}"),
            }));
        }
    }

    // Study: SRP/ERP/DRP + 6-9 digits
    if trimmed.len() >= 9 {
        let prefix_str = &upper[..3];
        if matches!(prefix_str, "SRP" | "ERP" | "DRP") {
            let number = &trimmed[3..];
            if number.len() < 6 || number.len() > 9 {
                return Err(Error::InvalidAccession(format!(
                    "'{trimmed}' has {}-digit number (expected 6-9)",
                    number.len()
                )));
            }
            if !number.chars().all(|c| c.is_ascii_digit()) {
                return Err(Error::InvalidAccession(format!(
                    "'{trimmed}' contains non-digit characters after prefix"
                )));
            }
            return Ok(InputAccession::Project(ProjectAccession {
                kind: ProjectKind::Study,
                raw: format!("{prefix_str}{number}"),
            }));
        }
    }

    // Run accession (SRR/ERR/DRR)
    parse(trimmed).map(InputAccession::Run)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_accessions() {
        let acc = parse("SRR000001").unwrap();
        assert_eq!(acc.prefix, AccessionPrefix::Srr);
        assert_eq!(acc.number, "000001");
        assert_eq!(acc.to_string(), "SRR000001");

        let acc = parse("ERR1234567").unwrap();
        assert_eq!(acc.prefix, AccessionPrefix::Err);

        let acc = parse("DRR123456789").unwrap();
        assert_eq!(acc.prefix, AccessionPrefix::Drr);
        assert_eq!(acc.number, "123456789");
    }

    #[test]
    fn case_insensitive() {
        let acc = parse("srr000001").unwrap();
        assert_eq!(acc.prefix, AccessionPrefix::Srr);
    }

    #[test]
    fn trims_whitespace() {
        let acc = parse("  SRR000001  ").unwrap();
        assert_eq!(acc.to_string(), "SRR000001");
    }

    #[test]
    fn rejects_bad_prefix() {
        assert!(parse("XRR000001").is_err());
    }

    #[test]
    fn rejects_short_number() {
        assert!(parse("SRR12345").is_err());
    }

    #[test]
    fn rejects_non_digits() {
        assert!(parse("SRR00000a").is_err());
    }

    // -----------------------------------------------------------------------
    // parse_input tests
    // -----------------------------------------------------------------------

    #[test]
    fn parse_input_run_accession() {
        let result = parse_input("SRR000001").unwrap();
        assert!(matches!(result, InputAccession::Run(_)));
        assert_eq!(result.to_string(), "SRR000001");
    }

    #[test]
    fn parse_input_study_accession() {
        let result = parse_input("SRP123456").unwrap();
        assert!(matches!(
            result,
            InputAccession::Project(ProjectAccession {
                kind: ProjectKind::Study,
                ..
            })
        ));
        assert_eq!(result.to_string(), "SRP123456");

        let result = parse_input("erp123456").unwrap();
        assert_eq!(result.to_string(), "ERP123456");

        let result = parse_input("DRP123456789").unwrap();
        assert_eq!(result.to_string(), "DRP123456789");
    }

    #[test]
    fn parse_input_bioproject_accession() {
        let result = parse_input("PRJNA123456").unwrap();
        assert!(matches!(
            result,
            InputAccession::Project(ProjectAccession {
                kind: ProjectKind::BioProject,
                ..
            })
        ));
        assert_eq!(result.to_string(), "PRJNA123456");

        let result = parse_input("prjeb12345").unwrap();
        assert_eq!(result.to_string(), "PRJEB12345");

        let result = parse_input("PRJDB1").unwrap();
        assert_eq!(result.to_string(), "PRJDB1");
    }

    #[test]
    fn parse_input_bioproject_rejects_non_digits() {
        assert!(parse_input("PRJNA123abc").is_err());
    }

    #[test]
    fn parse_input_bioproject_rejects_empty_number() {
        assert!(parse_input("PRJNA").is_err());
    }

    #[test]
    fn parse_input_bad_prefix_falls_through() {
        assert!(parse_input("XYZ000001").is_err());
    }
}
