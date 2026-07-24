//! # rdlt-bench — the declarative benchmark harness
//!
//! Cells are data (`benches/cells/*.toml`); this crate is the one protocol that
//! runs them. Library + thin binary so the harness's own logic (loaders, stats,
//! gate verdicts, report splicing) is unit-testable under nextest without
//! containers.
//!
//! Dev-only (`publish = false`); measures the product, never ships with it.

pub mod artifact;
pub mod cells;
pub mod competitors;
pub mod fixtures;
pub mod gate;
pub mod library_mode;
pub mod protocol;
pub mod report;
pub mod runner;
pub mod sample;

/// Harness errors: one type, offender always named in the message — a bad
/// cell file must say which file and which cell.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct BenchError(pub String);

impl From<std::io::Error> for BenchError {
    fn from(e: std::io::Error) -> Self {
        Self(format!("io: {e}"))
    }
}

pub type Result<T> = std::result::Result<T, BenchError>;

/// Read + parse one TOML config file, naming the file in either failure. The
/// one place the "reading {path}" / "parsing {path}" error wording lives — the
/// bars/fixtures/variants/cell loaders all funnel through here.
pub(crate) fn load_toml<T: serde::de::DeserializeOwned>(path: &std::path::Path) -> Result<T> {
    let raw = std::fs::read_to_string(path)
        .map_err(|e| BenchError(format!("reading {}: {e}", path.display())))?;
    toml::from_str(&raw).map_err(|e| BenchError(format!("parsing {}: {e}", path.display())))
}
