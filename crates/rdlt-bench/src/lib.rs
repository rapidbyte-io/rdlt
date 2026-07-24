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

impl BenchError {
    pub fn msg(m: impl Into<String>) -> Self {
        Self(m.into())
    }
}

impl From<std::io::Error> for BenchError {
    fn from(e: std::io::Error) -> Self {
        Self(format!("io: {e}"))
    }
}

pub type Result<T> = std::result::Result<T, BenchError>;
