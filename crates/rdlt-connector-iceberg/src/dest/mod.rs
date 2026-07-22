//! The Iceberg DESTINATION side: config vocabulary, the closed type
//! mapping, the one error boundary, and the commit machinery mapping
//! engine commits onto atomic snapshots (contracts ID1–ID7).

pub mod config;
// The allows are TRANSITIONAL: consumed by dest.rs/commit.rs (T005).
#[allow(dead_code)]
pub(crate) mod errors;
#[allow(dead_code)]
pub(crate) mod schema;

pub use config::{IcebergConfig, config_schema};
