//! The Iceberg DESTINATION side: config vocabulary, the closed type
//! mapping, the one error boundary, and the commit machinery mapping
//! engine commits onto atomic snapshots (contracts ID1–ID7).

pub mod config;

pub use config::{IcebergConfig, config_schema};
