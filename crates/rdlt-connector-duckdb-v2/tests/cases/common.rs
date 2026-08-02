//! Shared helpers for the duckdb-v2 suite.

#![allow(dead_code)] // shared across binaries; not every file uses every helper

use rdlt_connector_duckdb_v2::destination::{Config, Shell};

/// A destination over a fresh database file in `dir`.
pub fn dest_in(dir: &std::path::Path) -> (Config, Shell) {
    let config = Config::new(dir.join("out.duckdb"));
    let shell = Shell::new(config.clone()).expect("valid");
    (config, shell)
}
