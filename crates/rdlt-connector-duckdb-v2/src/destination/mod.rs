//! The DuckDB destination: one shared database instance, sdk-shelled.

mod client;
mod config;
mod dialect;
mod schema;

pub use config::{Config, ConfigError, config_schema};
// The sqlcore vocabulary IS this destination's options vocabulary —
// re-exported so consumers spell it from here (facade parity).
pub use rdlt_connector_sqlcore::{
    AbsentPolicy, DedupSort, DestinationOptions, MergeStrategy, Scd2Options, SortOrder,
    TableOptions,
};
