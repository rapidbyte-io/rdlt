//! # rdlt-connector-duckdb — DuckDB connector
//!
//! Currently destination-only; the module layout mirrors the connector-family
//! convention (`rdlt-connector-postgres`): the destination lives in [`dest`],
//! and this root stays a thin façade so a future source slots in beside it.

pub mod dest;

// Root re-exports: the crate exposed these types at the root before they
// moved into the `dest` module — re-export to keep the old import paths
// working.
#[cfg(feature = "failpoints")]
pub use dest::FAIL_POINTS;
pub use dest::{DestOptions, DuckDb, MergeStrategy, TableOptions};
