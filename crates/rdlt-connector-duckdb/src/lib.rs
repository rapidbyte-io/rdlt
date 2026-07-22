//! # rdlt-connector-duckdb — DuckDB connector
//!
//! Currently destination-only; the module layout mirrors the connector-family
//! convention (`rdlt-connector-postgres`): the destination lives in [`dest`],
//! and this root stays a thin façade so a future source slots in beside it.

pub mod dest;

// Root re-exports: the crate was destination-only at the root before the
// feature-013 `dest` module restructure — keep the old import paths working
// (013 review finding 10).
#[cfg(feature = "failpoints")]
pub use dest::FAIL_POINTS;
pub use dest::{DestOptions, DuckDb, MergeStrategy, TableOptions};
