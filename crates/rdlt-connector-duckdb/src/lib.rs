//! # rdlt-connector-duckdb — DuckDB connector
//!
//! Currently destination-only; the module layout mirrors the connector-family
//! convention (`rdlt-connector-postgres`): the destination lives in [`dest`],
//! and this root stays a thin façade so a future source slots in beside it.

pub mod dest;
