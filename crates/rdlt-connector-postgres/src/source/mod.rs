//! # PostgreSQL source
//!
//! Declarative (YAML) Postgres source: catalog reflection publishes declared
//! schemas, rows stream as typed Arrow batches decoded straight from the
//! binary COPY wire format (structured path — the shredder is bypassed), and
//! cursor-column incremental has dlt-parity boundary semantics with
//! mid-table checkpointed resume. Depends on the SPI only.
//!
//! Error policy: classify Transient/Fatal, never retry here.

pub(crate) mod cdc;
pub mod config;
mod connector;
pub(crate) mod copy_decode;
mod copy_pump;
mod cursor;
mod errors;
#[cfg(feature = "failpoints")]
mod fail_points;
mod reflect;
mod sqlgen;
#[doc(hidden)]
pub mod testhook;
mod type_map;

pub use config::{ConfigError, PostgresConfig, config_schema};
pub use connector::PostgresSource;
pub use type_map::HintType;

#[cfg(feature = "failpoints")]
#[doc(hidden)]
pub use fail_points::{CDC_FAIL_POINTS, FAIL_POINTS};

pub(crate) use connector::connect;
