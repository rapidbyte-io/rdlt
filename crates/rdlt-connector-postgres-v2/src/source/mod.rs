//! The Postgres SOURCE: binary-COPY→Arrow extraction, catalog reflection,
//! query streams, cursor-column incremental, and (when the `cdc:` block is
//! configured) change data capture over logical replication.
//!
//! Layout, one noun per module: `connector` is the SPI face; `config` the
//! document vocabulary + local validation; `plan` the one catalog-dependent
//! validation gate; `reflect` the catalog round trip; `sql` the injection-
//! safe text assembly; `copy` the shared COPY read loop; `cursor/` the
//! incremental machinery; `cdc/` change data capture; `errors` the
//! phase-tagged taxonomy.

mod cdc;
pub mod config;
mod connector;
mod copy;
mod cursor;
mod errors;
#[cfg(feature = "failpoints")]
mod fail_points;
mod plan;
pub(crate) mod reflect;
mod sql;

pub use config::{Config, ConfigError, config_schema};
pub use connector::Postgres;

pub(crate) use connector::connect;

#[cfg(feature = "failpoints")]
pub use fail_points::FAIL_POINTS;
