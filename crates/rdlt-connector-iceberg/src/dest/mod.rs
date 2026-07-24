//! The Iceberg DESTINATION side: config vocabulary, the closed type
//! mapping, the one error boundary, and the commit machinery mapping
//! engine commits onto atomic snapshots.

pub(crate) mod commit;
pub mod config;
#[allow(clippy::module_inception)]
mod dest;
pub(crate) mod errors;
pub(crate) mod schema;

pub use config::{IcebergConfig, config_schema};
#[cfg(feature = "failpoints")]
pub use dest::ICE_FAIL_POINTS;
pub use dest::IcebergDest;
