//! The Iceberg DESTINATION side: config vocabulary, the closed type
//! mapping, the one error boundary, and the commit machinery mapping
//! engine commits onto atomic snapshots.

mod catalog;
mod commit;
pub mod config;
mod ensure;
mod errors;
mod schema;
mod session;
mod state;
#[cfg(test)]
mod test_support;
mod writer;

pub use config::{IcebergConfig, config_schema};
#[cfg(feature = "failpoints")]
pub use session::ICE_FAIL_POINTS;
pub use session::IcebergDest;
