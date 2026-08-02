//! The destination half: files as an exactly-once target.
//!
//! Staging lives under `.rdlt-staging/{scope}/{load}/`, receipts
//! forever in `_rdlt_commits.{scope}.json`, state beside them — the
//! 4-phase commit is [`load`]'s doc comment.

mod config;
mod connector;
mod inspect;
mod layout;
mod load;
mod stage;
mod truncate;

pub use config::{Config, ConfigError, DestFormat, config_schema};
#[doc(hidden)]
pub use connector::testhook;
pub use connector::{FAIL_POINTS, File, S3_FAIL_POINTS};
pub(crate) use layout::STAGING_DIR;

/// The sdk shell around [`File`] — the destination's public form.
pub type Shell = rdlt_connector_sdk::destination::Shell<File>;

/// The CANONICAL local-parquet spelling — supported by name, not a
/// deprecated alias (bench, CLI, and crash-sweep tooling consume it).
pub type ParquetDir = Shell;
