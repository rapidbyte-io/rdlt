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
pub use connector::{FAIL_POINTS, File, S3_FAIL_POINTS};
pub(crate) use layout::STAGING_DIR;

/// The sdk shell around [`File`] — the destination's public form.
pub type Shell = rdlt_connector_sdk::destination::Shell<File>;

/// The CANONICAL local-parquet spelling — supported by name, not a
/// deprecated alias (bench, CLI, and crash-sweep tooling consume it).
pub type ParquetDir = Shell;

/// Seams the tests need and nothing else may use. Not a public API.
#[doc(hidden)]
pub mod testhook {
    use rdlt_connector_sdk::spi::DestinationError;

    use crate::location::Location;

    /// Count rows over the ownership listing, both protocols.
    pub async fn count_rows_async(
        config: &super::Config,
        table: &str,
    ) -> Result<u64, DestinationError> {
        let location = Location::for_dest(&config.path, config.location.as_ref())?;
        super::inspect::count_rows_async(&location, table).await
    }

    /// The synchronous local-only form with its frozen refusal.
    pub fn count_rows(config: &super::Config, table: &str) -> Result<u64, DestinationError> {
        let location = Location::for_dest(&config.path, config.location.as_ref())?;
        super::inspect::count_rows(&location, table)
    }
}
