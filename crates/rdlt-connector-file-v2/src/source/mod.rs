//! The source half: files as streams.

mod config;
mod connector;
pub mod cursor;
pub(crate) mod list;
pub(crate) mod read;

pub use config::{Config, ConfigError, CsvOptions, HintType, Stream, config_schema};

/// The crash points this source arms — exported so the sweep iterates
/// exactly this list. These spellings are frozen.
pub const FAIL_POINTS: &[&str] = &["file.list", "file.read"];
pub use connector::File;
#[doc(hidden)]
pub use connector::skipped_fetches;

/// The canonical face: the sdk shell over [`File`].
pub type Shell = rdlt_connector_sdk::source::Shell<File>;
