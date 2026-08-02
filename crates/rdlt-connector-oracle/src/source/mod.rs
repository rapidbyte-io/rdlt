//! The source half: Oracle tables as streams.

mod client;
mod config;
mod connector;
pub mod cursor;
mod read;
mod schema;

pub use config::{Config, ConfigError, Stream, config_schema};
pub use connector::{FAIL_POINTS, Oracle};

/// The sdk shell around [`Oracle`] — the source's public form.
pub type Shell = rdlt_connector_sdk::source::Shell<Oracle>;
