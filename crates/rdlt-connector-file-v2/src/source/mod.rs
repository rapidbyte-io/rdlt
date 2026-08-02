//! The source half: files as streams.

mod config;
pub(crate) mod cursor;
pub(crate) mod list;
pub(crate) mod read;

pub use config::{Config, ConfigError, CsvOptions, HintType, Stream, config_schema};
