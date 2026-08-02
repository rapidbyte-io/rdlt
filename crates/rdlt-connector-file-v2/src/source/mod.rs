//! The source half: files as streams.

mod config;
mod cursor;
mod list;

pub use config::{Config, ConfigError, CsvOptions, HintType, Stream, config_schema};
