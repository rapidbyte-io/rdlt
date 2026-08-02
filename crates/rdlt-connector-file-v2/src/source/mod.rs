//! The source half: files as streams.

mod config;
mod cursor;

pub use config::{Config, ConfigError, CsvOptions, HintType, Stream, config_schema};
