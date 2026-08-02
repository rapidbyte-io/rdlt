//! The source half: Oracle tables and queries as streams.

mod client;
mod config;
pub mod cursor;

pub use config::{Config, ConfigError, Stream, config_schema};
