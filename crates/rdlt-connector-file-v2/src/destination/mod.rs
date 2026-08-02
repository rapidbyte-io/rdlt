//! The destination half: files as an exactly-once target.

mod config;
mod layout;
mod stage;

pub use config::{Config, ConfigError, DestFormat, config_schema};
pub(crate) use layout::STAGING_DIR;
