//! The Snowflake destination.

mod config;

pub use config::{
    Auth, ConfigError, KeyPair, Password, SnowflakeConfig, TableType, config_schema,
};
