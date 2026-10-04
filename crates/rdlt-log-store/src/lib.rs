//! Where an rdlt pipeline keeps its write-ahead logs: a local directory, or an S3 bucket reached
//! with credentials only the operator's secret resolvers give.
//!
//! ```
//! use rdlt_log_store::LogStoreConfig;
//!
//! let config = LogStoreConfig::parse(&serde_json::json!({
//!     "s3": {
//!         "bucket": "rdlt-logs",
//!         "prefix": "pipelines/logs",
//!         "region": "eu-west-1",
//!         "access_key_id": "${secret:s3_key_id}",
//!         "secret_access_key": "${file:/run/secrets/s3_secret_key}",
//!     }
//! }))?;
//! assert!(matches!(config, LogStoreConfig::S3(_)));
//! # Ok::<(), rdlt_log_store::LogStoreError>(())
//! ```

#![forbid(unsafe_code)]

mod config;
mod credentials;
mod error;
pub mod limits;
mod s3;

pub use config::{LogStoreConfig, S3Config};
pub use error::{LogStoreError, LogStoreErrorKind};
