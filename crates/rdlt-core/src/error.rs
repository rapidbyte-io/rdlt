//! The embedder-facing error taxonomy (design doc §4.3, embedder-api.md X1–X3).
//!
//! Each variant maps to exactly one operator action. Fully serde-representable
//! (sources are flattened to strings) so platforms can persist and render failures.

use serde::{Deserialize, Serialize};

use crate::ids::{StreamName, TableName};
use crate::types::LogicalType;

/// A frozen contract turned a would-be schema delta into a typed failure — before any
/// row of the violating batch was written.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error(
    "schema contract violation on table `{table}`{}: {change} (policy: freeze)",
    column.as_deref().map(|c| format!(" column `{c}`")).unwrap_or_default()
)]
pub struct ContractViolation {
    pub table: TableName,
    pub column: Option<String>,
    /// Human-readable description of the refused change.
    pub change: String,
    /// The refused widening, if the change was a type change.
    pub from: Option<LogicalType>,
    pub to: Option<LogicalType>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, thiserror::Error)]
#[serde(tag = "error", rename_all = "snake_case")]
#[non_exhaustive]
pub enum RdltError {
    /// Operator action: fix the pipeline configuration.
    #[error("configuration error: {message}")]
    Config { message: String },

    /// Operator action: unfreeze or adjust the schema contract.
    #[error(transparent)]
    Schema(#[from] ContractViolation),

    /// Operator action: check the upstream API/source.
    #[error("source error on stream `{stream}`: {message}")]
    Source { stream: StreamName, message: String },

    /// Operator action: check the destination/warehouse.
    #[error("destination error: {message}")]
    Destination { message: String },

    /// Operator action: check local disk / the work directory.
    #[error("WAL error: {message}")]
    Wal { message: String },

    /// The run was cancelled; recovery on next run is identical to a crash.
    #[error("run cancelled")]
    Cancelled,
}

impl RdltError {
    pub fn config(message: impl Into<String>) -> Self {
        RdltError::Config {
            message: message.into(),
        }
    }

    pub fn destination(message: impl std::fmt::Display) -> Self {
        RdltError::Destination {
            message: message.to_string(),
        }
    }

    pub fn source(stream: StreamName, message: impl std::fmt::Display) -> Self {
        RdltError::Source {
            stream,
            message: message.to_string(),
        }
    }

    pub fn wal(message: impl std::fmt::Display) -> Self {
        RdltError::Wal {
            message: message.to_string(),
        }
    }
}
