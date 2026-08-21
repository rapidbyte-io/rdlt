//! The embedder-facing error taxonomy.
//!
//! Each variant maps to exactly one operator action. Fully serde-representable
//! (sources are flattened to strings) so platforms can persist and render failures.

use serde::{Deserialize, Serialize};

use crate::id::{StreamName, TableName};
use crate::types::LogicalType;

/// A frozen contract turned a would-be schema delta into a typed failure — before any
/// row of the violating batch was written.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error(
    "schema contract violation on table `{table}`{}: {change} (policy: freeze)",
    column.as_deref().map(|c| format!(" column `{c}`")).unwrap_or_default()
)]
pub struct ContractViolation {
    /// The table whose contract refused the change.
    pub table: TableName,
    /// The column concerned; `None` for a table-level change such as creation.
    pub column: Option<String>,
    /// Human-readable description of the refused change.
    pub change: String,
    /// The refused widening, if the change was a type change.
    pub from: Option<LogicalType>,
    /// The type the change would have moved TO, when both ends are scalars.
    /// Matched by value rather than read out of the rendered message.
    pub to: Option<LogicalType>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, thiserror::Error)]
#[serde(tag = "error", rename_all = "snake_case")]
#[non_exhaustive]
/// The one error taxonomy the whole workspace classifies into.
///
/// The variant IS the diagnosis: it says who can act. `Config` means edit the
/// pipeline, `Internal` means report a bug, `Source`/`Destination` mean look
/// upstream or downstream. The CLI maps these onto distinct exit codes for
/// exactly that reason.
///
/// Match on the VARIANT, never on the rendered text: the prose is for humans and
/// is free to change, while the shape is the contract.
///
/// `#[non_exhaustive]`: match with a wildcard arm.
pub enum Error {
    /// Operator action: fix the pipeline configuration.
    #[error("configuration error: {message}")]
    Config {
        /// What was wrong with the configuration.
        message: String,
    },

    /// Operator action: unfreeze or adjust the schema contract.
    #[error(transparent)]
    Schema(#[from] ContractViolation),

    /// Operator action: check the upstream API/source.
    #[error("source error on stream `{stream}`: {message}")]
    Source {
        /// The stream that failed.
        stream: StreamName,
        /// What the source reported.
        message: String,
        /// `true` for transient/rate-limited failures the engine may retry by
        /// restarting the run from committed state.
        #[serde(default)]
        retryable: bool,
        /// Rate-limit hint carried from `SourceError::RateLimited`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        retry_after_ms: Option<u64>,
    },

    /// Operator action: check the destination/warehouse.
    #[error("destination error: {message}")]
    Destination {
        /// What the destination reported.
        message: String,
        /// True for transient/rate-limited destination failures: the run
        /// driver restarts from committed state instead of aborting.
        #[serde(default)]
        retryable: bool,
        /// Rate-limit hint carried from `DestinationError::RateLimited`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        retry_after_ms: Option<u64>,
    },

    /// Operator action: check local disk / the work directory.
    #[error("WAL error: {message}")]
    Wal {
        /// What failed in the write-ahead log or its directory.
        message: String,
    },

    /// Operator action: check the local filesystem — a document or
    /// artifact the path named could not be read. Distinct from
    /// [`Error::Config`] on purpose: the invocation was well-formed and
    /// the configuration may be perfect, the FILESYSTEM refused, and a
    /// scripting caller treats the two differently.
    #[error("io error: {message}")]
    Io {
        /// What failed and on which path.
        message: String,
    },

    /// An engine invariant broke — a background task panicked, or an internal
    /// contract was violated. Not operator-actionable: it means a bug to report,
    /// not a pipeline/config/destination change. Additive variant; older clients
    /// that never saw it are unaffected (`#[non_exhaustive]`).
    #[error("internal engine error: {message}")]
    Internal {
        /// The invariant that broke. Not operator-actionable.
        message: String,
    },

    /// The run was cancelled; recovery on next run is identical to a crash.
    #[error("run cancelled")]
    Cancelled,
}

impl Error {
    /// A configuration failure: the operator can fix this by editing the pipeline.
    pub fn config(message: impl Into<String>) -> Self {
        Error::Config {
            message: message.into(),
        }
    }

    /// A filesystem refusal: the path could not be read. The message
    /// names the path.
    pub fn io(message: impl Into<String>) -> Self {
        Error::Io {
            message: message.into(),
        }
    }

    /// A permanent destination failure. The run aborts.
    pub fn destination(message: impl std::fmt::Display) -> Self {
        Error::Destination {
            message: message.to_string(),
            retryable: false,
            retry_after_ms: None,
        }
    }

    /// A transient destination failure. The run driver restarts from committed
    /// state rather than aborting, honouring `retry_after` when the server gave one.
    pub fn destination_retryable(
        message: impl std::fmt::Display,
        retry_after: Option<std::time::Duration>,
    ) -> Self {
        Error::Destination {
            message: message.to_string(),
            retryable: true,
            // Saturate rather than truncate an implausibly-long hint.
            retry_after_ms: retry_after.map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX)),
        }
    }

    /// A permanent source failure on one stream. The run aborts.
    pub fn source(stream: StreamName, message: impl std::fmt::Display) -> Self {
        Error::Source {
            stream,
            message: message.to_string(),
            retryable: false,
            retry_after_ms: None,
        }
    }

    /// A transient source failure. The run restarts from committed state,
    /// honouring `retry_after` when the server gave one.
    pub fn source_retryable(
        stream: StreamName,
        message: impl std::fmt::Display,
        retry_after: Option<std::time::Duration>,
    ) -> Self {
        Error::Source {
            stream,
            message: message.to_string(),
            retryable: true,
            // `Duration::as_millis` is u128; saturate rather than truncate an
            // implausibly-long hint into a small wrapped value.
            retry_after_ms: retry_after.map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX)),
        }
    }

    /// A write-ahead-log or work-directory failure: check local disk.
    pub fn wal(message: impl std::fmt::Display) -> Self {
        Error::Wal {
            message: message.to_string(),
        }
    }

    /// An engine invariant broke. This is a bug to report, not a configuration
    /// problem — do NOT use it for anything an operator could fix.
    pub fn internal(message: impl Into<String>) -> Self {
        Error::Internal {
            message: message.into(),
        }
    }
}
