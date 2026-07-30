//! Map connector-classified errors onto the embedder taxonomy, preserving
//! retryability for the run-level retry driver.

use rdlt_connector::SourceError;
use rdlt_core::{RdltError, StreamName};

/// Map a connector-classified destination error onto the embedder taxonomy,
/// preserving retryability for the run-level driver — a transient warehouse
/// failure (lock, rate limit, network) restarts the run from committed state
/// exactly like a transient source failure, instead of aborting.
pub(crate) fn classify_dest_error(e: &rdlt_connector::DestinationError) -> RdltError {
    use rdlt_connector::DestinationError;
    match e {
        DestinationError::Transient(inner) => {
            RdltError::destination_retryable(format!("transient: {inner}"), None)
        }
        DestinationError::RateLimited {
            retry_after,
            source,
        } => RdltError::destination_retryable(format!("rate limited: {source}"), *retry_after),
        DestinationError::Fatal(inner) => RdltError::destination(format!("fatal: {inner}")),
        other => RdltError::destination(other.to_string()),
    }
}

/// Map a connector-classified source error onto the embedder taxonomy, preserving
/// retryability for the run-level driver.
pub(crate) fn classify_source_error(stream: StreamName, e: &SourceError) -> RdltError {
    match e {
        SourceError::Transient(inner) => {
            RdltError::source_retryable(stream, format!("transient: {inner}"), None)
        }
        SourceError::RateLimited {
            retry_after,
            source,
        } => RdltError::source_retryable(stream, format!("rate limited: {source}"), *retry_after),
        SourceError::Fatal(inner) => RdltError::source(stream, format!("fatal: {inner}")),
        other => RdltError::source(stream, other.to_string()),
    }
}
