//! Map connector-classified errors onto the embedder taxonomy, preserving
//! retryability for the run-level retry driver.

use rdlt_connector::error::SourceError;
use rdlt_core::error::Error;
use rdlt_core::id::StreamName;

/// Map a connector-classified destination error onto the embedder taxonomy,
/// preserving retryability for the run-level driver — a transient warehouse
/// failure (lock, rate limit, network) restarts the run from committed state
/// exactly like a transient source failure, instead of aborting.
pub(crate) fn classify_dest_error(e: &rdlt_connector::error::DestinationError) -> Error {
    use rdlt_connector::error::DestinationError;
    match e {
        DestinationError::Transient(inner) => {
            Error::destination_retryable(format!("transient: {inner}"), None)
        }
        DestinationError::RateLimited {
            retry_after,
            source,
        } => Error::destination_retryable(format!("rate limited: {source}"), *retry_after),
        DestinationError::Fatal(inner) => Error::destination(format!("fatal: {inner}")),
        other => Error::destination(other.to_string()),
    }
}

/// Map a connector-classified source error onto the embedder taxonomy, preserving
/// retryability for the run-level driver.
pub(crate) fn classify_source_error(stream: StreamName, e: &SourceError) -> Error {
    match e {
        SourceError::Transient(inner) => {
            Error::source_retryable(stream, format!("transient: {inner}"), None)
        }
        SourceError::RateLimited {
            retry_after,
            source,
        } => Error::source_retryable(stream, format!("rate limited: {source}"), *retry_after),
        SourceError::Fatal(inner) => Error::source(stream, format!("fatal: {inner}")),
        other => Error::source(stream, other.to_string()),
    }
}
