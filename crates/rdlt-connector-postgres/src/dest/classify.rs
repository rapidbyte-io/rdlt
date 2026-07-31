//! Driver-error rendering and classification: the server message + SQLSTATE
//! always reach the operator, and retryability is decided in one place.

use rdlt_connector::DestinationError;

/// Render a driver error with its server message + SQLSTATE — the shared
/// rendering both connectors use (tokio-postgres's own Display for a db error
/// is just "db error"; non-db errors render their full source chain).
pub(crate) fn describe(e: &tokio_postgres::Error) -> String {
    crate::driver_error::detail(e)
}

pub(crate) fn transient(e: tokio_postgres::Error) -> DestinationError {
    DestinationError::transient(describe(&e))
}

/// Statement-error classification shared by the COPY write path AND table DDL:
/// data-shaped SQLSTATE classes (22 data exception, 23 integrity, 42
/// syntax/access) are PERMANENT — a poisoned batch or an unwinnable 42xxx DDL
/// statement must not burn the engine's retry budget on retries that cannot
/// win. Everything else stays transient (connection-shaped).
pub(crate) fn classify_stmt(e: tokio_postgres::Error) -> DestinationError {
    match e.as_db_error() {
        Some(db) if crate::driver_error::is_permanent_statement_sqlstate(db.code().code()) => {
            DestinationError::fatal(describe(&e))
        }
        _ => DestinationError::transient(describe(&e)),
    }
}

pub(crate) fn fatal(e: impl std::fmt::Display) -> DestinationError {
    DestinationError::fatal(e.to_string())
}
