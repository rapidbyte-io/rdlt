//! Shared tokio-postgres error rendering + SQLSTATE classification for BOTH
//! Postgres connectors (source and destination) and the TLS connect path.
//!
//! Two facts about the driver drive this module. First, tokio-postgres's
//! `Display` for a server error is just "db error" — the real message and the
//! SQLSTATE live in the `DbError`, reachable only through `as_db_error()`, so
//! every rendered detail reaches through it. Second, the SQLSTATE *class* (or
//! its absence) decides retry-worthiness: connection/resource/shutdown/
//! serialization classes are transient (retrying may succeed), everything
//! server-classified is permanent. Both connectors classify identically, so
//! the rule lives here once and cannot drift.

/// Render a driver error with its full meaning. A server (db) error yields its
/// message + SQLSTATE, plus the statement's column context when the server
/// supplied one (the COPY `where_` field). A non-db error renders its whole
/// source chain — `Display` alone drops the cause.
pub(crate) fn pg_error_detail(err: &tokio_postgres::Error) -> String {
    use std::error::Error as _;
    if let Some(db) = err.as_db_error() {
        let context = db.where_().map(|w| format!(" [{w}]")).unwrap_or_default();
        return format!(
            "{err}: {} (SQLSTATE {}){context}",
            db.message(),
            db.code().code()
        );
    }
    let mut rendered = err.to_string();
    let mut source = err.source();
    while let Some(cause) = source {
        rendered.push_str(": ");
        rendered.push_str(&cause.to_string());
        source = cause.source();
    }
    rendered
}

/// Whether a driver error is transient — retrying MAY succeed. A missing
/// SQLSTATE means the failure never reached the server (io error, connection
/// closed before a reply) and is transient by definition. Otherwise the
/// SQLSTATE class decides: 08 connection exception, 53 insufficient resources,
/// 57 operator intervention (shutdown), 40 transaction rollback
/// (serialization). Every other class is server-classified and permanent —
/// retrying an auth, syntax, or data error cannot fix it.
pub(crate) fn is_transient_sqlstate(err: &tokio_postgres::Error) -> bool {
    match err.code() {
        None => true,
        Some(state) => matches!(&state.code()[..2], "08" | "53" | "57" | "40"),
    }
}
