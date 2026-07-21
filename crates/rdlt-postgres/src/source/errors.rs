//! Typed, phase-tagged error surface + SPI classification (corrected R6).
//!
//! The source NEVER retries (clause S3). It classifies: connection-shaped
//! failures are `Transient` (the engine retries with backoff, clause E5, and
//! resume-from-committed-cursor makes that double-apply-safe via E6/S1/D1–D4);
//! config/auth/data-shaped failures are `Fatal`.

use rdlt_connector::SourceError;

/// Where in the source lifecycle a failure happened — part of every message
/// (spec FR-008: errors name table and phase).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Phase {
    Connect,
    Reflect,
    Copy,
    Decode,
    /// CDC slot/publication lifecycle + feed access (feature 009, R9).
    Slot,
}

impl std::fmt::Display for Phase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Phase::Connect => "connect",
            Phase::Reflect => "reflect",
            Phase::Copy => "copy",
            Phase::Decode => "decode",
            Phase::Slot => "cdc-slot",
        })
    }
}

#[derive(Debug, thiserror::Error)]
#[error("postgres source, {phase} phase{}: {detail}", table.as_deref().map(|t| format!(" (table `{t}`)")).unwrap_or_default())]
pub(crate) struct PgSourceError {
    pub phase: Phase,
    pub table: Option<String>,
    pub detail: String,
}

fn tagged(phase: Phase, table: Option<&str>, detail: impl std::fmt::Display) -> PgSourceError {
    PgSourceError {
        phase,
        table: table.map(str::to_owned),
        detail: detail.to_string(),
    }
}

/// Render an error WITH its source chain — tokio-postgres's Display for db
/// errors is just "db error"; the actual message lives in the cause.
fn full_chain(err: &tokio_postgres::Error) -> String {
    use std::error::Error as _;
    let mut out = err.to_string();
    let mut source = err.source();
    while let Some(cause) = source {
        out.push_str(": ");
        out.push_str(&cause.to_string());
        source = cause.source();
    }
    out
}

pub(crate) fn fatal(
    phase: Phase,
    table: Option<&str>,
    detail: impl std::fmt::Display,
) -> SourceError {
    SourceError::fatal(tagged(phase, table, detail))
}

pub(crate) fn transient(
    phase: Phase,
    table: Option<&str>,
    detail: impl std::fmt::Display,
) -> SourceError {
    SourceError::transient(tagged(phase, table, detail))
}

/// Classify a driver error per corrected R6. SQLSTATE class decides when
/// present; a missing code means the failure never reached the server (io,
/// closed connection) — transient by definition.
pub(crate) fn classify(
    phase: Phase,
    table: Option<&str>,
    err: &tokio_postgres::Error,
) -> SourceError {
    let transient_shaped = match err.code() {
        // No SQLSTATE: io error / connection closed before a server reply.
        None => true,
        Some(state) => matches!(
            &state.code()[..2],
            // 08 connection exception, 53 insufficient resources,
            // 57 operator intervention (shutdown), 40 tx rollback (serialization).
            "08" | "53" | "57" | "40"
        ),
    };
    let err = full_chain(err);
    if transient_shaped {
        transient(phase, table, err)
    } else {
        // 28 auth, 3D invalid catalog, 42 syntax/undefined object, 22 data
        // exception, 0A unsupported — and anything else server-classified:
        // retrying cannot fix it.
        fatal(phase, table, err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phase_and_table_appear_in_message() {
        let e = tagged(Phase::Copy, Some("orders"), "boom");
        let msg = e.to_string();
        assert!(msg.contains("copy phase"), "{msg}");
        assert!(msg.contains("`orders`"), "{msg}");
    }

    #[test]
    fn phaseless_table_message() {
        let msg = tagged(Phase::Connect, None, "refused").to_string();
        assert!(msg.contains("connect phase:"), "{msg}");
        assert!(!msg.contains("table"), "{msg}");
    }
}
