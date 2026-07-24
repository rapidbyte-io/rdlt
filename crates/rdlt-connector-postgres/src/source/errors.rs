//! Typed, phase-tagged error surface + SPI classification.
//!
//! The source NEVER retries. It classifies: connection-shaped failures are
//! `Transient` (the engine retries with backoff, and
//! resume-from-committed-cursor makes that double-apply-safe);
//! config/auth/data-shaped failures are `Fatal`.

use rdlt_connector::SourceError;

/// Where in the source lifecycle a failure happened — part of every message
/// (errors name table and phase).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Phase {
    Connect,
    Reflect,
    Copy,
    Decode,
    /// CDC slot/publication lifecycle + feed access.
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

/// Classify a driver error into Transient/Fatal by SQLSTATE class (the shared
/// [`crate::pgerror::is_transient_sqlstate`] rule), carrying the server's full
/// rendered detail either way.
pub(crate) fn classify(
    phase: Phase,
    table: Option<&str>,
    err: &tokio_postgres::Error,
) -> SourceError {
    let detail = crate::pgerror::pg_error_detail(err);
    if crate::pgerror::is_transient_sqlstate(err) {
        transient(phase, table, detail)
    } else {
        // 28 auth, 3D invalid catalog, 42 syntax/undefined object, 22 data
        // exception, 0A unsupported — and anything else server-classified:
        // retrying cannot fix it.
        fatal(phase, table, detail)
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
