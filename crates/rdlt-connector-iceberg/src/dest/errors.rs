//! The ONE error boundary: every `iceberg::Error` is
//! classified HERE into the typed Dest posture — nothing above this module
//! sees library error types. Classification: catalog/storage transport,
//! 5xx, throttling, credential expiry → transient (the engine budget);
//! auth rejection, missing warehouse/namespace/table, schema conflicts,
//! commit-conflict exhaustion → fatal, always naming the subject.

use iceberg::ErrorKind;
use rdlt_connector::DestinationError;

/// A fatal Dest error from any Display subject. Defined ONCE here — the
/// error boundary is the natural home — so the commit, schema, and session
/// modules share one spelling instead of each keeping a private copy.
pub(crate) fn fatal(message: impl std::fmt::Display) -> DestinationError {
    DestinationError::fatal(message.to_string())
}

/// Classify a library error with its subject context (catalog/table/…).
pub(crate) fn classify(context: &str, error: iceberg::Error) -> DestinationError {
    match error.kind() {
        // The system said "not now": service hiccups and unexpected
        // internal errors ride the engine's retry budget. The library
        // surfaces network failures and 5xx as Unexpected — but ALSO
        // 401/403 (its fallback for unmatched status codes), and a
        // rejected credential never heals by retrying. The transport
        // status rides the error's `status` context entry (the only seam
        // the library exposes for it); 401/403 there fail fast, while a
        // 429, a 5xx, or an error carrying NO decodable status (network
        // faults, and bodies that merely quote a status in their text)
        // ride the retry budget.
        ErrorKind::Unexpected => {
            let rendered = error.to_string();
            match status_from_context(&error) {
                Some(401) | Some(403) => DestinationError::fatal(format!(
                    "{context}: authentication/authorization rejected — fix the \
                     credential or its grants: {rendered}"
                )),
                // Throttling gets its own channel so the engine can honor
                // pacing instead of guessing with generic backoff.
                Some(429) => DestinationError::rate_limited(format!("{context}: {rendered}"), None),
                // Any other 4xx is the CLIENT's request being wrong —
                // deterministic; retrying an identical request cannot
                // win, so it must not burn the retry budget.
                Some(status) if (400..500).contains(&status) => {
                    DestinationError::fatal(format!("{context}: {rendered}"))
                }
                // 5xx and undecodable statuses (network faults, bodies
                // merely quoting a status) ride the retry budget.
                _ => DestinationError::transient(format!("{context}: {rendered}")),
            }
        }
        // Optimistic-concurrency conflicts are RETRIED by the commit loop;
        // reaching classification means the loop exhausted its budget —
        // fatal with the conflict context.
        ErrorKind::CatalogCommitConflicts => DestinationError::fatal(format!(
            "{context}: commit conflicts exhausted the bounded retry — a \
             competing writer keeps winning: {error}"
        )),
        // Configuration/data problems are the operator's to fix.
        ErrorKind::DataInvalid
        | ErrorKind::FeatureUnsupported
        | ErrorKind::PreconditionFailed
        | ErrorKind::NamespaceAlreadyExists
        | ErrorKind::TableAlreadyExists
        | ErrorKind::NamespaceNotFound
        | ErrorKind::TableNotFound => DestinationError::fatal(format!("{context}: {error}")),
        // ErrorKind is non_exhaustive upstream: unknown kinds classify
        // FATAL (loud) rather than silently retrying forever.
        _ => DestinationError::fatal(format!("{context}: {error}")),
    }
}

/// Is this the library's commit-conflict signal? (drives the retry loop).
pub(crate) fn is_commit_conflict(error: &iceberg::Error) -> bool {
    matches!(error.kind(), ErrorKind::CatalogCommitConflicts)
}

/// Recover the numeric HTTP status the REST catalog attaches to an error
/// via `with_context("status", status.to_string())`. The library has no
/// public getter for its context vec, so we read it back off the rendered
/// Display: context entries render as `, context: { key: value, key2: v2 }`
/// (a format pinned by iceberg's own Display tests), and a reqwest status
/// renders as `<code> <reason>` (e.g. `401 Unauthorized`). We anchor on
/// the entry KEY (`status: `) inside that block — the status entry is the
/// first the client pushes and its value has no interior comma, so it is
/// one whole `split(", ")` piece — so a status quoted in a response BODY
/// (which renders after the ` => ` message marker, outside the context
/// block) can never be mistaken for the transport's status.
fn status_from_context(error: &iceberg::Error) -> Option<u16> {
    let rendered = error.to_string();
    let context = rendered.split_once(", context: { ")?.1;
    context
        .split(", ")
        .find_map(|entry| entry.strip_prefix("status: "))
        .and_then(|value| value.split_whitespace().next())
        .and_then(|code| code.parse().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classification_matrix() {
        let transient = classify(
            "catalog `c`",
            iceberg::Error::new(ErrorKind::Unexpected, "connection reset"),
        );
        assert!(matches!(transient, DestinationError::Transient { .. }));
        let conflict = classify(
            "table `t`",
            iceberg::Error::new(ErrorKind::CatalogCommitConflicts, "competing snapshot 42"),
        );
        let rendered = conflict.to_string();
        assert!(
            matches!(conflict, DestinationError::Fatal { .. })
                && rendered.contains("table `t`")
                && rendered.contains("competing"),
            "{rendered}"
        );
        let missing = classify(
            "warehouse `w`",
            iceberg::Error::new(ErrorKind::NamespaceNotFound, "raw"),
        );
        assert!(missing.to_string().contains("warehouse `w`"));
    }

    /// The REST client surfaces 401/403 as Unexpected (its fallback kind) —
    /// a rejected credential must FAIL FAST, not ride the retry budget.
    #[test]
    fn auth_rejection_is_fatal_not_transient() {
        for status in ["401 Unauthorized", "403 Forbidden"] {
            let error = iceberg::Error::new(
                ErrorKind::Unexpected,
                "Received response with unexpected status code",
            )
            .with_context("status", status.to_string());
            let classified = classify("catalog `c`", error);
            let rendered = classified.to_string();
            assert!(
                matches!(classified, DestinationError::Fatal { .. })
                    && rendered.contains("authorization rejected"),
                "{status}: {rendered}"
            );
        }
        // Throttling rides its own channel: the engine honors pacing
        // instead of guessing with generic backoff.
        let throttled = iceberg::Error::new(
            ErrorKind::Unexpected,
            "Received response with unexpected status code",
        )
        .with_context("status", "429 Too Many Requests".to_string());
        assert!(matches!(
            classify("catalog `c`", throttled),
            DestinationError::RateLimited { .. }
        ));
        // Other 4xx client errors are deterministic: fatal, never retried.
        for status in ["400 Bad Request", "422 Unprocessable Entity"] {
            let error = iceberg::Error::new(
                ErrorKind::Unexpected,
                "Received response with unexpected status code",
            )
            .with_context("status", status.to_string());
            assert!(
                matches!(
                    classify("catalog `c`", error),
                    DestinationError::Fatal { .. }
                ),
                "{status} must be fatal"
            );
        }
        // 5xx stays transient.
        let error = iceberg::Error::new(
            ErrorKind::Unexpected,
            "Received response with unexpected status code",
        )
        .with_context("status", "503 Service Unavailable".to_string());
        assert!(matches!(
            classify("catalog `c`", error),
            DestinationError::Transient { .. }
        ));
    }

    /// Pin the context parser against the exact seam the REST client uses
    /// (`with_context("status", <code> <reason>)`). If this regresses, the
    /// upstream iceberg Error Display format changed and status_from_context
    /// must be re-derived from the new rendering.
    #[test]
    fn status_parser_reads_the_context_entry() {
        let error = iceberg::Error::new(ErrorKind::Unexpected, "unexpected status code")
            .with_context("status", "401 Unauthorized")
            .with_context("headers", "{ }");
        assert_eq!(
            status_from_context(&error),
            Some(401),
            "the REST catalog attaches the transport status as a `status: <code> <reason>` \
             context entry; a mismatch means iceberg's Error Display format changed"
        );
    }

    /// A rejected credential rides the error's `status` context entry —
    /// NOT a payload that merely echoes "401 Unauthorized" in its body.
    /// An Unexpected error with no transport status must stay transient
    /// (a real 5xx body can quote a client-side 401 without the request
    /// itself being unauthorized); only the context entry is authoritative.
    #[test]
    fn body_text_status_without_context_stays_transient() {
        let error = iceberg::Error::new(
            ErrorKind::Unexpected,
            "upstream service replied 401 Unauthorized in its error body",
        );
        assert!(
            matches!(
                classify("catalog `c`", error),
                DestinationError::Transient { .. }
            ),
            "a status only in the message body must not be read as the transport status"
        );
    }

    #[test]
    fn conflict_detection() {
        assert!(is_commit_conflict(&iceberg::Error::new(
            ErrorKind::CatalogCommitConflicts,
            "x"
        )));
        assert!(!is_commit_conflict(&iceberg::Error::new(
            ErrorKind::Unexpected,
            "x"
        )));
    }
}
