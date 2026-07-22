//! The ONE error boundary (contract ID1/ID6): every `iceberg::Error` is
//! classified HERE into the typed Dest posture — nothing above this module
//! sees library error types. Classification: catalog/storage transport,
//! 5xx, throttling, credential expiry → transient (the engine budget);
//! auth rejection, missing warehouse/namespace/table, schema conflicts,
//! commit-conflict exhaustion → fatal, always naming the subject.

use iceberg::ErrorKind;
use rdlt_connector::DestError;

/// Classify a library error with its subject context (catalog/table/…).
pub(crate) fn classify(context: &str, error: iceberg::Error) -> DestError {
    match error.kind() {
        // The system said "not now": service hiccups and unexpected
        // internal errors ride the engine's retry budget. The library
        // surfaces network failures and 5xx as Unexpected.
        ErrorKind::Unexpected => DestError::transient(format!("{context}: {error}")),
        // Optimistic-concurrency conflicts are RETRIED by the commit loop;
        // reaching classification means the loop exhausted its budget —
        // fatal with the conflict context (ID3).
        ErrorKind::CatalogCommitConflicts => DestError::fatal(format!(
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
        | ErrorKind::TableNotFound => DestError::fatal(format!("{context}: {error}")),
        // ErrorKind is non_exhaustive upstream: unknown kinds classify
        // FATAL (loud) rather than silently retrying forever.
        _ => DestError::fatal(format!("{context}: {error}")),
    }
}

/// Is this the library's commit-conflict signal? (drives the retry loop).
pub(crate) fn is_commit_conflict(error: &iceberg::Error) -> bool {
    matches!(error.kind(), ErrorKind::CatalogCommitConflicts)
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
        assert!(matches!(transient, DestError::Transient { .. }));
        let conflict = classify(
            "table `t`",
            iceberg::Error::new(ErrorKind::CatalogCommitConflicts, "competing snapshot 42"),
        );
        let rendered = conflict.to_string();
        assert!(
            matches!(conflict, DestError::Fatal { .. })
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
