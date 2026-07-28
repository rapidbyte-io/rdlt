//! The object-store recoverability rule.
//!
//! Every connector that talks to an object store has to answer the same
//! question about the same error type — is this worth another attempt? — and
//! the answer must not depend on which connector is asking. It lives here so
//! there is one of it: a second copy would drift, and the drift would show up
//! as one connector burning its retry budget on a certainty while another
//! reported the real cause.

/// Is this store failure worth another attempt?
///
/// An ALLOW-LIST, deliberately. Every variant outside it states a determined
/// fact about the request or the configuration — a missing object, a rejected
/// credential, an unusable path, an unsupported operation, an unknown setting —
/// and retrying one burns the engine's budget on a certainty, then reports
/// transient exhaustion in place of the real cause. It is also the safe default
/// for a `#[non_exhaustive]` upstream enum: a variant added by a future release
/// costs a retry that would not have helped, rather than hiding one that never
/// will.
///
/// Three variants are recoverable:
///
/// - `Generic` wraps transport-level failure.
/// - `AlreadyExists` is what HTTP 409 becomes, and `Precondition` HTTP 412.
///   Those are determined answers only to a CONDITIONAL request — and these
///   connectors issue none. What a plain put, copy or delete gets a 409 for is
///   S3's `OperationAborted` ("a conflicting conditional operation is in
///   progress; try again"), which is a retry-me condition. The store's own retry
///   loop will not cover it: it retries 409 only when `retry_on_conflict` is
///   set, which upstream sets solely for its conditional put.
pub fn is_recoverable(error: &object_store::Error) -> bool {
    matches!(
        error,
        object_store::Error::Generic { .. }
            | object_store::Error::AlreadyExists { .. }
            | object_store::Error::Precondition { .. }
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_failure_is_the_only_open_ended_recoverable_case() {
        assert!(is_recoverable(&object_store::Error::Generic {
            store: "S3",
            source: "connection reset by peer".into(),
        }));
    }

    #[test]
    fn a_determined_answer_never_rides_the_retry_budget() {
        // Retrying one of these burns the budget on a certainty and then
        // reports transient exhaustion, hiding the real cause behind a timeout.
        for deterministic in [
            object_store::Error::NotFound {
                path: "x".into(),
                source: "gone".into(),
            },
            object_store::Error::InvalidPath {
                source: object_store::path::Error::EmptySegment {
                    path: "a//b".into(),
                },
            },
            object_store::Error::NotSupported {
                source: "range write".into(),
            },
            object_store::Error::NotImplemented,
            object_store::Error::UnknownConfigurationKey {
                store: "S3",
                key: "nope".into(),
            },
            object_store::Error::PermissionDenied {
                path: "x".into(),
                source: "denied".into(),
            },
            object_store::Error::Unauthenticated {
                path: "x".into(),
                source: "bad key".into(),
            },
        ] {
            let rendered = deterministic.to_string();
            assert!(
                !is_recoverable(&deterministic),
                "`{rendered}` cannot heal on retry"
            );
        }
    }

    #[test]
    fn a_conflict_on_an_unconditional_request_heals() {
        // 409 and 412 are determined answers only to a CONDITIONAL request, and
        // no connector here issues one — so what reaches this rule is S3's
        // `OperationAborted`, which the store's own retry loop does not cover.
        for conflict in [
            object_store::Error::AlreadyExists {
                path: "x".into(),
                source: "operation aborted".into(),
            },
            object_store::Error::Precondition {
                path: "x".into(),
                source: "conflicting operation".into(),
            },
        ] {
            let rendered = conflict.to_string();
            assert!(
                is_recoverable(&conflict),
                "`{rendered}` heals on retry for an unconditional request"
            );
        }
    }

    /// The rule reads the error's SHAPE, never its rendered text.
    ///
    /// Classification by message is what this exists to keep out: a service or
    /// a library release is free to reword any of these, and a retry policy
    /// keyed on the wording would change with it, silently.
    #[test]
    fn classification_does_not_depend_on_the_message() {
        let odd = object_store::Error::Generic {
            store: "S3",
            source: "".into(),
        };
        assert!(
            is_recoverable(&odd),
            "an empty message classifies the same as a descriptive one"
        );
    }
}
