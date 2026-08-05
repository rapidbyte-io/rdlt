//! The client's error surface, and the frame→SPI mappers.
//!
//! Two directions live here. [`ClientError`] is what THIS crate's own
//! operations (dialing, the handshake, a malformed reply) report to the
//! adapter driving them. The `*_error_from_frame` mappers go the other
//! way: a served connector's classified failure arrives as a
//! [`proto::ErrorFrame`], and the adapters (Tasks 3/4) hand it back to
//! the engine as the SPI's own [`SourceError`]/[`DestinationError`] so
//! the engine's retry machinery never learns the wire exists.

use std::path::PathBuf;
use std::time::Duration;

use rdlt_connector::{DestinationError, SourceError};
use rdlt_connector_protocol::proto;

/// Re-exported wire classification: [`ClientError::Handshake`] carries
/// it, so the client's callers name it through this crate rather than
/// importing the protocol crate for one enum.
pub use rdlt_connector_protocol::proto::Classification;

/// What dialing/handshaking a served connector can report.
///
/// `#[non_exhaustive]`: the client's failure surface can grow (a spawn
/// arm, a TLS arm for the future network binding) without a breaking
/// change — match with a wildcard arm.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ClientError {
    /// Connecting to the connector's Unix domain socket failed.
    #[error("dialing the connector socket at {path}: {source}")]
    Dial {
        /// The socket path that refused.
        path: PathBuf,
        /// The transport-level cause.
        #[source]
        source: tonic::transport::Error,
    },
    /// The connector refused the handshake with a typed [`proto::ErrorFrame`]
    /// — bad role, out-of-range protocol version, undecodable or invalid
    /// config. The connector's own wording rides in `message` verbatim.
    #[error("the connector refused the handshake: {message}")]
    Handshake {
        /// The frame's classification, normalized safe-loud: an
        /// `Unspecified` or unknown value arrives as [`Classification::Fatal`].
        classification: Classification,
        /// The connector's refusal, verbatim.
        message: String,
        /// The frame's wait hint, when the refusal carried one.
        retry_after_ms: Option<u64>,
    },
    /// The connector reported a different identity than the requirement
    /// resolved (D-039-2): refused, never worked around — a wrong binary
    /// at the resolved path is an operator problem, not a fallback.
    #[error(
        "connector identity mismatch: required `{expected}`, the connector reported `{reported}`"
    )]
    IdMismatch {
        /// The id the requirement named.
        expected: String,
        /// What the connector's `HandshakeOk` reported.
        reported: String,
    },
    /// The requirement pinned a version the connector does not report
    /// (D-039-2's version half).
    #[error(
        "connector version mismatch: required `{required}`, the connector reported `{reported}`"
    )]
    VersionMismatch {
        /// The version the requirement pinned.
        required: String,
        /// What the connector's `HandshakeOk` reported.
        reported: String,
    },
    /// The wire answered a shape the protocol does not define — a reply
    /// with no outcome, a `spec_json`/`capabilities_json` payload that
    /// does not decode.
    #[error("connector protocol violation: {0}")]
    Protocol(String),
    /// The RPC layer itself failed: a raw `Status` (the served side's
    /// protocol-state refusals ride this shape too — see the sdk's
    /// Status-vs-ErrorFrame rule) or a broken transport.
    #[error("connector transport: {0}")]
    Transport(#[source] tonic::Status),
}

impl ClientError {
    /// Build the [`ClientError::Handshake`] arm from a refusal frame —
    /// the one place the wire enum's raw `i32` is normalized for the
    /// handshake path.
    pub(crate) fn handshake_refusal(frame: &proto::ErrorFrame) -> Self {
        ClientError::Handshake {
            classification: normalized_classification(frame.classification),
            message: frame.message.clone(),
            retry_after_ms: frame.retry_after_ms,
        }
    }
}

/// Decode a frame's raw classification, failing safe-loud: `Unspecified`
/// (proto3's zero value — what a buggy server that never set the field
/// sends) and any value this build does not know both normalize to
/// `Fatal`. Retrying an unclassified failure could loop a run forever
/// on something permanent; aborting a retryable one merely costs a
/// re-run — so unknown means abort, loudly, with the message intact.
fn normalized_classification(raw: i32) -> Classification {
    match Classification::try_from(raw) {
        Ok(Classification::Unspecified) | Err(_) => Classification::Fatal,
        Ok(classification) => classification,
    }
}

/// The frame's wait hint as the SPI's `retry_after` shape.
fn retry_after(frame: &proto::ErrorFrame) -> Option<Duration> {
    frame.retry_after_ms.map(Duration::from_millis)
}

/// Map a served source's [`proto::ErrorFrame`] back to the SPI error the
/// engine acts on: `TRANSIENT`→`transient`, `RATE_LIMITED`→`rate_limited`
/// (wait hint forwarded), `FATAL`→`fatal` — and `Unspecified`/unknown
/// values →`fatal` too (see [`normalized_classification`]'s safe-loud
/// rationale). The frame's message becomes the cause verbatim.
pub(crate) fn source_error_from_frame(frame: &proto::ErrorFrame) -> SourceError {
    let message = frame.message.clone();
    match normalized_classification(frame.classification) {
        Classification::Transient => SourceError::transient(message),
        Classification::RateLimited => SourceError::rate_limited(message, retry_after(frame)),
        _ => SourceError::fatal(message),
    }
}

/// [`source_error_from_frame`]'s destination twin — same mapping, same
/// safe-loud rule, the SPI's [`DestinationError`] constructors.
#[allow(dead_code)] // Task 4's RemoteBackend is the caller; until it lands only the unit tests below call this.
pub(crate) fn dest_error_from_frame(frame: &proto::ErrorFrame) -> DestinationError {
    let message = frame.message.clone();
    match normalized_classification(frame.classification) {
        Classification::Transient => DestinationError::transient(message),
        Classification::RateLimited => DestinationError::rate_limited(message, retry_after(frame)),
        _ => DestinationError::fatal(message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(classification: i32, retry_after_ms: Option<u64>) -> proto::ErrorFrame {
        proto::ErrorFrame {
            classification,
            message: "the cause".to_string(),
            retry_after_ms,
        }
    }

    /// TRANSIENT maps to the retryable variant, message intact.
    #[test]
    fn a_transient_frame_maps_transient() {
        let error = source_error_from_frame(&frame(Classification::Transient as i32, None));
        assert!(matches!(error, SourceError::Transient(_)), "{error:?}");
        assert!(error.to_string().contains("the cause"));

        let error = dest_error_from_frame(&frame(Classification::Transient as i32, None));
        assert!(matches!(error, DestinationError::Transient(_)), "{error:?}");
        assert!(error.to_string().contains("the cause"));
    }

    /// RATE_LIMITED maps to the paced variant, the wait hint forwarded
    /// in milliseconds — and absent when the frame carried none.
    #[test]
    fn a_rate_limited_frame_forwards_the_wait_hint() {
        let error = source_error_from_frame(&frame(Classification::RateLimited as i32, Some(250)));
        match error {
            SourceError::RateLimited { retry_after, .. } => {
                assert_eq!(retry_after, Some(Duration::from_millis(250)));
            }
            other => panic!("expected RateLimited, got {other:?}"),
        }

        let error = dest_error_from_frame(&frame(Classification::RateLimited as i32, None));
        match error {
            DestinationError::RateLimited { retry_after, .. } => {
                assert_eq!(retry_after, None, "no hint given, none invented");
            }
            other => panic!("expected RateLimited, got {other:?}"),
        }
    }

    /// FATAL maps fatal.
    #[test]
    fn a_fatal_frame_maps_fatal() {
        let error = source_error_from_frame(&frame(Classification::Fatal as i32, None));
        assert!(matches!(error, SourceError::Fatal(_)), "{error:?}");

        let error = dest_error_from_frame(&frame(Classification::Fatal as i32, None));
        assert!(matches!(error, DestinationError::Fatal(_)), "{error:?}");
    }

    /// The safe-loud arm: UNSPECIFIED (a server that never set the
    /// field) maps FATAL, not retried-forever.
    #[test]
    fn an_unspecified_frame_maps_fatal() {
        let error = source_error_from_frame(&frame(Classification::Unspecified as i32, None));
        assert!(matches!(error, SourceError::Fatal(_)), "{error:?}");

        let error = dest_error_from_frame(&frame(Classification::Unspecified as i32, None));
        assert!(matches!(error, DestinationError::Fatal(_)), "{error:?}");
    }

    /// The safe-loud arm's other half: a value this build does not know
    /// (skew against a future protocol) maps FATAL, message intact.
    #[test]
    fn an_unknown_classification_maps_fatal() {
        let error = source_error_from_frame(&frame(42, None));
        assert!(matches!(error, SourceError::Fatal(_)), "{error:?}");
        assert!(error.to_string().contains("the cause"));

        let error = dest_error_from_frame(&frame(42, None));
        assert!(matches!(error, DestinationError::Fatal(_)), "{error:?}");
    }

    /// The handshake-refusal constructor normalizes the same way and
    /// carries the frame's fields through untouched.
    #[test]
    fn a_handshake_refusal_normalizes_safe_loud() {
        match ClientError::handshake_refusal(&frame(Classification::RateLimited as i32, Some(7))) {
            ClientError::Handshake {
                classification,
                message,
                retry_after_ms,
            } => {
                assert_eq!(classification, Classification::RateLimited);
                assert_eq!(message, "the cause");
                assert_eq!(retry_after_ms, Some(7));
            }
            other => panic!("expected Handshake, got {other:?}"),
        }

        match ClientError::handshake_refusal(&frame(Classification::Unspecified as i32, None)) {
            ClientError::Handshake { classification, .. } => {
                assert_eq!(classification, Classification::Fatal, "safe-loud");
            }
            other => panic!("expected Handshake, got {other:?}"),
        }
    }
}
