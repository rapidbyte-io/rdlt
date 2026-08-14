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

/// Which wire await exceeded the RPC deadline — carried by
/// [`ClientError::Timeout`] so an embedder can tell a connector that
/// never came up (dial, handshake) from one that went silent
/// mid-session (a read frame, a reply that never arrives).
///
/// `#[non_exhaustive]`: a future transport can add awaits of its own —
/// match with a wildcard arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TimedOutOperation {
    /// Establishing the transport to the advertised socket — the
    /// connector accepted the connection but never completed the
    /// HTTP/2 setup.
    Dial,
    /// The handshake reply.
    Handshake,
    /// The next frame of a server-streamed read.
    ReadFrame,
    /// An RPC reply — a unary reply, or the next reply on an open
    /// destination session.
    Reply,
}

impl std::fmt::Display for TimedOutOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            TimedOutOperation::Dial => "transport setup",
            TimedOutOperation::Handshake => "handshake reply",
            TimedOutOperation::ReadFrame => "read frame",
            TimedOutOperation::Reply => "reply",
        })
    }
}

/// Bound one wire await by the session's RPC deadline, elapsing into
/// the typed [`ClientError::Timeout`]. Every await in this crate that
/// waits on the connector goes through here — the deadline bounds the
/// QUIET interval of that one await, never a whole stream: each frame
/// or reply that arrives starts the next await's clock afresh, so a
/// slow-but-flowing connector never trips it while a silent one always
/// does.
pub(crate) async fn with_deadline<F: std::future::Future>(
    deadline: Duration,
    operation: TimedOutOperation,
    future: F,
) -> Result<F::Output, ClientError> {
    tokio::time::timeout(deadline, future)
        .await
        .map_err(|_elapsed| ClientError::Timeout {
            operation,
            deadline,
        })
}

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
    /// config. The connector's own wording rides in `message`, rendered
    /// inert (control characters spelled as escapes — the wire edge's
    /// escape seat).
    #[error("the connector refused the handshake: {message}")]
    Handshake {
        /// The frame's classification, normalized safe-loud: an
        /// `Unspecified` or unknown value arrives as [`Classification::Fatal`].
        classification: Classification,
        /// The connector's refusal — its own wording, with control
        /// characters spelled as escapes.
        message: String,
        /// The frame's wait hint, when the refusal carried one —
        /// clamped to the wire edge's one-minute ceiling (see
        /// `MAX_RETRY_AFTER`).
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
    /// The connector stayed silent past the RPC deadline: the wire is
    /// up but `operation` never arrived. The silent-but-alive twin of
    /// a dead socket's transport error — a connector that binds,
    /// handshakes, then answers nothing must fail typed, never hang
    /// its host (the same law the SIGKILL kill matrix holds the dead
    /// case to).
    #[error(
        "the connector went silent: no {operation} within {deadline:?} — a silent connector \
         fails typed, never hangs its host"
    )]
    Timeout {
        /// Which wire await elapsed.
        operation: TimedOutOperation,
        /// The deadline that elapsed — the requirement's `rpc_deadline`.
        deadline: Duration,
    },
}

impl ClientError {
    /// Build the [`ClientError::Handshake`] arm from a refusal frame —
    /// the one place the wire enum's raw `i32` is normalized for the
    /// handshake path.
    pub(crate) fn handshake_refusal(frame: &proto::ErrorFrame) -> Self {
        ClientError::Handshake {
            classification: normalized_classification(frame.classification),
            message: inert_message(frame),
            retry_after_ms: clamped_retry_after_ms(frame),
        }
    }
}

/// The frame's message rendered inert: control characters spelled as
/// escapes, everything else verbatim (the wire edge's escape seat —
/// see the `sanitize` module's rule). Escaped rather than refused
/// because a message is display text: the connector's real diagnostic
/// should survive its own bad bytes, not vanish behind a refusal.
fn inert_message(frame: &proto::ErrorFrame) -> String {
    crate::sanitize::escape_control_characters(&frame.message).into_owned()
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

/// The ceiling on a connector-supplied `retry_after_ms` wait hint —
/// one minute. The engine honors the hint DIRECTLY as its retry
/// pacing (`rdlt-engine`'s run loop sleeps the hinted duration in
/// place of its own backoff) — so an unclamped rogue hint of
/// `u64::MAX` would park a run for ~584 million years with no typed
/// anything. For scale: the engine's self-synthesized backoff FORMULA
/// caps at 6.4 s (100 ms doubled, at most six doublings), and under
/// the five-attempt run budget the reachable maximum is 1.6 s (the
/// fourth retry's doubling — the formula's cap sits past the attempts
/// that can use it). A minute is far above anything the engine would
/// pace on its own: generous to every honest Retry-After a
/// rate-limited service sends, while a clamped rogue costs at most
/// one minute per attempt across the bounded attempt budget. Clamped
/// HERE, at the wire edge, so no host layer needs to remember to.
const MAX_RETRY_AFTER: Duration = Duration::from_secs(60);

/// The frame's wait hint as the SPI's `retry_after` shape, clamped to
/// [`MAX_RETRY_AFTER`].
fn retry_after(frame: &proto::ErrorFrame) -> Option<Duration> {
    clamped_retry_after_ms(frame).map(Duration::from_millis)
}

/// The frame's raw millisecond hint, clamped to [`MAX_RETRY_AFTER`] —
/// the one clamp both the SPI mappers and the handshake refusal's raw
/// field ride.
fn clamped_retry_after_ms(frame: &proto::ErrorFrame) -> Option<u64> {
    frame
        .retry_after_ms
        .map(|ms| ms.min(MAX_RETRY_AFTER.as_millis() as u64))
}

/// Map a served source's [`proto::ErrorFrame`] back to the SPI error the
/// engine acts on: `TRANSIENT`→`transient`, `RATE_LIMITED`→`rate_limited`
/// (wait hint forwarded), `FATAL`→`fatal` — and `Unspecified`/unknown
/// values →`fatal` too (see [`normalized_classification`]'s safe-loud
/// rationale). The frame's message becomes the cause, rendered inert
/// ([`inert_message`]).
pub(crate) fn source_error_from_frame(frame: &proto::ErrorFrame) -> SourceError {
    let message = inert_message(frame);
    match normalized_classification(frame.classification) {
        Classification::Transient => SourceError::transient(message),
        Classification::RateLimited => SourceError::rate_limited(message, retry_after(frame)),
        _ => SourceError::fatal(message),
    }
}

/// [`source_error_from_frame`]'s destination twin — same mapping, same
/// safe-loud rule, the SPI's [`DestinationError`] constructors.
pub(crate) fn dest_error_from_frame(frame: &proto::ErrorFrame) -> DestinationError {
    let message = inert_message(frame);
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

    /// The wire edge's clamp on the wait hint: a rogue `u64::MAX`
    /// `retry_after_ms` (a ~584-million-year sleep the engine's retry
    /// pacing would honor) arrives as MAX_RETRY_AFTER, through both
    /// mappers and the handshake refusal alike; an honest hint inside
    /// the cap is untouched (the 250 ms pin above).
    #[test]
    fn an_absurd_wait_hint_is_clamped_at_the_wire_edge() {
        let hostile = frame(Classification::RateLimited as i32, Some(u64::MAX));
        match source_error_from_frame(&hostile) {
            SourceError::RateLimited { retry_after, .. } => {
                assert_eq!(retry_after, Some(MAX_RETRY_AFTER));
            }
            other => panic!("expected RateLimited, got {other:?}"),
        }
        match dest_error_from_frame(&hostile) {
            DestinationError::RateLimited { retry_after, .. } => {
                assert_eq!(retry_after, Some(MAX_RETRY_AFTER));
            }
            other => panic!("expected RateLimited, got {other:?}"),
        }
        match ClientError::handshake_refusal(&hostile) {
            ClientError::Handshake { retry_after_ms, .. } => {
                assert_eq!(retry_after_ms, Some(MAX_RETRY_AFTER.as_millis() as u64));
            }
            other => panic!("expected Handshake, got {other:?}"),
        }
    }

    /// A hint AT the cap passes exactly — the clamp bounds, it does
    /// not distort.
    #[test]
    fn a_wait_hint_at_the_cap_is_untouched() {
        let at_cap = frame(
            Classification::RateLimited as i32,
            Some(MAX_RETRY_AFTER.as_millis() as u64),
        );
        match source_error_from_frame(&at_cap) {
            SourceError::RateLimited { retry_after, .. } => {
                assert_eq!(retry_after, Some(MAX_RETRY_AFTER));
            }
            other => panic!("expected RateLimited, got {other:?}"),
        }
    }

    /// The wire edge's escape seat: a frame message carrying control
    /// characters (the OSC 52 clipboard escape, BEL, a forged log
    /// line) renders INERT through every mapper — the bytes arrive as
    /// their spelled-out escapes, never raw. Escaped rather than
    /// refused: an error message is display text, and a connector's
    /// real diagnostic should survive its own bad bytes rather than
    /// vanish behind a refusal.
    #[test]
    fn a_control_character_message_renders_inert_through_every_mapper() {
        let hostile = "\u{1b}]52;c;AAAA\u{7}\nFORGED line";
        let renderings = [
            source_error_from_frame(&proto::ErrorFrame {
                classification: Classification::Fatal as i32,
                message: hostile.to_string(),
                retry_after_ms: None,
            })
            .to_string(),
            dest_error_from_frame(&proto::ErrorFrame {
                classification: Classification::Fatal as i32,
                message: hostile.to_string(),
                retry_after_ms: None,
            })
            .to_string(),
            ClientError::handshake_refusal(&proto::ErrorFrame {
                classification: Classification::Fatal as i32,
                message: hostile.to_string(),
                retry_after_ms: None,
            })
            .to_string(),
        ];
        for rendered in renderings {
            assert!(
                !rendered.contains('\u{1b}')
                    && !rendered.contains('\u{7}')
                    && !rendered.contains('\n'),
                "no raw control byte survives the mapper: {rendered:?}"
            );
            assert!(
                rendered.contains("\\u{1b}]52;c;AAAA\\u{7}\\nFORGED line"),
                "the message survives, spelled inert: {rendered:?}"
            );
        }
    }

    /// An ordinary message — non-ASCII included, which is data, not
    /// control — passes through the mappers byte-identical.
    #[test]
    fn an_ordinary_message_is_untouched_by_the_escape_seat() {
        let error = source_error_from_frame(&frame(Classification::Fatal as i32, None));
        assert_eq!(error.to_string(), "fatal source error: the cause");
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
