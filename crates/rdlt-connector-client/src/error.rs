//! What this crate's own operations report, and the one mapping from a
//! connector's classified wire failure back to the SPI's own errors —
//! so the engine's retry machinery never learns the wire exists.

use std::path::PathBuf;
use std::time::Duration;

use rdlt_connector::error::{DestinationError, SourceError};
use rdlt_connector_protocol::proto;

use crate::{gate, wire};

/// Re-exported wire classification: [`Error::Handshake`] carries it,
/// so the client's callers name it through this crate rather than
/// importing the protocol crate for one enum.
pub use rdlt_connector_protocol::proto::Classification;

/// What dialing/handshaking a served connector can report.
///
/// `#[non_exhaustive]`: the client's failure surface can grow (a spawn
/// arm, a TLS arm for the future network binding) without a breaking
/// change — match with a wildcard arm.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Connecting to the connector's Unix domain socket failed.
    #[error("dialing the connector socket at {path:?}: {source}")]
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
    /// inert and bounded (control characters spelled as escapes, the
    /// render capped — the wire edge's escape seat).
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
        /// `gate::MAX_RETRY_AFTER`).
        retry_after_ms: Option<u64>,
    },
    /// The connector reported a different identity than the requirement
    /// resolved: refused, never worked around — a wrong binary at the
    /// resolved path is an operator problem, not a fallback.
    #[error(
        "connector identity mismatch: required `{expected}`, the connector reported `{reported}`"
    )]
    IdentityMismatch {
        /// The id the requirement named.
        expected: String,
        /// What the connector's `HandshakeOk` reported.
        reported: String,
    },
    /// The requirement pinned a version the connector does not report.
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
    /// Status-vs-ErrorFrame rule) or a broken transport. The status
    /// text is connector-authored display text: rendered escaped and
    /// bounded like a frame message.
    #[error("connector transport: code: {:?}, message: {}", .0.code(), gate::render_message(.0.message()))]
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
        operation: wire::Operation,
        /// The deadline that elapsed — the requirement's `rpc_deadline`.
        deadline: Duration,
    },
}

impl Error {
    /// Build the [`Error::Handshake`] arm from a refusal frame — the
    /// one place the wire enum's raw `i32` is normalized for the
    /// handshake path. The message is rendered inert (control
    /// characters spelled as escapes) and bounded rather than refused,
    /// because a message is display text: the connector's real
    /// diagnostic should survive its own bad bytes, not vanish behind
    /// a refusal — and a frame-sized one must not become a firehose.
    pub(crate) fn handshake_refusal(frame: &proto::ErrorFrame) -> Self {
        Error::Handshake {
            classification: gate::classification(frame.classification),
            message: gate::render_message(&frame.message),
            retry_after_ms: gate::retry_after(frame).map(|d| d.as_millis() as u64),
        }
    }
}

/// The one frame→SPI mapping, written once for both halves: TRANSIENT
/// maps retryable, RATE_LIMITED forwards the clamped wait hint, FATAL
/// — and, safe-loud, anything unspecified or unknown — aborts with the
/// message rendered inert.
pub(crate) trait FromWire: Sized {
    fn transient(message: String) -> Self;
    fn rate_limited(message: String, retry_after: Option<Duration>) -> Self;
    fn fatal(message: String) -> Self;
    /// A fatal carrying a structured client-side cause, so the
    /// rendering names the transport or protocol failure.
    fn fatal_error(error: Error) -> Self;

    fn from_frame(frame: &proto::ErrorFrame) -> Self {
        let message = gate::render_message(&frame.message);
        match gate::classification(frame.classification) {
            Classification::Transient => Self::transient(message),
            Classification::RateLimited => Self::rate_limited(message, gate::retry_after(frame)),
            _ => Self::fatal(message),
        }
    }
    /// A transport failure is fatal at this seam: restarting a died
    /// connector is the provider layer's job (`rdlt-runtime` supervises
    /// the process), never a reclassification here.
    fn transport(status: tonic::Status) -> Self {
        Self::fatal_error(Error::Transport(status))
    }
    fn protocol(message: String) -> Self {
        Self::fatal_error(Error::Protocol(message))
    }
}

impl FromWire for SourceError {
    fn transient(message: String) -> Self {
        SourceError::transient(message)
    }
    fn rate_limited(message: String, retry_after: Option<Duration>) -> Self {
        SourceError::rate_limited(message, retry_after)
    }
    fn fatal(message: String) -> Self {
        SourceError::fatal(message)
    }
    fn fatal_error(error: Error) -> Self {
        SourceError::fatal(error)
    }
}

impl FromWire for DestinationError {
    fn transient(message: String) -> Self {
        DestinationError::transient(message)
    }
    fn rate_limited(message: String, retry_after: Option<Duration>) -> Self {
        DestinationError::rate_limited(message, retry_after)
    }
    fn fatal(message: String) -> Self {
        DestinationError::fatal(message)
    }
    fn fatal_error(error: Error) -> Self {
        DestinationError::fatal(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gate;

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
        let error = SourceError::from_frame(&frame(Classification::Transient as i32, None));
        assert!(matches!(error, SourceError::Transient(_)), "{error:?}");
        assert!(error.to_string().contains("the cause"));

        let error = DestinationError::from_frame(&frame(Classification::Transient as i32, None));
        assert!(matches!(error, DestinationError::Transient(_)), "{error:?}");
        assert!(error.to_string().contains("the cause"));
    }

    /// RATE_LIMITED maps to the paced variant, the wait hint forwarded
    /// in milliseconds — and absent when the frame carried none.
    #[test]
    fn a_rate_limited_frame_forwards_the_wait_hint() {
        let error = SourceError::from_frame(&frame(Classification::RateLimited as i32, Some(250)));
        match error {
            SourceError::RateLimited { retry_after, .. } => {
                assert_eq!(retry_after, Some(Duration::from_millis(250)));
            }
            other => panic!("expected RateLimited, got {other:?}"),
        }

        let error = DestinationError::from_frame(&frame(Classification::RateLimited as i32, None));
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
        let error = SourceError::from_frame(&frame(Classification::Fatal as i32, None));
        assert!(matches!(error, SourceError::Fatal(_)), "{error:?}");

        let error = DestinationError::from_frame(&frame(Classification::Fatal as i32, None));
        assert!(matches!(error, DestinationError::Fatal(_)), "{error:?}");
    }

    /// The safe-loud arm: UNSPECIFIED (a server that never set the
    /// field) maps FATAL, not retried-forever.
    #[test]
    fn an_unspecified_frame_maps_fatal() {
        let error = SourceError::from_frame(&frame(Classification::Unspecified as i32, None));
        assert!(matches!(error, SourceError::Fatal(_)), "{error:?}");

        let error = DestinationError::from_frame(&frame(Classification::Unspecified as i32, None));
        assert!(matches!(error, DestinationError::Fatal(_)), "{error:?}");
    }

    /// The safe-loud arm's other half: a value this build does not know
    /// (skew against a future protocol) maps FATAL, message intact.
    #[test]
    fn an_unknown_classification_maps_fatal() {
        let error = SourceError::from_frame(&frame(42, None));
        assert!(matches!(error, SourceError::Fatal(_)), "{error:?}");
        assert!(error.to_string().contains("the cause"));

        let error = DestinationError::from_frame(&frame(42, None));
        assert!(matches!(error, DestinationError::Fatal(_)), "{error:?}");
    }

    /// The wire edge's clamp on the wait hint: a rogue `u64::MAX`
    /// `retry_after_ms` (a ~584-million-year sleep the engine's retry
    /// pacing would honor) arrives as the cap, through both halves of
    /// the mapping and the handshake refusal alike; an honest hint
    /// inside the cap is untouched (the 250 ms pin above).
    #[test]
    fn an_absurd_wait_hint_is_clamped_at_the_wire_edge() {
        let hostile = frame(Classification::RateLimited as i32, Some(u64::MAX));
        match SourceError::from_frame(&hostile) {
            SourceError::RateLimited { retry_after, .. } => {
                assert_eq!(retry_after, Some(gate::MAX_RETRY_AFTER));
            }
            other => panic!("expected RateLimited, got {other:?}"),
        }
        match DestinationError::from_frame(&hostile) {
            DestinationError::RateLimited { retry_after, .. } => {
                assert_eq!(retry_after, Some(gate::MAX_RETRY_AFTER));
            }
            other => panic!("expected RateLimited, got {other:?}"),
        }
        match Error::handshake_refusal(&hostile) {
            Error::Handshake { retry_after_ms, .. } => {
                assert_eq!(
                    retry_after_ms,
                    Some(gate::MAX_RETRY_AFTER.as_millis() as u64)
                );
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
            Some(gate::MAX_RETRY_AFTER.as_millis() as u64),
        );
        match SourceError::from_frame(&at_cap) {
            SourceError::RateLimited { retry_after, .. } => {
                assert_eq!(retry_after, Some(gate::MAX_RETRY_AFTER));
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
            SourceError::from_frame(&proto::ErrorFrame {
                classification: Classification::Fatal as i32,
                message: hostile.to_string(),
                retry_after_ms: None,
            })
            .to_string(),
            DestinationError::from_frame(&proto::ErrorFrame {
                classification: Classification::Fatal as i32,
                message: hostile.to_string(),
                retry_after_ms: None,
            })
            .to_string(),
            Error::handshake_refusal(&proto::ErrorFrame {
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

    /// The escape seat is BOUNDED: a frame message as large as the wire
    /// allows, made of one-byte control characters that escape to six,
    /// renders to a capped prefix naming the true length — never a
    /// string several times the frame's size. Every mapper.
    #[test]
    fn an_oversized_control_byte_message_renders_bounded_and_names_its_length() {
        let hostile = "\u{7}".repeat(4 << 20);
        let renderings = [
            SourceError::from_frame(&proto::ErrorFrame {
                classification: Classification::Fatal as i32,
                message: hostile.clone(),
                retry_after_ms: None,
            })
            .to_string(),
            DestinationError::from_frame(&proto::ErrorFrame {
                classification: Classification::Fatal as i32,
                message: hostile.clone(),
                retry_after_ms: None,
            })
            .to_string(),
            Error::handshake_refusal(&proto::ErrorFrame {
                classification: Classification::Fatal as i32,
                message: hostile.clone(),
                retry_after_ms: None,
            })
            .to_string(),
        ];
        for rendered in renderings {
            assert!(
                rendered.len() <= gate::MESSAGE_RENDER_CAP + 128,
                "the render is bounded by the cap plus its envelope: {} bytes",
                rendered.len()
            );
            assert!(
                rendered.contains(&format!("truncated; {} source bytes", 4 << 20)),
                "the render names the true length: {rendered:?}"
            );
            assert!(
                !rendered.contains('\u{7}') && rendered.contains("\\u{7}"),
                "the kept prefix is still escaped: {rendered:?}"
            );
        }
    }

    /// The Transport arm renders a Status's text through the same
    /// escape and bound: the Hangul fillers (blank glyphs Debug quoting
    /// leaves raw) and control bytes are spelled out, and an oversized
    /// status message renders bounded.
    #[test]
    fn a_transport_status_text_renders_escaped_and_bounded() {
        let error =
            SourceError::transport(tonic::Status::internal("\u{3164}filler\u{1b}[31mFORGED"));
        let rendered = error.to_string();
        assert!(
            rendered.starts_with("fatal source error: connector transport: "),
            "{rendered}"
        );
        assert!(
            !rendered.contains('\u{3164}') && !rendered.contains('\u{1b}'),
            "no raw filler or control byte survives: {rendered:?}"
        );
        assert!(
            rendered.contains("\\u{3164}filler\\u{1b}[31mFORGED"),
            "the text survives, spelled inert: {rendered:?}"
        );

        let huge = DestinationError::transport(tonic::Status::internal("x".repeat(1 << 20)));
        let rendered = huge.to_string();
        assert!(
            rendered.len() <= gate::MESSAGE_RENDER_CAP + 128,
            "a huge status message renders bounded: {} bytes",
            rendered.len()
        );
        assert!(
            rendered.contains(&format!("truncated; {} source bytes", 1 << 20)),
            "and names the true length: {}",
            &rendered[..200]
        );
    }

    /// An ordinary message — non-ASCII included, which is data, not
    /// control — passes through the mapping byte-identical.
    #[test]
    fn an_ordinary_message_is_untouched_by_the_escape_seat() {
        let error = SourceError::from_frame(&frame(Classification::Fatal as i32, None));
        assert_eq!(error.to_string(), "fatal source error: the cause");
    }

    /// The handshake-refusal constructor normalizes the same way and
    /// carries the frame's fields through untouched.
    #[test]
    fn a_handshake_refusal_normalizes_safe_loud() {
        match Error::handshake_refusal(&frame(Classification::RateLimited as i32, Some(7))) {
            Error::Handshake {
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

        match Error::handshake_refusal(&frame(Classification::Unspecified as i32, None)) {
            Error::Handshake { classification, .. } => {
                assert_eq!(classification, Classification::Fatal, "safe-loud");
            }
            other => panic!("expected Handshake, got {other:?}"),
        }
    }
}
