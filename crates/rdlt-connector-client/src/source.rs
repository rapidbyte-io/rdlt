//! [`Source`] — the SPI read seam over the wire: an SPI
//! [`rdlt_connector::Source`] whose every method is an RPC against a
//! served connector, so an engine (or any embedder holding a `dyn
//! Source`) drives a spawned connector without learning the wire
//! exists.
//!
//! The mapping is one-to-one and stateless past the handshake:
//! `spec()` answers from the handshake's cached document (no RPC),
//! `check()`/`streams()` are their unary RPCs with error frames mapped
//! back through `source_error_from_frame`, and `read()` forwards the
//! server-streamed frames into the request's own byte channel — the
//! wire adds transport, never semantics.

use std::path::Path;

use async_trait::async_trait;
use bytes::Bytes;
use rdlt_connector::core::Cursor;
use rdlt_connector::{ConnectorSpec, ReadRequest, RecordBatch, SourceError, StreamSpec};
use rdlt_connector_protocol::proto::{self, check_reply, read_frame, streams_reply};
use tonic::transport::Channel;

use crate::dial::{connector_client, dial, source_client};
use crate::error::{ClientError, source_error_from_frame};
use crate::handshake::{ConnectorRequirement, HandshakeOutcome, Role, handshake};

/// An SPI [`rdlt_connector::Source`] over the wire: the dialed channel
/// plus the handshake's cached spec. Constructed only through
/// [`Source::connect`] — there is no way to hold one whose identity was
/// not verified (D-039-2).
#[derive(Debug)]
pub struct Source {
    channel: Channel,
    spec: ConnectorSpec,
}

impl Source {
    /// Dial `socket_path` (the engine budget paces the wire — see
    /// [`dial`]) and run the [`Role::Source`] handshake, verifying the
    /// connector against `expected`. Returns the adapter AND the full
    /// [`HandshakeOutcome`] so a caller reads what the handshake
    /// established (state-format versions, the negotiated protocol)
    /// without a second RPC.
    pub async fn connect(
        socket_path: &Path,
        engine_budget_bytes: u64,
        config: &serde_json::Value,
        expected: &ConnectorRequirement,
    ) -> Result<(Source, HandshakeOutcome), ClientError> {
        let channel = dial(socket_path, engine_budget_bytes).await?;
        let outcome = handshake(&channel, Role::Source, config, expected).await?;
        Ok((
            Source {
                channel,
                spec: outcome.spec.clone(),
            },
            outcome,
        ))
    }
}

/// A transport-level RPC failure at the SPI seam, classified FATAL —
/// the same safe-loud posture as the frame mappers' unknown arm: a
/// broken transport gives this client no classification to trust, and
/// retrying an unclassified failure risks looping a run forever on
/// something permanent, while aborting a retryable one costs a re-run.
/// The [`ClientError`] rides inside as the cause, so the rendering
/// names the transport. Restarting a connector whose process died is
/// the provider layer's job (`rdlt-runtime`, which supervises the
/// process) — never a reclassification here.
fn transport_fatal(status: tonic::Status) -> SourceError {
    SourceError::fatal(ClientError::Transport(status))
}

/// A wire shape the protocol does not define (a reply with no outcome,
/// an undecodable payload) — FATAL for the same safe-loud reason as
/// [`transport_fatal`], carried as [`ClientError::Protocol`].
fn protocol_fatal(message: String) -> SourceError {
    SourceError::fatal(ClientError::Protocol(message))
}

/// Refuse a DECLARED stream name carrying control characters (C0
/// including newline/tab/DEL, and C1) — the wire edge's half of the
/// terminal-injection defense: a stream name travels into events,
/// tracing spans, and the CLI's lines, and control bytes in it are how
/// a hostile connector forges log lines or drives escape sequences
/// through an operator's terminal. Refused HERE, where third-party
/// bytes become host vocabulary, deliberately not in
/// `StreamName::new` — the core type stays free-form for hosts by its
/// own documented contract, and in-process embedders name their own
/// streams. The refusal renders the name in its `{:?}` escaped form,
/// so the message itself cannot carry the very bytes it refuses.
fn refuse_control_characters_in_name(spec: &StreamSpec) -> Result<(), SourceError> {
    if spec.name.as_str().chars().any(char::is_control) {
        return Err(SourceError::fatal(format!(
            "the connector declared a stream named {:?} — control characters in a \
             stream name are refused at the wire boundary",
            spec.name.as_str()
        )));
    }
    Ok(())
}

/// One `arrow_ipc` read frame's exactly-one record batch — the CLIENT
/// seat of the proto's one-batch rule (the field's own doc): `Read` is
/// server-streamed, so the refusal seat sits here — a conforming client
/// refuses a frame carrying a second batch rather than silently taking
/// the first (the destination direction measured that silence as row
/// loss, 038 T5 F3; this is its read-direction mirror).
///
/// The spelling `read frame violated the one-batch rule` is frozen;
/// the underlying arrow cause is appended where one exists (unreadable
/// stream, corrupt message). Zero-batch and second-batch violations
/// render the bare rule — no arrow cause exists for either, and
/// inventing sub-spellings would put unfrozen text in a pinned surface;
/// the serving side's own encoder is where the two are told apart.
fn decode_one_batch(bytes: &[u8]) -> Result<RecordBatch, SourceError> {
    const REFUSAL: &str = "read frame violated the one-batch rule";

    let mut reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .map_err(|error| SourceError::fatal(format!("{REFUSAL}: {error}")))?;
    let first = match reader.next() {
        Some(Ok(batch)) => batch,
        Some(Err(error)) => return Err(SourceError::fatal(format!("{REFUSAL}: {error}"))),
        None => return Err(SourceError::fatal(REFUSAL)),
    };
    match reader.next() {
        None => Ok(first),
        Some(Ok(_)) => Err(SourceError::fatal(REFUSAL)),
        Some(Err(error)) => Err(SourceError::fatal(format!("{REFUSAL}: {error}"))),
    }
}

#[async_trait]
impl rdlt_connector::Source for Source {
    /// The handshake's cached document — no RPC: the spec was verified
    /// and decoded once at [`Source::connect`], and a connector's
    /// self-description does not change mid-session.
    fn spec(&self) -> ConnectorSpec {
        self.spec.clone()
    }

    async fn check(&self) -> Result<(), SourceError> {
        let mut client = connector_client(self.channel.clone());
        let reply = client
            .check(proto::CheckRequest {})
            .await
            .map_err(transport_fatal)?
            .into_inner();
        match reply.outcome {
            Some(check_reply::Outcome::Ok(_)) => Ok(()),
            Some(check_reply::Outcome::Error(frame)) => Err(source_error_from_frame(&frame)),
            None => Err(protocol_fatal(
                "the check reply carried no outcome".to_string(),
            )),
        }
    }

    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        let mut client = source_client(self.channel.clone());
        let reply = client
            .streams(proto::StreamsRequest {})
            .await
            .map_err(transport_fatal)?
            .into_inner();
        match reply.outcome {
            Some(streams_reply::Outcome::Ok(list)) => list
                .stream_spec_json
                .iter()
                .map(|bytes| {
                    let spec = serde_json::from_slice::<StreamSpec>(bytes).map_err(|error| {
                        protocol_fatal(format!(
                            "undecodable stream_spec_json in the streams reply: {error}"
                        ))
                    })?;
                    refuse_control_characters_in_name(&spec)?;
                    Ok(spec)
                })
                .collect(),
            Some(streams_reply::Outcome::Error(frame)) => Err(source_error_from_frame(&frame)),
            None => Err(protocol_fatal(
                "the streams reply carried no outcome".to_string(),
            )),
        }
    }

    async fn read(&self, request: ReadRequest) -> Result<(), SourceError> {
        // `..` because `ReadRequest` is `#[non_exhaustive]`: a field a
        // future SPI adds is deliberately ignored here until this
        // adapter is taught to carry it.
        let ReadRequest {
            stream,
            since,
            mut out,
            ..
        } = request;

        let wire_request = proto::ReadRequest {
            stream_spec_json: serde_json::to_vec(&stream)
                .expect("a StreamSpec serializes to JSON infallibly"),
            since_cursor_json: since.as_ref().map(|cursor| {
                serde_json::to_vec(cursor.as_value())
                    .expect("a Cursor's value serializes to JSON infallibly")
            }),
        };

        let mut client = source_client(self.channel.clone());
        let mut frames = client
            .read(wire_request)
            .await
            .map_err(transport_fatal)?
            .into_inner();

        loop {
            let frame = match frames.message().await {
                Ok(Some(frame)) => frame,
                // Clean end of stream: the served read returned Ok and
                // every frame was forwarded.
                Ok(None) => return Ok(()),
                Err(status) => return Err(transport_fatal(status)),
            };
            let pushed = match frame.frame {
                Some(read_frame::Frame::RawJson(bytes)) => out.raw_json(Bytes::from(bytes)).await,
                Some(read_frame::Frame::ArrowIpc(bytes)) => {
                    out.arrow(decode_one_batch(&bytes)?).await
                }
                Some(read_frame::Frame::CheckpointCursorJson(bytes)) => {
                    let value: serde_json::Value =
                        serde_json::from_slice(&bytes).map_err(|error| {
                            protocol_fatal(format!(
                                "undecodable checkpoint_cursor_json in a read frame: {error}"
                            ))
                        })?;
                    out.checkpoint(Cursor::new(value)).await
                }
                // Terminal by the proto's own contract: the frame IS
                // the served read's classified failure, and nothing
                // follows it.
                Some(read_frame::Frame::Error(frame)) => {
                    return Err(source_error_from_frame(&frame));
                }
                None => {
                    return Err(protocol_fatal(
                        "a read frame carried no payload".to_string(),
                    ));
                }
            };
            if pushed.is_err() {
                // `ChannelClosed` = the HOST hung up on this read
                // (cancellation, or a failure downstream) — the SPI's
                // closed-channel-is-cancellation contract: return
                // `Ok(())` promptly, never escalate. Returning drops
                // `frames`, which resets the RPC; the server's
                // forwarding loop observes the reset at its next frame
                // send and closes ITS side's SPI channel (both halves —
                // queue and byte-budget semaphore), so the served
                // connector's next push observes `Break`. The caveat
                // rides the whole chain unchanged from in-process:
                // cancellation is OBSERVED AT THE NEXT PUSH at every
                // seat — a connector between pushes (a slow query, a
                // long poll) does not learn of it until it pushes
                // again.
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod name_boundary_tests {
    //! The wire edge's control-character gate on declared stream
    //! names, pinned directly on the helper the streams decode runs
    //! every spec through.

    use super::*;
    use rdlt_connector::StreamSpec;

    /// An OSC-52-shaped name (the clipboard-write escape) refuses
    /// fatal, and the refusal's own rendering is inert — the escaped
    /// `{:?}` form, no raw ESC byte.
    #[test]
    fn a_control_character_name_refuses_with_an_inert_message() {
        let spec = StreamSpec::new("\u{1b}]52;c;AAAA\u{7}");
        let error = refuse_control_characters_in_name(&spec)
            .expect_err("control characters in a declared name must refuse");
        assert!(matches!(error, SourceError::Fatal(_)), "{error:?}");
        let rendered = error.to_string();
        assert!(
            !rendered.contains('\u{1b}') && !rendered.contains('\u{7}'),
            "the refusal must not itself carry the bytes it refuses: {rendered:?}"
        );
        assert!(
            rendered.contains("refused at the wire boundary"),
            "the refusal names the gate: {rendered}"
        );
    }

    /// A newline is a control character too — log-forging material in
    /// a name — and refuses like the rest.
    #[test]
    fn a_newline_in_a_name_refuses() {
        let spec = StreamSpec::new("orders\nFORGED LINE");
        assert!(refuse_control_characters_in_name(&spec).is_err());
    }

    /// Ordinary names — including non-ASCII text, which is data, not
    /// control — pass untouched.
    #[test]
    fn ordinary_names_pass() {
        for name in ["orders", "Événements", "orders-v2.daily"] {
            let spec = StreamSpec::new(name);
            assert!(
                refuse_control_characters_in_name(&spec).is_ok(),
                "`{name}` must pass"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    //! The one-batch decode seat as a unit: the wire tests drive it end
    //! to end through a rogue server; these pin its arms — including
    //! the two the wire cannot cheaply reach — against the frozen
    //! spelling, full-string where no cause is appended.

    use std::sync::Arc;

    use super::*;

    /// The refusal, rendered as `read()` returns it.
    const FROZEN: &str = "fatal source error: read frame violated the one-batch rule";

    fn batch(values: &[i64]) -> RecordBatch {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("n", arrow::datatypes::DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::Int64Array::from(values.to_vec()))],
        )
        .expect("a matching batch constructs")
    }

    fn ipc_bytes(batches: &[RecordBatch]) -> Vec<u8> {
        let schema = batch(&[]).schema();
        let mut writer =
            arrow::ipc::writer::StreamWriter::try_new(Vec::new(), &schema).expect("ipc writer");
        for batch in batches {
            writer.write(batch).expect("ipc write");
        }
        writer.into_inner().expect("ipc finish")
    }

    /// Exactly one batch decodes, bit-identical.
    #[test]
    fn a_single_batch_frame_decodes() {
        let sent = batch(&[1, 2, 3]);
        let decoded =
            decode_one_batch(&ipc_bytes(std::slice::from_ref(&sent))).expect("one batch decodes");
        assert_eq!(decoded, sent);
    }

    /// Zero batches (a schema-only stream) refuse with the bare frozen
    /// spelling — full-string: no cause exists to append.
    #[test]
    fn a_zero_batch_frame_refuses_with_the_frozen_spelling() {
        let error = decode_one_batch(&ipc_bytes(&[])).expect_err("zero batches must refuse");
        assert!(matches!(error, SourceError::Fatal(_)), "{error:?}");
        assert_eq!(error.to_string(), FROZEN);
    }

    /// A second decodable batch refuses with the bare frozen spelling —
    /// full-string: refusing, not describing, is the client seat's job.
    #[test]
    fn a_two_batch_frame_refuses_with_the_frozen_spelling() {
        let error = decode_one_batch(&ipc_bytes(&[batch(&[1]), batch(&[2])]))
            .expect_err("a second batch must refuse");
        assert!(matches!(error, SourceError::Fatal(_)), "{error:?}");
        assert_eq!(error.to_string(), FROZEN);
    }

    /// Bytes that are no IPC stream at all refuse with the arrow cause
    /// APPENDED behind the frozen prefix — the diagnostic survives.
    #[test]
    fn undecodable_bytes_refuse_with_the_cause_appended() {
        let error = decode_one_batch(b"not an arrow ipc stream").expect_err("garbage must refuse");
        assert!(matches!(error, SourceError::Fatal(_)), "{error:?}");
        let rendered = error.to_string();
        assert!(
            rendered.starts_with(&format!("{FROZEN}: ")),
            "the cause rides behind the frozen prefix: {rendered}"
        );
        assert!(
            rendered.len() > FROZEN.len() + 2,
            "a cause is actually appended: {rendered}"
        );
    }
}
