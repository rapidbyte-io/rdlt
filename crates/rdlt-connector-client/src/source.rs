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
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use rdlt_connector::core::Cursor;
use rdlt_connector::{ConnectorSpec, ReadRequest, RecordBatch, SourceError, StreamSpec};
use rdlt_connector_protocol::proto::{self, check_reply, read_frame, streams_reply};
use tonic::transport::Channel;

use crate::dial::{connector_client, dial, source_client};
use crate::error::{ClientError, TimedOutOperation, source_error_from_frame, with_deadline};
use crate::handshake::{ConnectorRequirement, HandshakeOutcome, Role, handshake};

/// An SPI [`rdlt_connector::Source`] over the wire: the dialed channel
/// plus the handshake's cached spec and the requirement's RPC deadline
/// (every await below is bounded by it — a silent connector fails
/// typed, never hangs). Constructed only through [`Source::connect`] —
/// there is no way to hold one whose identity was not verified
/// (D-039-2).
#[derive(Debug)]
pub struct Source {
    channel: Channel,
    spec: ConnectorSpec,
    deadline: Duration,
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
        let channel = dial(socket_path, engine_budget_bytes, expected.rpc_deadline).await?;
        let outcome = handshake(&channel, Role::Source, config, expected).await?;
        Ok((
            Source {
                channel,
                spec: outcome.spec.clone(),
                deadline: expected.rpc_deadline,
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
/// including newline/tab/DEL, and C1) — the identifier seat of the
/// `sanitize` module's one rule, and the wire edge's half of the
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
    let hostile = std::iter::once(("stream name", spec.name.as_str()))
        .chain(
            spec.primary_key
                .iter()
                .flatten()
                .map(|field| ("primary-key field", field.as_str())),
        )
        .chain(
            spec.cursor_field
                .iter()
                .map(|field| ("cursor field", field.as_str())),
        )
        .chain(
            spec.type_hints
                .keys()
                .map(|field| ("type-hint field", field.as_str())),
        )
        .find(|(_, value)| crate::sanitize::contains_control(value));
    if let Some((seat, value)) = hostile {
        return Err(SourceError::fatal(format!(
            "the connector declared a {seat} of {value:?} — control or invisible formatting \
             characters in identifiers are refused at the wire boundary"
        )));
    }
    Ok(())
}

/// Validate every Arrow field name, including nested container fields.
/// The walk is iterative so an attacker-controlled schema cannot add a
/// second recursive traversal after Arrow's own verified decoder.
fn refuse_control_characters_in_arrow_fields(batch: &RecordBatch) -> Result<(), SourceError> {
    use arrow::datatypes::DataType;

    let schema = batch.schema();
    let mut pending: Vec<Arc<arrow::datatypes::Field>> = schema.fields().iter().cloned().collect();
    while let Some(field) = pending.pop() {
        if crate::sanitize::contains_control(field.name()) {
            return Err(SourceError::fatal(format!(
                "the connector sent an Arrow field named {:?} — control or invisible formatting \
                 characters in identifiers are refused at the wire boundary",
                field.name()
            )));
        }
        match field.data_type() {
            DataType::Struct(fields) => pending.extend(fields.iter().cloned()),
            DataType::List(child)
            | DataType::LargeList(child)
            | DataType::ListView(child)
            | DataType::LargeListView(child)
            | DataType::FixedSizeList(child, _)
            | DataType::Map(child, _) => pending.push(Arc::clone(child)),
            DataType::Union(fields, _) => {
                pending.extend(fields.iter().map(|(_, child)| Arc::clone(child)));
            }
            DataType::RunEndEncoded(run_ends, values) => {
                pending.push(Arc::clone(run_ends));
                pending.push(Arc::clone(values));
            }
            _ => {}
        }
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
pub(crate) fn decode_one_batch(bytes: &[u8]) -> Result<RecordBatch, SourceError> {
    // arrow's IPC reader PANICS on some crafted frames instead of
    // returning Err — the schema converter aborts on e.g. an Int field
    // declaring a negative bit width (found by the arrow_ipc_decode
    // fuzz target; pinned below with the 160-byte reproducer). The
    // whole decode runs under catch_unwind so this seat owns its own
    // failure as the typed refusal, rather than leaning on the
    // engine's task boundary to contain an unwind. The closure
    // captures only `bytes` (a shared slice — UnwindSafe) and no
    // mutable state escapes it, so a mid-decode unwind can leave
    // nothing broken behind. What this cannot suppress: the process
    // panic HOOK still writes its line to stderr before the unwind is
    // caught — a library must not replace the global hook.
    match std::panic::catch_unwind(|| decode_one_batch_erring(bytes)) {
        Ok(decoded) => decoded,
        Err(payload) => Err(SourceError::fatal(format!(
            "{ONE_BATCH_REFUSAL}: the arrow decoder panicked: {}",
            panic_text(payload.as_ref())
        ))),
    }
}

/// The frozen refusal prefix of the one-batch seat — shared by the
/// `Err`-shaped arms in [`decode_one_batch_erring`] and the
/// caught-panic arm in [`decode_one_batch`].
const ONE_BATCH_REFUSAL: &str = "read frame violated the one-batch rule";

/// The `Err`-shaped half of the decode: every failure arrow REPORTS
/// (as opposed to panics on) maps behind the frozen prefix here.
fn decode_one_batch_erring(bytes: &[u8]) -> Result<RecordBatch, SourceError> {
    let mut reader =
        arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
            .map_err(|error| SourceError::fatal(format!("{ONE_BATCH_REFUSAL}: {error}")))?;
    let first = match reader.next() {
        Some(Ok(batch)) => batch,
        Some(Err(error)) => {
            return Err(SourceError::fatal(format!("{ONE_BATCH_REFUSAL}: {error}")));
        }
        None => return Err(SourceError::fatal(ONE_BATCH_REFUSAL)),
    };
    match reader.next() {
        None => {
            refuse_control_characters_in_arrow_fields(&first)?;
            Ok(first)
        }
        Some(Ok(_)) => Err(SourceError::fatal(ONE_BATCH_REFUSAL)),
        Some(Err(error)) => Err(SourceError::fatal(format!("{ONE_BATCH_REFUSAL}: {error}"))),
    }
}

/// A panic payload's message, where one is extractable — panics carry
/// `&str` (the `panic!` literal form) or `String` (the formatted
/// form); anything else renders as the honest placeholder.
fn panic_text(payload: &(dyn std::any::Any + Send)) -> &str {
    if let Some(text) = payload.downcast_ref::<&str>() {
        text
    } else if let Some(text) = payload.downcast_ref::<String>() {
        text
    } else {
        "<non-text panic payload>"
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
        let reply = with_deadline(
            self.deadline,
            TimedOutOperation::Reply,
            client.check(proto::CheckRequest {}),
        )
        .await
        .map_err(SourceError::fatal)?
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
        let reply = with_deadline(
            self.deadline,
            TimedOutOperation::Reply,
            client.streams(proto::StreamsRequest {}),
        )
        .await
        .map_err(SourceError::fatal)?
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
        let mut frames = with_deadline(
            self.deadline,
            TimedOutOperation::Reply,
            client.read(wire_request),
        )
        .await
        .map_err(SourceError::fatal)?
        .map_err(transport_fatal)?
        .into_inner();

        loop {
            // The deadline bounds this ONE await — the quiet interval
            // until the next frame — never the stream's total duration:
            // each frame that arrives starts the next wait's clock
            // afresh, so a slow-but-flowing source of any length never
            // trips it, while a mid-stream stall always does.
            let next = with_deadline(
                self.deadline,
                TimedOutOperation::ReadFrame,
                frames.message(),
            )
            .await
            .map_err(SourceError::fatal)?;
            let frame = match next {
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

    #[test]
    fn every_stream_metadata_identifier_seat_uses_the_same_gate() {
        let cases = [
            StreamSpec::new("orders").with_primary_key(["id\u{202e}"]),
            StreamSpec::new("orders").with_cursor_field("cursor\nforged"),
            StreamSpec::new("orders")
                .with_type_hint("amount\u{200b}", rdlt_connector::core::LogicalType::Int64),
        ];
        for spec in cases {
            assert!(
                refuse_control_characters_in_name(&spec).is_err(),
                "all connector-authored field identifiers are gated: {spec:?}"
            );
        }
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

    #[test]
    fn nested_arrow_field_names_are_gated_after_decode() {
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new(
            "outer",
            DataType::Struct(vec![Field::new("inner\u{202e}", DataType::Int64, true)].into()),
            true,
        )]));
        let batch = RecordBatch::new_empty(schema);
        let error = refuse_control_characters_in_arrow_fields(&batch)
            .expect_err("nested field names use the identifier gate");
        assert!(error.to_string().contains("Arrow field"));
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

    /// The fuzzer-found panic seat (the arrow_ipc_decode target's
    /// first catch): this 160-byte crafted frame declares an Int field
    /// whose bit width is negative, and arrow-ipc 58.3's schema
    /// converter PANICS on it inside `StreamReader::try_new` instead
    /// of returning `Err`. The decode seat must own that failure — a
    /// typed fatal behind the frozen prefix, never an unwind escaping
    /// to the caller. Bytes embedded verbatim so the pin is hermetic.
    #[test]
    fn a_crafted_frame_that_panics_arrow_refuses_typed() {
        const REPRO: [u8; 160] = [
            0xff, 0xff, 0xff, 0xff, 0x78, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x0a, 0x00, 0x0c, 0x00, 0x06, 0x00, 0x05, 0x00, 0x08, 0x00, 0x0a, 0x00, 0x00, 0x00,
            0x00, 0x01, 0x04, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x08, 0x00, 0x08, 0x00, 0x00, 0x00,
            0x04, 0x00, 0x08, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
            0x14, 0x00, 0x00, 0x00, 0x10, 0x00, 0x14, 0x00, 0x08, 0x00, 0x06, 0x00, 0x07, 0x00,
            0x0c, 0x00, 0x00, 0x00, 0x10, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02,
            0x10, 0x00, 0x00, 0x00, 0x1c, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x69, 0x64, 0x00, 0x00, 0x08, 0x00, 0x0c, 0x00,
            0x08, 0x00, 0x07, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x40, 0xff,
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x29, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff,
            0x88, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0c, 0x00,
            0x16, 0x00, 0x06, 0x00, 0x05, 0x00,
        ];

        let error = decode_one_batch(&REPRO)
            .expect_err("a frame that panics the arrow decoder must refuse typed");
        assert!(matches!(error, SourceError::Fatal(_)), "{error:?}");
        let rendered = error.to_string();
        assert!(
            rendered.starts_with(&format!("{FROZEN}: ")),
            "the panic lands behind the frozen prefix: {rendered}"
        );
        assert!(
            rendered.len() > FROZEN.len() + 2,
            "a cause is actually appended: {rendered}"
        );

        // The fuzzing hook drives this same seat: returning here at
        // all IS the assertion (an escaped unwind would fail the test)
        // — the belt on top of the typed pin above, so the hook and
        // the seat cannot drift apart.
        crate::fuzzing::decode_one_batch(&REPRO);
    }

    #[test]
    fn flatbuffer_verification_rejects_deep_schema_before_arrow_conversion() {
        use arrow::datatypes::{DataType, Field, Schema};

        let mut data_type = DataType::Int64;
        for _ in 0..80 {
            data_type = DataType::Struct(vec![Field::new("nested", data_type, true)].into());
        }
        let schema = Schema::new(vec![Field::new("root", data_type, true)]);
        let mut bytes = Vec::new();
        {
            let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut bytes, &schema)
                .expect("the writer can encode the adversarial schema fixture");
            writer.finish().expect("finish schema-only stream");
        }

        let error = decode_one_batch(&bytes).expect_err(
            "FlatBuffers' verifier must reject nesting before Arrow's recursive converter",
        );
        assert!(
            error.to_string().to_ascii_lowercase().contains("depth"),
            "the dependency-level depth guard is the refusal: {error}"
        );
    }
}
