//! [`Remote`] — the SPI read seam over the wire: an SPI
//! [`rdlt_connector::source::Source`] whose every method is an RPC against a
//! served connector, so an engine (or any embedder holding a `dyn
//! Source`) drives a spawned connector without learning the wire
//! exists.
//!
//! The mapping is one-to-one and stateless past the handshake:
//! `spec()` answers from the handshake's cached document (no RPC),
//! `check()`/`streams()` are their unary RPCs with error frames mapped
//! back through the one `FromWire` mapping, and `read()` forwards the
//! server-streamed frames into the request's own byte channel — the
//! wire adds transport, never semantics.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use rdlt_connector::arrow::RecordBatch;
use rdlt_connector::core::cursor::Cursor;
use rdlt_connector::error::SourceError;
use rdlt_connector::source::{ReadRequest, StreamSpec};
use rdlt_connector::spec::ConnectorSpec;
use rdlt_connector_protocol::proto::{self, read_frame, streams_reply};
use tonic::transport::Channel;

use crate::error::FromWire;
use crate::{error, gate, handshake, wire};

/// An SPI [`rdlt_connector::source::Source`] over the wire: the dialed channel
/// plus the handshake's cached spec and the requirement's RPC deadline
/// (every await below is bounded by it — a silent connector fails
/// typed, never hangs). Constructed only through [`Remote::connect`],
/// so there is no way to hold one whose identity the handshake did not
/// verify.
#[derive(Debug)]
pub struct Remote {
    channel: Channel,
    spec: ConnectorSpec,
    deadline: Duration,
}

impl Remote {
    /// Dial `socket_path` (the byte budget paces the wire — see
    /// [`wire::dial`]) and run the [`handshake::Role::Source`] handshake,
    /// verifying the connector against `requirement`. Returns the
    /// adapter AND the full [`handshake::Outcome`] so a caller reads
    /// what the handshake established (state-format versions, the
    /// protocol version) without a second RPC.
    pub async fn connect(
        socket_path: &Path,
        budget_bytes: u64,
        config: &serde_json::Value,
        requirement: &handshake::Requirement,
    ) -> Result<(Remote, handshake::Outcome), error::Error> {
        let (channel, outcome) = handshake::establish(
            socket_path,
            budget_bytes,
            config,
            handshake::Role::Source,
            requirement,
        )
        .await?;
        Ok((
            Remote {
                channel,
                spec: outcome.spec.clone(),
                deadline: requirement.rpc_deadline,
            },
            outcome,
        ))
    }
}

/// Gate everything untrusted a declared stream spec carries: its
/// identifier seats — the name, the primary-key fields, the cursor
/// field, the type-hint keys — through the trust boundary's identifier
/// rule (these values travel into events, tracing spans, and the CLI's
/// lines, so each is either clean or refused), plus count caps on the
/// collections themselves. Refused HERE, where third-party bytes become
/// host vocabulary, deliberately not in `StreamName::new` — the core
/// type stays free-form for hosts by its own documented contract, and
/// a host's own SPI sources (test doubles) name their own streams.
fn refuse_untrusted_stream_spec(spec: &StreamSpec) -> Result<(), SourceError> {
    // Count caps beside the content gates — a spec with hundreds of
    // thousands of tiny keys passes every per-value gate within one
    // frame otherwise. An honest primary key names one to a few
    // columns; honest type hints cover at most the stream's columns,
    // and the engine's own per-table column cap is 4,096.
    if let Some(key) = &spec.primary_key {
        gate::count("primary-key fields", key.len(), 64).map_err(SourceError::fatal)?;
    }
    gate::count("type-hint fields", spec.type_hints.len(), 4096).map_err(SourceError::fatal)?;
    let seats = std::iter::once(("stream name", spec.name.as_str()))
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
        );
    for (seat, value) in seats {
        gate::identifier(seat, value).map_err(SourceError::fatal)?;
    }
    Ok(())
}

/// The most stream specs one streams reply may declare — the engine's
/// own default `max_streams_per_source` (1024), mirrored at the wire
/// edge because this seat serves EVERY host: the engine's cap refuses
/// only after full materialization, and an embedder driving the client
/// directly has no engine in front of it at all. A host that raises
/// `max_streams_per_source` past 1024 over a served source will have
/// its streams reply refused HERE, at discovery — the ceiling is not
/// yet configurable, a fixed 1024 today.
const MAX_DECLARED_STREAM_SPECS: usize = 1024;

/// Decode the streams reply's declared specs, gated BEFORE anything
/// materializes — literally: the reply is ONE newline-delimited bytes
/// blob (the proto field's framing rule — one JSON document per line,
/// no trailing newline, empty = zero streams; JSON cannot carry a raw
/// newline inside a string, so the split is unambiguous), decoded by
/// prost as a single allocation bounded by the frame. The gates then
/// run as a RAW BYTE SCAN over that blob: the line-count cap first
/// (one 64 MiB frame of minimal lines would otherwise yield millions
/// of `StreamSpec`s and a multi-second synchronous parse loop), then
/// each LINE's length against the shared document ceiling — and only
/// then does any line parse (a maximal honest spec — 4096 type hints
/// of 1024-byte keys plus the key fields — serializes to ~4.3 MiB, so
/// the 8 MiB ceiling admits every honest spec with headroom; a
/// gate-legal spec of quote-heavy keys can double under JSON escaping
/// to ~8.4 MiB and is refused loudly), then the per-value identifier
/// and count gates on the parsed spec.
fn decode_stream_specs(stream_specs_jsonl: &[u8]) -> Result<Vec<StreamSpec>, SourceError> {
    if stream_specs_jsonl.is_empty() {
        return Ok(Vec::new());
    }
    let declared = stream_specs_jsonl
        .iter()
        .filter(|byte| **byte == b'\n')
        .count()
        .saturating_add(1);
    gate::count("declared stream specs", declared, MAX_DECLARED_STREAM_SPECS)
        .map_err(SourceError::fatal)?;
    for line in stream_specs_jsonl.split(|byte| *byte == b'\n') {
        gate::document("stream_spec_json", line).map_err(SourceError::protocol)?;
    }
    stream_specs_jsonl
        .split(|byte| *byte == b'\n')
        .map(|line| {
            let spec = serde_json::from_slice::<StreamSpec>(line).map_err(|error| {
                SourceError::protocol(format!(
                    "undecodable stream_spec_json in the streams reply: {}",
                    rdlt_connector::gate::describe_parse_error(&error)
                ))
            })?;
            refuse_untrusted_stream_spec(&spec)?;
            Ok(spec)
        })
        .collect()
}

/// Validate every field name a decoded record batch carries, nested
/// container fields included. The walk is iterative so an
/// attacker-controlled schema cannot add a second recursive traversal
/// after Arrow's own verified decoder.
fn refuse_untrusted_record_batch_fields(batch: &RecordBatch) -> Result<(), SourceError> {
    use arrow::datatypes::DataType;

    let schema = batch.schema();
    let mut pending: Vec<Arc<arrow::datatypes::Field>> = schema.fields().iter().cloned().collect();
    while let Some(field) = pending.pop() {
        gate::identifier("record-batch field name", field.name()).map_err(SourceError::fatal)?;
        // A dictionary encodes another type without a field of its own,
        // but its VALUE type can carry named fields (a struct, a list,
        // another dictionary) — unwrap before matching, or those inner
        // names bypass the gate. The unwrap loop is bounded by the
        // schema's finite type depth, and a dictionary's key type is
        // always a bare integer carrying no fields.
        let mut data_type = field.data_type();
        while let DataType::Dictionary(_, value) = data_type {
            data_type = value;
        }
        match data_type {
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
/// the first, because on the write direction of this wire that same
/// silence was measured as row loss, and this is its read-direction
/// mirror.
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
            rdlt_connector::gate::panic_text(payload.as_ref())
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
    // The framing pre-pass runs FIRST, before arrow's reader sees a
    // byte: it refuses declared metadata/body lengths that exceed the
    // frame, which the reader would otherwise allocate on trust. The
    // SPI owns the one implementation for every wire decode seat; this
    // seat wraps its reasons in the frozen one-batch prefix.
    rdlt_connector::gate::refuse_overdeclared_framing(bytes)
        .map_err(|reason| SourceError::fatal(format!("{ONE_BATCH_REFUSAL}: {reason}")))?;
    // Arrow's error text is connector-authored at frame scale — a
    // `Field` render carries the field's whole metadata map — so every
    // appended cause goes through the bounded render, never raw, and
    // THROUGH the sink: the amplified text streams into its capped
    // prefix without ever materializing whole.
    let mut reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .map_err(|error| {
        SourceError::fatal(format!(
            "{ONE_BATCH_REFUSAL}: {}",
            gate::render_display(&error)
        ))
    })?;
    let first = match reader.next() {
        Some(Ok(batch)) => batch,
        Some(Err(error)) => {
            return Err(SourceError::fatal(format!(
                "{ONE_BATCH_REFUSAL}: {}",
                gate::render_display(&error)
            )));
        }
        None => return Err(SourceError::fatal(ONE_BATCH_REFUSAL)),
    };
    // Row count is the memory dimension the framing pre-pass cannot
    // see — it bounds declared LENGTHS, not a RecordBatch's `length`
    // field, and Null/run-end-encoded columns carry millions of rows
    // in almost no bytes. The engine enforces this cap at its own
    // ingress; this seat serves it to every host.
    if first.num_rows() > rdlt_connector::channel::MAX_RECORD_BATCH_ROWS {
        return Err(SourceError::fatal(format!(
            "{ONE_BATCH_REFUSAL}: the batch carries {} rows, over the {}-row wire cap — \
             row count is bounded separately from encoded bytes",
            first.num_rows(),
            rdlt_connector::channel::MAX_RECORD_BATCH_ROWS
        )));
    }
    match reader.next() {
        None => {
            refuse_untrusted_record_batch_fields(&first)?;
            Ok(first)
        }
        Some(Ok(_)) => Err(SourceError::fatal(ONE_BATCH_REFUSAL)),
        Some(Err(error)) => Err(SourceError::fatal(format!(
            "{ONE_BATCH_REFUSAL}: {}",
            gate::render_display(&error)
        ))),
    }
}

#[async_trait]
impl rdlt_connector::source::Source for Remote {
    /// The handshake's cached document — no RPC: the spec was verified
    /// and decoded once at [`Remote::connect`], and a connector's
    /// self-description does not change mid-session.
    fn spec(&self) -> ConnectorSpec {
        self.spec.clone()
    }

    async fn check(&self) -> Result<(), SourceError> {
        wire::check(&self.channel, self.deadline).await
    }

    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        let mut client = wire::source_client(self.channel.clone());
        let reply = wire::bounded(
            self.deadline,
            wire::Operation::Reply,
            client.streams(proto::StreamsRequest {}),
        )
        .await
        .map_err(SourceError::fatal_error)?
        .map_err(SourceError::transport)?
        .into_inner();
        match reply.outcome {
            Some(streams_reply::Outcome::Ok(list)) => decode_stream_specs(&list.stream_specs_jsonl),
            Some(streams_reply::Outcome::Error(frame)) => Err(SourceError::from_frame(&frame)),
            None => Err(SourceError::protocol(
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
            since_cursor_json: match &since {
                Some(cursor) => {
                    // The cursor contract, enforced pre-send on the
                    // SERIALIZED form — the bytes about to cross the wire
                    // are exactly the bytes the gate measures. An
                    // over-bound cursor came from this connector's own
                    // earlier checkpoint (or persisted state); the
                    // refusal tells it to summarize.
                    Some(gate::cursor(cursor.as_value()).map_err(SourceError::fatal)?)
                }
                None => None,
            },
        };

        let mut client = wire::source_client(self.channel.clone());
        let mut frames = wire::bounded(
            self.deadline,
            wire::Operation::Reply,
            client.read(wire_request),
        )
        .await
        .map_err(SourceError::fatal_error)?
        .map_err(SourceError::transport)?
        .into_inner();

        loop {
            // The deadline bounds this ONE await — the quiet interval
            // until the next frame — never the stream's total duration:
            // each frame that arrives starts the next wait's clock
            // afresh, so a slow-but-flowing source of any length never
            // trips it, while a mid-stream stall always does.
            let next = wire::bounded(self.deadline, wire::Operation::ReadFrame, frames.message())
                .await
                .map_err(SourceError::fatal_error)?;
            let frame = match next {
                Ok(Some(frame)) => frame,
                // Clean end of stream: the served read returned Ok and
                // every frame was forwarded.
                Ok(None) => return Ok(()),
                Err(status) => return Err(SourceError::transport(status)),
            };
            let pushed = match frame.frame {
                Some(read_frame::Frame::RawJson(bytes)) => out.raw_json(Bytes::from(bytes)).await,
                Some(read_frame::Frame::ArrowIpc(bytes)) => {
                    out.arrow(decode_one_batch(&bytes)?).await
                }
                Some(read_frame::Frame::CheckpointCursorJson(bytes)) => {
                    // The cursor contract, enforced at the trust boundary:
                    // this is the one UNTYPED inbound document seat — a
                    // compact 64 MiB frame would otherwise materialize
                    // several hundred MB of `Value` here, and the oversized
                    // cursor would then poison persisted state (every later
                    // resume refused at the gates that DO cap).
                    if bytes.len() as u64 > rdlt_connector::gate::MAX_CURSOR_BYTES {
                        return Err(SourceError::protocol(format!(
                            "a checkpoint cursor of {} bytes exceeds the {}-byte cursor \
                             contract — the connector must summarize its state rather than \
                             embed the data",
                            bytes.len(),
                            rdlt_connector::gate::MAX_CURSOR_BYTES
                        )));
                    }
                    let value: serde_json::Value =
                        serde_json::from_slice(&bytes).map_err(|error| {
                            SourceError::protocol(format!(
                                "undecodable checkpoint_cursor_json in a read frame: {}",
                                rdlt_connector::gate::describe_parse_error(&error)
                            ))
                        })?;
                    // The contract is on the form the host PERSISTS, so
                    // the gate measures the RE-SERIALIZED value — serde's
                    // own number rendering inflates a wire-legal cursor
                    // (`1e15` becomes `1000000000000000.0`), and the WAL
                    // line that receives the inflated spelling is capped
                    // tighter than the wire frame. Refusing here keeps the
                    // run from crash-looping against the write-time cap.
                    gate::cursor(&value).map_err(SourceError::protocol)?;
                    out.checkpoint(Cursor::new(value)).await
                }
                // Terminal by the proto's own contract: the frame IS
                // the served read's classified failure, and nothing
                // follows it.
                Some(read_frame::Frame::Error(frame)) => {
                    return Err(SourceError::from_frame(&frame));
                }
                None => {
                    return Err(SourceError::protocol(
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
mod stream_spec_gate_tests {
    //! The wire edge's gate on declared stream specs, pinned directly
    //! on the helper the streams decode runs every spec through.

    use super::*;
    use rdlt_connector::source::StreamSpec;

    /// An OSC-52-shaped name (the clipboard-write escape) refuses
    /// fatal, and the refusal's own rendering is inert — the escaped
    /// `{:?}` form, no raw ESC byte.
    #[test]
    fn a_control_character_name_refuses_with_an_inert_message() {
        let spec = StreamSpec::new("\u{1b}]52;c;AAAA\u{7}");
        let error = refuse_untrusted_stream_spec(&spec)
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
        assert!(refuse_untrusted_stream_spec(&spec).is_err());
    }

    /// The length half — a control-free but absurdly long name refuses
    /// at the same seat, and the boundary is exact.
    #[test]
    fn an_oversized_name_refuses_and_the_boundary_is_exact() {
        let at = "a".repeat(crate::gate::MAX_WIRE_IDENTIFIER_BYTES);
        assert!(refuse_untrusted_stream_spec(&StreamSpec::new(at)).is_ok());
        let over = "a".repeat(crate::gate::MAX_WIRE_IDENTIFIER_BYTES + 1);
        let error = refuse_untrusted_stream_spec(&StreamSpec::new(over))
            .expect_err("one byte over the ceiling refuses");
        assert!(
            error.to_string().contains("identifier ceiling"),
            "the refusal names the ceiling: {error}"
        );
    }

    #[test]
    fn every_stream_metadata_identifier_seat_uses_the_same_gate() {
        let cases = [
            StreamSpec::new("orders").with_primary_key(["id\u{202e}"]),
            StreamSpec::new("orders").with_cursor_field("cursor\nforged"),
            StreamSpec::new("orders").with_type_hint(
                "amount\u{200b}",
                rdlt_connector::core::types::LogicalType::Int64,
            ),
        ];
        for spec in cases {
            assert!(
                refuse_untrusted_stream_spec(&spec).is_err(),
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
                refuse_untrusted_stream_spec(&spec).is_ok(),
                "`{name}` must pass"
            );
        }
    }

    /// The reply-level count gate, BEFORE the decode loop: a 64 MiB
    /// frame of minimal specs would otherwise materialize millions of
    /// `StreamSpec`s (plus a multi-second synchronous parse) before the
    /// engine's own cap could see them. Refused by count alone — the
    /// specs here are minimal and individually legal — and legal at the
    /// cap itself.
    #[test]
    fn an_over_cap_spec_count_refuses_before_any_spec_decodes() {
        let minimal =
            || serde_json::to_vec(&StreamSpec::new("a")).expect("a minimal spec serializes");
        let blob_of = |count: usize| {
            (0..count)
                .map(|_| minimal())
                .collect::<Vec<_>>()
                .join(&b'\n')
        };
        let error = decode_stream_specs(&blob_of(MAX_DECLARED_STREAM_SPECS + 1))
            .expect_err("1025 declared specs refuse");
        assert!(matches!(error, SourceError::Fatal(_)), "{error:?}");
        let rendered = error.to_string();
        assert!(
            rendered.contains("1025 declared stream specs")
                && rendered.contains("over the 1024 ceiling"),
            "the refusal names the count and the ceiling: {rendered}"
        );
        let decoded = decode_stream_specs(&blob_of(MAX_DECLARED_STREAM_SPECS))
            .expect("a reply at the cap is legal");
        assert_eq!(decoded.len(), MAX_DECLARED_STREAM_SPECS);
    }

    /// The per-spec byte ceiling, BEFORE the parse whose map
    /// materialization it bounds: the fixture is NOT valid JSON, so the
    /// document-ceiling spelling in the refusal is the structural proof
    /// `from_slice` never ran — an unrefused parse would have reported
    /// undecodable bytes instead.
    #[test]
    fn an_oversized_spec_refuses_by_bytes_before_it_parses() {
        let oversized = vec![b'x'; rdlt_connector::gate::MAX_DOCUMENT_BYTES as usize + 1];
        let error =
            decode_stream_specs(&oversized).expect_err("a spec over the byte ceiling refuses");
        assert!(matches!(error, SourceError::Fatal(_)), "{error:?}");
        let rendered = error.to_string();
        assert!(
            rendered.contains("stream_spec_json") && rendered.contains("document ceiling"),
            "the byte gate's own spelling proves the parse never ran: {rendered}"
        );
        assert!(
            !rendered.contains("undecodable"),
            "not the parse error's spelling: {rendered}"
        );
    }

    /// Count caps beside the content gates — a spec with an absurd
    /// number of tiny keys passes every per-value gate within one frame
    /// otherwise, and the cost lands at plan time. At the caps
    /// themselves the spec is legal (the boundaries are honest).
    #[test]
    fn key_and_hint_counts_are_capped_at_the_wire_edge() {
        // Over the primary-key field cap.
        let mut spec = StreamSpec::new("orders");
        spec.primary_key = Some((0..65).map(|i| format!("k{i}")).collect());
        let error = refuse_untrusted_stream_spec(&spec).expect_err("65 primary-key fields refuse");
        assert!(
            error.to_string().contains("primary-key fields"),
            "the refusal names the seat: {error}"
        );
        // Over the type-hint field cap.
        let mut spec = StreamSpec::new("orders");
        for i in 0..4097 {
            spec.type_hints.insert(
                format!("c{i}"),
                rdlt_connector::core::types::LogicalType::Int64,
            );
        }
        let error = refuse_untrusted_stream_spec(&spec).expect_err("4097 type hints refuse");
        assert!(
            error.to_string().contains("type-hint fields"),
            "the refusal names the seat: {error}"
        );
        // At both caps: legal.
        let mut spec = StreamSpec::new("orders");
        spec.primary_key = Some((0..64).map(|i| format!("k{i}")).collect());
        for i in 0..4096 {
            spec.type_hints.insert(
                format!("c{i}"),
                rdlt_connector::core::types::LogicalType::Int64,
            );
        }
        refuse_untrusted_stream_spec(&spec).expect("a spec at both count caps is legal");
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

    /// Row count is the memory dimension the framing pre-pass cannot
    /// see — a boolean column packs eight rows per byte, so a ~125 KiB
    /// frame over the row cap must refuse at THIS seat (the engine
    /// enforces the same cap at its own ingress; this seat serves every
    /// host).
    #[test]
    fn an_over_cap_row_count_refuses_at_the_decode_seat() {
        fn boolean_batch(rows: usize) -> RecordBatch {
            let schema = Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("b", arrow::datatypes::DataType::Boolean, false),
            ]));
            RecordBatch::try_new(
                schema,
                vec![Arc::new(arrow::array::BooleanArray::from(vec![
                    false;
                    rows
                ]))],
            )
            .expect("a boolean-column batch constructs")
        }

        let sent = boolean_batch(rdlt_connector::channel::MAX_RECORD_BATCH_ROWS + 1);
        // This test's own writer: the shared `ipc_bytes` helper bakes in
        // the Int64 fixture schema, and this batch's is Boolean.
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(Vec::new(), sent.schema_ref())
            .expect("ipc writer");
        writer.write(&sent).expect("ipc write");
        let bytes = writer.into_inner().expect("ipc finish");
        assert!(
            bytes.len() < 256 * 1024,
            "the fixture is tiny for its row count — eight rows per byte: {}",
            bytes.len()
        );
        let error =
            decode_one_batch(&bytes).expect_err("a row count over the wire cap must refuse typed");
        let rendered = error.to_string();
        assert!(
            rendered.contains("over the 1000000-row wire cap"),
            "the refusal names the cap: {rendered}"
        );
        // At the cap itself: a legal batch decodes.
        let at_cap = boolean_batch(rdlt_connector::channel::MAX_RECORD_BATCH_ROWS);
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(Vec::new(), at_cap.schema_ref())
            .expect("ipc writer");
        writer.write(&at_cap).expect("ipc write");
        decode_one_batch(&writer.into_inner().expect("ipc finish"))
            .expect("a batch at exactly the row cap decodes");
    }

    /// The length half at the record-batch seat: a multi-megabyte
    /// control-FREE field name is still vocabulary abuse — the length
    /// gate applies here beside the content gate.
    #[test]
    fn an_oversized_arrow_field_name_refuses_at_the_decode_seat() {
        use arrow::datatypes::{DataType, Field, Schema};

        let long = "f".repeat(crate::gate::MAX_WIRE_IDENTIFIER_BYTES + 1);
        let schema = Arc::new(Schema::new(vec![Field::new(long, DataType::Int64, true)]));
        let batch = RecordBatch::new_empty(schema);
        let error = refuse_untrusted_record_batch_fields(&batch)
            .expect_err("an over-length field name refuses");
        let rendered = error.to_string();
        assert!(
            rendered.contains("identifier ceiling"),
            "the refusal names the ceiling: {rendered}"
        );
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
        let error = refuse_untrusted_record_batch_fields(&batch)
            .expect_err("nested field names use the identifier gate");
        assert!(error.to_string().contains("record-batch field"));
    }

    /// A dictionary-encoded nested container carries field names too:
    /// `Dictionary(Int32, Struct([...]))` is encodable by arrow's own
    /// writer, and without a Dictionary arm the walk never reached the
    /// inner struct's names.
    #[test]
    fn dictionary_inner_field_names_are_gated_too() {
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new(
            "outer",
            DataType::Dictionary(
                Box::new(DataType::Int32),
                Box::new(DataType::Struct(
                    vec![Field::new("inner\u{202e}", DataType::Int64, true)].into(),
                )),
            ),
            true,
        )]));
        let batch = RecordBatch::new_empty(schema);
        let error = refuse_untrusted_record_batch_fields(&batch)
            .expect_err("field names inside a dictionary's value type use the identifier gate");
        assert!(error.to_string().contains("record-batch field"));
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

    /// A ~24-byte frame whose 4-byte length field declares ~2 GiB of
    /// metadata. arrow-ipc 58.3's stream reader `resize`s its metadata
    /// buffer to the DECLARED size — committing and zero-filling the
    /// full 2 GiB — before `read_exact` discovers the frame holds no
    /// such bytes. The framing pre-pass must refuse first: the refusal
    /// spelling is the pre-pass's own, which is the structural proof
    /// arrow's reader (and its allocation) was never entered.
    #[test]
    fn a_frame_declaring_a_huge_metadata_length_refuses_before_arrow_allocates() {
        let mut frame = vec![0xff, 0xff, 0xff, 0xff];
        frame.extend_from_slice(&0x7fff_fff0_i32.to_le_bytes());
        frame.extend_from_slice(&[0u8; 16]);

        let error = decode_one_batch(&frame)
            .expect_err("a declared length past the frame's end must refuse");
        assert!(matches!(error, SourceError::Fatal(_)), "{error:?}");
        let rendered = error.to_string();
        assert_eq!(
            rendered,
            format!(
                "{FROZEN}: a declared metadata length of 2147483632 bytes exceeds \
                 the 24-byte frame"
            ),
            "the pre-pass's own spelling proves the arrow reader never ran"
        );
    }

    /// The body-length sibling: a real one-batch stream whose
    /// record-batch message is patched to declare a ~2 GiB body. The
    /// reader allocates `bodyLength` zeroed bytes on trust before
    /// reading; the pre-pass must refuse the declaration against the
    /// frame's actual size instead.
    #[test]
    fn a_frame_declaring_a_huge_body_length_refuses_before_arrow_allocates() {
        use arrow::datatypes::{DataType, Field, Schema};

        // Two Int64 columns, three rows — any small real batch works;
        // the patch locates `bodyLength` structurally, not by value.
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::Int64Array::from(vec![1, 2, 3])),
                Arc::new(arrow::array::Int64Array::from(vec![4, 5, 6])),
            ],
        )
        .expect("a matching batch constructs");
        let mut writer =
            arrow::ipc::writer::StreamWriter::try_new(Vec::new(), &schema).expect("ipc writer");
        writer.write(&batch).expect("ipc write");
        let mut bytes = writer.into_inner().expect("ipc finish");
        assert!(
            decode_one_batch(&bytes).is_ok(),
            "the unpatched stream decodes"
        );

        // Walk to the second message (the record batch), read its true
        // declared bodyLength, and overwrite that i64's little-endian
        // spelling inside the metadata with ~2 GiB.
        let (meta_start, meta_end) = {
            let mut pos = 0usize;
            let mut spans = Vec::new();
            while spans.len() < 2 {
                assert_eq!(&bytes[pos..pos + 4], [0xff; 4], "continuation marker");
                let meta_len =
                    i32::from_le_bytes(bytes[pos + 4..pos + 8].try_into().expect("4 bytes"))
                        as usize;
                let start = pos + 8;
                let end = start + meta_len;
                let message =
                    arrow::ipc::root_as_message(&bytes[start..end]).expect("valid metadata");
                let body = usize::try_from(message.bodyLength()).expect("non-negative body");
                spans.push((start, end));
                pos = end + body;
            }
            spans[1]
        };
        let true_body = arrow::ipc::root_as_message(&bytes[meta_start..meta_end])
            .expect("valid metadata")
            .bodyLength();
        // The field's byte position comes from the flatbuffer's own
        // structure: root table offset, its vtable, and the vtable's
        // slot for `bodyLength` (VT_BODYLENGTH = 10 in the generated
        // Message table) — self-verified against the decoded value
        // before patching.
        let field_pos = {
            let meta = &bytes[meta_start..meta_end];
            let root = u32::from_le_bytes(meta[0..4].try_into().expect("4 bytes")) as i64;
            let soffset = i32::from_le_bytes(
                meta[root as usize..root as usize + 4]
                    .try_into()
                    .expect("4 bytes"),
            ) as i64;
            let vtable = usize::try_from(root - soffset).expect("in-bounds vtable");
            let slot =
                u16::from_le_bytes(meta[vtable + 10..vtable + 12].try_into().expect("2 bytes"))
                    as i64;
            assert_ne!(slot, 0, "bodyLength is present in the message");
            meta_start + usize::try_from(root + slot).expect("in-bounds field")
        };
        assert_eq!(
            i64::from_le_bytes(bytes[field_pos..field_pos + 8].try_into().expect("8 bytes")),
            true_body,
            "the located field really is bodyLength"
        );
        bytes[field_pos..field_pos + 8].copy_from_slice(&0x7fff_fff0_i64.to_le_bytes());

        let frame_len = bytes.len();
        let error =
            decode_one_batch(&bytes).expect_err("a declared body past the frame's end must refuse");
        assert!(matches!(error, SourceError::Fatal(_)), "{error:?}");
        assert_eq!(
            error.to_string(),
            format!(
                "{FROZEN}: a declared body length of 2147483632 bytes exceeds \
                 the {frame_len}-byte frame"
            ),
            "the pre-pass's own spelling proves the arrow reader never ran"
        );
    }

    /// A legal frame whose SCHEMA field carries multi-megabyte control-byte
    /// metadata, and whose record-batch message omits that field's node:
    /// arrow's reader renders the whole `Field` — metadata map included —
    /// into its error text, so the appended cause must go through the
    /// bounded render or one small frame materializes a multi-megabyte
    /// (escape-amplified ~5×) error string. The refusal stays under the
    /// render cap plus its envelope and names the true source length.
    #[test]
    fn a_hostile_metadata_arrow_cause_renders_bounded_with_the_true_length() {
        use arrow::datatypes::{DataType, Field, Schema};

        // The declared schema: the batch will carry a node for `a` only,
        // so the reader's node walk fails AT `m` — the field whose
        // metadata is the payload.
        let metadata: std::collections::HashMap<String, String> =
            [("k".to_string(), "\u{7}".repeat(2 << 20))].into();
        let declared = Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("m", DataType::Int64, true).with_metadata(metadata),
        ]);
        let mut schema_stream = Vec::new();
        {
            let mut writer =
                arrow::ipc::writer::StreamWriter::try_new(&mut schema_stream, &declared)
                    .expect("ipc writer");
            writer.finish().expect("finish schema-only stream");
        }
        // A real one-column batch under a DIFFERENT (narrower) schema —
        // its record-batch message declares one field node.
        let batch_stream = ipc_bytes(&[batch(&[1, 2, 3])]);

        // Splice: the metadata-bearing schema message, then everything
        // after the narrow stream's own schema message. Every declared
        // length stays honest, so the framing pre-pass passes it clean.
        let first_message_end = |bytes: &[u8]| {
            assert_eq!(&bytes[0..4], [0xff; 4], "continuation marker");
            let meta_len = i32::from_le_bytes(bytes[4..8].try_into().expect("4 bytes")) as usize;
            let message =
                arrow::ipc::root_as_message(&bytes[8..8 + meta_len]).expect("valid metadata");
            8 + meta_len + usize::try_from(message.bodyLength()).expect("non-negative body")
        };
        let mut spliced = schema_stream[..first_message_end(&schema_stream)].to_vec();
        spliced.extend_from_slice(&batch_stream[first_message_end(&batch_stream)..]);

        // The true cause, measured at the dependency directly: this is
        // the text the seat would append unbounded.
        let mut reader =
            arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(&spliced), None)
                .expect("the spliced schema decodes");
        let true_cause = reader
            .next()
            .expect("a batch message follows")
            .expect_err("the node walk fails at the metadata field")
            .to_string();
        assert!(
            true_cause.len() > 10 << 20,
            "the fixture amplifies to a multi-megabyte cause: {} bytes",
            true_cause.len()
        );

        let error = decode_one_batch(&spliced)
            .expect_err("a batch that omits a declared field's node must refuse");
        assert!(matches!(error, SourceError::Fatal(_)), "{error:?}");
        let rendered = error.to_string();
        assert!(
            rendered.len() <= FROZEN.len() + 2 + crate::gate::MESSAGE_RENDER_CAP + 64,
            "the appended cause is render-capped, not frame-scale: {} bytes",
            rendered.len()
        );
        assert!(
            rendered.contains(&format!("truncated; {} source bytes", true_cause.len())),
            "the marker names the true source length: …{}",
            &rendered[rendered.len().saturating_sub(80)..]
        );
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
