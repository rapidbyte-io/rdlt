//! The wire source adapter against served counterparts: the sdk-served echo for
//! the honest paths, and the rogue for frames the sdk can never emit
//! (the one-batch violation, the unbudgeted blast the pacing
//! observation needs).

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use rdlt_connector::{
    PushPayload, ReadRequest, RecordBatch, Source as _, SourceError, StreamSpec, records_channel,
};
use rdlt_connector_client::handshake::Requirement;
use rdlt_connector_client::source::Remote;
use rdlt_connector_protocol::proto::{self, read_frame};
use rdlt_connector_sdk::serve;

use super::support::echo::EchoSource;
use super::support::rogue::{self, ReadScript};

/// A fresh temp directory plus a fixed socket name inside it — the
/// directory (and the socket file in it) is reclaimed on drop, so runs
/// leave no `.sock` litter. The `TempDir` must outlive the listener.
fn socket_path() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("connector.sock");
    (dir, path)
}

/// A budget in the middle of the SPI's real 8-64 MiB band — what an
/// engine would actually hand `connect`.
const BUDGET_BYTES: u64 = 8 * 1024 * 1024;

/// Every wait in this suite is bounded: the failure mode under test is
/// a HANG, and an unbounded assertion would report it as a timeout of
/// the whole suite instead of a named failure.
const BOUND: Duration = Duration::from_secs(10);

/// Connect to a served echo source with `config`, requiring the echo's
/// own identity.
async fn connect_echo(
    path: &std::path::Path,
    config: serde_json::Value,
) -> (Remote, rdlt_connector_client::handshake::Outcome) {
    Remote::connect(
        path,
        BUDGET_BYTES,
        &config,
        &Requirement::new("echo-source"),
    )
    .await
    .expect("connect")
}

/// One record batch over a single `n: Int64` column.
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

/// The scripted batches as ONE Arrow IPC stream — zero, one, or several
/// batch messages behind one schema message, which is exactly the knob
/// the one-batch tests turn.
fn ipc_bytes(schema: &arrow::datatypes::Schema, batches: &[RecordBatch]) -> Vec<u8> {
    let mut writer =
        arrow::ipc::writer::StreamWriter::try_new(Vec::new(), schema).expect("ipc writer");
    for batch in batches {
        writer.write(batch).expect("ipc write");
    }
    writer.into_inner().expect("ipc finish")
}

fn arrow_frame(schema: &arrow::datatypes::Schema, batches: &[RecordBatch]) -> proto::ReadFrame {
    proto::ReadFrame {
        frame: Some(read_frame::Frame::ArrowIpc(ipc_bytes(schema, batches))),
    }
}

/// `spec()` answers from the handshake's cache — the same document the
/// outcome carries, with no further RPC to answer from.
#[tokio::test]
async fn spec_is_cached_from_the_handshake() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve::source::serve_on::<EchoSource>(&path)
        .await
        .expect("bind");
    let (remote, outcome) = connect_echo(&path, serde_json::json!({"rows": 1})).await;

    let spec = remote.spec();
    assert_eq!(spec.name, "echo-source");
    assert_eq!(spec.version, "0.0.0");
    assert_eq!(spec, outcome.spec, "the cache IS the handshake's document");
}

/// `check()` round-trips a healthy probe.
#[tokio::test]
async fn check_answers_ok_over_the_wire() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve::source::serve_on::<EchoSource>(&path)
        .await
        .expect("bind");
    let (remote, _) = connect_echo(&path, serde_json::json!({"rows": 1})).await;

    remote.check().await.expect("a healthy echo probes clean");
}

/// A failing `check` round-trips classification AND cause: transient
/// stays transient, and the full rendering carries the classification
/// frame exactly once — the wire ships the CAUSE, the client's mapper
/// re-frames it, and nothing frames it twice.
#[tokio::test]
async fn a_failing_check_round_trips_the_classified_cause() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve::source::serve_on::<EchoSource>(&path)
        .await
        .expect("bind");
    let (remote, _) = connect_echo(&path, serde_json::json!({"rows": 1, "fail_check": true})).await;

    let error = remote.check().await.expect_err("the induced check failure");
    assert!(matches!(error, SourceError::Transient(_)), "{error:?}");
    assert_eq!(
        error.to_string(),
        "transient source error: echo: induced check failure"
    );
}

/// `streams()` round-trips the echo's declaration through the JSON
/// list, field for field.
#[tokio::test]
async fn streams_round_trips_the_declaration() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve::source::serve_on::<EchoSource>(&path)
        .await
        .expect("bind");
    let (remote, _) = connect_echo(&path, serde_json::json!({"rows": 1})).await;

    let streams = remote.streams().await.expect("streams");
    assert_eq!(streams, vec![StreamSpec::new("numbers")]);
}

/// The full read: N raw_json frames then the checkpoint, forwarded into
/// the byte channel IN ORDER — the wire adds transport, never
/// reordering.
#[tokio::test]
async fn a_full_read_forwards_frames_in_order() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve::source::serve_on::<EchoSource>(&path)
        .await
        .expect("bind");
    let (remote, _) = connect_echo(&path, serde_json::json!({"rows": 3})).await;

    let stream = remote.streams().await.expect("streams").remove(0);
    let (out, mut input) = records_channel(1 << 20);
    tokio::time::timeout(BOUND, remote.read(ReadRequest::new(stream, None, out)))
        .await
        .expect("the read completes")
        .expect("a clean read");

    // `out` died inside `read`, so the channel drains to `None`.
    let mut payloads = Vec::new();
    while let Some(push) = input.recv().await {
        payloads.push(push.payload);
    }
    assert_eq!(payloads.len(), 4, "three row frames plus the checkpoint");
    for (n, payload) in payloads[..3].iter().enumerate() {
        match payload {
            PushPayload::RawJson(bytes) => {
                assert_eq!(&bytes[..], format!("{{\"n\":{n}}}\n").as_bytes());
            }
            other => panic!("row {n} lands as RawJson, got {other:?}"),
        }
    }
    match &payloads[3] {
        PushPayload::Checkpoint(cursor) => {
            assert_eq!(cursor.as_value(), &serde_json::json!({"n": 2}));
        }
        other => panic!("the trailing frame is the checkpoint, got {other:?}"),
    }
}

/// The terminal-error round-trip pin: a served fatal read failure,
/// mapped back through the wire adapter, renders the classification
/// frame EXACTLY ONCE — full-string, so a second frame (each mapping
/// layer wrapping the classification again) cannot hide in a substring
/// match.
#[tokio::test]
async fn a_terminal_error_renders_one_classification_frame() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve::source::serve_on::<EchoSource>(&path)
        .await
        .expect("bind");
    let (remote, _) = connect_echo(&path, serde_json::json!({"rows": 1, "fail_read": true})).await;

    let stream = remote.streams().await.expect("streams").remove(0);
    let (out, _input) = records_channel(1 << 20);
    let error = tokio::time::timeout(BOUND, remote.read(ReadRequest::new(stream, None, out)))
        .await
        .expect("the read fails promptly")
        .expect_err("the induced read failure");

    assert!(matches!(error, SourceError::Fatal(_)), "{error:?}");
    assert_eq!(
        error.to_string(),
        "fatal source error: echo: induced read failure"
    );
}

/// Cancellation, the mpsc leg: the host drops its receiving half after
/// one frame, and `read` returns `Ok(())` within the bound — a closed
/// channel is an instruction to stop, not an error to escalate.
#[tokio::test]
async fn dropping_the_receiver_cancels_the_read_as_ok() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve::source::serve_on::<EchoSource>(&path)
        .await
        .expect("bind");
    // Enough rows that the read CANNOT finish without the receiver
    // draining (the channel's 64-message cap parks the forwarder), so
    // the drop below always lands on a still-running read. The budget
    // is generous on purpose: a producer parked on the BYTE budget
    // waits on a semaphore that a plain drop never closes — that leg
    // is the blast test's, via `close()`.
    let (remote, _) = connect_echo(&path, serde_json::json!({"rows": 10000})).await;

    let stream = remote.streams().await.expect("streams").remove(0);
    let (out, mut input) = records_channel(BUDGET_BYTES as usize);
    let read = tokio::spawn(async move { remote.read(ReadRequest::new(stream, None, out)).await });

    let first = tokio::time::timeout(BOUND, input.recv())
        .await
        .expect("a first frame arrives")
        .expect("the stream is live");
    drop(first);
    drop(input);

    let outcome = tokio::time::timeout(BOUND, read)
        .await
        .expect("read returns promptly after the drop")
        .expect("the read task joins");
    assert!(
        outcome.is_ok(),
        "cancellation is Ok(()), never an error: {outcome:?}"
    );
}

/// A conforming arrow frame forwards as exactly one `PushPayload::Arrow`
/// batch, bit-identical.
#[tokio::test]
async fn an_arrow_frame_forwards_as_exactly_one_batch() {
    let (_dir, path) = socket_path();
    let sent = batch(&[1, 2, 3]);
    let _serving = rogue::serve(
        &path,
        ReadScript::Frames(vec![arrow_frame(
            &sent.schema(),
            std::slice::from_ref(&sent),
        )]),
    );
    let (remote, _) = Remote::connect(
        &path,
        BUDGET_BYTES,
        &serde_json::json!({}),
        &Requirement::new("rogue"),
    )
    .await
    .expect("connect");

    let (out, mut input) = records_channel(1 << 20);
    tokio::time::timeout(
        BOUND,
        remote.read(ReadRequest::new(StreamSpec::new("scripted"), None, out)),
    )
    .await
    .expect("the read completes")
    .expect("a clean read");

    let push = input.recv().await.expect("the one batch arrives");
    match push.payload {
        PushPayload::Arrow(received) => assert_eq!(received, sent),
        other => panic!("the frame lands as Arrow, got {other:?}"),
    }
    assert!(input.recv().await.is_none(), "nothing follows the batch");
}

/// The client seat of the proto's one-batch rule: a frame carrying a
/// second record batch is refused FATAL with the frozen spelling —
/// full-string, wire-level, against a server the sdk could never be.
#[tokio::test]
async fn a_two_batch_frame_is_refused_at_the_client_seat() {
    let (_dir, path) = socket_path();
    let first = batch(&[1]);
    let second = batch(&[2]);
    let schema = first.schema();
    let _serving = rogue::serve(
        &path,
        ReadScript::Frames(vec![arrow_frame(&schema, &[first, second])]),
    );
    let (remote, _) = Remote::connect(
        &path,
        BUDGET_BYTES,
        &serde_json::json!({}),
        &Requirement::new("rogue"),
    )
    .await
    .expect("connect");

    let (out, _input) = records_channel(1 << 20);
    let error = tokio::time::timeout(
        BOUND,
        remote.read(ReadRequest::new(StreamSpec::new("scripted"), None, out)),
    )
    .await
    .expect("the refusal is prompt")
    .expect_err("a two-batch frame must refuse");

    assert!(matches!(error, SourceError::Fatal(_)), "{error:?}");
    assert_eq!(
        error.to_string(),
        "fatal source error: read frame violated the one-batch rule"
    );
}

/// The checkpoint decode seat, wire-level: a read frame whose
/// `checkpoint_cursor_json` does not parse is refused FATAL as the
/// protocol violation it is — the read fails typed rather than
/// forwarding a cursor that could never round-trip. Only a rogue can
/// script this frame: the sdk's serve half encodes checkpoints from a
/// live `Cursor`, so its JSON is well-formed by construction.
#[tokio::test]
async fn a_malformed_checkpoint_is_refused_typed() {
    let (_dir, path) = socket_path();
    let _serving = rogue::serve(
        &path,
        ReadScript::Frames(vec![proto::ReadFrame {
            frame: Some(read_frame::Frame::CheckpointCursorJson(
                b"{not json".to_vec(),
            )),
        }]),
    );
    let (remote, _) = Remote::connect(
        &path,
        BUDGET_BYTES,
        &serde_json::json!({}),
        &Requirement::new("rogue"),
    )
    .await
    .expect("connect");

    let (out, _input) = records_channel(1 << 20);
    let error = tokio::time::timeout(
        BOUND,
        remote.read(ReadRequest::new(StreamSpec::new("scripted"), None, out)),
    )
    .await
    .expect("the refusal is prompt")
    .expect_err("an undecodable checkpoint must refuse");

    assert!(matches!(error, SourceError::Fatal(_)), "{error:?}");
    let rendered = error.to_string();
    assert!(
        rendered.starts_with(
            "fatal source error: connector protocol violation: \
             undecodable checkpoint_cursor_json in a read frame: "
        ),
        "the refusal names the field, single-framed: {rendered}"
    );
}

/// The cursor cap's inbound half, wire-level: a checkpoint frame whose
/// cursor document exceeds the cursor contract is refused FATAL at the decode
/// seat — the one untyped inbound document — before its `Value` ever
/// materializes (and before it could poison persisted state, which every
/// later resume would then refuse).
#[tokio::test]
async fn an_oversized_checkpoint_cursor_is_refused_at_the_decode_seat() {
    let (_dir, path) = socket_path();
    let _serving = rogue::serve(
        &path,
        ReadScript::Frames(vec![proto::ReadFrame {
            frame: Some(read_frame::Frame::CheckpointCursorJson(vec![
                b'x';
                rdlt_connector::MAX_CURSOR_BYTES
                    as usize
                    + 1
            ])),
        }]),
    );
    let (remote, _) = Remote::connect(
        &path,
        BUDGET_BYTES,
        &serde_json::json!({}),
        &Requirement::new("rogue"),
    )
    .await
    .expect("connect");

    let (out, _input) = records_channel(1 << 20);
    let error = tokio::time::timeout(
        BOUND,
        remote.read(ReadRequest::new(StreamSpec::new("scripted"), None, out)),
    )
    .await
    .expect("the refusal is prompt")
    .expect_err("an over-bound cursor must refuse");

    assert!(matches!(error, SourceError::Fatal(_)), "{error:?}");
    let rendered = error.to_string();
    assert!(
        rendered.contains("cursor contract"),
        "the refusal names the contract: {rendered}"
    );
}

/// The cursor cap's outbound half: a stored cursor over the contract bound is
/// refused BEFORE the re-send — the server is live and would serve, so
/// the refusal proves the document never crossed the wire.
#[tokio::test]
async fn an_oversized_stored_cursor_is_refused_before_resend() {
    let (_dir, path) = socket_path();
    let _serving = rogue::serve(&path, ReadScript::Frames(vec![]));
    let (remote, _) = Remote::connect(
        &path,
        BUDGET_BYTES,
        &serde_json::json!({}),
        &Requirement::new("rogue"),
    )
    .await
    .expect("connect");

    let oversized = rdlt_connector::Cursor::new(serde_json::Value::String(
        "x".repeat(rdlt_connector::MAX_CURSOR_BYTES as usize),
    ));
    let (out, _input) = records_channel(1 << 20);
    let error = tokio::time::timeout(
        BOUND,
        remote.read(ReadRequest::new(
            StreamSpec::new("scripted"),
            Some(oversized),
            out,
        )),
    )
    .await
    .expect("the refusal is prompt")
    .expect_err("an over-bound stored cursor must refuse pre-send");

    assert!(matches!(error, SourceError::Fatal(_)), "{error:?}");
    assert!(
        error.to_string().contains("cursor contract"),
        "the refusal names the contract: {error}"
    );
}

/// The pacing observation the dial-time window clamp exists for: a
/// producer with NO in-connector byte budget (the rogue's blast) against
/// a client dialed at the tiniest budget and a host that never drains.
/// What leaves the producer is bounded to window scale — the SPI budget
/// holds one frame plus one parked push, the h2 windows hold ~64 KiB a
/// side, tonic buffers a few frames of slack — far below what an
/// unpaced 300 ms blast would move. The bound asserted is deliberately
/// generous (4 MiB against an expected few hundred KiB) because it is
/// an UPPER bound on a mechanism whose hard cap is the windows: timing
/// can only push the observed figure DOWN, so the assertion does not
/// flake on a slow machine.
#[tokio::test]
async fn a_tiny_window_bounds_an_unread_blast() {
    const FRAME_BYTES: usize = 64 * 1024;
    const SENT_CEILING: u64 = 4 * 1024 * 1024;

    let (_dir, path) = socket_path();
    let sent_bytes = Arc::new(AtomicU64::new(0));
    let frame = proto::ReadFrame {
        frame: Some(read_frame::Frame::RawJson(vec![b'x'; FRAME_BYTES])),
    };
    let _serving = rogue::serve(
        &path,
        ReadScript::Blast {
            frame,
            sent_bytes: Arc::clone(&sent_bytes),
        },
    );
    // Budget 1: dial floors it to h2's 64 KiB minimum — the tiniest
    // window the clamp can legally advertise.
    let (remote, _) = Remote::connect(&path, 1, &serde_json::json!({}), &Requirement::new("rogue"))
        .await
        .expect("connect");

    // An SPI budget of exactly one frame: the first forward fills it,
    // the second parks — the host never drains.
    let (out, mut input) = records_channel(FRAME_BYTES);
    let read = tokio::spawn(async move {
        remote
            .read(ReadRequest::new(StreamSpec::new("blast"), None, out))
            .await
    });

    // Liveness first (bounded), then a fixed observation window.
    let deadline = tokio::time::Instant::now() + BOUND;
    while sent_bytes.load(Ordering::Relaxed) == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the blast never produced a frame"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    let sent = sent_bytes.load(Ordering::Relaxed);
    assert!(
        sent <= SENT_CEILING,
        "an unread blast must stay window-bounded: {sent} bytes left the producer"
    );

    // Unwind through the OTHER cancellation leg: `close()` wakes the
    // forwarder parked on the byte-budget semaphore, and read returns
    // Ok — the drop test above covers the mpsc leg.
    input.close();
    let outcome = tokio::time::timeout(BOUND, read)
        .await
        .expect("read returns promptly after close")
        .expect("the read task joins");
    assert!(
        outcome.is_ok(),
        "cancellation is Ok(()), never an error: {outcome:?}"
    );
}

/// The cursor seat of the control-character judgment, pinned as a
/// DECISION: a checkpoint cursor is an opaque data document, so
/// control characters inside its string values are data, not refused
/// — and the only render path a cursor has (JSON serialization, the
/// WAL manifest's and state doc's form) spells them inert by
/// construction. Gating here would refuse legitimate source data; the
/// escape lives at the render.
#[tokio::test]
async fn a_control_character_cursor_value_is_data_not_refused() {
    let (_dir, path) = socket_path();
    let hostile_value = "last-key-\u{1b}]52;c;AAAA\u{7}";
    let _serving = rogue::serve(
        &path,
        rogue::ReadScript::Frames(vec![proto::ReadFrame {
            frame: Some(read_frame::Frame::CheckpointCursorJson(
                serde_json::to_vec(&serde_json::json!({"k": hostile_value}))
                    .expect("a cursor value serializes"),
            )),
        }]),
    );
    let (remote, _) = Remote::connect(
        &path,
        BUDGET_BYTES,
        &serde_json::json!({}),
        &rdlt_connector_client::handshake::Requirement::new("rogue"),
    )
    .await
    .expect("connect");

    let (out, mut input) = records_channel(1 << 20);
    let stream = StreamSpec::new("numbers");
    tokio::time::timeout(BOUND, remote.read(ReadRequest::new(stream, None, out)))
        .await
        .expect("the read completes")
        .expect("a control-character cursor value is data — never refused");

    let push = input.recv().await.expect("the checkpoint arrives");
    match &push.payload {
        PushPayload::Checkpoint(cursor) => {
            assert_eq!(
                cursor.as_value(),
                &serde_json::json!({"k": hostile_value}),
                "the value survives byte-identical"
            );
            let rendered =
                serde_json::to_string(cursor.as_value()).expect("a cursor renders as JSON");
            assert!(
                !rendered.contains('\u{1b}') && !rendered.contains('\u{7}'),
                "the JSON render spells control bytes inert: {rendered:?}"
            );
        }
        other => panic!("the frame is the checkpoint, got {other:?}"),
    }
}

/// The inflation seat, wire-level: a checkpoint frame whose WIRE bytes sit under the
/// cursor contract but whose RE-SERIALIZED form inflates past it (the
/// float-notation shape: `1e15` parses compact, serde re-serializes
/// `1000000000000000.0`) is refused on the serialized measurement —
/// the form the WAL line cap receives. Without this gate the run would
/// crash-loop against the write-time line cap on every resume.
#[tokio::test]
async fn an_inflating_checkpoint_cursor_refuses_on_the_serialized_form() {
    // ~1.4 MiB of compact wire bytes — well under the 4 MiB raw gate —
    // whose re-serialization crosses it.
    let floats = format!("[{}]", vec!["1e15"; 300_000].join(","));
    assert!(floats.len() < rdlt_connector::MAX_CURSOR_BYTES as usize);

    let (_dir, path) = socket_path();
    let _serving = rogue::serve(
        &path,
        ReadScript::Frames(vec![proto::ReadFrame {
            frame: Some(read_frame::Frame::CheckpointCursorJson(floats.into_bytes())),
        }]),
    );
    let (remote, _) = Remote::connect(
        &path,
        BUDGET_BYTES,
        &serde_json::json!({}),
        &Requirement::new("rogue"),
    )
    .await
    .expect("connect");

    let (out, _input) = records_channel(1 << 20);
    let error = tokio::time::timeout(
        BOUND,
        remote.read(ReadRequest::new(StreamSpec::new("scripted"), None, out)),
    )
    .await
    .expect("the refusal is prompt")
    .expect_err("an inflating cursor must refuse on serialization");

    assert!(matches!(error, SourceError::Fatal(_)), "{error:?}");
    let rendered = error.to_string();
    assert!(
        rendered.contains("cursor contract"),
        "the refusal names the contract: {rendered}"
    );
}

/// The inbound cursor gate is inclusive at its boundary — a
/// checkpoint frame of EXACTLY the contract's bytes parses and the
/// checkpoint reaches the records channel.
#[tokio::test]
async fn a_checkpoint_at_exactly_the_cursor_ceiling_is_accepted() {
    // A JSON string whose serialized length is exactly the ceiling
    // (2 quote bytes around the padding).
    let cursor = "c".repeat(rdlt_connector::MAX_CURSOR_BYTES as usize - 2);
    let frame = format!("\"{cursor}\"").into_bytes();
    assert_eq!(frame.len() as u64, rdlt_connector::MAX_CURSOR_BYTES);

    let (_dir, path) = socket_path();
    let _serving = rogue::serve(
        &path,
        ReadScript::Frames(vec![proto::ReadFrame {
            frame: Some(read_frame::Frame::CheckpointCursorJson(frame)),
        }]),
    );
    let (remote, _) = Remote::connect(
        &path,
        BUDGET_BYTES,
        &serde_json::json!({}),
        &Requirement::new("rogue"),
    )
    .await
    .expect("connect");

    let (out, mut input) = records_channel(1 << 20);
    remote
        .read(ReadRequest::new(StreamSpec::new("scripted"), None, out))
        .await
        .expect("a checkpoint at the ceiling reads cleanly");
    match input.recv().await.expect("the checkpoint arrives").payload {
        PushPayload::Checkpoint(_) => {}
        other => panic!("expected a checkpoint, got {other:?}"),
    }
}
