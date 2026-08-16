//! The wire `Destination`/`Backend` against the served echo
//! destination: the sdk's D3 exactly-once choreography running
//! CLIENT-side over the wire — the same `Session<B>` type the
//! in-process path composes, over a `Backend` whose every method is a
//! frame on the bidi stream. The echo's process-global call log is the
//! server-side witness: the tests assert the exact `Backend` sequence
//! the wire delivered, in order. The rogue destination serves the one
//! shape the sdk never emits — `Err(Status)` INSIDE the reply stream.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rdlt_connector::core::{
    ColumnDef, ColumnType, CommitCounters, CommitMeta, LoadId, LogicalType, PipelineId, Provenance,
    StateDoc, TableName, TableSchema, WriteMode,
};
use rdlt_connector::{
    Destination as _, DestinationError, LoadSession, OpenContext, PartCloseReason, PartClosed,
    RecordBatch,
};
use rdlt_connector_client::{ConnectorRequirement, destination::Destination};
use rdlt_connector_sdk::destination::Backend as _;
use rdlt_connector_sdk::serve;

use super::support::echo::{self, EchoDestination};
use super::support::rogue::{self, SessionScript};

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
const ENGINE_BUDGET_BYTES: u64 = 8 * 1024 * 1024;

/// Every wait whose failure mode is a HANG is bounded, so a hang
/// reports as a named failure rather than a suite timeout.
const BOUND: Duration = Duration::from_secs(10);

/// Connect to a served echo destination with `config`, requiring the
/// echo's own identity.
async fn connect_echo(path: &std::path::Path, config: serde_json::Value) -> Destination {
    Destination::connect(
        path,
        ENGINE_BUDGET_BYTES,
        &config,
        &ConnectorRequirement::new("echo-destination"),
    )
    .await
    .expect("connect")
    .0
}

/// The context every session here opens under — one pipeline, one load.
fn context() -> OpenContext {
    OpenContext::new(PipelineId::new("pipe"), LoadId::new("load-1"))
}

/// 5M6: a destination declaring an out-of-range `ident_rules.max_len`
/// is refused at the handshake — the field drives the engine's naming
/// probe loop, so the trust boundary validates it before any engine
/// ever sees it.
#[tokio::test]
async fn an_out_of_range_ident_rules_declaration_refuses_the_handshake() {
    let (_dir, path) = socket_path();
    let _serving = rogue::serve_destination_with_capabilities(
        &path,
        SessionScript::FailNextCallWithStatus {
            code: tonic::Code::Unavailable,
            message: "unused".to_string(),
        },
        Some(
            rdlt_connector::DestinationCapabilities::default()
                .with_ident_rules(rdlt_connector::core::naming::IdentRules { max_len: 2 }),
        ),
    );
    let error = Destination::connect(
        &path,
        ENGINE_BUDGET_BYTES,
        &serde_json::json!({}),
        &ConnectorRequirement::new("rogue"),
    )
    .await
    .expect_err("an exhaustible max_len must refuse the handshake");
    assert!(
        error.to_string().contains("identifier rules"),
        "the refusal names the field: {error}"
    );
}

/// The one-column `id: Int64` logical schema, hand-built (this crate's
/// test support imports nothing cross-crate).
fn schema_for(table: &str) -> TableSchema {
    TableSchema {
        table: TableName::new(table),
        parent: None,
        columns: vec![ColumnDef {
            name: "id".into(),
            column_type: ColumnType::scalar(LogicalType::Int64),
            nullable: false,
            provenance: Provenance::Inferred,
        }],
    }
}

/// The matching physical batch.
fn batch_of(ids: &[i64]) -> RecordBatch {
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![Arc::new(arrow::array::Int64Array::from(ids.to_vec()))],
    )
    .expect("a matching batch constructs")
}

/// A commit envelope at `seq` with an otherwise-fresh state doc.
fn meta_for(seq: u64) -> CommitMeta {
    CommitMeta {
        load_id: LoadId::new("load-1"),
        commit_seq: seq,
        state: StateDoc::new(PipelineId::new("pipe"), "test"),
        counters: CommitCounters::default(),
    }
}

/// The full fresh-load choreography, driven AS `dyn LoadSession` (the
/// engine's shape): ensure → write → commit → read_state → close, with
/// `existing_receipt` answering `None` so commit takes the publish leg
/// — and the server-side call log pinning the EXACT `Backend` sequence
/// the wire delivered, in order.
#[tokio::test]
async fn the_full_choreography_crosses_the_wire_as_a_load_session() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve::destination::serve_on::<EchoDestination>(&path)
        .await
        .expect("bind");
    let remote = connect_echo(&path, serde_json::json!({})).await;
    echo::clear_calls();

    let mut session: Box<dyn LoadSession> = remote.open(context()).await.expect("open");
    session
        .ensure_table(&schema_for("numbers"), &WriteMode::Append)
        .await
        .expect("ensure");
    session
        .write(&TableName::new("numbers"), batch_of(&[1, 2, 3]))
        .await
        .expect("write");
    let receipt = tokio::time::timeout(BOUND, session.commit(meta_for(1)))
        .await
        .expect("commit completes")
        .expect("commit");
    assert_eq!(receipt.load_id, LoadId::new("load-1"));
    assert_eq!(receipt.commit_seq, 1, "the receipt the echo minted");
    let state = session
        .read_state(&PipelineId::new("pipe"))
        .await
        .expect("read_state");
    assert!(state.is_none(), "a fresh echo pipeline has no state");
    session.close().await.expect("close");

    assert_eq!(
        echo::calls_snapshot(),
        vec![
            "ensure_table",
            "write",
            "existing_receipt",
            "publish",
            "read_state",
            "close"
        ],
        "the D3 choreography, frame for frame, in the server's own order"
    );
}

/// The replay leg over the wire — the D3 choreography working
/// client-side across the transport: the echo's `existing_receipt`
/// answers `Some`, so `Session::commit` returns the PRIOR receipt and
/// runs `replay`, never `publish`.
#[tokio::test]
async fn a_replayed_commit_takes_the_replay_path_over_the_wire() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve::destination::serve_on::<EchoDestination>(&path)
        .await
        .expect("bind");
    let remote = connect_echo(&path, serde_json::json!({"replay_seq": 7})).await;
    echo::clear_calls();

    let mut session: Box<dyn LoadSession> = remote.open(context()).await.expect("open");
    session
        .ensure_table(&schema_for("numbers"), &WriteMode::Append)
        .await
        .expect("ensure");
    session
        .write(&TableName::new("numbers"), batch_of(&[1]))
        .await
        .expect("write");
    let receipt = tokio::time::timeout(BOUND, session.commit(meta_for(1)))
        .await
        .expect("commit completes")
        .expect("commit");
    assert_eq!(
        receipt.commit_seq, 1,
        "the stored receipt for THIS identity (7M1: a conforming backend \
         answers the identity it was asked about), not a fresh publish's"
    );
    session.close().await.expect("close");

    assert_eq!(
        echo::calls_snapshot(),
        vec![
            "ensure_table",
            "write",
            "existing_receipt",
            "replay",
            "close"
        ],
        "replay, NOT publish — the choreography's replay leg crossed the wire"
    );
}

/// Part events cross the wire back into the SPI seam: the echo reports
/// two closed parts DURING `publish`, and the client's `part_events`
/// callback has already received both — count and payload fields —
/// by the time `commit` returns (the serving side drains queued parts
/// before the call's own reply; the client forwards each before
/// resolving it).
#[tokio::test]
async fn part_events_reach_the_callback_before_commit_returns() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve::destination::serve_on::<EchoDestination>(&path)
        .await
        .expect("bind");
    let remote = connect_echo(&path, serde_json::json!({"emit_parts": 2})).await;

    let received: Arc<Mutex<Vec<PartClosed>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&received);
    let context = context().with_part_events(Arc::new(move |part| {
        sink.lock().expect("part sink lock").push(part);
    }));

    let mut session: Box<dyn LoadSession> = remote.open(context).await.expect("open");
    session
        .ensure_table(&schema_for("numbers"), &WriteMode::Append)
        .await
        .expect("ensure");
    session
        .write(&TableName::new("numbers"), batch_of(&[1]))
        .await
        .expect("write");
    tokio::time::timeout(BOUND, session.commit(meta_for(1)))
        .await
        .expect("commit completes")
        .expect("commit");

    // Read IMMEDIATELY after commit, before any further awaits: the
    // events were delivered inside the commit call or not at all.
    let parts = received.lock().expect("part sink lock").clone();
    assert_eq!(parts.len(), 2, "both parts arrived before commit returned");
    for (index, part) in parts.iter().enumerate() {
        assert_eq!(part.table, TableName::new("numbers"));
        assert_eq!(part.encoded_bytes, 512 + index as u64);
        assert_eq!(part.reason, PartCloseReason::Commit);
    }

    session.close().await.expect("close");
}

/// The error round-trip pin: a served transient publish failure, mapped
/// back through the wire, renders the classification frame EXACTLY ONCE
/// — full-string, so a second frame (the 026 double-frame class, end to
/// end over the wire) cannot hide in a substring match. The session
/// stays usable afterward: close still works, matching serve semantics.
#[tokio::test]
async fn a_failed_publish_round_trips_the_classified_cause() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve::destination::serve_on::<EchoDestination>(&path)
        .await
        .expect("bind");
    let remote = connect_echo(&path, serde_json::json!({"fail_publish": true})).await;

    let mut session: Box<dyn LoadSession> = remote.open(context()).await.expect("open");
    session
        .ensure_table(&schema_for("numbers"), &WriteMode::Append)
        .await
        .expect("ensure");
    let error = tokio::time::timeout(BOUND, session.commit(meta_for(1)))
        .await
        .expect("the failure is prompt")
        .expect_err("the induced publish failure");

    assert!(matches!(error, DestinationError::Transient(_)), "{error:?}");
    assert_eq!(
        error.to_string(),
        "transient destination error: echo: induced publish failure"
    );

    session
        .close()
        .await
        .expect("the session survives a classified failure — close still works");
}

/// A refused `connect` crosses back as the Open frame's `ErrorFrame`
/// reply: `open` fails typed with the echo's own classification and
/// cause — full-string, single-prefixed.
#[tokio::test]
async fn a_refused_connect_maps_the_open_error() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve::destination::serve_on::<EchoDestination>(&path)
        .await
        .expect("bind");
    let remote = connect_echo(&path, serde_json::json!({"fail_connect": true})).await;

    let error = remote
        .open(context())
        .await
        .map(|_| ())
        .expect_err("the induced connect failure");
    assert!(matches!(error, DestinationError::Transient(_)), "{error:?}");
    assert_eq!(
        error.to_string(),
        "transient destination error: echo: induced connect failure"
    );
}

/// The SERVER's write guard, reached through the raw wire `Backend`
/// (bypassing the client-side `Session`'s own guard, which would refuse
/// identically and locally): the refusal crosses the wire as a typed
/// fatal with the guard's frozen spelling — full-string — and the
/// session stays usable afterward.
#[tokio::test]
async fn a_write_before_ensure_maps_the_servers_frozen_refusal() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve::destination::serve_on::<EchoDestination>(&path)
        .await
        .expect("bind");
    let remote = connect_echo(&path, serde_json::json!({})).await;

    let mut backend = remote.open_backend(&context()).await.expect("open");
    let error = backend
        .write(&TableName::new("numbers"), batch_of(&[1]))
        .await
        .expect_err("the server guard must refuse");
    assert!(matches!(error, DestinationError::Fatal(_)), "{error:?}");
    assert_eq!(
        error.to_string(),
        "fatal destination error: write before ensure_table for `numbers` on this session — \
         the host contract guarantees an ensure precedes the first write, so this is a harness \
         or host defect, not data"
    );

    // The refusal was a reply, not a session end: the same stream still
    // carries an honest ensure-then-write.
    backend
        .ensure_table(&schema_for("numbers"), &WriteMode::Append)
        .await
        .expect("ensure after the refusal");
    backend
        .write(&TableName::new("numbers"), batch_of(&[1]))
        .await
        .expect("write after ensure");
    backend.close().await.expect("close");
}

/// Session death mid-call: after `Close` the server ends the reply
/// stream, so a further call finds the stream ended before its reply —
/// the frozen fatal, full-string.
#[tokio::test]
async fn a_call_after_the_session_ended_is_the_frozen_fatal() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve::destination::serve_on::<EchoDestination>(&path)
        .await
        .expect("bind");
    let remote = connect_echo(&path, serde_json::json!({})).await;

    let mut backend = remote.open_backend(&context()).await.expect("open");
    backend.close().await.expect("close");

    let error = tokio::time::timeout(BOUND, backend.publish(meta_for(1)))
        .await
        .expect("the dead session answers promptly")
        .expect_err("no session is left to publish");
    assert!(matches!(error, DestinationError::Fatal(_)), "{error:?}");
    assert_eq!(
        error.to_string(),
        "fatal destination error: the connector session ended before replying"
    );
}

/// The Status leg (the served side's protocol-state refusals ride raw
/// `Status`, not `ErrorFrame`): a second concurrent `OpenSession` is
/// refused with the serve side's frozen one-session wording, and the
/// client maps it fatal safe-loud with the transport named.
#[tokio::test]
async fn a_second_session_maps_the_status_refusal_fatal() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve::destination::serve_on::<EchoDestination>(&path)
        .await
        .expect("bind");
    let remote = connect_echo(&path, serde_json::json!({})).await;

    let _first = remote
        .open_backend(&context())
        .await
        .expect("first session");
    let error = remote
        .open_backend(&context())
        .await
        .map(|_| ())
        .expect_err("v0's one-session ceiling refuses the second");
    assert!(matches!(error, DestinationError::Fatal(_)), "{error:?}");
    let rendered = error.to_string();
    assert!(
        rendered.starts_with("fatal destination error: connector transport: "),
        "a Status refusal names the transport: {rendered}"
    );
    assert!(
        rendered.contains("one session per connector process"),
        "the serve side's frozen wording survives: {rendered}"
    );
}

/// The reply loop's OTHER Status leg — `Err(status)` arriving INSIDE
/// the bidi reply stream, mid-session, while a Backend call is in
/// flight (a handler-level Err surfaces trailers-only at the open
/// seat, so only a rogue yielding the error as a STREAM ITEM reaches
/// this arm): the in-flight call fails typed transport-fatal with the
/// rogue's own wording carried, exactly like the open-seat leg above.
#[tokio::test]
async fn a_mid_stream_status_fails_the_in_flight_call_transport_fatal() {
    let (_dir, path) = socket_path();
    let _serving = rogue::serve_destination(
        &path,
        SessionScript::FailNextCallWithStatus {
            code: tonic::Code::Unavailable,
            message: "rogue: induced mid-session failure".to_string(),
        },
    );
    let remote = Destination::connect(
        &path,
        ENGINE_BUDGET_BYTES,
        &serde_json::json!({}),
        &ConnectorRequirement::new("rogue"),
    )
    .await
    .expect("connect")
    .0;

    let mut backend = remote
        .open_backend(&context())
        .await
        .expect("the rogue accepts the Open frame — the failure is mid-session");
    let error = tokio::time::timeout(
        BOUND,
        backend.ensure_table(&schema_for("numbers"), &WriteMode::Append),
    )
    .await
    .expect("the mid-stream Status answers promptly")
    .expect_err("the scripted mid-stream Err(Status)");

    assert!(matches!(error, DestinationError::Fatal(_)), "{error:?}");
    let rendered = error.to_string();
    assert!(
        rendered.starts_with("fatal destination error: connector transport: "),
        "a mid-stream Status names the transport: {rendered}"
    );
    assert!(
        rendered.contains("rogue: induced mid-session failure"),
        "the rogue's own wording survives the mapping: {rendered}"
    );
}

/// `capabilities()` (and `spec()`) answer SYNCHRONOUSLY from the
/// handshake-cached sheet: the server is killed after the handshake and
/// both still answer — no RPC is left to make.
#[tokio::test]
async fn capabilities_answer_from_the_handshake_cache_without_an_rpc() {
    let (_dir, path) = socket_path();
    let (_line, handle) = serve::destination::serve_on::<EchoDestination>(&path)
        .await
        .expect("bind");
    let (remote, outcome) = Destination::connect(
        &path,
        ENGINE_BUDGET_BYTES,
        &serde_json::json!({}),
        &ConnectorRequirement::new("echo-destination"),
    )
    .await
    .expect("connect");

    // While the server lives, the wire half works too.
    remote.check().await.expect("check over the wire");

    handle.abort();

    let capabilities = remote.capabilities();
    assert!(
        capabilities.merge && capabilities.structs,
        "the echo's non-default declaration, served from the cache"
    );
    assert_eq!(
        outcome.capabilities,
        Some(capabilities),
        "the cache IS the handshake's document"
    );
    assert_eq!(remote.spec().name, "echo-destination", "spec is cached too");
}

/// The part-event table seat of the wire edge's control-character
/// gate: a `part_closed` naming a hostile table refuses typed and
/// inert — the event never reaches the callback, because a table name
/// is a filesystem-adjacent identifier on its way into host telemetry,
/// the same class as a declared stream name.
#[tokio::test]
async fn a_control_character_table_in_a_part_event_refuses_typed() {
    let (_dir, path) = socket_path();
    let _serving = rogue::serve_destination(
        &path,
        SessionScript::FloodPartsThenSilence {
            parts: 1,
            table: "num\u{1b}]52;c;AAAA\u{7}bers".to_string(),
        },
    );
    let remote = Destination::connect(
        &path,
        ENGINE_BUDGET_BYTES,
        &serde_json::json!({}),
        &ConnectorRequirement::new("rogue"),
    )
    .await
    .expect("connect")
    .0;

    let seen = Arc::new(Mutex::new(Vec::<PartClosed>::new()));
    let sink = Arc::clone(&seen);
    let context = context().with_part_events(Arc::new(move |part| {
        sink.lock().expect("part log lock").push(part);
    }));
    let mut backend = remote.open_backend(&context).await.expect("open");

    let error = tokio::time::timeout(
        BOUND,
        backend.ensure_table(&schema_for("numbers"), &WriteMode::Append),
    )
    .await
    .expect("the refusal answers promptly — the gate fires before any deadline")
    .expect_err("a control-character table must refuse");

    assert!(matches!(error, DestinationError::Fatal(_)), "{error:?}");
    let rendered = error.to_string();
    assert!(
        !rendered.contains('\u{1b}') && !rendered.contains('\u{7}'),
        "the refusal must not itself carry the bytes it refuses: {rendered:?}"
    );
    assert!(
        rendered.contains("refused at the wire boundary"),
        "the refusal names the gate: {rendered}"
    );
    assert!(
        seen.lock().expect("part log lock").is_empty(),
        "the hostile event never reaches the callback"
    );
}

/// 6M1's document half, wire-level: a `ReadState` reply whose document
/// exceeds the document ceiling is refused FATAL before its `Value`
/// materializes — `StateDoc` is a typed shell around UNTYPED cursor
/// values, so the seat gets the same ceiling every untyped parse runs.
#[tokio::test]
async fn an_oversized_state_document_is_refused_at_the_decode_seat() {
    let (_dir, path) = socket_path();
    let _serving = rogue::serve_destination(
        &path,
        SessionScript::AnswerReadStateWith {
            state_doc_json: vec![b'x'; rdlt_connector::MAX_DOCUMENT_BYTES as usize + 1],
        },
    );
    let remote = Destination::connect(
        &path,
        ENGINE_BUDGET_BYTES,
        &serde_json::json!({}),
        &ConnectorRequirement::new("rogue"),
    )
    .await
    .expect("connect")
    .0;

    let mut backend = remote.open_backend(&context()).await.expect("open");
    let error = tokio::time::timeout(
        BOUND,
        backend.read_state(&rdlt_connector::core::PipelineId::new("p")),
    )
    .await
    .expect("the refusal answers promptly")
    .expect_err("an oversized state document must refuse");

    assert!(matches!(error, DestinationError::Fatal(_)), "{error:?}");
    let rendered = error.to_string();
    assert!(
        rendered.contains("document ceiling"),
        "the refusal names the ceiling: {rendered}"
    );
}

/// 6M1's cursor half + 6L1: a state document inside the ceiling whose
/// CURSOR inflates past the cursor contract on re-serialization (the
/// float-notation shape: `1e15` parses compact and re-serializes as
/// `1000000000000000.0`) is refused naming the per-stream contract —
/// the persisted form is what the WAL line cap receives.
#[tokio::test]
async fn an_inflating_cursor_inside_the_state_document_refuses_on_serialization() {
    // A document well under the 8 MiB document ceiling whose one cursor
    // serializes past the 4 MiB cursor contract — built through the real
    // `StateDoc` so the shape is the wire's own.
    let floats = format!("[{}]", vec!["1e15"; 300_000].join(","));
    let mut doc =
        rdlt_connector::core::StateDoc::new(rdlt_connector::core::PipelineId::new("p"), "test");
    doc.cursors.insert(
        rdlt_connector::core::StreamName::new("s"),
        rdlt_connector::core::Cursor::new({
            let inflated: serde_json::Value =
                serde_json::from_str(&floats).expect("compact exponent notation parses");
            inflated
        }),
    );
    let doc = serde_json::to_vec(&doc).expect("a StateDoc serializes");
    assert!(doc.len() < rdlt_connector::MAX_DOCUMENT_BYTES as usize);

    let (_dir, path) = socket_path();
    let _serving = rogue::serve_destination(
        &path,
        SessionScript::AnswerReadStateWith {
            state_doc_json: doc,
        },
    );
    let remote = Destination::connect(
        &path,
        ENGINE_BUDGET_BYTES,
        &serde_json::json!({}),
        &ConnectorRequirement::new("rogue"),
    )
    .await
    .expect("connect")
    .0;

    let mut backend = remote.open_backend(&context()).await.expect("open");
    let error = tokio::time::timeout(
        BOUND,
        backend.read_state(&rdlt_connector::core::PipelineId::new("p")),
    )
    .await
    .expect("the refusal answers promptly")
    .expect_err("an inflating cursor must refuse");

    assert!(matches!(error, DestinationError::Fatal(_)), "{error:?}");
    let rendered = error.to_string();
    assert!(
        rendered.contains("cursor contract") && rendered.contains("`s`"),
        "the refusal names the stream and the contract: {rendered}"
    );
}

/// 6M1's render quality: a malformed state document's refusal carries
/// KIND and LOCATION, never the document's own bytes (6L7 — serde's
/// data arms quote the parsed token, and state docs run to megabytes).
#[tokio::test]
async fn a_malformed_state_document_refusal_never_echoes_the_document() {
    // A document whose `format_version` carries a string: serde's data
    // error quotes the parsed token verbatim — the renderer must not.
    // Built from a real StateDoc so only the one field is hostile.
    let mut value = serde_json::to_value(rdlt_connector::core::StateDoc::new(
        rdlt_connector::core::PipelineId::new("p"),
        "test",
    ))
    .expect("a StateDoc serializes");
    value["format_version"] = serde_json::json!("TOCTOKEN");
    let doc = serde_json::to_vec(&value).expect("a JSON value serializes to JSON infallibly");
    let (_dir, path) = socket_path();
    let _serving = rogue::serve_destination(
        &path,
        SessionScript::AnswerReadStateWith {
            state_doc_json: doc,
        },
    );
    let remote = Destination::connect(
        &path,
        ENGINE_BUDGET_BYTES,
        &serde_json::json!({}),
        &ConnectorRequirement::new("rogue"),
    )
    .await
    .expect("connect")
    .0;

    let mut backend = remote.open_backend(&context()).await.expect("open");
    let error = tokio::time::timeout(
        BOUND,
        backend.read_state(&rdlt_connector::core::PipelineId::new("p")),
    )
    .await
    .expect("the refusal answers promptly")
    .expect_err("a malformed state document must refuse");

    let rendered = error.to_string();
    assert!(
        !rendered.contains("TOCTOKEN"),
        "kind and location, never the value: {rendered}"
    );
    assert!(
        rendered.contains("undecodable state_doc_json"),
        "the refusal names the field: {rendered}"
    );
}

/// 6L2's part-event half: a `part_closed` naming an over-length table
/// (clean of control characters — length alone is the abuse) refuses
/// typed before the event reaches the callback.
#[tokio::test]
async fn an_oversized_table_in_a_part_event_refuses_typed() {
    let (_dir, path) = socket_path();
    let _serving = rogue::serve_destination(
        &path,
        SessionScript::FloodPartsThenSilence {
            parts: 1,
            table: "t".repeat(1025),
        },
    );
    let remote = Destination::connect(
        &path,
        ENGINE_BUDGET_BYTES,
        &serde_json::json!({}),
        &ConnectorRequirement::new("rogue"),
    )
    .await
    .expect("connect")
    .0;

    let seen = Arc::new(Mutex::new(Vec::<PartClosed>::new()));
    let sink = Arc::clone(&seen);
    let context = context().with_part_events(Arc::new(move |part| {
        sink.lock().expect("part log lock").push(part);
    }));
    let mut backend = remote.open_backend(&context).await.expect("open");

    let error = tokio::time::timeout(
        BOUND,
        backend.ensure_table(&schema_for("numbers"), &WriteMode::Append),
    )
    .await
    .expect("the refusal answers promptly")
    .expect_err("an oversized table must refuse");

    assert!(matches!(error, DestinationError::Fatal(_)), "{error:?}");
    assert!(
        error.to_string().contains("identifier ceiling"),
        "the refusal names the ceiling: {error}"
    );
    assert!(
        seen.lock().expect("part log lock").is_empty(),
        "the hostile event never reaches the callback"
    );
}
