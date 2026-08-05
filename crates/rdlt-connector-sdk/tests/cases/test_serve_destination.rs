//! `serve::destination` end to end: a raw tonic client dials the UDS
//! `serve_on` binds and drives `OpenSession` — one bidi stream carrying
//! the whole session — against the echo destination.
//!
//! Written against ADR D5 (038 T5 review round 1): the wire session
//! drives the connector's raw `Backend` directly (`Ensure`/`Write`/
//! `ExistingReceipt`/`Replay`/`Publish`/`ReadState`/`Close` each reach
//! their own `Backend` method), NOT a collapsed `LoadSession::commit` —
//! see `serve::destination`'s own module doc for the full rationale.
//!
//! `dial`/`socket_path` mirror `test_serve_source`'s identical helpers
//! (see there for why `serve_on`, not the print-and-block `destination`,
//! is the seam these tests use).

use std::os::fd::AsFd;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rdlt_connector::DestinationCapabilities;
use rdlt_connector::core::{CommitReceipt, LoadId, PipelineId, WriteMode};
use rdlt_connector_protocol::proto::connector_client::ConnectorClient;
use rdlt_connector_protocol::proto::destination_service_client::DestinationServiceClient;
use rdlt_connector_protocol::proto::{
    self, Classification, HandshakeRequest, SessionReply, SessionRequest, handshake_reply,
    session_reply, session_request,
};
use rdlt_connector_sdk::serve::destination::serve_on;
use rdlt_testkit::{batch_of, commit_meta_for, schema_for};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Endpoint};

use super::support::echo::{self, ECHOED_TABLE, EchoDestination};

/// A fresh temp directory plus a short, fixed socket path inside it —
/// see `test_serve_source::socket_path` for why `tempfile::tempdir()`,
/// not `std::env::temp_dir()`.
fn socket_path() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("connector.sock");
    (dir, path)
}

/// Dial `path` the way a spawning host dials the handshake line's
/// `socket_path` — see `test_serve_source::dial` for the full comment.
async fn dial(path: &std::path::Path) -> Channel {
    let path = path.to_path_buf();
    Endpoint::try_from("http://[::]:50051")
        .expect("a static placeholder endpoint parses")
        .connect_with_connector(tower::service_fn(move |_: tonic::transport::Uri| {
            let path = path.clone();
            async move {
                let io = tokio::net::UnixStream::connect(path).await?;
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(io))
            }
        }))
        .await
        .expect("connect over uds")
}

/// Like `dial`, but also captures a SAFE duplicate of the connecting
/// socket's file descriptor (`BorrowedFd::try_clone_to_owned` — stable
/// std, no `unsafe` keyword) as a plain blocking `UnixStream`. The test
/// that uses this later calls `.shutdown(Shutdown::Both)` on it to
/// sever the connection abruptly, out from under the client's own h2
/// machinery — see that test's doc comment for why this, rather than
/// aborting `serve_on`'s `JoinHandle`, is what actually proves a
/// transport error surfaces.
async fn dial_severable(path: &std::path::Path) -> (Channel, std::os::unix::net::UnixStream) {
    let path = path.to_path_buf();
    let captured: Arc<Mutex<Option<std::os::unix::net::UnixStream>>> = Arc::new(Mutex::new(None));
    let stash = Arc::clone(&captured);
    let channel = Endpoint::try_from("http://[::]:50051")
        .expect("a static placeholder endpoint parses")
        .connect_with_connector(tower::service_fn(move |_: tonic::transport::Uri| {
            let path = path.clone();
            let stash = Arc::clone(&stash);
            async move {
                let io = tokio::net::UnixStream::connect(path).await?;
                let owned = io
                    .as_fd()
                    .try_clone_to_owned()
                    .expect("dup the connecting socket's fd");
                *stash.lock().expect("lock") = Some(std::os::unix::net::UnixStream::from(owned));
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(io))
            }
        }))
        .await
        .expect("connect over uds");
    let severable = captured
        .lock()
        .expect("lock")
        .take()
        .expect("the connector captured a clone before returning");
    (channel, severable)
}

fn echo_destination_config(fail_publish: bool, receipt_exists: bool) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "fail_publish": fail_publish,
        "receipt_exists": receipt_exists,
    }))
    .expect("echo destination config serializes")
}

fn encode_arrow_ipc(batch: &rdlt_connector::RecordBatch) -> Vec<u8> {
    let mut writer = arrow::ipc::writer::StreamWriter::try_new(Vec::new(), batch.schema_ref())
        .expect("open an arrow ipc stream writer");
    writer
        .write(batch)
        .expect("write an arrow ipc record batch");
    writer
        .into_inner()
        .expect("close an arrow ipc stream writer")
}

/// One `Write` frame whose Arrow IPC stream carries MULTIPLE record
/// batches — `decode_arrow_ipc`'s F3 refusal pin (038 T5 review): a
/// second batch message refuses with its own distinct spelling rather
/// than silently keeping only the first.
fn write_frame_multi(table: &str, batches: &[&[i64]]) -> SessionRequest {
    let batches: Vec<_> = batches.iter().map(|ids| batch_of(ids)).collect();
    let mut writer = arrow::ipc::writer::StreamWriter::try_new(Vec::new(), batches[0].schema_ref())
        .expect("open an arrow ipc stream writer");
    for batch in &batches {
        writer
            .write(batch)
            .expect("write an arrow ipc record batch");
    }
    let arrow_ipc = writer
        .into_inner()
        .expect("close an arrow ipc stream writer");
    SessionRequest {
        request: Some(session_request::Request::Write(proto::Write {
            table: table.to_string(),
            arrow_ipc,
        })),
    }
}

fn open_frame(pipeline: &str, load_id: &str) -> SessionRequest {
    SessionRequest {
        request: Some(session_request::Request::Open(proto::Open {
            pipeline: pipeline.to_string(),
            load_id: load_id.to_string(),
        })),
    }
}

fn ensure_frame(table: &str) -> SessionRequest {
    SessionRequest {
        request: Some(session_request::Request::Ensure(proto::Ensure {
            table_schema_json: serde_json::to_vec(&schema_for(table)).expect("schema json"),
            write_mode_json: serde_json::to_vec(&WriteMode::Append).expect("write mode json"),
        })),
    }
}

fn write_frame(table: &str, ids: &[i64]) -> SessionRequest {
    SessionRequest {
        request: Some(session_request::Request::Write(proto::Write {
            table: table.to_string(),
            arrow_ipc: encode_arrow_ipc(&batch_of(ids)),
        })),
    }
}

/// Bytes that are not an Arrow IPC stream at all — `decode_arrow_ipc`'s
/// "not decodable at all" leg, distinct from "decodable but more than
/// one batch".
fn write_frame_malformed(table: &str) -> SessionRequest {
    SessionRequest {
        request: Some(session_request::Request::Write(proto::Write {
            table: table.to_string(),
            arrow_ipc: vec![0, 1, 2, 3],
        })),
    }
}

fn existing_receipt_frame(load_id: &str, commit_seq: u64) -> SessionRequest {
    SessionRequest {
        request: Some(session_request::Request::ExistingReceipt(
            proto::ExistingReceipt {
                load_id: load_id.to_string(),
                commit_seq,
            },
        )),
    }
}

fn replay_frame(pipeline: &str, load_id: &str, commit_seq: u64) -> SessionRequest {
    let meta = commit_meta_for(
        &PipelineId::new(pipeline),
        &LoadId::new(load_id),
        commit_seq,
    );
    let receipt = CommitReceipt {
        load_id: LoadId::new(load_id),
        commit_seq,
    };
    SessionRequest {
        request: Some(session_request::Request::Replay(proto::Replay {
            commit_meta_json: serde_json::to_vec(&meta).expect("commit meta json"),
            receipt_json: serde_json::to_vec(&receipt).expect("receipt json"),
        })),
    }
}

fn publish_frame(pipeline: &str, load_id: &str, commit_seq: u64) -> SessionRequest {
    let meta = commit_meta_for(
        &PipelineId::new(pipeline),
        &LoadId::new(load_id),
        commit_seq,
    );
    SessionRequest {
        request: Some(session_request::Request::Publish(proto::Publish {
            commit_meta_json: serde_json::to_vec(&meta).expect("commit meta json"),
        })),
    }
}

fn read_state_frame(pipeline: &str) -> SessionRequest {
    SessionRequest {
        request: Some(session_request::Request::ReadState(proto::ReadState {
            pipeline: pipeline.to_string(),
        })),
    }
}

fn close_frame() -> SessionRequest {
    SessionRequest {
        request: Some(session_request::Request::Close(proto::Close {})),
    }
}

/// Pull the next reply within a generous timeout — every test below is
/// driving an in-process UDS round trip, so a hang here means a real
/// defect, not slow IO; failing fast beats the suite's own timeout.
async fn next_reply(
    replies: &mut tonic::Streaming<SessionReply>,
) -> Result<Option<SessionReply>, tonic::Status> {
    tokio::time::timeout(Duration::from_secs(5), replies.message())
        .await
        .expect("a reply within the timeout")
}

async fn open_session(
    destination: &mut DestinationServiceClient<Channel>,
) -> (
    tokio::sync::mpsc::Sender<SessionRequest>,
    tonic::Streaming<SessionReply>,
) {
    let (req_tx, req_rx) = tokio::sync::mpsc::channel(16);
    let replies = destination
        .open_session(ReceiverStream::new(req_rx))
        .await
        .expect("open_session rpc")
        .into_inner();
    (req_tx, replies)
}

async fn handshake(
    connector: &mut ConnectorClient<Channel>,
    fail_publish: bool,
    receipt_exists: bool,
) {
    connector
        .handshake(HandshakeRequest {
            protocol_version: 0,
            expected_role: "destination".to_string(),
            config_json: echo_destination_config(fail_publish, receipt_exists),
        })
        .await
        .expect("handshake rpc");
}

/// The full choreography against a raw tonic client: handshake
/// (`capabilities_json` round-trips), Open, Ensure, Write,
/// ExistingReceipt (→ a REAL `Backend::existing_receipt` lookup, `None`
/// here since nothing published yet), Publish (→ `part_closed` BEFORE
/// `published`, pinning the interleave the callback's synchronicity
/// promises), ReadState (→ `None`), Close (→ clean end) — and the echo
/// backend's own call log proves every frame reached its OWN `Backend`
/// method (ADR D5), not a collapsed `commit`.
#[tokio::test]
async fn the_full_choreography_pins_part_closed_before_published() {
    echo::clear_call_log();
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve_on::<EchoDestination>(&path).await.expect("bind");
    let channel = dial(&path).await;
    let mut connector = ConnectorClient::new(channel.clone());
    let mut destination = DestinationServiceClient::new(channel);

    let reply = connector
        .handshake(HandshakeRequest {
            protocol_version: 0,
            expected_role: "destination".to_string(),
            config_json: echo_destination_config(false, false),
        })
        .await
        .expect("handshake rpc")
        .into_inner();
    match reply.outcome {
        Some(handshake_reply::Outcome::Ok(ok)) => {
            assert_eq!(ok.connector_id, "echo-destination");
            let capabilities: DestinationCapabilities =
                serde_json::from_slice(&ok.capabilities_json).expect("capabilities json");
            assert!(capabilities.merge, "the declared capability, not a default");
            assert!(
                capabilities.structs,
                "the declared capability, not a default"
            );
        }
        other => panic!("expected handshake ok, got {other:?}"),
    }

    let (req_tx, mut replies) = open_session(&mut destination).await;

    req_tx.send(open_frame("p", "l")).await.expect("send open");
    assert!(
        matches!(
            next_reply(&mut replies)
                .await
                .expect("reply")
                .expect("frame")
                .reply,
            Some(session_reply::Reply::Opened(_))
        ),
        "Open replies Opened"
    );

    req_tx
        .send(ensure_frame(ECHOED_TABLE))
        .await
        .expect("send ensure");
    assert!(
        matches!(
            next_reply(&mut replies)
                .await
                .expect("reply")
                .expect("frame")
                .reply,
            Some(session_reply::Reply::Ensured(_))
        ),
        "Ensure replies Ensured"
    );

    req_tx
        .send(write_frame(ECHOED_TABLE, &[1, 2, 3]))
        .await
        .expect("send write");
    assert!(
        matches!(
            next_reply(&mut replies)
                .await
                .expect("reply")
                .expect("frame")
                .reply,
            Some(session_reply::Reply::Written(_))
        ),
        "Write replies Written"
    );

    req_tx
        .send(existing_receipt_frame("l", 1))
        .await
        .expect("send existing_receipt");
    match next_reply(&mut replies)
        .await
        .expect("reply")
        .expect("frame")
        .reply
    {
        Some(session_reply::Reply::Receipt(receipt)) => {
            assert!(
                receipt.receipt_json.is_none(),
                "receipt_exists=false — a real lookup answering truthfully, not a stub"
            );
        }
        other => panic!("expected a receipt reply, got {other:?}"),
    }

    req_tx
        .send(publish_frame("p", "l", 1))
        .await
        .expect("send publish");
    match next_reply(&mut replies)
        .await
        .expect("reply")
        .expect("frame")
        .reply
    {
        Some(session_reply::Reply::PartClosed(part)) => {
            assert_eq!(part.table, ECHOED_TABLE);
            assert_eq!(part.encoded_bytes, 64);
            assert_eq!(part.reason, "commit");
        }
        other => panic!("expected part_closed BEFORE published, got {other:?}"),
    }
    match next_reply(&mut replies)
        .await
        .expect("reply")
        .expect("frame")
        .reply
    {
        Some(session_reply::Reply::Published(published)) => {
            assert!(!published.receipt_json.is_empty());
        }
        other => panic!("expected published to follow part_closed, got {other:?}"),
    }

    req_tx
        .send(read_state_frame("p"))
        .await
        .expect("send read_state");
    match next_reply(&mut replies)
        .await
        .expect("reply")
        .expect("frame")
        .reply
    {
        Some(session_reply::Reply::State(state)) => {
            assert!(state.state_doc_json.is_none());
        }
        other => panic!("expected a state reply, got {other:?}"),
    }

    req_tx.send(close_frame()).await.expect("send close");
    assert!(
        matches!(
            next_reply(&mut replies)
                .await
                .expect("reply")
                .expect("frame")
                .reply,
            Some(session_reply::Reply::Closed(_))
        ),
        "Close replies Closed"
    );
    assert!(
        next_reply(&mut replies).await.expect("reply").is_none(),
        "the stream ends cleanly after Close"
    );

    assert_eq!(
        echo::call_log_snapshot(),
        vec![
            "ensure_table".to_string(),
            "write".to_string(),
            "existing_receipt".to_string(),
            "publish".to_string(),
            "read_state".to_string(),
            "close".to_string(),
        ],
        "every wire frame reaches its OWN Backend method (ADR D5) — the \
         wire ExistingReceipt frame touches the backend directly, and \
         Publish does NOT run its own internal existing_receipt lookup \
         first (that choreography is the CALLER's job, not this server's)"
    );
}

/// ADR D5's reversal, pinned directly: `ExistingReceipt` answers `Some`
/// (a real receipt) when the backend has one, and `Replay` reaches
/// `Backend::replay` for real — both visible in the call log, proving
/// the wire genuinely dispatches to the backend rather than answering
/// from a stub.
#[tokio::test]
async fn existing_receipt_and_replay_reach_the_real_backend() {
    echo::clear_call_log();
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve_on::<EchoDestination>(&path).await.expect("bind");
    let channel = dial(&path).await;
    let mut connector = ConnectorClient::new(channel.clone());
    let mut destination = DestinationServiceClient::new(channel);

    handshake(&mut connector, false, true).await;

    let (req_tx, mut replies) = open_session(&mut destination).await;
    req_tx.send(open_frame("p", "l")).await.expect("send open");
    next_reply(&mut replies)
        .await
        .expect("reply")
        .expect("opened");

    req_tx
        .send(existing_receipt_frame("l", 1))
        .await
        .expect("send existing_receipt");
    match next_reply(&mut replies)
        .await
        .expect("reply")
        .expect("frame")
        .reply
    {
        Some(session_reply::Reply::Receipt(receipt)) => {
            let receipt_json = receipt
                .receipt_json
                .expect("receipt_exists=true answers Some");
            let receipt: CommitReceipt =
                serde_json::from_slice(&receipt_json).expect("receipt json");
            assert_eq!(receipt.load_id.as_str(), "l");
            assert_eq!(receipt.commit_seq, 1);
        }
        other => panic!("expected a Some receipt, got {other:?}"),
    }

    req_tx
        .send(replay_frame("p", "l", 1))
        .await
        .expect("send replay");
    assert!(
        matches!(
            next_reply(&mut replies)
                .await
                .expect("reply")
                .expect("frame")
                .reply,
            Some(session_reply::Reply::Replayed(_))
        ),
        "Replay replies Replayed"
    );

    req_tx.send(close_frame()).await.expect("send close");
    next_reply(&mut replies)
        .await
        .expect("reply")
        .expect("closed");

    assert_eq!(
        echo::call_log_snapshot(),
        vec![
            "existing_receipt".to_string(),
            "replay".to_string(),
            "close".to_string(),
        ],
        "both frames reached their own real Backend method — no Publish \
         was ever sent, so no publish call log entry exists either"
    );
}

/// A publish induced to fail classifies TRANSIENT with the induced
/// message, and the session stays usable afterward — a failed call does
/// not end the session, only `Close` (or the transport) does.
#[tokio::test]
async fn a_failed_publish_classifies_transient_and_the_session_stays_usable() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve_on::<EchoDestination>(&path).await.expect("bind");
    let channel = dial(&path).await;
    let mut connector = ConnectorClient::new(channel.clone());
    let mut destination = DestinationServiceClient::new(channel);

    handshake(&mut connector, true, false).await;

    let (req_tx, mut replies) = open_session(&mut destination).await;

    req_tx.send(open_frame("p", "l")).await.expect("send open");
    next_reply(&mut replies)
        .await
        .expect("reply")
        .expect("opened");

    req_tx
        .send(ensure_frame(ECHOED_TABLE))
        .await
        .expect("send ensure");
    next_reply(&mut replies)
        .await
        .expect("reply")
        .expect("ensured");

    req_tx
        .send(publish_frame("p", "l", 1))
        .await
        .expect("send publish");
    match next_reply(&mut replies)
        .await
        .expect("reply")
        .expect("frame")
        .reply
    {
        Some(session_reply::Reply::Error(error)) => {
            assert_eq!(error.classification, Classification::Transient as i32);
            assert_eq!(
                error.message,
                "transient destination error: echo: induced publish failure"
            );
        }
        other => panic!("expected a transient error frame, got {other:?}"),
    }

    req_tx.send(close_frame()).await.expect("send close");
    assert!(
        matches!(
            next_reply(&mut replies)
                .await
                .expect("reply")
                .expect("frame")
                .reply,
            Some(session_reply::Reply::Closed(_))
        ),
        "the session survives the failed publish and Close still works"
    );
}

/// A `Write` sent as the session's very first frame — before any `Open`
/// — refuses with the frozen spelling, as an `ErrorFrame`, not a
/// stream-ending `Status`.
#[tokio::test]
async fn a_write_before_open_refuses_with_the_frozen_spelling() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve_on::<EchoDestination>(&path).await.expect("bind");
    let channel = dial(&path).await;
    let mut connector = ConnectorClient::new(channel.clone());
    let mut destination = DestinationServiceClient::new(channel);

    handshake(&mut connector, false, false).await;

    let (req_tx, mut replies) = open_session(&mut destination).await;

    req_tx
        .send(write_frame(ECHOED_TABLE, &[1]))
        .await
        .expect("send write");
    match next_reply(&mut replies)
        .await
        .expect("reply")
        .expect("frame")
        .reply
    {
        Some(session_reply::Reply::Error(error)) => {
            assert_eq!(error.classification, Classification::Fatal as i32);
            assert_eq!(error.message, "the session's first frame must be Open");
        }
        other => panic!("expected the frozen refusal, got {other:?}"),
    }
}

/// A `Write` to a table this session never `Ensure`d refuses with
/// `WriteGuard::check_write`'s frozen spelling — the wire enforcement of
/// the same write-before-ensure rule an in-process caller gets for free
/// (see `serve::destination`'s module doc on the trust-boundary split).
/// The session stays usable afterward.
#[tokio::test]
async fn a_write_before_ensure_refuses_with_the_frozen_spelling() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve_on::<EchoDestination>(&path).await.expect("bind");
    let channel = dial(&path).await;
    let mut connector = ConnectorClient::new(channel.clone());
    let mut destination = DestinationServiceClient::new(channel);

    handshake(&mut connector, false, false).await;

    let (req_tx, mut replies) = open_session(&mut destination).await;
    req_tx.send(open_frame("p", "l")).await.expect("send open");
    next_reply(&mut replies)
        .await
        .expect("reply")
        .expect("opened");

    req_tx
        .send(write_frame(ECHOED_TABLE, &[1]))
        .await
        .expect("send write before ensure");
    match next_reply(&mut replies)
        .await
        .expect("reply")
        .expect("frame")
        .reply
    {
        Some(session_reply::Reply::Error(error)) => {
            assert_eq!(error.classification, Classification::Fatal as i32);
            assert!(
                error
                    .message
                    .contains(&format!("write before ensure_table for `{ECHOED_TABLE}`")),
                "unexpected message: {}",
                error.message
            );
        }
        other => panic!("expected the write-before-ensure refusal, got {other:?}"),
    }

    req_tx.send(close_frame()).await.expect("send close");
    assert!(
        matches!(
            next_reply(&mut replies)
                .await
                .expect("reply")
                .expect("frame")
                .reply,
            Some(session_reply::Reply::Closed(_))
        ),
        "the session survives the refusal"
    );
}

/// A second `Open` frame and an entirely empty request frame
/// (`request: None` on the wire oneof) each refuse with their own
/// byte-exact spelling, and neither ends the session.
#[tokio::test]
async fn a_second_open_and_an_empty_frame_refuse_with_pinned_spellings() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve_on::<EchoDestination>(&path).await.expect("bind");
    let channel = dial(&path).await;
    let mut connector = ConnectorClient::new(channel.clone());
    let mut destination = DestinationServiceClient::new(channel);

    handshake(&mut connector, false, false).await;

    let (req_tx, mut replies) = open_session(&mut destination).await;
    req_tx.send(open_frame("p", "l")).await.expect("send open");
    next_reply(&mut replies)
        .await
        .expect("reply")
        .expect("opened");

    req_tx
        .send(open_frame("p", "l"))
        .await
        .expect("send second open");
    match next_reply(&mut replies)
        .await
        .expect("reply")
        .expect("frame")
        .reply
    {
        Some(session_reply::Reply::Error(error)) => {
            assert_eq!(error.classification, Classification::Fatal as i32);
            assert_eq!(
                error.message,
                "fatal destination error: a session accepts at most one Open frame, and it must be first"
            );
        }
        other => panic!("expected the already-open refusal, got {other:?}"),
    }

    req_tx
        .send(SessionRequest { request: None })
        .await
        .expect("send empty frame");
    match next_reply(&mut replies)
        .await
        .expect("reply")
        .expect("frame")
        .reply
    {
        Some(session_reply::Reply::Error(error)) => {
            assert_eq!(error.classification, Classification::Fatal as i32);
            assert_eq!(
                error.message,
                "the session received a request frame with no payload"
            );
        }
        other => panic!("expected the empty-frame refusal, got {other:?}"),
    }

    req_tx.send(close_frame()).await.expect("send close");
    assert!(
        matches!(
            next_reply(&mut replies)
                .await
                .expect("reply")
                .expect("frame")
                .reply,
            Some(session_reply::Reply::Closed(_))
        ),
        "the session survives both refusals"
    );
}

/// F3 fix pin (038 T5 review round 1): a `Write` frame whose Arrow IPC
/// stream carries a SECOND record batch refuses with its own distinct
/// spelling — not silently accepted with only the first batch written
/// (the defect this refusal exists to prevent: measured row loss
/// reported as success). No `write` call log entry exists, since the
/// decode failure precedes any `Backend::write` call.
#[tokio::test]
async fn a_multi_batch_write_frame_refuses_with_its_own_spelling() {
    echo::clear_call_log();
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve_on::<EchoDestination>(&path).await.expect("bind");
    let channel = dial(&path).await;
    let mut connector = ConnectorClient::new(channel.clone());
    let mut destination = DestinationServiceClient::new(channel);

    handshake(&mut connector, false, false).await;

    let (req_tx, mut replies) = open_session(&mut destination).await;
    req_tx.send(open_frame("p", "l")).await.expect("send open");
    next_reply(&mut replies)
        .await
        .expect("reply")
        .expect("opened");

    req_tx
        .send(ensure_frame(ECHOED_TABLE))
        .await
        .expect("send ensure");
    next_reply(&mut replies)
        .await
        .expect("reply")
        .expect("ensured");

    req_tx
        .send(write_frame_multi(
            ECHOED_TABLE,
            &[&[1, 2, 3], &[4, 5, 6, 7]],
        ))
        .await
        .expect("send multi-batch write");
    match next_reply(&mut replies)
        .await
        .expect("reply")
        .expect("frame")
        .reply
    {
        Some(session_reply::Reply::Error(error)) => {
            assert_eq!(error.classification, Classification::Fatal as i32);
            assert_eq!(
                error.message,
                "write carried more than one record batch; a Write frame is exactly one batch"
            );
        }
        other => panic!("expected the multi-batch refusal, got {other:?}"),
    }

    assert_eq!(
        echo::call_log_snapshot(),
        vec!["ensure_table".to_string()],
        "the refused write never reaches Backend::write"
    );
}

/// Minor 1 fix pin (038 T5 review round 1): bytes that are not a
/// decodable Arrow IPC stream at all refuse with the frozen prefix PLUS
/// the arrow error that actually caused it — not the bare prefix alone,
/// which would discard exactly the detail a connector author needs.
#[tokio::test]
async fn an_undecodable_write_frame_carries_the_arrow_cause() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve_on::<EchoDestination>(&path).await.expect("bind");
    let channel = dial(&path).await;
    let mut connector = ConnectorClient::new(channel.clone());
    let mut destination = DestinationServiceClient::new(channel);

    handshake(&mut connector, false, false).await;

    let (req_tx, mut replies) = open_session(&mut destination).await;
    req_tx.send(open_frame("p", "l")).await.expect("send open");
    next_reply(&mut replies)
        .await
        .expect("reply")
        .expect("opened");

    req_tx
        .send(ensure_frame(ECHOED_TABLE))
        .await
        .expect("send ensure");
    next_reply(&mut replies)
        .await
        .expect("reply")
        .expect("ensured");

    req_tx
        .send(write_frame_malformed(ECHOED_TABLE))
        .await
        .expect("send malformed write");
    match next_reply(&mut replies)
        .await
        .expect("reply")
        .expect("frame")
        .reply
    {
        Some(session_reply::Reply::Error(error)) => {
            assert_eq!(error.classification, Classification::Fatal as i32);
            const PREFIX: &str = "write carried no decodable record batch: ";
            assert!(
                error.message.starts_with(PREFIX),
                "expected the frozen prefix, got: {}",
                error.message
            );
            assert!(
                error.message.len() > PREFIX.len(),
                "expected the arrow cause appended after the prefix, got: {}",
                error.message
            );
        }
        other => panic!("expected the undecodable-write refusal, got {other:?}"),
    }
}

/// I1 fix pin (038 T5 review round 1, the 037 US2 T7 leak class
/// reopened for the wire): a session ABANDONED without ever sending
/// `Close` — both client halves simply dropped, the shape a crashed or
/// killed client leaves behind — still gets its backend closed.
/// `drive_session`'s F2 best-effort cleanup is what makes this true;
/// before this fix, letting the backend fall out of scope dropped it
/// WITHOUT ever calling `close()` on it. Polling with a timeout (not
/// asserting immediately) because the server task needs real scheduling
/// time after the drop to notice and run the cleanup — the same idiom
/// `test_serve_source`'s cancellation test uses.
#[tokio::test]
async fn an_abandoned_session_still_closes_the_backend() {
    echo::clear_call_log();
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve_on::<EchoDestination>(&path).await.expect("bind");
    let channel = dial(&path).await;
    let mut connector = ConnectorClient::new(channel.clone());
    let mut destination = DestinationServiceClient::new(channel);

    handshake(&mut connector, false, false).await;

    let (req_tx, mut replies) = open_session(&mut destination).await;
    req_tx.send(open_frame("p", "l")).await.expect("send open");
    next_reply(&mut replies)
        .await
        .expect("reply")
        .expect("opened");

    req_tx
        .send(ensure_frame(ECHOED_TABLE))
        .await
        .expect("send ensure");
    next_reply(&mut replies)
        .await
        .expect("reply")
        .expect("ensured");

    // Abandon the session: neither half ever sends `Close`.
    drop(req_tx);
    drop(replies);

    let observed = tokio::time::timeout(Duration::from_secs(5), async {
        while !echo::call_log_snapshot().contains(&"close".to_string()) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        observed.is_ok(),
        "the abandoned session's backend must still be closed, within the timeout"
    );
}

/// F5 fix pin (038 T5 review round 1): v0 allows exactly one live
/// session per connector process — a second concurrent `OpenSession`
/// while the first is still active refuses outright, at the RPC level
/// (`Status::failed_precondition`, not a `SessionReply`), with the
/// frozen wording.
#[tokio::test]
async fn a_second_concurrent_open_session_refuses_while_the_first_is_active() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve_on::<EchoDestination>(&path).await.expect("bind");
    let channel = dial(&path).await;
    let mut connector = ConnectorClient::new(channel.clone());
    let mut destination = DestinationServiceClient::new(channel);

    handshake(&mut connector, false, false).await;

    let (_req_tx, _replies) = open_session(&mut destination).await;

    let error = destination
        .open_session(ReceiverStream::new(tokio::sync::mpsc::channel(1).1))
        .await
        .expect_err("a second concurrent session must refuse");
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert_eq!(error.message(), "one session per connector process");
}

/// First wire-level crash test (038): with a session established and a
/// successful `Write` behind it, the underlying transport is severed
/// abruptly (no `Close`, no GOAWAY) — the client's next `recv` must
/// surface a genuine transport error rather than hang, or worse, look
/// identical to a clean end.
///
/// This does NOT abort `serve_on`'s `JoinHandle` to do it, and that is
/// a measured finding, not a stylistic choice: this workspace's pinned
/// tonic 0.14.6 spawns EACH accepted connection onto its own
/// independent `tokio::spawn`'d task deep inside
/// `serve_connection` (`transport/server/mod.rs`) — the `JoinHandle`
/// `serve_on` returns owns only the ACCEPT LOOP. An earlier version of
/// this test called `handle.abort()` after `Write` and it reliably
/// HUNG (proved red, not assumed): the already-established session's
/// task keeps running untouched, because tonic never exposes a handle
/// reaching that far down. What DOES reliably sever an established
/// connection, with no `unsafe` and no production-code changes: a safe
/// `dup()` of the CLIENT's own connecting socket
/// (`BorrowedFd::try_clone_to_owned`, stable std), captured by
/// `dial_severable` at connect time, then `shutdown(Both)`'d here —
/// collapsing the h2 connection out from under the client's own
/// machinery exactly as abruptly as a vanished peer would. `handle.
/// abort()` is still called too, alongside it: it does not reach the
/// live session, but it IS still the correct half of "make the server
/// go away" for the accept loop.
///
/// The TRUE process-kill matrix — SIGKILL against a real out-of-process
/// connector mid-session — is feature 040's conformance kit to build
/// (ADR 0001 D8); this is the wire-level approximation available
/// in-process today.
#[tokio::test]
async fn a_severed_transport_mid_session_surfaces_as_a_client_error() {
    let (_dir, path) = socket_path();
    let (_line, handle) = serve_on::<EchoDestination>(&path).await.expect("bind");
    let (channel, severable) = dial_severable(&path).await;
    let mut connector = ConnectorClient::new(channel.clone());
    let mut destination = DestinationServiceClient::new(channel);

    handshake(&mut connector, false, false).await;

    let (req_tx, mut replies) = open_session(&mut destination).await;

    req_tx.send(open_frame("p", "l")).await.expect("send open");
    next_reply(&mut replies)
        .await
        .expect("reply")
        .expect("opened");

    req_tx
        .send(ensure_frame(ECHOED_TABLE))
        .await
        .expect("send ensure");
    next_reply(&mut replies)
        .await
        .expect("reply")
        .expect("ensured");

    req_tx
        .send(write_frame(ECHOED_TABLE, &[1]))
        .await
        .expect("send write");
    next_reply(&mut replies)
        .await
        .expect("reply")
        .expect("written");

    handle.abort();
    severable
        .shutdown(std::net::Shutdown::Both)
        .expect("sever the transport");

    let outcome = tokio::time::timeout(Duration::from_secs(5), replies.message()).await;
    match outcome {
        Ok(result) => assert!(
            result.is_err(),
            "a severed transport surfaces as an error, not a clean end: {result:?}"
        ),
        Err(_) => panic!("the client's next recv hung instead of observing the severed transport"),
    }
}
