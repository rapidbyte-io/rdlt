//! `serve::destination` end to end: a raw tonic client dials the UDS
//! `serve_on` binds and drives `OpenSession` — one bidi stream carrying
//! the whole session — against the echo destination.
//!
//! `dial`/`socket_path` mirror `test_serve_source`'s identical helpers
//! (see there for why `serve_on`, not the print-and-block `destination`,
//! is the seam these tests use).

use std::os::fd::AsFd;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rdlt_connector::DestinationCapabilities;
use rdlt_connector::core::{LoadId, PipelineId, WriteMode};
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

fn echo_destination_config(fail_publish: bool) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({"fail_publish": fail_publish}))
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

/// The full choreography against a raw tonic client: handshake
/// (`capabilities_json` round-trips), Open, Ensure, Write, ExistingReceipt
/// (→ `None` — see `serve::destination`'s module doc for why), Publish
/// (→ `part_closed` BEFORE `published`, pinning the interleave the
/// callback's synchronicity promises), ReadState (→ `None`), Close (→
/// clean end) — and the echo backend's own call log proves the SDK's
/// session choreography ran underneath, not just that replies arrived.
#[tokio::test]
async fn the_full_choreography_pins_part_closed_before_published() {
    echo::clear_call_log();
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve_on::<EchoDestination>(&path).await.expect("bind");
    let channel = dial(&path).await;
    let mut connector = ConnectorClient::new(channel.clone());
    let mut destination = DestinationServiceClient::new(channel);

    let handshake = connector
        .handshake(HandshakeRequest {
            protocol_version: 0,
            expected_role: "destination".to_string(),
            config_json: echo_destination_config(false),
        })
        .await
        .expect("handshake rpc")
        .into_inner();
    match handshake.outcome {
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
                "the sdk-wrapped session has no standalone receipt lookup — always None"
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
        "the wire ExistingReceipt frame never touches the backend — only \
         commit's OWN internal lookup (inside Publish) does"
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

    connector
        .handshake(HandshakeRequest {
            protocol_version: 0,
            expected_role: "destination".to_string(),
            config_json: echo_destination_config(true),
        })
        .await
        .expect("handshake rpc");

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

    connector
        .handshake(HandshakeRequest {
            protocol_version: 0,
            expected_role: "destination".to_string(),
            config_json: echo_destination_config(false),
        })
        .await
        .expect("handshake rpc");

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

    connector
        .handshake(HandshakeRequest {
            protocol_version: 0,
            expected_role: "destination".to_string(),
            config_json: echo_destination_config(false),
        })
        .await
        .expect("handshake rpc");

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
