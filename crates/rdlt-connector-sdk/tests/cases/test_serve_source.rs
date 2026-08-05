//! `serve::source` end to end: a raw tonic client dials the UDS
//! `serve_on` binds — the same tonic-UDS idiom a real out-of-process
//! host would use (research.md #3:
//! `Endpoint::try_from(..).connect_with_connector(service_fn(..))`) —
//! and drives Handshake/Streams/Read against the echo connector.
//!
//! `serve_on` (not the print-and-block `source()`) is the seam these
//! tests use: it returns the [`Line`] instead of printing it, so the
//! rendered line is asserted directly (`Line::render()`, exercised in
//! the round-trip test below) rather than through stdout capture, which
//! is not reliably interceptable in-process.

use std::path::PathBuf;

use rdlt_connector_protocol::proto::connector_client::ConnectorClient;
use rdlt_connector_protocol::proto::source_service_client::SourceServiceClient;
use rdlt_connector_protocol::proto::{
    Classification, HandshakeRequest, ReadRequest, StreamsRequest, handshake_reply, read_frame,
    streams_reply,
};
use rdlt_connector_sdk::serve::source::serve_on;
use tonic::transport::{Channel, Endpoint};

use super::support::echo::{self, EchoSource};

/// A fresh temp directory plus a short, fixed socket path inside it —
/// used for every test's UDS listener. `tempfile::tempdir()`, not
/// `std::env::temp_dir()` plus a hand-rolled unique name: the directory
/// (and the socket file living in it) is reclaimed on drop, so a run
/// does not litter the shared system temp directory with abandoned
/// `.sock` files. The `TempDir` must outlive the listener — callers
/// bind it to a variable rather than discarding it.
fn socket_path() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("connector.sock");
    (dir, path)
}

/// Dial `path` the way a spawning host dials the handshake line's
/// `socket_path`: an arbitrary placeholder URI (the connector below
/// ignores it — every connection goes to the UDS) wrapped over
/// `tokio::net::UnixStream` via `hyper_util`'s IO adapter.
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

fn echo_config(rows: u64, fail_read: bool) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({"rows": rows, "fail_read": fail_read}))
        .expect("echo config serializes")
}

fn numbers_stream_spec_json() -> Vec<u8> {
    serde_json::to_vec(&rdlt_connector::StreamSpec::new("numbers")).expect("stream spec json")
}

/// A full pass: handshake ok (and the `Line` it was reached through
/// renders the frozen spelling), `Streams` lists `numbers`, `Read`
/// streams N `raw_json` frames (one per row, in order) plus exactly one
/// checkpoint frame.
#[tokio::test]
async fn handshake_streams_and_read_round_trip() {
    let (_dir, path) = socket_path();
    let (line, _handle) = serve_on::<EchoSource>(&path).await.expect("bind");
    assert_eq!(line.socket_path, path);
    assert_eq!(line.proto_min, 0);
    assert_eq!(line.proto_max, 0);
    assert_eq!(
        line.render(),
        format!("rdlt-connector|1|0|0|{}", path.to_string_lossy()),
        "the rendered line carries the frozen five-field spelling"
    );

    let channel = dial(&path).await;
    let mut connector = ConnectorClient::new(channel.clone());
    let mut source = SourceServiceClient::new(channel);

    let reply = connector
        .handshake(HandshakeRequest {
            protocol_version: 0,
            expected_role: "source".to_string(),
            config_json: echo_config(3, false),
        })
        .await
        .expect("handshake rpc")
        .into_inner();
    match reply.outcome {
        Some(handshake_reply::Outcome::Ok(ok)) => {
            assert_eq!(ok.connector_id, "echo-source");
            assert_eq!(ok.connector_version, "0.0.0");
        }
        other => panic!("expected handshake ok, got {other:?}"),
    }

    let streams = source
        .streams(StreamsRequest {})
        .await
        .expect("streams rpc")
        .into_inner();
    let names: Vec<String> = match streams.outcome {
        Some(streams_reply::Outcome::Ok(list)) => list
            .stream_spec_json
            .iter()
            .map(|bytes| {
                let spec: rdlt_connector::StreamSpec =
                    serde_json::from_slice(bytes).expect("stream spec json");
                spec.name.as_str().to_string()
            })
            .collect(),
        other => panic!("expected streams ok, got {other:?}"),
    };
    assert_eq!(names, vec!["numbers".to_string()]);

    let mut frames = source
        .read(ReadRequest {
            stream_spec_json: numbers_stream_spec_json(),
            since_cursor_json: None,
        })
        .await
        .expect("read rpc")
        .into_inner();

    let mut rows = Vec::new();
    let mut checkpoints = 0;
    while let Some(frame) = frames.message().await.expect("frame") {
        match frame.frame {
            Some(read_frame::Frame::RawJson(bytes)) => {
                let row: serde_json::Value = serde_json::from_slice(&bytes).expect("row json");
                rows.push(row);
            }
            Some(read_frame::Frame::CheckpointCursorJson(bytes)) => {
                checkpoints += 1;
                let cursor: serde_json::Value =
                    serde_json::from_slice(&bytes).expect("checkpoint json");
                assert_eq!(cursor, serde_json::json!({"n": 2}));
            }
            other => panic!("unexpected frame: {other:?}"),
        }
    }
    assert_eq!(
        rows,
        vec![
            serde_json::json!({"n": 0}),
            serde_json::json!({"n": 1}),
            serde_json::json!({"n": 2}),
        ],
        "N raw_json frames, one per row, in order"
    );
    assert_eq!(checkpoints, 1, "exactly one checkpoint frame");
}

/// A handshake asking for the wrong role refuses with the frozen
/// spelling — the mirrored spelling is pinned on the destination side by
/// `test_serve_destination::handshake_refusal_matrix_pins_every_remaining_arm`
/// (038 T6; this comment used to say "future destination side" before
/// that test existed).
#[tokio::test]
async fn wrong_role_handshake_refuses_with_the_frozen_spelling() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve_on::<EchoSource>(&path).await.expect("bind");
    let mut connector = ConnectorClient::new(dial(&path).await);

    let reply = connector
        .handshake(HandshakeRequest {
            protocol_version: 0,
            expected_role: "destination".to_string(),
            config_json: echo_config(3, false),
        })
        .await
        .expect("handshake rpc")
        .into_inner();
    match reply.outcome {
        Some(handshake_reply::Outcome::Error(error)) => {
            assert_eq!(error.classification, Classification::Fatal as i32);
            assert_eq!(
                error.message,
                "this connector is a source; the handshake asked for a destination"
            );
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
}

/// A handshake asking for a role the protocol doesn't define at all
/// (a typo, or skew against a future version that added one) gets its
/// own wording, distinct from the source/destination mismatch above —
/// worded around the bogus request rather than around what this
/// connector actually is.
#[tokio::test]
async fn unrecognized_role_handshake_refuses_with_its_own_spelling() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve_on::<EchoSource>(&path).await.expect("bind");
    let mut connector = ConnectorClient::new(dial(&path).await);

    let reply = connector
        .handshake(HandshakeRequest {
            protocol_version: 0,
            expected_role: "orchestrator".to_string(),
            config_json: echo_config(3, false),
        })
        .await
        .expect("handshake rpc")
        .into_inner();
    match reply.outcome {
        Some(handshake_reply::Outcome::Error(error)) => {
            assert_eq!(error.classification, Classification::Fatal as i32);
            assert_eq!(
                error.message,
                "the handshake asked for role `orchestrator`, which this connector does not recognize"
            );
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
}

/// An invalid config document refuses FATAL, carrying the `Document`'s
/// own validate wording verbatim — nothing the serve layer adds.
#[tokio::test]
async fn an_invalid_config_refuses_fatal_with_the_documents_own_wording() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve_on::<EchoSource>(&path).await.expect("bind");
    let mut connector = ConnectorClient::new(dial(&path).await);

    let reply = connector
        .handshake(HandshakeRequest {
            protocol_version: 0,
            expected_role: "source".to_string(),
            config_json: echo_config(0, false),
        })
        .await
        .expect("handshake rpc")
        .into_inner();
    match reply.outcome {
        Some(handshake_reply::Outcome::Error(error)) => {
            assert_eq!(error.classification, Classification::Fatal as i32);
            assert_eq!(error.message, "echo: rows must be > 0");
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
}

/// `config_json` that is not even valid JSON (never mind failing
/// `Document::validate`) refuses FATAL too — the arm
/// `serde_json::from_slice` itself trips inside `common::handshake`,
/// before a `Document` is ever constructed. Only the frozen
/// `invalid config_json: ` prefix is ours; the rest is `serde_json`'s
/// own error text, so this asserts a fragment rather than full-string —
/// the same discipline the destination-role twin of this row uses in
/// `test_serve_destination::handshake_refusal_matrix_pins_every_remaining_arm`
/// (038 T6 fix round 1: an earlier report of this task falsely claimed
/// this test already existed here; it did not — this is the first real
/// source-side pin of this row).
#[tokio::test]
async fn an_undecodable_config_json_refuses_fatal_with_the_frozen_prefix() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve_on::<EchoSource>(&path).await.expect("bind");
    let mut connector = ConnectorClient::new(dial(&path).await);

    let reply = connector
        .handshake(HandshakeRequest {
            protocol_version: 0,
            expected_role: "source".to_string(),
            config_json: b"{ this is not json".to_vec(),
        })
        .await
        .expect("handshake rpc")
        .into_inner();
    match reply.outcome {
        Some(handshake_reply::Outcome::Error(error)) => {
            assert_eq!(error.classification, Classification::Fatal as i32);
            assert!(
                error.message.starts_with("invalid config_json: "),
                "expected the frozen prefix, got: {}",
                error.message
            );
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
}

/// A protocol version outside `[proto_min, proto_max]` refuses FATAL —
/// the message pinned byte-exact, like its three siblings above. (No
/// "below min" sibling exists — see
/// `test_serve_destination::handshake_refusal_matrix_pins_every_remaining_arm`'s
/// own doc comment for why v0 has no legal input to construct one.)
#[tokio::test]
async fn an_out_of_range_protocol_version_refuses_fatal() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve_on::<EchoSource>(&path).await.expect("bind");
    let mut connector = ConnectorClient::new(dial(&path).await);

    let reply = connector
        .handshake(HandshakeRequest {
            protocol_version: 99,
            expected_role: "source".to_string(),
            config_json: echo_config(3, false),
        })
        .await
        .expect("handshake rpc")
        .into_inner();
    match reply.outcome {
        Some(handshake_reply::Outcome::Error(error)) => {
            assert_eq!(error.classification, Classification::Fatal as i32);
            assert_eq!(
                error.message,
                "protocol version 99 is outside this connector's supported range [0, 0]"
            );
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
}

/// A second handshake on an already-populated session refuses typed,
/// whether or not it repeats the same (otherwise valid) request.
#[tokio::test]
async fn a_second_handshake_refuses() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve_on::<EchoSource>(&path).await.expect("bind");
    let mut connector = ConnectorClient::new(dial(&path).await);

    let first = connector
        .handshake(HandshakeRequest {
            protocol_version: 0,
            expected_role: "source".to_string(),
            config_json: echo_config(3, false),
        })
        .await
        .expect("first handshake rpc")
        .into_inner();
    assert!(matches!(
        first.outcome,
        Some(handshake_reply::Outcome::Ok(_))
    ));

    let second = connector
        .handshake(HandshakeRequest {
            protocol_version: 0,
            expected_role: "source".to_string(),
            config_json: echo_config(3, false),
        })
        .await
        .expect("second handshake rpc")
        .into_inner();
    match second.outcome {
        Some(handshake_reply::Outcome::Error(error)) => {
            assert_eq!(error.classification, Classification::Fatal as i32);
            assert_eq!(error.message, "handshake already completed");
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
}

/// `Streams` before a handshake refuses as a raw `Status`, not an
/// `ErrorFrame` — unlike a classified refusal (wrong role, bad config,
/// a failing check/read), which the wire carries as reply-payload state
/// so a caller can inspect it uniformly, "you never handshook" is a
/// client-protocol violation the RPC layer rejects directly. 038 T6
/// recorded this Status-vs-ErrorFrame split as a DELIBERATE rule rather
/// than an inconsistency to unify away — see `serve::mod`'s module doc
/// (`rdlt-connector-sdk::serve`) for the full rule this test pins one
/// instance of.
#[tokio::test]
async fn streams_before_a_handshake_refuses_as_a_status() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve_on::<EchoSource>(&path).await.expect("bind");
    let mut source = SourceServiceClient::new(dial(&path).await);

    let error = source
        .streams(StreamsRequest {})
        .await
        .expect_err("streams before a handshake must refuse");
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert_eq!(error.message(), "handshake has not completed");
}

/// `Read` before a handshake refuses the same way `Streams` does — see
/// the comment there for the Status-vs-ErrorFrame note.
#[tokio::test]
async fn read_before_a_handshake_refuses_as_a_status() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve_on::<EchoSource>(&path).await.expect("bind");
    let mut source = SourceServiceClient::new(dial(&path).await);

    let error = source
        .read(ReadRequest {
            stream_spec_json: numbers_stream_spec_json(),
            since_cursor_json: None,
        })
        .await
        .expect_err("read before a handshake must refuse");
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert_eq!(error.message(), "handshake has not completed");
}

/// B2 fix pin (038 review round 1): an undecodable `stream_spec_json`
/// answers INSIDE the response stream — the `Read` RPC itself completes
/// normally, and the stream's first and only frame is a terminal FATAL
/// `ErrorFrame` with the frozen `invalid stream_spec_json: ` prefix
/// (the rest is serde's own text, so a fragment is asserted, same
/// discipline as the config-decode rows). Pre-fix this answered
/// `Status::invalid_argument` — a THIRD refusal shape the
/// Status-vs-ErrorFrame rule (serve/mod.rs; the protocol README)
/// forbids, while the destination side's `*_json` decode failures
/// already rode `ErrorFrame`.
#[tokio::test]
async fn an_undecodable_stream_spec_answers_a_terminal_error_frame_not_a_status() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve_on::<EchoSource>(&path).await.expect("bind");
    let channel = dial(&path).await;
    let mut connector = ConnectorClient::new(channel.clone());
    let mut source = SourceServiceClient::new(channel);

    connector
        .handshake(HandshakeRequest {
            protocol_version: 0,
            expected_role: "source".to_string(),
            config_json: echo_config(3, false),
        })
        .await
        .expect("handshake rpc");

    let mut frames = source
        .read(ReadRequest {
            stream_spec_json: b"{ this is not json".to_vec(),
            since_cursor_json: None,
        })
        .await
        .expect("the Read RPC completes normally — the refusal is IN the stream")
        .into_inner();

    let frame = frames
        .message()
        .await
        .expect("frame")
        .expect("one terminal frame");
    match frame.frame {
        Some(read_frame::Frame::Error(error)) => {
            assert_eq!(error.classification, Classification::Fatal as i32);
            assert!(
                error.message.starts_with("invalid stream_spec_json: "),
                "expected the frozen prefix, got: {}",
                error.message
            );
        }
        other => panic!("expected a terminal error frame, got {other:?}"),
    }
    assert!(
        frames.message().await.expect("stream ends").is_none(),
        "nothing follows the terminal error"
    );
}

/// The `since_cursor_json` twin of the test above — same rule, same
/// shape, its own frozen prefix. The stream spec is VALID here, so the
/// cursor decode is provably the arm that refused.
#[tokio::test]
async fn an_undecodable_since_cursor_answers_a_terminal_error_frame_not_a_status() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve_on::<EchoSource>(&path).await.expect("bind");
    let channel = dial(&path).await;
    let mut connector = ConnectorClient::new(channel.clone());
    let mut source = SourceServiceClient::new(channel);

    connector
        .handshake(HandshakeRequest {
            protocol_version: 0,
            expected_role: "source".to_string(),
            config_json: echo_config(3, false),
        })
        .await
        .expect("handshake rpc");

    let mut frames = source
        .read(ReadRequest {
            stream_spec_json: numbers_stream_spec_json(),
            since_cursor_json: Some(b"{ this is not json".to_vec()),
        })
        .await
        .expect("the Read RPC completes normally — the refusal is IN the stream")
        .into_inner();

    let frame = frames
        .message()
        .await
        .expect("frame")
        .expect("one terminal frame");
    match frame.frame {
        Some(read_frame::Frame::Error(error)) => {
            assert_eq!(error.classification, Classification::Fatal as i32);
            assert!(
                error.message.starts_with("invalid since_cursor_json: "),
                "expected the frozen prefix, got: {}",
                error.message
            );
        }
        other => panic!("expected a terminal error frame, got {other:?}"),
    }
    assert!(
        frames.message().await.expect("stream ends").is_none(),
        "nothing follows the terminal error"
    );
}

/// A connector read that fails forwards exactly one terminal `ErrorFrame`
/// — no rows, no checkpoint, classification/message taken from the
/// `SourceError` itself, and nothing follows it on the stream.
#[tokio::test]
async fn a_failed_read_forwards_one_terminal_error_frame() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve_on::<EchoSource>(&path).await.expect("bind");
    let channel = dial(&path).await;
    let mut connector = ConnectorClient::new(channel.clone());
    let mut source = SourceServiceClient::new(channel);

    connector
        .handshake(HandshakeRequest {
            protocol_version: 0,
            expected_role: "source".to_string(),
            config_json: echo_config(5, true),
        })
        .await
        .expect("handshake rpc");

    let mut frames = source
        .read(ReadRequest {
            stream_spec_json: numbers_stream_spec_json(),
            since_cursor_json: None,
        })
        .await
        .expect("read rpc")
        .into_inner();

    let frame = frames
        .message()
        .await
        .expect("frame")
        .expect("one terminal frame");
    match frame.frame {
        Some(read_frame::Frame::Error(error)) => {
            assert_eq!(error.classification, Classification::Fatal as i32);
            assert_eq!(
                error.message,
                "fatal source error: echo: induced read failure"
            );
        }
        other => panic!("expected a terminal error frame, got {other:?}"),
    }
    assert!(
        frames.message().await.expect("stream ends").is_none(),
        "nothing follows the terminal error"
    );
}

/// The cancellation chain, load-bearing for 039's out-of-process
/// adapter: a client that drops its `Read` response stream mid-flight
/// must not leave the connector's producer running forever.
/// `rows: 10_000_000` keeps `EchoSource` producing well past the drop;
/// pulling a few frames first proves the producer is actually running
/// ahead of the client (not merely parked before its first push) when
/// the drop happens. `EchoSource` flips `echo::break_observed()` the
/// moment its push observes `ControlFlow::Break` — this is what makes
/// the forwarder's `records_in.close()` on a failed send deletion-red:
/// remove that close and this test hangs to its timeout instead of
/// passing.
#[tokio::test]
async fn a_dropped_response_stream_cancels_the_connector_within_a_timeout() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve_on::<EchoSource>(&path).await.expect("bind");
    let channel = dial(&path).await;
    let mut connector = ConnectorClient::new(channel.clone());
    let mut source = SourceServiceClient::new(channel);

    connector
        .handshake(HandshakeRequest {
            protocol_version: 0,
            expected_role: "source".to_string(),
            config_json: echo_config(10_000_000, false),
        })
        .await
        .expect("handshake rpc");

    let mut frames = source
        .read(ReadRequest {
            stream_spec_json: numbers_stream_spec_json(),
            since_cursor_json: None,
        })
        .await
        .expect("read rpc")
        .into_inner();

    for _ in 0..3 {
        frames
            .message()
            .await
            .expect("frame")
            .expect("a row frame before the drop");
    }

    drop(frames);

    let observed = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !echo::break_observed() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await;

    assert!(
        observed.is_ok(),
        "the connector's read task must observe Break within the timeout, not hang"
    );
}
