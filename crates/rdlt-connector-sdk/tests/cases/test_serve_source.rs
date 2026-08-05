//! `serve::source` end to end: a raw tonic client dials the UDS
//! `serve_on` binds — the same tonic-UDS idiom a real out-of-process
//! host would use (research.md #3:
//! `Endpoint::try_from(..).connect_with_connector(service_fn(..))`) —
//! and drives Handshake/Streams/Read against the echo connector.
//!
//! `serve_on` (not the print-and-block `source()`) is the seam these
//! tests use: it returns the [`Line`] instead of printing it, so the
//! RENDERED line is asserted directly rather than through stdout
//! capture, which is not reliably interceptable in-process.

use std::path::PathBuf;

use rdlt_connector_protocol::proto::connector_client::ConnectorClient;
use rdlt_connector_protocol::proto::source_service_client::SourceServiceClient;
use rdlt_connector_protocol::proto::{
    Classification, HandshakeRequest, ReadRequest, StreamsRequest, handshake_reply, read_frame,
    streams_reply,
};
use rdlt_connector_sdk::serve::source::serve_on;
use tonic::transport::{Channel, Endpoint};

use super::support::echo::EchoSource;

/// A per-test-name, per-process socket path — unique enough that
/// concurrently running tests (nextest isolates by process anyway, but
/// plain `cargo test` does not) never collide.
fn socket_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("rdlt-sdk-test-{}-{name}.sock", std::process::id()))
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

/// A full pass: handshake ok, `Streams` lists `numbers`, `Read` streams
/// N `raw_json` frames (one per row, in order) plus exactly one
/// checkpoint frame.
#[tokio::test]
async fn handshake_streams_and_read_round_trip() {
    let path = socket_path("round-trip");
    let (line, _handle) = serve_on::<EchoSource>(&path).await.expect("bind");
    assert_eq!(line.socket_path, path);
    assert_eq!(line.proto_min, 0);
    assert_eq!(line.proto_max, 0);

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
/// spelling — the mirrored spelling lives on the (future) destination
/// side.
#[tokio::test]
async fn wrong_role_handshake_refuses_with_the_frozen_spelling() {
    let path = socket_path("wrong-role");
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

/// An invalid config document refuses FATAL, carrying the `Document`'s
/// own validate wording verbatim — nothing the serve layer adds.
#[tokio::test]
async fn an_invalid_config_refuses_fatal_with_the_documents_own_wording() {
    let path = socket_path("bad-config");
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

/// A protocol version outside `[proto_min, proto_max]` refuses FATAL.
#[tokio::test]
async fn an_out_of_range_protocol_version_refuses_fatal() {
    let path = socket_path("bad-version");
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
            assert!(error.message.contains("99"), "{}", error.message);
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
}

/// A second handshake on an already-populated session refuses typed,
/// whether or not it repeats the same (otherwise valid) request.
#[tokio::test]
async fn a_second_handshake_refuses() {
    let path = socket_path("second-handshake");
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

/// A connector read that fails forwards exactly one terminal `ErrorFrame`
/// — no rows, no checkpoint, classification/message taken from the
/// `SourceError` itself, and nothing follows it on the stream.
#[tokio::test]
async fn a_failed_read_forwards_one_terminal_error_frame() {
    let path = socket_path("fail-read");
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
