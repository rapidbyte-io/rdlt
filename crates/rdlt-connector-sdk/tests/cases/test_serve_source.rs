//! `serve::source` end to end: a raw tonic client dials the UDS
//! `run_on` binds — the same tonic-UDS idiom a real out-of-process host
//! uses (`Endpoint::try_from(..).connect_with_connector(service_fn(..))`)
//! — and drives Handshake/Streams/Read against the echo connector.
//!
//! `run_on` (not the print-and-block `run()`) is the seam these tests
//! use: it returns the [`Line`] instead of printing it, so the rendered
//! line is asserted directly rather than through stdout capture, which
//! is not reliably interceptable in-process.

use std::path::PathBuf;

use rdlt_connector_protocol::PROTOCOL_VERSION;
use rdlt_connector_protocol::proto::connector_client::ConnectorClient;
use rdlt_connector_protocol::proto::source_service_client::SourceServiceClient;
use rdlt_connector_protocol::proto::{
    Classification, HandshakeRequest, ReadRequest, SpecRequest, StreamsRequest, handshake_reply,
    read_frame, streams_reply,
};
use rdlt_connector_sdk::serve::source::{
    BYTE_FRAME_BUDGET, MAX_CONCURRENT_READS, READ_CHANNEL_BUDGET, run_on,
};
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

/// The same config with every push sized to `push_bytes` — the sized
/// half of the echo source, used by the frame-budget test below.
fn sized_echo_config(rows: u64, push_bytes: usize) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({"rows": rows, "push_bytes": push_bytes}))
        .expect("echo config serializes")
}

fn numbers_stream_spec_json() -> Vec<u8> {
    serde_json::to_vec(&rdlt_connector::source::StreamSpec::new("numbers"))
        .expect("stream spec json")
}

/// A full pass: handshake ok (and the `Line` it was reached through
/// renders the frozen spelling), `Streams` lists `numbers`, `Read`
/// streams N `raw_json` frames (one per row, in order) plus exactly one
/// checkpoint frame.
#[tokio::test]
async fn handshake_streams_and_read_round_trip() {
    let (_dir, path) = socket_path();
    let (line, _handle) = run_on::<EchoSource>(&path).await.expect("bind");
    assert_eq!(line.socket_path, path);
    assert_eq!(line.protocol_min, PROTOCOL_VERSION);
    assert_eq!(line.protocol_max, PROTOCOL_VERSION);
    assert_eq!(
        line.render(),
        format!(
            "rdlt-connector|1|{PROTOCOL_VERSION}|{PROTOCOL_VERSION}|{}",
            path.to_string_lossy()
        ),
        "the rendered line carries the frozen five-field spelling"
    );

    let channel = dial(&path).await;
    let mut connector = ConnectorClient::new(channel.clone());
    let mut source = SourceServiceClient::new(channel);

    let reply = connector
        .handshake(HandshakeRequest {
            protocol_version: PROTOCOL_VERSION,
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
        // The framing rule at the reading seat: one JSON document per
        // line, no trailing newline.
        Some(streams_reply::Outcome::Ok(list)) => list
            .stream_specs_jsonl
            .split(|byte| *byte == b'\n')
            .map(|line| {
                let spec: rdlt_connector::source::StreamSpec =
                    serde_json::from_slice(line).expect("stream spec json");
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
/// `test_serve_destination::handshake_refusal_matrix_pins_every_remaining_arm`.
#[tokio::test]
async fn wrong_role_handshake_refuses_with_the_frozen_spelling() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = run_on::<EchoSource>(&path).await.expect("bind");
    let mut connector = ConnectorClient::new(dial(&path).await);

    let reply = connector
        .handshake(HandshakeRequest {
            protocol_version: PROTOCOL_VERSION,
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
    let (_line, _handle) = run_on::<EchoSource>(&path).await.expect("bind");
    let mut connector = ConnectorClient::new(dial(&path).await);

    let reply = connector
        .handshake(HandshakeRequest {
            protocol_version: PROTOCOL_VERSION,
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

/// An invalid config document refuses FATAL, preserving the validation
/// wording while redacting scalar values repeated from the document.
#[tokio::test]
async fn an_invalid_config_refuses_fatal_with_scalar_values_redacted() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = run_on::<EchoSource>(&path).await.expect("bind");
    let mut connector = ConnectorClient::new(dial(&path).await);

    let reply = connector
        .handshake(HandshakeRequest {
            protocol_version: PROTOCOL_VERSION,
            expected_role: "source".to_string(),
            config_json: echo_config(0, false),
        })
        .await
        .expect("handshake rpc")
        .into_inner();
    match reply.outcome {
        Some(handshake_reply::Outcome::Error(error)) => {
            assert_eq!(error.classification, Classification::Fatal as i32);
            assert_eq!(
                error.message,
                "echo: rows must be > [redacted config value]"
            );
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
}

/// `config_json` that is not even valid JSON (never mind failing
/// `Document::validate`) refuses FATAL too — the arm
/// `serde_json::from_slice` itself trips inside `wire::handshake`,
/// before a `Document` is ever constructed. The frozen
/// `invalid config_json: ` prefix is ours; the rest is the error's
/// kind and location (never serde's own text, which the secrecy pins
/// below hold), so this asserts the prefix rather than full-string —
/// the same discipline the destination-role twin of this row uses.
#[tokio::test]
async fn an_undecodable_config_json_refuses_fatal_with_the_frozen_prefix() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = run_on::<EchoSource>(&path).await.expect("bind");
    let mut connector = ConnectorClient::new(dial(&path).await);

    let reply = connector
        .handshake(HandshakeRequest {
            protocol_version: PROTOCOL_VERSION,
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

/// A `config_json` above the 8 MiB document ceiling refuses FATAL by
/// SIZE, before any parse: the 64 MiB frame cap bounds only wire bytes,
/// and a compact document materializes as an untyped `Value` at many
/// times its wire size inside the connector process. The payload is
/// deliberately VALID JSON — a parse error here would mean the parse
/// arm refused it, not the ceiling.
#[tokio::test]
async fn an_oversized_config_json_refuses_fatal_by_size_before_any_parse() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = run_on::<EchoSource>(&path).await.expect("bind");
    let mut connector = ConnectorClient::new(dial(&path).await);

    // One byte past the ceiling: `[0,0,0,…]`, padded to exact size with
    // a trailing string element.
    let ceiling = 8 * 1024 * 1024;
    let mut config_json = Vec::with_capacity(ceiling + 1);
    config_json.push(b'[');
    while config_json.len() < ceiling - 8 {
        config_json.extend_from_slice(b"0,");
    }
    config_json.push(b'"');
    config_json.resize(ceiling, b'x');
    config_json.extend_from_slice(b"\"]");
    assert!(config_json.len() > ceiling);
    assert!(serde_json::from_slice::<serde_json::Value>(&config_json).is_ok());

    let len = config_json.len();
    let reply = connector
        .handshake(HandshakeRequest {
            protocol_version: PROTOCOL_VERSION,
            expected_role: "source".to_string(),
            config_json,
        })
        .await
        .expect("handshake rpc")
        .into_inner();
    match reply.outcome {
        Some(handshake_reply::Outcome::Error(error)) => {
            assert_eq!(error.classification, Classification::Fatal as i32);
            assert_eq!(
                error.message,
                format!(
                    "config_json is {len} bytes — larger than the 8388608-byte document \
                     ceiling; a config or cursor document measures in kilobytes, so a payload \
                     this size is a wrong path, refused before it can expand in memory"
                )
            );
        }
        other => panic!("expected a size refusal, got {other:?}"),
    }
}

/// A peer still speaking the RETIRED protocol version refuses loudly at
/// the handshake — the version bump is what keeps a skewed old binary
/// from silently mis-decoding the fields the new version reshaped (the
/// streams blob, the state-format document). The doc-comment on
/// "below min": with the bump, a legal below-min input now EXISTS, and
/// this is it.
#[tokio::test]
async fn a_retired_protocol_version_peer_refuses_loudly() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = run_on::<EchoSource>(&path).await.expect("bind");
    let mut connector = ConnectorClient::new(dial(&path).await);

    let reply = connector
        .handshake(HandshakeRequest {
            protocol_version: PROTOCOL_VERSION - 1,
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
                format!(
                    "protocol version {} is outside this connector's supported range \
                     [{PROTOCOL_VERSION}, {PROTOCOL_VERSION}]",
                    PROTOCOL_VERSION - 1
                )
            );
        }
        other => panic!("expected the version refusal, got {other:?}"),
    }
}

/// A protocol version outside `[protocol_min, protocol_max]` refuses FATAL —
/// the message pinned byte-exact, like its three siblings above. (No
/// "below min" sibling exists — see
/// `test_serve_destination::handshake_refusal_matrix_pins_every_remaining_arm`'s
/// own doc comment for why the ORIGINAL wire had no legal input to
/// construct one — the version bump minted one, pinned above.)
#[tokio::test]
async fn an_out_of_range_protocol_version_refuses_fatal() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = run_on::<EchoSource>(&path).await.expect("bind");
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
                format!(
                    "protocol version 99 is outside this connector's supported range \
                     [{PROTOCOL_VERSION}, {PROTOCOL_VERSION}]"
                )
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
    let (_line, _handle) = run_on::<EchoSource>(&path).await.expect("bind");
    let mut connector = ConnectorClient::new(dial(&path).await);

    let first = connector
        .handshake(HandshakeRequest {
            protocol_version: PROTOCOL_VERSION,
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
            protocol_version: PROTOCOL_VERSION,
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
/// client-protocol violation the RPC layer rejects directly — the
/// Status-vs-ErrorFrame rule `serve::wire`'s module doc states, pinned
/// here at one instance.
#[tokio::test]
async fn streams_before_a_handshake_refuses_as_a_status() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = run_on::<EchoSource>(&path).await.expect("bind");
    let mut source = SourceServiceClient::new(dial(&path).await);

    let error = source
        .streams(StreamsRequest {})
        .await
        .expect_err("streams before a handshake must refuse");
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert_eq!(error.message(), "handshake has not completed");
}

/// The config-free `Spec` RPC answers BEFORE any handshake — the one
/// deliberate exemption from the pre-handshake refusal its two `Status`
/// siblings below pin. It serves the connector's static identity
/// (`C::NAME`/`C::VERSION`/`C::config_schema()`) without ever touching
/// the handshake-populated shell, so a provider can ask a spawned
/// connector what it IS before deciding what config to hand it.
#[tokio::test]
async fn spec_answers_before_any_handshake() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = run_on::<EchoSource>(&path).await.expect("bind");
    let mut connector = ConnectorClient::new(dial(&path).await);

    let reply = connector
        .spec(SpecRequest {})
        .await
        .expect("pre-handshake Spec")
        .into_inner();
    let spec: rdlt_connector::spec::ConnectorSpec =
        serde_json::from_slice(&reply.spec_json).expect("ConnectorSpec JSON");
    assert_eq!(spec.name, "echo-source");
    assert_eq!(spec.version, "0.0.0");
    assert!(
        spec.config_schema.is_none(),
        "echo declares no config schema — the trait default"
    );
}

/// `Read` before a handshake refuses the same way `Streams` does — see
/// the comment there for the Status-vs-ErrorFrame note.
#[tokio::test]
async fn read_before_a_handshake_refuses_as_a_status() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = run_on::<EchoSource>(&path).await.expect("bind");
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

/// An undecodable `stream_spec_json` answers INSIDE the response stream
/// — the `Read` RPC itself completes normally, and the stream's first
/// and only frame is a terminal FATAL `ErrorFrame` with the frozen
/// `invalid stream_spec_json: ` prefix (the rest is the error's kind and
/// location, so a fragment is asserted, same discipline as the
/// config-decode rows). A `Status::invalid_argument` here would be a
/// THIRD refusal shape the Status-vs-ErrorFrame rule forbids.
#[tokio::test]
async fn an_undecodable_stream_spec_answers_a_terminal_error_frame_not_a_status() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = run_on::<EchoSource>(&path).await.expect("bind");
    let channel = dial(&path).await;
    let mut connector = ConnectorClient::new(channel.clone());
    let mut source = SourceServiceClient::new(channel);

    connector
        .handshake(HandshakeRequest {
            protocol_version: PROTOCOL_VERSION,
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

/// The client twin's COUNT caps, mirrored at the serve seat: a spec of
/// thousands of tiny gate-legal keys passes every per-value gate within
/// the document ceiling otherwise, and the spec is RETAINED for the
/// read's lifetime. 65 primary-key fields and 4097 type hints each
/// refuse as the stream's terminal error frame, before the connector
/// sees the spec.
#[tokio::test]
async fn an_over_count_stream_spec_refuses_at_the_serve_seat() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = run_on::<EchoSource>(&path).await.expect("bind");
    let channel = dial(&path).await;
    let mut connector = ConnectorClient::new(channel.clone());
    let mut source = SourceServiceClient::new(channel);

    connector
        .handshake(HandshakeRequest {
            protocol_version: PROTOCOL_VERSION,
            expected_role: "source".to_string(),
            config_json: echo_config(3, false),
        })
        .await
        .expect("handshake rpc");

    let mut over_keys = rdlt_connector::source::StreamSpec::new("numbers");
    over_keys.primary_key = Some((0..65).map(|i| format!("k{i}")).collect());
    let mut over_hints = rdlt_connector::source::StreamSpec::new("numbers");
    for i in 0..4097 {
        over_hints.type_hints.insert(
            format!("c{i}"),
            rdlt_connector::core::types::LogicalType::Int64,
        );
    }
    for (spec, seat) in [
        (over_keys, "primary-key fields"),
        (over_hints, "type-hint fields"),
    ] {
        let mut frames = source
            .read(ReadRequest {
                stream_spec_json: serde_json::to_vec(&spec).expect("spec json"),
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
                    error.message.contains(seat) && error.message.contains("ceiling"),
                    "the refusal names the seat and the ceiling: {}",
                    error.message
                );
            }
            other => panic!("expected a terminal error frame for {seat}, got {other:?}"),
        }
        assert!(
            frames.message().await.expect("stream ends").is_none(),
            "nothing follows the terminal error"
        );
    }
}

/// The declared stream spec's identifiers — its name here, the
/// worst-carrying seat — ride the wire identifier ceiling the session
/// seats hold theirs to: the spec is retained for the read's lifetime
/// and its names are quoted by connector refusals, so an oversized one
/// refuses as the stream's terminal error frame, before the connector
/// ever sees the spec.
#[tokio::test]
async fn an_oversized_stream_spec_name_answers_a_terminal_error_frame() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = run_on::<EchoSource>(&path).await.expect("bind");
    let channel = dial(&path).await;
    let mut connector = ConnectorClient::new(channel.clone());
    let mut source = SourceServiceClient::new(channel);

    connector
        .handshake(HandshakeRequest {
            protocol_version: PROTOCOL_VERSION,
            expected_role: "source".to_string(),
            config_json: echo_config(3, false),
        })
        .await
        .expect("handshake rpc");

    let oversized = serde_json::json!({
        "name": "n".repeat(rdlt_connector_sdk::spi::gate::MAX_WIRE_IDENTIFIER_BYTES + 1),
        "primary_key": null,
        "cursor_field": null,
        "type_hints": {},
    });
    let mut frames = source
        .read(ReadRequest {
            stream_spec_json: serde_json::to_vec(&oversized).expect("spec json"),
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
                error.message.contains("identifier ceiling"),
                "the refusal names the ceiling: {}",
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
    let (_line, _handle) = run_on::<EchoSource>(&path).await.expect("bind");
    let channel = dial(&path).await;
    let mut connector = ConnectorClient::new(channel.clone());
    let mut source = SourceServiceClient::new(channel);

    connector
        .handshake(HandshakeRequest {
            protocol_version: PROTOCOL_VERSION,
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

/// A `since_cursor_json` above the 4 MiB cursor ceiling answers a
/// terminal error frame by SIZE, before any parse — the cursor is
/// retained inside the read for its whole lifetime, so the ceiling
/// guards a RESIDENT expansion, not just a transient one. Valid JSON,
/// like the config twin: the parse arm must provably not be the one
/// that refused.
#[tokio::test]
async fn an_oversized_since_cursor_answers_a_terminal_error_frame_by_size() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = run_on::<EchoSource>(&path).await.expect("bind");
    let channel = dial(&path).await;
    let mut connector = ConnectorClient::new(channel.clone());
    let mut source = SourceServiceClient::new(channel);

    connector
        .handshake(HandshakeRequest {
            protocol_version: PROTOCOL_VERSION,
            expected_role: "source".to_string(),
            config_json: echo_config(3, false),
        })
        .await
        .expect("handshake rpc");

    // The cursor gate is the cursor contract's own bound
    // (`MAX_CURSOR_BYTES`, 4 MiB) — deliberately tighter than the config
    // ceiling, and the same constant the client enforces pre-send.
    let mut cursor_json = Vec::new();
    cursor_json.push(b'"');
    cursor_json.resize(4 * 1024 * 1024 + 1, b'x');
    cursor_json.push(b'"');
    assert!(serde_json::from_slice::<serde_json::Value>(&cursor_json).is_ok());
    let len = cursor_json.len();

    let mut frames = source
        .read(ReadRequest {
            stream_spec_json: numbers_stream_spec_json(),
            since_cursor_json: Some(cursor_json),
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
            assert_eq!(
                error.message,
                format!(
                    "since_cursor_json is {len} bytes — larger than the 4194304-byte cursor \
                     ceiling; a cursor summarizes resume state and measures in kilobytes, so a \
                     payload this size is a wrong path, refused before it can expand in memory"
                )
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
/// — no rows, no checkpoint, the classification from the `SourceError`
/// and the message its BARE inner cause (the frame carries the cause
/// text, never the SPI `Display` frame — the receiving client renders
/// the classification frame exactly once on reconstruction), and
/// nothing follows it on the stream.
#[tokio::test]
async fn a_failed_read_forwards_one_terminal_error_frame() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = run_on::<EchoSource>(&path).await.expect("bind");
    let channel = dial(&path).await;
    let mut connector = ConnectorClient::new(channel.clone());
    let mut source = SourceServiceClient::new(channel);

    connector
        .handshake(HandshakeRequest {
            protocol_version: PROTOCOL_VERSION,
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
            assert_eq!(error.message, "echo: induced read failure");
        }
        other => panic!("expected a terminal error frame, got {other:?}"),
    }
    assert!(
        frames.message().await.expect("stream ends").is_none(),
        "nothing follows the terminal error"
    );
}

/// THE BYTE-BOUND PIN: what a STALLED reader lets the serve layer buffer
/// is bounded in BYTES, not in frames. The client here starts a `Read`
/// and then never polls its response stream, while the connector
/// pushes 8 MiB documents as fast as the layer admits them; the echo
/// source counts only pushes that RETURNED, so the counter reads
/// exactly how much sat buffered before everything parked.
///
/// The ceiling is the sum of the layer's own budgets, each named below
/// rather than folded into one magic number. Under the frame-COUNT
/// bound this replaces (16 already-encoded frames regardless of size)
/// the same run admitted ~19 frames — over 150 MiB — because a count
/// prices a 64-byte frame and an 8 MiB frame identically; that is the
/// defect this pin exists to keep closed, and it is why the frames here
/// are large: a byte bound and a count bound are indistinguishable at
/// small frame sizes.
#[tokio::test]
async fn a_stalled_reader_buffers_bounded_bytes_not_a_fixed_frame_count() {
    // 8 MiB frames: the shape a large-frame source's ~10 MiB frames
    // take in practice, and far enough above the read channel's own
    // per-frame arithmetic that the two regimes cannot be confused.
    const FRAME_BYTES: usize = 8 * 1024 * 1024;
    // More rows than any regime can admit, so admission — not the row
    // supply — is what stops the producer.
    const ROWS: u64 = 64;
    // The budgets themselves (imported, not restated — lowering either
    // constant tightens this pin with it), plus four frames of slack:
    // the one the forwarding loop holds in hand, what tonic has pulled
    // for the wire and is holding against the client's h2 window, and
    // margin so a tonic that buffers one frame more than measured is
    // not a failure. The recorded measurement is 7 frames (56 MiB)
    // admitted — the slack is the only part of this ceiling that is
    // not somebody's declared budget.
    const CEILING: u64 = (BYTE_FRAME_BUDGET + READ_CHANNEL_BUDGET + 4 * FRAME_BYTES) as u64;

    let (_dir, path) = socket_path();
    let (_line, _handle) = run_on::<EchoSource>(&path).await.expect("bind");
    let channel = dial(&path).await;
    let mut connector = ConnectorClient::new(channel.clone());
    let mut source = SourceServiceClient::new(channel);

    connector
        .handshake(HandshakeRequest {
            protocol_version: PROTOCOL_VERSION,
            expected_role: "source".to_string(),
            config_json: sized_echo_config(ROWS, FRAME_BYTES),
        })
        .await
        .expect("handshake rpc");

    // Held, never polled: this IS the stall under test. Dropping it
    // would cancel the read instead of stalling it.
    let _frames = source
        .read(ReadRequest {
            stream_spec_json: numbers_stream_spec_json(),
            since_cursor_json: None,
        })
        .await
        .expect("read rpc")
        .into_inner();

    // Bounded polling until admission stops moving — the producer parks
    // silently, so there is no event to await; five unchanged samples
    // (250 ms) is the quiescence signal, and the overall bound turns a
    // never-parking regression into a named failure rather than a hung
    // suite.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    let mut admitted = echo::pushed_bytes();
    let mut unchanged = 0;
    while unchanged < 5 && std::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let now = echo::pushed_bytes();
        unchanged = if now == admitted { unchanged + 1 } else { 0 };
        admitted = now;
    }

    assert!(
        admitted > 0,
        "the harness proves nothing if the connector never pushed: \
         admission must reach the layer's budgets before it stops"
    );
    assert!(
        admitted <= CEILING,
        "a stalled reader admitted {admitted} bytes ({} frames of {FRAME_BYTES}), \
         above the {CEILING}-byte ceiling the layer's own budgets set — \
         a frame-COUNT bound admits ~19 frames here regardless of size",
        admitted / FRAME_BYTES as u64
    );
}

/// The cancellation chain, load-bearing for the out-of-process adapter:
/// a client that drops its `Read` response stream mid-flight must not
/// leave the connector's producer running forever.
/// `rows: 10_000_000` keeps `EchoSource` producing well past the drop;
/// pulling a few frames first proves the producer is actually running
/// ahead of the client when the drop happens. Cancellation may either
/// let an in-flight push observe `ControlFlow::Break` or abort the read
/// immediately; the durable contract is that the task is dropped.
#[tokio::test]
async fn a_dropped_response_stream_drops_the_connector_task_within_a_timeout() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = run_on::<EchoSource>(&path).await.expect("bind");
    let channel = dial(&path).await;
    let mut connector = ConnectorClient::new(channel.clone());
    let mut source = SourceServiceClient::new(channel);

    connector
        .handshake(HandshakeRequest {
            protocol_version: PROTOCOL_VERSION,
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
        while !echo::read_task_dropped() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await;

    assert!(
        observed.is_ok(),
        "the connector's read task must be dropped within the timeout, not hang"
    );
}

#[tokio::test]
async fn a_dropped_response_stream_aborts_a_connector_parked_between_pushes() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = run_on::<EchoSource>(&path).await.expect("bind");
    let channel = dial(&path).await;
    let mut connector = ConnectorClient::new(channel.clone());
    let mut source = SourceServiceClient::new(channel);
    connector
        .handshake(HandshakeRequest {
            protocol_version: PROTOCOL_VERSION,
            expected_role: "source".to_string(),
            config_json: serde_json::to_vec(&serde_json::json!({
                "rows": 2,
                "park_after_first": true
            }))
            .expect("config"),
        })
        .await
        .expect("handshake");

    let mut frames = source
        .read(ReadRequest {
            stream_spec_json: numbers_stream_spec_json(),
            since_cursor_json: None,
        })
        .await
        .expect("read")
        .into_inner();
    frames
        .message()
        .await
        .expect("frame transport")
        .expect("the first frame arrives");
    drop(frames);

    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !echo::read_task_dropped() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the forwarding task aborts a reader parked between pushes");
}

/// The refusal-secrecy pin, parse seat: `config_json` that is not
/// valid JSON refuses with the error's KIND and location alone —
/// never serde's own message text, and never a byte of the document
/// (which may carry credentials; the protocol's rule is that no
/// `*_json` payload is ever echoed verbatim).
#[tokio::test]
async fn a_truncated_config_refusal_never_echoes_the_document() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = run_on::<EchoSource>(&path).await.expect("bind");
    let mut connector = ConnectorClient::new(dial(&path).await);

    let reply = connector
        .handshake(HandshakeRequest {
            protocol_version: PROTOCOL_VERSION,
            expected_role: "source".to_string(),
            config_json: br#"{"password": "hunter2-secret""#.to_vec(),
        })
        .await
        .expect("handshake rpc")
        .into_inner();
    match reply.outcome {
        Some(handshake_reply::Outcome::Error(error)) => {
            assert!(
                !error.message.contains("hunter2-secret"),
                "a config byte crossed back over the wire: {}",
                error.message
            );
            assert_eq!(
                error.message,
                "invalid config_json: unexpected end of input at line 1 column 29"
            );
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
}

/// The refusal-secrecy pin, typed-config seat: a WELL-FORMED document
/// whose value has the wrong type reaches the connector's own config
/// gate, whose serde arm quotes the parsed token verbatim — a secret
/// handed to the wrong field would ride the refusal into host logs.
/// The serve edge shields it: every string value the document carries
/// is redacted from the rendered refusal before it crosses back.
#[tokio::test]
async fn a_wrong_typed_config_refusal_redacts_the_secret_value() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = run_on::<EchoSource>(&path).await.expect("bind");
    let mut connector = ConnectorClient::new(dial(&path).await);

    let reply = connector
        .handshake(HandshakeRequest {
            protocol_version: PROTOCOL_VERSION,
            expected_role: "source".to_string(),
            config_json: serde_json::to_vec(&serde_json::json!({
                "rows": "hunter2-secret-value"
            }))
            .expect("config serializes"),
        })
        .await
        .expect("handshake rpc")
        .into_inner();
    match reply.outcome {
        Some(handshake_reply::Outcome::Error(error)) => {
            assert!(
                !error.message.contains("hunter2-secret-value"),
                "a config value crossed back over the wire: {}",
                error.message
            );
            assert_eq!(
                error.message,
                "echo json: invalid type: string \"[redacted config value]\", expected u64"
            );
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
}

/// The shield applies to connector validation messages too: a scalar
/// config value repeated by the connector is still removed.
#[tokio::test]
async fn a_validate_refusal_redacts_a_repeated_scalar_value() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = run_on::<EchoSource>(&path).await.expect("bind");
    let mut connector = ConnectorClient::new(dial(&path).await);

    let reply = connector
        .handshake(HandshakeRequest {
            protocol_version: PROTOCOL_VERSION,
            expected_role: "source".to_string(),
            config_json: echo_config(0, false),
        })
        .await
        .expect("handshake rpc")
        .into_inner();
    match reply.outcome {
        Some(handshake_reply::Outcome::Error(error)) => {
            assert_eq!(
                error.message,
                "echo: rows must be > [redacted config value]"
            );
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
}

/// The shield holds for serde's Debug-escaped rendering too: the
/// `invalid type` arm renders the token through `{:?}`, so a secret
/// carrying quotes or backslashes appears ESCAPED in the message and
/// a raw-value match alone would miss it. Neither the raw form nor
/// the escaped form survives the refusal.
#[tokio::test]
async fn a_debug_escaped_secret_is_redacted_too() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = run_on::<EchoSource>(&path).await.expect("bind");
    let mut connector = ConnectorClient::new(dial(&path).await);

    let secret = "pa\"ss\\word";
    let reply = connector
        .handshake(HandshakeRequest {
            protocol_version: PROTOCOL_VERSION,
            expected_role: "source".to_string(),
            config_json: serde_json::to_vec(&serde_json::json!({ "rows": secret }))
                .expect("config serializes"),
        })
        .await
        .expect("handshake rpc")
        .into_inner();
    match reply.outcome {
        Some(handshake_reply::Outcome::Error(error)) => {
            assert!(
                !error.message.contains(secret),
                "the raw secret crossed back over the wire: {}",
                error.message
            );
            assert!(
                !error.message.contains("pa\\\"ss\\\\word"),
                "the Debug-escaped secret crossed back over the wire: {}",
                error.message
            );
            assert_eq!(
                error.message,
                "echo json: invalid type: string \"[redacted config value]\", expected u64"
            );
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
}

/// `expected_role` is an inbound identifier like every other: one over
/// the identifier ceiling refuses WITHOUT echoing it — the refusal
/// stays bounded however large the frame was.
#[tokio::test]
async fn an_oversized_expected_role_refuses_bounded_without_echo() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = run_on::<EchoSource>(&path).await.expect("bind");
    let mut connector = ConnectorClient::new(dial(&path).await);

    let role = "R".repeat(1 << 20);
    let reply = connector
        .handshake(HandshakeRequest {
            protocol_version: PROTOCOL_VERSION,
            expected_role: role,
            config_json: echo_config(3, false),
        })
        .await
        .expect("handshake rpc")
        .into_inner();
    match reply.outcome {
        Some(handshake_reply::Outcome::Error(error)) => {
            assert_eq!(error.classification, Classification::Fatal as i32);
            assert!(
                error.message.len() < 1024,
                "the refusal is bounded, not an echo: {} bytes",
                error.message.len()
            );
            assert!(
                !error.message.contains("RRRR"),
                "no fragment of the role rides the refusal: {}",
                error.message
            );
            assert!(
                error.message.contains("1048576"),
                "the refusal names the true length: {}",
                error.message
            );
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
}

/// A legitimate host reads a source's streams concurrently, one `Read`
/// per stream: sixteen parked reads over one connection are all
/// admitted and each yields its first frame — the ceiling is far above
/// any ordinary fan-out.
#[tokio::test]
async fn sixteen_concurrent_reads_are_all_admitted() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = run_on::<EchoSource>(&path).await.expect("bind");
    let channel = dial(&path).await;
    let mut connector = ConnectorClient::new(channel.clone());
    let mut source = SourceServiceClient::new(channel);
    connector
        .handshake(HandshakeRequest {
            protocol_version: PROTOCOL_VERSION,
            expected_role: "source".to_string(),
            config_json: serde_json::to_vec(&serde_json::json!({
                "rows": 2,
                "park_after_first": true
            }))
            .expect("config"),
        })
        .await
        .expect("handshake");
    let mut admitted = Vec::new();
    for _ in 0..16 {
        let mut frames = source
            .read(ReadRequest {
                stream_spec_json: numbers_stream_spec_json(),
                since_cursor_json: None,
            })
            .await
            .expect("an admitted read")
            .into_inner();
        frames
            .message()
            .await
            .expect("frame transport")
            .expect("the admitted read yields its first frame");
        admitted.push(frames);
    }
    assert_eq!(admitted.len(), 16);
}

/// The source-side admission ceiling: `MAX_CONCURRENT_READS` parked
/// reads all proceed (each yields its first frame — spread over several
/// connections, since the ceiling is per PROCESS, not per connection);
/// the next `Read` refuses RESOURCE_EXHAUSTED naming the ceiling;
/// releasing one read admits another.
#[tokio::test]
async fn reads_over_the_concurrency_ceiling_refuse_while_admitted_ones_proceed() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = run_on::<EchoSource>(&path).await.expect("bind");
    let channel = dial(&path).await;
    let mut connector = ConnectorClient::new(channel.clone());
    connector
        .handshake(HandshakeRequest {
            protocol_version: PROTOCOL_VERSION,
            expected_role: "source".to_string(),
            config_json: serde_json::to_vec(&serde_json::json!({
                "rows": 2,
                "park_after_first": true
            }))
            .expect("config"),
        })
        .await
        .expect("handshake");
    let read = || ReadRequest {
        stream_spec_json: numbers_stream_spec_json(),
        since_cursor_json: None,
    };

    // 32 connections × 32 reads each: the per-connection h2 stream
    // limit never bites, and the process-wide count reaches the ceiling.
    const PER_CONNECTION: usize = 32;
    assert_eq!(MAX_CONCURRENT_READS % PER_CONNECTION, 0);
    let mut admitted = Vec::new();
    for _ in 0..MAX_CONCURRENT_READS / PER_CONNECTION {
        let mut source = SourceServiceClient::new(dial(&path).await);
        for _ in 0..PER_CONNECTION {
            let mut frames = source
                .read(read())
                .await
                .expect("an admitted read")
                .into_inner();
            frames
                .message()
                .await
                .expect("frame transport")
                .expect("the admitted read yields its first frame");
            admitted.push(frames);
        }
    }
    assert_eq!(admitted.len(), MAX_CONCURRENT_READS);

    let mut source = SourceServiceClient::new(dial(&path).await);
    let refused = source
        .read(read())
        .await
        .expect_err("the read past the ceiling must refuse");
    assert_eq!(
        refused.code(),
        tonic::Code::ResourceExhausted,
        "{refused:?}"
    );
    assert!(
        refused
            .message()
            .contains(&MAX_CONCURRENT_READS.to_string()),
        "the refusal names the ceiling: {}",
        refused.message()
    );

    // Releasing one admitted read frees its permit — the next read is
    // admitted once the server observes the hang-up.
    drop(admitted.pop());
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            match source.read(read()).await {
                Ok(_) => break,
                Err(status) if status.code() == tonic::Code::ResourceExhausted => {
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
                Err(status) => panic!("unexpected read failure: {status:?}"),
            }
        }
    })
    .await
    .expect("a released permit admits the next read");
}
