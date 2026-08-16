//! The RPC deadline over live sockets: a silent-but-alive connector —
//! the wire is up, keep-alives answered by the stack, and nothing ever
//! comes back — fails typed at every await seat, while a
//! slow-but-flowing one survives however long its stream runs. The
//! SIGKILL kill matrix proves the DEAD half of the law (the socket
//! dies, h2 errors); these rogues prove the SILENT half, which no
//! transport error can catch.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rdlt_connector::core::{LoadId, PipelineId};
use rdlt_connector::{
    OpenContext, PushPayload, ReadRequest, Source as _, SourceError, records_channel,
};
use rdlt_connector_client::error::Error;
use rdlt_connector_client::handshake::Requirement;
use rdlt_connector_client::wire::{DEFAULT_DEADLINE, Operation};
use rdlt_connector_client::{destination::Destination, source::Source};
use rdlt_connector_protocol::proto::{self, read_frame};
use rdlt_connector_sdk::destination::Backend as _;

use super::support::rogue::{self, MuteSeat, ReadScript, SessionScript};

/// A fresh temp directory plus a fixed socket name inside it.
fn socket_path() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("connector.sock");
    (dir, path)
}

/// A budget in the middle of the SPI's real 8-64 MiB band.
const ENGINE_BUDGET_BYTES: u64 = 8 * 1024 * 1024;

/// The tight deadline the silence tests dial with — long enough that a
/// healthy in-process reply never trips it, short enough to keep the
/// suite fast.
const TIGHT: Duration = Duration::from_millis(400);

/// Every wait whose failure mode is a HANG is bounded, so a broken
/// deadline reports as a named failure rather than a suite timeout.
const BOUND: Duration = Duration::from_secs(10);

fn raw_json_frame(n: u64) -> proto::ReadFrame {
    proto::ReadFrame {
        frame: Some(read_frame::Frame::RawJson(
            format!("{{\"n\":{n}}}").into_bytes(),
        )),
    }
}

/// The ten-second law, pinned at its constant: the default deadline is
/// the same ten seconds the certifier's kill window and the runtime's
/// handshake-line timeout speak (each sibling crate pins equality from
/// its side), and a fresh requirement carries it.
#[test]
fn the_default_rpc_deadline_is_the_ten_second_law() {
    assert_eq!(DEFAULT_DEADLINE, Duration::from_secs(10));
    assert_eq!(Requirement::new("any").rpc_deadline, DEFAULT_DEADLINE);
    let tightened = Requirement::new("any").with_rpc_deadline(TIGHT);
    assert_eq!(tightened.rpc_deadline, TIGHT);
}

/// A listener that accepts the socket connection but never speaks
/// HTTP/2: tonic completes the channel lazily (the h2 setup runs in
/// the background connection task), so the silence surfaces at the
/// FIRST RPC's deadline — `connect` as a whole fails typed within it,
/// never hangs. The dial's own timeout arm stays as the bound on the
/// io connect itself; its rendering is pinned below.
#[tokio::test]
async fn a_peer_that_never_speaks_h2_fails_typed_at_connect() {
    let (_dir, path) = socket_path();
    let _listener = tokio::net::UnixListener::bind(&path).expect("bind");

    let error = tokio::time::timeout(
        BOUND,
        Source::connect(
            &path,
            ENGINE_BUDGET_BYTES,
            &serde_json::json!({}),
            &Requirement::new("mute").with_rpc_deadline(TIGHT),
        ),
    )
    .await
    .expect("the connect fails within the bound — never hangs")
    .expect_err("a peer that never speaks h2 must time out");
    assert!(
        matches!(
            error,
            Error::Timeout {
                operation: Operation::Handshake,
                deadline,
            } if deadline == TIGHT
        ),
        "{error:?}"
    );
}

/// The timeout's rendering, full-string — inert prose naming the seat
/// and the deadline, stating the law's refusal half.
#[test]
fn the_timeout_rendering_names_the_seat_and_the_deadline() {
    let error = Error::Timeout {
        operation: Operation::Dial,
        deadline: TIGHT,
    };
    assert_eq!(
        error.to_string(),
        format!(
            "the connector went silent: no transport setup within {TIGHT:?} — a silent \
             connector fails typed, never hangs its host"
        )
    );
}

/// A connector that completes the transport and never answers the
/// handshake times out typed, naming the handshake.
#[tokio::test]
async fn a_silent_handshake_times_out_typed() {
    let (_dir, path) = socket_path();
    let _serving = rogue::serve_mute(&path, MuteSeat::Handshake);

    let error = tokio::time::timeout(
        BOUND,
        Source::connect(
            &path,
            ENGINE_BUDGET_BYTES,
            &serde_json::json!({}),
            &Requirement::new("mute").with_rpc_deadline(TIGHT),
        ),
    )
    .await
    .expect("the connect fails within the bound — never hangs")
    .expect_err("a silent handshake must time out");
    assert!(
        matches!(
            error,
            Error::Timeout {
                operation: Operation::Handshake,
                ..
            }
        ),
        "{error:?}"
    );
}

/// A connector that handshakes and then never answers `check` fails
/// the SPI call fatal, carrying the typed timeout as the cause — the
/// unary seats ride the same deadline as the streams.
#[tokio::test]
async fn a_silent_check_times_out_fatal() {
    let (_dir, path) = socket_path();
    let _serving = rogue::serve_mute(&path, MuteSeat::Check);
    let (remote, _) = Source::connect(
        &path,
        ENGINE_BUDGET_BYTES,
        &serde_json::json!({}),
        &Requirement::new("mute").with_rpc_deadline(TIGHT),
    )
    .await
    .expect("the mute connector handshakes honestly");

    let error = tokio::time::timeout(BOUND, remote.check())
        .await
        .expect("check fails within the bound — never hangs")
        .expect_err("a silent check must time out");
    assert!(matches!(error, SourceError::Fatal(_)), "{error:?}");
    assert_eq!(
        error.to_string(),
        format!(
            "fatal source error: the connector went silent: no reply within {TIGHT:?} — a \
             silent connector fails typed, never hangs its host"
        )
    );
}

/// A read stream that stalls mid-flight: the frames before the stall
/// are forwarded, and the stall itself fails typed within the deadline
/// — never a hang, never a clean end.
#[tokio::test]
async fn a_stalled_read_stream_times_out_typed_after_forwarding_its_frames() {
    let (_dir, path) = socket_path();
    let _serving = rogue::serve(
        &path,
        ReadScript::FramesThenSilence(vec![raw_json_frame(0), raw_json_frame(1)]),
    );
    let (remote, _) = Source::connect(
        &path,
        ENGINE_BUDGET_BYTES,
        &serde_json::json!({}),
        &Requirement::new("rogue").with_rpc_deadline(TIGHT),
    )
    .await
    .expect("the rogue handshakes");

    let (out, mut input) = records_channel(1 << 20);
    let stream = rdlt_connector::StreamSpec::new("numbers");
    let read = tokio::spawn(async move { remote.read(ReadRequest::new(stream, None, out)).await });

    for n in 0..2u64 {
        let push = tokio::time::timeout(BOUND, input.recv())
            .await
            .expect("a scripted frame arrives")
            .expect("the stream is live");
        match &push.payload {
            PushPayload::RawJson(bytes) => {
                // Verbatim rogue bytes — no newline framing: the
                // client forwards, never re-frames.
                assert_eq!(&bytes[..], format!("{{\"n\":{n}}}").as_bytes());
            }
            other => panic!("frame {n} lands as RawJson, got {other:?}"),
        }
    }

    let error = tokio::time::timeout(BOUND, read)
        .await
        .expect("the read fails within the bound — never hangs")
        .expect("the read task ran")
        .expect_err("silence after the frames must time out");
    assert!(matches!(error, SourceError::Fatal(_)), "{error:?}");
    assert!(
        error.to_string().contains("no read frame within"),
        "the timeout names the read-frame seat: {error}"
    );
}

/// The deadline's other direction, pinned: it bounds the QUIET
/// interval, never the stream's total duration. A source dripping
/// frames each well inside the deadline survives a stream that in
/// total runs well past it.
#[tokio::test]
async fn a_slow_dripping_stream_inside_the_deadline_survives() {
    let deadline = Duration::from_millis(1000);
    let interval = Duration::from_millis(250);
    let frames: Vec<_> = (0..8).map(raw_json_frame).collect();
    let total = interval * frames.len() as u32;
    assert!(
        total > deadline,
        "the scripted stream must outlast the deadline for the pin to mean anything"
    );

    let (_dir, path) = socket_path();
    let _serving = rogue::serve(&path, ReadScript::Drip { frames, interval });
    let (remote, _) = Source::connect(
        &path,
        ENGINE_BUDGET_BYTES,
        &serde_json::json!({}),
        &Requirement::new("rogue").with_rpc_deadline(deadline),
    )
    .await
    .expect("the rogue handshakes");

    let (out, mut input) = records_channel(1 << 20);
    let stream = rdlt_connector::StreamSpec::new("numbers");
    let started = std::time::Instant::now();
    let read = tokio::spawn(async move { remote.read(ReadRequest::new(stream, None, out)).await });

    let mut received = 0u64;
    while let Some(push) = input.recv().await {
        assert!(matches!(push.payload, PushPayload::RawJson(_)));
        received += 1;
    }
    tokio::time::timeout(BOUND, read)
        .await
        .expect("the read completes")
        .expect("the read task ran")
        .expect("a slow-but-flowing stream must never trip the per-frame deadline");
    assert_eq!(received, 8, "every dripped frame arrived");
    assert!(
        started.elapsed() > deadline,
        "the stream really did outlive the deadline: {:?}",
        started.elapsed()
    );
}

/// A session whose reply never comes fails the in-flight call typed —
/// the destination seat of the same law.
#[tokio::test]
async fn a_silent_session_reply_times_out_typed() {
    let (_dir, path) = socket_path();
    let _serving = rogue::serve_destination(
        &path,
        SessionScript::FloodPartsThenSilence {
            parts: 0,
            table: "numbers".to_string(),
        },
    );
    let (remote, _) = Destination::connect(
        &path,
        ENGINE_BUDGET_BYTES,
        &serde_json::json!({}),
        &Requirement::new("rogue").with_rpc_deadline(TIGHT),
    )
    .await
    .expect("the rogue handshakes");
    let context = OpenContext::new(PipelineId::new("pipe"), LoadId::new("load-1"));
    let mut backend = remote.open_backend(&context).await.expect("open");

    let error = tokio::time::timeout(
        BOUND,
        backend.ensure_table(
            &schema_for("numbers"),
            &rdlt_connector::core::WriteMode::Append,
        ),
    )
    .await
    .expect("the call fails within the bound — never hangs")
    .expect_err("a silent session must time out");
    assert!(
        error.to_string().contains("no reply within"),
        "the timeout names the reply seat: {error}"
    );
}

/// The `part_closed` flood, judged honestly: a flood before silence
/// does NOT defeat the deadline — every event resets the quiet-interval
/// clock and is forwarded to the callback, and the silence after the
/// flood still fails the call typed. (A rogue that floods forever
/// without ever going quiet keeps the loop spinning for as long as it
/// keeps sending — bounded memory, ended by the host dropping the
/// session; that half is recorded at the call loop's doc, not closed
/// by the deadline.)
#[tokio::test]
async fn a_part_closed_flood_followed_by_silence_still_times_out_typed() {
    let (_dir, path) = socket_path();
    let _serving = rogue::serve_destination(
        &path,
        SessionScript::FloodPartsThenSilence {
            parts: 5,
            table: "numbers".to_string(),
        },
    );
    let (remote, _) = Destination::connect(
        &path,
        ENGINE_BUDGET_BYTES,
        &serde_json::json!({}),
        &Requirement::new("rogue").with_rpc_deadline(TIGHT),
    )
    .await
    .expect("the rogue handshakes");

    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);
    let context = OpenContext::new(PipelineId::new("pipe"), LoadId::new("load-1"))
        .with_part_events(Arc::new(move |part| {
            sink.lock().expect("part log lock").push(part);
        }));
    let mut backend = remote.open_backend(&context).await.expect("open");

    let error = tokio::time::timeout(
        BOUND,
        backend.ensure_table(
            &schema_for("numbers"),
            &rdlt_connector::core::WriteMode::Append,
        ),
    )
    .await
    .expect("the call fails within the bound — the flood must not become a hang")
    .expect_err("silence after the flood must time out");
    assert!(
        error.to_string().contains("no reply within"),
        "the timeout names the reply seat: {error}"
    );
    let seen = seen.lock().expect("part log lock");
    assert_eq!(
        seen.len(),
        5,
        "every flood event reached the callback before the silence was judged"
    );
}

/// The one-column `id: Int64` logical schema, hand-built (this crate's
/// test support imports nothing cross-crate).
fn schema_for(table: &str) -> rdlt_connector::core::TableSchema {
    use rdlt_connector::core::{ColumnDef, ColumnType, LogicalType, Provenance, TableName};
    rdlt_connector::core::TableSchema {
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
