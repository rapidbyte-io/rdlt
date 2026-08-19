//! `wire::dial` + `handshake::run` against this crate's echo pair
//! served in-process through the sdk's `run_on` seam — the exact
//! wire a spawned connector process would answer on, minus the spawn.

use std::path::PathBuf;

use rdlt_connector_client::error::{Classification, Error};
use rdlt_connector_client::handshake::{self, Requirement, Role};
use rdlt_connector_client::wire::{DEFAULT_DEADLINE, dial};
use rdlt_connector_sdk::serve;
use rdlt_connector_sdk::source::SourceConnector as _;

use super::support::echo::{EchoDestination, EchoSource};
use super::support::rogue;

/// A fresh temp directory plus a fixed socket name inside it — the
/// directory (and the socket file in it) is reclaimed on drop, so runs
/// leave no `.sock` litter. The `TempDir` must outlive the listener.
fn socket_path() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("connector.sock");
    (dir, path)
}

/// A budget in the middle of the SPI's real 8-64 MiB band — what an
/// engine would actually hand `dial`.
const BUDGET_BYTES: u64 = 8 * 1024 * 1024;

fn source_config(rows: u64) -> serde_json::Value {
    serde_json::json!({ "rows": rows })
}

/// The happy path, source role: every `handshake::Outcome` field lands —
/// the spec parsed from `spec_json`, NO capabilities (the proto pins
/// `capabilities_json` empty for sources), the v0-empty state-format
/// map, and the protocol version both sides settled on.
#[tokio::test]
async fn a_source_handshake_populates_the_outcome() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve::source::run_on::<EchoSource>(&path)
        .await
        .expect("bind");

    let channel = dial(&path, BUDGET_BYTES, DEFAULT_DEADLINE)
        .await
        .expect("dial");
    let outcome = handshake::run(
        &channel,
        Role::Source,
        &source_config(3),
        &Requirement::new("echo-source"),
    )
    .await
    .expect("handshake");

    assert_eq!(outcome.spec.name, "echo-source");
    assert_eq!(outcome.spec.version, "0.0.0");
    // The VERIFIED identity rides in the outcome — the wire's own
    // reported values, not a re-read of the unverified `spec` decode.
    assert_eq!(outcome.connector_id, EchoSource::NAME);
    assert_eq!(outcome.connector_version, EchoSource::VERSION);
    assert!(
        outcome.spec.config_schema.is_none(),
        "echo declares no config schema — the trait default"
    );
    assert!(
        outcome.capabilities.is_none(),
        "a source handshake carries no destination capabilities"
    );
    assert!(
        outcome.state_format_versions.is_empty(),
        "v0 servers send an empty state-format map"
    );
    assert_eq!(outcome.protocol_version, 0);
}

/// The happy path, destination role: `capabilities` is `Some` and
/// carries the echo's deliberately NON-default declaration, so a
/// silently dropped payload cannot pass as an all-false default.
/// Dialed with a deliberately tiny budget (1 byte) so the happy path
/// also proves the window clamp floors it into h2's legal range rather
/// than handing h2 an unworkable window.
#[tokio::test]
async fn a_destination_handshake_carries_capabilities() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve::destination::run_on::<EchoDestination>(&path)
        .await
        .expect("bind");

    let channel = dial(&path, 1, DEFAULT_DEADLINE).await.expect("dial");
    let outcome = handshake::run(
        &channel,
        Role::Destination,
        &serde_json::json!({}),
        &Requirement::new("echo-destination"),
    )
    .await
    .expect("handshake");

    assert_eq!(outcome.spec.name, "echo-destination");
    let capabilities = outcome
        .capabilities
        .expect("a destination handshake carries capabilities");
    assert!(capabilities.merge, "the echo declares merge");
    assert!(capabilities.structs, "the echo declares structs");
    assert!(
        !capabilities.scalar_lists,
        "undeclared capabilities stay false"
    );
}

/// The pre-send gate's exact-boundary acceptance — a config whose
/// serialized form IS the ceiling must pass both ends (the refusal is
/// `>`, and the serve gate shares the constant, so an at-cap document
/// completes the handshake).
#[tokio::test]
async fn a_config_at_exactly_the_document_ceiling_is_accepted() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve::source::run_on::<EchoSource>(&path)
        .await
        .expect("bind");

    let mut config = serde_json::json!({ "rows": 3, "pad": "" });
    let base = serde_json::to_vec(&config).expect("serialize").len();
    let ceiling = rdlt_connector_sdk::spi::gate::MAX_DOCUMENT_BYTES as usize;
    // Lengthening the string by N grows the document by exactly N.
    config["pad"] = serde_json::Value::String("x".repeat(ceiling - base));
    assert_eq!(
        serde_json::to_vec(&config).expect("serialize").len(),
        ceiling,
        "the fixture IS the ceiling"
    );

    let channel = dial(&path, BUDGET_BYTES, DEFAULT_DEADLINE)
        .await
        .expect("dial");
    handshake::run(
        &channel,
        Role::Source,
        &config,
        &Requirement::new("echo-source"),
    )
    .await
    .expect("an exactly-at-cap document passes both ends");
}

/// A config whose SERIALIZED form exceeds the document ceiling is
/// refused before SEND — the serve side's post-receive refusal would
/// fire anyway, but the host-side refusal names the real cause (the
/// YAML→JSON inflation edge: a just-legal source file re-serializing
/// past the ceiling). The server is live and reachable; the refusal
/// proves the document never crossed the wire.
#[tokio::test]
async fn an_oversized_serialized_config_is_refused_before_send() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve::source::run_on::<EchoSource>(&path)
        .await
        .expect("bind");

    let channel = dial(&path, BUDGET_BYTES, DEFAULT_DEADLINE)
        .await
        .expect("dial");
    let oversized = serde_json::json!({
        "rows": 1,
        "pad": "x".repeat(rdlt_connector_sdk::spi::gate::MAX_DOCUMENT_BYTES as usize),
    });
    let error = handshake::run(
        &channel,
        Role::Source,
        &oversized,
        &Requirement::new("echo-source"),
    )
    .await
    .expect_err("an over-ceiling document must refuse at the host");
    assert!(
        matches!(error, Error::Protocol(ref text) if text.contains("document ceiling")),
        "the refusal names the ceiling: {error:?}"
    );
}

/// The provider resolved a connector id, and the connector reported a
/// different one — refused typed, never worked around.
#[tokio::test]
async fn an_id_mismatch_refuses_typed() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve::source::run_on::<EchoSource>(&path)
        .await
        .expect("bind");

    let channel = dial(&path, BUDGET_BYTES, DEFAULT_DEADLINE)
        .await
        .expect("dial");
    let error = handshake::run(
        &channel,
        Role::Source,
        &source_config(3),
        &Requirement::new("postgres"),
    )
    .await
    .expect_err("an id mismatch must refuse");

    match error {
        Error::IdentityMismatch { expected, reported } => {
            assert_eq!(expected, "postgres");
            assert_eq!(reported, "echo-source");
        }
        other => panic!("expected IdentityMismatch, got {other:?}"),
    }
}

/// The identity check's version half: a requirement pinned to a
/// version the connector does not report refuses typed, carrying both
/// spellings.
#[tokio::test]
async fn a_version_mismatch_refuses_typed() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve::source::run_on::<EchoSource>(&path)
        .await
        .expect("bind");

    let channel = dial(&path, BUDGET_BYTES, DEFAULT_DEADLINE)
        .await
        .expect("dial");
    let error = handshake::run(
        &channel,
        Role::Source,
        &source_config(3),
        &Requirement::new("echo-source").with_version("9.9.9"),
    )
    .await
    .expect_err("a version mismatch must refuse");

    match error {
        Error::VersionMismatch { required, reported } => {
            assert_eq!(required, "9.9.9");
            assert_eq!(reported, "0.0.0");
        }
        other => panic!("expected VersionMismatch, got {other:?}"),
    }
}

/// A connector-side config refusal surfaces as `Error::Handshake`
/// — FATAL, with scalar config values redacted and no wait hint.
#[tokio::test]
async fn a_config_refusal_surfaces_as_a_fatal_handshake_error() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve::source::run_on::<EchoSource>(&path)
        .await
        .expect("bind");

    let channel = dial(&path, BUDGET_BYTES, DEFAULT_DEADLINE)
        .await
        .expect("dial");
    let error = handshake::run(
        &channel,
        Role::Source,
        &source_config(0),
        &Requirement::new("echo-source"),
    )
    .await
    .expect_err("an invalid config must refuse");

    match error {
        Error::Handshake {
            classification,
            message,
            retry_after_ms,
        } => {
            assert_eq!(classification, Classification::Fatal);
            assert_eq!(message, "echo: rows must be > [redacted config value]");
            assert_eq!(retry_after_ms, None);
        }
        other => panic!("expected a Handshake refusal, got {other:?}"),
    }
}

/// The wire edge refuses control characters in the REPORTED identity
/// before any equality check can render it: a hostile connector_id
/// must not reach the IdentityMismatch message (which quotes the reported
/// value) — it refuses as a protocol violation whose own rendering is
/// inert.
#[tokio::test]
async fn a_control_character_identity_refuses_inert_before_the_mismatch_render() {
    let (_dir, path) = socket_path();
    let _serving = rogue::serve_identity(&path, "ev\u{1b}]52;c;AAAA\u{7}il", "0.0.0");

    let channel = dial(&path, BUDGET_BYTES, DEFAULT_DEADLINE)
        .await
        .expect("dial");
    let error = handshake::run(
        &channel,
        Role::Source,
        &serde_json::json!({}),
        &Requirement::new("clean-id"),
    )
    .await
    .expect_err("a control-character identity must refuse");

    assert!(matches!(error, Error::Protocol(_)), "{error:?}");
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

/// The version field rides the same gate — a wire-reported version
/// with control characters refuses inert even when no version is
/// pinned (unpinned versions are carried into logs and reports as
/// reported).
#[tokio::test]
async fn a_control_character_version_refuses_inert() {
    let (_dir, path) = socket_path();
    let _serving = rogue::serve_identity(&path, "clean-id", "1.0\u{7}");

    let channel = dial(&path, BUDGET_BYTES, DEFAULT_DEADLINE)
        .await
        .expect("dial");
    let error = handshake::run(
        &channel,
        Role::Source,
        &serde_json::json!({}),
        &Requirement::new("clean-id"),
    )
    .await
    .expect_err("a control-character version must refuse");

    assert!(matches!(error, Error::Protocol(_)), "{error:?}");
    let rendered = error.to_string();
    assert!(
        !rendered.contains('\u{7}'),
        "the refusal must not itself carry the bytes it refuses: {rendered:?}"
    );
}

/// The LENGTH gate — a control-free but absurdly long identity is
/// refused at the wire boundary like the content hostiles. (Content
/// gates alone priced nothing about size within the frame cap.)
#[tokio::test]
async fn an_oversized_identity_refuses_at_the_wire_boundary() {
    let (_dir, path) = socket_path();
    let oversized: &'static str = Box::leak("a".repeat(1025).into_boxed_str());
    let _serving = rogue::serve_identity(&path, oversized, "0.0.0");

    let channel = dial(&path, BUDGET_BYTES, DEFAULT_DEADLINE)
        .await
        .expect("dial");
    let error = handshake::run(
        &channel,
        Role::Source,
        &serde_json::json!({}),
        &Requirement::new("clean-id"),
    )
    .await
    .expect_err("an over-length identity must refuse");

    assert!(matches!(error, Error::Protocol(_)), "{error:?}");
    assert!(
        error.to_string().contains("identifier ceiling"),
        "the refusal names the ceiling: {error}"
    );
}

/// `spec_json` is a typed shell around one UNTYPED value — its
/// `config_schema` is a free-form document the host caches for the
/// session's lifetime — so the seat gets the document ceiling every
/// untyped parse runs, on the RAW bytes before the parse whose
/// materialization it bounds. A rogue's oversized spec refuses typed
/// at the handshake, never materializing.
#[tokio::test]
async fn an_oversized_spec_json_is_refused_at_the_handshake() {
    let (_dir, path) = socket_path();
    let spec = rdlt_connector::spec::ConnectorSpec::new("rogue", "0.0.0");
    let mut ok = rdlt_connector_protocol::proto::HandshakeOk {
        connector_id: "rogue".to_string(),
        connector_version: "0.0.0".to_string(),
        spec_json: serde_json::to_vec(&spec).expect("a spec serializes"),
        capabilities_json: Vec::new(),
        state_format_versions: Default::default(),
    };
    ok.spec_json = vec![b'x'; rdlt_connector::gate::MAX_DOCUMENT_BYTES as usize + 1];
    let _serving = rogue::serve_handshake_ok(&path, ok);

    let channel = dial(&path, BUDGET_BYTES, DEFAULT_DEADLINE)
        .await
        .expect("dial");
    let error = handshake::run(
        &channel,
        Role::Source,
        &serde_json::json!({}),
        &Requirement::new("rogue"),
    )
    .await
    .expect_err("an oversized spec_json must refuse at the handshake");

    assert!(matches!(error, Error::Protocol(_)), "{error:?}");
    let rendered = error.to_string();
    assert!(
        rendered.contains("document ceiling"),
        "the refusal names the ceiling: {rendered}"
    );
}

/// The handshake reply's DECODE layer is capped at the reply's legal
/// maximum: a reply over the connector-service cap (18 MiB, computed
/// at `wire::connector_client` — a legal reply cannot exceed ~16.1
/// MiB) refuses AT DECODE, before prost materializes anything — the
/// content gates behind it never see the frame. The refusal is the
/// transport family's (tonic refuses the message length), so the
/// pin asserts that shape, not the post-decode document refusal a
/// smaller oversize (the test above) still gets.
#[tokio::test]
async fn a_reply_over_the_decode_cap_refuses_before_materialization() {
    let (_dir, path) = socket_path();
    let mut ok = rdlt_connector_protocol::proto::HandshakeOk {
        connector_id: "rogue".to_string(),
        connector_version: "0.0.0".to_string(),
        spec_json: Vec::new(),
        capabilities_json: Vec::new(),
        state_format_versions: Default::default(),
    };
    // Over the 18 MiB connector-reply decode cap, well under the
    // 64 MiB frame the wire itself admits.
    ok.spec_json = vec![b'x'; 20 << 20];
    let _serving = rogue::serve_handshake_ok(&path, ok);

    let channel = dial(&path, BUDGET_BYTES, DEFAULT_DEADLINE)
        .await
        .expect("dial");
    let error = handshake::run(
        &channel,
        Role::Source,
        &serde_json::json!({}),
        &Requirement::new("rogue"),
    )
    .await
    .expect_err("a reply over the decode cap must refuse");

    assert!(
        matches!(error, Error::Transport(_)),
        "the refusal is the decode layer's, through the transport arm: {error:?}"
    );
    // The classification OBSERVED, not assumed: tonic refuses the
    // over-cap message as `OutOfRange` with its length-too-large text.
    let rendered = error.to_string();
    assert!(
        rendered.contains("OutOfRange") && rendered.contains("message length too large"),
        "tonic's decode refusal, named: {rendered}"
    );
    assert!(
        !rendered.contains("document ceiling"),
        "the post-decode document gate never ran: {rendered}"
    );
}
