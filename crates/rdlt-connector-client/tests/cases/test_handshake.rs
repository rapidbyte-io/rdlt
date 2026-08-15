//! `dial` + `handshake` against this crate's echo pair served
//! in-process through the sdk's `serve_on` seam — the exact wire a
//! spawned connector process would answer on, minus the spawn.

use std::path::PathBuf;

use rdlt_connector_client::{
    Classification, ClientError, ConnectorRequirement, DEFAULT_RPC_DEADLINE, Role, dial, handshake,
};
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
const ENGINE_BUDGET_BYTES: u64 = 8 * 1024 * 1024;

fn source_config(rows: u64) -> serde_json::Value {
    serde_json::json!({ "rows": rows })
}

/// The happy path, source role: every `HandshakeOutcome` field lands —
/// the spec parsed from `spec_json`, NO capabilities (the proto pins
/// `capabilities_json` empty for sources), the v0-empty state-format
/// map, and the protocol version the client negotiated.
#[tokio::test]
async fn a_source_handshake_populates_the_outcome() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve::source::serve_on::<EchoSource>(&path)
        .await
        .expect("bind");

    let channel = dial(&path, ENGINE_BUDGET_BYTES, DEFAULT_RPC_DEADLINE)
        .await
        .expect("dial");
    let outcome = handshake(
        &channel,
        Role::Source,
        &source_config(3),
        &ConnectorRequirement::new("echo-source"),
    )
    .await
    .expect("handshake");

    assert_eq!(outcome.spec.name, "echo-source");
    assert_eq!(outcome.spec.version, "0.0.0");
    // The VERIFIED identity (D-039-2) rides in the outcome — the 039
    // final-review skew record closed: the wire's own reported values,
    // not a re-read of the unverified `spec` decode.
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
    assert_eq!(outcome.negotiated_protocol, 0);
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
    let (_line, _handle) = serve::destination::serve_on::<EchoDestination>(&path)
        .await
        .expect("bind");

    let channel = dial(&path, 1, DEFAULT_RPC_DEADLINE).await.expect("dial");
    let outcome = handshake(
        &channel,
        Role::Destination,
        &serde_json::json!({}),
        &ConnectorRequirement::new("echo-destination"),
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

/// D-039-2: the provider resolved a connector id, and the connector
/// reported a different one — refused typed, never worked around.
#[tokio::test]
async fn an_id_mismatch_refuses_typed() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve::source::serve_on::<EchoSource>(&path)
        .await
        .expect("bind");

    let channel = dial(&path, ENGINE_BUDGET_BYTES, DEFAULT_RPC_DEADLINE)
        .await
        .expect("dial");
    let error = handshake(
        &channel,
        Role::Source,
        &source_config(3),
        &ConnectorRequirement::new("postgres"),
    )
    .await
    .expect_err("an id mismatch must refuse");

    match error {
        ClientError::IdMismatch { expected, reported } => {
            assert_eq!(expected, "postgres");
            assert_eq!(reported, "echo-source");
        }
        other => panic!("expected IdMismatch, got {other:?}"),
    }
}

/// D-039-2's version half: a requirement pinned to a version the
/// connector does not report refuses typed, carrying both spellings.
#[tokio::test]
async fn a_version_mismatch_refuses_typed() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve::source::serve_on::<EchoSource>(&path)
        .await
        .expect("bind");

    let channel = dial(&path, ENGINE_BUDGET_BYTES, DEFAULT_RPC_DEADLINE)
        .await
        .expect("dial");
    let error = handshake(
        &channel,
        Role::Source,
        &source_config(3),
        &ConnectorRequirement::new("echo-source").with_version("9.9.9"),
    )
    .await
    .expect_err("a version mismatch must refuse");

    match error {
        ClientError::VersionMismatch { required, reported } => {
            assert_eq!(required, "9.9.9");
            assert_eq!(reported, "0.0.0");
        }
        other => panic!("expected VersionMismatch, got {other:?}"),
    }
}

/// A connector-side config refusal surfaces as `ClientError::Handshake`
/// — FATAL, with scalar config values redacted and no wait hint.
#[tokio::test]
async fn a_config_refusal_surfaces_as_a_fatal_handshake_error() {
    let (_dir, path) = socket_path();
    let (_line, _handle) = serve::source::serve_on::<EchoSource>(&path)
        .await
        .expect("bind");

    let channel = dial(&path, ENGINE_BUDGET_BYTES, DEFAULT_RPC_DEADLINE)
        .await
        .expect("dial");
    let error = handshake(
        &channel,
        Role::Source,
        &source_config(0),
        &ConnectorRequirement::new("echo-source"),
    )
    .await
    .expect_err("an invalid config must refuse");

    match error {
        ClientError::Handshake {
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
/// must not reach the IdMismatch message (which quotes the reported
/// value) — it refuses as a protocol violation whose own rendering is
/// inert.
#[tokio::test]
async fn a_control_character_identity_refuses_inert_before_the_mismatch_render() {
    let (_dir, path) = socket_path();
    let _serving = rogue::serve_identity(&path, "ev\u{1b}]52;c;AAAA\u{7}il", "0.0.0");

    let channel = dial(&path, ENGINE_BUDGET_BYTES, DEFAULT_RPC_DEADLINE)
        .await
        .expect("dial");
    let error = handshake(
        &channel,
        Role::Source,
        &serde_json::json!({}),
        &ConnectorRequirement::new("clean-id"),
    )
    .await
    .expect_err("a control-character identity must refuse");

    assert!(matches!(error, ClientError::Protocol(_)), "{error:?}");
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

    let channel = dial(&path, ENGINE_BUDGET_BYTES, DEFAULT_RPC_DEADLINE)
        .await
        .expect("dial");
    let error = handshake(
        &channel,
        Role::Source,
        &serde_json::json!({}),
        &ConnectorRequirement::new("clean-id"),
    )
    .await
    .expect_err("a control-character version must refuse");

    assert!(matches!(error, ClientError::Protocol(_)), "{error:?}");
    let rendered = error.to_string();
    assert!(
        !rendered.contains('\u{7}'),
        "the refusal must not itself carry the bytes it refuses: {rendered:?}"
    );
}
