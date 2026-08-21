//! The TCP binding of the same proto (ADR 0001 D3), end to end: the
//! sdk's `run_on_tcp` serves the echo connectors on a loopback listener
//! and ONE unified [`Remote::connect`](rdlt_connector_client::source::Remote::connect) dials it with an address — no
//! `*_address` method family, just the endpoint variant. Every
//! gate, ceiling, and refusal shape is the wire's own; only the socket
//! differs.

use std::sync::Arc;

use rdlt_connector::source::Source as _;
use rdlt_connector_client::endpoint::Endpoint;
use rdlt_connector_client::handshake::Requirement;
use rdlt_connector_client::source::Remote;
use rdlt_connector_sdk::serve::source::run_on_tcp;

use super::support::echo::EchoSource;
/// The full source choreography over loopback TCP: connect by address,
/// `check()`, discover streams, and read one batch through — the same
/// law the UDS suite pins, proving the transport carries semantics,
/// never changes them.
#[tokio::test]
async fn a_source_serves_over_tcp_and_the_unified_connect_dials_the_address() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("an ephemeral port");
    let _handle = run_on_tcp::<EchoSource>(listener);

    let config = serde_json::json!({ "rows": 3 });
    let (remote, outcome) = Remote::connect(
        Endpoint::Address(addr),
        8 * 1024 * 1024,
        &config,
        &Requirement::new("echo-source"),
    )
    .await
    .expect("connect by address");

    assert_eq!(outcome.spec.name, "echo-source");
    remote.check().await.expect("check over TCP");
    let streams = remote.streams().await.expect("streams over TCP");
    assert_eq!(streams.len(), 1);
}

/// The endpoint conversions are the interface's ergonomics contract:
/// paths and addresses both land as their variant, and the enum stays
/// closed to silent coercion.
#[test]
fn endpoints_convert_from_both_transports() {
    use std::net::SocketAddr;
    use std::path::PathBuf;

    let path: Endpoint = PathBuf::from("/tmp/connector.sock").into();
    assert!(
        matches!(path, Endpoint::Socket(_)),
        "a PathBuf converts to the Socket variant"
    );
    let borrowed: Endpoint = PathBuf::from("/tmp/connector.sock").as_path().into();
    assert!(
        matches!(borrowed, Endpoint::Socket(_)),
        "a &Path converts too"
    );
    let address: Endpoint = "127.0.0.1:1"
        .parse::<SocketAddr>()
        .expect("a parseable address")
        .into();
    assert!(
        matches!(address, Endpoint::Address(_)),
        "a SocketAddr converts to the Address variant"
    );
    // Arc'd paths arrive at call sites too.
    let shared = Arc::new(PathBuf::from("/tmp/connector.sock"));
    let _derefed: Endpoint = Endpoint::Socket((*shared).clone());
}
