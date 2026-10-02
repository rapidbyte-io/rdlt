//! Raw connections to connectors, spawned or listening, over which a client speaks the protocol
//! itself.

use rdlt_connector::wire::v1;
use rdlt_connector::{ConnectorId, Role};
use rdlt_host::remote::client;
use rdlt_host::{Connection, ConnectorRef, Local, Options, ProviderError, Remote};
use rdlt_testkit::tls::Pki;
use rdlt_wire::{PROTOCOL_MAJOR, PROTOCOL_MINOR};

use crate::network::{identity, listening, port};
use crate::process::example;

fn scripted() -> ConnectorRef {
    ConnectorRef::new(ConnectorId::parse("test.scripted").expect("a valid id"))
}

/// A handshake as a source.
fn handshake() -> v1::HandshakeRequest {
    v1::HandshakeRequest {
        protocol_major: PROTOCOL_MAJOR,
        protocol_minor: PROTOCOL_MINOR,
        features: Vec::new(),
        role: v1::Role::Source as i32,
        traceparent: String::new(),
        limits: None,
    }
}

#[tokio::test]
async fn a_client_speaks_the_protocol_raw_over_a_spawned_connectors_wire() {
    let local = Local::trusting_binaries().env_passthrough("LLVM_PROFILE_FILE");
    let wire = local
        .wire(&scripted().path(example("scripted_connector")))
        .await
        .expect("the connector spawns");
    assert!(format!("{wire:?}").contains("spawned: true"), "{wire:?}");
    let mut client = client(wire, Options::default())
        .await
        .expect("the client connects");
    let answer = client
        .handshake(handshake())
        .await
        .expect("the connector answers")
        .into_inner();
    assert_eq!(
        answer.spec.map(|spec| spec.id).as_deref(),
        Some("test.scripted")
    );
}

#[tokio::test]
async fn a_client_speaks_the_protocol_raw_over_a_listening_connectors_wire() {
    let pki = Pki::new("ca");
    let (_connector, address) =
        listening(&pki, &pki.server("server", &["localhost"]), "127.0.0.1:0").await;
    let endpoint = format!("grpcs://localhost:{}", port(&address));
    let wire = Remote::new(identity(&pki.client("host")), pki.ca())
        .wire(&scripted().endpoint(endpoint))
        .await
        .expect("the connector is dialed");
    let connection = Connection::connect(
        wire,
        Role::Source,
        &serde_json::json!({}),
        Options::default(),
    )
    .await
    .expect("the connector handshakes");
    let spec = connection
        .connector_spec(Role::Source)
        .expect("the spec is the contract's");
    assert_eq!(
        (spec.id.as_str(), spec.role),
        ("test.scripted", Role::Source)
    );
}

#[tokio::test]
async fn a_wire_to_a_connector_that_cannot_be_found_is_not_found() {
    let local = Local::trusting_binaries()
        .wire(&scripted().path("/nonexistent/connector"))
        .await;
    assert!(
        matches!(local, Err(ProviderError::NotFound { .. })),
        "{local:?}"
    );
    let pki = Pki::new("ca");
    let remote = Remote::new(identity(&pki.client("host")), pki.ca())
        .wire(&scripted())
        .await;
    assert!(
        matches!(remote, Err(ProviderError::NotFound { .. })),
        "{remote:?}"
    );
    let unreachable = Remote::new(identity(&pki.client("host")), pki.ca())
        .wire(&scripted().endpoint("grpcs://127.0.0.1:1"))
        .await;
    assert!(
        matches!(unreachable, Err(ProviderError::Unreachable { .. })),
        "{unreachable:?}"
    );
}
