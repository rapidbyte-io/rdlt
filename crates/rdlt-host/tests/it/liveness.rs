//! The heartbeat's verdict against connectors that answer it wrongly, and hosts that stall.

use std::time::Duration;

use rdlt_connector::serve::Served;
use rdlt_connector::{Role, Source as _, source_factory};
use rdlt_connector_reference::MemorySource;
use rdlt_host::{CONNECTOR_LOST, Connection, Options, RemoteSource};

use crate::support::{Fake, Fault, serve_fake, served};

/// Options that notice a lost connector within about a tenth of a second.
fn quick() -> Options {
    Options {
        heartbeat: Duration::from_millis(20),
        missed: 3,
        ..Options::default()
    }
}

#[tokio::test]
async fn a_connector_that_echoes_heartbeats_never_sent_is_lost() {
    let connection = Connection::connect(
        serve_fake(Fake(Fault::EchoAhead)),
        Role::Source,
        &serde_json::json!({}),
        quick(),
    )
    .await
    .expect("the fake handshakes");
    let source = RemoteSource::new(connection);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let error = source.check().await.unwrap_err();
    assert_eq!(error.code(), Some(CONNECTOR_LOST));
}

#[tokio::test]
async fn a_host_that_stalls_does_not_lose_a_live_connector() {
    let io = served(Served::new().with_source(source_factory::<MemorySource>()));
    let config = serde_json::json!({ "streams": {} });
    let connection = Connection::connect(io, Role::Source, &config, quick())
        .await
        .expect("the source handshakes");
    let source = RemoteSource::new(connection);
    // The heartbeat runs; then the whole runtime stalls, as a suspended process does, for many
    // heartbeats' worth.
    tokio::time::sleep(Duration::from_millis(100)).await;
    std::thread::sleep(Duration::from_millis(300));
    tokio::time::sleep(Duration::from_millis(200)).await;
    source.check().await.expect("the source is still connected");
}
