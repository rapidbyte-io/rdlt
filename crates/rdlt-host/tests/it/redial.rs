//! Remote connectors that are lost: stopped by their operator or dropped, then listening again,
//! redialed, and refused when what listens again is another connector.

use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::serve::{Served, serve_connection};
use rdlt_connector::{
    BoxFuture, ConnectContext, ConnectorErrorKind, ConnectorId, ConnectorSpec, Source,
    SourceFactory, source_factory,
};
use rdlt_connector_reference::MemorySource;
use rdlt_host::{ConnectorRef, Provider as _, Remote};
use rdlt_testkit::tls::Pki;
use tokio::sync::oneshot;

use crate::network::{identity, listening, port, scripted, stop};

/// Checks `source` until it passes, which it must within 10 s, while every failure is transient:
/// the loss being noticed, or the connector not yet listening again.
async fn redialed(source: &dyn Source) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        match source.check().await {
            Ok(()) => return,
            Err(error) => {
                assert_eq!(error.kind(), ConnectorErrorKind::Transient, "{error}");
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "never redialed: {error}"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lost_connector_is_redialed_once_it_listens_again() {
    let pki = Pki::new("ca");
    let server = pki.server("server", &["localhost"]);
    let (mut connector, address) = listening(&pki, &server, "127.0.0.1:0").await;
    let endpoint = format!("grpcs://localhost:{}", port(&address));
    let options = rdlt_host::Options {
        heartbeat: Duration::from_millis(100),
        missed: 3,
        ..rdlt_host::Options::default()
    };
    let remote = Remote::new(identity(&pki.client("host")), pki.ca()).options(options);
    let placed = remote
        .source(&scripted(&endpoint), &serde_json::json!({}))
        .await
        .expect("the connector is placed");
    placed.connector.check().await.expect("the check passes");
    // Killed and reaped, so its port is free for the connector listening again.
    connector.kill().await.expect("the connector is killed");
    let (_again, _) = listening(&pki, &server, &address).await;
    redialed(placed.connector.as_ref()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stopped_connector_ends_while_its_host_is_connected_and_is_redialed() {
    let pki = Pki::new("ca");
    let server = pki.server("server", &["localhost"]);
    let (mut connector, address) = listening(&pki, &server, "127.0.0.1:0").await;
    let endpoint = format!("grpcs://localhost:{}", port(&address));
    // The default heartbeat's patience is longer than the wait below: the host notices the stop
    // from the connector itself.
    let remote = Remote::new(identity(&pki.client("host")), pki.ca());
    let placed = remote
        .source(&scripted(&endpoint), &serde_json::json!({}))
        .await
        .expect("the connector is placed");
    placed.connector.check().await.expect("the check passes");
    stop(&connector);
    let status = tokio::time::timeout(Duration::from_secs(10), connector.wait())
        .await
        .expect("it stops while its host is connected")
        .expect("its status reads");
    assert!(status.success(), "{status}");
    let (_again, _) = listening(&pki, &server, &address).await;
    redialed(placed.connector.as_ref()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stopping_connector_finishes_calls_in_flight_and_frees_its_address() {
    let pki = Pki::new("ca");
    let server = pki.server("server", &["localhost"]);
    let (mut connector, address) = listening(&pki, &server, "127.0.0.1:0").await;
    let endpoint = format!("grpcs://localhost:{}", port(&address));
    let remote = Remote::new(identity(&pki.client("host")), pki.ca());
    let placed = remote
        .source(
            &scripted(&endpoint),
            &serde_json::json!({ "slow_check_ms": 2000 }),
        )
        .await
        .expect("the connector is placed");
    let source = Arc::<dyn Source>::from(placed.connector);
    let in_flight = tokio::spawn({
        let source = Arc::clone(&source);
        async move { source.check().await }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    stop(&connector);
    // Another connector listens at the address while the stopping one drains.
    let (_again, _) =
        tokio::time::timeout(Duration::from_secs(1), listening(&pki, &server, &address))
            .await
            .expect("the address is free while the stopping connector drains");
    assert!(
        connector.try_wait().expect("its status reads").is_none(),
        "it drained before the other listened"
    );
    in_flight
        .await
        .expect("the check ran")
        .expect("the check in flight finishes");
    let status = tokio::time::timeout(Duration::from_secs(10), connector.wait())
        .await
        .expect("it stops")
        .expect("its status reads");
    assert!(status.success(), "{status}");
}

/// The memory source, under another spec.
struct Posing {
    spec: ConnectorSpec,
    inner: Box<dyn SourceFactory>,
}

impl SourceFactory for Posing {
    fn spec(&self) -> &ConnectorSpec {
        &self.spec
    }

    fn connect(
        &self,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> BoxFuture<'_, rdlt_connector::Result<Box<dyn Source>>> {
        self.inner.connect(config, context)
    }
}

/// The memory source, posing as `id` at `version`.
fn posing(id: &str, version: &str) -> Served {
    let inner = source_factory::<MemorySource>();
    let spec = ConnectorSpec {
        id: ConnectorId::parse(id).expect("a valid id"),
        version: version.to_owned(),
        ..inner.spec().clone()
    };
    Served::new().with_source(Box::new(Posing { spec, inner }))
}

/// Listens in this process over mutual TLS with `pki`'s certificates, on the port it returns.
///
/// It serves the first host `first` until `lose` fires, then drops its connection, and serves
/// every later host `then`.
async fn succeeded(pki: &Pki, first: Served, then: Served, lose: oneshot::Receiver<()>) -> u16 {
    let server = pki.server("server", &["localhost"]);
    let config = rdlt_wire::tls::server_config(&identity(&server), &pki.ca())
        .expect("the server's configuration builds");
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a port is free");
    let port = listener.local_addr().expect("a local address").port();
    tokio::spawn(async move {
        let mut serving = Some((Arc::new(first), lose));
        let then = Arc::new(then);
        loop {
            let (stream, _) = listener.accept().await.expect("a host connects");
            let tls = acceptor.accept(stream).await.expect("the host handshakes");
            let Some((first, lose)) = serving.take() else {
                let then = Arc::clone(&then);
                tokio::spawn(serve_connection(then, tls, rdlt_wire::Limits::default()));
                continue;
            };
            tokio::spawn(async move {
                tokio::select! {
                    biased;
                    _ = lose => {}
                    _ = serve_connection(first, tls, rdlt_wire::Limits::default()) => {}
                }
            });
        }
    });
    port
}

#[tokio::test(flavor = "multi_thread")]
async fn a_redialed_endpoint_serving_another_connector_is_refused() {
    let memory = "io.rapidbyte.memory";
    let version = source_factory::<MemorySource>().spec().version.clone();
    for (id, successor_version) in [("io.rapidbyte.other", version.as_str()), (memory, "9.9.9")] {
        let pki = Pki::new("ca");
        let (drop_first, dropped) = oneshot::channel();
        let successor = posing(id, successor_version);
        let port = succeeded(&pki, posing(memory, &version), successor, dropped).await;
        let reference = ConnectorRef::new(ConnectorId::parse(memory).expect("a valid id"))
            .endpoint(format!("grpcs://localhost:{port}"));
        let config = serde_json::json!({ "streams": { "rows": [{ "id": 1 }] } });
        let placed = Remote::new(identity(&pki.client("host")), pki.ca())
            .source(&reference, &config)
            .await
            .expect("the connector is placed");
        placed.connector.check().await.expect("the check passes");
        drop_first.send(()).expect("the connection is dropped");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let refused = loop {
            match placed.connector.check().await {
                Ok(()) => panic!("{id} {successor_version} was taken for {memory} {version}"),
                Err(error) if error.kind() == ConnectorErrorKind::Transient => {
                    assert!(tokio::time::Instant::now() < deadline, "{error}");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(error) => break error,
            }
        };
        assert_eq!(refused.kind(), ConnectorErrorKind::Config, "{refused}");
    }
}
