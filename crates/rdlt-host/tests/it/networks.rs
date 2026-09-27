//! Connectors served through a listener of another network than the operating system's TCP, and
//! reached through that network.

use std::net::SocketAddr;
use std::sync::Arc;

use rdlt_connector::serve::{Listener, Served, serve_listener};
use rdlt_connector::{BoxFuture, ConnectorId, source_factory};
use rdlt_connector_reference::MemorySource;
use rdlt_host::{ConnectorRef, Network, Provider as _, Remote, Stream};
use rdlt_testkit::tls::Pki;
use tokio::io::DuplexStream;
use tokio::sync::{mpsc, oneshot};

use crate::network::identity;

/// A network in this process: each connection is an in-memory pipe.
#[derive(Debug)]
struct Pipes(mpsc::UnboundedSender<DuplexStream>);

impl Network for Pipes {
    fn connect<'a>(
        &'a self,
        _host: &'a str,
        _port: u16,
    ) -> BoxFuture<'a, std::io::Result<Box<dyn Stream>>> {
        Box::pin(async move {
            let (host, connector) = tokio::io::duplex(64 * 1024);
            self.0
                .send(connector)
                .map_err(|_| std::io::Error::from(std::io::ErrorKind::ConnectionRefused))?;
            Ok(Box::new(host) as Box<dyn Stream>)
        })
    }
}

/// The connector's end of [`Pipes`].
struct Piped(mpsc::UnboundedReceiver<DuplexStream>);

impl Listener for Piped {
    type Stream = DuplexStream;

    async fn accept(&mut self) -> std::io::Result<(DuplexStream, SocketAddr)> {
        let stream = self.0.recv().await;
        stream
            .map(|stream| (stream, SocketAddr::from(([127, 0, 0, 1], 1))))
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::BrokenPipe))
    }
}

#[tokio::test]
async fn a_connector_served_on_another_network_is_placed_through_it_and_stops_cleanly() {
    let pki = Pki::new("ca");
    let server = pki.server("server", &["connector"]);
    let tls = rdlt_wire::tls::server_config(&identity(&server), &pki.ca())
        .expect("the server's configuration builds");
    let (connections, accepted) = mpsc::unbounded_channel();
    let (stop, stopped) = oneshot::channel::<()>();
    let memory = Arc::new(Served::new().with_source(source_factory::<MemorySource>()));
    let listening = tokio::spawn(serve_listener(
        memory,
        Piped(accepted),
        Arc::new(tls),
        rdlt_wire::Limits::default(),
        async {
            stopped.await.ok();
        },
    ));
    let id = ConnectorId::parse("io.rapidbyte.memory").expect("a valid id");
    let reference = ConnectorRef::new(id).endpoint("grpcs://connector:7443");
    let config = serde_json::json!({ "streams": { "rows": [{ "id": 1 }] } });
    let placed = Remote::new(identity(&pki.client("host")), pki.ca())
        .network(Pipes(connections))
        .source(&reference, &config)
        .await
        .expect("the connector is placed");
    placed.connector.check().await.expect("the check passes");
    // A stop ends the listener while its host is connected.
    stop.send(()).expect("the listener runs");
    listening.await.expect("the listener stops");
}

/// A listener whose every accept fails, as one out of file descriptors does; it counts them.
struct Failing(Arc<std::sync::atomic::AtomicU32>);

impl Listener for Failing {
    type Stream = DuplexStream;

    async fn accept(&mut self) -> std::io::Result<(DuplexStream, SocketAddr)> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Err(std::io::Error::from(std::io::ErrorKind::OutOfMemory))
    }
}

#[tokio::test(start_paused = true)]
async fn a_listener_that_keeps_failing_is_tried_again_after_a_pause() {
    let pki = Pki::new("ca");
    let server = pki.server("server", &["connector"]);
    let tls = rdlt_wire::tls::server_config(&identity(&server), &pki.ca())
        .expect("the server's configuration builds");
    let accepts = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let memory = Arc::new(Served::new().with_source(source_factory::<MemorySource>()));
    let stop = tokio::time::sleep(std::time::Duration::from_secs(1));
    serve_listener(
        memory,
        Failing(Arc::clone(&accepts)),
        Arc::new(tls),
        rdlt_wire::Limits::default(),
        stop,
    )
    .await;
    // One accept every 100 ms: about ten in a second, not a spin.
    let accepted = accepts.load(std::sync::atomic::Ordering::SeqCst);
    assert!(
        (9..=11).contains(&accepted),
        "{accepted} accepts in a second"
    );
}

/// A network whose connections never complete, as one that drops every packet.
#[derive(Debug)]
struct Blackhole;

impl Network for Blackhole {
    fn connect<'a>(
        &'a self,
        _host: &'a str,
        _port: u16,
    ) -> BoxFuture<'a, std::io::Result<Box<dyn Stream>>> {
        Box::pin(std::future::pending())
    }
}

#[tokio::test(start_paused = true)]
async fn a_network_that_never_connects_is_unreachable_within_the_connect_deadline() {
    let pki = Pki::new("ca");
    let options = rdlt_host::Options {
        deadlines: rdlt_host::Deadlines {
            connect: std::time::Duration::from_secs(3),
            ..rdlt_host::Deadlines::default()
        },
        ..rdlt_host::Options::default()
    };
    let id = ConnectorId::parse("io.rapidbyte.memory").expect("a valid id");
    let reference = ConnectorRef::new(id).endpoint("grpcs://connector:7443");
    let started = tokio::time::Instant::now();
    let refused = Remote::new(identity(&pki.client("host")), pki.ca())
        .network(Blackhole)
        .options(options)
        .source(&reference, &serde_json::json!({}))
        .await
        .err()
        .expect("refused");
    assert!(
        matches!(refused, rdlt_host::ProviderError::Unreachable { .. }),
        "{refused}"
    );
    assert_eq!(started.elapsed(), std::time::Duration::from_secs(3));
}
