//! Which connections a listening connector admits, and how many it holds: peers that never
//! authenticate cost an authenticated host nothing.

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use rdlt_connector::limits::ListenLimits;
use rdlt_connector::serve::{Listener, Listening, Log, Served, serve_listener};
use rdlt_connector::wire::v1;
use rdlt_connector::{
    Acknowledging, BoxFuture, ConnectContext, ConnectorId, ConnectorSpec, Destination,
    DestinationFactory, Epoch, LoadId, OpenContext, PipelineId, Reading, Source, SourceFactory,
    acknowledging_source_factory, destination_factory, readable_destination_factory,
    source_factory,
};
use rdlt_connector_reference::{ChangesSource, MemoryDestination, MemorySource};
use rdlt_host::remote::client;
use rdlt_host::{ConnectorRef, Provider as _, ProviderError, Remote};
use rdlt_testkit::tls::Pki;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt as _, DuplexStream, ReadBuf};
use tokio::sync::mpsc;

use crate::network::identity;
use crate::networks::{Piped, Pipes, listening};

fn memory() -> (ConnectorRef, serde_json::Value) {
    let id = ConnectorId::parse("io.rapidbyte.memory").expect("a valid id");
    let reference = ConnectorRef::new(id).endpoint("grpcs://connector:7443");
    let config = serde_json::json!({ "streams": { "rows": [{ "id": 1 }] } });
    (reference, config)
}

#[tokio::test(start_paused = true)]
async fn silent_peers_do_not_delay_an_authenticated_host() {
    let pki = Pki::new("ca");
    let server = pki.server("server", &["connector"]);
    let (connections, accepted) = mpsc::unbounded_channel();
    let source = Arc::new(Served::new().with_source(source_factory::<MemorySource>()));
    tokio::spawn(serve_listener(
        source,
        Piped(accepted),
        listening(&pki, &server),
        rdlt_wire::Limits::default(),
        std::future::pending(),
    ));
    // Peers that connect and then say nothing, ahead of the host.
    let mut silent = Vec::new();
    for _ in 0..4096 {
        let (peer, connector) = tokio::io::duplex(64 * 1024);
        connections.send(connector).expect("the connector listens");
        silent.push(peer);
    }
    let (reference, config) = memory();
    let started = tokio::time::Instant::now();
    let placed = Remote::new(identity(&pki.client("host")), pki.ca())
        .network(Pipes(connections))
        .source(&reference, &config)
        .await
        .expect("the connector is placed");
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "the host waited {:?}",
        started.elapsed()
    );
    placed.connector.check().await.expect("the check passes");
    drop(silent);
}

/// A listener that counts the connections the connector holds: those accepted and not yet
/// dropped.
struct Held {
    inner: Piped,
    live: Arc<AtomicUsize>,
    most: Arc<AtomicUsize>,
}

/// A connection that counts itself out when the connector drops it.
struct Counted(DuplexStream, Arc<AtomicUsize>);

impl Drop for Counted {
    fn drop(&mut self) {
        self.1.fetch_sub(1, Ordering::SeqCst);
    }
}

impl AsyncRead for Counted {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_read(context, buffer)
    }
}

impl AsyncWrite for Counted {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(context, buffer)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(context)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(context)
    }
}

impl Listener for Held {
    type Stream = Counted;

    async fn accept(&mut self) -> std::io::Result<(Counted, SocketAddr)> {
        let (stream, peer) = self.inner.accept().await?;
        let live = self.live.fetch_add(1, Ordering::SeqCst) + 1;
        self.most.fetch_max(live, Ordering::SeqCst);
        Ok((Counted(stream, Arc::clone(&self.live)), peer))
    }
}

/// A memory source listening for the hosts named `hosts` within `limits`: the network that
/// reaches it, the lines it reports, and how many connections it holds now and held at most.
struct Connector {
    connections: mpsc::UnboundedSender<DuplexStream>,
    lines: Arc<Mutex<Vec<String>>>,
    live: Arc<AtomicUsize>,
    most: Arc<AtomicUsize>,
    pki: Pki,
    /// Stops the connector, as its operator would.
    stop: tokio_util::sync::CancellationToken,
    /// The connector listening, until it has stopped.
    listening: tokio::task::JoinHandle<()>,
}

impl Connector {
    fn listening(hosts: &[&str], limits: ListenLimits) -> Self {
        let source = Served::new().with_source(source_factory::<MemorySource>());
        Self::serving(source, hosts, limits)
    }

    /// As [`Connector::listening`], serving `served`.
    fn serving(served: Served, hosts: &[&str], limits: ListenLimits) -> Self {
        let pki = Pki::new("ca");
        let certificate = pki.server("server", &["connector"]);
        let accepted = rdlt_wire::tls::Accepted {
            ca: pki.ca(),
            hosts: rdlt_wire::tls::Hosts::new(hosts.iter().copied()).expect("hosts are named"),
            crl: None,
        };
        let tls = rdlt_wire::tls::server_config(&identity(&certificate), &accepted)
            .expect("the server's configuration builds");
        let lines = Arc::new(Mutex::new(Vec::new()));
        let written = Arc::clone(&lines);
        let listening = Listening {
            limits,
            log: Log::new(move |line| written.lock().expect("no panic").push(line.to_owned())),
            ..Listening::new(Arc::new(tls), accepted.hosts).expect("the hosts have their shares")
        };
        let (connections, accepted) = mpsc::unbounded_channel();
        let (live, most) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let held = Held {
            inner: Piped(accepted),
            live: Arc::clone(&live),
            most: Arc::clone(&most),
        };
        let stop = tokio_util::sync::CancellationToken::new();
        let listening = tokio::spawn(serve_listener(
            Arc::new(served),
            held,
            listening,
            rdlt_wire::Limits::default(),
            stop.clone().cancelled_owned(),
        ));
        Self {
            connections,
            lines,
            live,
            most,
            pki,
            stop,
            listening,
        }
    }

    /// A peer that connects and says nothing.
    fn silent(&self) -> DuplexStream {
        let (peer, connector) = tokio::io::duplex(64 * 1024);
        self.connections
            .send(connector)
            .expect("the connector listens");
        peer
    }

    /// The host named `host`, reaching the connector over its network.
    fn remote(&self, host: &str) -> Remote {
        Remote::new(identity(&self.pki.client(host)), self.pki.ca())
            .network(Pipes(self.connections.clone()))
    }

    fn lines(&self) -> Vec<String> {
        self.lines.lock().expect("no panic").clone()
    }

    /// How many connections the lines reported so far count as refused.
    fn refused(&self) -> u64 {
        refused(&self.lines())
    }
}

/// How many connections `lines` count as refused: each such line begins with its total.
pub(crate) fn refused(lines: &[String]) -> u64 {
    lines
        .iter()
        .filter(|line| !line.contains('`'))
        .filter_map(|line| line.split(' ').nth(1)?.parse::<u64>().ok())
        .sum()
}

/// Lets every task run as far as it can without the clock moving on by more than `pause`.
async fn settle(pause: Duration) {
    tokio::time::sleep(pause).await;
}

#[tokio::test(start_paused = true)]
async fn a_flood_of_silent_peers_takes_no_more_than_the_unauthenticated_and_leaves_a_session_served()
 {
    let limits = ListenLimits {
        unauthenticated: 8,
        ..ListenLimits::default()
    };
    let connector = Connector::listening(&["host"], limits);
    let (reference, config) = memory();
    let placed = connector
        .remote("host")
        .source(&reference, &config)
        .await
        .expect("the connector is placed");
    assert_eq!(connector.live.load(Ordering::SeqCst), 1);
    let mut silent = Vec::new();
    for wave in 0..50 {
        for _ in 0..200 {
            silent.push(connector.silent());
        }
        settle(Duration::from_millis(1)).await;
        // The session, the unauthenticated, and a peer just accepted that closes one of them.
        assert!(
            connector.most.load(Ordering::SeqCst) <= 1 + 8 + 1,
            "wave {wave}"
        );
        assert_eq!(connector.live.load(Ordering::SeqCst), 1 + 8, "wave {wave}");
        placed
            .connector
            .check()
            .await
            .expect("the session is served");
    }
    // The last of them are closed once their handshakes are overdue.
    settle(limits.handshake + Duration::from_millis(1)).await;
    assert_eq!(connector.live.load(Ordering::SeqCst), 1);
    placed
        .connector
        .check()
        .await
        .expect("the session is served");
    // Every silent peer was refused, in a line for each interval at most.
    settle(limits.report_every).await;
    let lines = connector.lines();
    let refusals: Vec<&String> = lines
        .iter()
        .filter(|line| !line.contains("`host`"))
        .collect();
    assert!((1..=2).contains(&refusals.len()), "{lines:?}");
    let refused: u64 = refusals
        .iter()
        .filter_map(|line| line.split(' ').nth(1)?.parse::<u64>().ok())
        .sum();
    assert_eq!(refused, 10_000, "{lines:?}");
    drop(silent);
}

#[tokio::test(start_paused = true)]
async fn refused_connections_are_reported_once_an_interval_however_many() {
    let limits = ListenLimits {
        report_every: Duration::from_secs(10),
        ..ListenLimits::default()
    };
    let connector = Connector::listening(&["host"], limits);
    // Thirty-five seconds of peers that send what is no TLS and leave.
    for _ in 0..350 {
        for _ in 0..20 {
            let mut peer = connector.silent();
            peer.write_all(b"GET / HTTP/1.1\r\n\r\n")
                .await
                .expect("written");
        }
        settle(Duration::from_millis(100)).await;
    }
    let lines = connector.lines();
    assert_eq!(lines.len(), 3, "{lines:?}");
    // A quiet interval reports nothing.
    settle(Duration::from_secs(30)).await;
    assert_eq!(connector.lines().len(), 4);
    assert_eq!(connector.refused(), 7000);
}

#[tokio::test(start_paused = true)]
async fn a_host_holds_no_more_connections_than_one_host_may_and_another_host_is_served() {
    let limits = ListenLimits {
        host_sessions: 2,
        ..ListenLimits::default()
    };
    let connector = Connector::listening(&["host", "other"], limits);
    let (reference, config) = memory();
    let host = connector.remote("host");
    let mut held = Vec::new();
    for _ in 0..2 {
        let placed = host.source(&reference, &config).await;
        held.push(placed.expect("the connector is placed"));
    }
    let started = tokio::time::Instant::now();
    let refused = host
        .source(&reference, &config)
        .await
        .err()
        .expect("refused");
    assert!(
        matches!(refused, ProviderError::Unreachable { .. }),
        "{refused}"
    );
    // It is closed at once, not left to wait.
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(connector.live.load(Ordering::SeqCst), 2);
    let other = connector.remote("other").source(&reference, &config).await;
    let other = other.expect("another host is served");
    other.connector.check().await.expect("the check passes");
    for placed in &held {
        placed.connector.check().await.expect("the check passes");
    }
    // Each session's host was said once.
    let lines = connector.lines();
    let named = |host: &str| lines.iter().filter(|line| line.contains(host)).count();
    assert_eq!((named("`host`"), named("`other`")), (2, 1), "{lines:?}");
    // A connection the host closes makes room for its next, and for no more than that.
    held.pop();
    settle(Duration::from_millis(10)).await;
    let again = host.source(&reference, &config).await;
    held.push(again.expect("the host is served again"));
    let refused = host
        .source(&reference, &config)
        .await
        .err()
        .expect("refused");
    assert!(
        matches!(refused, ProviderError::Unreachable { .. }),
        "{refused}"
    );
    settle(limits.report_every).await;
    assert_eq!(connector.refused(), 2, "{:?}", connector.lines());
}

#[tokio::test(start_paused = true)]
async fn connections_waiting_for_a_session_are_few_and_wait_no_longer_than_they_may() {
    let limits = ListenLimits {
        sessions: 1,
        waiting: 1,
        wait: Duration::from_secs(10),
        ..ListenLimits::default()
    };
    let connector = Connector::listening(&["host", "second", "third"], limits);
    let (reference, config) = memory();
    let first = connector.remote("host").source(&reference, &config).await;
    let first = first.expect("the connector is placed");
    // The second waits for the session; the third finds the queue full, and is closed at once.
    let second = connector.remote("second");
    let waiting = tokio::spawn({
        let (reference, config) = (reference.clone(), config.clone());
        async move {
            let started = tokio::time::Instant::now();
            let refused = second.source(&reference, &config).await.err();
            (refused, started.elapsed())
        }
    });
    settle(Duration::from_millis(10)).await;
    assert_eq!(connector.live.load(Ordering::SeqCst), 2);
    let started = tokio::time::Instant::now();
    let third = connector.remote("third").source(&reference, &config).await;
    let third = third.err().expect("refused");
    assert!(
        matches!(third, ProviderError::Unreachable { .. }),
        "{third}"
    );
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(connector.live.load(Ordering::SeqCst), 2);
    let (refused, waited) = waiting.await.expect("the dial ends");
    let refused = refused.expect("refused");
    assert!(
        matches!(refused, ProviderError::Unreachable { .. }),
        "{refused}"
    );
    assert!(
        waited >= Duration::from_secs(10) && waited < Duration::from_secs(11),
        "waited {waited:?}"
    );
    settle(Duration::from_millis(10)).await;
    assert_eq!(connector.live.load(Ordering::SeqCst), 1);
    first
        .connector
        .check()
        .await
        .expect("the session is served");
}

#[tokio::test(start_paused = true)]
async fn a_waiting_connection_is_served_once_a_session_ends() {
    let limits = ListenLimits {
        sessions: 1,
        ..ListenLimits::default()
    };
    let connector = Connector::listening(&["host", "second"], limits);
    let (reference, config) = memory();
    let first = connector.remote("host").source(&reference, &config).await;
    let first = first.expect("the connector is placed");
    let second = connector.remote("second");
    let waiting = tokio::spawn({
        let (reference, config) = (reference.clone(), config.clone());
        async move { second.source(&reference, &config).await.map(|_| ()) }
    });
    settle(Duration::from_secs(3)).await;
    assert!(!waiting.is_finished());
    drop(first);
    let served = tokio::time::timeout(Duration::from_secs(1), waiting).await;
    served
        .expect("the waiting host is served")
        .expect("the dial ends")
        .expect("the connector is placed");
}

#[tokio::test(start_paused = true)]
async fn a_certificate_the_listener_cannot_name_a_host_by_is_refused_and_counted() {
    let pki = Pki::new("ca");
    let server = pki.server("server", &["connector"]);
    let lines = Arc::new(Mutex::new(Vec::new()));
    let written = Arc::clone(&lines);
    // Its TLS accepts the host, and its list of hosts does not name it.
    let listening = Listening {
        hosts: rdlt_wire::tls::Hosts::new(["another"]).expect("a host is named"),
        log: Log::new(move |line| written.lock().expect("no panic").push(line.to_owned())),
        ..listening(&pki, &server)
    };
    let (connections, accepted) = mpsc::unbounded_channel();
    let source = Arc::new(Served::new().with_source(source_factory::<MemorySource>()));
    tokio::spawn(serve_listener(
        source,
        Piped(accepted),
        listening,
        rdlt_wire::Limits::default(),
        std::future::pending(),
    ));
    let (reference, config) = memory();
    let refused = Remote::new(identity(&pki.client("host")), pki.ca())
        .network(Pipes(connections.clone()))
        .source(&reference, &config)
        .await
        .err()
        .expect("refused");
    assert!(
        matches!(refused, ProviderError::Unreachable { .. }),
        "{refused}"
    );
    settle(ListenLimits::default().report_every + Duration::from_millis(1)).await;
    let lines = lines.lock().expect("no panic").clone();
    assert_eq!((lines.len(), self::refused(&lines)), (1, 1), "{lines:?}");
    drop(connections);
}

#[tokio::test(start_paused = true)]
async fn hosts_of_a_listening_memory_destination_each_have_stores_of_their_own() {
    let memory = Served::new().with_destination(destination_factory::<MemoryDestination>());
    let connector = Connector::serving(memory, &["host", "other"], ListenLimits::default());
    let id = ConnectorId::parse("io.rapidbyte.memory").expect("a valid id");
    let reference = ConnectorRef::new(id).endpoint("grpcs://connector:7443");
    let config = serde_json::json!({ "store": "shared" });
    let context = OpenContext {
        pipeline: PipelineId::parse("pipeline").expect("a valid id"),
        load_id: LoadId::from_parts(std::time::UNIX_EPOCH, 1),
    };
    let mut epochs = Vec::new();
    // Each host opens the same pipeline of the same store twice, over a connection each time.
    for host in ["host", "other", "host", "other"] {
        let placed = connector
            .remote(host)
            .destination(&reference, &config)
            .await;
        let placed = placed.expect("the connector is placed");
        let opened = placed.connector.open(&context).await.expect("it opens");
        epochs.push(opened.epoch);
    }
    // A host's second open follows its first, and no other host's.
    assert_eq!(epochs, [Epoch(1), Epoch(1), Epoch(2), Epoch(2)]);
}

/// The hosts a factory was asked to connect for, in order.
type Asked = Arc<Mutex<Vec<Option<String>>>>;

/// The memory factories, noting the host each connect is told it serves.
struct Noting {
    source: Box<dyn SourceFactory>,
    destination: Box<dyn DestinationFactory>,
    asked: Asked,
}

impl Noting {
    fn new(asked: &Asked) -> Self {
        Self {
            source: acknowledging_source_factory::<ChangesSource>(),
            destination: readable_destination_factory::<MemoryDestination>(),
            asked: Arc::clone(asked),
        }
    }

    fn note(&self, context: &ConnectContext) {
        let host = context.host().map(str::to_owned);
        self.asked.lock().expect("no panic").push(host);
    }
}

impl SourceFactory for Noting {
    fn spec(&self) -> &ConnectorSpec {
        self.source.spec()
    }

    fn connect(
        &self,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> BoxFuture<'_, rdlt_connector::Result<Box<dyn Source>>> {
        self.note(&context);
        self.source.connect(config, context)
    }

    fn acknowledges(&self) -> bool {
        true
    }

    fn connect_acknowledging(
        &self,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> BoxFuture<'_, rdlt_connector::Result<Acknowledging>> {
        self.note(&context);
        self.source.connect_acknowledging(config, context)
    }
}

impl DestinationFactory for Noting {
    fn spec(&self) -> &ConnectorSpec {
        self.destination.spec()
    }

    fn connect(
        &self,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> BoxFuture<'_, rdlt_connector::Result<Box<dyn Destination>>> {
        self.note(&context);
        self.destination.connect(config, context)
    }

    fn reads_back(&self) -> bool {
        true
    }

    fn connect_reading(
        &self,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> BoxFuture<'_, rdlt_connector::Result<Reading>> {
        self.note(&context);
        self.destination.connect_reading(config, context)
    }
}

#[tokio::test(start_paused = true)]
async fn a_connector_is_told_which_host_it_serves_in_either_role_and_with_either_probe() {
    let asked = Asked::default();
    let both = Served::new()
        .with_source(Box::new(Noting::new(&asked)))
        .with_destination(Box::new(Noting::new(&asked)));
    let connector = Connector::serving(both, &["host", "other"], ListenLimits::default());
    let changes = ConnectorId::parse("io.rapidbyte.changes").expect("a valid id");
    let memory = ConnectorId::parse("io.rapidbyte.memory").expect("a valid id");
    let source = ConnectorRef::new(changes).endpoint("grpcs://connector:7443");
    let destination = ConnectorRef::new(memory).endpoint("grpcs://connector:7443");
    let streams = serde_json::json!({ "seed": 1, "streams": [] });
    let store = serde_json::json!({ "store": "noted" });
    // A host's own connections, which offer no probe.
    let host = connector.remote("host");
    host.source(&source, &streams).await.expect("placed");
    host.destination(&destination, &store)
        .await
        .expect("placed");
    // Certification's, which offer each role's probe.
    let other = connector.remote("other");
    for (reference, role, feature, config) in [
        (&source, v1::Role::Source, rdlt_wire::ACKNOWLEDGED, &streams),
        (
            &destination,
            v1::Role::Destination,
            rdlt_wire::PUBLISHED,
            &store,
        ),
    ] {
        let wire = other
            .wire(reference)
            .await
            .expect("the connector is dialed");
        let mut client = client(wire, rdlt_host::Options::default())
            .await
            .expect("it connects");
        let offered = v1::HandshakeRequest {
            protocol_major: rdlt_wire::PROTOCOL_MAJOR,
            protocol_minor: rdlt_wire::PROTOCOL_MINOR,
            features: vec![feature.to_owned()],
            role: role as i32,
            traceparent: String::new(),
            limits: None,
        };
        let agreed = client.handshake(offered).await.expect("it agrees");
        assert_eq!(agreed.into_inner().accepted_features, [feature]);
        let config_json = config.to_string();
        let configured = client.configure(v1::ConfigureRequest { config_json });
        configured.await.expect("it is configured");
    }
    let asked = asked.lock().expect("no panic").clone();
    let hosts: Vec<Option<&str>> = asked.iter().map(Option::as_deref).collect();
    assert_eq!(
        hosts,
        [Some("host"), Some("host"), Some("other"), Some("other")]
    );
    // In a host's own process, and spawned by it, a connector is told of no host.
    assert_eq!(ConnectContext::new().host(), None);
    assert_eq!(ConnectContext::serving("named").host(), Some("named"));
}

#[tokio::test(start_paused = true)]
async fn hosts_serving_fewer_sessions_each_keep_a_share_no_other_host_can_take() {
    // What `--max-sessions 8` makes of the limits for two named hosts.
    let limits = ListenLimits::default()
        .shared(2, Some(8), None)
        .expect("two hosts share eight sessions");
    let connector = Connector::listening(&["host", "other"], limits);
    let (reference, config) = memory();
    let host = connector.remote("host");
    let mut held = Vec::new();
    // One host takes all it can: its share, and no more.
    for _ in 0..4 {
        let placed = host.source(&reference, &config).await;
        held.push(placed.expect("the connector is placed"));
    }
    for _ in 0..4 {
        let refused = host
            .source(&reference, &config)
            .await
            .err()
            .expect("refused");
        assert!(
            matches!(refused, ProviderError::Unreachable { .. }),
            "{refused}"
        );
    }
    // The other is served at once, to its own share.
    let other = connector.remote("other");
    for _ in 0..4 {
        let started = tokio::time::Instant::now();
        let placed = other.source(&reference, &config).await;
        held.push(placed.expect("another named host is served"));
        assert!(started.elapsed() < Duration::from_secs(1));
    }
    for placed in &held {
        placed.connector.check().await.expect("the check passes");
    }
}

/// A listener whose connections each come from the address sent with them.
struct Addressed(mpsc::UnboundedReceiver<(DuplexStream, SocketAddr)>);

impl Listener for Addressed {
    type Stream = DuplexStream;

    async fn accept(&mut self) -> std::io::Result<(DuplexStream, SocketAddr)> {
        let accepted = self.0.recv().await;
        accepted.ok_or_else(|| std::io::Error::from(std::io::ErrorKind::BrokenPipe))
    }
}

/// A network that hands out the connection it was given, once.
#[derive(Debug)]
struct Handed(Mutex<Option<DuplexStream>>);

impl rdlt_host::Network for Handed {
    fn connect<'a>(
        &'a self,
        _host: &'a str,
        _port: u16,
    ) -> BoxFuture<'a, std::io::Result<Box<dyn rdlt_host::Stream>>> {
        let stream = self.0.lock().expect("no panic").take();
        Box::pin(async move {
            stream
                .map(|stream| Box::new(stream) as Box<dyn rdlt_host::Stream>)
                .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::ConnectionRefused))
        })
    }
}

#[tokio::test(start_paused = true)]
async fn a_flood_from_other_addresses_never_closes_a_hosts_handshake_while_one_holds_several() {
    let pki = Pki::new("ca");
    let certificate = pki.server("server", &["connector"]);
    let (connections, accepted) = mpsc::unbounded_channel();
    let source = Arc::new(Served::new().with_source(source_factory::<MemorySource>()));
    tokio::spawn(serve_listener(
        source,
        Addressed(accepted),
        listening(&pki, &certificate),
        rdlt_wire::Limits::default(),
        std::future::pending(),
    ));
    // The host has connected, from an address of its own, and not yet begun its handshake.
    let (host, connector) = tokio::io::duplex(64 * 1024);
    let own: SocketAddr = "198.51.100.1:40000".parse().expect("an address");
    connections.send((connector, own)).expect("it listens");
    // Silent peers from three other addresses, far more than the connector holds at once.
    let mut silent = Vec::new();
    for peer in 0..9000_u16 {
        let (kept, connector) = tokio::io::duplex(1024);
        let address = SocketAddr::from(([192, 0, 2, u8::try_from(peer % 3).expect("small")], peer));
        connections.send((connector, address)).expect("it listens");
        silent.push(kept);
        if peer % 500 == 0 {
            settle(Duration::from_millis(1)).await;
        }
    }
    let (reference, config) = memory();
    let placed = Remote::new(identity(&pki.client("host")), pki.ca())
        .network(Handed(Mutex::new(Some(host))))
        .source(&reference, &config)
        .await
        .expect("the host's handshake was never closed");
    placed.connector.check().await.expect("the check passes");
    drop(silent);
}

/// A listener whose connections panic when read once `broken` is set.
struct Fragile(Piped, Arc<std::sync::atomic::AtomicBool>);

struct Breaking(DuplexStream, Arc<std::sync::atomic::AtomicBool>);

impl AsyncRead for Breaking {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        assert!(!self.1.load(Ordering::SeqCst), "the connection broke");
        Pin::new(&mut self.0).poll_read(context, buffer)
    }
}

impl AsyncWrite for Breaking {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(context, buffer)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(context)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(context)
    }
}

impl Listener for Fragile {
    type Stream = Breaking;

    async fn accept(&mut self) -> std::io::Result<(Breaking, SocketAddr)> {
        let (stream, peer) = self.0.accept().await?;
        Ok((Breaking(stream, Arc::clone(&self.1)), peer))
    }
}

#[tokio::test(start_paused = true)]
async fn a_session_that_panics_gives_its_host_its_place_back() {
    let pki = Pki::new("ca");
    let certificate = pki.server("server", &["connector"]);
    let limits = ListenLimits {
        host_sessions: 1,
        ..ListenLimits::default()
    };
    let lines = Arc::new(Mutex::new(Vec::new()));
    let written = Arc::clone(&lines);
    let listening = Listening {
        limits,
        log: Log::new(move |line| written.lock().expect("no panic").push(line.to_owned())),
        ..listening(&pki, &certificate)
    };
    let (connections, accepted) = mpsc::unbounded_channel();
    let broken = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let source = Arc::new(Served::new().with_source(source_factory::<MemorySource>()));
    tokio::spawn(serve_listener(
        source,
        Fragile(Piped(accepted), Arc::clone(&broken)),
        listening,
        rdlt_wire::Limits::default(),
        std::future::pending(),
    ));
    let (reference, config) = memory();
    let remote =
        Remote::new(identity(&pki.client("host")), pki.ca()).network(Pipes(connections.clone()));
    let first = remote.source(&reference, &config).await;
    let first = first.expect("the connector is placed");
    // The session's task panics at its next read.
    broken.store(true, Ordering::SeqCst);
    first
        .connector
        .check()
        .await
        .expect_err("the session is gone");
    drop(first);
    broken.store(false, Ordering::SeqCst);
    settle(Duration::from_millis(10)).await;
    // The host holds no connection, so its next is served, and the loss is counted.
    let again = remote.source(&reference, &config).await;
    again.expect("the host is served again");
    settle(limits.report_every).await;
    let lines = lines.lock().expect("no panic").clone();
    assert_eq!(refused(&lines), 1, "{lines:?}");
    drop(connections);
}

#[tokio::test(start_paused = true)]
async fn a_host_that_does_not_ask_for_http2_is_refused_and_no_session_served() {
    use rdlt_host::Network as _;
    let connector = Connector::listening(&["host"], ListenLimits::default());
    let mut tls = rdlt_wire::tls::client_config(
        &identity(&connector.pki.client("host")),
        &connector.pki.ca(),
    )
    .expect("the host's configuration builds");
    // A named host, whose handshake asks for no protocol.
    tls.alpn_protocols.clear();
    let stream = Pipes(connector.connections.clone())
        .connect("connector", 7443)
        .await
        .expect("the pipe connects");
    let name = rustls::pki_types::ServerName::try_from("connector").expect("a valid name");
    let mut host = tokio_rustls::TlsConnector::from(Arc::new(tls))
        .connect(name, stream)
        .await
        .expect("the host's handshake completes");
    // The connector closes the connection: it answers nothing, and serves no session.
    let mut answer = Vec::new();
    let read = tokio::io::AsyncReadExt::read_to_end(&mut host, &mut answer);
    let closed = tokio::time::timeout(Duration::from_secs(1), read).await;
    assert!(closed.is_ok(), "the connection stays open");
    assert!(answer.is_empty(), "{answer:?}");
    settle(ListenLimits::default().report_every + Duration::from_millis(1)).await;
    let lines = connector.lines();
    assert_eq!((lines.len(), connector.refused()), (1, 1), "{lines:?}");
}

/// The line a connector whose hosts' revocation lists are `crl` reports once its named host was
/// refused.
async fn refusal_line(pki: &Pki, crl: Option<std::path::PathBuf>, host: &str) -> String {
    let certificate = pki.server("server", &["connector"]);
    let accepted = rdlt_wire::tls::Accepted {
        crl,
        ..crate::network::accepted(pki)
    };
    let tls = rdlt_wire::tls::server_config(&identity(&certificate), &accepted)
        .expect("the server's configuration builds");
    let lines = Arc::new(Mutex::new(Vec::new()));
    let written = Arc::clone(&lines);
    let listening = Listening {
        log: Log::new(move |line| written.lock().expect("no panic").push(line.to_owned())),
        ..Listening::new(Arc::new(tls), accepted.hosts).expect("the hosts have their shares")
    };
    let (connections, accepting) = mpsc::unbounded_channel();
    let source = Arc::new(Served::new().with_source(source_factory::<MemorySource>()));
    tokio::spawn(serve_listener(
        source,
        Piped(accepting),
        listening,
        rdlt_wire::Limits::default(),
        std::future::pending(),
    ));
    let (reference, config) = memory();
    let files = pki.dir().join(format!("{host}.pem"));
    let host = if files.exists() {
        rdlt_testkit::tls::Files {
            cert: files,
            key: pki.dir().join(format!("{host}.key")),
        }
    } else {
        pki.client(host)
    };
    let refused = Remote::new(identity(&host), pki.ca())
        .network(Pipes(connections.clone()))
        .source(&reference, &config)
        .await;
    assert!(refused.is_err(), "the host was served");
    settle(ListenLimits::default().report_every + Duration::from_millis(1)).await;
    let lines = lines.lock().expect("no panic").clone();
    assert_eq!((lines.len(), self::refused(&lines)), (1, 1), "{lines:?}");
    drop(connections);
    lines[0].clone()
}

#[tokio::test(start_paused = true)]
async fn a_host_refused_for_revocation_is_reported_apart_from_one_refused_otherwise() {
    let pki = Pki::new("ca");
    let host = pki.client("host");
    let unnamed = refusal_line(&pki, None, "another").await;
    let revoked = refusal_line(&pki, Some(pki.revoking("revoked", &[&host])), "host").await;
    let stale = refusal_line(&pki, Some(pki.revoking_stale("stale", &[])), "host").await;
    // Three reasons, three lines: an operator sees a list gone stale for what it is.
    assert_ne!(unnamed, revoked);
    assert_ne!(unnamed, stale);
    assert_ne!(revoked, stale);
}

/// A network whose streams take no more bytes from the connector once `frozen` is set, as a host
/// that vanished, or holds its window shut, takes none.
#[derive(Debug)]
struct Freezing {
    pipes: Pipes,
    frozen: Arc<std::sync::atomic::AtomicBool>,
}

impl rdlt_host::Network for Freezing {
    fn connect<'a>(
        &'a self,
        host: &'a str,
        port: u16,
    ) -> BoxFuture<'a, std::io::Result<Box<dyn rdlt_host::Stream>>> {
        Box::pin(async move {
            let stream = self.pipes.connect(host, port).await?;
            let frozen = Arc::clone(&self.frozen);
            Ok(Box::new(Frozen { stream, frozen }) as Box<dyn rdlt_host::Stream>)
        })
    }
}

/// A stream that reads nothing more once frozen.
struct Frozen {
    stream: Box<dyn rdlt_host::Stream>,
    frozen: Arc<std::sync::atomic::AtomicBool>,
}

impl AsyncRead for Frozen {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.frozen.load(Ordering::SeqCst) {
            return Poll::Pending;
        }
        Pin::new(&mut self.stream).poll_read(context, buffer)
    }
}

impl AsyncWrite for Frozen {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(context, buffer)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(context)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(context)
    }
}

/// A generator of more rows than a read ever sends here, listening within `limits`, and a host
/// placing it over a network that freezes once `frozen` is set: the read it starts, which has
/// sent its first event.
async fn reading(
    limits: ListenLimits,
    frozen: &Arc<std::sync::atomic::AtomicBool>,
) -> (
    Connector,
    tokio::task::JoinHandle<rdlt_connector::Result<()>>,
) {
    use rdlt_connector::{Partition, PartitionId, ReadRequest, StreamName};
    let served =
        Served::new().with_source(source_factory::<rdlt_connector_reference::GeneratorSource>());
    let connector = Connector::serving(served, &["host"], limits);
    let remote = Remote::new(identity(&connector.pki.client("host")), connector.pki.ca()).network(
        Freezing {
            pipes: Pipes(connector.connections.clone()),
            frozen: Arc::clone(frozen),
        },
    );
    let id = ConnectorId::parse("io.rapidbyte.generator").expect("a valid id");
    let reference = ConnectorRef::new(id).endpoint("grpcs://connector:7443");
    let config = serde_json::json!({
        "seed": 1,
        "streams": [{ "name": "rows", "rows": 100_000_000, "batch_rows": 10_000 }],
    });
    let placed = remote.source(&reference, &config).await;
    let source = placed.expect("the connector is placed").connector;
    let (sink, mut feed) = rdlt_connector::partition_channel(std::num::NonZeroUsize::MIN);
    let partition = Partition::new(PartitionId::parse("0").expect("a valid id"));
    let stream = StreamName::new("rows").expect("a valid name");
    let read = tokio::spawn(async move {
        source
            .read(ReadRequest::new(stream, partition, None), sink)
            .await
    });
    feed.recv().await.expect("the read sends an event");
    // The feed is kept, so the host keeps taking frames until it is frozen.
    tokio::spawn(async move { while feed.recv().await.is_some() {} });
    (connector, read)
}

#[tokio::test(start_paused = true)]
async fn a_connection_whose_host_takes_no_bytes_is_closed_at_its_send_wait() {
    // Shorter than HTTP/2's own keepalive patience, which a stalled write may hold off.
    const SEND: Duration = Duration::from_secs(10);
    let frozen = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let limits = ListenLimits {
        send: SEND,
        ..ListenLimits::default()
    };
    let (connector, read) = reading(limits, &frozen).await;
    frozen.store(true, Ordering::SeqCst);
    let started = tokio::time::Instant::now();
    // The connector fills the pipe, and then nothing it writes makes progress, HTTP/2's pings
    // included. Bounded on the paused clock: a connection held for ever fails here.
    let closed = async {
        while connector.live.load(Ordering::SeqCst) > 0 {
            settle(Duration::from_secs(1)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(3600), closed)
        .await
        .expect("the connection is closed");
    let elapsed = started.elapsed();
    assert!(elapsed >= SEND && elapsed < SEND * 2, "{elapsed:?}");
    read.abort();
}

#[tokio::test(start_paused = true)]
async fn a_stopping_connector_closes_a_connection_still_busy_at_its_drain_wait() {
    use crate::support::connectors::SlowCommits;
    use rdlt_connector::{CommitMeta, CommitSeq, SegmentSet};
    const DRAIN: Duration = Duration::from_secs(120);
    let limits = ListenLimits {
        drain: DRAIN,
        ..ListenLimits::default()
    };
    // A commit that takes a day, longer than a drain may.
    let day = Duration::from_hours(24);
    let served = Served::new().with_destination(SlowCommits::factory(day));
    let connector = Connector::serving(served, &["host"], limits);
    let id = ConnectorId::parse("io.rapidbyte.memory").expect("a valid id");
    let reference = ConnectorRef::new(id).endpoint("grpcs://connector:7443");
    let config = serde_json::json!({ "store": "drained" });
    let placed = connector
        .remote("host")
        .destination(&reference, &config)
        .await;
    let destination = placed.expect("the connector is placed").connector;
    let load_id = LoadId::from_parts(std::time::UNIX_EPOCH, 1);
    let context = OpenContext {
        pipeline: PipelineId::parse("drained").expect("a valid id"),
        load_id,
    };
    let mut opened = destination.open(&context).await.expect("the session opens");
    let meta = CommitMeta {
        load_id,
        commit_seq: CommitSeq::FIRST,
        epoch: opened.epoch,
        segments: SegmentSet::new(),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
        child_tables: Vec::new(),
        drop_tables: Vec::new(),
    };
    let committing = tokio::spawn(async move { opened.session.commit(&meta).await });
    settle(Duration::from_secs(1)).await;
    connector.stop.cancel();
    let started = tokio::time::Instant::now();
    tokio::time::timeout(Duration::from_secs(3600), connector.listening)
        .await
        .expect("the connector stops")
        .expect("the connector does not panic");
    let elapsed = started.elapsed();
    assert!(elapsed >= DRAIN && elapsed < DRAIN * 2, "{elapsed:?}");
    committing.abort();
}
