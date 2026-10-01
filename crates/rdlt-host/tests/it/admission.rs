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
use rdlt_connector::{ConnectorId, source_factory};
use rdlt_connector_reference::MemorySource;
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
}

impl Connector {
    fn listening(hosts: &[&str], limits: ListenLimits) -> Self {
        let pki = Pki::new("ca");
        let server = pki.server("server", &["connector"]);
        let accepted = rdlt_wire::tls::Accepted {
            ca: pki.ca(),
            hosts: rdlt_wire::tls::Hosts::new(hosts.iter().copied()).expect("hosts are named"),
            crl: None,
        };
        let tls = rdlt_wire::tls::server_config(&identity(&server), &accepted)
            .expect("the server's configuration builds");
        let lines = Arc::new(Mutex::new(Vec::new()));
        let written = Arc::clone(&lines);
        let listening = Listening {
            limits,
            log: Log::new(move |line| written.lock().expect("no panic").push(line.to_owned())),
            ..Listening::new(Arc::new(tls), accepted.hosts)
        };
        let (connections, accepted) = mpsc::unbounded_channel();
        let (live, most) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let held = Held {
            inner: Piped(accepted),
            live: Arc::clone(&live),
            most: Arc::clone(&most),
        };
        let source = Arc::new(Served::new().with_source(source_factory::<MemorySource>()));
        tokio::spawn(serve_listener(
            source,
            held,
            listening,
            rdlt_wire::Limits::default(),
            std::future::pending(),
        ));
        Self {
            connections,
            lines,
            live,
            most,
            pki,
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
