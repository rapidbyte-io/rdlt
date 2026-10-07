//! The served path: the passthrough batches through the engine with the destination, the source
//! or both served over the wire protocol, over a `UnixStream` pair and over mutual TLS on
//! loopback, the read window of a served source a parameter.
//!
//! The bench lives beside the integration tests to serve connectors as they do.

#![forbid(unsafe_code)]

#[expect(
    dead_code,
    reason = "the bench serves connectors as the tests do and needs only some of their helpers"
)]
#[path = "support/served.rs"]
mod served;

use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rdlt_connector::serve::{Listening, Served, serve_listener};
use rdlt_connector::{ConnectContext, Destination, Role, Source};
use rdlt_engine::Cores;
use rdlt_engine::bench::{
    Passthrough, Replayed, Sinking, ipc_sink, register, replay_config, replay_factory, sink_factory,
};
use rdlt_host::{
    Connection, ConnectorRef, Identity, Options, Provider as _, Remote, RemoteDestination,
    RemoteSource,
};
use rdlt_testkit::tls::{Files, Pki};
use rdlt_wire::Limits;
use rdlt_wire::limits::CREDIT_WINDOW;

/// The replay every run's source pushes.
const REPLAY: &str = "served";

/// Which of a run's connectors are served.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Destination,
    Source,
    Both,
}

impl Mode {
    fn name(self) -> &'static str {
        match self {
            Self::Destination => "destination",
            Self::Source => "source",
            Self::Both => "both",
        }
    }

    fn serves_source(self) -> bool {
        self != Self::Destination
    }

    fn serves_destination(self) -> bool {
        self != Self::Source
    }
}

/// How a served connector is reached.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Transport {
    /// Over its end of a `UnixStream` pair.
    Socket,
    /// Over mutual TLS on loopback, through a listener.
    Tls,
}

/// The cases: each mode over each transport, a served source at windows of one, two and four
/// times the default.
fn cases() -> Vec<(Transport, Mode, u64)> {
    let mut cases = Vec::new();
    for transport in [Transport::Socket, Transport::Tls] {
        cases.push((transport, Mode::Destination, CREDIT_WINDOW));
        for mode in [Mode::Source, Mode::Both] {
            let windows: &[u64] = match transport {
                Transport::Socket => &[1, 2, 4],
                Transport::Tls => &[1],
            };
            cases.extend(
                windows
                    .iter()
                    .map(|times| (transport, mode, CREDIT_WINDOW * times)),
            );
        }
    }
    cases
}

/// A listener serving both connectors over mutual TLS on loopback, and how a host reaches it.
struct Listener {
    pki: Pki,
    host: Files,
    endpoint: String,
}

impl Listener {
    /// Listens on a free loopback port on `passthrough`'s runtime.
    fn new(passthrough: &Passthrough) -> Self {
        let pki = Pki::new("ca");
        let server = pki.server("server", &["localhost"]);
        let accepted = rdlt_wire::tls::Accepted {
            ca: pki.ca(),
            hosts: rdlt_wire::tls::Hosts::new(["host"]).expect("a host is named"),
            crl: None,
        };
        let identity = Identity {
            cert: server.cert.clone(),
            key: server.key.clone(),
        };
        let tls = rdlt_wire::tls::server_config(&identity, &accepted).expect("the server's TLS");
        let listening =
            Listening::new(Arc::new(tls), accepted.hosts).expect("a host has its share");
        let both = Served::new()
            .with_source(replay_factory())
            .with_destination(sink_factory());
        let port = passthrough.block_on(async {
            let tcp = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("a port is free");
            let port = tcp.local_addr().expect("a local address").port();
            let stop = std::future::pending();
            let limits = Limits::default();
            tokio::spawn(serve_listener(Arc::new(both), tcp, listening, limits, stop));
            port
        });
        let host = pki.client("host");
        Self {
            pki,
            host,
            endpoint: format!("grpcs://localhost:{port}"),
        }
    }

    /// The host's provider reaching the listener within `options`.
    fn remote(&self, options: &Options) -> Remote {
        let identity = Identity {
            cert: self.host.cert.clone(),
            key: self.host.key.clone(),
        };
        Remote::new(identity, self.pki.ca()).options(*options)
    }
}

/// A run's source and destination, those `mode` names served over `transport` within `options`.
async fn connectors(
    mode: Mode,
    transport: Transport,
    listener: Option<&Listener>,
    options: &Options,
) -> (Arc<dyn Source>, Arc<dyn Destination>) {
    let source: Arc<dyn Source> = match (mode.serves_source(), transport) {
        (false, _) => Arc::from(
            replay_factory()
                .connect(replay_config(REPLAY), ConnectContext::new())
                .await
                .expect("the replay connects"),
        ),
        (true, Transport::Socket) => {
            let io = served::served(Served::new().with_source(replay_factory()));
            let config = replay_config(REPLAY);
            let connection = Connection::connect(io, Role::Source, &config, *options).await;
            Arc::new(RemoteSource::new(
                connection.expect("the source handshakes"),
            ))
        }
        (true, Transport::Tls) => {
            let listener = listener.expect("a listener serves over TLS");
            let id = replay_factory().spec().id.clone();
            let reference = ConnectorRef::new(id).endpoint(&listener.endpoint);
            let (remote, config) = (listener.remote(options), replay_config(REPLAY));
            let placed = remote.source(&reference, &config).await;
            Arc::from(placed.expect("the source is placed").connector)
        }
    };
    let destination: Arc<dyn Destination> = match (mode.serves_destination(), transport) {
        (false, _) => ipc_sink().await,
        (true, Transport::Socket) => {
            let io = served::served(Served::new().with_destination(sink_factory()));
            let config = Sinking::Ipc.config();
            let connection = Connection::connect(io, Role::Destination, &config, *options).await;
            let connection = connection.expect("the destination handshakes");
            Arc::new(RemoteDestination::new(connection).expect("the destination declares itself"))
        }
        (true, Transport::Tls) => {
            let listener = listener.expect("a listener serves over TLS");
            let id = sink_factory().spec().id.clone();
            let reference = ConnectorRef::new(id).endpoint(&listener.endpoint);
            let (remote, config) = (listener.remote(options), Sinking::Ipc.config());
            let placed = remote.destination(&reference, &config).await;
            Arc::from(placed.expect("the destination is placed").connector)
        }
    };
    (source, destination)
}

#[expect(
    clippy::print_stdout,
    reason = "criterion reports times; the layout is printed beside them"
)]
fn served(c: &mut Criterion) {
    let cores = Cores::try_from_host().expect("the host says how many cores the bench may use");
    let mut passthrough = None;
    let mut listener = None;
    let mut group = c.benchmark_group("served");
    group.sample_size(10);
    let bytes = Passthrough::bytes(Passthrough::BATCHES, Passthrough::ROWS);
    group.throughput(Throughput::Bytes(bytes));
    for (transport, mode, window) in cases() {
        let transport_name = match transport {
            Transport::Socket => "socket",
            Transport::Tls => "tls",
        };
        let id = match mode {
            Mode::Destination => BenchmarkId::new(transport_name, mode.name()),
            _ => BenchmarkId::new(
                transport_name,
                format!("{}/{}MiB", mode.name(), window >> 20),
            ),
        };
        group.bench_function(id, |b| {
            let passthrough = passthrough.get_or_insert_with(|| {
                let made = Passthrough::try_new(cores, Passthrough::BATCHES, Passthrough::ROWS)
                    .expect("the pool starts");
                register(REPLAY, Replayed::Batches(made.batches().to_vec()));
                println!(
                    "served: {} runtime workers, {} compute threads",
                    cores.workers(),
                    cores.compute_threads(),
                );
                made
            });
            let listener = match transport {
                Transport::Tls => {
                    Some(&*listener.get_or_insert_with(|| Listener::new(passthrough)))
                }
                Transport::Socket => None,
            };
            let options = Options {
                read_window: window,
                ..Options::default()
            };
            b.iter_custom(|runs| {
                let mut took = Duration::ZERO;
                for _ in 0..runs {
                    let (source, destination) =
                        passthrough.block_on(connectors(mode, transport, listener, &options));
                    let started = Instant::now();
                    black_box(passthrough.run(source, destination));
                    took += started.elapsed();
                }
                took
            });
        });
    }
    group.finish();
}

criterion_group!(benches, served);
criterion_main!(benches);
