//! The served path: the passthrough batches through the engine with the destination, the source
//! or both served over the wire protocol, over a `UnixStream` pair and over mutual TLS on
//! loopback, in frames as large as the default coalescing target makes and in eighths of that.
//!
//! Each benchmark prints the process's CPU time a gigabyte its runs moved. The bench lives beside
//! the integration tests to serve connectors as they do.

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
use nix::sys::time::TimeValLike as _;
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

/// Which of a run's connectors are served.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Destination,
    Source,
    Both,
}

impl Mode {
    const ALL: [Self; 3] = [Self::Destination, Self::Source, Self::Both];

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

impl Transport {
    fn name(self) -> &'static str {
        match self {
            Self::Socket => "socket",
            Self::Tls => "tls",
        }
    }
}

/// The batches a run moves, each a frame on the wire: as large as the default coalescing target
/// makes them, or an eighth of that, the same rows in all.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Frames {
    Large,
    Small,
}

impl Frames {
    /// Batches and rows a batch.
    fn shape(self) -> (u32, u32) {
        match self {
            Self::Large => (Passthrough::BATCHES, Passthrough::ROWS),
            Self::Small => (Passthrough::BATCHES * 8, Passthrough::ROWS / 8),
        }
    }

    /// The replay a run's source pushes.
    fn replay(self) -> &'static str {
        match self {
            Self::Large => "large",
            Self::Small => "small",
        }
    }
}

/// One benchmark: which connectors are served, how, and the frames they move.
#[derive(Clone, Copy, Debug)]
struct Case {
    transport: Transport,
    mode: Mode,
    frames: Frames,
}

impl Case {
    /// Every mode over each transport in large and in small frames.
    fn all() -> Vec<Self> {
        let mut cases = Vec::new();
        for transport in [Transport::Socket, Transport::Tls] {
            for frames in [Frames::Large, Frames::Small] {
                cases.extend(Mode::ALL.map(|mode| Self {
                    transport,
                    mode,
                    frames,
                }));
            }
        }
        cases
    }

    /// What the case moves: its mode, batches and rows a batch.
    fn shape(self) -> String {
        let (batches, rows) = self.frames.shape();
        format!("{}/{batches}x{rows}", self.mode.name())
    }

    fn id(self) -> BenchmarkId {
        BenchmarkId::new(self.transport.name(), self.shape())
    }
}

/// The batches of one size of frame, and the listener serving them over mutual TLS once a case
/// asks for it, on the batches' runtime.
struct Workload {
    passthrough: Passthrough,
    listener: Option<Listener>,
}

/// The process's user and system time so far.
fn cpu() -> Duration {
    let usage = nix::sys::resource::getrusage(nix::sys::resource::UsageWho::RUSAGE_SELF)
        .expect("the process's usage reads");
    let seconds = |time: nix::sys::time::TimeVal| {
        Duration::from_micros(u64::try_from(time.num_microseconds()).unwrap_or(0))
    };
    seconds(usage.user_time()) + seconds(usage.system_time())
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

/// A run's source and destination, those `case` names served as it says within `options`.
async fn connectors(
    case: Case,
    listener: Option<&Listener>,
    options: &Options,
) -> (Arc<dyn Source>, Arc<dyn Destination>) {
    let (mode, transport, replay) = (case.mode, case.transport, case.frames.replay());
    let source: Arc<dyn Source> = match (mode.serves_source(), transport) {
        (false, _) => Arc::from(
            replay_factory()
                .connect(replay_config(replay), ConnectContext::new())
                .await
                .expect("the replay connects"),
        ),
        (true, Transport::Socket) => {
            let io = served::served(Served::new().with_source(replay_factory()));
            let config = replay_config(replay);
            let connection = Connection::connect(io, Role::Source, &config, *options).await;
            Arc::new(RemoteSource::new(
                connection.expect("the source handshakes"),
            ))
        }
        (true, Transport::Tls) => {
            let listener = listener.expect("a listener serves over TLS");
            let id = replay_factory().spec().id.clone();
            let reference = ConnectorRef::new(id).endpoint(&listener.endpoint);
            let (remote, config) = (listener.remote(options), replay_config(replay));
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

impl Workload {
    /// The batches of `frames` and a runtime and an engine within `cores` to move them, their
    /// replay registered.
    fn new(cores: Cores, frames: Frames) -> Self {
        let (batches, rows) = frames.shape();
        let passthrough = Passthrough::try_new(cores, batches, rows).expect("the pool starts");
        register(
            frames.replay(),
            Replayed::Batches(passthrough.batches().to_vec()),
        );
        Self {
            passthrough,
            listener: None,
        }
    }

    /// Runs `case` `runs` times, each with connectors of its own: how long the runs took, and the
    /// CPU time they took a gigabyte of the `bytes` each moved.
    fn timed(&mut self, case: Case, runs: u64, bytes: u64) -> (Duration, f64) {
        let passthrough = &self.passthrough;
        let listener = match case.transport {
            Transport::Tls => Some(
                &*self
                    .listener
                    .get_or_insert_with(|| Listener::new(passthrough)),
            ),
            Transport::Socket => None,
        };
        let options = Options::default();
        let (mut took, mut busy) = (Duration::ZERO, Duration::ZERO);
        for _ in 0..runs {
            let connected = connectors(case, listener, &options);
            let (source, destination) = passthrough.block_on(connected);
            let (started, before) = (Instant::now(), cpu());
            black_box(passthrough.run(source, destination));
            took += started.elapsed();
            busy += cpu().saturating_sub(before);
        }
        #[expect(clippy::cast_precision_loss, reason = "bytes stay far below 2^52")]
        let gigabytes = bytes as f64 * runs as f64 / 1e9;
        (took, busy.as_secs_f64() / gigabytes)
    }
}

fn served(c: &mut Criterion) {
    let cores = Cores::try_from_host().expect("the host says how many cores the bench may use");
    let mut workloads: [Option<Workload>; 2] = [None, None];
    let mut used = Vec::new();
    let mut group = c.benchmark_group("served");
    group.sample_size(10);
    for case in Case::all() {
        let (batches, rows) = case.frames.shape();
        let bytes = Passthrough::bytes(batches, rows);
        group.throughput(Throughput::Bytes(bytes));
        let mut seconds = Vec::new();
        group.bench_function(case.id(), |b| {
            let slot = &mut workloads[usize::from(case.frames == Frames::Small)];
            let workload = slot.get_or_insert_with(|| Workload::new(cores, case.frames));
            b.iter_custom(|runs| {
                let (took, a_gigabyte) = workload.timed(case, runs, bytes);
                seconds.push(a_gigabyte);
                took
            });
        });
        // Only a benchmark that ran has timed its runs, so a listing prints nothing.
        if !seconds.is_empty() {
            used.push((case, seconds));
        }
    }
    group.finish();
    print(cores, used);
}

/// Prints the layout, and the CPU time a gigabyte each case that ran took.
#[expect(
    clippy::print_stdout,
    reason = "criterion reports times; the layout and the CPU time a run takes are printed beside \
              them"
)]
fn print(cores: Cores, used: Vec<(Case, Vec<f64>)>) {
    if !used.is_empty() {
        let (workers, threads) = (cores.workers(), cores.compute_threads());
        println!("served: {workers} runtime workers, {threads} compute threads");
    }
    for (case, mut seconds) in used {
        seconds.sort_by(f64::total_cmp);
        let (low, high) = (seconds[0], seconds[seconds.len() - 1]);
        let median = seconds[seconds.len() / 2];
        println!(
            "served/{}/{}: {median:.3} CPU seconds a GB, user and system, {low:.3} to {high:.3} \
             over {} samples",
            case.transport.name(),
            case.shape(),
            seconds.len(),
        );
    }
}

criterion_group!(benches, served);
criterion_main!(benches);
