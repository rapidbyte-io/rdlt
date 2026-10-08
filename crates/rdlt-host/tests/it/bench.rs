//! The served path: the passthrough batches through the engine with the destination, the source
//! or both served over the wire protocol, over a `UnixStream` pair and over mutual TLS on
//! loopback, in frames as large as the default coalescing target makes and in eighths of that.
//!
//! Served connectors run in the bench's own process, on the engine's runtime, which measures the
//! wire path's CPU alone; or each in a process of its own, as in production, the bench's binary
//! serving as the connector, on the cores [`connector::CORES`] names. Each benchmark prints the
//! CPU time a gigabyte its runs moved: the bench's process's, and each connector process's.
//! The bench lives beside the integration tests to serve connectors as they do.

#![forbid(unsafe_code)]

#[path = "bench/connector.rs"]
mod connector;
#[path = "bench/processes.rs"]
mod processes;
#[expect(
    dead_code,
    reason = "the bench serves connectors as the tests do and needs only some of their helpers"
)]
#[path = "support/served.rs"]
mod served;

use std::hint::black_box;
use std::num::NonZeroUsize;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group};
use nix::sys::time::TimeValLike as _;
use rdlt_connector::serve::{Listening, Served, serve_listener};
use rdlt_connector::{ConnectContext, Destination, Role, Source};
use rdlt_engine::Cores;
use rdlt_engine::bench::{
    Passthrough, Replayed, Sinking, ipc_sink, register, replay_factory, sink_factory,
    split_replay_config,
};
use rdlt_host::{
    Connection, ConnectorRef, Identity, Options, Provider as _, Remote, RemoteDestination,
    RemoteSource,
};
use rdlt_testkit::tls::{Files, Pki};
use rdlt_wire::Limits;

use processes::{Placed, Processes};

/// How long the connectors the bench spawned have to stop once it is done.
const STOPPING: Duration = Duration::from_secs(20);

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

/// Where served connectors run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Layout {
    /// In the bench's own process, on the engine's runtime.
    Shared,
    /// Each in a process of its own, on the connectors' cores.
    Processes,
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

    /// The frames whose replay is `name`.
    fn named(name: &str) -> Option<Self> {
        [Self::Large, Self::Small]
            .into_iter()
            .find(|frames| frames.replay() == name)
    }

    /// The frames' place in a pair of slots.
    fn index(self) -> usize {
        usize::from(self == Self::Small)
    }

    /// The batches, as [`Passthrough`] makes them.
    fn batches(self) -> Vec<arrow_array::RecordBatch> {
        let (batches, rows) = self.shape();
        (0..batches)
            .map(|index| Passthrough::batch(i64::from(index) * i64::from(rows), rows))
            .collect()
    }
}

/// One benchmark: where and how its connectors are served, which, the frames they move, and
/// the partitions the source reads them from, each an even share.
#[derive(Clone, Copy, Debug)]
struct Case {
    layout: Layout,
    transport: Transport,
    mode: Mode,
    frames: Frames,
    partitions: NonZeroUsize,
}

impl Case {
    /// Every mode over each transport in large and in small frames, in each layout, read from
    /// one partition; and in processes of their own, a few of them in large frames read from
    /// four and from eight.
    fn all() -> Vec<Self> {
        let mut cases = Vec::new();
        for layout in [Layout::Shared, Layout::Processes] {
            for transport in [Transport::Socket, Transport::Tls] {
                for frames in [Frames::Large, Frames::Small] {
                    cases.extend(Mode::ALL.map(|mode| Self {
                        layout,
                        transport,
                        mode,
                        frames,
                        partitions: NonZeroUsize::MIN,
                    }));
                }
            }
        }
        let split = [
            (Transport::Socket, Mode::Both),
            (Transport::Tls, Mode::Destination),
            (Transport::Tls, Mode::Both),
        ];
        for (transport, mode) in split {
            cases.extend([4, 8].map(|partitions| Self {
                layout: Layout::Processes,
                transport,
                mode,
                frames: Frames::Large,
                partitions: NonZeroUsize::new(partitions).expect("a count of partitions"),
            }));
        }
        cases
    }

    /// The configuration of the replay the case's source pushes.
    fn replay(self) -> serde_json::Value {
        split_replay_config(self.frames.replay(), self.partitions)
    }

    /// Where and how its connectors are served: `process/tls`, or `tls` in the bench's process.
    fn served(self) -> String {
        match self.layout {
            Layout::Shared => self.transport.name().to_owned(),
            Layout::Processes => format!("process/{}", self.transport.name()),
        }
    }

    /// What the case moves: its mode, batches and rows a batch, and its partitions where it
    /// has several.
    fn shape(self) -> String {
        let (batches, rows) = self.frames.shape();
        let mode = self.mode.name();
        match self.partitions.get() {
            1 => format!("{mode}/{batches}x{rows}"),
            partitions => format!("{mode}/{batches}x{rows}/{partitions}-partitions"),
        }
    }

    fn id(self) -> BenchmarkId {
        BenchmarkId::new(self.served(), self.shape())
    }

    /// Whether the source runs in a process of its own.
    fn source_apart(self) -> bool {
        self.layout == Layout::Processes && self.mode.serves_source()
    }

    /// Whether the destination runs in a process of its own.
    fn destination_apart(self) -> bool {
        self.layout == Layout::Processes && self.mode.serves_destination()
    }
}

/// The certificates of mutual TLS on loopback: a CA, a connector's, and the host's.
struct Tls {
    pki: Pki,
    server: Files,
    host: Files,
}

impl Tls {
    fn new() -> Self {
        let pki = Pki::new("ca");
        let server = pki.server("server", &["localhost"]);
        let host = pki.client("host");
        Self { pki, server, host }
    }

    /// The host's provider reaching a connector of this CA within `options`.
    fn remote(&self, options: &Options) -> Remote {
        let identity = Identity {
            cert: self.host.cert.clone(),
            key: self.host.key.clone(),
        };
        Remote::new(identity, self.pki.ca()).options(*options)
    }
}

/// A listener serving both connectors over mutual TLS on loopback, on the engine's runtime.
struct Listener {
    tls: Tls,
    endpoint: String,
}

impl Listener {
    /// Listens on a free loopback port on `passthrough`'s runtime.
    fn new(passthrough: &Passthrough) -> Self {
        let tls = Tls::new();
        let accepted = rdlt_wire::tls::Accepted {
            ca: tls.pki.ca(),
            hosts: rdlt_wire::tls::Hosts::new(["host"]).expect("a host is named"),
            crl: None,
        };
        let identity = Identity {
            cert: tls.server.cert.clone(),
            key: tls.server.key.clone(),
        };
        let config = rdlt_wire::tls::server_config(&identity, &accepted).expect("the server's TLS");
        let listening =
            Listening::new(Arc::new(config), accepted.hosts).expect("a host has its share");
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
        Self {
            tls,
            endpoint: format!("grpcs://localhost:{port}"),
        }
    }
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

/// The connector processes of a run, for those that have one.
#[derive(Clone, Copy, Debug, Default)]
struct Pids {
    source: Option<u32>,
    destination: Option<u32>,
}

/// CPU time: the bench's process, which hosts the engine and every connector served in it, and
/// each connector process's, where the platform tells it.
#[derive(Clone, Copy, Debug, Default)]
struct Usage {
    host: Duration,
    source: Option<Duration>,
    destination: Option<Duration>,
}

impl Usage {
    /// What the bench's process and `pids` have taken so far.
    fn now(pids: Pids) -> Self {
        Self {
            host: cpu(),
            source: pids.source.and_then(processes::cpu),
            destination: pids.destination.and_then(processes::cpu),
        }
    }

    /// What was taken from `before` to `self`, added to `sum`.
    fn add_since(self, before: Self, sum: &mut Self) {
        let since = |after: Option<Duration>, before: Option<Duration>| {
            after
                .zip(before)
                .map(|(after, before)| after.saturating_sub(before))
        };
        let plus = |sum: Option<Duration>, more: Option<Duration>| match (sum, more) {
            (Some(sum), Some(more)) => Some(sum + more),
            (sum, more) => sum.or(more),
        };
        sum.host += self.host.saturating_sub(before.host);
        sum.source = plus(sum.source, since(self.source, before.source));
        sum.destination = plus(sum.destination, since(self.destination, before.destination));
    }
}

/// One sample's CPU seconds a gigabyte: every process's together, the bench's, and each
/// connector process's.
#[derive(Clone, Copy, Debug)]
struct Seconds {
    total: f64,
    host: f64,
    source: Option<f64>,
    destination: Option<f64>,
    busy: Busy,
}

/// The CPUs each process kept busy over a sample's runs: its CPU time over theirs.
#[derive(Clone, Copy, Debug)]
struct Busy {
    host: f64,
    source: Option<f64>,
    destination: Option<f64>,
}

/// A run's source and destination, those `case` names served as it says within `options`, and
/// the processes of those served in processes of their own.
async fn connectors(
    case: Case,
    listener: Option<&Listener>,
    processes: Option<&mut Processes>,
    options: &Options,
) -> (Arc<dyn Source>, Arc<dyn Destination>, Pids) {
    if let Some(processes) = processes {
        return apart(case, processes, options).await;
    }
    let (mode, transport, replay) = (case.mode, case.transport, case.replay());
    let source: Arc<dyn Source> = match (mode.serves_source(), transport) {
        (false, _) => in_process_source(&replay).await,
        (true, Transport::Socket) => {
            let io = served::served(Served::new().with_source(replay_factory()));
            let connection = Connection::connect(io, Role::Source, &replay, *options).await;
            Arc::new(RemoteSource::new(
                connection.expect("the source handshakes"),
            ))
        }
        (true, Transport::Tls) => {
            let listener = listener.expect("a listener serves over TLS");
            let id = replay_factory().spec().id.clone();
            let reference = ConnectorRef::new(id).endpoint(&listener.endpoint);
            let placed = listener
                .tls
                .remote(options)
                .source(&reference, &replay)
                .await;
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
            let (remote, config) = (listener.tls.remote(options), Sinking::Ipc.config());
            let placed = remote.destination(&reference, &config).await;
            Arc::from(placed.expect("the destination is placed").connector)
        }
    };
    (source, destination, Pids::default())
}

/// A run's source and destination, those `case` names served each in a process of its own,
/// and those processes.
async fn apart(
    case: Case,
    processes: &mut Processes,
    options: &Options,
) -> (Arc<dyn Source>, Arc<dyn Destination>, Pids) {
    let (mode, transport, replay) = (case.mode, case.transport, case.replay());
    let mut pids = Pids::default();
    let source = if mode.serves_source() {
        let Placed { connector, pid } = processes.source(transport, &replay, options).await;
        pids.source = Some(pid);
        connector
    } else {
        in_process_source(&replay).await
    };
    let destination = if mode.serves_destination() {
        let Placed { connector, pid } = processes.destination(transport, options).await;
        pids.destination = Some(pid);
        connector
    } else {
        ipc_sink().await
    };
    (source, destination, pids)
}

/// The replay source configured as `replay`, in the bench's process.
async fn in_process_source(replay: &serde_json::Value) -> Arc<dyn Source> {
    let connected = replay_factory()
        .connect(replay.clone(), ConnectContext::new())
        .await;
    Arc::from(connected.expect("the replay connects"))
}

/// The batches of one size of frame, and what serves them once a case asks for it: a listener
/// over mutual TLS on the batches' runtime, and connectors in processes of their own.
struct Workload {
    passthrough: Passthrough,
    listener: Option<Listener>,
    processes: Option<Processes>,
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
            processes: None,
        }
    }

    /// Runs `case` `runs` times, each with connectors of its own: how long the runs took, and the
    /// CPU time they took a gigabyte of the `bytes` each moved.
    fn timed(&mut self, case: Case, runs: u64, bytes: u64) -> (Duration, Seconds) {
        let passthrough = &self.passthrough;
        let listener = match (case.layout, case.transport) {
            (Layout::Shared, Transport::Tls) => Some(
                &*self
                    .listener
                    .get_or_insert_with(|| Listener::new(passthrough)),
            ),
            _ => None,
        };
        let mut processes = match case.layout {
            Layout::Processes => Some(self.processes.get_or_insert_with(Processes::new)),
            Layout::Shared => None,
        };
        let options = Options::default();
        let (mut took, mut usage) = (Duration::ZERO, Usage::default());
        for _ in 0..runs {
            let connected = connectors(case, listener, processes.as_deref_mut(), &options);
            let (source, destination, pids) = passthrough.block_on(connected);
            // A connector in a process of its own is held until its CPU time is read: dropped,
            // it stops.
            let apart = pids.source.is_some() || pids.destination.is_some();
            let held = apart.then(|| (Arc::clone(&source), Arc::clone(&destination)));
            let (started, before) = (Instant::now(), Usage::now(pids));
            black_box(passthrough.run(source, destination));
            took += started.elapsed();
            Usage::now(pids).add_since(before, &mut usage);
            drop(held);
        }
        #[expect(clippy::cast_precision_loss, reason = "bytes stay far below 2^52")]
        let gigabytes = bytes as f64 * runs as f64 / 1e9;
        let a_gigabyte = |time: Duration| time.as_secs_f64() / gigabytes;
        let apart = |time: Option<Duration>| time.unwrap_or_default();
        let total = usage.host + apart(usage.source) + apart(usage.destination);
        let busy = |time: Duration| time.as_secs_f64() / took.as_secs_f64();
        let seconds = Seconds {
            total: a_gigabyte(total),
            host: a_gigabyte(usage.host),
            source: usage.source.map(a_gigabyte),
            destination: usage.destination.map(a_gigabyte),
            busy: Busy {
                host: busy(usage.host),
                source: usage.source.map(busy),
                destination: usage.destination.map(busy),
            },
        };
        (took, seconds)
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
            let slot = &mut workloads[case.frames.index()];
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
    print(cores, &used);
}

/// Prints the layout, and the CPU time a gigabyte each case that ran took.
#[expect(
    clippy::print_stdout,
    reason = "criterion reports times; the layout and the CPU time a run takes are printed beside \
              them"
)]
fn print(cores: Cores, used: &[(Case, Vec<Seconds>)]) {
    if !used.is_empty() {
        let (workers, threads) = (cores.workers(), cores.compute_threads());
        let apart = std::env::var(connector::CORES).map_or_else(
            |_| "the bench's CPUs".to_owned(),
            |cpus| format!("CPUs {cpus}"),
        );
        println!(
            "served: {workers} runtime workers, {threads} compute threads; connector processes \
             on {apart}"
        );
    }
    for (case, seconds) in used {
        let total = spread(seconds.iter().map(|sample| Some(sample.total)));
        println!(
            "served/{}/{}: {total} CPU seconds a GB, user and system, over {} samples",
            case.served(),
            case.shape(),
            seconds.len(),
        );
        if case.layout == Layout::Processes {
            print_sides(*case, seconds);
        }
    }
}

/// Prints each process's CPU time a gigabyte and the CPUs it kept busy.
#[expect(
    clippy::print_stdout,
    reason = "each process's CPU time is printed beside criterion's times"
)]
fn print_sides(case: Case, seconds: &[Seconds]) {
    let side = |apart: bool, side: &dyn Fn(&Seconds) -> Option<f64>| {
        if apart {
            spread(seconds.iter().map(side))
        } else {
            "in the bench's process".to_owned()
        }
    };
    let (source, destination) = (case.source_apart(), case.destination_apart());
    let host = side(true, &|sample| Some(sample.host));
    let source_seconds = side(source, &|sample| sample.source);
    let destination_seconds = side(destination, &|sample| sample.destination);
    println!(
        "  CPU seconds a GB: the bench's process {host}; source {source_seconds}; destination \
         {destination_seconds}"
    );
    let host = side(true, &|sample| Some(sample.busy.host));
    let source_busy = side(source, &|sample| sample.busy.source);
    let destination_busy = side(destination, &|sample| sample.busy.destination);
    println!(
        "  CPUs busy: the bench's process {host}; source {source_busy}; destination \
         {destination_busy}"
    );
}

/// The median of `samples` and their range, or `unmeasured` where any is unknown.
fn spread(samples: impl Iterator<Item = Option<f64>>) -> String {
    let Some(mut samples) = samples.collect::<Option<Vec<f64>>>() else {
        return "unmeasured".to_owned();
    };
    samples.sort_by(f64::total_cmp);
    let (low, high) = (samples[0], samples[samples.len() - 1]);
    let median = samples[samples.len() / 2];
    format!("{median:.3} ({low:.3} to {high:.3})")
}

criterion_group!(benches, served);

fn main() -> ExitCode {
    if connector::asked() {
        return connector::serve();
    }
    benches();
    Criterion::default().configure_from_args().final_summary();
    rdlt_host::stop_spawned(STOPPING).expect("every connector the bench spawned stops");
    ExitCode::SUCCESS
}
