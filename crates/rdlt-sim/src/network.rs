//! A simulated network: the source and the destination listen on hosts of their own over mutual
//! TLS, and the engine places them there through the host's remote placement, all on turmoil's
//! hosts, each a paused tokio clock the network steps together.

mod connectors;
mod faults;
mod hostile;
mod stalled;
#[cfg(test)]
mod tests;

use std::cell::RefCell;
use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::num::NonZeroU32;
use std::panic::{self, AssertUnwindSafe};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use rdlt_connector::serve::{Listener, Listening, Log, Served, serve_listener};
use rdlt_connector::{
    BoxFuture, Destination, Source, SourceFactory, destination_factory, source_factory,
};
use rdlt_host::{
    ConnectorRef, Deadlines, Identity, Network, Options, Placed, Provider as _, ProviderError,
    Remote, Stream,
};
use rdlt_testkit::tls::{Files, Pki};
use rdlt_wire::Limits;
use rdlt_wire::tls::{Accepted, Hosts};
use tokio::runtime::Handle;

use crate::canary::Canary;
use crate::changes::SimChangeSource;
use crate::destination::SimDestination;
use crate::env::SimEnv;
use crate::reached::Reached;
use crate::rng::SplitMix64;
use crate::seed::{Seed, Weight, report_failure};
use crate::source::SimSource;
use connectors::Connectors;
use faults::{Healing, disrupt};
use hostile::Watch;
use stalled::Stalls;

/// The engine's host.
const ENGINE: &str = "engine";

/// The port each connector listens on.
const PORT: u16 = 7443;

/// The simulated time each step of the network advances every host's clock.
const TICK: Duration = Duration::from_millis(1);

/// The simulated time after which a simulation that has not ended fails.
const LIMIT: Duration = Duration::from_secs(10_000_000);

/// How many times placing a connector is tried before the simulation calls it unreachable.
const PLACEMENTS: u32 = 600;

/// How long a healed network takes to deliver what it held, and its connectors to act on it.
///
/// A message's latency and a destination's pause are at most 70 ms together. It is shorter than any
/// patience a run draws, so a connection that outlives its run until its pings time out shows.
const DRAIN: Duration = Duration::from_millis(80);

/// How long placing a connector waits before trying again.
const REPLACE: Duration = Duration::from_millis(100);

/// The host of the peer that stalls, which the fault driver never disrupts.
const STALLED: &str = "stalled";

/// A connector's side of a pipeline, each on a host of its own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Side {
    Source,
    Destination,
}

impl Side {
    const BOTH: [Self; 2] = [Self::Source, Self::Destination];

    /// The name of the host the connector listens on.
    fn host(self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Destination => "destination",
        }
    }
}

/// Which source the source's host serves.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Serving {
    /// The simulated source of rows, of the exactly-once oracle's worlds.
    Rows,
    /// The simulated change source, of the change oracle's worlds.
    Changes,
}

impl Serving {
    /// A factory of the source it names; `network`'s own modules serve and dial by it.
    pub(crate) fn factory(self) -> Box<dyn SourceFactory> {
        match self {
            Self::Rows => source_factory::<SimSource>(),
            Self::Changes => source_factory::<SimChangeSource>(),
        }
    }
}

/// The network one simulation runs on: its certificates, and its listening connectors.
pub(crate) struct Net {
    pki: Pki,
    server: Files,
    client: Files,
    connectors: Connectors,
    /// The runtime of each connector's host, recorded when it starts listening.
    served: Mutex<Vec<Handle>>,
    /// What the stalled peer's connections share.
    stalls: Arc<Stalls>,
    /// Which source its source's host serves, and the peer that stalls.
    serving: Serving,
    /// Whether a hostile host attacks the connectors, which are then watched, and start again now
    /// and then under another version.
    hostile: bool,
    /// What a hostile seed's connectors keep across their lives and see of each hostile
    /// connection.
    watch: Watch,
    /// The paths the network's faults and connectors reached.
    reached: Mutex<Reached>,
}

impl Net {
    fn new(serving: Serving, hostile: bool) -> Self {
        let pki = Pki::new("ca");
        let server = pki.server(
            "server",
            &[Side::Source.host(), Side::Destination.host(), STALLED],
        );
        let client = pki.client(ENGINE);
        Self {
            pki,
            server,
            client,
            connectors: Connectors::default(),
            served: Mutex::default(),
            stalls: Arc::default(),
            serving,
            hostile,
            watch: Watch::default(),
            reached: Mutex::default(),
        }
    }
}

/// Runs `scenario` as the engine's host of a simulated network, on which the source and the
/// destination listen, and the peer that stalls, on a host of its own, and a hostile host's targets
/// where `hostile`, with a [`SimEnv`] seeded by `seed`, and turmoil's network seeded by it too.
///
/// # Panics
///
/// Re-raises any panic from `scenario` after printing the seed that reproduces it.
pub(crate) fn run_networked<F, Fut, T>(
    seed: Seed,
    serving: Serving,
    hostile: bool,
    scenario: F,
) -> T
where
    F: FnOnce(Arc<SimEnv>, Arc<Net>) -> Fut + 'static,
    Fut: Future<Output = T> + 'static,
    T: 'static,
{
    let mut rng = SplitMix64::new(seed.value() ^ LATENCY);
    let net = Arc::new(Net::new(serving, hostile));
    let mut sim = turmoil::Builder::new()
        .tick_duration(TICK)
        .simulation_duration(LIMIT)
        .min_message_latency(Duration::ZERO)
        .max_message_latency(Duration::from_millis(1 + rng.below(20)))
        .rng_seed(seed.value())
        .build();
    for side in Side::BOTH {
        let net = Arc::clone(&net);
        sim.host(side.host(), move || listen(Arc::clone(&net), side));
    }
    let peer = Arc::clone(&net);
    sim.host(STALLED, move || stalled::stall(Arc::clone(&peer)));
    let outcome = Rc::new(RefCell::new(None));
    let slot = Rc::clone(&outcome);
    sim.client(ENGINE, async move {
        let value = scenario(Arc::new(SimEnv::new(seed)), net).await;
        *slot.borrow_mut() = Some(value);
        Ok(())
    });
    let ran = panic::catch_unwind(AssertUnwindSafe(|| sim.run()));
    drop(sim);
    let error = match ran {
        Ok(Ok(())) => {
            return outcome
                .take()
                .expect("the engine's host ran its scenario to the end");
        }
        Ok(Err(error)) => error,
        Err(payload) => {
            report_failure(seed, Weight::One);
            panic::resume_unwind(payload)
        }
    };
    report_failure(seed, Weight::One);
    match error.downcast::<tokio::task::JoinError>() {
        Ok(joined) if joined.is_panic() => panic::resume_unwind(joined.into_panic()),
        Ok(joined) => panic!("{joined}"),
        Err(error) => panic!("{error}"),
    }
}

/// What the latency's draw mixes into the seed: "latency" in ASCII.
const LATENCY: u64 = 0x006c_6174_656e_6379;

/// Serves `side`'s connector on its host, again after each crash or stop, as the network's
/// connectors say.
///
/// A stopping connector lets go of its address before it drains its connections, so a connector
/// started after it listens at once, as a process restarted beside a draining process does; the
/// drain goes on apart, until its connections end.
async fn listen(net: Arc<Net>, side: Side) -> turmoil::Result {
    net.served.lock().push(Handle::current());
    // The connectors accept the engine's host alone, by the name in its certificate.
    let accepted = Accepted {
        ca: net.pki.ca(),
        hosts: Hosts::new([ENGINE]).map_err(|error| error.to_string())?,
        crl: None,
    };
    let tls = rdlt_wire::tls::server_config(&identity(&net.server), &accepted)
        .map(Arc::new)
        .map_err(|error| error.to_string())?;
    // A seed's output is its failure alone: what the connectors would report is dropped.
    let listening = Listening {
        log: Log::new(|_| {}),
        ..Listening::new(tls, accepted.hosts).map_err(|error| error.to_string())?
    };
    loop {
        net.connectors.up(side).await;
        let crashes = net.connectors.crashes(side);
        let listener = turmoil::net::TcpListener::bind((Ipv4Addr::UNSPECIFIED, PORT)).await?;
        let (held, freed) = tokio::sync::oneshot::channel::<()>();
        let served = Arc::new(match side {
            Side::Source => Served::new().with_source(net.source_factory(crashes)),
            Side::Destination => Served::new().with_destination(net.destination_factory(crashes)),
        });
        let stop = {
            let net = Arc::clone(&net);
            async move { net.connectors.stopping(side).await }
        };
        let limits = Limits::default();
        let sockets = Sockets {
            listener,
            _held: held,
        };
        let mut serving = Box::pin(serve_listener(
            served,
            sockets,
            listening.clone(),
            limits,
            stop,
        ));
        let draining = tokio::select! {
            biased;
            // Dropping the serving drops its listener and every connection at once, as a crashed
            // process does; calls it had begun run to their end.
            () = net.connectors.crashed(side, crashes) => false,
            () = &mut serving => false,
            // Stopping, it let go of the address: the next listens while it drains.
            _ = freed => true,
        };
        if draining {
            tokio::spawn(serving);
        }
    }
}

/// A turmoil host's listening socket, which tells whoever started it once it is dropped.
struct Sockets {
    listener: turmoil::net::TcpListener,
    _held: tokio::sync::oneshot::Sender<()>,
}

impl Listener for Sockets {
    type Stream = turmoil::net::TcpStream;

    async fn accept(&mut self) -> std::io::Result<(turmoil::net::TcpStream, SocketAddr)> {
        self.listener.accept().await
    }
}

/// Turmoil's network, from the engine's host.
#[derive(Debug)]
struct Turmoil;

impl Network for Turmoil {
    fn connect<'a>(
        &'a self,
        host: &'a str,
        port: u16,
    ) -> BoxFuture<'a, std::io::Result<Box<dyn Stream>>> {
        Box::pin(async move {
            let stream = turmoil::net::TcpStream::connect((host, port)).await?;
            Ok(Box::new(stream) as Box<dyn Stream>)
        })
    }
}

fn identity(files: &Files) -> Identity {
    Identity {
        cert: files.cert.clone(),
        key: files.key.clone(),
    }
}

/// Places the simulation's connectors on its network: the engine's side of it.
#[derive(Clone)]
pub(crate) struct Networked {
    net: Arc<Net>,
    remote: Remote,
    remote_options: Options,
    canary: Canary,
}

impl Networked {
    /// Places connectors on `net`, running their connections with `options`, and resolving the
    /// secrets of `canary` their configurations refer to.
    pub(crate) fn new(net: Arc<Net>, options: &Options, canary: &Canary) -> Self {
        net.stalls.judge_by(patience(options));
        let remote = Remote::new(identity(&net.client), net.pki.ca())
            .network(Turmoil)
            .options(*options)
            .secrets(canary.vault());
        Self {
            net,
            remote,
            remote_options: *options,
            canary: canary.clone(),
        }
    }

    /// The paths the network's faults and connectors reached.
    pub(crate) fn reached(&self) -> Reached {
        *self.net.reached.lock()
    }

    /// Bytes: the least credit the engine's host grants a read of a connector it placed.
    pub(crate) fn read_floor(&self) -> u64 {
        self.remote_options.read_floor
    }

    /// Runs `work` while the network is disrupted as `rng` draws, then heals the network, however
    /// `work` ends.
    pub(crate) async fn disrupting<T>(&self, rng: SplitMix64, work: impl Future<Output = T>) -> T {
        let healing = Healing(&self.net);
        // Faults last up to twice the host's patience, so it notices some and not others.
        let patience = patience(&self.remote_options);
        let done = tokio::select! {
            biased;
            done = work => done,
            never = disrupt(&self.net, rng, patience) => match never {},
        };
        drop(healing);
        // What the network held lands once it heals, a commit among them: the runs' effects are
        // judged once it has.
        tokio::time::sleep(DRAIN).await;
        done
    }

    /// Starts the source's connector again, as its operator would: the process that listens next
    /// remembers nothing the last sent.
    pub(crate) async fn restart_source(&self) {
        self.net.connectors.crash(Side::Source);
        // Long enough for its host to see it down.
        tokio::time::sleep(DRAIN).await;
        self.net.connectors.restart(Side::Source);
    }

    /// The runtime of each connector's host, so a check sees what they still run.
    pub(crate) fn hosts(&self) -> Vec<Handle> {
        self.net.served.lock().clone()
    }

    /// The source, placed with `config` on its host, or on the peer that stalls where `stalled`,
    /// trying again until it is reachable.
    pub(crate) async fn source(
        &self,
        config: &serde_json::Value,
        stalled: bool,
    ) -> Arc<dyn Source> {
        let id = self.net.serving.factory().spec().id.clone();
        let reference = reference(host(Side::Source, stalled), id);
        Arc::from(
            placed("source", &self.canary, async || {
                self.remote.source(&reference, config).await
            })
            .await,
        )
    }

    /// The destination, placed with `config` on its host, or on the peer that stalls where
    /// `stalled`, trying again until it is reachable.
    pub(crate) async fn destination(
        &self,
        config: &serde_json::Value,
        stalled: bool,
    ) -> Arc<dyn Destination> {
        let id = destination_factory::<SimDestination>().spec().id.clone();
        let reference = reference(host(Side::Destination, stalled), id);
        let place = async || self.remote.destination(&reference, config).await;
        Arc::from(placed("destination", &self.canary, place).await)
    }

    /// How many connections to the stalled peer that stalled have been judged.
    #[cfg(test)]
    pub(crate) fn stalls_judged(&self) -> usize {
        self.net.stalls.judged()
    }

    /// Every connection to the stalled peer the engine kept open beyond its heartbeat's patience
    /// once it stalled.
    pub(crate) fn stall_violations(&self) -> Vec<String> {
        self.net.stalls.violations()
    }
}

/// The connector `place` places, trying again until it is reachable; no error on the way may hold
/// a secret of `canary`'s.
pub(crate) async fn placed<C>(
    what: &str,
    canary: &Canary,
    place: impl AsyncFn() -> Result<Placed<C>, ProviderError>,
) -> C {
    let mut last = None;
    for _ in 0..PLACEMENTS {
        match place().await {
            Ok(placed) => return placed.connector,
            Err(error) => {
                canary.placed(what, &error);
                last = Some(error.to_string());
            }
        }
        tokio::time::sleep(REPLACE).await;
    }
    panic!("the {what} was never placed: {last:?}");
}

/// The reference to connector `id`, listening on `host`.
fn reference(host: &str, id: rdlt_connector::ConnectorId) -> ConnectorRef {
    ConnectorRef::new(id).endpoint(format!("grpcs://{host}:{PORT}"))
}

/// The host `side`'s connector is placed on: its own, or the stalled peer where `stalled`.
fn host(side: Side, stalled: bool) -> &'static str {
    if stalled { STALLED } else { side.host() }
}

/// How long the engine's heartbeat waits before it counts a connector lost: its interval, times
/// the beats it may miss.
fn patience(options: &Options) -> Duration {
    options.heartbeat.saturating_mul(options.missed.get())
}

/// The options the engine's connections run with, drawn from `rng`: heartbeats between 50 ms and
/// a second apart, two to five of them missed before a connector counts as lost, and between
/// 200 ms and 3 s to connect; its connectors are told `limits`, those the engine admits within.
pub(crate) fn options(rng: &mut SplitMix64, limits: Limits) -> Options {
    Options {
        heartbeat: Duration::from_millis(50 + rng.below(950)),
        missed: NonZeroU32::new(2 + u32::try_from(rng.below(4)).unwrap_or(0))
            .unwrap_or(NonZeroU32::MIN),
        deadlines: Deadlines {
            connect: Duration::from_millis(200 + rng.below(2800)),
            ..Deadlines::default()
        },
        limits,
        ..Options::default()
    }
}
