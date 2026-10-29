//! A simulated network: the source and the destination listen on hosts of their own over mutual
//! TLS, and the engine places them there through the host's remote placement, all on turmoil's
//! hosts, each a paused tokio clock the network steps together.
//!
//! In half the seeds the engine reaches them as it reaches a spawned connector instead: a stream
//! of its own for the control connection and each data call.

mod broken;
mod connectors;
mod faults;
mod hostile;
mod stalled;
mod streams;
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
    Connect, ConnectorRef, Deadlines, Identity, Network, Opener, Options, Placed, Provider as _,
    ProviderError, Remote, Stream,
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
use crate::swarm::FRAMING;
use connectors::Connectors;
use faults::{Healing, cut, disrupt};
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

/// The host of the connectors that break the protocol, whose link only a driver of its own
/// cuts.
const BROKEN: &str = "broken";

/// Which hostile peers a simulated network holds beside the connectors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Peers {
    /// A hostile host that attacks the connectors' serving, and connectors that start
    /// again under a version named after a secret.
    pub(crate) hostile: bool,
    /// Connectors on a host of their own that break the protocol to the engine's host.
    pub(crate) framing: bool,
}

impl Peers {
    /// No hostile peer.
    pub(crate) const HONEST: Self = Self {
        hostile: false,
        framing: false,
    };

    /// A hostile host, and no broken connector.
    #[cfg(test)]
    pub(crate) const HOSTILE: Self = Self {
        hostile: true,
        framing: false,
    };
}

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
    /// Whether connectors that break the protocol listen on a host of their own.
    framing: bool,
    /// What a hostile seed's connectors keep across their lives and see of each hostile
    /// connection.
    watch: Watch,
    /// The paths the network's faults and connectors reached.
    reached: Mutex<Reached>,
    /// Whether the engine reaches the connectors over streams of their own rather than over TLS.
    streams: bool,
    /// Where the connectors take the streams the engine opens, where it does.
    pipes: Arc<streams::Pipes>,
}

impl Net {
    fn new(serving: Serving, peers: Peers, streams: bool) -> Self {
        let pki = Pki::new("ca");
        let mut hosts = vec![Side::Source.host(), Side::Destination.host(), STALLED];
        // Named only where it listens, so no other seed's handshakes carry its name.
        if peers.framing {
            hosts.push(BROKEN);
        }
        let server = pki.server("server", &hosts);
        let client = pki.client(ENGINE);
        Self {
            pki,
            server,
            client,
            connectors: Connectors::default(),
            served: Mutex::default(),
            stalls: Arc::default(),
            serving,
            hostile: peers.hostile,
            framing: peers.framing,
            watch: Watch::default(),
            reached: Mutex::default(),
            streams,
            pipes: Arc::default(),
        }
    }
}

/// Runs `scenario` as the engine's host of a simulated network, on which the source and the
/// destination listen, and the peer that stalls, on a host of its own, a hostile host's targets
/// where `peers` holds one, and the broken connectors on a host of their own where it holds them,
/// with a [`SimEnv`] seeded by `seed`, and turmoil's network seeded by it too.
///
/// # Panics
///
/// Re-raises any panic from `scenario` after printing the seed that reproduces it.
pub(crate) fn run_networked<F, Fut, T>(seed: Seed, serving: Serving, peers: Peers, scenario: F) -> T
where
    F: FnOnce(Arc<SimEnv>, Arc<Net>) -> Fut + 'static,
    Fut: Future<Output = T> + 'static,
    T: 'static,
{
    let mut rng = SplitMix64::new(seed.value() ^ LATENCY);
    let streams = SplitMix64::new(seed.value() ^ STREAMS).chance(500);
    let net = Arc::new(Net::new(serving, peers, streams));
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
    if peers.framing {
        let broken = Arc::clone(&net);
        sim.host(BROKEN, move || broken::host(Arc::clone(&broken)));
    }
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

/// How long a connector's TLS handshake may take before its host gives up on it.
const HANDSHAKE: Duration = Duration::from_secs(10);

/// The TLS configuration every connector's host serves with, and the hosts it accepts: the
/// engine's alone, by the name in its certificate.
fn mutual(
    net: &Net,
) -> Result<(Arc<tokio_rustls::rustls::ServerConfig>, Hosts), Box<dyn std::error::Error>> {
    let accepted = Accepted {
        ca: net.pki.ca(),
        hosts: Hosts::new([ENGINE])?,
        crl: None,
    };
    let tls = Arc::new(rdlt_wire::tls::server_config(
        &identity(&net.server),
        &accepted,
    )?);
    Ok((tls, accepted.hosts))
}

/// Accepts the TLS handshake of `stream` within [`HANDSHAKE`], or none.
async fn handshake<IO>(
    acceptor: &tokio_rustls::TlsAcceptor,
    stream: IO,
) -> Option<tokio_rustls::server::TlsStream<IO>>
where
    IO: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    tokio::time::timeout(HANDSHAKE, acceptor.accept(stream))
        .await
        .ok()?
        .ok()
}

/// What the latency's draw mixes into the seed: "latency" in ASCII.
const LATENCY: u64 = 0x006c_6174_656e_6379;

/// What the draw of the streams' half mixes into the seed: "streams" in ASCII.
const STREAMS: u64 = 0x0073_7472_6561_6d73;

/// Serves `side`'s connector on its host, again after each crash or stop, as the network's
/// connectors say.
///
/// A stopping connector lets go of its address before it drains its connections, so a connector
/// started after it listens at once, as a process restarted beside a draining process does; the
/// drain goes on apart, until its connections end.
async fn listen(net: Arc<Net>, side: Side) -> turmoil::Result {
    net.served.lock().push(Handle::current());
    let (tls, hosts) = mutual(&net)?;
    // A seed's output is its failure alone: what the connectors would report is dropped.
    let listening = Listening {
        log: Log::new(|_| {}),
        ..Listening::new(tls, hosts).map_err(|error| error.to_string())?
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
        let stop = futures_util::FutureExt::shared(Box::pin(stop));
        let listening_served = serve_listener(
            Arc::clone(&served),
            sockets,
            listening.clone(),
            limits,
            stop.clone(),
        );
        let streamed = net
            .streams
            .then(|| streams::serve(Arc::clone(&net.pipes), side.host(), served, limits, stop));
        let mut serving = Box::pin(async move {
            match streamed {
                Some(streamed) => {
                    tokio::join!(listening_served, streamed);
                }
                None => {
                    listening_served.await;
                }
            }
        });
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

/// Places the connector on `host` reached over streams, through `net`'s pipes.
fn connecting(net: &Arc<Net>, host: &'static str, options: &Options, canary: &Canary) -> Connect {
    let pipes = Arc::clone(&net.pipes);
    Connect::new(move || {
        let dialing = streams::Dialing::new(Arc::clone(&pipes), host);
        Box::pin(async move { Ok(Box::new(dialing) as Box<dyn Opener>) })
    })
    .options(*options)
    .secrets(canary.vault())
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

    /// Bytes: what the network holds of one data call's stream: an HTTP/2 call's window over
    /// TLS, or a pipe's buffer.
    pub(crate) fn transport(&self) -> u64 {
        if self.net.streams {
            streams::BUFFERED
        } else {
            rdlt_wire::limits::STREAM_WINDOW
        }
    }

    /// Bytes: the frame limit the engine's host tells a connector it placed.
    pub(crate) fn frame_bytes(&self) -> u64 {
        self.remote_options.limits.frame_bytes
    }

    /// Runs `work` while the network is disrupted as `rng` draws, then heals the network, however
    /// `work` ends.
    pub(crate) async fn disrupting<T>(&self, rng: SplitMix64, work: impl Future<Output = T>) -> T {
        let healing = Healing(&self.net);
        // Faults last up to twice the host's patience, so it notices some and not others.
        let patience = patience(&self.remote_options);
        // The broken connectors' link is cut from a draw of its own, which takes nothing from
        // the other links' draws.
        let broken = SplitMix64::new(rng.clone().next_u64() ^ FRAMING);
        let done = tokio::select! {
            biased;
            done = work => done,
            never = disrupt(&self.net, rng, patience) => match never {},
            never = cut(broken, patience), if self.net.framing => match never {},
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
        if self.net.streams && !stalled {
            let connect = connecting(
                &self.net,
                Side::Source.host(),
                &self.remote_options,
                &self.canary,
            );
            let reference = ConnectorRef::new(id);
            let place = async || connect.source(&reference, config).await;
            return Arc::from(placed("source", &self.canary, place).await);
        }
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
        if self.net.streams && !stalled {
            let options = &self.remote_options;
            let connect = connecting(&self.net, Side::Destination.host(), options, &self.canary);
            let reference = ConnectorRef::new(id);
            let place = async || connect.destination(&reference, config).await;
            return Arc::from(placed("destination", &self.canary, place).await);
        }
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

/// The least a drawn heartbeat waits for its echoes before its connector counts as lost.
///
/// An echo waits its turn behind a transport frame of every call with data queued on its
/// connection. The slowest simulated link, 20 ms of latency with turmoil's 64 segments of up to a
/// TLS record in flight, carries about 1 MiB every 20 ms, so in a second a transport frame of each
/// of dozens of calls crosses it, more than a simulated run holds open on one connection.
const LEAST_PATIENCE: Duration = Duration::from_secs(1);

/// How long the engine's heartbeat waits before it counts a connector lost: its interval, times
/// the beats it may miss.
fn patience(options: &Options) -> Duration {
    options.heartbeat.saturating_mul(options.missed.get())
}

/// The options the engine's connections run with, drawn from `rng`: heartbeats between 50 ms and
/// a second apart, two to five of them missed before a connector counts as lost, the beats
/// further apart where those would leave less than [`LEAST_PATIENCE`], and between 200 ms and 3 s
/// to connect; its connectors are told `limits`, those the engine admits within.
pub(crate) fn options(rng: &mut SplitMix64, limits: Limits) -> Options {
    let drawn = Duration::from_millis(50 + rng.below(950));
    let missed =
        NonZeroU32::new(2 + u32::try_from(rng.below(4)).unwrap_or(0)).unwrap_or(NonZeroU32::MIN);
    let least = LEAST_PATIENCE
        .as_millis()
        .div_ceil(u128::from(missed.get()));
    let least = Duration::from_millis(u64::try_from(least).unwrap_or(u64::MAX));
    Options {
        heartbeat: drawn.max(least),
        missed,
        deadlines: Deadlines {
            connect: Duration::from_millis(200 + rng.below(2800)),
            ..Deadlines::default()
        },
        limits,
    }
}
