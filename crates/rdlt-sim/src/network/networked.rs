//! The engine's side of the simulated network: its host's connections, which place the
//! connectors on hosts of their own and disrupt the links between them.

use std::future::Future;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::{BoxFuture, Destination, Source, destination_factory};
use rdlt_host::{
    ConnectorRef, Deadlines, Network, Options, Placed, Provider as _, ProviderError, Remote, Stream,
};
use rdlt_wire::Limits;
use tokio::runtime::Handle;

use super::faults::{Healing, cut, disrupt};
use super::{DRAIN, Net, PLACEMENTS, PORT, REPLACE, STALLED, Side, identity};
use crate::canary::Canary;
use crate::destination::SimDestination;
use crate::reached::Reached;
use crate::rng::SplitMix64;
use crate::swarm::FRAMING;

/// Places the simulation's connectors on its network: the engine's side of it.
#[derive(Clone)]
pub(crate) struct Networked {
    pub(super) net: Arc<Net>,
    pub(super) remote: Remote,
    pub(super) remote_options: Options,
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
pub(super) fn reference(host: &str, id: rdlt_connector::ConnectorId) -> ConnectorRef {
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
pub(super) const LEAST_PATIENCE: Duration = Duration::from_secs(1);

/// How long the engine's heartbeat waits before it counts a connector lost: its interval, times
/// the beats it may miss.
pub(super) fn patience(options: &Options) -> Duration {
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
        ..Options::default()
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
