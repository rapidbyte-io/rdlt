//! Where a simulation's connectors are placed apart from the engine: on hosts of their own on a
//! simulated network, or in processes the host's process placement plays in the engine's own.

use std::future::Future;
use std::sync::Arc;

use rdlt_connector::{Destination, Source};
use rdlt_wire::Limits;
use tokio::runtime::Handle;

use crate::canary::Canary;
use crate::network::{self, Net, Networked, Serving};
use crate::process::Spawning;
use crate::reached::Reached;
use crate::rng::SplitMix64;
use crate::seed::Seed;

/// The connectors' placement, apart from the engine.
#[derive(Clone)]
pub(crate) enum Placing {
    /// On hosts of their own, over mutual TLS.
    Network(Box<Networked>),
    /// In played processes, through `Local`.
    Process(Arc<Spawning>),
}

impl Placing {
    /// Where `seed`'s connectors are placed: on `net` where there is one, in played processes
    /// serving `serving` where the seed draws `process`, and in this process otherwise.
    ///
    /// Their secrets are `canary`'s and their connections' options are drawn from `rng`, whose
    /// limits are `limits`. Drawn only where the connectors are placed apart, so every other seed
    /// draws as it always has.
    pub(crate) fn drawn(
        seed: Seed,
        process: bool,
        net: Option<Arc<Net>>,
        rng: &mut SplitMix64,
        limits: Limits,
        canary: &Canary,
        serving: Serving,
    ) -> Option<Self> {
        match net {
            Some(net) => Some(Self::Network(Box::new(Networked::new(
                net,
                &network::options(rng, limits),
                canary,
            )))),
            None if process => Some(Self::Process(Arc::new(Spawning::new(
                seed,
                &network::options(rng, limits),
                canary,
                serving,
            )))),
            None => None,
        }
    }

    /// The source, placed with `config`; on the network, on the peer that stalls where `stalled`.
    pub(crate) async fn source(
        &self,
        config: &serde_json::Value,
        stalled: bool,
    ) -> Arc<dyn Source> {
        match self {
            Self::Network(networked) => networked.source(config, stalled).await,
            // A played process stalls as the seed's disruptions draw, wherever it is placed.
            Self::Process(spawning) => spawning.source(config).await,
        }
    }

    /// The destination, placed with `config`; on the network, on the peer that stalls where
    /// `stalled`.
    pub(crate) async fn destination(
        &self,
        config: &serde_json::Value,
        stalled: bool,
    ) -> Arc<dyn Destination> {
        match self {
            Self::Network(networked) => networked.destination(config, stalled).await,
            Self::Process(spawning) => spawning.destination(config).await,
        }
    }

    /// Runs `work` while the placement is disrupted as `rng` draws, then heals it.
    pub(crate) async fn disrupting<T>(&self, rng: SplitMix64, work: impl Future<Output = T>) -> T {
        match self {
            Self::Network(networked) => networked.disrupting(rng, work).await,
            Self::Process(spawning) => spawning.disrupting(rng, work).await,
        }
    }

    /// Starts the source again, as its operator would: it remembers nothing the last was sent.
    pub(crate) async fn restart_source(&self) {
        match self {
            Self::Network(networked) => networked.restart_source().await,
            Self::Process(spawning) => spawning.restart_source().await,
        }
    }

    /// The runtime of each connector's host where they have hosts of their own; played processes
    /// run on the engine's.
    pub(crate) fn hosts(&self) -> Vec<Handle> {
        match self {
            Self::Network(networked) => networked.hosts(),
            Self::Process(_) => Vec::new(),
        }
    }

    /// The paths the network's faults and connectors reached; played processes record none.
    pub(crate) fn reached(&self) -> Reached {
        match self {
            Self::Network(networked) => networked.reached(),
            Self::Process(_) => Reached::default(),
        }
    }

    /// Bytes: the least credit the engine's host grants a read of a connector it placed.
    pub(crate) fn read_floor(&self) -> u64 {
        match self {
            Self::Network(networked) => networked.read_floor(),
            Self::Process(spawning) => spawning.read_floor(),
        }
    }

    /// Bytes: the frame limit the engine's host tells a connector it placed, within which the
    /// connector's reads hold the pushes their sources send ahead.
    pub(crate) fn frame_bytes(&self) -> u64 {
        match self {
            Self::Network(networked) => networked.frame_bytes(),
            Self::Process(spawning) => spawning.frame_bytes(),
        }
    }

    /// Every connection to the stalled peer the engine kept open beyond its heartbeat's patience;
    /// played processes have no stalled peer.
    pub(crate) fn stall_violations(&self) -> Vec<String> {
        match self {
            Self::Network(networked) => networked.stall_violations(),
            Self::Process(_) => Vec::new(),
        }
    }

    /// The placement on the network, where the connectors are there: a hostile host attacks only
    /// those.
    pub(crate) fn networked(&self) -> Option<&Networked> {
        match self {
            Self::Network(networked) => Some(networked),
            Self::Process(_) => None,
        }
    }
}
