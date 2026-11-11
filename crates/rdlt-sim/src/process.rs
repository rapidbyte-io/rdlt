//! Process placement played in the engine's process: the host places each connector through
//! `Local`, as production places a spawned one, and each process it would spawn is a script that
//! serves the simulated connector over an in-memory socket, says its configuration's canary on its
//! output, and stalls, ends by itself or is killed as the seed draws.

mod installed;
mod script;
#[cfg(test)]
mod tests;

use std::convert::Infallible;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::{Destination, Source, destination_factory};
use rdlt_host::{ConnectorRef, Kills, Local, Options, Provider as _};

use crate::canary::Canary;
use crate::destination::SimDestination;
use crate::network::{Serving, placed};
use crate::rng::SplitMix64;
use crate::seed::Seed;
use installed::Installed;
use script::Script;

/// How long a host takes to act on what was done to a process: a kill, or a stall's end.
const DRAIN: Duration = Duration::from_millis(80);

/// What the played processes' draws mix into the seed: "played" in ASCII.
const PLAYED: u64 = 0x706c_6179_6564;

/// The source's and the destination's placement, each through a `Local` of its own, so a kill of
/// one leaves the other.
pub(crate) struct Spawning {
    /// The connector directory, holding what the host opens, checks and hashes, which nothing
    /// runs.
    _connectors: tempfile::TempDir,
    /// The source's, then the destination's.
    sides: [Side; 2],
    options: Options,
    /// The secrets the connectors' configurations refer to, which no placement's error may hold.
    canary: Canary,
}

/// One connector's placement: its host, what kills its processes, what plays them, and the
/// reference that names it.
struct Side {
    local: Local,
    kills: Kills,
    script: Arc<Script>,
    reference: ConnectorRef,
}

impl Spawning {
    /// Places the source `serving` names and the destination in played processes, their
    /// connections run with `options`, their secrets `canary`'s, each process and how the
    /// connector is installed drawn from `seed`.
    pub(crate) fn new(seed: Seed, options: &Options, canary: &Canary, serving: Serving) -> Self {
        Self::installed(seed, options, canary, serving, Installed::drawn(seed))
    }

    /// Places them as [`new`](Self::new) does, the connector installed as `installed` says.
    fn installed(
        seed: Seed,
        options: &Options,
        canary: &Canary,
        serving: Serving,
        installed: Installed,
    ) -> Self {
        let connectors = tempfile::tempdir().expect("a connector directory");
        let source = serving.factory().spec().id.clone();
        let destination = destination_factory::<SimDestination>().spec().id.clone();
        let mut rng = SplitMix64::new(seed.value() ^ PLAYED);
        let sides = [(1_000_000_u32, source), (2_000_000, destination)].map(|(first, id)| {
            let script = Arc::new(Script::new(rng.next_u64(), first, serving, installed));
            let (kills, playing) = (Kills::new(), Arc::clone(&script));
            let local = Local::trusting_binaries()
                .connectors(connectors.path())
                .options(*options)
                .secrets(canary.vault())
                .kills(&kills)
                .played(move |played| Ok(playing.play(played)));
            Side {
                local,
                kills,
                script,
                reference: installed.install(connectors.path(), id),
            }
        });
        Self {
            _connectors: connectors,
            sides,
            options: *options,
            canary: canary.clone(),
        }
    }

    /// The source, placed with `config`, trying again until it is placed.
    pub(crate) async fn source(&self, config: &serde_json::Value) -> Arc<dyn Source> {
        let side = &self.sides[0];
        let place = async || side.local.source(&side.reference, config).await;
        Arc::from(placed("source", &self.canary, place).await)
    }

    /// The destination, placed with `config`, trying again until it is placed.
    pub(crate) async fn destination(&self, config: &serde_json::Value) -> Arc<dyn Destination> {
        let side = &self.sides[1];
        let place = async || side.local.destination(&side.reference, config).await;
        Arc::from(placed("destination", &self.canary, place).await)
    }

    /// Bytes: the least credit the engine's host grants a read of a connector it placed.
    pub(crate) fn read_floor(&self) -> u64 {
        self.options.read_floor
    }

    /// Bytes: the frame limit the engine's host tells a connector it placed.
    pub(crate) fn frame_bytes(&self) -> u64 {
        self.options.limits.frame_bytes
    }

    /// Runs `work` while the connectors' processes are killed, stalled and ended as `rng` draws,
    /// then lets every stalled one go on, however `work` ends.
    pub(crate) async fn disrupting<T>(&self, rng: SplitMix64, work: impl Future<Output = T>) -> T {
        let healing = Healing(self);
        let patience = self
            .options
            .heartbeat
            .saturating_mul(self.options.missed.get());
        let done = tokio::select! {
            biased;
            // The work wins: the disruptions end with it.
            done = work => done,
            never = self.disrupt(rng, patience) => match never {},
        };
        drop(healing);
        tokio::time::sleep(DRAIN).await;
        done
    }

    /// Disrupts the connectors' processes as `rng` draws, until dropped, a stall lasting up to
    /// twice `patience`, so that the host notices some and not others.
    async fn disrupt(&self, mut rng: SplitMix64, patience: Duration) -> Infallible {
        let longest = u64::try_from(patience.as_millis()).unwrap_or(u64::MAX / 2) * 2;
        loop {
            tokio::time::sleep(Duration::from_millis(rng.below(500))).await;
            let side = &self.sides[usize::from(rng.chance(500))];
            match rng.below(3) {
                0 => side.kills.kill(),
                1 => {
                    side.script.stall(true);
                    tokio::time::sleep(Duration::from_millis(rng.below(longest + 1))).await;
                    side.script.stall(false);
                }
                _ => side.script.end_latest(),
            }
        }
    }

    /// Kills the source's processes, as an operator starting it again would: the next process
    /// remembers nothing the last was sent.
    pub(crate) async fn restart_source(&self) {
        self.sides[0].kills.kill();
        tokio::time::sleep(DRAIN).await;
    }
}

/// Lets every stalled process go on when dropped.
struct Healing<'a>(&'a Spawning);

impl Drop for Healing<'_> {
    fn drop(&mut self) {
        for side in &self.0.sides {
            side.script.stall(false);
        }
    }
}
