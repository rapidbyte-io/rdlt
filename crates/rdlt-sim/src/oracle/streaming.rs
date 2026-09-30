//! A streaming phase: runs follow the source for a while as the rows of its followed streams
//! arrive, before the phase's runs read every row to its end.

use std::collections::BTreeMap;
use std::time::Duration;

use rdlt_engine::{Report, Until};

use super::Simulation;
use super::scenario::{Scenario, pick};
use crate::rng::SplitMix64;
use crate::world::World;

/// How many runs follow the source each streaming phase.
const RUNS: usize = 2;

/// How often rows arrive.
const TICK: Duration = Duration::from_millis(100);

/// What a streaming phase's draws mix into the seed: "arrive" in ASCII.
const ARRIVALS: u64 = 0x6172_7269_7665;

impl Simulation {
    /// Where the workload streams, follows phase `phase`'s arriving rows with runs that end at a
    /// deadline, as more arrive, then lets every row arrive; true when a run met a refusal no
    /// operator can relax.
    ///
    /// The reports of runs that end join `reports`.
    /// Its draws come from a generator of their own, so a seed that does not stream draws as it
    /// always has.
    pub(super) async fn stream(&mut self, phase: usize, reports: &mut Vec<Report>) -> bool {
        let features = self.world.workload.features;
        if !features.streaming {
            return false;
        }
        let phase_salt = u64::try_from(phase).unwrap_or(0);
        let mut rng = SplitMix64::new(self.seed.value() ^ ARRIVALS ^ phase_salt);
        let world = std::sync::Arc::clone(&self.world);
        world.produce(Some(arrived(&world, phase, &mut rng)));
        for _ in 0..RUNS {
            let deadline = Duration::from_millis(500 + rng.below(1500));
            let scenario = if features.disruptions {
                pick(&mut rng)
            } else {
                Scenario::Plain
            };
            self.world.set_faulty(features.faults);
            let clean = !(features.faults || features.disruptions);
            let ticks = 1 + rng.below(20);
            let arriving = SplitMix64::new(rng.next_u64());
            let running =
                self.attempt_until(phase, scenario, clean, None, reports, Until::For(deadline));
            let (ran, ()) = tokio::join!(running, arrive(&world, phase, arriving, ticks));
            if ran.stopped {
                world.produce(None);
                return true;
            }
        }
        world.produce(None);
        false
    }
}

/// How many rows of each followed partition have arrived as phase `phase` begins: those of the
/// phases before, and some of its own.
fn arrived(world: &World, phase: usize, rng: &mut SplitMix64) -> BTreeMap<(usize, usize), usize> {
    let mut arrived = BTreeMap::new();
    for (index, stream) in world.workload.streams.iter().enumerate() {
        if !stream.follows {
            continue;
        }
        for partition in 0..stream.partitions.len() {
            let before = match phase.checked_sub(1) {
                Some(previous) => stream.rows(partition, previous).len(),
                None => 0,
            };
            let rows = stream.rows(partition, phase).len();
            let some = usize::try_from(rng.below(u64::try_from(rows - before).unwrap_or(0) + 1))
                .unwrap_or(0);
            arrived.insert((index, partition), before + some);
        }
    }
    arrived
}

/// Makes more of phase `phase`'s rows arrive every tick, `ticks` times.
async fn arrive(world: &World, phase: usize, mut rng: SplitMix64, ticks: u64) {
    for _ in 0..ticks {
        tokio::time::sleep(TICK).await;
        let mut arrived = BTreeMap::new();
        for (index, stream) in world.workload.streams.iter().enumerate() {
            if !stream.follows {
                continue;
            }
            for partition in 0..stream.partitions.len() {
                let rows = stream.rows(partition, phase).len();
                let now = world.available(index, partition, rows);
                let more = usize::try_from(rng.below(4)).unwrap_or(0);
                arrived.insert((index, partition), (now + more).min(rows));
            }
        }
        world.produce(Some(arrived));
    }
}
