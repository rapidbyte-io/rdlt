//! A streaming phase: runs follow the source for a while as the rows of its followed streams
//! arrive, and, where the seed churns, as its partitions come and go, before the phase's runs
//! read every row to its end.

use std::collections::BTreeMap;
use std::time::Duration;

use rdlt_engine::{Report, Until};

use super::churn::{self, CHANGES, NAMING};
use super::scenario::{Scenario, pick};
use super::{DISRUPTED, Simulation};
use crate::reached::Reach;
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
        // Drawn apart, so a seed whose source does not churn draws as it always has.
        let mut churning = features
            .churn
            .then(|| SplitMix64::new(self.seed.value() ^ NAMING ^ phase_salt));
        let world = std::sync::Arc::clone(&self.world);
        if let Some(churning) = churning.as_mut() {
            world.named.name(Some(churn::first(&world, churning)));
        }
        world.produce(Some(arrived(&world, phase, &mut rng)));
        for _ in 0..RUNS {
            let deadline = Duration::from_millis(500 + rng.below(1500));
            let scenario = if features.disruptions {
                pick(&mut rng, Some(deadline))
            } else {
                Scenario::Plain
            };
            self.world.set_faulty(features.faults);
            let clean = !(features.faults || features.disruptions);
            let ticks = 1 + rng.below(20);
            let arriving = SplitMix64::new(rng.next_u64());
            // Drawn apart from the phase's draws, and only where the network is.
            let network = (features.faults && self.placing.is_some())
                .then(|| SplitMix64::new(rng.clone().next_u64() ^ DISRUPTED));
            let ticking = churning
                .as_mut()
                .map(|churning| SplitMix64::new(churning.next_u64()));
            let running = self.attempt_until(
                phase,
                scenario,
                clean,
                network,
                reports,
                Until::For(deadline),
            );
            let (ran, ()) = tokio::join!(running, arrive(&world, phase, arriving, ticks, ticking));
            world.reach(Reach::Followed);
            if ran.stopped {
                world.produce(None);
                world.named.name(None);
                return true;
            }
        }
        world.produce(None);
        world.named.name(None);
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

/// Makes more of phase `phase`'s rows arrive every tick, `ticks` times; where the source churns,
/// drawing from `churning`, it also retires or names a partition at a third of them.
async fn arrive(
    world: &World,
    phase: usize,
    mut rng: SplitMix64,
    ticks: u64,
    mut churning: Option<SplitMix64>,
) {
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
        if let Some(churning) = churning.as_mut()
            && churning.chance(CHANGES)
        {
            churn::change(world, churning);
        }
    }
}
