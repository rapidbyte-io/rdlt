//! The exactly-once oracle: a seeded workload runs through faults, retries, crashes, stops and
//! concurrent runs, and the destination must end up holding exactly what a reference model says.

mod arrivals;
mod attempts;
mod changes;
mod churn;
mod config;
mod events;
mod expected;
mod faulty;
mod intruder;
mod ledger;
mod memory;
mod names;
mod pressure;
mod refusals;
mod replanned;
mod reports;
mod reset;
mod rows;
mod scenario;
mod settle;
mod streaming;
mod tables;

use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::{PartitionId, ReadMode, StreamName};
use rdlt_engine::{Env as _, Report};
use rdlt_wire::Limits;

use crate::canary::Canary;
use crate::destination::{Digest, committed_next, completions, reads_in_progress};
use crate::env::SimEnv;
use crate::network::{Net, Peers, Serving, run_networked};
use crate::placing::Placing;
use crate::reached::Reached;
use crate::rng::SplitMix64;
use crate::seed::{Seed, run, run_threaded};
use crate::swarm::Features;
use crate::workload::{PHASES, Relaxed, Row};
use crate::world::World;
use attempts::{PIPELINES, kept_to_the_protocol, plan};
pub use changes::check_changes;
use config::{config, endless, growth, pressed, waited};
use events::Runs;
use expected::Discards;
use faulty::Faulty;
use ledger::Ledger;
#[cfg(test)]
pub(crate) use memory::beyond_charge;
pub use memory::{Held, TRACE_VAR};
pub(crate) use memory::{Kept, Window, account, aside, charged, freed};
pub use pressure::Checked;
use scenario::{Scenario, pick};
use settle::settled;

/// What a draw of network faults made apart from a generator mixes in: "disrupt" in ASCII.
const DISRUPTED: u64 = 0x0064_6973_7275_7074;

/// Checks the exactly-once guarantee for the workload `seed` generates; returns what the
/// simulation left.
///
/// # Panics
///
/// Panics, naming the seed, when the destination's contents differ from the reference model, an
/// invariant breaks, a run hangs, or a task outlives its run.
pub fn check_exactly_once(seed: Seed) -> Checked {
    let features = Features::draw(&mut SplitMix64::new(seed.value()));
    if features.network {
        run_networked(
            seed,
            Serving::Rows,
            Peers {
                hostile: features.hostile,
                framing: features.framing,
            },
            move |env, net| async move { Box::pin(simulate(seed, env, Some(net), false)).await },
        )
    } else {
        run(seed, |env| async move {
            Box::pin(simulate(seed, env, None, false)).await
        })
    }
}

/// Checks the exactly-once guarantee for the workload `seed` generates, run on many threads and
/// the real clock, where races the simulation's single thread never meets can happen.
///
/// # Panics
///
/// Panics, naming the seed, as [`check_exactly_once`] does.
pub fn stress(seed: Seed) {
    run_threaded(seed, |env| async move {
        Box::pin(simulate(seed, env, None, true)).await
    });
}

/// The heap `seed`'s attempts may hold, and where its connectors are placed apart from the engine,
/// where they are, drawn from `rng`, their secrets `canary`'s and their connections' limits
/// `limits`.
fn placed(
    seed: Seed,
    world: &World,
    inline: bool,
    net: Option<Arc<Net>>,
    rng: &mut SplitMix64,
    limits: Limits,
    canary: &Canary,
) -> (Option<u64>, Option<Placing>) {
    let features = world.workload.features;
    // Played connectors share this thread with the engine as networked ones do, and their reads
    // cross the wire too; a hostile host attacks only those on the network.
    let apart = net.is_some() || features.process;
    let hostile = features.hostile && net.is_some();
    let allowance = memory::allowance(seed, inline, apart, hostile);
    let placing = Placing::drawn(
        seed,
        features.process,
        net,
        rng,
        limits,
        canary,
        Serving::Rows,
    );
    (allowance, placing)
}

/// Checks the exactly-once guarantee for the workload `seed` generates, with the connectors on
/// `net` when there is one, and in this process otherwise, its connectors' calls hanging now and
/// then unless `threaded`, whose real clock does not wait a hang out at once.
async fn simulate(seed: Seed, env: Arc<SimEnv>, net: Option<Arc<Net>>, threaded: bool) -> Checked {
    let clock = Arc::clone(&env);
    let started = clock.instant();
    let mut rng = SplitMix64::new(seed.value());
    let name = format!("oracle-{seed}");
    let canary = Canary::of(&name);
    // Kept until the checks: every event logged on this thread until then is looked at.
    let (logged, logging) = canary.watching();
    let registered = World::register(&name, &mut rng);
    let world = Arc::clone(registered.world());
    if threaded {
        world.without_hangs();
    }
    env.perturb(world.workload.features.perturb);
    let config = config(
        &mut rng,
        world.workload.features.streaming,
        endless(&world.workload),
        growth(seed),
        waited(seed, !threaded && world.workload.features.faults),
    );
    let (limits, budget) = (config.limits(), config.memory().get());
    let partitions = u64::try_from(config.partitions().get()).unwrap_or(u64::MAX);
    let buffer = config.partition_buffer().get();
    let (allowance, placing) = placed(seed, &world, env.inline(), net, &mut rng, limits, &canary);
    world.press(pressed(seed, &config, &world.workload));
    let mut simulation = Simulation {
        seed,
        runs: Runs::new(config, env),
        placing,
        relaxed: vec![Relaxed::default(); world.workload.streams.len()],
        world,
        name,
        budget,
        ledger: Ledger::default(),
        partitions,
        buffer,
        threaded,
        allowance,
        attacks: 0,
        origin: charged(),
        held: None,
    };
    // Boxed: a run's report makes the load's future larger than clippy allows on the stack.
    let stopped = Box::pin(simulation.load(&mut rng)).await;
    // What the destination holds once the workload is loaded; the checks that follow load
    // nothing more.
    let digest = simulation.world.store.lock().digest();
    if !stopped {
        simulation.check_reports(seed).await;
    }
    let mut violations = simulation.violations();
    drop(logging);
    violations.extend(logged.leaks());
    drop(registered);
    assert!(violations.is_empty(), "seed {seed}: {violations:#?}");
    let reached = reached(&simulation.world, simulation.placing.as_ref());
    simulation.checked(digest, clock.instant() - started, reached)
}

/// One simulation: its world, what its runs share, and what an operator relaxed after refusals.
struct Simulation {
    seed: Seed,
    name: String,
    world: Arc<World>,
    runs: Runs,
    /// Where the connectors are placed apart from the engine: on a simulated network or in played
    /// processes.
    placing: Option<Placing>,
    relaxed: Vec<Relaxed>,
    /// Bytes: the engine's memory budget, which no run reserves more than.
    budget: u64,
    /// What its runs left: their trace, their waits on the budget, and the last one's length.
    ledger: Ledger,
    /// Partitions each run reads at once.
    partitions: u64,
    /// Events each partition's channel holds: what a read may still send once its run stopped it,
    /// beside a send in flight.
    buffer: usize,
    /// Whether its runs share many threads and the real clock, where reads on other threads go on
    /// between the source's answer and the engine's stop for as long as the scheduler pauses
    /// the thread between them.
    threaded: bool,
    /// Bytes: the allowance an attempt's heap is held to beyond what is charged for it;
    /// `None` where nothing is measured, as `memory::allowance` says.
    allowance: Option<u64>,
    /// How many attempts its runs made, which mixes each attempt's hostile draws apart.
    attacks: u64,
    /// Bytes: what was charged on the seed's thread as it began.
    origin: i64,
    /// What the worst attempt held beyond what was charged for it, where any was measured.
    held: Option<Held>,
}

impl Simulation {
    /// What the simulation left, `simulated` being how long it took on its own clock.
    fn checked(self, digest: Digest, simulated: Duration, reached: Reached) -> Checked {
        Checked {
            digest,
            trace: self.ledger.trace(),
            waits: self.ledger.waits,
            simulated,
            features: self.world.workload.features,
            held: self.held,
            reached,
        }
    }

    /// What the world and the network found wrong.
    fn violations(&self) -> Vec<String> {
        let mut violations = self.world.violations();
        violations.extend(self.placing.iter().flat_map(Placing::stall_violations));
        // A played process is no group of this process's: none is listed among those it spawned.
        let listed = !rdlt_host::spawned().is_empty();
        violations.extend(listed.then(|| "a played process was listed".to_owned()));
        violations
    }

    /// Runs every phase, checking the destination after each and intruding before the next,
    /// then resets the last and runs it again; whether a refusal no operator can relax stopped
    /// it short.
    async fn load(&mut self, rng: &mut SplitMix64) -> bool {
        let seed = self.seed;
        let mut stopped = false;
        for phase in 0..PHASES {
            let (reports, stopped_short) = self.converge(phase, rng).await;
            stopped = stopped_short;
            settled(seed, &self.world, self.placing.as_ref()).await;
            check_contents(&self.world, phase, stopped, seed);
            check_acknowledged(&self.world, stopped, seed);
            check_discards(&self.world, phase, &reports, stopped, seed);
            if stopped {
                break;
            }
            self.intrude(seed, phase).await;
            settled(seed, &self.world, self.placing.as_ref()).await;
        }
        let last = PHASES - 1;
        if !stopped && Box::pin(self.reset(seed, last)).await {
            let (_, short) = self.converge(last, rng).await;
            stopped = short;
            settled(seed, &self.world, self.placing.as_ref()).await;
            check_contents(&self.world, last, stopped, seed);
            check_acknowledged(&self.world, stopped, seed);
        }
        stopped
    }

    /// Runs `phase` until it converges, or a refusal no operator can relax stops it short; the
    /// reports of its runs that ended, and whether it stopped.
    ///
    /// A phase converges once a run of each pipeline has succeeded and no full read is left half
    /// done, so each full read the model counts is complete and none lands in a later phase. The
    /// first four to sixteen runs have faults and disruptions where the seed has them, the rest
    /// none.
    async fn converge(&mut self, phase: usize, rng: &mut SplitMix64) -> (Vec<Report>, bool) {
        let (seed, features) = (self.seed, self.world.workload.features);
        self.world.set_phase(phase);
        let (mut runs, mut failure) = (0, None);
        let mut succeeded = vec![false; self.world.workload.pipelines];
        let mut reports = Vec::new();
        let mut faults = Faulty::draw(seed.value(), phase);
        if self.stream(phase, &mut reports).await {
            return (reports, true);
        }
        loop {
            let pending = succeeded.contains(&false) || reads_in_progress(&self.world);
            if !pending {
                break;
            }
            let disrupted = faults.faulty(runs);
            self.world.set_faulty(disrupted && features.faults);
            let draws = faults.draws(runs, rng);
            let scenario = if disrupted && features.disruptions {
                pick(draws, self.ledger.length)
            } else {
                Scenario::Plain
            };
            runs += 1;
            let clean = !(disrupted && (features.faults || features.disruptions));
            // Drawn only where the connectors are placed apart, so every other seed draws as it
            // always has.
            let network = (disrupted && features.faults && self.placing.is_some())
                .then(|| SplitMix64::new(draws.next_u64()));
            let ran = self
                .attempt(phase, scenario, clean, network, &mut reports)
                .await;
            for (pipeline, success) in ran.succeeded.iter().enumerate() {
                succeeded[pipeline] |= success;
            }
            failure = ran.failure.or(failure);
            if ran.stopped {
                return (reports, true);
            }
            assert!(
                runs < 32,
                "seed {seed}: phase {phase} did not converge; the last error was {failure:?}"
            );
        }
        (reports, false)
    }
}

/// The paths a simulation of `world` reached, and its network's where `placing` places on one.
fn reached(world: &World, placing: Option<&Placing>) -> Reached {
    world
        .reached()
        .with(placing.map_or_else(Reached::default, Placing::reached))
}

/// Checks that every offset acknowledged to a stream that cannot read again landed: a source is
/// told an offset only once the commit holding the rows before it has landed, so the destination
/// holds every row a forgetting source let go.
fn check_acknowledged(world: &World, stopped: bool, seed: Seed) {
    if stopped {
        return;
    }
    for ((stream, partition), acknowledged) in world.acknowledged.lock().iter() {
        let name = StreamName::new(stream).expect("valid stream name");
        let id = PartitionId::parse(partition).expect("valid partition id");
        let committed = committed_next(world, &name, &id);
        assert!(
            committed.is_some_and(|committed| committed >= *acknowledged),
            "seed {seed}: stream {stream} partition {partition} acknowledged offset \
             {acknowledged}, but committed {committed:?}"
        );
    }
}

/// Checks every stream's tables against the reference model after `phase`: where the phase
/// `stopped` short, each row the model expects at most as often.
fn check_contents(world: &World, phase: usize, stopped: bool, seed: Seed) {
    for stream in &world.workload.streams {
        let delivered: Vec<Row> = (0..=phase).flat_map(|done| stream.all_rows(done)).collect();
        tables::check(
            world,
            stream,
            &rows::groups(world, stream, phase, stopped, seed),
            &delivered,
            stopped,
            seed,
        );
    }
}

/// Checks the rows and values each stream's policy discarded in `phase`, as its runs' `reports`
/// count them, against those the model's policy discards: within the least and most it may
/// discard where the reports are complete, and otherwise never more, as a dropped run's report
/// or a commit whose response was lost goes uncounted, and a phase that `stopped` short may leave
/// a full read in progress.
fn check_discards(world: &World, phase: usize, reports: &[Report], stopped: bool, seed: Seed) {
    let complete = world.workload.features.reports_complete() && !stopped;
    for stream in &world.workload.streams {
        let counted = reports
            .iter()
            .filter_map(|report| report.streams.get(&stream.name))
            .fold((0, 0), |(rows, values), counts| {
                (
                    rows + counts.discarded_rows,
                    values + counts.discarded_values,
                )
            });
        let read: Vec<Row> = match stream.read {
            ReadMode::Full => {
                let copies = completions(world, &stream.name, phase) + usize::from(stopped);
                std::iter::repeat_n(stream.all_rows(phase), copies)
                    .flatten()
                    .collect()
            }
            _ => stream
                .all_rows(phase)
                .into_iter()
                .filter(|row| row.delivered == phase)
                .collect(),
        };
        let expected = read.iter().fold(Discards::default(), |sum, row| {
            let discards = expected::discards(stream, row);
            Discards {
                rows: (sum.rows.0 + discards.rows.0, sum.rows.1 + discards.rows.1),
                values: (
                    sum.values.0 + discards.values.0,
                    sum.values.1 + discards.values.1,
                ),
            }
        });
        let within = |counted: u64, (least, most): (u64, u64)| {
            counted <= most && (!complete || counted >= least)
        };
        assert!(
            within(counted.0, expected.rows) && within(counted.1, expected.values),
            "seed {seed}: stream {} ({:?}, {:?}, {:?}) in phase {phase} counted {counted:?} \
             (rows, values) discarded; the model counts {expected:?}, which reports that are \
             {} must {}",
            stream.name,
            stream.read,
            stream.write,
            stream.schema,
            if complete { "complete" } else { "incomplete" },
            if complete {
                "fall within"
            } else {
                "not exceed"
            },
        );
    }
}
