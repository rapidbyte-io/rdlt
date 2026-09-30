//! The exactly-once oracle: a seeded workload runs through faults, retries, crashes, stops and
//! concurrent runs, and the destination must end up holding exactly what a reference model says.

mod arrivals;
mod changes;
mod expected;
mod intruder;
mod names;
mod refusals;
mod rows;
mod scenario;
mod tables;

use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::{ColumnPath, PartitionId, PipelineId, ReadMode, StreamName};
use rdlt_engine::{
    CommitPolicy, Engine, EngineConfig, PipelinePlan, Report, RetryPolicy, StreamPlan, WalStore,
};

use crate::destination::{Digest, committed_next, completions, reads_in_progress};
use crate::env::SimEnv;
use crate::network::{self, Net, Placing, run_networked};
use crate::rng::SplitMix64;
use crate::seed::{Seed, run, run_threaded};
use crate::swarm::Features;
use crate::workload::{Level, PHASES, Relaxed, Row, Workload};
use crate::world::World;
pub use changes::check_changes;
use expected::Discards;
use scenario::{Scenario, execute_all, pick};

/// Runs before this many have faults injected; the rest run clean, so every phase converges.
const FAULTY_RUNS: usize = 4;

/// Checks the exactly-once guarantee for the workload `seed` generates; returns a digest of what
/// the destination holds at the end, which the same seed always leaves alike.
///
/// # Panics
///
/// Panics, naming the seed, when the destination's contents differ from the reference model, an
/// invariant breaks, a run hangs, or a task outlives its run.
pub fn check_exactly_once(seed: Seed) -> Digest {
    if Features::draw(&mut SplitMix64::new(seed.value())).network {
        run_networked(seed, move |env, net| async move {
            simulate(seed, env, Some(net)).await
        })
    } else {
        run(seed, |env| async move { simulate(seed, env, None).await })
    }
}

/// Checks the exactly-once guarantee for the workload `seed` generates, run on many threads and
/// the real clock, where races the simulation's single thread never meets can happen.
///
/// # Panics
///
/// Panics, naming the seed, as [`check_exactly_once`] does.
pub fn stress(seed: Seed) {
    run_threaded(seed, |env| async move { simulate(seed, env, None).await });
}

/// Checks the exactly-once guarantee for the workload `seed` generates, with the connectors on
/// `net` when there is one, and in this process otherwise.
async fn simulate(seed: Seed, env: Arc<SimEnv>, net: Option<Arc<Net>>) -> Digest {
    let mut rng = SplitMix64::new(seed.value());
    let name = format!("oracle-{seed}");
    let world = World::register(&name, &mut rng);
    env.perturb(world.workload.features.perturb);
    env.keep_logs(Arc::clone(&world.wal) as Arc<dyn WalStore>);
    let engine = Engine::new(config(&mut rng), env);
    let placing = net.map(|net| Placing::new(net, network::options(&mut rng)));
    let mut simulation = Simulation {
        seed,
        engine,
        placing,
        relaxed: vec![Relaxed::default(); world.workload.streams.len()],
        world,
        name,
    };
    for phase in 0..PHASES {
        let (reports, stopped) = simulation.converge(phase, &mut rng).await;
        settle(seed).await;
        let world = &simulation.world;
        check_contents(world, phase, stopped, seed);
        check_acknowledged(world, stopped, seed);
        check_discards(world, phase, &reports, stopped, seed);
        if stopped {
            break;
        }
        simulation.intrude(seed, phase).await;
        settle(seed).await;
    }
    let violations = simulation.world.violations();
    let digest = simulation.world.store.lock().digest();
    World::unregister(&simulation.name);
    assert!(violations.is_empty(), "seed {seed}: {violations:#?}");
    digest
}

/// One simulation: its world, the engine its runs share, and what an operator relaxed after
/// refusals.
struct Simulation {
    seed: Seed,
    name: String,
    world: Arc<World>,
    engine: Engine,
    /// Where the connectors are placed, when they listen on a simulated network.
    placing: Option<Placing>,
    relaxed: Vec<Relaxed>,
}

impl Simulation {
    /// Runs `phase` until it converges, or a refusal no operator can relax stops it short; the
    /// reports of its runs that ended, and whether it stopped.
    ///
    /// A phase converges once a run of each pipeline has succeeded, no full read is left half
    /// done, and no write-ahead log is left to replay, so each full read the model counts is
    /// complete and none lands in a later phase.
    async fn converge(&mut self, phase: usize, rng: &mut SplitMix64) -> (Vec<Report>, bool) {
        let (seed, features) = (self.seed, self.world.workload.features);
        self.world.set_phase(phase);
        let (mut runs, mut failure) = (0, None);
        let mut succeeded = vec![false; self.world.workload.pipelines];
        let mut reports = Vec::new();
        while succeeded.contains(&false)
            || reads_in_progress(&self.world)
            || self.world.wal.holds_logs()
        {
            let faulty = runs < FAULTY_RUNS;
            self.world.set_faulty(faulty && features.faults);
            let scenario = if faulty && features.disruptions {
                pick(rng)
            } else {
                Scenario::Plain
            };
            runs += 1;
            let clean = !(faulty && (features.faults || features.disruptions));
            // Drawn only where the network is, so every other seed draws as it always has.
            let network = (faulty && features.faults && self.placing.is_some())
                .then(|| SplitMix64::new(rng.next_u64()));
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

    /// Runs `scenario` in `phase` for every pipeline at once, keeping the reports of runs that
    /// end, and checks their failures against the refusals the model predicts.
    ///
    /// A refusal an operator can relax is relaxed, and a `clean` run, with neither faults nor
    /// disruptions, fails with nothing else. With `network` faults, drawn from it, the network is
    /// disrupted while the runs last, and healed once they end.
    async fn attempt(
        &mut self,
        phase: usize,
        scenario: Scenario,
        clean: bool,
        network: Option<SplitMix64>,
        reports: &mut Vec<Report>,
    ) -> Ran {
        let seed = self.seed;
        let prediction = refusals::predict(&self.world, &self.relaxed, phase);
        let workload = &self.world.workload;
        let plans: Vec<PipelinePlan> = (0..workload.pipelines)
            .map(|pipeline| plan(workload, &self.relaxed, pipeline))
            .collect();
        let placing = self.placing.as_ref();
        let executing = execute_all(&self.engine, &plans, &self.name, placing, scenario);
        let executed = match (placing, network) {
            (Some(placing), Some(rng)) => placing.disrupting(rng, executing).await,
            _ => executing.await,
        };
        let mut failures = Vec::new();
        for (plan, executed) in plans.iter().zip(&executed) {
            let refused = plan
                .streams()
                .iter()
                .find(|stream| prediction.must.contains(&stream.name().to_string()));
            assert!(
                !(executed.succeeded && refused.is_some()),
                "seed {seed}: phase {phase}: a run succeeded, though every run of {:?} must meet \
                 one of {:?}",
                refused.map(StreamPlan::name),
                prediction.may
            );
        }
        let succeeded = executed.iter().map(|executed| executed.succeeded).collect();
        for executed in executed {
            failures.extend(executed.failures);
            reports.extend(executed.reports);
        }
        kept_to_the_protocol(&failures, seed, phase);
        let refused = refusals::refused(&failures, &prediction)
            .unwrap_or_else(|finding| panic!("seed {seed}: phase {phase}: {finding}"));
        if let Some(failure) = refusals::unexplained(&failures, &prediction).filter(|_| clean) {
            panic!(
                "seed {seed}: phase {phase}: a run without faults failed with {}",
                failure.text
            );
        }
        let failure = failures.into_iter().last().map(|failure| failure.text);
        let mut stopped = false;
        if let Some((stream, code)) = refused {
            if code == refusals::KEY_CHANGED {
                stopped = true;
            } else {
                refusals::relax(&self.world, &mut self.relaxed, &stream, code);
            }
        }
        Ran {
            succeeded,
            failure,
            stopped,
        }
    }
}

/// Checks that none of `failures` breaks the wire protocol, which no fault excuses.
fn kept_to_the_protocol(failures: &[refusals::Failure], seed: Seed, phase: usize) {
    if let Some(broken) = refusals::violation(failures) {
        panic!(
            "seed {seed}: phase {phase}: a run broke the wire protocol: {}",
            broken.text
        );
    }
}

/// How one scenario's runs went.
struct Ran {
    /// Whether one succeeded, for each pipeline.
    succeeded: Vec<bool>,
    /// The last failure among them, printed.
    failure: Option<String>,
    /// Whether one met a refusal no operator can relax.
    stopped: bool,
}

fn config(rng: &mut SplitMix64) -> EngineConfig {
    let every = rng
        .chance(700)
        .then(|| Duration::from_millis(100 + rng.below(3000)));
    let rows = (every.is_none() || rng.chance(500)).then(|| 5 + rng.below(60));
    let commit = CommitPolicy::new(every, rows, None).expect("the drawn policy has a threshold");
    let retry = RetryPolicy::default()
        .max_attempts(4)
        .initial(Duration::from_millis(1))
        .max_delay(Duration::from_millis(100));
    let lanes = u16::try_from(1 + rng.below(3)).unwrap_or(1);
    EngineConfig::builder()
        .memory(512 + rng.below(8192))
        .lanes(lanes)
        .lane_window(to_usize(1 + rng.below(3)))
        .partitions(to_usize(1 + rng.below(4)))
        .partition_buffer(to_usize(1 + rng.below(4)))
        .barrier_wait(Duration::from_millis(10 + rng.below(2000)))
        .commit(commit)
        .retry(retry)
        .build()
        .expect("the drawn configuration is valid")
}

fn to_usize(value: u64) -> usize {
    usize::try_from(value).unwrap_or(1)
}

/// The identifiers of the pipelines sharing the destination.
const PIPELINES: [&str; 2] = ["sim", "sim-b"];

/// The plan of `workload`'s pipeline `pipeline`, of the streams it loads, each stream's settings
/// as `relaxed` leaves them.
fn plan(workload: &Workload, relaxed: &[Relaxed], pipeline: usize) -> PipelinePlan {
    let id = PipelineId::parse(PIPELINES[pipeline]).expect("valid pipeline id");
    let streams = workload
        .streams
        .iter()
        .zip(relaxed)
        .enumerate()
        .filter(|(index, _)| index % workload.pipelines == pipeline)
        .map(|(_, (stream, relaxed))| {
            let name = StreamName::new(&stream.name).expect("valid stream name");
            let settings = stream.schema.relaxing(stream.pipeline, *relaxed);
            let mut plan = StreamPlan::new(name)
                .read(stream.read)
                .write(stream.write)
                .schema(settings.engine());
            for drift in &stream.drift {
                let column = ColumnPath::from(drift.name.as_str());
                if drift.settings != Level::default() {
                    plan = plan.column(column.clone(), drift.settings.relaxed(*relaxed).engine());
                }
                if let Some(hint) = &drift.hint {
                    plan = plan.hint(column, hint.clone());
                }
            }
            if stream.keys > 0 && stream.plan_key {
                plan.key(stream.key_columns())
            } else {
                plan
            }
        });
    PipelinePlan::new(id, streams)
        .expect("the simulated plan is valid")
        .schema(workload.pipeline.engine())
        .with_wal(workload.features.wal)
}

/// Lets aborted tasks finish, then checks that no task outlived its run.
async fn settle(seed: Seed) {
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    let alive = tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks();
    assert_eq!(alive, 0, "seed {seed}: {alive} tasks outlived their runs");
}

/// Checks that every offset acknowledged to a stream that cannot read again landed: it was
/// heard before its commit, so only the write-ahead log could see it through a failure.
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
