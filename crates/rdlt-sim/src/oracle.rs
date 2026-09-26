//! The exactly-once oracle: a seeded workload runs through faults, retries, crashes, stops and
//! concurrent runs, and the destination must end up holding exactly what a reference model says.

mod arrivals;
mod expected;
mod names;
mod refusals;
mod rows;
mod tables;

use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::{
    ColumnPath, ConnectContext, PipelineId, ReadMode, StreamName, destination_factory,
    source_factory,
};
use rdlt_engine::{
    CommitPolicy, Engine, EngineConfig, PipelinePlan, Report, RetryPolicy, RunHandle, RunStatus,
    StopMode, StreamPlan,
};
use serde_json::json;

use crate::destination::{SimDestination, completions, reads_in_progress};
use crate::env::SimEnv;
use crate::rng::SplitMix64;
use crate::seed::{Seed, run};
use crate::source::SimSource;
use crate::workload::{Level, PHASES, Relaxed, Row, Workload};
use crate::world::World;
use expected::Discards;
use refusals::Failure;

/// Runs before this many have faults injected; the rest run clean, so every phase converges.
const FAULTY_RUNS: usize = 4;

/// The longest a single run may take in virtual time before the oracle calls it hung.
const RUN_LIMIT: Duration = Duration::from_secs(3600);

/// How one run of a phase goes.
#[derive(Clone, Copy, Debug)]
enum Scenario {
    /// The run proceeds undisturbed.
    Plain,
    /// The run is dropped after the given time, as if its worker crashed.
    Crash(Duration),
    /// The run is asked to stop after committing, after the given time.
    Stop(Duration),
    /// A second run of the same pipeline starts after the given time.
    Concurrent(Duration),
}

/// Checks the exactly-once guarantee for the workload `seed` generates.
///
/// # Panics
///
/// Panics, naming the seed, when the destination's contents differ from the reference model, an
/// invariant breaks, a run hangs, or a task outlives its run.
pub fn check_exactly_once(seed: Seed) {
    run(seed, |env| async move { simulate(seed, env).await });
}

async fn simulate(seed: Seed, env: Arc<SimEnv>) {
    let mut rng = SplitMix64::new(seed.value());
    let name = format!("oracle-{seed}");
    let world = World::register(&name, &mut rng);
    let mut simulation = Simulation {
        seed,
        engine: Engine::new(config(&mut rng), env),
        relaxed: vec![Relaxed::default(); world.workload.streams.len()],
        world,
        name,
    };
    for phase in 0..PHASES {
        let (reports, stopped) = simulation.converge(phase, &mut rng).await;
        settle(seed).await;
        let world = &simulation.world;
        check_contents(world, phase, stopped, seed);
        check_discards(world, phase, &reports, stopped, seed);
        if stopped {
            break;
        }
    }
    let violations = simulation.world.violations();
    World::unregister(&simulation.name);
    assert!(violations.is_empty(), "seed {seed}: {violations:#?}");
}

/// One simulation: its world, the engine its runs share, and what an operator relaxed after
/// refusals.
struct Simulation {
    seed: Seed,
    name: String,
    world: Arc<World>,
    engine: Engine,
    relaxed: Vec<Relaxed>,
}

impl Simulation {
    /// Runs `phase` until it converges, or a refusal no operator can relax stops it short; the
    /// reports of its runs that ended, and whether it stopped.
    ///
    /// A phase converges once a run has succeeded and no full read is left half done, so each
    /// full read the model counts is complete.
    async fn converge(&mut self, phase: usize, rng: &mut SplitMix64) -> (Vec<Report>, bool) {
        let (seed, features) = (self.seed, self.world.workload.features);
        self.world.set_phase(phase);
        let (mut runs, mut succeeded, mut failure) = (0, false, None);
        let mut reports = Vec::new();
        while !succeeded || reads_in_progress(&self.world) {
            let faulty = runs < FAULTY_RUNS;
            self.world.set_faulty(faulty && features.faults);
            let scenario = if faulty && features.disruptions {
                pick(rng)
            } else {
                Scenario::Plain
            };
            runs += 1;
            let ran = self.attempt(phase, scenario, &mut reports).await;
            succeeded |= ran.succeeded;
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

    /// Runs `scenario` in `phase`, keeping the reports of runs that end, and checks its failures
    /// against the refusals the model predicts: a refusal an operator can relax is relaxed.
    async fn attempt(
        &mut self,
        phase: usize,
        scenario: Scenario,
        reports: &mut Vec<Report>,
    ) -> Ran {
        let seed = self.seed;
        let prediction = refusals::predict(&self.world, &self.relaxed, phase);
        let plan = plan(&self.world.workload, &self.relaxed);
        let (succeeded, failures) =
            execute(&self.engine, &plan, &self.name, scenario, reports).await;
        assert!(
            !(succeeded && prediction.must),
            "seed {seed}: phase {phase}: a run succeeded, though every run must meet one of {:?}",
            prediction.may
        );
        let refused = refusals::refused(&failures, &prediction)
            .unwrap_or_else(|finding| panic!("seed {seed}: phase {phase}: {finding}"));
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

/// How one scenario's runs went.
struct Ran {
    /// Whether one succeeded.
    succeeded: bool,
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

/// The plan of `workload`'s pipeline, each stream's settings as `relaxed` leaves them.
fn plan(workload: &Workload, relaxed: &[Relaxed]) -> PipelinePlan {
    let pipeline = PipelineId::parse("sim").expect("valid pipeline id");
    let streams = workload
        .streams
        .iter()
        .zip(relaxed)
        .map(|(stream, relaxed)| {
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
    PipelinePlan::new(pipeline, streams)
        .expect("the simulated plan is valid")
        .schema(workload.pipeline.engine())
}

fn pick(rng: &mut SplitMix64) -> Scenario {
    let after = Duration::from_millis(rng.below(2000));
    match rng.below(4) {
        0 => Scenario::Plain,
        1 => Scenario::Crash(after),
        2 => Scenario::Stop(after),
        _ => Scenario::Concurrent(after / 2),
    }
}

async fn start(engine: &Engine, plan: &PipelinePlan, world: &str) -> RunHandle {
    let config = json!({ "world": world });
    let source = source_factory::<SimSource>()
        .connect(config.clone(), ConnectContext::new())
        .await
        .expect("the simulated source connects");
    let destination = destination_factory::<SimDestination>()
        .connect(config, ConnectContext::new())
        .await
        .expect("the simulated destination connects");
    engine.run(plan.clone(), Arc::from(source), Arc::from(destination))
}

/// Runs `scenario`, keeping the report of each run that ends; whether some run succeeded, and the
/// failures of runs that failed.
async fn execute(
    engine: &Engine,
    plan: &PipelinePlan,
    world: &str,
    scenario: Scenario,
    reports: &mut Vec<Report>,
) -> (bool, Vec<Failure>) {
    let (ended, dropped) = match scenario {
        Scenario::Plain => (vec![bounded(start(engine, plan, world).await).await], false),
        Scenario::Crash(after) => {
            let run = bounded(start(engine, plan, world).await);
            tokio::select! {
                biased;
                ended = run => (vec![ended], false),
                // Dropping the run is the crash.
                () = tokio::time::sleep(after) => (Vec::new(), true),
            }
        }
        Scenario::Stop(after) => {
            let handle = start(engine, plan, world).await;
            let control = handle.control();
            let stop = async {
                tokio::time::sleep(after).await;
                control.stop(StopMode::AfterCommit);
            };
            let (ended, ()) = tokio::join!(bounded(handle), stop);
            (vec![ended], false)
        }
        Scenario::Concurrent(delay) => {
            let first = bounded(start(engine, plan, world).await);
            let second = async {
                tokio::time::sleep(delay).await;
                bounded(start(engine, plan, world).await).await
            };
            let (first, second) = tokio::join!(first, second);
            (vec![first, second], false)
        }
    };
    let succeeded = ended
        .iter()
        .any(|(report, _)| report.status == RunStatus::Succeeded);
    let mut failures = Vec::new();
    for (report, failure) in ended {
        reports.push(report);
        failures.extend(failure);
    }
    (succeeded && !dropped, failures)
}

/// Awaits `run`, panicking if it takes longer than [`RUN_LIMIT`]; its report, and its failure.
async fn bounded(run: RunHandle) -> (Report, Option<Failure>) {
    let outcome = tokio::time::timeout(RUN_LIMIT, run)
        .await
        .expect("every run ends within the limit of virtual time");
    let failure = outcome.error.as_ref().map(Failure::of);
    (outcome.report, failure)
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
