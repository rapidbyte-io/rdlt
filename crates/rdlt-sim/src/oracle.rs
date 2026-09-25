//! The exactly-once oracle: a seeded workload runs through faults, retries, crashes, stops and
//! concurrent runs, and the destination must end up holding exactly what a reference model says.

mod expected;
mod names;
mod tables;

use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::{
    ConnectContext, PipelineId, ReadMode, StreamName, destination_factory, source_factory,
};
use rdlt_engine::{
    CommitPolicy, Engine, EngineConfig, PipelinePlan, Report, RetryPolicy, RunHandle, RunStatus,
    SchemaPolicy, SchemaSettings, StopMode, StreamPlan, WriteMode,
};
use serde_json::json;

use crate::destination::{SimDestination, completions, reads_in_progress};
use crate::env::SimEnv;
use crate::rng::SplitMix64;
use crate::seed::{Seed, run};
use crate::source::SimSource;
use crate::workload::{PHASES, Row, SimStream};
use crate::world::World;

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
    let engine = Engine::new(config(&mut rng), env);
    let plan = plan(&world.workload.streams);
    let features = world.workload.features;
    for phase in 0..PHASES {
        world.set_phase(phase);
        let mut runs = 0;
        let mut succeeded = false;
        let mut failure = None;
        let mut reports = Vec::new();
        // A phase ends once a run has succeeded and no full read is left half done, so each full
        // read the model counts is complete.
        while !succeeded || reads_in_progress(&world) {
            let faulty = runs < FAULTY_RUNS;
            world.set_faulty(faulty && features.faults);
            let scenario = if faulty && features.disruptions {
                pick(&mut rng)
            } else {
                Scenario::Plain
            };
            runs += 1;
            let (success, error) = execute(&engine, &plan, &name, scenario, &mut reports).await;
            succeeded |= success;
            failure = error.or(failure);
            assert!(
                runs < 32,
                "seed {seed}: phase {phase} did not converge; the last error was {failure:?}"
            );
        }
        settle(seed).await;
        check_contents(&world, phase, seed);
        check_discards(&world, phase, &reports, features.reports_complete(), seed);
    }
    let violations = world.violations();
    World::unregister(&name);
    assert!(violations.is_empty(), "seed {seed}: {violations:#?}");
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

fn plan(streams: &[SimStream]) -> PipelinePlan {
    let pipeline = PipelineId::parse("sim").expect("valid pipeline id");
    let streams = streams.iter().map(|stream| {
        let name = StreamName::new(&stream.name).expect("valid stream name");
        let settings = SchemaSettings::new()
            .policy(stream.policy)
            .nested(stream.nested);
        let plan = StreamPlan::new(name)
            .read(stream.read)
            .write(stream.write)
            .schema(settings);
        if stream.keys > 0 && stream.plan_key {
            plan.key(["key"])
        } else {
            plan
        }
    });
    PipelinePlan::new(pipeline, streams).expect("the simulated plan is valid")
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
/// error of a run that failed.
async fn execute(
    engine: &Engine,
    plan: &PipelinePlan,
    world: &str,
    scenario: Scenario,
    reports: &mut Vec<Report>,
) -> (bool, Option<String>) {
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
    let error = ended.iter().find_map(|(_, error)| error.clone());
    reports.extend(ended.into_iter().map(|(report, _)| report));
    (succeeded && !dropped, error)
}

/// Awaits `run`, panicking if it takes longer than [`RUN_LIMIT`]; its report, and its error.
async fn bounded(run: RunHandle) -> (Report, Option<String>) {
    let outcome = tokio::time::timeout(RUN_LIMIT, run)
        .await
        .expect("every run ends within the limit of virtual time");
    let error = outcome.error.map(|error| format!("{error:?}"));
    (outcome.report, error)
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

/// Checks every stream's tables against the reference model after `phase`.
fn check_contents(world: &World, phase: usize, seed: Seed) {
    for stream in &world.workload.streams {
        let delivered: Vec<Row> = (0..=phase).flat_map(|done| stream.all_rows(done)).collect();
        tables::check(
            world,
            stream,
            &kept_rows(world, stream, phase, seed),
            &delivered,
            seed,
        );
    }
}

/// Checks the rows and values each stream's policy discarded in `phase`, as its runs' `reports`
/// count them, against those the model's policy discards: exactly where the reports are
/// `complete`, and otherwise never more, as a dropped run's report or a commit whose response was
/// lost goes uncounted.
fn check_discards(world: &World, phase: usize, reports: &[Report], complete: bool, seed: Seed) {
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
                let copies = completions(world, &stream.name, phase);
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
        let expected = read.iter().fold((0, 0), |(rows, values), row| {
            let held = expected::drift_values(stream, row);
            match stream.policy {
                SchemaPolicy::DiscardRow => (rows + u64::from(held > 0), values),
                SchemaPolicy::DiscardValue => (rows, values + held),
                _ => (rows, values),
            }
        });
        let fits = if complete {
            counted == expected
        } else {
            counted.0 <= expected.0 && counted.1 <= expected.1
        };
        assert!(
            fits,
            "seed {seed}: stream {} ({:?}, {:?}, {:?}) in phase {phase} counted {counted:?} \
             (rows, values) discarded; the model counts {expected:?}, which reports that are \
             {} must {}",
            stream.name,
            stream.read,
            stream.write,
            stream.policy,
            if complete { "complete" } else { "incomplete" },
            if complete { "equal" } else { "not exceed" },
        );
    }
}

/// The rows the reference model says a stream that does not merge loaded by `phase`, each as
/// often as its table holds it.
fn expected_rows(world: &World, stream: &SimStream, phase: usize, seed: Seed) -> Vec<Row> {
    match (stream.read, stream.write) {
        (ReadMode::Full, WriteMode::Append) => (0..=phase)
            .flat_map(|done| {
                let copies = completions(world, &stream.name, done);
                assert!(
                    copies > 0 || done < phase,
                    "seed {seed}: stream {} never completed",
                    stream.name
                );
                std::iter::repeat_n(stream.all_rows(done), copies).flatten()
            })
            .collect(),
        _ => stream.all_rows(phase),
    }
}

/// The rows `stream`'s table holds after `phase` as the model has them: for a merge stream, each
/// key's last row the policy keeps; otherwise each row the policy keeps, as often as the table
/// holds it.
fn kept_rows(world: &World, stream: &SimStream, phase: usize, seed: Seed) -> Vec<Row> {
    if stream.write == WriteMode::Merge {
        return merged_rows(stream, phase);
    }
    expected_rows(world, stream, phase, seed)
        .into_iter()
        .filter(|row| expected::kept(stream, row))
        .collect()
}

/// For each key of a merge stream, the last row delivered by `phase` that the policy keeps.
fn merged_rows(stream: &SimStream, phase: usize) -> Vec<Row> {
    let mut rows: std::collections::BTreeMap<i64, Row> = std::collections::BTreeMap::new();
    for delivered in 0..=phase {
        for row in stream.all_rows(delivered) {
            if row.delivered != delivered || !expected::kept(stream, &row) {
                continue;
            }
            if let Some(key) = row.key {
                rows.insert(key, row);
            }
        }
    }
    rows.into_values().collect()
}
