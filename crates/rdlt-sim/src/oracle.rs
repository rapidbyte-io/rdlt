//! The exactly-once oracle: a seeded workload runs through faults, retries, crashes, stops and
//! concurrent runs, and the destination must end up holding exactly what a reference model says.

use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::{
    ConnectContext, PipelineId, ReadMode, StreamName, destination_factory, source_factory,
};
use rdlt_engine::{
    CommitPolicy, Engine, EngineConfig, PipelinePlan, RetryPolicy, RunHandle, RunStatus, StopMode,
    StreamPlan, WriteMode,
};
use serde_json::json;

use crate::destination::{SimDestination, completions, published, reads_in_progress};
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
    for phase in 0..PHASES {
        world.set_phase(phase);
        let mut runs = 0;
        let mut succeeded = false;
        // A phase ends once a run has succeeded and no full read is left half done, so each full
        // read the model counts is complete.
        while !succeeded || reads_in_progress(&world) {
            let faulty = runs < FAULTY_RUNS;
            world.set_faulty(faulty);
            let scenario = if faulty {
                pick(&mut rng)
            } else {
                Scenario::Plain
            };
            runs += 1;
            succeeded |= execute(&engine, &plan, &name, scenario).await;
            assert!(runs < 32, "seed {seed}: phase {phase} did not converge");
        }
        settle(seed).await;
        check_contents(&world, phase, seed);
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
        StreamPlan::new(name).read(stream.read).write(stream.write)
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

/// Runs `scenario`; whether some run succeeded.
async fn execute(engine: &Engine, plan: &PipelinePlan, world: &str, scenario: Scenario) -> bool {
    let succeeded = |status: RunStatus| status == RunStatus::Succeeded;
    match scenario {
        Scenario::Plain => succeeded(bounded(start(engine, plan, world).await).await),
        Scenario::Crash(after) => {
            let run = bounded(start(engine, plan, world).await);
            tokio::select! {
                biased;
                status = run => succeeded(status),
                // Dropping the run is the crash.
                () = tokio::time::sleep(after) => false,
            }
        }
        Scenario::Stop(after) => {
            let handle = start(engine, plan, world).await;
            let control = handle.control();
            let stop = async {
                tokio::time::sleep(after).await;
                control.stop(StopMode::AfterCommit);
            };
            let (status, ()) = tokio::join!(bounded(handle), stop);
            succeeded(status)
        }
        Scenario::Concurrent(delay) => {
            let first = bounded(start(engine, plan, world).await);
            let second = async {
                tokio::time::sleep(delay).await;
                bounded(start(engine, plan, world).await).await
            };
            let (first, second) = tokio::join!(first, second);
            succeeded(first) || succeeded(second)
        }
    }
}

/// Awaits `run`, panicking if it takes longer than [`RUN_LIMIT`].
async fn bounded(run: RunHandle) -> RunStatus {
    let outcome = tokio::time::timeout(RUN_LIMIT, run)
        .await
        .expect("every run ends within the limit of virtual time");
    outcome.report.status
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

/// Checks every stream's table against the reference model after `phase`.
fn check_contents(world: &World, phase: usize, seed: Seed) {
    let salt = world.workload.salt;
    for stream in &world.workload.streams {
        let mut expected: Vec<Row> = match (stream.read, stream.write) {
            (ReadMode::Full, WriteMode::Append) => (0..=phase)
                .flat_map(|done| {
                    let copies = completions(world, &stream.name, done);
                    assert!(
                        copies > 0 || done < phase,
                        "seed {seed}: stream {} never completed",
                        stream.name
                    );
                    std::iter::repeat_n(stream.all_rows(salt, done), copies).flatten()
                })
                .collect(),
            _ => stream.all_rows(salt, phase),
        };
        let mut actual = published(world, &stream.name);
        expected.sort_unstable();
        actual.sort_unstable();
        assert!(
            actual == expected,
            "seed {seed}: stream {} ({:?}, {:?}) after phase {phase} holds {} rows; the model \
             expects {}",
            stream.name,
            stream.read,
            stream.write,
            actual.len(),
            expected.len()
        );
    }
}
