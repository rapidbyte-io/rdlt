//! The exactly-once oracle: a seeded workload runs through faults, retries, crashes, stops and
//! concurrent runs, and the destination must end up holding exactly what a reference model says.

use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::{
    ConnectContext, PipelineId, ReadMode, StreamName, destination_factory, source_factory,
};
use rdlt_engine::{
    CommitPolicy, Engine, EngineConfig, PipelinePlan, RetryPolicy, RunHandle, RunStatus,
    SchemaPolicy, SchemaSettings, StopMode, StreamPlan, WriteMode,
};
use serde_json::{Map, Value, json};

use crate::destination::{SimDestination, canonical, completions, published, reads_in_progress};
use crate::env::SimEnv;
use crate::rng::SplitMix64;
use crate::seed::{Seed, run};
use crate::source::SimSource;
use crate::workload::{Extra, PHASES, Row, SimStream};
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
        let mut failure = None;
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
            let (success, error) = execute(&engine, &plan, &name, scenario).await;
            succeeded |= success;
            failure = error.or(failure);
            assert!(
                runs < 32,
                "seed {seed}: phase {phase} did not converge; the last error was {failure:?}"
            );
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

/// Runs `scenario`; whether some run succeeded, and the error of a run that failed.
async fn execute(
    engine: &Engine,
    plan: &PipelinePlan,
    world: &str,
    scenario: Scenario,
) -> (bool, Option<String>) {
    match scenario {
        Scenario::Plain => bounded(start(engine, plan, world).await).await,
        Scenario::Crash(after) => {
            let run = bounded(start(engine, plan, world).await);
            tokio::select! {
                biased;
                ended = run => ended,
                // Dropping the run is the crash.
                () = tokio::time::sleep(after) => (false, None),
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
            ended
        }
        Scenario::Concurrent(delay) => {
            let first = bounded(start(engine, plan, world).await);
            let second = async {
                tokio::time::sleep(delay).await;
                bounded(start(engine, plan, world).await).await
            };
            let (first, second) = tokio::join!(first, second);
            (first.0 || second.0, first.1.or(second.1))
        }
    }
}

/// Awaits `run`, panicking if it takes longer than [`RUN_LIMIT`]; whether it succeeded, and its
/// error.
async fn bounded(run: RunHandle) -> (bool, Option<String>) {
    let outcome = tokio::time::timeout(RUN_LIMIT, run)
        .await
        .expect("every run ends within the limit of virtual time");
    let error = outcome.error.map(|error| format!("{error:?}"));
    (outcome.report.status == RunStatus::Succeeded, error)
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
    for stream in &world.workload.streams {
        let mut expected = expected(world, stream, phase, seed);
        let mut actual = published(world, &stream.name);
        let order = |row: &Map<String, Value>| Value::Object(row.clone()).to_string();
        expected.sort_by_key(order);
        actual.sort_by_key(order);
        if actual != expected {
            let first = actual
                .iter()
                .zip(&expected)
                .find(|(actual, expected)| actual != expected);
            panic!(
                "seed {seed}: stream {} ({:?}, {:?}, {:?}) after phase {phase} holds {} rows; the \
                 model expects {}; first difference {first:?}",
                stream.name,
                stream.read,
                stream.write,
                stream.policy,
                actual.len(),
                expected.len()
            );
        }
    }
}

/// What the reference model says `stream`'s table holds after `phase`, as source rows.
fn expected(
    world: &World,
    stream: &SimStream,
    phase: usize,
    seed: Seed,
) -> Vec<Map<String, Value>> {
    let salt = world.workload.salt;
    let rows: Vec<Row> = match (stream.read, stream.write) {
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
        (_, WriteMode::Merge) => return merged(stream, salt, phase),
        _ => stream.all_rows(salt, phase),
    };
    rows.iter()
        .filter_map(|row| source_row(stream, row))
        .collect()
}

/// The rows a merge stream's table holds after `phase`: for each key, the last row delivered.
fn merged(stream: &SimStream, salt: u64, phase: usize) -> Vec<Map<String, Value>> {
    let mut rows: std::collections::BTreeMap<i64, Map<String, Value>> =
        std::collections::BTreeMap::new();
    for delivered in 0..=phase {
        for row in stream.all_rows(salt, delivered) {
            if row.delivered != delivered {
                continue;
            }
            if let (Some(key), Some(source)) = (row.key, source_row(stream, &row)) {
                rows.insert(key, source);
            }
        }
    }
    rows.into_values().collect()
}

/// `row` as the source sent it, once the stream's policy has discarded what it discards; `None`
/// when the policy drops the row.
fn source_row(stream: &SimStream, row: &Row) -> Option<Map<String, Value>> {
    let mut source = Map::new();
    source.insert("id".to_owned(), json!(row.id));
    source.insert("partition".to_owned(), json!(row.partition));
    source.insert("offset".to_owned(), json!(row.offset));
    source.insert("value".to_owned(), json!(row.value));
    if let Some(key) = row.key {
        source.insert("key".to_owned(), json!(key));
    }
    let extras = stream
        .drift
        .iter()
        .zip(&row.extras)
        .filter_map(|(drift, extra)| Some((drift.name.clone(), extra.as_ref()?)));
    for (name, extra) in extras {
        match stream.policy {
            SchemaPolicy::DiscardRow => return None,
            SchemaPolicy::DiscardValue => continue,
            _ => {}
        }
        let value = match extra {
            Extra::Int(value) => json!(value),
            Extra::Quarters(quarters) => {
                json!(f64::from(i32::try_from(*quarters).unwrap_or(0)) / 4.0)
            }
            Extra::Text(text) => json!(text),
            Extra::Object(n) => json!({ "n": n }),
            Extra::List(items) => json!(items),
        };
        source.insert(name, canonical(value));
    }
    Some(source)
}
