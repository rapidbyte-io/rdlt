//! The change streams' oracle: runs a change workload through faults, crashes, stops and racing
//! runs, round after round, and checks each table against the model: a merged table holds each
//! key as the changes leave it, and a log holds every change exactly once.

use std::collections::BTreeMap;
use std::sync::Arc;

use rdlt_connector::{PipelineId, ReadMode, StreamName};
use rdlt_engine::{DeleteMode, Engine, PipelinePlan, StreamPlan, WriteMode};
use rdlt_testkit::canon::Canon;

use super::scenario::{Scenario, execute_all, pick};
use super::{FAULTY_RUNS, config, settle};
use crate::changes::{ChangeStream, Logged, Merged, ROUNDS};
use crate::destination::{Digest, Stored};
use crate::env::SimEnv;
use crate::rng::SplitMix64;
use crate::seed::{Seed, run};
use crate::world::World;

/// What the change oracle mixes into the seed, so its draws differ from the exactly-once
/// oracle's: "changes" in ASCII.
const CHANGES: u64 = 0x0063_6861_6e67_6573;

/// Checks that every change of the workload `seed` generates lands as the model says, through
/// faults, crashes, stops and racing runs; the destination's digest.
///
/// # Panics
///
/// Panics, naming the seed, when a table differs from the model, a run without faults fails, the
/// runs do not converge, or a connector saw a broken invariant.
pub fn check_changes(seed: Seed) -> Digest {
    run(seed, |env| async move { simulate(seed, env).await })
}

async fn simulate(seed: Seed, env: Arc<SimEnv>) -> Digest {
    let mut rng = SplitMix64::new(seed.value() ^ CHANGES);
    let name = format!("changes-{seed}");
    let world = World::register_changes(&name, &mut rng);
    let features = world.workload.features;
    env.perturb(features.perturb);
    let engine = Engine::new(config(&mut rng), env);
    let plan = plan(&world.changes.streams);
    for round in 0..ROUNDS {
        world.set_phase(round);
        converge(&engine, &plan, &world, &name, round, seed, &mut rng).await;
        settle(seed).await;
        for stream in &world.changes.streams {
            check(&world, stream, round, seed);
        }
    }
    let violations = world.violations();
    let digest = world.store.lock().digest();
    World::unregister(&name);
    assert!(violations.is_empty(), "seed {seed}: {violations:#?}");
    digest
}

/// The pipeline of every change stream, each written as the workload says.
fn plan(streams: &[ChangeStream]) -> PipelinePlan {
    let streams = streams.iter().map(|stream| {
        let plan = StreamPlan::new(StreamName::new(&stream.name).expect("a valid stream name"))
            .read(ReadMode::Cdc)
            .write(stream.write);
        if stream.write == WriteMode::Merge {
            plan.deletes(stream.deletes).on_truncate(stream.truncates)
        } else {
            plan
        }
    });
    PipelinePlan::new(PipelineId::parse("changes").expect("a valid id"), streams)
        .expect("the change plan is valid")
}

/// Runs `round` until a run succeeds: the first few runs with faults and disruptions where the
/// seed has them, the rest without.
async fn converge(
    engine: &Engine,
    plan: &PipelinePlan,
    world: &World,
    name: &str,
    round: usize,
    seed: Seed,
    rng: &mut SplitMix64,
) {
    let features = world.workload.features;
    for runs in 0.. {
        let faulty = runs < FAULTY_RUNS;
        world.set_faulty(faulty && features.faults);
        let scenario = if faulty && features.disruptions {
            pick(rng)
        } else {
            Scenario::Plain
        };
        let executed = execute_all(engine, std::slice::from_ref(plan), name, None, scenario).await;
        let clean = !(faulty && (features.faults || features.disruptions));
        for executed in &executed {
            if let (true, Some(failure)) = (clean, executed.failures.first()) {
                panic!(
                    "seed {seed}: round {round}: a run without faults failed with {}",
                    failure.text
                );
            }
        }
        if executed.iter().all(|executed| executed.succeeded) {
            return;
        }
        assert!(runs < 32, "seed {seed}: round {round} did not converge");
    }
}

/// Checks `stream`'s table after `round` against the model.
fn check(world: &World, stream: &ChangeStream, round: usize, seed: Seed) {
    let store = world.store.lock();
    let rows: Vec<Stored> = store.published(&stream.name);
    drop(store);
    let context = || format!("seed {seed}: round {round}: stream {}", stream.name);
    if stream.write == WriteMode::Append {
        let mut log: Vec<Logged> = rows.iter().map(logged).collect();
        log.sort();
        assert_eq!(log, stream.log(round), "{}: the log", context());
        return;
    }
    let soft = stream.deletes == DeleteMode::Soft;
    let mut table: BTreeMap<i64, Merged> = BTreeMap::new();
    for row in &rows {
        let key = number(row, "id").unwrap_or_else(|| panic!("{}: a row without a key", context()));
        let merged = Merged {
            value: text(row, "value"),
            n: number(row, "n").unwrap_or_default(),
            deleted: soft && !matches!(row.cells.get("_rdlt_deleted_at"), None | Some(Canon::Null)),
        };
        let previous = table.insert(key, merged);
        assert!(
            previous.is_none(),
            "{}: key {key} is published twice",
            context()
        );
    }
    assert_eq!(
        table,
        stream.merged(round),
        "{}: the merged table",
        context()
    );
}

/// A row of a log: its op, key, value and counter.
fn logged(row: &Stored) -> Logged {
    let op = number(row, "_rdlt_op").and_then(|op| i8::try_from(op).ok());
    (
        op.unwrap_or(-1),
        number(row, "id"),
        text(row, "value"),
        number(row, "n"),
    )
}

fn number(row: &Stored, column: &str) -> Option<i64> {
    match row.cells.get(column)? {
        Canon::Number(text) | Canon::Text(text) => text.parse().ok(),
        _ => None,
    }
}

fn text(row: &Stored, column: &str) -> Option<String> {
    match row.cells.get(column)? {
        Canon::Text(text) => Some(text.clone()),
        _ => None,
    }
}
