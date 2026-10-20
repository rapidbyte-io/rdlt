//! The change streams' oracle: runs a change workload through faults, crashes, stops and racing
//! runs, round after round, and checks each table against the model: a merged table holds each
//! key as the changes leave it, and a log holds every change exactly once.

use std::collections::BTreeMap;
use std::sync::Arc;

use rdlt_connector::{PartitionState, PipelineId, ReadMode, StateEntry, StreamName};
use rdlt_engine::{DeleteMode, Engine, Env as _, PipelinePlan, StreamPlan, WriteMode};
use rdlt_testkit::canon::Canon;

use super::Checked;
use super::config::{config, growth, waited};
use super::ledger::Ledger;
use super::reports::Reported;
use super::scenario::{Scenario, execute_all, pick};
use super::{FAULTY_RUNS, settled};
use crate::canary::Canary;
use crate::changes::{
    CHANGES_PARTITION, CHANGES_PHASE, ChangeStream, Logged, Merged, Position, ROUNDS, Version,
};
use crate::destination::Stored;
use crate::env::SimEnv;
use crate::rng::SplitMix64;
use crate::seed::{Seed, run};
use crate::world::World;

/// What the change oracle mixes into the seed, so its draws differ from the exactly-once
/// oracle's: "changes" in ASCII.
const CHANGES: u64 = 0x0063_6861_6e67_6573;

/// What the change oracle mixes into the seed to draw which merge streams keep history, apart
/// from every other draw: "history" in ASCII.
const HISTORY: u64 = 0x0068_6973_746f_7279;

/// Checks that every change of the workload `seed` generates lands as the model says, through
/// faults, crashes, stops and racing runs; what the simulation left.
///
/// # Panics
///
/// Panics, naming the seed, when a table differs from the model, a run without faults fails, the
/// runs do not converge, or a connector saw a broken invariant.
pub fn check_changes(seed: Seed) -> Checked {
    run(
        seed,
        |env| async move { Box::pin(simulate(seed, env)).await },
    )
}

async fn simulate(seed: Seed, env: Arc<SimEnv>) -> Checked {
    let clock = Arc::clone(&env);
    let started = clock.instant();
    let mut rng = SplitMix64::new(seed.value() ^ CHANGES);
    let name = format!("changes-{seed}");
    let canary = Canary::of(&name);
    // Kept until the checks: every event logged on this thread until then is looked at.
    let (logged, logging) = canary.watching();
    let registered = World::register_changes(
        &name,
        &mut rng,
        &mut SplitMix64::new(seed.value() ^ HISTORY),
    );
    let world = Arc::clone(registered.world());
    let features = world.workload.features;
    env.perturb(features.perturb);
    world.keep_logs(&env, seed).await;
    let engine = Engine::new(
        config(
            &mut rng,
            false,
            0,
            |partitions| growth(seed, None, partitions),
            waited(seed, features.faults),
        ),
        env,
    );
    let plan = plan(&world.changes.streams).with_wal(features.wal);
    let mut changing = Changing {
        engine: &engine,
        plan: &plan,
        world: &world,
        name: &name,
        seed,
        ledger: Ledger::default(),
    };
    changing.rounds(&mut rng).await;
    // What the destination holds once the workload is loaded; the checks that follow load
    // nothing more.
    let digest = world.store.lock().digest();
    changing.report().await;
    let mut violations = world.violations();
    drop(logging);
    violations.extend(logged.leaks());
    drop(registered);
    assert!(violations.is_empty(), "seed {seed}: {violations:#?}");
    Checked {
        digest,
        trace: changing.ledger.trace(),
        waits: changing.ledger.waits,
        simulated: clock.instant() - started,
        features,
        held: None,
    }
}

/// The change oracle's runs: what they load and where, and what they left.
struct Changing<'a> {
    engine: &'a Engine,
    plan: &'a PipelinePlan,
    world: &'a World,
    name: &'a str,
    seed: Seed,
    ledger: Ledger,
}

impl Changing<'_> {
    /// Runs every round to its end and checks each table against the model.
    async fn rounds(&mut self, rng: &mut SplitMix64) {
        let (seed, world) = (self.seed, self.world);
        for round in 0..ROUNDS {
            world.set_phase(round);
            self.converge(round, rng).await;
            settled(seed, world, None).await;
            for stream in &world.changes.streams {
                check(world, stream, round, seed);
            }
            check_acknowledged(world, round, seed);
        }
    }

    /// Checks what the runs reported.
    async fn report(&self) {
        let world = self.world;
        let reported = Reported {
            engine: self.engine,
            plans: vec![self.plan.clone()],
            world,
            name: self.name,
            placing: None,
            stands,
            repeats: |_, _| true,
            refusable: world
                .changes
                .streams
                .iter()
                .all(|stream| stream.replay.is_none()),
        };
        reported.check(self.seed).await;
    }

    /// Runs `round` until a run succeeds and no write-ahead log is left to replay: the first few
    /// runs with faults and disruptions where the seed has them, the rest without.
    async fn converge(&mut self, round: usize, rng: &mut SplitMix64) {
        let (seed, features) = (self.seed, self.world.workload.features);
        for runs in 0.. {
            let faulty = runs < FAULTY_RUNS;
            self.world.set_faulty(faulty && features.faults);
            let scenario = if faulty && features.disruptions {
                pick(rng)
            } else {
                Scenario::Plain
            };
            let plans = std::slice::from_ref(self.plan);
            let executed = execute_all(self.engine, plans, self.name, None, scenario).await;
            self.ledger.ended(round, scenario, &executed);
            let clean = !(faulty && (features.faults || features.disruptions));
            for executed in &executed {
                if let (true, Some(failure)) = (clean, executed.failures.first()) {
                    panic!(
                        "seed {seed}: round {round}: a run without faults failed with {}",
                        failure.text
                    );
                }
            }
            if executed.iter().all(|executed| executed.succeeded) && !self.world.holds_logs().await
            {
                return;
            }
            assert!(runs < 32, "seed {seed}: round {round} did not converge");
        }
    }
}

/// Where state holds `partition` of `stream` at a cursor.
fn stands(world: &World, stream: &str, partition: &str) -> Option<u64> {
    let entries = world.store.lock();
    let entries = entries
        .states()
        .flat_map(|records| records.values())
        .filter_map(|record| StateEntry::from_record(record).ok());
    let positions = entries.filter_map(|entry| match entry {
        StateEntry::Partition {
            stream: named,
            partition: id,
            state: PartitionState::Cursor(cursor),
            ..
        } if named.name() == stream && id.as_str() == partition => {
            cursor.decode::<Position>(1).ok()
        }
        _ => None,
    });
    positions.map(|position| position.next).max()
}

/// The pipeline of every change stream, each written as the workload says.
fn plan(streams: &[ChangeStream]) -> PipelinePlan {
    let streams = streams.iter().map(|stream| {
        let write = if stream.history {
            WriteMode::History
        } else {
            stream.write
        };
        let plan = StreamPlan::new(StreamName::new(&stream.name).expect("a valid stream name"))
            .read(ReadMode::Cdc)
            .write(write);
        if stream.write == WriteMode::Merge {
            plan.deletes(stream.deletes).on_truncate(stream.truncates)
        } else {
            plan
        }
    });
    PipelinePlan::new(PipelineId::parse("changes").expect("a valid id"), streams)
        .expect("the change plan is valid")
}

/// Checks `stream`'s table after `round` against the model.
fn check(world: &World, stream: &ChangeStream, round: usize, seed: Seed) {
    let store = world.store.lock();
    let rows: Vec<Stored> = store.published(&stream.name);
    drop(store);
    let context = || format!("seed {seed}: round {round}: stream {}", stream.name);
    if stream.history {
        let mut versions: Vec<Version> = rows.iter().map(version).collect();
        versions.sort();
        assert_eq!(
            versions,
            stream.history(round),
            "{}: the history",
            context()
        );
        return;
    }
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

/// Checks that every position acknowledged to a stream that cannot read again landed after
/// `round`.
///
/// It was heard before its commit, so only the write-ahead log could see it through a failure. A
/// snapshot partition's position is gone once its stream reads its changes, which counts as
/// landed.
fn check_acknowledged(world: &World, round: usize, seed: Seed) {
    let entries: Vec<StateEntry> = world
        .store
        .lock()
        .states()
        .flat_map(|records| records.values())
        .filter_map(|record| StateEntry::from_record(record).ok())
        .collect();
    for ((stream, partition), acknowledged) in world.acknowledged.lock().iter() {
        let reads_changes = entries.iter().any(|entry| {
            matches!(entry, StateEntry::Phase { stream: named, phase }
                if named.name() == stream && *phase == CHANGES_PHASE)
        });
        let committed = entries.iter().find_map(|entry| match entry {
            StateEntry::Partition {
                stream: named,
                partition: id,
                state: PartitionState::Cursor(cursor),
                ..
            } if named.name() == stream && id.as_str() == partition => cursor
                .decode::<Position>(1)
                .ok()
                .map(|position| position.next),
            _ => None,
        });
        let landed = committed.is_some_and(|committed| committed >= *acknowledged)
            || (partition != CHANGES_PARTITION && reads_changes);
        assert!(
            landed,
            "seed {seed}: round {round}: stream {stream} partition {partition} acknowledged \
             position {acknowledged}, but committed {committed:?}"
        );
    }
}

/// A version of a history table: its key, beginning, value, counter, end, whether it is current,
/// and whether a soft delete opened it.
fn version(row: &Stored) -> Version {
    let micros = |column: &str| match row.cells.get(column) {
        Some(Canon::Instant(nanos)) => u64::try_from(nanos / 1_000).ok(),
        _ => None,
    };
    (
        number(row, "id").unwrap_or(-1),
        micros("_rdlt_valid_from").unwrap_or(u64::MAX),
        text(row, "value"),
        number(row, "n").unwrap_or_default(),
        micros("_rdlt_valid_to"),
        row.cells.get("_rdlt_is_current") == Some(&Canon::Bool(true)),
        !matches!(row.cells.get("_rdlt_deleted_at"), None | Some(Canon::Null)),
    )
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
