//! Change streams whose source cannot read again what it acknowledged: the write-ahead log holds
//! each phase's transition and rows until the destination has them.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::UNIX_EPOCH;

use rdlt_connector::{
    CommitMeta, CommitSeq, ConnectContext, LoadId, OpenContext, PipelineId, ReadMode, Receipt,
    SegmentSet, Source, StateChange, StateEntry, source_factory,
};
use rdlt_connector_reference::ChangesSource;
use rdlt_connector_reference::changes::{ChangedStream, expected};
use rdlt_engine::{LocalWal, RunStatus, WalStore, WriteMode};
use serde_json::json;

use crate::changes::{config, log, logged, orders, rows};
use crate::support::faults::{Fault, Rule, begins, failing_commits};
use crate::support::{logging_engine, memory, pipeline, retrying, stream};

/// Where a load's commits fail.
#[derive(Clone, Copy, Debug)]
enum Place {
    /// The snapshot's second commit.
    Snapshot,
    /// The commit that begins the change phase.
    Transition,
    /// The third commit after it.
    Changes,
}

/// A rule failing the first commit at each of `places` once, with `fault`.
fn at(places: &[Place], fault: Fault) -> Arc<Rule> {
    let places = places.to_vec();
    let transition = Mutex::new(None::<usize>);
    let fired: Vec<AtomicBool> = places.iter().map(|_| AtomicBool::new(false)).collect();
    Arc::new(move |n: usize, meta: &CommitMeta| {
        let mut transition = transition.lock().expect("an unpoisoned lock");
        if transition.is_none() && begins(meta) == Some(1) {
            *transition = Some(n);
        }
        for (place, fired) in places.iter().zip(&fired) {
            let here = match place {
                Place::Snapshot => n == 2,
                Place::Transition => *transition == Some(n),
                Place::Changes => transition.is_some_and(|at| n == at + 3),
            };
            if here && !fired.swap(true, Ordering::SeqCst) {
                return fault;
            }
        }
        Fault::None
    })
}

/// A change stream whose source forgets what it acknowledged.
fn forgetful(truncates: &[u64]) -> ChangedStream {
    ChangedStream {
        replayable: false,
        ..orders(truncates)
    }
}

/// The change source of `stream_spec` under `seed`, acknowledging in a slot named `name`, which
/// no other test's runs acknowledged positions in.
async fn forgetting(name: &str, seed: u64, stream_spec: &ChangedStream) -> Arc<dyn Source> {
    let mut config = config(seed, std::slice::from_ref(stream_spec));
    config["slot"] = json!(name);
    Arc::from(
        source_factory::<ChangesSource>()
            .connect(config, ConnectContext::new())
            .await
            .expect("the source connects"),
    )
}

/// Runs `stream_spec` under `seed` as `name`, written as `mode`, with commits failing at
/// `places`; returns how many attempts the run took.
async fn run(
    name: &str,
    seed: u64,
    stream_spec: &ChangedStream,
    mode: WriteMode,
    rule: Arc<Rule>,
) -> usize {
    let base = tempfile::tempdir().expect("a temporary directory");
    let store: Arc<dyn WalStore> = Arc::new(LocalWal::new(base.path()));
    let pipeline_name = name.replace('_', "-");
    let plan = pipeline(
        &pipeline_name,
        [stream("orders").read(ReadMode::Cdc).write(mode)],
    );
    let destination = failing_commits(memory(name).await, rule);
    let outcome = logging_engine(retrying(8), Arc::clone(&store))
        .run(plan, forgetting(name, seed, stream_spec).await, destination)
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{name}: {:?}",
        outcome.error
    );
    let pipeline = PipelineId::parse(pipeline_name).expect("a valid pipeline");
    assert_eq!(store.loads(&pipeline).await.expect("loads list"), []);
    outcome.report.attempts.len()
}

#[tokio::test(start_paused = true)]
async fn a_forgetting_change_source_logs_every_change_once_through_failed_commits() {
    let placements: [&[Place]; 4] = [
        &[Place::Snapshot],
        &[Place::Transition],
        &[Place::Changes],
        &[Place::Snapshot, Place::Transition, Place::Changes],
    ];
    let stream_spec = forgetful(&[]);
    for (index, places) in placements.into_iter().enumerate() {
        for fault in [Fault::Before, Fault::After] {
            let name = format!("wal_changes_log_{index}_{fault:?}").to_lowercase();
            let attempts = run(&name, 5, &stream_spec, WriteMode::Append, at(places, fault)).await;
            // Each failed commit costs one attempt; a read from before what the source
            // acknowledged would cost more, until the retries ran out.
            assert_eq!(attempts, places.len() + 1, "{name}");
            assert_eq!(logged(&name, "orders"), log(5, &stream_spec), "{name}");
        }
    }
}

#[tokio::test(start_paused = true)]
async fn a_forgetting_change_source_merges_into_the_table_it_holds_through_failed_commits() {
    let stream_spec = forgetful(&[120]);
    let every = [Place::Snapshot, Place::Transition, Place::Changes];
    for fault in [Fault::Before, Fault::After] {
        let name = format!("wal_changes_merge_{fault:?}").to_lowercase();
        let attempts = run(&name, 6, &stream_spec, WriteMode::Merge, at(&every, fault)).await;
        assert_eq!(attempts, every.len() + 1, "{name}");
        assert_eq!(rows(&name, "orders"), expected(6, &stream_spec), "{name}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_forgetting_change_source_needs_every_commit_it_acknowledged_from_the_log() {
    // Without the log's transition, the change partition would be read again from its start,
    // which the source no longer holds: count the commits that begin the change phase.
    let begun = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&begun);
    let rule: Arc<Rule> = Arc::new(move |_, meta: &CommitMeta| {
        if begins(meta) == Some(1) && counted.fetch_add(1, Ordering::SeqCst) == 0 {
            Fault::Before
        } else {
            Fault::None
        }
    });
    let stream_spec = forgetful(&[]);
    let attempts = run(
        "wal_changes_begun",
        7,
        &stream_spec,
        WriteMode::Append,
        rule,
    )
    .await;
    assert_eq!(attempts, 2);
    // The transition lands once, from the log, and is not begun again by the next attempt.
    assert_eq!(begun.load(Ordering::SeqCst), 2);
    assert_eq!(logged("wal_changes_begun", "orders"), log(7, &stream_spec));
}

#[tokio::test(start_paused = true)]
async fn a_transition_a_newer_load_committed_past_lands_from_the_log() {
    let name = "wal_changes_newer";
    let base = tempfile::tempdir().expect("a temporary directory");
    let store: Arc<dyn WalStore> = Arc::new(LocalWal::new(base.path()));
    let plan = || pipeline("wal-changes-newer", [stream("orders").read(ReadMode::Cdc)]);
    let stream_spec = forgetful(&[]);
    // The first run's transition fails before it lands, after the source acknowledged its changes,
    // and the run gives up: only its log holds them.
    let rule: Arc<Rule> = Arc::new(|_, meta: &CommitMeta| {
        if begins(meta) == Some(1) {
            Fault::Before
        } else {
            Fault::None
        }
    });
    let failed = logging_engine(retrying(1), Arc::clone(&store))
        .run(
            plan(),
            forgetting(name, 8, &stream_spec).await,
            failing_commits(memory(name).await, rule),
        )
        .await;
    assert_eq!(failed.report.status, RunStatus::Failed);
    // A newer load commits meanwhile, so the destination no longer stands where the log's load
    // left it, and replay lands only what still matches: the transition among it.
    let destination = memory(name).await;
    let context = OpenContext {
        pipeline: PipelineId::parse("wal-changes-newer").expect("a valid pipeline"),
        load_id: LoadId::from_parts(UNIX_EPOCH, 99),
    };
    let mut newer = destination.open(&context).await.expect("a session opens");
    // Its commit records its receipt, as every commit of the engine's does.
    let receipt = Receipt {
        load_id: context.load_id,
        commit_seq: CommitSeq::FIRST,
        committed_at: UNIX_EPOCH,
        rows: 0,
        bytes: 0,
    };
    let meta = CommitMeta {
        load_id: context.load_id,
        commit_seq: CommitSeq::FIRST,
        epoch: newer.epoch,
        segments: SegmentSet::new(),
        abandoned: SegmentSet::new(),
        state_delta: vec![StateChange::Put(StateEntry::Receipt(receipt).to_record())],
        finish_generations: Vec::new(),
        child_tables: Vec::new(),
        drop_tables: Vec::new(),
        horizon: None,
    };
    newer
        .session
        .commit(&meta)
        .await
        .expect("the newer load commits");
    let outcome = logging_engine(retrying(1), Arc::clone(&store))
        .run(plan(), forgetting(name, 8, &stream_spec).await, destination)
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert_eq!(logged(name, "orders"), log(8, &stream_spec));
}
