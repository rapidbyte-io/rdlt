//! Write-ahead logs in an object store: a source that cannot read again loads every row once
//! through one, whatever its requests meet.

use std::sync::Arc;
use std::time::Duration;

use object_store::memory::InMemory;
use rdlt_connector::{PipelineId, ReadMode};
use rdlt_engine::{ObjectStoreOptions, ObjectStoreWal, RunStatus, SystemClock, WalStore};
use rdlt_testkit::objects::{Fault, Faulty, Op, Plan, faultless};

use crate::support::destinations::{Step, failing};
use crate::support::script::{Script, ScriptStream, id};
use crate::support::{logging_engine, memory, pipeline, published_ids, retrying, stream};

/// A log beneath `logs` in an in-memory store whose requests `plan` faults, once the probe passed
/// unfaulted.
async fn store(plan: Plan) -> (Arc<Faulty<InMemory>>, Arc<dyn WalStore>) {
    let objects = Arc::new(Faulty::new(InMemory::new(), faultless()));
    // Parts of a kilobyte, so most commits upload theirs in parts, and a second a request, so
    // one never answered is given up within the test's limit.
    let options = ObjectStoreOptions::default()
        .with_part_bytes(1024.try_into().expect("not zero"))
        .with_deadline(Duration::from_secs(1), Duration::ZERO);
    let wal = ObjectStoreWal::open(
        Arc::clone(&objects) as _,
        "logs",
        Arc::new(SystemClock),
        options,
    )
    .await
    .expect("the probe passes");
    objects.plan(plan);
    (objects, Arc::new(wal))
}

/// The ids two partitions of thirty rows hold, in order.
fn ids() -> Vec<i64> {
    let mut ids: Vec<i64> = (0..2)
        .flat_map(|partition| (0..30).map(move |offset| id(partition, offset)))
        .collect();
    ids.sort_unstable();
    ids
}

/// Loads two partitions of thirty rows a source cannot read again as `name`, through `wal` and
/// into a destination failing as `step` says; the run's status and the error it ended with.
async fn loaded(name: &str, wal: Arc<dyn WalStore>, step: Option<Step>) -> RunStatus {
    let mut events = ScriptStream::new("events", 2, 30, 7);
    events.replayable = false;
    let (_, source) = Script::new(vec![events]).connect(name).await;
    let destination = match step {
        Some(step) => failing(memory(name).await, step),
        None => memory(name).await,
    };
    let plan = pipeline(
        &name.replace('_', "-"),
        [stream("events").read(ReadMode::Incremental)],
    );
    let outcome = logging_engine(retrying(10), Arc::clone(&wal))
        .run(plan, source, destination)
        .await;
    assert_eq!(
        published_ids(name, "events"),
        ids(),
        "{name}: {:?}",
        outcome.error
    );
    let pipeline = PipelineId::parse(name.replace('_', "-")).expect("a valid pipeline");
    assert_eq!(
        wal.loads(&pipeline).await.expect("lists"),
        [],
        "{name}: a log is left"
    );
    outcome.report.status
}

#[tokio::test(start_paused = true)]
async fn a_source_that_cannot_read_again_loads_once_through_an_object_store() {
    let (_, wal) = store(faultless()).await;
    let status = loaded("objects_plain", wal, None).await;
    assert_eq!(status, RunStatus::Succeeded);
}

#[tokio::test(start_paused = true)]
async fn a_commit_whose_destination_failed_lands_once_replayed_from_the_object_store() {
    for (name, step) in [
        ("objects_commit_failed", Step::CommitOnce),
        ("objects_response_lost", Step::LoseResponse),
    ] {
        let (objects, wal) = store(faultless()).await;
        let status = loaded(name, wal, Some(step)).await;
        assert_eq!(status, RunStatus::Succeeded, "{name}");
        // The replay read the commit back from the store.
        assert!(
            objects.calls().iter().any(|call| call.op == Op::Get),
            "{name}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_load_lands_once_whatever_fault_its_requests_meet() {
    let faults = [Fault::Fail, Fault::Slow(5), Fault::Hang, Fault::Answerless];
    for fault in faults {
        let mut seen = 0_u32;
        let plan: Plan = Box::new(move |_| {
            seen += 1;
            if seen.is_multiple_of(3) {
                fault
            } else {
                Fault::None
            }
        });
        let (_, wal) = store(plan).await;
        let name = format!("objects_{fault:?}")
            .to_lowercase()
            .replace(['(', ')'], "");
        let status = loaded(&name, wal, None).await;
        assert_eq!(status, RunStatus::Succeeded, "{fault:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_store_failing_every_request_for_a_while_fails_attempts_retryably_then_lands_once() {
    // The first eighty requests fail, more than a request's attempts several times over.
    let mut seen = 0_u32;
    let plan: Plan = Box::new(move |_| {
        seen += 1;
        if seen <= 80 { Fault::Fail } else { Fault::None }
    });
    let (_, wal) = store(plan).await;
    let mut events = ScriptStream::new("events", 2, 30, 7);
    events.replayable = false;
    let name = "objects_down";
    let (_, source) = Script::new(vec![events]).connect(name).await;
    let plan = pipeline(
        "objects-down",
        [stream("events").read(ReadMode::Incremental)],
    );
    let outcome = logging_engine(retrying(30), Arc::clone(&wal))
        .run(plan, source, memory(name).await)
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    let failed = &outcome.report.attempts[0];
    let error = failed.error.as_ref().expect("the first attempt failed");
    assert_eq!(
        error.code.as_deref(),
        Some("wal_storage_unavailable"),
        "{error:?}"
    );
    assert!(error.retryable);
    assert_eq!(published_ids(name, "events"), ids());
}
