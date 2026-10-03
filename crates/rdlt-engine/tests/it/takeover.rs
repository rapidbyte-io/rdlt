//! Two engines running one pipeline: a load that stalls while the other takes its log over,
//! replays and removes it never publishes again, and leaves nothing for a later run to replay.

use std::sync::Arc;

use parking_lot::Mutex;
use rdlt_connector::{PipelineId, ReadMode};
use rdlt_engine::{LocalWal, RunOutcome, RunStatus, WalStore};

use crate::support::hooked::{At, Hook, hooked};
use crate::support::memory_wal::Memory;
use crate::support::script::{Script, ScriptStream, id, reconnect};
use crate::support::{commit_every, logging_engine, memory, pipeline, published_ids, stream};

/// Runs a load of `name` over `store` that stalls once its first commit lands, while another
/// engine runs the pipeline to its end, then wakes: what both runs came to.
async fn stalled(store: Arc<dyn WalStore>, name: &str) -> (RunOutcome, RunOutcome) {
    let mut events = ScriptStream::new("events", 1, 30, 5);
    events.replayable = false;
    let (_, source) = Script::new(vec![events]).connect(name).await;
    let plan = pipeline(
        &name.replace('_', "-"),
        [stream("events").read(ReadMode::Incremental)],
    );
    let other = Arc::new(Mutex::new(None));
    let hook: Hook = {
        let (store, plan, other, name) = (
            Arc::clone(&store),
            plan.clone(),
            Arc::clone(&other),
            name.to_owned(),
        );
        Arc::new(move || {
            let (store, plan, other, name) = (
                Arc::clone(&store),
                plan.clone(),
                Arc::clone(&other),
                name.clone(),
            );
            Box::pin(async move {
                let source = reconnect(&name).await;
                let outcome = logging_engine(commit_every(10), store)
                    .run(plan, source, memory(&name).await)
                    .await;
                *other.lock() = Some(outcome);
            })
        })
    };
    let destination = hooked(memory(name).await, At::Landed, hook);
    let stalled = logging_engine(commit_every(10), Arc::clone(&store))
        .run(plan, source, destination)
        .await;
    let other = other.lock().take().expect("the other engine ran");
    (stalled, other)
}

#[tokio::test(start_paused = true)]
async fn a_load_whose_log_another_engine_took_never_publishes_again() {
    let base = tempfile::tempdir().expect("a temporary directory");
    for (store, name) in [
        (
            Arc::new(LocalWal::new(base.path())) as Arc<dyn WalStore>,
            "taken_local",
        ),
        (
            Arc::new(Memory::default()) as Arc<dyn WalStore>,
            "taken_memory",
        ),
    ] {
        let (stalled, other) = stalled(Arc::clone(&store), name).await;
        assert_eq!(
            other.report.status,
            RunStatus::Succeeded,
            "{name}: {:?}",
            other.error
        );
        let error = stalled.error.expect("the stalled load fails");
        assert_eq!(error.code(), Some("wal_fenced"), "{name}: {error}");
        let every: Vec<i64> = (0..30).map(|offset| id(0, offset)).collect();
        assert_eq!(published_ids(name, "events"), every, "{name}");
        // Nothing of the stalled load is left for a later run to replay.
        let pipeline = PipelineId::parse(name.replace('_', "-")).expect("a valid pipeline");
        assert_eq!(store.loads(&pipeline).await.expect("lists"), [], "{name}");
        assert_eq!(
            store.leftovers(&pipeline).await.expect("lists"),
            [],
            "{name}"
        );
    }
}
