//! A replay that fails, wherever it fails, fails closed: the log it read stays to be replayed,
//! no chunk of it is gone, the destination records nothing the replay did not commit, and the
//! next attempt lands every row once.

use std::collections::BTreeMap;
use std::sync::Arc;

use rdlt_connector::{LoadId, OpenContext, PipelineId, ReadMode, StateKey, StateRecord};
use rdlt_engine::{LocalWal, RunStatus, WalStore};

use crate::support::destinations::{Step, failing};
use crate::support::script::{Script, ScriptStream, id};
use crate::support::{logging_engine, memory, pipeline, published_ids, retrying, stream};

/// A log of pipeline `name` in `store` holding a commit its source was told of and that never
/// landed: the first load's every commit fails.
async fn logged(name: &str, store: &Arc<dyn WalStore>) -> Arc<dyn rdlt_connector::Source> {
    let mut events = ScriptStream::new("events", 1, 20, 5);
    events.replayable = false;
    let (_, source) = Script::new(vec![events]).connect(name).await;
    let failed = logging_engine(retrying(1), Arc::clone(store))
        .run(
            plan(name),
            Arc::clone(&source),
            failing(memory(name).await, Step::Commit),
        )
        .await;
    assert_eq!(failed.report.status, RunStatus::Failed, "{name}");
    source
}

fn plan(name: &str) -> rdlt_engine::PipelinePlan {
    pipeline(
        &name.replace('_', "-"),
        [stream("events").read(ReadMode::Incremental)],
    )
}

fn pipeline_id(name: &str) -> PipelineId {
    PipelineId::parse(name.replace('_', "-")).expect("a valid pipeline")
}

/// The logs of `name`'s pipeline in `store`, each by the numbers of its chunks.
async fn logs(name: &str, store: &Arc<dyn WalStore>) -> BTreeMap<LoadId, Vec<u64>> {
    let pipeline = pipeline_id(name);
    let mut logs = BTreeMap::new();
    for load in store.loads(&pipeline).await.expect("lists") {
        let chunks = store.chunks(&pipeline, load).await.expect("lists");
        logs.insert(load, chunks.into_iter().map(|(number, _)| number).collect());
    }
    logs
}

/// What the destination `name` records of its pipeline, but for the epoch its opens move.
async fn recorded(name: &str) -> Vec<StateRecord> {
    let context = OpenContext {
        pipeline: pipeline_id(name),
        load_id: LoadId::from_parts(std::time::UNIX_EPOCH, 1),
    };
    let opened = memory(name).await.open(&context).await.expect("opens");
    drop(opened.session.close().await);
    let epoch = StateKey::Epoch.encode();
    let mut state: Vec<StateRecord> = opened
        .state
        .into_iter()
        .filter(|record| record.key != epoch)
        .collect();
    state.sort_by(|left, right| left.key.cmp(&right.key));
    state
}

#[tokio::test(start_paused = true)]
async fn a_replay_that_fails_at_the_destination_keeps_the_log_and_records_nothing() {
    let base = tempfile::tempdir().expect("a temporary directory");
    for (step, lands) in [
        (Step::Open, false),
        (Step::CreateTable, false),
        (Step::Writer, false),
        (Step::Commit, false),
        (Step::LoseResponse, true),
    ] {
        let name = format!("replay_failing_{step:?}").to_lowercase();
        let store: Arc<dyn WalStore> = Arc::new(LocalWal::new(base.path().join(&name)));
        let source = logged(&name, &store).await;
        let (before, state) = (logs(&name, &store).await, recorded(&name).await);
        assert_eq!(before.len(), 1, "{name}: the log of the failed load");
        let failed = logging_engine(retrying(1), Arc::clone(&store))
            .run(
                plan(&name),
                Arc::clone(&source),
                failing(memory(&name).await, step),
            )
            .await;
        assert_eq!(failed.report.status, RunStatus::Failed, "{name}");
        // The log stays, every chunk of it, and the destination holds only what landed.
        let after = logs(&name, &store).await;
        for (load, chunks) in &before {
            let kept = &after[load];
            assert!(chunks.iter().all(|number| kept.contains(number)), "{name}");
        }
        if lands {
            // The logged commit landed, once, though its answer was lost.
            let landed = published_ids(&name, "events");
            let first: Vec<i64> = (0..landed.len())
                .map(|offset| id(0, offset as u64))
                .collect();
            assert!(!landed.is_empty(), "{name}");
            assert_eq!(landed, first, "{name}");
        } else {
            assert_eq!(published_ids(&name, "events"), Vec::<i64>::new(), "{name}");
            assert_eq!(recorded(&name).await, state, "{name}");
        }
        let landed = logging_engine(retrying(1), Arc::clone(&store))
            .run(plan(&name), source, memory(&name).await)
            .await;
        assert_eq!(
            landed.report.status,
            RunStatus::Succeeded,
            "{name}: {:?}",
            landed.error
        );
        let every: Vec<i64> = (0..20).map(|offset| id(0, offset)).collect();
        assert_eq!(published_ids(&name, "events"), every, "{name}");
        assert_eq!(logs(&name, &store).await, BTreeMap::new(), "{name}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_log_damaged_on_disk_is_refused_by_every_attempt_and_kept() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let name = "replay_damaged";
    let store: Arc<dyn WalStore> = Arc::new(LocalWal::new(base.path()));
    let source = logged(name, &store).await;
    let before = logs(name, &store).await;
    let (load, chunks) = before.iter().next().expect("a log");
    let chunk = LocalWal::new(base.path())
        .pipeline_dir(&pipeline_id(name))
        .join(load.to_string())
        .join(format!("{:08}.wal", chunks[0]));
    let mut bytes = std::fs::read(&chunk).expect("reads");
    let at = bytes.len() / 2;
    bytes[at] ^= 0x40;
    std::fs::write(&chunk, &bytes).expect("writes");
    let state = recorded(name).await;
    for _ in 0..2 {
        let refused = logging_engine(retrying(1), Arc::clone(&store))
            .run(plan(name), Arc::clone(&source), memory(name).await)
            .await;
        let error = refused.error.expect("the log is refused");
        assert_eq!(error.code(), Some("wal_unreadable"), "{error}");
        let after = logs(name, &store).await;
        assert!(chunks.iter().all(|number| after[load].contains(number)));
        assert_eq!(published_ids(name, "events"), Vec::<i64>::new());
        assert_eq!(recorded(name).await, state);
    }
}
