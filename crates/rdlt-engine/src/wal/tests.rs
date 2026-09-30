//! A whole load through its log: however slow the log's disk, a commit's frame follows every
//! batch frame of its segments.

use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::sync::Arc;

use rdlt_connector::{ConnectContext, PipelineId, SegmentId, destination_factory, source_factory};
use rdlt_connector_reference::{GeneratorSource, MemoryDestination};
use serde_json::json;

use super::WalStore;
use super::frame::{Frame, Frames};
use super::memory::MemoryWal;
use crate::compute::RayonPool;
use crate::config::{CommitPolicy, EngineConfig};
use crate::env::SystemEnv;
use crate::plan::{PipelinePlan, StreamPlan};
use crate::report::RunStatus;
use crate::run::Engine;

#[tokio::test]
async fn a_commit_s_frame_follows_every_batch_of_its_segments_however_slow_the_disk() {
    let store = Arc::new(MemoryWal {
        slow: true,
        ..MemoryWal::default()
    });
    let pool = RayonPool::new(NonZeroUsize::MIN).expect("a pool starts");
    let env = SystemEnv::new(pool).with_wal(Arc::clone(&store) as Arc<dyn WalStore>);
    let config = EngineConfig::builder()
        .lanes(3)
        .commit(CommitPolicy::new(None, Some(50), None).expect("a valid policy"))
        .build()
        .expect("a valid configuration");
    let engine = Engine::new(config, Arc::new(env));
    let streams = json!({
        "seed": 3,
        "streams": [{ "name": "orders", "rows": 600, "partitions": 4, "batch_rows": 7 }],
    });
    let source = source_factory::<GeneratorSource>()
        .connect(streams, ConnectContext::new())
        .await
        .expect("the generator connects");
    let destination = destination_factory::<MemoryDestination>()
        .connect(json!({ "store": "wal_ordered" }), ConnectContext::new())
        .await
        .expect("the destination connects");
    let pipeline = PipelineId::parse("wal-ordered").expect("a valid pipeline");
    let stream = StreamPlan::new(rdlt_connector::StreamName::new("orders").expect("a name"));
    let plan = PipelinePlan::new(pipeline, [stream])
        .expect("a valid plan")
        .with_wal(true);
    let outcome = engine
        .run(plan, Arc::from(source), Arc::from(destination))
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    let mut logged: BTreeSet<SegmentId> = BTreeSet::new();
    let mut commits = 0;
    for frame in store.appended.lock().iter() {
        match Frames::new(frame).next() {
            Some(Ok((_, Frame::Batch(batch)))) => {
                logged.insert(batch.segment);
            }
            Some(Ok((_, Frame::Commit(meta)))) => {
                commits += 1;
                for segment in meta.segments.iter() {
                    assert!(
                        logged.contains(&segment),
                        "segment {segment:?} after its commit"
                    );
                }
            }
            _ => {}
        }
    }
    assert!(commits >= 2, "{commits} commits");
}
