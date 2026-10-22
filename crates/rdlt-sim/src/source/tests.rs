use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::{
    ConnectContext, PartitionFeed, PartitionPlan, ReadRequest, Source, SourceEvent, StreamName,
    StreamState, partition_channel, source_factory,
};
use serde_json::json;

use super::{SimCursor, SimSource};
use crate::seed::{Seed, run};
use crate::world::churning;

/// Bounds every wait of a test: a call that hangs fails its test.
const WAIT: Duration = Duration::from_secs(60);

#[test]
fn a_churning_plan_names_what_its_world_names_and_starts_each_partition_at_its_newest_row() {
    let name = "churning-plan";
    let (registered, index) = churning(name);
    let world = Arc::clone(registered.world());
    let stream = &world.workload.streams[index];
    let last = stream.partitions.len() - 1;
    let plans = run(Seed::new(1), {
        let (world, stream) = (Arc::clone(&world), StreamName::new(&stream.name).unwrap());
        |_env| async move {
            let config = json!({ "world": name });
            let factory = source_factory::<SimSource>();
            let connecting = factory.connect(config, ConnectContext::new());
            let source: Arc<dyn Source> = Arc::from(
                tokio::time::timeout(WAIT, connecting)
                    .await
                    .expect("the connect ends")
                    .expect("the source connects"),
            );
            let (mut plans, state) = (Vec::new(), StreamState::default());
            // The stream's last partition alone, then none of its partitions.
            for named in [BTreeSet::from([last]), BTreeSet::new()] {
                world.named.name(Some(BTreeMap::from([(index, named)])));
                let planning = source.plan(&stream, &state);
                let plan = tokio::time::timeout(WAIT, planning)
                    .await
                    .expect("the plan ends")
                    .expect("the plan is answered");
                plans.push(plan);
            }
            plans
        }
    });
    let ids = |plan: &PartitionPlan| -> Vec<String> {
        plan.partitions
            .iter()
            .map(|partition| partition.id().to_string())
            .collect()
    };
    assert_eq!(ids(&plans[0]), vec![format!("p{last}")]);
    assert!(plans[0].phase.is_none(), "{:?}", plans[0]);
    let start = plans[0]
        .starts
        .values()
        .next()
        .expect("each partition named has a start")
        .decode::<SimCursor>(1)
        .unwrap();
    assert_eq!(
        start.next,
        stream.rows(last, 0).len() as u64,
        "every row has arrived"
    );
    assert!(
        plans[1].partitions.is_empty() && plans[1].starts.is_empty(),
        "{:?}",
        plans[1]
    );
    drop(registered);
}

/// Connects a source to the world registered as `name`.
async fn connected(name: &str) -> Arc<dyn Source> {
    let factory = source_factory::<SimSource>();
    let connecting = factory.connect(json!({ "world": name }), ConnectContext::new());
    let source = tokio::time::timeout(WAIT, connecting)
        .await
        .expect("the connect ends")
        .expect("the source connects");
    Arc::from(source)
}

/// Plans `stream` of `source`.
async fn planned(source: &dyn Source, stream: &StreamName) -> PartitionPlan {
    let state = StreamState::default();
    let planning = source.plan(stream, &state);
    tokio::time::timeout(WAIT, planning)
        .await
        .expect("the plan ends")
        .expect("the plan is answered")
}

/// Takes the events of `feed` until it ends: how many, and the furthest checkpoint among them.
async fn drained(feed: &mut PartitionFeed) -> (u64, u64) {
    let (mut count, mut checkpoint) = (0, 0);
    while let Some(event) = tokio::time::timeout(WAIT, feed.recv())
        .await
        .expect("the read goes on")
    {
        count += 1;
        if let SourceEvent::Checkpoint { cursor, .. } = event {
            let cursor = cursor.decode::<SimCursor>(1).unwrap();
            checkpoint = checkpoint.max(cursor.next);
        }
    }
    (count, checkpoint)
}

#[test]
fn a_connections_record_holds_its_plans_and_what_its_reads_sent_after_a_plan_dropped_them() {
    let name = "recorded-plans";
    let (registered, index) = churning(name);
    let world = Arc::clone(registered.world());
    let stream = StreamName::new(&world.workload.streams[index].name).unwrap();
    assert!(world.workload.streams[index].partitions.len() >= 2);
    let (after, checkpoint) = run(Seed::new(1), {
        let world = Arc::clone(&world);
        |_env| async move {
            let source = connected(name).await;
            let both = BTreeSet::from([0, 1]);
            world.named.name(Some(BTreeMap::from([(index, both)])));
            let plan = planned(&*source, &stream).await;
            let partition = plan.partitions[0].clone();
            let request = ReadRequest::new(stream.clone(), partition, None);
            // One event at a time: the read begins, then a plan stops naming its partition.
            let (sink, mut feed) = partition_channel(NonZeroUsize::MIN);
            let reading = source.read(request, sink);
            let controller = async {
                let first = feed.recv().await;
                assert!(first.is_some(), "the read sends something");
                world
                    .named
                    .name(Some(BTreeMap::from([(index, BTreeSet::from([1]))])));
                planned(&*source, &stream).await;
                drained(&mut feed).await
            };
            let (read, taken) =
                tokio::time::timeout(WAIT, async { tokio::join!(reading, controller) })
                    .await
                    .expect("the read ends");
            read.expect("the read ends well");
            taken
        }
    });
    let connections = world.connections();
    assert_eq!(connections.len(), 1);
    let (plans, key) = (&connections[0], (index, 0));
    let findings = plans.findings();
    assert!(findings.replanned, "{findings:?}");
    assert!(findings.doubled.is_empty(), "{findings:?}");
    assert!(
        after >= 2,
        "the read sends more than the plan outlasts: {after}"
    );
    assert!(
        findings.late.get(&key).copied().unwrap_or(0) >= after - 1,
        "{findings:?} after {after}"
    );
    let drained = plans.drained();
    assert_eq!(drained.len(), usize::from(checkpoint > 0), "{drained:?}");
    if checkpoint > 0 {
        assert_eq!(drained, vec![(key, checkpoint)]);
    }
    drop(registered);
}
