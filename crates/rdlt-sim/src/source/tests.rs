use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::{
    ConnectContext, PartitionPlan, Source, StreamName, StreamState, source_factory,
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
