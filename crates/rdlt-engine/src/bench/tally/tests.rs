use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use rdlt_connector::{ConnectContext, PipelineId, StreamName};

use super::{Staged, staged, tally};
use crate::bench::{Replayed, Sinking, replay, sink_factory};
use crate::{Engine, EngineConfig, PipelinePlan, StreamPlan, SystemEnv};

#[test]
fn a_tally_adds_the_rows_each_table_stages_until_it_is_taken() {
    let shared = tally("added");
    shared.lock().stage("a", 3);
    tally("added").lock().stage("b", 1);
    shared.lock().stage("a", 2);
    shared.lock().commits += 1;
    let expected = Staged {
        rows: [("a".to_owned(), 5), ("b".to_owned(), 1)].into(),
        commits: 1,
    };
    assert_eq!(staged("added"), expected);
    assert_eq!(staged("added"), Staged::default());
    assert_eq!(staged("never"), Staged::default());
}

#[tokio::test]
async fn a_tallying_sink_tallies_each_table_s_rows_and_each_commit() {
    let batches = (0..3)
        .map(|batch| {
            let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(batch * 10..batch * 10 + 10));
            RecordBatch::try_from_iter([("id", ids)]).unwrap()
        })
        .collect();
    let engine = Engine::new(EngineConfig::default(), Arc::new(SystemEnv::one_core()));
    let stream = StreamName::new("events").unwrap();
    let plan = PipelinePlan::new(
        PipelineId::parse("tallied").unwrap(),
        [StreamPlan::new(stream)],
    );
    let sink = sink_factory()
        .connect(Sinking::Discard.tallied("sunk"), ConnectContext::new())
        .await
        .unwrap();
    let outcome = engine
        .run(
            plan.unwrap(),
            replay("tallied", Replayed::Batches(batches)).await,
            Arc::from(sink),
        )
        .await;
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    let expected = Staged {
        rows: [("events".to_owned(), 30)].into(),
        commits: outcome.report.commits,
    };
    assert_eq!(expected.commits, 1);
    assert_eq!(staged("sunk"), expected);
}
