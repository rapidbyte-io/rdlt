//! Planning a following run's streams again: a partition the source adds starts as initial
//! planning would start it.

use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::{
    BoxFuture, Catalog, ConnectContext, Cursor, PartitionId, PartitionPlan, PartitionSink,
    ReadMode, ReadRequest, Source, StreamName, StreamState, source_factory,
};
use rdlt_connector_reference::LogSource;
use rdlt_engine::{EngineConfig, RunStatus, Until};
use serde_json::json;

use crate::support::{engine, memory, pipeline, published_json, stream};

/// The log source, whose plans name where each partition would start in a new phase: offset 3.
struct Started(Arc<dyn Source>);

impl Source for Started {
    fn check(&self) -> BoxFuture<'_, rdlt_connector::Result<()>> {
        self.0.check()
    }

    fn discover(&self) -> BoxFuture<'_, rdlt_connector::Result<Catalog>> {
        self.0.discover()
    }

    fn plan<'a>(
        &'a self,
        stream: &'a StreamName,
        state: &'a StreamState,
    ) -> BoxFuture<'a, rdlt_connector::Result<PartitionPlan>> {
        Box::pin(async move {
            let mut plan = self.0.plan(stream, state).await?;
            let start = Cursor::encode(1, &json!({ "next": 3 }))?;
            for partition in &plan.partitions {
                plan.starts.insert(partition.id().clone(), start.clone());
            }
            Ok(plan)
        })
    }

    fn committed<'a>(
        &'a self,
        stream: &'a StreamName,
        cursors: &'a [(PartitionId, Cursor)],
    ) -> BoxFuture<'a, rdlt_connector::Result<()>> {
        self.0.committed(stream, cursors)
    }

    fn read(
        &self,
        request: ReadRequest,
        sink: PartitionSink,
    ) -> BoxFuture<'_, rdlt_connector::Result<()>> {
        self.0.read(request, sink)
    }
}

#[tokio::test(start_paused = true)]
async fn a_partition_added_mid_run_starts_from_its_beginning_as_one_planned_first_does() {
    let config = json!({
        "seed": 5, "group": "added_from_the_start",
        "streams": [{
            "name": "events", "partitions": 1, "messages": 6,
            "partitions_later": 1, "later_after_ms": 500,
        }],
    });
    let log = source_factory::<LogSource>()
        .connect(config, ConnectContext::new())
        .await
        .expect("the log connects");
    let source = Arc::new(Started(Arc::from(log)));
    let incremental = stream("events").read(ReadMode::Incremental);
    let plan = pipeline("added", [incremental]).with_until(Until::For(Duration::from_secs(2)));
    let config = EngineConfig::builder()
        .lanes(2)
        .barrier_wait(Duration::from_millis(100))
        .replan(Duration::from_millis(200));
    let outcome = engine(config)
        .run(plan, source, memory("added_from_the_start").await)
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    // A plan's starts place only a new phase's partitions: each partition, the added one too,
    // is read from its first message.
    let mut offsets: Vec<(String, u64)> = published_json("added_from_the_start", "events")
        .iter()
        .map(|row| {
            let partition = row["partition"].as_str().expect("a partition").to_owned();
            (partition, row["offset"].as_u64().expect("an offset"))
        })
        .collect();
    offsets.sort();
    let every: Vec<(String, u64)> = ["p0", "p1"]
        .into_iter()
        .flat_map(|partition| (0..6).map(move |offset| (partition.to_owned(), offset)))
        .collect();
    assert_eq!(offsets, every);
}
