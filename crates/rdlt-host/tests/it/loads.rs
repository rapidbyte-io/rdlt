use std::sync::Arc;

use rdlt_connector::{PipelineId, StreamName};
use rdlt_connector_reference::published;
use rdlt_engine::{PipelinePlan, RunStatus, StreamPlan};
use rdlt_host::{Options, RemoteSource};

use crate::support::{engine, memory_destination, memory_source};

#[tokio::test(flavor = "multi_thread")]
async fn a_pipeline_loads_through_connectors_served_over_sockets() {
    let rows: Vec<_> = (0..250)
        .map(|id| serde_json::json!({ "id": id, "name": format!("row {id}") }))
        .collect();
    let config = serde_json::json!({ "streams": { "items": rows }, "page_size": 20 });
    let source = memory_source(config, Options::default()).await;
    let destination = memory_destination("served_items", Options::default()).await;
    let stream = StreamPlan::new(StreamName::new("items").expect("a valid stream name"));
    let plan = PipelinePlan::new(PipelineId::parse("served").unwrap(), [stream]).unwrap();
    let outcome = engine(50)
        .run(plan, Arc::new(source), Arc::new(destination))
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    let loaded: usize = published("served_items", "items")
        .iter()
        .map(arrow_array::RecordBatch::num_rows)
        .sum();
    assert_eq!(loaded, 250);
    assert!(
        rdlt_connector_reference::schema("served_items", "items").is_some(),
        "the table was created"
    );
}

/// The on-demand source, served, reading `rows` rows or without end.
async fn ticks(rows: Option<u64>, pace_ms: u64) -> RemoteSource {
    let io = crate::support::served(rdlt_connector::serve::Served::new().with_source(
        rdlt_connector::source_factory::<crate::support::connectors::Ticks>(),
    ));
    let config = serde_json::json!({ "rows": rows, "pace_ms": pace_ms });
    let connection = rdlt_host::Connection::connect(
        io,
        rdlt_connector::Role::Source,
        &config,
        Options::default(),
    )
    .await
    .expect("the source handshakes");
    RemoteSource::new(connection)
}

fn ticks_plan(name: &str) -> PipelinePlan {
    let stream = StreamPlan::new(StreamName::new("ticks").expect("a valid stream name"));
    PipelinePlan::new(
        PipelineId::parse(name).expect("a valid pipeline id"),
        [stream],
    )
    .expect("a valid plan")
}

#[tokio::test(flavor = "multi_thread")]
async fn a_source_that_checkpoints_on_demand_answers_barriers_sent_across_the_wire() {
    let destination = memory_destination("served_ticks", Options::default()).await;
    let outcome = engine(100)
        .run(
            ticks_plan("ticks"),
            Arc::new(ticks(Some(1000), 1).await),
            Arc::new(destination),
        )
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    let loaded: usize = published("served_ticks", "ticks")
        .iter()
        .map(arrow_array::RecordBatch::num_rows)
        .sum();
    assert_eq!(loaded, 1000);
    // Without the engine's barriers the source checkpoints once, at its end.
    assert!(
        outcome.report.commits > 1,
        "{} commits",
        outcome.report.commits
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn stopping_a_run_at_once_ends_a_read_served_without_end() {
    let destination = memory_destination("stopped_ticks", Options::default()).await;
    let run = engine(100).run(
        ticks_plan("stopped"),
        Arc::new(ticks(None, 1).await),
        Arc::new(destination),
    );
    let control = run.control();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        control.stop(rdlt_engine::StopMode::Now);
    });
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(10), run)
        .await
        .expect("the run stops");
    // Stopping at once cancels the attempt, as it does in process.
    assert_eq!(
        outcome.report.status,
        RunStatus::Cancelled,
        "{:?}",
        outcome.error
    );
}
