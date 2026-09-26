use std::num::NonZeroUsize;
use std::sync::Arc;

use rdlt_connector::serve::Served;
use rdlt_connector::{
    ConnectorErrorKind, LogLevel, Partition, PartitionId, PipelineId, ReadRequest, Role,
    Source as _, SourceEvent, StreamName, partition_channel, source_factory,
};
use rdlt_connector_reference::published;
use rdlt_engine::{PipelinePlan, RunStatus, StreamPlan};
use rdlt_host::{Connection, Options, RemoteSource};

use crate::support::connectors::{COMMITTED, Ticks};
use crate::support::{engine, memory_destination, served};

/// The ticks source, served, with `config`.
async fn ticks(config: serde_json::Value) -> RemoteSource {
    let io = served(Served::new().with_source(source_factory::<Ticks>()));
    let connection = Connection::connect(io, Role::Source, &config, Options::default())
        .await
        .expect("the source handshakes");
    RemoteSource::new(connection)
}

fn request() -> ReadRequest {
    ReadRequest {
        stream: StreamName::new("ticks").expect("a valid stream name"),
        partition: Partition::single(),
        cursor: None,
    }
}

/// Reads `source`'s ticks to the end; the events it sent, and how the read ended.
async fn events(source: &RemoteSource) -> (Vec<SourceEvent>, rdlt_connector::Result<()>) {
    let (sink, mut feed) = partition_channel(NonZeroUsize::new(4096).expect("not zero"));
    let read = source.read(request(), sink).await;
    let mut events = Vec::new();
    while let Some(event) = feed.recv().await {
        events.push(event);
    }
    (events, read)
}

#[tokio::test(flavor = "multi_thread")]
async fn arrow_batches_whose_schema_changes_mid_read_load_across_the_wire() {
    let source = ticks(serde_json::json!({ "rows": 200, "arrow": true })).await;
    let destination = memory_destination("served_arrow", Options::default()).await;
    let plan = PipelinePlan::new(
        PipelineId::parse("arrow").unwrap(),
        [StreamPlan::new(
            StreamName::new("ticks").expect("a valid stream name"),
        )],
    )
    .unwrap();
    let outcome = engine(50)
        .run(plan, Arc::new(source), Arc::new(destination))
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    let batches = published("served_arrow", "ticks");
    let loaded: usize = batches.iter().map(arrow_array::RecordBatch::num_rows).sum();
    assert_eq!(loaded, 200);
    assert!(
        batches
            .iter()
            .any(|batch| batch.schema().column_with_name("note").is_some()),
        "the second schema's column arrived"
    );
}

#[tokio::test]
async fn a_warning_and_a_metric_cross_the_wire_as_themselves() {
    let source = ticks(serde_json::json!({ "rows": 3, "chatty": true })).await;
    let (events, read) = events(&source).await;
    read.unwrap();
    assert!(events.contains(&SourceEvent::Log {
        level: LogLevel::Warn,
        message: "ticking".to_owned()
    }));
    assert!(events.contains(&SourceEvent::Metric {
        name: "ticks.started".to_owned(),
        value: 1.5
    }));
}

#[tokio::test]
async fn a_read_that_fails_fails_with_the_connectors_error() {
    let source = ticks(serde_json::json!({ "rows": 10, "fail_after": 4 })).await;
    let (_, read) = events(&source).await;
    let error = read.unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Data);
    assert_eq!(error.to_string(), "the ticks broke");
}

#[tokio::test]
async fn a_read_the_engine_stops_ends_as_the_source_returns() {
    let source = ticks(serde_json::json!({ "pace_ms": 1 })).await;
    let (sink, mut feed) = partition_channel(NonZeroUsize::new(64).expect("not zero"));
    let reading = tokio::spawn(async move { source.read(request(), sink).await });
    feed.recv().await.expect("the read sends");
    feed.stop();
    while feed.recv().await.is_some() {}
    // As in process, a read the engine stops ends as its source's read returned: cleanly.
    reading.await.unwrap().unwrap();
}

#[tokio::test]
async fn a_read_stopped_while_the_engine_is_full_ends_as_the_source_returns() {
    let source = ticks(serde_json::json!({})).await;
    let (sink, mut feed) = partition_channel(NonZeroUsize::new(1).expect("not zero"));
    let reading = tokio::spawn(async move { source.read(request(), sink).await });
    // The engine takes nothing, so the host waits to hand it the next frame when the stop comes.
    feed.recv().await.expect("the read sends");
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    feed.stop();
    while feed.recv().await.is_some() {}
    reading.await.unwrap().unwrap();
}

#[tokio::test]
async fn committed_cursors_reach_the_source() {
    let source = ticks(serde_json::json!({ "rows": 3 })).await;
    let (events, read) = events(&source).await;
    read.unwrap();
    let cursor = events
        .into_iter()
        .find_map(|event| match event {
            SourceEvent::Checkpoint { cursor, .. } => Some(cursor),
            _ => None,
        })
        .expect("the read checkpoints");
    let whole = PartitionId::parse("whole").unwrap();
    source
        .committed(
            &StreamName::new("ticks").expect("a valid stream name"),
            &[(whole, cursor)],
        )
        .await
        .unwrap();
    assert_eq!(COMMITTED.lock().unwrap().as_slice(), [3]);
}
