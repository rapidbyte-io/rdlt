//! Exactly-once, live: replays publish nothing, resume reads catalog
//! state, a narrowed stream null-fills, and Replace refuses against a
//! real catalog.

use rdlt_connector_iceberg_v2::destination::Shell;
use rdlt_connector_sdk::spi::StreamSpec;
use rdlt_connector_sdk::spi::core::WriteMode;
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::{MemoryBatch, MemorySource, MemoryStream};
use serde_json::json;

use super::common::CatalogFixture;

fn one_batch(rows: Vec<serde_json::Value>) -> MemorySource {
    MemorySource::new(vec![MemoryStream::new(
        StreamSpec::new("events"),
        vec![MemoryBatch::new(rows).with_checkpoint(1)],
    )])
}

/// A RESUMED pipeline redelivers nothing: run 2 reads run 1's state
/// from the CATALOG (the marker table) and the source resumes past
/// its checkpoint — totals stay exact.
#[tokio::test]
async fn a_resumed_pipeline_republishes_nothing() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let namespace = "resume_v2";
    let doc = fixture.doc(namespace);
    let workdir = tempfile::tempdir().expect("workdir");
    let wal = workdir.path().join("wal");

    for _ in 0..2 {
        Engine::new(
            EngineConfig::new("ice-resume-v2").with_workdir(wal.clone()),
            one_batch(vec![json!({"id": 1}), json!({"id": 2})]),
            Shell::from_value(doc.clone()).expect("valid"),
        )
        .run()
        .await
        .expect("the load settles");
    }

    let summaries = fixture.snapshot_summaries(namespace, "events").await;
    let total: u64 = summaries
        .iter()
        .filter_map(|s| s.get("added-records").and_then(|v| v.parse::<u64>().ok()))
        .sum();
    assert_eq!(
        total, 2,
        "run 2 resumed past the checkpoint and republished nothing: {summaries:?}"
    );
}

/// A REPLAYED commit publishes nothing: the same (load, seq)
/// redelivered through the SPI — a fresh session staging the same
/// batch — converges on the snapshot history instead of appending a
/// second copy.
#[tokio::test]
async fn a_replayed_commit_publishes_nothing() {
    use std::sync::Arc;

    use rdlt_connector_sdk::spi::core::{
        ColumnDef, ColumnType, LoadId, LogicalType, PipelineId, Provenance, StateDoc, TableName,
        TableSchema, WriteMode,
    };
    use rdlt_connector_sdk::spi::{CommitMeta, Destination, OpenContext};

    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let namespace = "spi_replay_v2";
    let shell = Shell::from_value(fixture.doc(namespace)).expect("valid");

    let schema = TableSchema {
        table: TableName::from("events"),
        parent: None,
        columns: vec![ColumnDef {
            name: "id".into(),
            column_type: ColumnType::scalar(LogicalType::Int64),
            nullable: false,
            provenance: Provenance::Inferred,
        }],
    };
    let batch = arrow_array::RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )])),
        vec![Arc::new(arrow_array::Int64Array::from(vec![1, 2]))],
    )
    .expect("batch");
    let pipeline = PipelineId::from("ice-spi-replay");
    let load = LoadId::from("load-replay-1");
    let meta = || CommitMeta {
        load_id: load.clone(),
        commit_seq: 1,
        state: StateDoc::new(pipeline.clone(), "test"),
        counters: Default::default(),
    };

    // The same (load, seq) staged and published by TWO sessions — the
    // second is the redelivery.
    for _ in 0..2 {
        let mut session = shell
            .open(OpenContext::new(pipeline.clone(), load.clone()))
            .await
            .expect("open");
        session
            .ensure_table(&schema, &WriteMode::Append)
            .await
            .expect("ensure");
        session
            .write(&TableName::from("events"), batch.clone())
            .await
            .expect("write");
        session.commit(meta()).await.expect("commit");
    }

    let summaries = fixture.snapshot_summaries(namespace, "events").await;
    assert_eq!(
        summaries.len(),
        1,
        "the replay published NOTHING: {summaries:?}"
    );
    let total: u64 = summaries
        .iter()
        .filter_map(|s| s.get("added-records").and_then(|v| v.parse::<u64>().ok()))
        .sum();
    assert_eq!(total, 2, "one copy of the rows: {summaries:?}");
}

/// A stream that stops providing a nullable column keeps loading —
/// the absent column null-fills against the live table.
#[tokio::test]
async fn a_narrowed_stream_null_fills_the_missing_column() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let namespace = "narrow_v2";
    let doc = fixture.doc(namespace);
    let workdir = tempfile::tempdir().expect("workdir");

    Engine::new(
        EngineConfig::new("ice-narrow-1").with_workdir(workdir.path().join("wal1")),
        one_batch(vec![json!({"id": 1, "note": "present"})]),
        Shell::from_value(doc.clone()).expect("valid"),
    )
    .run()
    .await
    .expect("run 1 settles");

    Engine::new(
        EngineConfig::new("ice-narrow-2").with_workdir(workdir.path().join("wal2")),
        one_batch(vec![json!({"id": 2})]),
        Shell::from_value(doc).expect("valid"),
    )
    .run()
    .await
    .expect("run 2 settles despite the narrowed stream");

    let summaries = fixture.snapshot_summaries(namespace, "events").await;
    assert_eq!(summaries.len(), 2, "{summaries:?}");
}

/// Replace refuses LIVE with the frozen spelling — before any table
/// is touched.
#[tokio::test]
async fn replace_is_refused_against_the_live_catalog() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let doc = fixture.doc("replace_v2");
    let workdir = tempfile::tempdir().expect("workdir");
    let err = Engine::new(
        EngineConfig::new("ice-replace-v2")
            .with_workdir(workdir.path().join("wal"))
            .with_write_mode(WriteMode::Replace),
        one_batch(vec![json!({"id": 1})]),
        Shell::from_value(doc).expect("valid"),
    )
    .run()
    .await
    .expect_err("Replace must refuse");
    let text = format!("{err}");
    assert!(
        text.contains("Replace is not supported")
            && text.contains("use Append, or a SQL destination for replace semantics"),
        "{text}"
    );
}
