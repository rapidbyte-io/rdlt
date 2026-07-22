//! T006 exactly-once + state cells (contract ID2): replayed commits
//! publish NOTHING, incremental runs resume from the catalog-persisted
//! state doc, and receipts are readable from raw snapshot summaries
//! alone. Live against Polaris + RUSTFS, skip-not-fail.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use common::CatalogFixture;
use rdlt_connector::core::{
    ColumnDef, ColumnType, CommitCounters, CommitMeta, Cursor, LoadId, LogicalType, PipelineId,
    Provenance, StateDoc, TableName, TableSchema, WriteMode,
};
use rdlt_connector::{Destination, OpenCtx};
use rdlt_connector_iceberg::IcebergDest;
use serde_json::json;

fn schema_for(table: &str) -> TableSchema {
    TableSchema {
        table: TableName::new(table),
        parent: None,
        columns: vec![ColumnDef {
            name: "id".into(),
            ty: ColumnType::scalar(LogicalType::Int64),
            nullable: false,
            provenance: Provenance::Inferred,
        }],
    }
}

fn batch_of(ids: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(ids.to_vec()))],
    )
    .expect("batch")
}

fn meta_for(pipeline: &PipelineId, load: &LoadId, seq: u64) -> CommitMeta {
    CommitMeta {
        load_id: load.clone(),
        commit_seq: seq,
        state: StateDoc {
            format_version: 1,
            pipeline: pipeline.clone(),
            cursors: BTreeMap::new(),
            schema_hashes: BTreeMap::new(),
            last_commit: None,
            engine_version: "test".into(),
        },
        counters: CommitCounters::default(),
    }
}

/// A crash-recovery replay — a FRESH session re-staging and
/// re-committing an identity that already landed — publishes NOTHING:
/// snapshot count unchanged, no duplicate rows referenced.
#[tokio::test(flavor = "multi_thread")]
async fn replayed_commit_publishes_nothing() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let dest = IcebergDest::from_config(fixture.config("replay")).expect("dest");
    let pipeline = PipelineId::new("replay-pipe");
    let load = LoadId::new("load-a");
    let table = TableName::new("events");

    // Commit #1 lands.
    let mut s1 = dest
        .open(OpenCtx::new(pipeline.clone(), load.clone()))
        .await
        .expect("open s1");
    s1.ensure_table(&schema_for("events"), &WriteMode::Append)
        .await
        .expect("ensure");
    s1.write(&table, batch_of(&[1, 2, 3])).await.expect("write");
    s1.commit(meta_for(&pipeline, &load, 1))
        .await
        .expect("commit 1");
    let after_first = fixture.snapshot_summaries("replay", "events").await;
    assert_eq!(after_first.len(), 1);
    assert_eq!(after_first[0]["added-records"], "3");

    // Crash-recovery replay: fresh session, SAME (load, seq), same rows.
    let mut s2 = dest
        .open(OpenCtx::new(pipeline.clone(), load.clone()))
        .await
        .expect("open replay session");
    s2.ensure_table(&schema_for("events"), &WriteMode::Append)
        .await
        .expect("ensure again");
    s2.write(&table, batch_of(&[1, 2, 3]))
        .await
        .expect("write replayed rows");
    let receipt = s2
        .commit(meta_for(&pipeline, &load, 1))
        .await
        .expect("replayed commit succeeds (converges, not errors)");
    assert_eq!(receipt.commit_seq, 1);

    let after_replay = fixture.snapshot_summaries("replay", "events").await;
    assert_eq!(
        after_replay.len(),
        1,
        "the replayed identity must publish no new snapshot"
    );

    // A genuinely NEW commit still lands on top.
    let mut s3 = dest
        .open(OpenCtx::new(pipeline.clone(), load.clone()))
        .await
        .expect("open s3");
    s3.ensure_table(&schema_for("events"), &WriteMode::Append)
        .await
        .expect("ensure");
    s3.write(&table, batch_of(&[4])).await.expect("write");
    s3.commit(meta_for(&pipeline, &load, 2))
        .await
        .expect("commit 2");
    let after_second = fixture.snapshot_summaries("replay", "events").await;
    assert_eq!(after_second.len(), 2, "the new identity appends normally");
    assert_eq!(after_second[1]["added-records"], "1");
}

/// A second engine run against the same catalog resumes from the
/// state doc persisted on the `_rdlt_state` marker table: the source
/// is handed the committed cursor and re-reads nothing.
#[tokio::test(flavor = "multi_thread")]
async fn incremental_run_resumes_from_catalog_state() {
    use rdlt_connector::StreamSpec;
    use rdlt_engine::{Engine, EngineConfig};
    use rdlt_testkit::{MemoryBatch, MemorySource, MemoryStream};

    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let batches = || {
        vec![
            MemoryBatch::new(vec![json!({"seq": 1}), json!({"seq": 2})]).with_checkpoint(2),
            MemoryBatch::new(vec![json!({"seq": 3})]).with_checkpoint(3),
        ]
    };
    let source_for =
        |batches| MemorySource::new(vec![MemoryStream::new(StreamSpec::new("events"), batches)]);

    // Run 1: fresh — loads everything.
    let source1 = source_for(batches());
    let log1 = source1.since_log();
    let dest = IcebergDest::from_config(fixture.config("resume")).expect("dest");
    let report1 = Engine::new(EngineConfig::new("ice-resume"), source1, dest.clone())
        .run()
        .await
        .expect("run 1");
    assert_eq!(report1.total_rows(), 3);
    assert_eq!(log1.lock().unwrap()[0].1, None, "fresh run gets no cursor");
    let after_run1 = fixture.snapshot_summaries("resume", "events").await;
    let snapshots_run1 = after_run1.len();

    // Run 2: fresh source instance, same catalog — the engine must read
    // the state doc back from the marker table and pass cursor 3.
    let source2 = source_for(batches());
    let log2 = source2.since_log();
    let report2 = Engine::new(EngineConfig::new("ice-resume"), source2, dest)
        .run()
        .await
        .expect("run 2");
    assert_eq!(
        log2.lock().unwrap()[0].1,
        Some(Cursor::new(3)),
        "run 2 must resume from the catalog-persisted cursor"
    );
    assert_eq!(report2.total_rows(), 0, "no re-read of completed ranges");
    let after_run2 = fixture.snapshot_summaries("resume", "events").await;
    assert_eq!(
        after_run2.len(),
        snapshots_run1,
        "an all-caught-up run publishes no snapshot"
    );
}
