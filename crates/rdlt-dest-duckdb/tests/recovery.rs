//! Crash-recovery regression (feature 003 review): the Replace-mode
//! truncate-once guard must be DURABLE — a crash between commits of one load
//! recovers into a fresh session, which must NOT re-truncate rows an earlier
//! commit already published (the parquet twin was the feature-002 review's
//! confirmed data-loss finding; DuckDB carried the same latent pattern).

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use rdlt_connector::core::{
    ColumnDef, ColumnType, CommitCounters, CommitMeta, LoadId, LogicalType, PipelineId, Provenance,
    StateDoc, TableName, TableSchema, WriteMode,
};
use rdlt_connector::{Destination, OpenCtx};
use rdlt_dest_duckdb::DuckDb;

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

#[tokio::test]
async fn replace_recovery_session_keeps_prior_commits_of_same_load() {
    let dir = tempfile::tempdir().expect("tempdir");
    let dest = DuckDb::open(dir.path().join("out.duckdb")).expect("open dest");
    let pipeline = PipelineId::new("p1");
    let load = LoadId::new("load-a");
    let table = TableName::new("events");

    // Commit #1 lands durably.
    let mut s1 = dest
        .open(OpenCtx::new(pipeline.clone(), load.clone()))
        .await
        .expect("open s1");
    s1.ensure_table(&schema_for("events"), &WriteMode::Replace)
        .await
        .expect("ensure");
    s1.write(&table, batch_of(&[1, 2, 3])).await.expect("write");
    s1.commit(meta_for(&pipeline, &load, 1))
        .await
        .expect("commit 1");
    assert_eq!(dest.count_rows("events").expect("count"), 3);

    // Crash before commit #2's receipt: recovery opens a FRESH session with
    // the SAME load id and replays only the uncommitted tail.
    let mut s2 = dest
        .open(OpenCtx::new(pipeline.clone(), load.clone()))
        .await
        .expect("open recovery session");
    s2.ensure_table(&schema_for("events"), &WriteMode::Replace)
        .await
        .expect("ensure again");
    s2.write(&table, batch_of(&[4, 5]))
        .await
        .expect("write tail");
    s2.commit(meta_for(&pipeline, &load, 2))
        .await
        .expect("commit 2");

    assert_eq!(
        dest.count_rows("events").expect("count"),
        5,
        "commit #1's published rows must survive recovery (durable Replace guard)"
    );

    // A genuinely NEW load still replaces from scratch.
    let load_b = LoadId::new("load-b");
    let mut s3 = dest
        .open(OpenCtx::new(pipeline.clone(), load_b.clone()))
        .await
        .expect("open next load");
    s3.ensure_table(&schema_for("events"), &WriteMode::Replace)
        .await
        .expect("ensure");
    s3.write(&table, batch_of(&[9])).await.expect("write");
    s3.commit(meta_for(&pipeline, &load_b, 1))
        .await
        .expect("commit");
    assert_eq!(
        dest.count_rows("events").expect("count"),
        1,
        "a new load's first commit still truncates Replace tables"
    );
}
