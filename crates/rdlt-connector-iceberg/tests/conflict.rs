//! T007 live conflict cell (contract ID3): two independent writers
//! hammering the SAME table concurrently — every commit lands, nobody's
//! snapshot is lost. (The deterministic retry/exhaustion pins live as
//! unit tests against a mock conflicting catalog in `dest/commit.rs` —
//! a live competitor cannot be timed into the load→commit window
//! reliably.) Skip-not-fail without a container runtime.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use common::CatalogFixture;
use rdlt_connector::core::{
    ColumnDef, ColumnType, CommitCounters, CommitMeta, LoadId, LogicalType, PipelineId, Provenance,
    StateDoc, TableName, TableSchema, WriteMode,
};
use rdlt_connector::{DestError, Destination, OpenCtx};
use rdlt_connector_iceberg::IcebergDest;

const COMMITS_PER_WRITER: u64 = 4;

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

async fn run_writer(dest: IcebergDest, pipeline: &str, load: &str) -> Result<(), DestError> {
    let pipeline = PipelineId::new(pipeline);
    let load = LoadId::new(load);
    let table = TableName::new("contested");
    let mut session = dest
        .open(OpenCtx::new(pipeline.clone(), load.clone()))
        .await?;
    session
        .ensure_table(&schema_for("contested"), &WriteMode::Append)
        .await?;
    for seq in 1..=COMMITS_PER_WRITER {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![seq as i64]))],
        )
        .expect("batch");
        session.write(&table, batch).await?;
        session
            .commit(CommitMeta {
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
            })
            .await?;
    }
    Ok(())
}

/// Two writers, one table, interleaved commits: the bounded retry
/// (refresh → rebuild → commit) must land every commit without ever
/// dropping the competitor's snapshots from history.
#[tokio::test(flavor = "multi_thread")]
async fn competing_writers_lose_no_snapshots() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let dest_a = IcebergDest::from_config(fixture.config("contest")).expect("dest a");
    let dest_b = IcebergDest::from_config(fixture.config("contest")).expect("dest b");

    let (a, b) = tokio::join!(
        run_writer(dest_a, "writer-a", "load-a"),
        run_writer(dest_b, "writer-b", "load-b"),
    );
    a.expect("writer a lands all commits");
    b.expect("writer b lands all commits");

    let snapshots = fixture.snapshot_summaries("contest", "contested").await;
    assert_eq!(
        snapshots.len() as u64,
        2 * COMMITS_PER_WRITER,
        "every commit from both writers is a snapshot — none lost"
    );
    for load in ["load-a", "load-b"] {
        let seqs: Vec<&str> = snapshots
            .iter()
            .filter(|s| s["rdlt.load-id"] == load)
            .map(|s| s["rdlt.commit-seq"].as_str())
            .collect();
        assert_eq!(
            seqs.len() as u64,
            COMMITS_PER_WRITER,
            "{load}: all identities present"
        );
    }
    let total: u64 = snapshots
        .iter()
        .map(|s| s["added-records"].parse::<u64>().expect("count"))
        .sum();
    assert_eq!(total, 2 * COMMITS_PER_WRITER, "one row per commit, exact");
}
