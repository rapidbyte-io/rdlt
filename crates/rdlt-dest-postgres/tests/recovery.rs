//! Crash-recovery regression (feature 003 review): the Replace-mode
//! truncate-once guard must be DURABLE across sessions — the parquet twin was
//! the feature-002 review's confirmed data-loss finding; Postgres carried the
//! same latent in-memory pattern (never crash-swept until now).

use std::collections::BTreeMap;

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use rdlt_connector::core::{
    ColumnDef, ColumnType, CommitCounters, CommitMeta, LoadId, LogicalType, PipelineId, Provenance,
    StateDoc, TableName, TableSchema, WriteMode,
};
use rdlt_connector::{Destination, OpenCtx};
use rdlt_dest_postgres::Postgres;
use testcontainers_modules::postgres::Postgres as PostgresImage;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

async fn count(conn: &str, dataset: &str, table: &str) -> u64 {
    let (client, connection) = tokio_postgres::connect(conn, tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    match client
        .query_one(
            &format!("SELECT count(*) FROM \"{dataset}\".\"{table}\""),
            &[],
        )
        .await
    {
        Ok(row) => row.get::<_, i64>(0) as u64,
        Err(_) => 0,
    }
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

#[tokio::test(flavor = "multi_thread")]
async fn replace_recovery_session_keeps_prior_commits_of_same_load() {
    let container = PostgresImage::default()
        .with_tag("16-alpine")
        .start()
        .await
        .expect("start postgres container (needs docker/podman)");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("mapped port");
    let conn =
        format!("host=127.0.0.1 port={port} user=postgres password=postgres dbname=postgres");
    let dest = Postgres::connect(&conn).dataset("rec");
    let pipeline = PipelineId::new("p1");
    let load = LoadId::new("load-a");
    let schema = TableSchema {
        table: TableName::new("events"),
        parent: None,
        columns: vec![ColumnDef {
            name: "id".into(),
            ty: ColumnType::scalar(LogicalType::Int64),
            nullable: false,
            provenance: Provenance::Inferred,
        }],
    };
    let batch_of = |ids: &[i64]| {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(ids.to_vec()))],
        )
        .expect("batch")
    };
    let batch1 = batch_of(&[1, 2, 3]);
    let batch2 = batch_of(&[4, 5]);
    let table = TableName::new("events");

    let mut s1 = dest
        .open(OpenCtx::new(pipeline.clone(), load.clone()))
        .await
        .expect("open s1");
    s1.ensure_table(&schema, &WriteMode::Replace)
        .await
        .expect("ensure");
    s1.write(&table, batch1).await.expect("write");
    s1.commit(meta_for(&pipeline, &load, 1))
        .await
        .expect("commit 1");
    assert_eq!(count(&conn, "rec", "events").await, 3);

    // Crash before commit #2's receipt: fresh session, same load.
    let mut s2 = dest
        .open(OpenCtx::new(pipeline.clone(), load.clone()))
        .await
        .expect("open recovery session");
    s2.ensure_table(&schema, &WriteMode::Replace)
        .await
        .expect("ensure again");
    s2.write(&table, batch2).await.expect("write tail");
    s2.commit(meta_for(&pipeline, &load, 2))
        .await
        .expect("commit 2");

    assert_eq!(
        count(&conn, "rec", "events").await,
        5,
        "commit #1's published rows must survive recovery (durable Replace guard)"
    );
}
