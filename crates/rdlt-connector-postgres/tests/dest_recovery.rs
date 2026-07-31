//! Crash-recovery regression (feature 003 review): the Replace-mode
//! truncate-once guard must be DURABLE across sessions — the parquet twin was
//! the feature-002 review's confirmed data-loss finding; Postgres carried the
//! same latent in-memory pattern (never crash-swept until now).

use rdlt_connector::core::{LoadId, PipelineId, TableName, WriteMode};
use rdlt_connector::{Destination, OpenCtx};
use rdlt_connector_postgres::dest::Postgres;
use rdlt_connector_postgres::fixtures::PgFixture;
use rdlt_testkit::{batch_of, commit_meta_for, schema_for};

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

#[tokio::test(flavor = "multi_thread")]
async fn replace_recovery_session_keeps_prior_commits_of_same_load() {
    let Some(pg) = PgFixture::start().await else {
        return;
    };
    let conn = pg.conn.clone();
    let dest = Postgres::connect(&conn).dataset("rec");
    let pipeline = PipelineId::new("p1");
    let load = LoadId::new("load-a");
    let schema = schema_for("events");
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
    s1.commit(commit_meta_for(&pipeline, &load, 1))
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
    s2.commit(commit_meta_for(&pipeline, &load, 2))
        .await
        .expect("commit 2");

    assert_eq!(
        count(&conn, "rec", "events").await,
        5,
        "commit #1's published rows must survive recovery (durable Replace guard)"
    );
}
