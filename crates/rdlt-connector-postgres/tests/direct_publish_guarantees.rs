//! Redelivery and Replace-clear guarantees on the DIRECT publish path.
//!
//! Both tests here were written from code-review findings against feature 019
//! US5, and both reproduced real defects. The first is fixed and stays as its
//! regression pin. The second is a CONFIRMED, UNFIXED defect and is
//! `#[ignore]`d with its reason — visible and runnable, rather than deleted
//! and forgotten.
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use rdlt_connector::core::{
    ColumnDef, ColumnType, CommitCounters, LoadId, LogicalType, PipelineId, Provenance, StateDoc,
    TableName, TableSchema,
};
use rdlt_connector::{CommitMeta, Destination, OpenCtx, WriteMode};
use rdlt_testkit::containers::PgFixture;
use std::sync::Arc;

fn schema(t: &str) -> TableSchema {
    TableSchema {
        table: TableName::new(t),
        parent: None,
        columns: vec![ColumnDef {
            name: "id".into(),
            column_type: ColumnType::Scalar {
                scalar: LogicalType::Int64,
            },
            nullable: false,
            provenance: Provenance::Hinted,
        }],
    }
}
fn batch(ids: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(ids.to_vec()))],
    )
    .expect("batch")
}
async fn count(conn: &str, t: &str) -> i64 {
    let (c, connection) = tokio_postgres::connect(conn, tokio_postgres::NoTls)
        .await
        .expect("conn");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    c.query_one(&format!("SELECT count(*) FROM {t}"), &[])
        .await
        .expect("count")
        .get(0)
}

/// A redelivered commit unit must not double its rows.
///
/// Writing straight into the target means a replayed unit has ALREADY put its
/// rows there by the time `commit` discovers the receipt exists. Committing
/// would land them twice; the unit is rolled back instead.
///
/// The staged path got this structurally — redelivered rows sat in a stage the
/// replay branch truncated without publishing — so nothing tested it, and the
/// crash sweep passed 23/23 while this was broken.
#[tokio::test(flavor = "multi_thread")]
async fn probe_redelivered_unit_does_not_duplicate() {
    let Some(pg) = PgFixture::start().await else {
        return;
    };
    let conn = pg.conn.clone();
    let dest = rdlt_connector_postgres::dest::Postgres::connect(&conn).dataset("rp1");
    let pipeline = PipelineId::new("rp1");
    let meta = |seq: u64| CommitMeta {
        load_id: LoadId::new("rp1-load"),
        commit_seq: seq,
        state: StateDoc::new(pipeline.clone(), env!("CARGO_PKG_VERSION")),
        counters: CommitCounters::default(),
    };
    let mut s = dest
        .open(OpenCtx::new(pipeline.clone(), LoadId::new("rp1-load")))
        .await
        .expect("open");
    s.ensure_table(&schema("t"), &WriteMode::Append)
        .await
        .expect("ensure");
    s.write(&TableName::new("t"), batch(&[1, 2, 3]))
        .await
        .expect("write");
    s.commit(meta(0)).await.expect("commit");
    assert_eq!(count(&conn, "rp1.t").await, 3, "first delivery");

    // The redelivery window: the client died without learning the outcome, so
    // the same (load_id, commit_seq) is delivered again.
    s.write(&TableName::new("t"), batch(&[1, 2, 3]))
        .await
        .expect("re-write");
    s.commit(meta(0)).await.expect("replay commit");
    assert_eq!(
        count(&conn, "rp1.t").await,
        3,
        "REDELIVERY MUST NOT DUPLICATE"
    );
}

/// A Replace target whose first rows arrive after the load's first commit
/// unit must still be cleared.
///
/// `prepare_target` clears at the FIRST WRITE of a target, guarded by
/// `load_committed_before` so a crash-recovery session cannot re-truncate rows
/// an earlier unit published. That guard is per-LOAD, but the question it
/// answers has to be per-(LOAD, TARGET): a table registered by `ensure_table`
/// in unit 1 and first written in unit 2 finds the guard already set, skips
/// its TRUNCATE, and appends to the previous load's rows.
///
/// The staged path did not have this hole — `commit_script` emitted
/// `ClearTarget` for every Replace table at unit 1's publish regardless of
/// whether it had staged anything.
///
/// Fixed by making the guard per-(load, target) and durable: the executor
/// seeds it from `_rdlt_cleared` before planning, and records the clear in the
/// same transaction as the TRUNCATE, so a rolled-back clear leaves no record
/// claiming it happened.
#[tokio::test(flavor = "multi_thread")]
async fn probe_replace_first_written_in_unit_two_is_cleared() {
    let Some(pg) = PgFixture::start().await else {
        return;
    };
    let conn = pg.conn.clone();
    let dest = rdlt_connector_postgres::dest::Postgres::connect(&conn).dataset("rp2");
    let pipeline = PipelineId::new("rp2");
    let mk = |load: &str, seq: u64| CommitMeta {
        load_id: LoadId::new(load),
        commit_seq: seq,
        state: StateDoc::new(pipeline.clone(), env!("CARGO_PKG_VERSION")),
        counters: CommitCounters::default(),
    };
    // Load 1 leaves rows behind.
    let mut s = dest
        .open(OpenCtx::new(pipeline.clone(), LoadId::new("L1")))
        .await
        .expect("open");
    s.ensure_table(&schema("t"), &WriteMode::Replace)
        .await
        .expect("ensure");
    s.write(&TableName::new("t"), batch(&[1, 2, 3]))
        .await
        .expect("write");
    s.commit(mk("L1", 0)).await.expect("commit");
    drop(s);
    assert_eq!(count(&conn, "rp2.t").await, 3, "load 1 landed");

    // Load 2: table T gets NO write in unit 0, which still commits; its first
    // rows arrive in unit 1.
    let mut s = dest
        .open(OpenCtx::new(pipeline.clone(), LoadId::new("L2")))
        .await
        .expect("open");
    s.ensure_table(&schema("t"), &WriteMode::Replace)
        .await
        .expect("ensure");
    s.commit(mk("L2", 0)).await.expect("empty unit commits");
    s.write(&TableName::new("t"), batch(&[9]))
        .await
        .expect("write in unit 1");
    s.commit(mk("L2", 1)).await.expect("commit unit 1");
    assert_eq!(
        count(&conn, "rp2.t").await,
        1,
        "REPLACE MUST CLEAR load 1's rows"
    );
}
