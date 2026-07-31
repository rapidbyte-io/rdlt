//! Native type fidelity: NUMERIC(p,s)/JSONB/UUID/NOT NULL land as native
//! columns with zero user configuration (contract dest-types.md).

use std::sync::Arc;

use arrow_array::{Decimal128Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use rdlt_connector::core::{
    ColumnDef, ColumnType, CommitCounters, LoadId, LogicalType, PipelineId, Provenance, StateDoc,
    TableName, TableSchema,
};
use rdlt_connector::{CommitMeta, Destination as _, OpenCtx, WriteMode};
use rdlt_testkit::TableProbe as _;

use super::{PgFixture, PgProbe};

fn col(name: &str, scalar: LogicalType, nullable: bool) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        column_type: ColumnType::Scalar { scalar },
        nullable,
        provenance: Provenance::Hinted,
    }
}

fn fidelity_schema() -> TableSchema {
    TableSchema {
        table: TableName::new("fidelity"),
        parent: None,
        columns: vec![
            col("id", LogicalType::Int64, false),
            col(
                "amount",
                LogicalType::Decimal {
                    precision: 12,
                    scale: 4,
                },
                true,
            ),
            col("doc", LogicalType::Json, true),
            col("uid", LogicalType::Uuid, true),
        ],
    }
}

type FidelityRow<'a> = (i64, Option<i128>, Option<&'a str>, Option<&'a str>);

fn fidelity_batch(rows: &[FidelityRow<'_>]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("amount", DataType::Decimal128(12, 4), true),
            Field::new("doc", DataType::Utf8, true),
            Field::new("uid", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(
                Decimal128Array::from(rows.iter().map(|r| r.1).collect::<Vec<_>>())
                    .with_precision_and_scale(12, 4)
                    .expect("decimal shape"),
            ),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.3).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("batch")
}

fn meta(pipeline: &PipelineId, load: &str, seq: u64) -> CommitMeta {
    CommitMeta {
        load_id: LoadId::new(load),
        commit_seq: seq,
        state: StateDoc::new(pipeline.clone(), env!("CARGO_PKG_VERSION")),
        counters: CommitCounters::default(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn native_types_land_with_exact_values() {
    let Some(pg) = PgFixture::start().await else {
        return;
    };
    let conn = pg.conn.clone();
    let dest = rdlt_connector_postgres::dest::Postgres::connect(&conn).dataset("fid");
    let pipeline = PipelineId::new("fid");
    const LOAD: &str = "fid-load";
    let mut session = dest
        .open(OpenCtx::new(pipeline.clone(), LoadId::new(LOAD)))
        .await
        .expect("open");

    let schema = fidelity_schema();
    session
        .ensure_table(&schema, &WriteMode::Append)
        .await
        .expect("ensure");
    session
        .write(
            &schema.table,
            fidelity_batch(&[
                (
                    1,
                    Some(123_456_781_234), // 12345678.1234
                    Some(r#"{"city": "NYC", "zip": 10001}"#),
                    Some("550e8400-e29b-41d4-a716-446655440000"),
                ),
                (
                    2,
                    Some(-5), // -0.0005
                    Some(r#"{"city": "LA"}"#),
                    Some("00000000-0000-0000-0000-000000000001"),
                ),
                (3, None, None, None), // NULLs survive in every native type
            ]),
        )
        .await
        .expect("write");
    session
        .commit(meta(&pipeline, LOAD, 0))
        .await
        .expect("commit");

    let probe = PgProbe {
        conn: conn.clone(),
        schema: "fid".into(),
    };
    assert_eq!(probe.count(&TableName::new("fidelity")).await, 3);

    let (client, connection) = tokio_postgres::connect(&conn, tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    // T1/T2/T3 catalog assertions: the COLUMN TYPES are native.
    let type_of = |name: &'static str| {
        let client = &client;
        async move {
            let t: String = client
                .query_one(
                    "SELECT format_type(atttypid, atttypmod) FROM pg_attribute
                     WHERE attrelid = 'fid.fidelity'::regclass AND attname = $1",
                    &[&name],
                )
                .await
                .expect("catalog")
                .get(0);
            t
        }
    };
    assert_eq!(type_of("amount").await, "numeric(12,4)", "T1");
    assert_eq!(type_of("doc").await, "jsonb", "T2");
    assert_eq!(type_of("uid").await, "uuid", "T3");

    // T4: NOT NULL honored on the target.
    let nullable: bool = client
        .query_one(
            "SELECT attnotnull FROM pg_attribute
             WHERE attrelid = 'fid.fidelity'::regclass AND attname = 'id'",
            &[],
        )
        .await
        .expect("nullability")
        .get(0);
    assert!(nullable, "T4: id declared non-nullable");

    // T1: exact decimal math, zero float involvement.
    let sum: String = client
        .query_one("SELECT SUM(amount)::text FROM fid.fidelity", &[])
        .await
        .expect("sum")
        .get(0);
    assert_eq!(sum, "12345678.1229", "exact NUMERIC sum");

    // T2: native JSON path query.
    let city: String = client
        .query_one("SELECT doc->>'city' FROM fid.fidelity WHERE id = 1", &[])
        .await
        .expect("json path")
        .get(0);
    assert_eq!(city, "NYC");

    // T3: uuid-literal equality join.
    let id: i64 = client
        .query_one(
            "SELECT id FROM fid.fidelity
             WHERE uid = '550e8400-e29b-41d4-a716-446655440000'::uuid",
            &[],
        )
        .await
        .expect("uuid join")
        .get(0);
    assert_eq!(id, 1);

    // NULL row survived in every native type.
    let nulls: i64 = client
        .query_one(
            "SELECT count(*) FROM fid.fidelity
             WHERE id = 3 AND amount IS NULL AND doc IS NULL AND uid IS NULL",
            &[],
        )
        .await
        .expect("nulls")
        .get(0);
    assert_eq!(nulls, 1);
}

/// Review F1 live proof: a 38-digit NUMERIC at a pad-requiring scale —
/// the exact shape whose encoding overflowed pre-review — round-trips
/// exactly through a real server.
#[tokio::test(flavor = "multi_thread")]
async fn extreme_decimal_round_trips_through_the_server() {
    use rdlt_connector::core::TableName;
    let Some(pg) = PgFixture::start().await else {
        return;
    };
    let conn = pg.conn.clone();
    let dest = rdlt_connector_postgres::dest::Postgres::connect(&conn).dataset("wide");
    let pipeline = PipelineId::new("wide");
    const LOAD: &str = "w-load";
    let mut session = dest
        .open(OpenCtx::new(pipeline.clone(), LoadId::new(LOAD)))
        .await
        .expect("open");
    let schema = TableSchema {
        table: TableName::new("wide"),
        parent: None,
        columns: vec![
            col("id", LogicalType::Int64, false),
            col(
                "amount",
                LogicalType::Decimal {
                    precision: 38,
                    scale: 3,
                },
                true,
            ),
        ],
    };
    session
        .ensure_table(&schema, &WriteMode::Append)
        .await
        .expect("ensure");
    // 38 nines at scale 3 (pad = 1 in base-10000 alignment).
    let value: i128 = 10i128.pow(38) - 1;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("amount", DataType::Decimal128(38, 3), true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1i64])),
            Arc::new(
                Decimal128Array::from(vec![Some(value)])
                    .with_precision_and_scale(38, 3)
                    .expect("decimal shape"),
            ),
        ],
    )
    .expect("batch");
    session.write(&schema.table, batch).await.expect("write");
    session
        .commit(meta(&pipeline, LOAD, 0))
        .await
        .expect("commit");

    let (client, connection) = tokio_postgres::connect(&conn, tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let text: String = client
        .query_one("SELECT amount::text FROM wide.wide WHERE id = 1", &[])
        .await
        .expect("value")
        .get(0);
    assert_eq!(
        text, "99999999999999999999999999999999999.999",
        "38-digit value at scale 3 lands exactly"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn rejected_documents_and_uuids_fail_typed_naming_the_column() {
    let Some(pg) = PgFixture::start().await else {
        return;
    };
    let conn = pg.conn.clone();
    let dest = rdlt_connector_postgres::dest::Postgres::connect(&conn).dataset("fidbad");
    let pipeline = PipelineId::new("fidbad");
    const LOAD: &str = "fb-load";
    let mut session = dest
        .open(OpenCtx::new(pipeline.clone(), LoadId::new(LOAD)))
        .await
        .expect("open");
    let schema = fidelity_schema();
    session
        .ensure_table(&schema, &WriteMode::Append)
        .await
        .expect("ensure");

    // Non-canonical uuid: OUR typed error, names the column, before COPY.
    let err = session
        .write(
            &schema.table,
            fidelity_batch(&[(1, None, None, Some("not-a-uuid"))]),
        )
        .await
        .expect_err("bad uuid must fail");
    let msg = err.to_string();
    assert!(msg.contains("uid") && msg.contains("not-a-uuid"), "{msg}");

    // JSONB-rejected document (NUL escape): the SERVER refuses it and the
    // surfaced error carries its message + SQLSTATE.
    let nul_doc = "{\"k\": \"\\u0000\"}".to_string();
    let err = session
        .write(
            &schema.table,
            fidelity_batch(&[(1, None, Some(&nul_doc), None)]),
        )
        .await
        .expect_err("NUL escape must be rejected by jsonb");
    // Review F5: a poisoned document is PERMANENT (never retried) and
    // the server's CONTEXT line names the column.
    let dbg = format!("{err:?}");
    assert!(dbg.starts_with("Fatal"), "data error must be fatal: {dbg}");
    let msg = err.to_string();
    assert!(
        msg.contains("Unicode escape") && msg.contains("SQLSTATE") && msg.contains("doc"),
        "server message + SQLSTATE + column context: {msg}"
    );
}

/// ANY forced db failure carries the server message + SQLSTATE.
#[tokio::test(flavor = "multi_thread")]
async fn forced_db_failure_surfaces_server_message_and_sqlstate() {
    let Some(pg) = PgFixture::start().await else {
        return;
    };
    let conn = pg.conn.clone();
    let dest = rdlt_connector_postgres::dest::Postgres::connect(&conn).dataset("f6");
    let pipeline = PipelineId::new("f6");
    const LOAD: &str = "f6-load";
    let mut session = dest
        .open(OpenCtx::new(pipeline.clone(), LoadId::new(LOAD)))
        .await
        .expect("open");
    let schema = fidelity_schema();
    session
        .ensure_table(&schema, &WriteMode::Append)
        .await
        .expect("ensure");
    // NOT NULL violation. Append rows go STRAIGHT into the target, so the
    // constraint is enforced by the COPY itself and the failure surfaces
    // at `write` — at the offending row, which the server names — rather
    // than a whole batch later at publish. It used to surface at publish
    // because the row first landed in a nullable stage table. What the rule
    // requires is unchanged either way: the server's message and SQLSTATE
    // reach the caller, never a bare "db error".
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)])),
        vec![Arc::new(Int64Array::from(vec![None::<i64>]))],
    )
    .expect("batch");
    let err = session
        .write(&schema.table, batch)
        .await
        .expect_err("NOT NULL violation on the direct write");
    let msg = err.to_string();
    assert!(
        msg.contains("null value") && msg.contains("SQLSTATE 23502"),
        "F6: server message + SQLSTATE, never bare db error: {msg}"
    );
    // The failed unit rolled back, so the session is still usable — the
    // engine may retry a transient failure on this same session.
    assert!(
        session.read_state(&pipeline).await.is_ok(),
        "a failed unit must leave the connection out of its aborted state"
    );
}
