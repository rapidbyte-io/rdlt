//! T059: Postgres destination via testcontainers — destination conformance suite plus
//! an end-to-end engine run with flatten lowering and merge.
//!
//! Requires a container runtime; each test spins up postgres:16-alpine.

use async_trait::async_trait;
use rdlt_connector_postgres::dest::Postgres;
use rdlt_connector_postgres::fixtures::PgFixture;
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::conformance::dest::verify_destination;
use rdlt_testkit::{MemorySource, TableProbe, assert_conformant};
use serde_json::json;

struct PgProbe {
    conn: String,
    schema: String,
}

#[async_trait]
impl TableProbe for PgProbe {
    async fn count(&self, table: &rdlt_connector::TableName) -> u64 {
        let (client, connection) = tokio_postgres::connect(&self.conn, tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let sql = format!(
            "SELECT count(*) FROM \"{}\".\"{}\"",
            self.schema,
            table.as_str().replace('"', "")
        );
        match client.query_one(&sql, &[]).await {
            Ok(row) => row.get::<_, i64>(0) as u64,
            Err(_) => 0, // missing table counts as empty (probe contract)
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn postgres_destination_is_conformant() {
    let Some(pg) = PgFixture::start().await else {
        return;
    };
    let conn = pg.conn.clone();
    let dest = Postgres::connect(&conn).dataset("raw");
    let probe = PgProbe {
        conn: conn.clone(),
        schema: "raw".into(),
    };
    assert_conformant(verify_destination(&dest, &probe).await);
}

#[tokio::test(flavor = "multi_thread")]
async fn end_to_end_flattened_sync_into_postgres() {
    let Some(pg) = PgFixture::start().await else {
        return;
    };
    let conn = pg.conn.clone();
    let dest = Postgres::connect(&conn).dataset("raw");

    let source = MemorySource::single_stream(
        rdlt_connector::StreamSpec::new("users").with_primary_key(["id"]),
        vec![
            json!({"id": 1, "name": "ada", "profile": {"city": "NYC"},
                   "tags": [{"label": "x"}, {"label": "y"}]}),
            json!({"id": 2, "name": "grace", "profile": {"city": "LA"}, "tags": []}),
        ],
    );
    let mut config = EngineConfig::new("pg-e2e");
    config = config.with_write_mode(rdlt_connector::WriteMode::Merge {
        key: vec!["id".into()],
    });
    let report = Engine::new(config, source, dest.clone())
        .run()
        .await
        .expect("run");
    assert_eq!(report.total_rows(), 4);

    let probe = PgProbe {
        conn: conn.clone(),
        schema: "raw".into(),
    };
    assert_eq!(
        probe.count(&rdlt_connector::TableName::new("users")).await,
        2
    );
    assert_eq!(
        probe
            .count(&rdlt_connector::TableName::new("users__tags"))
            .await,
        2
    );

    // Flatten lowering: nested field arrived as a flat, prefixed column.
    let (client, connection) = tokio_postgres::connect(&conn, tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let city: String = client
        .query_one("SELECT profile__city FROM raw.users WHERE id = 1", &[])
        .await
        .expect("flattened column query")
        .get(0);
    assert_eq!(city, "NYC");

    // Merge run 2: user 1 updated with one new tag — subtree replaced.
    let source = MemorySource::single_stream(
        rdlt_connector::StreamSpec::new("users").with_primary_key(["id"]),
        vec![
            json!({"id": 1, "name": "ada lovelace", "profile": {"city": "London"},
                    "tags": [{"label": "z"}]}),
        ],
    );
    let mut config = EngineConfig::new("pg-e2e");
    config = config.with_write_mode(rdlt_connector::WriteMode::Merge {
        key: vec!["id".into()],
    });
    Engine::new(config, source, dest.clone())
        .run()
        .await
        .expect("run 2");

    assert_eq!(
        probe.count(&rdlt_connector::TableName::new("users")).await,
        2
    );
    let labels: Vec<String> = client
        .query("SELECT label FROM raw.users__tags ORDER BY label", &[])
        .await
        .expect("labels")
        .into_iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(
        labels,
        vec!["z"],
        "x/y subtree replaced; grace had no children"
    );
}

/// Feature 006 (merge-structured.md): keyed STRUCTURED merge — no `_rdlt_id`
/// exists, the declared key drives delete+insert, update-heavy runs converge.
#[tokio::test(flavor = "multi_thread")]
async fn keyed_structured_merge_into_postgres() {
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use rdlt_connector::{ConnectorSpec, ReadRequest, Source, SourceError, StreamSpec};

    struct KeyedArrowSource {
        batch: RecordBatch,
    }

    #[async_trait]
    impl Source for KeyedArrowSource {
        fn spec(&self) -> ConnectorSpec {
            ConnectorSpec::new("keyed-arrow-test", "0.0.0")
        }

        async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
            Ok(vec![
                StreamSpec::new("metrics")
                    .with_structured()
                    .with_primary_key(["id"]),
            ])
        }

        async fn read(&self, mut req: ReadRequest) -> Result<(), SourceError> {
            let _ = req.out.arrow(self.batch.clone()).await;
            Ok(())
        }
    }

    fn batch(ids: &[i64], names: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
            ],
        )
        .expect("batch")
    }

    let Some(pg) = PgFixture::start().await else {
        return;
    };
    let conn = pg.conn.clone();
    let dest = Postgres::connect(&conn).dataset("raw");
    let merge_config = || {
        let mut config = EngineConfig::new("pg-kmerge");
        config = config.with_write_mode(rdlt_connector::WriteMode::Merge {
            key: vec!["id".into()],
        });
        config
    };

    let source = KeyedArrowSource {
        batch: batch(&[1, 2], &["a", "b"]),
    };
    Engine::new(merge_config(), source, dest.clone())
        .run()
        .await
        .expect("keyed merge run 1");

    // Run 2 updates key 2 and adds key 3.
    let source = KeyedArrowSource {
        batch: batch(&[2, 3], &["b2", "c"]),
    };
    Engine::new(merge_config(), source, dest.clone())
        .run()
        .await
        .expect("keyed merge run 2");

    let (client, connection) = tokio_postgres::connect(&conn, tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let count: i64 = client
        .query_one("SELECT count(*) FROM raw.metrics", &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(count, 3, "one row per key after update-heavy run");
    let name: String = client
        .query_one("SELECT name FROM raw.metrics WHERE id = 2", &[])
        .await
        .expect("updated row")
        .get(0);
    assert_eq!(name, "b2", "merge took the updated value");
}

// ---- Feature 008 US1: native type fidelity (contract dest-types.md) ----

mod native_types {
    use std::sync::Arc;

    use arrow_array::{Decimal128Array, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use rdlt_connector::core::{
        ColumnDef, ColumnType, CommitCounters, LoadId, LogicalType, PipelineId, Provenance,
        StateDoc, TableName, TableSchema,
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

    /// F6 regression (SC-007): ANY forced db failure carries message + SQLSTATE.
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
        // because the row first landed in a nullable stage table. What SC-007
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
}

// ---- Feature 008 US2: merge strategies (contract merge-strategies.md) ----

mod strategies {
    use std::sync::Arc;

    use arrow_array::{BooleanArray, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use async_trait::async_trait;
    use rdlt_connector::{ConnectorSpec, Cursor, ReadRequest, Source, SourceError, StreamSpec};
    use rdlt_connector_postgres::dest::{DestOptions, MergeStrategy, Postgres, TableOptions};
    use rdlt_engine::{Engine, EngineConfig};

    use super::PgFixture;

    /// Keyed structured stream with a bool `deleted` flag column.
    struct FlaggedSource {
        batch: RecordBatch,
    }

    #[async_trait]
    impl Source for FlaggedSource {
        fn spec(&self) -> ConnectorSpec {
            ConnectorSpec::new("flagged-test", "0.0.0")
        }

        async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
            Ok(vec![
                StreamSpec::new("events")
                    .with_structured()
                    .with_primary_key(["id"]),
            ])
        }

        async fn read(&self, mut req: ReadRequest) -> Result<(), SourceError> {
            let _ = req.out.arrow(self.batch.clone()).await;
            let _ = req.out.checkpoint(Cursor::new(1u64)).await;
            Ok(())
        }
    }

    fn batch(rows: &[(i64, &str, Option<bool>)]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, true),
                Field::new("deleted", DataType::Boolean, true),
            ])),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
                Arc::new(BooleanArray::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("batch")
    }

    fn upsert_dest(conn: &str, dataset: &str) -> Postgres {
        Postgres::connect(conn)
            .dataset(dataset)
            .options(DestOptions {
                merge_strategy: Some(MergeStrategy::Upsert),
                tables: [(
                    "events".to_string(),
                    TableOptions {
                        hard_delete: Some("deleted".into()),
                        ..TableOptions::default()
                    },
                )]
                .into_iter()
                .collect(),
            })
            .expect("valid options")
    }

    async fn run_merge(dest: Postgres, rows: &[(i64, &str, Option<bool>)]) {
        let mut config = EngineConfig::new("strat");
        config = config.with_write_mode(rdlt_connector::WriteMode::Merge {
            key: vec!["id".into()],
        });
        Engine::new(config, FlaggedSource { batch: batch(rows) }, dest)
            .run()
            .await
            .expect("merge run");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn upsert_converges_and_hard_delete_removes_keys() {
        let Some(pg) = PgFixture::start().await else {
            return;
        };
        let conn = pg.conn.clone();

        // Run 1: three live rows.
        run_merge(
            upsert_dest(&conn, "strat"),
            &[(1, "a", Some(false)), (2, "b", None), (3, "c", Some(false))],
        )
        .await;

        // Run 2 (SC-002/003): key 2 updates in place, key 4 is new, key 1 is
        // FLAGGED deleted, and key 9 is a never-loaded flagged key (no-op).
        let round2: &[(i64, &str, Option<bool>)] = &[
            (1, "gone", Some(true)),
            (2, "b2", None),
            (4, "d", Some(false)),
            (9, "ghost", Some(true)),
        ];
        run_merge(upsert_dest(&conn, "strat"), round2).await;

        let (client, connection) = tokio_postgres::connect(&conn, tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let rows: Vec<(i64, String)> = client
            .query("SELECT id, name FROM strat.events ORDER BY id", &[])
            .await
            .expect("rows")
            .into_iter()
            .map(|r| (r.get(0), r.get(1)))
            .collect();
        assert_eq!(
            rows,
            vec![
                (2, "b2".to_string()),
                (3, "c".to_string()),
                (4, "d".to_string())
            ],
            "updated in place, inserted new, deleted flagged, ghost no-op"
        );

        // Three further re-runs: totals never move (idempotent conflict-update).
        for _ in 0..3 {
            run_merge(upsert_dest(&conn, "strat"), round2).await;
            let n: i64 = client
                .query_one("SELECT count(*) FROM strat.events", &[])
                .await
                .expect("count")
                .get(0);
            assert_eq!(n, 3, "SC-002: totals exactly stable");
        }

        // M3/M5: the unique index the strategy required exists.
        let indexes: i64 = client
            .query_one(
                "SELECT count(*) FROM pg_indexes
                 WHERE schemaname = 'strat' AND tablename = 'events'
                   AND indexname LIKE 'rdlt_ux%'",
                &[],
            )
            .await
            .expect("indexes")
            .get(0);
        assert_eq!(indexes, 1, "unique index auto-ensured");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn duplicate_keys_under_upsert_fail_typed_naming_the_key() {
        let Some(pg) = PgFixture::start().await else {
            return;
        };
        let conn = pg.conn.clone();
        // A table that already violates key uniqueness.
        {
            let (client, connection) = tokio_postgres::connect(&conn, tokio_postgres::NoTls)
                .await
                .expect("connect");
            tokio::spawn(async move {
                let _ = connection.await;
            });
            client
                .batch_execute(
                    "CREATE SCHEMA IF NOT EXISTS dup;
                     CREATE TABLE dup.events (
                         id BIGINT NOT NULL, name TEXT, deleted BOOLEAN);
                     INSERT INTO dup.events VALUES (1, 'x', NULL), (1, 'x-again', NULL);",
                )
                .await
                .expect("dup table");
        }
        let mut config = EngineConfig::new("dup");
        config = config.with_write_mode(rdlt_connector::WriteMode::Merge {
            key: vec!["id".into()],
        });
        let err = Engine::new(
            config,
            FlaggedSource {
                batch: batch(&[(1, "a", None)]),
            },
            upsert_dest(&conn, "dup"),
        )
        .run()
        .await
        .expect_err("duplicate keys must block upsert");
        let msg = err.to_string();
        assert!(
            msg.contains("unique index") && msg.contains("id") && msg.contains("23505"),
            "M3 typed, names the key: {msg}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shredded_upsert_is_rejected_typed_at_ensure() {
        // Review F4 / contract M7 (amended): a keyless shredded stream's
        // _rdlt_id is a content hash — conflict-update can never match an
        // updated row, so upsert on shredded streams is rejected outright.
        use rdlt_testkit::MemorySource;
        use serde_json::json;

        let Some(pg) = PgFixture::start().await else {
            return;
        };
        let conn = pg.conn.clone();
        let dest = Postgres::connect(&conn)
            .dataset("shup")
            .options(DestOptions {
                merge_strategy: Some(MergeStrategy::Upsert),
                ..DestOptions::default()
            })
            .expect("options");
        let mut config = EngineConfig::new("shup");
        config = config.with_write_mode(rdlt_connector::WriteMode::Merge {
            key: vec!["id".into()],
        });
        let source = MemorySource::single_stream(
            rdlt_connector::StreamSpec::new("users").with_primary_key(["id"]),
            vec![json!({"id": 1, "name": "ada"})],
        );
        let err = Engine::new(config, source, dest)
            .run()
            .await
            .expect_err("shredded upsert must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("KEYED structured") && msg.contains("delete_insert"),
            "{msg}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn flagged_then_recreated_root_keeps_its_subtree() {
        // Review F3: the hard-delete flag decision must come from the
        // DEDUPED last-wins row — a root flagged then re-created in the
        // SAME load keeps its row AND its children.
        use rdlt_testkit::MemorySource;
        use serde_json::json;

        let Some(pg) = PgFixture::start().await else {
            return;
        };
        let conn = pg.conn.clone();
        let dest = Postgres::connect(&conn)
            .dataset("recreate")
            .options(DestOptions {
                tables: [(
                    "users".to_string(),
                    TableOptions {
                        hard_delete: Some("deleted".into()),
                        ..TableOptions::default()
                    },
                )]
                .into_iter()
                .collect(),
                ..DestOptions::default()
            })
            .expect("options");
        let mut config = EngineConfig::new("recreate");
        config = config.with_write_mode(rdlt_connector::WriteMode::Merge {
            key: vec!["id".into()],
        });
        // One load, same key twice: flagged first, re-created after (arrival
        // order matters — last wins).
        let source = MemorySource::single_stream(
            rdlt_connector::StreamSpec::new("users").with_primary_key(["id"]),
            vec![
                json!({"id": 1, "name": "ada", "deleted": true, "tags": []}),
                json!({"id": 1, "name": "ada-again", "deleted": null,
                       "tags": [{"label": "back"}]}),
            ],
        );
        Engine::new(config, source, dest).run().await.expect("run");

        let (client, connection) = tokio_postgres::connect(&conn, tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let name: String = client
            .query_one("SELECT name FROM recreate.users WHERE id = 1", &[])
            .await
            .expect("root survives")
            .get(0);
        assert_eq!(name, "ada-again", "last-wins row survives the flag");
        let tags: i64 = client
            .query_one("SELECT count(*) FROM recreate.users__tags", &[])
            .await
            .expect("children")
            .get(0);
        assert_eq!(tags, 1, "the re-created root keeps its subtree");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn child_hard_delete_is_rejected_typed() {
        // Review F6: hard_delete on a child table was silently inert.
        use rdlt_testkit::MemorySource;
        use serde_json::json;

        let Some(pg) = PgFixture::start().await else {
            return;
        };
        let conn = pg.conn.clone();
        let dest = Postgres::connect(&conn)
            .dataset("childhd")
            .options(DestOptions {
                tables: [(
                    "users__tags".to_string(),
                    TableOptions {
                        hard_delete: Some("deleted".into()),
                        ..TableOptions::default()
                    },
                )]
                .into_iter()
                .collect(),
                ..DestOptions::default()
            })
            .expect("options");
        let mut config = EngineConfig::new("childhd");
        config = config.with_write_mode(rdlt_connector::WriteMode::Merge {
            key: vec!["id".into()],
        });
        let source = MemorySource::single_stream(
            rdlt_connector::StreamSpec::new("users").with_primary_key(["id"]),
            vec![json!({"id": 1, "tags": [{"label": "x"}]})],
        );
        let err = Engine::new(config, source, dest)
            .run()
            .await
            .expect_err("child hard_delete must be rejected");
        let msg = err.to_string();
        assert!(msg.contains("ROOT table"), "{msg}");
    }
}

// ---- Feature 010: merge refinements (contract merge-refinements.md) ----

mod refinements {
    use std::sync::Arc;

    use arrow_array::{BooleanArray, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use async_trait::async_trait;
    use rdlt_connector::{ConnectorSpec, Cursor, ReadRequest, Source, SourceError, StreamSpec};
    use rdlt_connector_postgres::dest::{
        DedupSort, DestOptions, MergeStrategy, Postgres, SortOrder, TableOptions,
    };
    use rdlt_engine::{Engine, EngineConfig};

    use super::PgFixture;

    /// (id, day, seq, name, deleted) — id is the identity key, day the
    /// scope column, seq the dedup-sort column.
    type Row = (i64, Option<i64>, Option<i64>, &'static str, Option<bool>);

    fn batch(rows: &[Row]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("day", DataType::Int64, true),
                Field::new("seq", DataType::Int64, true),
                Field::new("name", DataType::Utf8, true),
                Field::new("deleted", DataType::Boolean, true),
            ])),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.3).collect::<Vec<_>>(),
                )),
                Arc::new(BooleanArray::from(
                    rows.iter().map(|r| r.4).collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("batch")
    }

    /// Pushes each batch with its own checkpoint — under the default
    /// commit policy every batch is its OWN COMMIT UNIT (the multi-unit
    /// cells depend on this).
    struct UnitsSource {
        units: Vec<RecordBatch>,
    }

    #[async_trait]
    impl Source for UnitsSource {
        fn spec(&self) -> ConnectorSpec {
            ConnectorSpec::new("refinements-test", "0.0.0")
        }

        async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
            Ok(vec![
                StreamSpec::new("events")
                    .with_structured()
                    .with_primary_key(["id"]),
            ])
        }

        async fn read(&self, mut req: ReadRequest) -> Result<(), SourceError> {
            for (i, unit) in self.units.iter().enumerate() {
                let _ = req.out.arrow(unit.clone()).await;
                let _ = req.out.checkpoint(Cursor::new(i as u64 + 1)).await;
            }
            Ok(())
        }
    }

    #[derive(Clone, Copy, Default)]
    struct Opts {
        strategy: Option<MergeStrategy>,
        dedup: Option<(&'static str, SortOrder)>,
        merge_scope: Option<&'static [&'static str]>,
        hard_delete: bool,
        scd2_retire: bool,
    }

    fn dest(conn: &str, dataset: &str, opts: Opts) -> Postgres {
        Postgres::connect(conn)
            .dataset(dataset)
            .options(DestOptions {
                merge_strategy: opts.strategy,
                tables: [(
                    "events".to_string(),
                    TableOptions {
                        hard_delete: opts.hard_delete.then(|| "deleted".into()),
                        dedup_sort: opts.dedup.map(|(column, order)| DedupSort {
                            column: column.into(),
                            order,
                        }),
                        merge_scope: opts
                            .merge_scope
                            .map(|c| c.iter().map(|s| s.to_string()).collect()),
                        scd2: opts.scd2_retire.then(|| {
                            rdlt_connector_postgres::dest::Scd2Options {
                                absent: rdlt_connector_postgres::dest::AbsentPolicy::Retire,
                                ..rdlt_connector_postgres::dest::Scd2Options::default()
                            }
                        }),
                        ..TableOptions::default()
                    },
                )]
                .into_iter()
                .collect(),
            })
            .expect("valid options")
    }

    async fn run(conn: &str, dataset: &str, opts: Opts, units: Vec<Vec<Row>>) {
        let mut config = EngineConfig::new(format!("mr-{dataset}"));
        config = config.with_write_mode(rdlt_connector::WriteMode::Merge {
            key: vec!["id".into()],
        });
        let units = units.iter().map(|u| batch(u)).collect();
        Engine::new(config, UnitsSource { units }, dest(conn, dataset, opts))
            .run()
            .await
            .expect("merge run");
    }

    async fn scalar(conn: &str, sql: &str) -> i64 {
        let (client, connection) = tokio_postgres::connect(conn, tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.query_one(sql, &[]).await.expect("scalar").get(0)
    }

    async fn text(conn: &str, sql: &str) -> String {
        let (client, connection) = tokio_postgres::connect(conn, tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.query_one(sql, &[]).await.expect("text").get(0)
    }

    /// `(id, day, seq, name)` rows of `<dataset>.events`, id-ordered.
    async fn rows(conn: &str, dataset: &str) -> Vec<(i64, Option<i64>, Option<i64>, String)> {
        let (client, connection) = tokio_postgres::connect(conn, tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .query(
                &format!(
                    "SELECT id, day, seq, name FROM \"{dataset}\".events ORDER BY id, day, seq"
                ),
                &[],
            )
            .await
            .expect("rows")
            .into_iter()
            .map(|r| (r.get(0), r.get(1), r.get(2), r.get::<_, String>(3)))
            .collect()
    }

    async fn run_expect_err(conn: &str, dataset: &str, opts: Opts, units: Vec<Vec<Row>>) -> String {
        let mut config = EngineConfig::new(format!("mr-{dataset}"));
        config = config.with_write_mode(rdlt_connector::WriteMode::Merge {
            key: vec!["id".into()],
        });
        let units = units.iter().map(|u| batch(u)).collect();
        Engine::new(config, UnitsSource { units }, dest(conn, dataset, opts))
            .run()
            .await
            .expect_err("run should fail")
            .to_string()
    }

    // ---- US1: ordered survivor selection (MR1/MR2, SC-001) ----

    #[tokio::test(flavor = "multi_thread")]
    async fn dedup_sort_orders_survivors_not_arrival() {
        let Some(pg) = PgFixture::start().await else {
            return;
        };
        let conn = pg.conn.clone();
        // Wrong arrival order: the newest version arrives FIRST.
        let load: Vec<Vec<Row>> = vec![vec![
            (1, None, Some(5), "newest", None),
            (1, None, Some(3), "older", None),
        ]];

        // desc: greatest seq survives, despite arriving first.
        let desc = Opts {
            dedup: Some(("seq", SortOrder::Desc)),
            ..Opts::default()
        };
        run(&conn, "mr_desc", desc, load.clone()).await;
        assert_eq!(
            rows(&conn, "mr_desc").await,
            vec![(1, None, Some(5), "newest".into())]
        );

        // asc: least seq survives.
        let asc = Opts {
            dedup: Some(("seq", SortOrder::Asc)),
            ..Opts::default()
        };
        run(&conn, "mr_asc", asc, load.clone()).await;
        assert_eq!(
            rows(&conn, "mr_asc").await,
            vec![(1, None, Some(3), "older".into())]
        );

        // FR-002: absent the option, arrival-order last-wins is UNCHANGED.
        run(&conn, "mr_absent", Opts::default(), load.clone()).await;
        assert_eq!(
            rows(&conn, "mr_absent").await,
            vec![(1, None, Some(3), "older".into())]
        );

        // The same desc rule under the UPSERT arm (MR2 — one shared shape).
        let upsert = Opts {
            strategy: Some(MergeStrategy::Upsert),
            dedup: Some(("seq", SortOrder::Desc)),
            ..Opts::default()
        };
        run(&conn, "mr_upsert", upsert, load).await;
        assert_eq!(
            rows(&conn, "mr_upsert").await,
            vec![(1, None, Some(5), "newest".into())]
        );
    }

    // ---- US2: scope-key replacement (MR3–MR5, SC-002) ----

    #[tokio::test(flavor = "multi_thread")]
    async fn merge_scope_replaces_delivered_scopes_only() {
        let Some(pg) = PgFixture::start().await else {
            return;
        };
        let conn = pg.conn.clone();
        let opts = Opts {
            merge_scope: Some(&["day"]),
            ..Opts::default()
        };
        // Seed two scopes.
        run(
            &conn,
            "mr_scope",
            opts,
            vec![vec![
                (1, Some(1), None, "d1-a", None),
                (2, Some(1), None, "d1-b", None),
                (3, Some(2), None, "d2-a", None),
            ]],
        )
        .await;
        // Re-deliver day 1 WITHOUT id 2, with id 1 updated; day 2 untouched.
        run(
            &conn,
            "mr_scope",
            opts,
            vec![vec![(1, Some(1), None, "d1-a2", None)]],
        )
        .await;
        assert_eq!(
            rows(&conn, "mr_scope").await,
            vec![
                (1, Some(1), None, "d1-a2".into()),
                (3, Some(2), None, "d2-a".into()),
            ],
            "undelivered row in the delivered scope is GONE; day 2 intact (US2-AS1)"
        );

        // Review F8: the scope columns get a supporting index automatically
        // (the scope delete must never seq-scan the target).
        assert_eq!(
            scalar(
                &conn,
                "SELECT count(*) FROM pg_indexes WHERE schemaname = 'mr_scope' \
                 AND tablename = 'events' AND indexname LIKE 'rdlt_ix%' \
                 AND indexdef LIKE '%(day)%'",
            )
            .await,
            1,
            "merge_scope scope index auto-ensured"
        );

        // An unseen scope simply lands (US2-AS2); replay is idempotent
        // (US2-AS5).
        let unseen: Vec<Vec<Row>> = vec![vec![(9, Some(9), None, "d9", None)]];
        run(&conn, "mr_scope", opts, unseen.clone()).await;
        run(&conn, "mr_scope", opts, unseen).await;
        assert_eq!(
            rows(&conn, "mr_scope").await,
            vec![
                (1, Some(1), None, "d1-a2".into()),
                (3, Some(2), None, "d2-a".into()),
                (9, Some(9), None, "d9".into()),
            ]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn merge_scope_scope_moves_and_null_scopes() {
        let Some(pg) = PgFixture::start().await else {
            return;
        };
        let conn = pg.conn.clone();
        let opts = Opts {
            merge_scope: Some(&["day"]),
            ..Opts::default()
        };
        run(
            &conn,
            "mr_move",
            opts,
            vec![vec![
                (1, Some(1), None, "in-d1", None),
                (2, None, None, "no-scope", None),
            ]],
        )
        .await;
        // id 1 MOVES from day 1 to day 2 — held once, in its new scope
        // (US2-AS3); the NULL-scope row is untouched by scope deletion and
        // still merges by identity (US2-AS4).
        run(
            &conn,
            "mr_move",
            opts,
            vec![vec![
                (1, Some(2), None, "in-d2", None),
                (2, None, None, "no-scope-v2", None),
            ]],
        )
        .await;
        assert_eq!(
            rows(&conn, "mr_move").await,
            vec![
                (1, Some(2), None, "in-d2".into()),
                (2, None, None, "no-scope-v2".into()),
            ]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn merge_scope_requires_a_single_commit_unit() {
        // The NON-OPTIONAL cell (plan rule; the 008 S6/F2 lesson, sharpened
        // by this feature's own crash sweep): "the batch is the complete
        // truth for its scope" only holds when the scope's truth arrives in
        // ONE commit unit — a crash-resumed load is a NEW load delivering a
        // PARTIAL feed, indistinguishable destination-side from a fresh one.
        // Multi-unit scoped loads are therefore a TYPED error, never silent
        // partial replacement.
        let Some(pg) = PgFixture::start().await else {
            return;
        };
        let conn = pg.conn.clone();
        let opts = Opts {
            merge_scope: Some(&["day"]),
            ..Opts::default()
        };
        run(
            &conn,
            "mr_units",
            opts,
            vec![vec![(99, Some(1), None, "stale", None)]],
        )
        .await;
        let err = run_expect_err(
            &conn,
            "mr_units",
            opts,
            vec![
                vec![(1, Some(1), None, "u1-a", None)],
                vec![(2, Some(1), None, "u2-a", None)],
            ],
        )
        .await;
        assert!(
            err.contains("SINGLE commit unit") && err.contains("commit thresholds"),
            "{err}"
        );

        // Recovery: the same feed in one unit converges.
        run(
            &conn,
            "mr_units",
            opts,
            vec![vec![
                (1, Some(1), None, "u1-a", None),
                (2, Some(1), None, "u2-a", None),
            ]],
        )
        .await;
        assert_eq!(
            rows(&conn, "mr_units").await,
            vec![
                (1, Some(1), None, "u1-a".into()),
                (2, Some(1), None, "u2-a".into()),
            ],
            "stale row gone exactly once; the full-feed retry converges"
        );

        // A later unit with NOTHING staged for the scoped table is fine —
        // multi-unit pipelines where the scoped table fits unit 1 work.
        run(
            &conn,
            "mr_units",
            opts,
            vec![
                vec![
                    (1, Some(1), None, "v2", None),
                    (2, Some(1), None, "u2-a", None),
                ],
                vec![],
            ],
        )
        .await;
        assert_eq!(
            rows(&conn, "mr_units").await,
            vec![
                (1, Some(1), None, "v2".into()),
                (2, Some(1), None, "u2-a".into()),
            ]
        );
    }

    // ---- Review round: per-table single-unit rule + composition pins ----

    #[tokio::test(flavor = "multi_thread")]
    async fn scoped_feed_in_a_later_unit_of_a_multi_unit_load_is_fine() {
        // Review F2: the single-unit rule is PER TABLE — other streams'
        // checkpoints split the LOAD without splitting this table's feed. A
        // leading empty unit (another stream committed first) must not
        // reject the scoped table.
        let Some(pg) = PgFixture::start().await else {
            return;
        };
        let conn = pg.conn.clone();
        let opts = Opts {
            merge_scope: Some(&["day"]),
            ..Opts::default()
        };
        run(
            &conn,
            "mr_lead_empty",
            opts,
            vec![vec![(99, Some(1), None, "stale", None)]],
        )
        .await;
        run(
            &conn,
            "mr_lead_empty",
            opts,
            vec![vec![], vec![(1, Some(1), None, "fresh", None)]],
        )
        .await;
        assert_eq!(
            rows(&conn, "mr_lead_empty").await,
            vec![(1, Some(1), None, "fresh".into())],
            "scope replaced from the table's FIRST STAGED unit, wherever it lands"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn scd2_retire_shares_the_per_table_single_unit_rule() {
        // One rule, both consumers. Retire tolerates units where the table
        // stages nothing (an empty stage must not read as "every key absent"
        // = mass retirement), and rejects a split feed typed.
        let Some(pg) = PgFixture::start().await else {
            return;
        };
        let conn = pg.conn.clone();
        let opts = Opts {
            strategy: Some(MergeStrategy::Scd2),
            scd2_retire: true,
            ..Opts::default()
        };
        let full: Vec<Vec<Row>> =
            vec![vec![(1, None, None, "a", None), (2, None, None, "b", None)]];
        run(&conn, "mr_scd2_units", opts, full.clone()).await;
        // Trailing empty unit: fine — and it retires NOTHING.
        run(&conn, "mr_scd2_units", opts, vec![full[0].clone(), vec![]]).await;
        assert_eq!(
            scalar(
                &conn,
                "SELECT count(*) FROM mr_scd2_units.events WHERE _rdlt_valid_to IS NULL",
            )
            .await,
            2,
            "empty unit retired nothing"
        );
        // Split feed: typed, names the single-unit rule.
        let err = run_expect_err(
            &conn,
            "mr_scd2_units",
            opts,
            vec![
                vec![(1, None, None, "a2", None)],
                vec![(2, None, None, "b2", None)],
            ],
        )
        .await;
        assert!(err.contains("SINGLE commit unit"), "{err}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dedup_sort_survivor_drives_scd2_change_detection() {
        // Review F9 / MR2: the scd2 arm consumes the SAME deduped shape —
        // the ordered survivor decides the active version.
        let Some(pg) = PgFixture::start().await else {
            return;
        };
        let conn = pg.conn.clone();
        let opts = Opts {
            strategy: Some(MergeStrategy::Scd2),
            dedup: Some(("seq", SortOrder::Desc)),
            ..Opts::default()
        };
        // Wrong arrival order: the survivor (seq=5) becomes the active row.
        run(
            &conn,
            "mr_scd2_dedup",
            opts,
            vec![vec![
                (1, None, Some(5), "newest", None),
                (1, None, Some(3), "older", None),
            ]],
        )
        .await;
        assert_eq!(
            text(
                &conn,
                "SELECT name FROM mr_scd2_dedup.events WHERE _rdlt_valid_to IS NULL",
            )
            .await,
            "newest",
            "the ordered survivor is the active version"
        );
        // A later load creates history; the stale-arrival version never
        // polluted it.
        run(
            &conn,
            "mr_scd2_dedup",
            opts,
            vec![vec![(1, None, Some(9), "newer-still", None)]],
        )
        .await;
        assert_eq!(
            scalar(&conn, "SELECT count(*) FROM mr_scd2_dedup.events").await,
            2,
            "exactly two versions ever existed"
        );
    }

    // ---- US3: open-time validation matrix (MR6, SC-004) ----

    #[tokio::test(flavor = "multi_thread")]
    async fn refinement_options_validate_typed_at_open() {
        let Some(pg) = PgFixture::start().await else {
            return;
        };
        let conn = pg.conn.clone();
        let one_row: Vec<Vec<Row>> = vec![vec![(1, Some(1), Some(1), "x", None)]];

        // Nonexistent columns: table AND column named, before any data moves.
        let err = run_expect_err(
            &conn,
            "mr_bad_dedup",
            Opts {
                dedup: Some(("nope", SortOrder::Desc)),
                ..Opts::default()
            },
            one_row.clone(),
        )
        .await;
        assert!(err.contains("`nope`") && err.contains("`events`"), "{err}");

        let err = run_expect_err(
            &conn,
            "mr_bad_scope",
            Opts {
                merge_scope: Some(&["ghost"]),
                ..Opts::default()
            },
            one_row.clone(),
        )
        .await;
        assert!(err.contains("`ghost`") && err.contains("`events`"), "{err}");

        // The hard_delete flag is neither an ordering column nor a scope.
        let err = run_expect_err(
            &conn,
            "mr_flag_dedup",
            Opts {
                dedup: Some(("deleted", SortOrder::Desc)),
                hard_delete: true,
                ..Opts::default()
            },
            one_row.clone(),
        )
        .await;
        assert!(
            err.contains("hard_delete") && err.contains("`deleted`"),
            "{err}"
        );

        let err = run_expect_err(
            &conn,
            "mr_flag_scope",
            Opts {
                merge_scope: Some(&["deleted"]),
                hard_delete: true,
                ..Opts::default()
            },
            one_row.clone(),
        )
        .await;
        assert!(err.contains("not a scope"), "{err}");

        // Review F4: a merge-key column is constant per identity group — the
        // ordering could never pick a survivor; silent no-op forbidden.
        let err = run_expect_err(
            &conn,
            "mr_key_dedup",
            Opts {
                dedup: Some(("id", SortOrder::Desc)),
                ..Opts::default()
            },
            one_row.clone(),
        )
        .await;
        assert!(err.contains("part of the merge key"), "{err}");

        // Review F5: the options under a non-merge write mode are rejected,
        // never silently inert (the 008 F6 lesson).
        let mut config = EngineConfig::new("mr-inert");
        config = config.with_write_mode(rdlt_connector::WriteMode::Append);
        let units = one_row.iter().map(|u| batch(u)).collect();
        let err = Engine::new(
            config,
            UnitsSource { units },
            dest(
                &conn,
                "mr_inert",
                Opts {
                    merge_scope: Some(&["day"]),
                    ..Opts::default()
                },
            ),
        )
        .run()
        .await
        .expect_err("inert option must be rejected")
        .to_string();
        assert!(err.contains("requires the merge write mode"), "{err}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn refinement_options_reject_shredded_streams() {
        use rdlt_testkit::MemorySource;
        use serde_json::json;

        let Some(pg) = PgFixture::start().await else {
            return;
        };
        let conn = pg.conn.clone();
        for (dataset, table_opts, needle) in [
            (
                "mr_sh_dedup",
                TableOptions {
                    dedup_sort: Some(DedupSort {
                        column: "seq".into(),
                        order: SortOrder::Desc,
                    }),
                    ..TableOptions::default()
                },
                "dedup_sort requires a KEYED structured",
            ),
            (
                "mr_sh_scope",
                TableOptions {
                    merge_scope: Some(vec!["day".into()]),
                    ..TableOptions::default()
                },
                "merge_scope requires a KEYED structured",
            ),
        ] {
            let dest = Postgres::connect(&conn)
                .dataset(dataset)
                .options(DestOptions {
                    tables: [("users".to_string(), table_opts)].into_iter().collect(),
                    ..DestOptions::default()
                })
                .expect("options");
            let mut config = EngineConfig::new(dataset);
            config = config.with_write_mode(rdlt_connector::WriteMode::Merge {
                key: vec!["id".into()],
            });
            let source = MemorySource::single_stream(
                rdlt_connector::StreamSpec::new("users").with_primary_key(["id"]),
                vec![json!({"id": 1, "seq": 2, "day": 3})],
            );
            let err = Engine::new(config, source, dest)
                .run()
                .await
                .expect_err("shredded stream must reject the option")
                .to_string();
            assert!(err.contains(needle), "{err}");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn merge_scope_composes_with_upsert_hard_delete_and_dedup_sort() {
        let Some(pg) = PgFixture::start().await else {
            return;
        };
        let conn = pg.conn.clone();
        let opts = Opts {
            strategy: Some(MergeStrategy::Upsert),
            dedup: Some(("seq", SortOrder::Desc)),
            merge_scope: Some(&["day"]),
            hard_delete: true,
            ..Opts::default()
        };
        run(
            &conn,
            "mr_compose",
            opts,
            vec![vec![
                (1, Some(1), Some(1), "keep-old", None),
                (2, Some(1), Some(1), "stale", None),
            ]],
        )
        .await;
        // Day 1 re-delivered: id 2 not re-delivered (scope-dies), id 1
        // arrives twice in wrong order (survivor by seq), id 3 arrives
        // flagged (hard-delete wins over insert).
        run(
            &conn,
            "mr_compose",
            opts,
            vec![vec![
                (1, Some(1), Some(9), "newest", None),
                (1, Some(1), Some(5), "older", None),
                (3, Some(1), Some(1), "kill", Some(true)),
            ]],
        )
        .await;
        assert_eq!(
            rows(&conn, "mr_compose").await,
            vec![(1, Some(1), Some(9), "newest".into())],
            "scope delete + ordered survivor + hard delete compose (MR3/MR1/MR2)"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dedup_sort_survivor_drives_hard_delete() {
        let Some(pg) = PgFixture::start().await else {
            return;
        };
        let conn = pg.conn.clone();
        let opts = Opts {
            dedup: Some(("seq", SortOrder::Desc)),
            hard_delete: true,
            ..Opts::default()
        };
        // Seed the key, then a load where the NEWEST version is flagged
        // deleted but an OLDER unflagged version arrives after it.
        run(
            &conn,
            "mr_flag",
            opts,
            vec![vec![(1, None, Some(1), "seed", None)]],
        )
        .await;
        run(
            &conn,
            "mr_flag",
            opts,
            vec![vec![
                (1, None, Some(5), "kill", Some(true)),
                (1, None, Some(3), "stale", None),
            ]],
        )
        .await;
        assert_eq!(
            rows(&conn, "mr_flag").await,
            vec![],
            "the SURVIVOR's flag decides (US1-AS3) — the row is gone"
        );

        // Under asc the unflagged older version survives instead.
        let asc = Opts {
            dedup: Some(("seq", SortOrder::Asc)),
            hard_delete: true,
            ..Opts::default()
        };
        run(
            &conn,
            "mr_flag_asc",
            asc,
            vec![vec![(1, None, Some(1), "seed", None)]],
        )
        .await;
        run(
            &conn,
            "mr_flag_asc",
            asc,
            vec![vec![
                (1, None, Some(5), "kill", Some(true)),
                (1, None, Some(3), "stale", None),
            ]],
        )
        .await;
        assert_eq!(
            rows(&conn, "mr_flag_asc").await,
            vec![(1, None, Some(3), "stale".into())]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dedup_sort_null_and_tie_policy_is_deterministic() {
        let Some(pg) = PgFixture::start().await else {
            return;
        };
        let conn = pg.conn.clone();
        let opts = Opts {
            dedup: Some(("seq", SortOrder::Desc)),
            ..Opts::default()
        };
        // NULL loses to a value in EITHER direction (US1-AS4).
        run(
            &conn,
            "mr_null",
            opts,
            vec![vec![
                (1, None, None, "null-seq", None),
                (1, None, Some(3), "valued", None),
            ]],
        )
        .await;
        assert_eq!(
            rows(&conn, "mr_null").await,
            vec![(1, None, Some(3), "valued".into())]
        );

        // All NULL: deterministic last-wins.
        run(
            &conn,
            "mr_all_null",
            opts,
            vec![vec![
                (1, None, None, "first", None),
                (1, None, None, "last", None),
            ]],
        )
        .await;
        assert_eq!(
            rows(&conn, "mr_all_null").await,
            vec![(1, None, None, "last".into())]
        );

        // Tie: arrival breaks it, replay converges to the same survivor
        // (US1-AS5) — a second identical run moves the state nowhere.
        let tie: Vec<Vec<Row>> = vec![vec![
            (1, None, Some(5), "first", None),
            (1, None, Some(5), "last", None),
        ]];
        run(&conn, "mr_tie", opts, tie.clone()).await;
        assert_eq!(
            rows(&conn, "mr_tie").await,
            vec![(1, None, Some(5), "last".into())]
        );
        run(&conn, "mr_tie", opts, tie).await;
        assert_eq!(
            rows(&conn, "mr_tie").await,
            vec![(1, None, Some(5), "last".into())]
        );
    }
}

// ---- Feature 011 (contract PM1/PM2): parameter-matrix gap cells ----

mod param_matrix {
    use rdlt_connector_postgres::dest::{DestOptions, MergeStrategy, Postgres, TableOptions};
    use rdlt_engine::{Engine, EngineConfig};
    use rdlt_testkit::MemorySource;
    use serde_json::json;

    use super::PgFixture;

    /// `dataset` default — omitted, tables land in `public` (observed,
    /// not inferred; PM3).
    #[tokio::test(flavor = "multi_thread")]
    async fn default_dataset_is_public() {
        let Some(pg) = PgFixture::start().await else {
            return;
        };
        let conn = pg.conn.clone();
        let dest = Postgres::connect(&conn); // no .dataset(...)
        let source = MemorySource::single_stream(
            rdlt_connector::StreamSpec::new("things").with_primary_key(["id"]),
            vec![json!({"id": 1, "v": "a"})],
        );
        Engine::new(EngineConfig::new("dflt-ds"), source, dest)
            .run()
            .await
            .expect("run");
        let (client, connection) = tokio_postgres::connect(&conn, tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let n: i64 = client
            .query_one("SELECT count(*) FROM public.things", &[])
            .await
            .expect("public table")
            .get(0);
        assert_eq!(n, 1, "omitted dataset lands in the `public` schema");
    }

    /// Feature 011 R5 (PM7): an EXPLICITLY configured merge_strategy under
    /// append/replace is a typed error; the unconfigured default never
    /// rejects.
    #[tokio::test(flavor = "multi_thread")]
    async fn explicit_strategy_under_non_merge_mode_is_typed() {
        let Some(pg) = PgFixture::start().await else {
            return;
        };
        let conn = pg.conn.clone();
        let source = || {
            MemorySource::single_stream(
                rdlt_connector::StreamSpec::new("things").with_primary_key(["id"]),
                vec![json!({"id": 1, "v": "a"})],
            )
        };
        let run = |mode: rdlt_connector::WriteMode, options: DestOptions| {
            let dest = Postgres::connect(&conn)
                .dataset("r5")
                .options(options)
                .expect("options");
            let mut config = EngineConfig::new("r5");
            config = config.with_write_mode(mode);
            Engine::new(config, source(), dest).run()
        };

        // Destination-wide explicit strategy under APPEND: typed.
        let err = run(
            rdlt_connector::WriteMode::Append,
            DestOptions {
                merge_strategy: Some(MergeStrategy::Upsert),
                ..DestOptions::default()
            },
        )
        .await
        .expect_err("explicit strategy under append")
        .to_string();
        assert!(err.contains("merge_strategy"), "{err}");
        assert!(err.contains("requires the merge write mode"), "{err}");

        // Per-table explicit strategy under REPLACE: typed too.
        let err = run(
            rdlt_connector::WriteMode::Replace,
            DestOptions {
                tables: [(
                    "things".to_string(),
                    TableOptions {
                        merge_strategy: Some(MergeStrategy::DeleteInsert),
                        ..TableOptions::default()
                    },
                )]
                .into_iter()
                .collect(),
                ..DestOptions::default()
            },
        )
        .await
        .expect_err("per-table explicit strategy under replace")
        .to_string();
        assert!(err.contains("`things`"), "{err}");

        // UNCONFIGURED default: append works exactly as before.
        run(rdlt_connector::WriteMode::Append, DestOptions::default())
            .await
            .expect("default options never reject append");
    }

    /// `hard_delete` on a NON-boolean column — M4's other arm: the flag
    /// fires on `IS NOT NULL` (any value), keeps on NULL.
    #[tokio::test(flavor = "multi_thread")]
    async fn non_bool_hard_delete_flag_uses_is_not_null() {
        use std::sync::Arc;

        use arrow_array::{Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
        use arrow_schema::{DataType, Field, Schema, TimeUnit};
        use async_trait::async_trait;
        use rdlt_connector::{ConnectorSpec, ReadRequest, Source, SourceError, StreamSpec};

        struct TsFlagged {
            batch: RecordBatch,
        }

        #[async_trait]
        impl Source for TsFlagged {
            fn spec(&self) -> ConnectorSpec {
                ConnectorSpec::new("ts-flagged", "0.0.0")
            }
            async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
                Ok(vec![
                    StreamSpec::new("ev")
                        .with_structured()
                        .with_primary_key(["id"]),
                ])
            }
            async fn read(&self, mut req: ReadRequest) -> Result<(), SourceError> {
                let _ = req.out.arrow(self.batch.clone()).await;
                Ok(())
            }
        }

        fn batch(rows: &[(i64, &str, Option<i64>)]) -> RecordBatch {
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("name", DataType::Utf8, true),
                    Field::new(
                        "deleted_at",
                        DataType::Timestamp(TimeUnit::Microsecond, None),
                        true,
                    ),
                ])),
                vec![
                    Arc::new(Int64Array::from(
                        rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                    )),
                    Arc::new(TimestampMicrosecondArray::from(
                        rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                    )),
                ],
            )
            .expect("batch")
        }

        let Some(pg) = PgFixture::start().await else {
            return;
        };
        let conn = pg.conn.clone();
        let dest = Postgres::connect(&conn)
            .dataset("nbhd")
            .options(DestOptions {
                merge_strategy: Some(MergeStrategy::Upsert),
                tables: [(
                    "ev".to_string(),
                    TableOptions {
                        hard_delete: Some("deleted_at".into()),
                        ..TableOptions::default()
                    },
                )]
                .into_iter()
                .collect(),
            })
            .expect("options");
        let run = |rows: &[(i64, &str, Option<i64>)]| {
            let mut config = EngineConfig::new("nbhd");
            config = config.with_write_mode(rdlt_connector::WriteMode::Merge {
                key: vec!["id".into()],
            });
            Engine::new(config, TsFlagged { batch: batch(rows) }, dest.clone()).run()
        };
        run(&[(1, "a", None), (2, "b", None)]).await.expect("seed");
        // A deletion TIMESTAMP (non-bool) fires the flag; NULL keeps.
        run(&[(1, "a2", Some(1_700_000_000_000_000)), (2, "b2", None)])
            .await
            .expect("flagged");
        let (client, connection) = tokio_postgres::connect(&conn, tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let names: Vec<String> = client
            .query("SELECT name FROM nbhd.ev ORDER BY id", &[])
            .await
            .expect("rows")
            .into_iter()
            .map(|r| r.get(0))
            .collect();
        assert_eq!(
            names,
            vec!["b2"],
            "non-bool flag: IS NOT NULL deletes, NULL merges normally (M4)"
        );
    }
}

// ---- Feature 019 US5: what the direct-to-target unit transaction guarantees
// to concurrent readers, and what it deliberately does not. ----

mod unit_isolation {
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use rdlt_connector::core::{
        ColumnDef, ColumnType, CommitCounters, LoadId, LogicalType, PipelineId, Provenance,
        StateDoc, TableName, TableSchema,
    };
    use rdlt_connector::{CommitMeta, Destination, OpenCtx, WriteMode};
    use rdlt_connector_postgres::fixtures::PgFixture;

    fn schema() -> TableSchema {
        TableSchema {
            table: TableName::new("iso"),
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

    async fn count(client: &tokio_postgres::Client, table: &str) -> i64 {
        client
            .query_one(&format!("SELECT count(*) FROM {table}"), &[])
            .await
            .expect("count")
            .get(0)
    }

    async fn reader(conn: &str) -> tokio_postgres::Client {
        let (client, connection) = tokio_postgres::connect(conn, tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    }

    /// A Replace load clears its target and refills it in ONE transaction, so
    /// no reader ever observes the gap between the two. Pinned here because
    /// the MECHANISM is not the one an isolation-level argument would predict,
    /// and the difference is what operators feel:
    ///
    /// `clear_table` is `TRUNCATE`, which takes ACCESS EXCLUSIVE. A concurrent
    /// `SELECT` needs ACCESS SHARE, which conflicts — so a reader arriving
    /// mid-unit does not see the old rows under MVCC, it **blocks** until the
    /// unit commits and then sees the new ones. (A `DELETE`-based clear would
    /// take only ROW EXCLUSIVE and readers would proceed against the old
    /// snapshot; `TRUNCATE` is chosen for speed, and this is its price.)
    ///
    /// The guarantee "never observed empty" therefore holds by locking, not by
    /// snapshots — and it holds at every isolation level for the same reason.
    /// What this story changed is the WINDOW: the lock used to be held for the
    /// publish alone, and is now held from the first batch to the commit. See
    /// the module doc on what a unit transaction costs.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_replace_reload_is_never_observed_empty() {
        let Some(pg) = PgFixture::start().await else {
            return;
        };
        let conn = pg.conn.clone();
        let dest = rdlt_connector_postgres::dest::Postgres::connect(&conn).dataset("iso");
        let pipeline = PipelineId::new("iso");

        let meta = |load: &str, seq: u64| CommitMeta {
            load_id: LoadId::new(load),
            commit_seq: seq,
            state: StateDoc::new(pipeline.clone(), env!("CARGO_PKG_VERSION")),
            counters: CommitCounters::default(),
        };

        // Load 1 establishes the "previous contents".
        let mut first = dest
            .open(OpenCtx::new(pipeline.clone(), LoadId::new("iso-1")))
            .await
            .expect("open");
        first
            .ensure_table(&schema(), &WriteMode::Replace)
            .await
            .expect("ensure");
        first
            .write(&TableName::new("iso"), batch(&[1, 2, 3]))
            .await
            .expect("write");
        first.commit(meta("iso-1", 0)).await.expect("commit");
        drop(first);

        let observer = reader(&conn).await;
        assert_eq!(count(&observer, "iso.iso").await, 3, "load 1 landed");

        // Load 2 clears and refills. Mid-unit — after the TRUNCATE and the
        // COPY, before the commit — the reader must still see load 1.
        let mut second = dest
            .open(OpenCtx::new(pipeline.clone(), LoadId::new("iso-2")))
            .await
            .expect("open");
        second
            .ensure_table(&schema(), &WriteMode::Replace)
            .await
            .expect("ensure");
        second
            .write(&TableName::new("iso"), batch(&[4, 5]))
            .await
            .expect("write");
        // The reader is now behind the TRUNCATE's ACCESS EXCLUSIVE lock. It
        // must NOT complete — and must not come back with 0 — while the unit
        // is open.
        let mut blocked = tokio::spawn(async move { count(&observer, "iso.iso").await });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(750), &mut blocked)
                .await
                .is_err(),
            "a reader must block on a cleared-but-uncommitted target, never observe it empty"
        );

        second.commit(meta("iso-2", 0)).await.expect("commit");
        assert_eq!(
            blocked.await.expect("reader task"),
            2,
            "the blocked reader resumes and sees load 2's contents, never an empty table"
        );
    }

    /// FR-024: a Replace load must not cost the target anything that lives on
    /// the table rather than in it. The clear is `TRUNCATE`, which keeps the
    /// table's identity — so indexes, constraints, grants and dependent views
    /// all survive.
    ///
    /// This is the property that ruled out the faster-looking alternative. A
    /// swap (`CREATE new; ... ; ALTER TABLE RENAME`) clears in ~21 ms against
    /// TRUNCATE-plus-refill, but it produces a table with a NEW oid, which
    /// means rebuilding every index, re-granting every privilege and breaking
    /// every view bound to the old one. Pinned so a future "optimization"
    /// cannot take that trade silently.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_replace_load_preserves_indexes_grants_and_dependents() {
        let Some(pg) = PgFixture::start().await else {
            return;
        };
        let conn = pg.conn.clone();
        let admin = reader(&conn).await;
        admin
            .batch_execute(
                "CREATE SCHEMA IF NOT EXISTS iso3;
                 DROP VIEW IF EXISTS iso3.iso_v;
                 DROP TABLE IF EXISTS iso3.iso;
                 CREATE TABLE iso3.iso (id BIGINT CONSTRAINT iso_id_positive CHECK (id > 0));
                 CREATE INDEX iso_id_idx ON iso3.iso (id);
                 CREATE VIEW iso3.iso_v AS SELECT id FROM iso3.iso;
                 CREATE ROLE iso_reader;
                 GRANT SELECT ON iso3.iso TO iso_reader;",
            )
            .await
            .expect("fixture objects");
        let oid_before: u32 = admin
            .query_one("SELECT 'iso3.iso'::regclass::oid", &[])
            .await
            .expect("oid")
            .get(0);

        let dest = rdlt_connector_postgres::dest::Postgres::connect(&conn).dataset("iso3");
        let pipeline = PipelineId::new("iso3");
        let mut session = dest
            .open(OpenCtx::new(pipeline.clone(), LoadId::new("iso3-load")))
            .await
            .expect("open");
        session
            .ensure_table(&schema(), &WriteMode::Replace)
            .await
            .expect("ensure");
        session
            .write(&TableName::new("iso"), batch(&[7, 8]))
            .await
            .expect("write");
        session
            .commit(CommitMeta {
                load_id: LoadId::new("iso3-load"),
                commit_seq: 0,
                state: StateDoc::new(pipeline.clone(), env!("CARGO_PKG_VERSION")),
                counters: CommitCounters::default(),
            })
            .await
            .expect("commit");

        let oid_after: u32 = admin
            .query_one("SELECT 'iso3.iso'::regclass::oid", &[])
            .await
            .expect("oid")
            .get(0);
        assert_eq!(oid_before, oid_after, "the target keeps its identity");

        let survivors = |what: &'static str, sql: &'static str| {
            let admin = &admin;
            async move {
                let n: i64 = admin.query_one(sql, &[]).await.expect(what).get(0);
                assert_eq!(n, 1, "{what} did not survive the Replace load");
            }
        };
        survivors(
            "the index",
            "SELECT count(*) FROM pg_indexes WHERE schemaname='iso3' AND indexname='iso_id_idx'",
        )
        .await;
        survivors(
            "the check constraint",
            "SELECT count(*) FROM pg_constraint WHERE conname='iso_id_positive'",
        )
        .await;
        survivors(
            "the grant",
            "SELECT count(*) FROM information_schema.table_privileges \
             WHERE table_schema='iso3' AND table_name='iso' AND grantee='iso_reader' \
               AND privilege_type='SELECT'",
        )
        .await;
        // The dependent view still resolves AND sees the new contents.
        let through_view: i64 = admin
            .query_one("SELECT count(*) FROM iso3.iso_v", &[])
            .await
            .expect("the dependent view")
            .get(0);
        assert_eq!(through_view, 2, "the view reads the reloaded table");

        admin
            .batch_execute("DROP OWNED BY iso_reader; DROP ROLE IF EXISTS iso_reader;")
            .await
            .expect("cleanup");
    }

    /// The clear happens once per LOAD, not once per commit unit: unit 2 of a
    /// Replace load appends to what unit 1 published rather than wiping it.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_multi_unit_replace_load_clears_exactly_once() {
        let Some(pg) = PgFixture::start().await else {
            return;
        };
        let conn = pg.conn.clone();
        let dest = rdlt_connector_postgres::dest::Postgres::connect(&conn).dataset("iso2");
        let pipeline = PipelineId::new("iso2");
        let mut session = dest
            .open(OpenCtx::new(pipeline.clone(), LoadId::new("iso2-load")))
            .await
            .expect("open");
        session
            .ensure_table(&schema(), &WriteMode::Replace)
            .await
            .expect("ensure");

        let meta = |seq: u64| CommitMeta {
            load_id: LoadId::new("iso2-load"),
            commit_seq: seq,
            state: StateDoc::new(pipeline.clone(), env!("CARGO_PKG_VERSION")),
            counters: CommitCounters::default(),
        };

        for (seq, ids) in [(0u64, [1i64, 2].as_slice()), (1, [3].as_slice())] {
            session
                .write(&TableName::new("iso"), batch(ids))
                .await
                .expect("write");
            session.commit(meta(seq)).await.expect("commit");
        }

        let client = reader(&conn).await;
        assert_eq!(
            count(&client, "iso2.iso").await,
            3,
            "unit 2 must append to the load's own rows, not re-clear them"
        );
    }
}
