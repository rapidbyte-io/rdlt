//! T059: Postgres destination via testcontainers — destination conformance suite plus
//! an end-to-end engine run with flatten lowering and merge.
//!
//! Requires a container runtime; each test spins up postgres:16-alpine.

use async_trait::async_trait;
use rdlt_engine::{Engine, EngineConfig};
use rdlt_postgres::dest::Postgres;
use rdlt_testkit::conformance::dest::verify_destination;
use rdlt_testkit::{MemorySource, TableProbe, assert_conformant};
use serde_json::json;
use testcontainers_modules::postgres::Postgres as PostgresImage;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

async fn start_pg() -> (
    testcontainers_modules::testcontainers::ContainerAsync<PostgresImage>,
    String,
) {
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
    (container, conn)
}

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
    let (_container, conn) = start_pg().await;
    let dest = Postgres::connect(&conn).dataset("raw");
    let probe = PgProbe {
        conn: conn.clone(),
        schema: "raw".into(),
    };
    assert_conformant(verify_destination(&dest, &probe).await);
}

#[tokio::test(flavor = "multi_thread")]
async fn end_to_end_flattened_sync_into_postgres() {
    let (_container, conn) = start_pg().await;
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
    config.write_mode = rdlt_connector::WriteMode::Merge {
        key: vec!["id".into()],
    };
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
    config.write_mode = rdlt_connector::WriteMode::Merge {
        key: vec!["id".into()],
    };
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
                    .structured()
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

    let (_container, conn) = start_pg().await;
    let dest = Postgres::connect(&conn).dataset("raw");
    let merge_config = || {
        let mut config = EngineConfig::new("pg-kmerge");
        config.write_mode = rdlt_connector::WriteMode::Merge {
            key: vec!["id".into()],
        };
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

    use super::{PgProbe, start_pg};

    fn col(name: &str, scalar: LogicalType, nullable: bool) -> ColumnDef {
        ColumnDef {
            name: name.into(),
            ty: ColumnType::Scalar { scalar },
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

    fn meta(pipeline: &PipelineId, seq: u64) -> CommitMeta {
        CommitMeta {
            load_id: LoadId::new("fid-load"),
            commit_seq: seq,
            state: StateDoc::new(pipeline.clone()),
            counters: CommitCounters::default(),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn native_types_land_with_exact_values() {
        let (_container, conn) = start_pg().await;
        let dest = rdlt_postgres::dest::Postgres::connect(&conn).dataset("fid");
        let pipeline = PipelineId::new("fid");
        let mut session = dest
            .open(OpenCtx::new(pipeline.clone(), LoadId::new("fid-load")))
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
        session.commit(meta(&pipeline, 0)).await.expect("commit");

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
        let (_container, conn) = start_pg().await;
        let dest = rdlt_postgres::dest::Postgres::connect(&conn).dataset("wide");
        let pipeline = PipelineId::new("wide");
        let mut session = dest
            .open(OpenCtx::new(pipeline.clone(), LoadId::new("w-load")))
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
        session.commit(meta(&pipeline, 0)).await.expect("commit");

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
        let (_container, conn) = start_pg().await;
        let dest = rdlt_postgres::dest::Postgres::connect(&conn).dataset("fidbad");
        let pipeline = PipelineId::new("fidbad");
        let mut session = dest
            .open(OpenCtx::new(pipeline.clone(), LoadId::new("fb-load")))
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
        // surfaced error carries its message + SQLSTATE (T2 + review F6).
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
        let (_container, conn) = start_pg().await;
        let dest = rdlt_postgres::dest::Postgres::connect(&conn).dataset("f6");
        let pipeline = PipelineId::new("f6");
        let mut session = dest
            .open(OpenCtx::new(pipeline.clone(), LoadId::new("f6-load")))
            .await
            .expect("open");
        let schema = fidelity_schema();
        session
            .ensure_table(&schema, &WriteMode::Append)
            .await
            .expect("ensure");
        // NOT NULL violation at publish: the batch stages fine (stage is
        // nullable) but violates the target at INSERT.
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)])),
            vec![Arc::new(Int64Array::from(vec![None::<i64>]))],
        )
        .expect("batch");
        session.write(&schema.table, batch).await.expect("stage ok");
        let err = session
            .commit(meta(&pipeline, 0))
            .await
            .expect_err("NOT NULL violation at publish");
        let msg = err.to_string();
        assert!(
            msg.contains("null value") && msg.contains("SQLSTATE 23502"),
            "F6: server message + SQLSTATE, never bare db error: {msg}"
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
    use rdlt_engine::{Engine, EngineConfig};
    use rdlt_postgres::dest::{MergeStrategy, PgDestOptions, PgTableOptions, Postgres};

    use super::start_pg;

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
                    .structured()
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
            .options(PgDestOptions {
                merge_strategy: MergeStrategy::Upsert,
                tables: [(
                    "events".to_string(),
                    PgTableOptions {
                        hard_delete: Some("deleted".into()),
                        ..PgTableOptions::default()
                    },
                )]
                .into_iter()
                .collect(),
            })
            .expect("valid options")
    }

    async fn run_merge(dest: Postgres, rows: &[(i64, &str, Option<bool>)]) {
        let mut config = EngineConfig::new("strat");
        config.write_mode = rdlt_connector::WriteMode::Merge {
            key: vec!["id".into()],
        };
        Engine::new(config, FlaggedSource { batch: batch(rows) }, dest)
            .run()
            .await
            .expect("merge run");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn upsert_converges_and_hard_delete_removes_keys() {
        let (_container, conn) = start_pg().await;

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
        let (_container, conn) = start_pg().await;
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
        config.write_mode = rdlt_connector::WriteMode::Merge {
            key: vec!["id".into()],
        };
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

        let (_container, conn) = start_pg().await;
        let dest = Postgres::connect(&conn)
            .dataset("shup")
            .options(PgDestOptions {
                merge_strategy: MergeStrategy::Upsert,
                ..PgDestOptions::default()
            })
            .expect("options");
        let mut config = EngineConfig::new("shup");
        config.write_mode = rdlt_connector::WriteMode::Merge {
            key: vec!["id".into()],
        };
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

        let (_container, conn) = start_pg().await;
        let dest = Postgres::connect(&conn)
            .dataset("recreate")
            .options(PgDestOptions {
                tables: [(
                    "users".to_string(),
                    PgTableOptions {
                        hard_delete: Some("deleted".into()),
                        ..PgTableOptions::default()
                    },
                )]
                .into_iter()
                .collect(),
                ..PgDestOptions::default()
            })
            .expect("options");
        let mut config = EngineConfig::new("recreate");
        config.write_mode = rdlt_connector::WriteMode::Merge {
            key: vec!["id".into()],
        };
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

        let (_container, conn) = start_pg().await;
        let dest = Postgres::connect(&conn)
            .dataset("childhd")
            .options(PgDestOptions {
                tables: [(
                    "users__tags".to_string(),
                    PgTableOptions {
                        hard_delete: Some("deleted".into()),
                        ..PgTableOptions::default()
                    },
                )]
                .into_iter()
                .collect(),
                ..PgDestOptions::default()
            })
            .expect("options");
        let mut config = EngineConfig::new("childhd");
        config.write_mode = rdlt_connector::WriteMode::Merge {
            key: vec!["id".into()],
        };
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
    use rdlt_engine::{Engine, EngineConfig};
    use rdlt_postgres::dest::{
        DedupSort, MergeStrategy, PgDestOptions, PgTableOptions, Postgres, SortOrder,
    };

    use super::start_pg;

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
                    .structured()
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
        merge_key: Option<&'static [&'static str]>,
        hard_delete: bool,
    }

    fn dest(conn: &str, dataset: &str, opts: Opts) -> Postgres {
        Postgres::connect(conn)
            .dataset(dataset)
            .options(PgDestOptions {
                merge_strategy: opts.strategy.unwrap_or_default(),
                tables: [(
                    "events".to_string(),
                    PgTableOptions {
                        hard_delete: opts.hard_delete.then(|| "deleted".into()),
                        dedup_sort: opts.dedup.map(|(column, order)| DedupSort {
                            column: column.into(),
                            order,
                        }),
                        merge_key: opts
                            .merge_key
                            .map(|c| c.iter().map(|s| s.to_string()).collect()),
                        ..PgTableOptions::default()
                    },
                )]
                .into_iter()
                .collect(),
            })
            .expect("valid options")
    }

    async fn run(conn: &str, dataset: &str, opts: Opts, units: Vec<Vec<Row>>) {
        let mut config = EngineConfig::new(format!("mr-{dataset}"));
        config.write_mode = rdlt_connector::WriteMode::Merge {
            key: vec!["id".into()],
        };
        let units = units.iter().map(|u| batch(u)).collect();
        Engine::new(config, UnitsSource { units }, dest(conn, dataset, opts))
            .run()
            .await
            .expect("merge run");
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

    // ---- US1: ordered survivor selection (MR1/MR2, SC-001) ----

    #[tokio::test(flavor = "multi_thread")]
    async fn dedup_sort_orders_survivors_not_arrival() {
        let (_c, conn) = start_pg().await;
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

    #[tokio::test(flavor = "multi_thread")]
    async fn dedup_sort_survivor_drives_hard_delete() {
        let (_c, conn) = start_pg().await;
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
        let (_c, conn) = start_pg().await;
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
