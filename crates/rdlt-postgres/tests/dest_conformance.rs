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

    fn fidelity_batch(rows: &[(i64, Option<i128>, Option<&str>, Option<&str>)]) -> RecordBatch {
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
        let msg = err.to_string();
        assert!(
            msg.contains("Unicode escape") && msg.contains("SQLSTATE"),
            "server message + SQLSTATE surfaced: {msg}"
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
