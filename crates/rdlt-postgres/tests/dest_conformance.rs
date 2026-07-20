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
