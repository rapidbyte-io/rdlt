//! T059: Postgres destination via testcontainers — destination conformance suite plus
//! an end-to-end engine run with flatten lowering and merge.
//!
//! Requires a container runtime; each test spins up postgres:16-alpine.

use async_trait::async_trait;
use rdlt_dest_postgres::Postgres;
use rdlt_engine::{Engine, EngineConfig};
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
