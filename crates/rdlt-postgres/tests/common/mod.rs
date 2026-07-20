//! Shared testcontainers fixture (T002): one postgres:16-alpine container per
//! test binary, seeded via SQL batches, handing out conn strings/clients.
//! Mirrors the rdlt-dest-postgres test conventions.

use testcontainers_modules::postgres::Postgres as PostgresImage;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};
use tokio_postgres::{Client, NoTls};

pub struct PgFixture {
    // Held for its Drop: the container stops when the fixture drops.
    _container: ContainerAsync<PostgresImage>,
    pub conn: String,
}

impl PgFixture {
    /// Start a fresh postgres:16-alpine (needs docker/podman; the workspace's
    /// existing suites already require this).
    pub async fn start() -> Self {
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
        Self {
            _container: container,
            conn,
        }
    }

    /// A raw client for seeding/asserting, independent of the source under test.
    pub async fn client(&self) -> Client {
        let (client, connection) = tokio_postgres::connect(&self.conn, NoTls)
            .await
            .expect("connect to fixture postgres");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    }

    /// Run semicolon-separated DDL/DML (simple batch seeding).
    pub async fn seed(&self, sql: &str) {
        let client = self.client().await;
        client.batch_execute(sql).await.expect("seed SQL");
    }

    /// The source-config YAML `conn:` value for this fixture.
    pub fn conn_url(&self) -> String {
        self.conn.clone()
    }
}
