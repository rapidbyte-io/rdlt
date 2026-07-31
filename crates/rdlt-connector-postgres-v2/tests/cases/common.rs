//! Shared scaffolding for the case files: the raw-client idiom, scalar and
//! count readbacks, the bare YAML source builder, the leaked-tempdir DuckDB
//! destination, and the table probe the destination conformance suites read
//! back through.

use async_trait::async_trait;
use rdlt_connector_duckdb::dest::DuckDb;
use rdlt_connector_postgres_v2::source;
use rdlt_testkit::TableProbe;
use tokio_postgres::Client;

/// A raw client on `connection_string`, its connection task detached — the
/// one spelling of the connect idiom in the case files.
pub async fn connect(connection_string: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(connection_string, tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// One value from one row — the readback most assertions need.
pub async fn scalar<T>(connection_string: &str, sql: &str) -> T
where
    T: for<'a> tokio_postgres::types::FromSql<'a>,
{
    connect(connection_string)
        .await
        .query_one(sql, &[])
        .await
        .expect("scalar")
        .get(0)
}

/// Row count of `<dataset>.<table>`; a missing table counts as empty (the
/// probe contract — recovery suites ask before the table exists).
pub async fn count(connection_string: &str, dataset: &str, table: &str) -> u64 {
    let sql = format!(
        "SELECT count(*) FROM \"{dataset}\".\"{}\"",
        table.replace('"', "")
    );
    match connect(connection_string).await.query_one(&sql, &[]).await {
        Ok(row) => row.get::<_, i64>(0) as u64,
        Err(_) => 0,
    }
}

/// A source from the bare `conn:` line plus whatever YAML the suite appends.
pub fn source(connection_string: &str, extra_yaml: &str) -> source::Postgres {
    source::Postgres::from_yaml(&format!("conn: \"{connection_string}\"\n{extra_yaml}"))
        .expect("config")
}

/// A DuckDB destination in a leaked tempdir — leaked deliberately: the file
/// must outlive the test body so late engine writes never race teardown.
pub fn duckdb_destination() -> DuckDb {
    let directory = tempfile::tempdir().expect("tempdir");
    let destination = DuckDb::open(directory.path().join("out.duckdb")).expect("open db");
    std::mem::forget(directory);
    destination
}

/// The rowcount face the conformance harness reads a loaded table through:
/// one schema on one database, counted with the same missing-table-is-empty
/// contract [`count`] carries.
pub struct Probe {
    pub connection_string: String,
    pub schema: String,
}

#[async_trait]
impl TableProbe for Probe {
    async fn count(&self, table: &rdlt_connector::TableName) -> u64 {
        count(&self.connection_string, &self.schema, table.as_str()).await
    }
}
