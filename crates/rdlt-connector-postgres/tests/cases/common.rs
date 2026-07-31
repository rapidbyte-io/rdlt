//! Shared scaffolding for the case files: the raw-client idiom, scalar and
//! count readbacks, the bare YAML source builder, the leaked-tempdir DuckDB
//! destination, and the table probe the destination conformance suites read
//! back through.

use async_trait::async_trait;
use rdlt_connector_duckdb::dest::DuckDb;
use rdlt_connector_postgres::source::PostgresSource;
use rdlt_testkit::TableProbe;
use tokio_postgres::Client;

/// A raw client on `conn`, its connection task detached — the one spelling
/// of the connect idiom in the case files.
pub async fn connect(conn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(conn, tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// One value from one row — the readback most assertions need.
pub async fn scalar<T>(conn: &str, sql: &str) -> T
where
    T: for<'a> tokio_postgres::types::FromSql<'a>,
{
    connect(conn)
        .await
        .query_one(sql, &[])
        .await
        .expect("scalar")
        .get(0)
}

/// Row count of `<dataset>.<table>`; a missing table counts as empty (the
/// probe contract — recovery suites ask before the table exists).
pub async fn count(conn: &str, dataset: &str, table: &str) -> u64 {
    let sql = format!(
        "SELECT count(*) FROM \"{dataset}\".\"{}\"",
        table.replace('"', "")
    );
    match connect(conn).await.query_one(&sql, &[]).await {
        Ok(row) => row.get::<_, i64>(0) as u64,
        Err(_) => 0,
    }
}

/// A source from the bare `conn:` line plus whatever YAML the suite appends.
pub fn source(conn: &str, extra: &str) -> PostgresSource {
    PostgresSource::from_yaml(&format!("conn: \"{conn}\"\n{extra}")).expect("config")
}

/// A DuckDB destination in a leaked tempdir — leaked deliberately: the file
/// must outlive the test body so late engine writes never race teardown.
pub fn duckdb_dest() -> DuckDb {
    let dir = tempfile::tempdir().expect("tempdir");
    let dest = DuckDb::open(dir.path().join("out.duckdb")).expect("open db");
    std::mem::forget(dir);
    dest
}

pub struct PgProbe {
    pub conn: String,
    pub schema: String,
}

#[async_trait]
impl TableProbe for PgProbe {
    async fn count(&self, table: &rdlt_connector::TableName) -> u64 {
        count(&self.conn, &self.schema, table.as_str()).await
    }
}
