//! Shared scaffolding for the case files: the table probe the destination
//! conformance suites read back through.

use async_trait::async_trait;
use rdlt_testkit::TableProbe;

pub struct PgProbe {
    pub conn: String,
    pub schema: String,
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
