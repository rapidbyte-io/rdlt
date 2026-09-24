use arrow_array::RecordBatch;
use rdlt_connector::testing::{Outcome, Probe, certify_destination, certify_source};
use rdlt_connector::{BoxFuture, Result, TableRef};
use rdlt_connector_reference::{
    GeneratorSource, MemoryDestination, MemorySource, SqliteDestination, published, sqlite,
};
use serde_json::json;

struct MemoryProbe(&'static str);

impl Probe for MemoryProbe {
    fn published<'a>(&'a self, table: &'a TableRef) -> BoxFuture<'a, Result<Vec<RecordBatch>>> {
        let batches = published(self.0, &table.name);
        Box::pin(async move { Ok(batches) })
    }
}

struct SqliteProbe(std::path::PathBuf);

impl Probe for SqliteProbe {
    fn published<'a>(&'a self, table: &'a TableRef) -> BoxFuture<'a, Result<Vec<RecordBatch>>> {
        let batches = sqlite::published(&self.0, &table.name);
        Box::pin(async move { batches })
    }
}

#[tokio::test]
async fn the_memory_source_is_certified() {
    let config = json!({
        "streams": { "users": [{"id": 1}, {"id": 2}, {"id": 3}], "empty": [] },
        "page_size": 2,
    });
    let report = certify_source::<MemorySource>(config).await;
    report.assert_passed();
    assert!(
        matches!(report.outcome("S-BARRIER"), Some(Outcome::Skipped(_))),
        "{report}"
    );
}

#[tokio::test]
async fn the_generator_is_certified() {
    let config = json!({
        "seed": 7,
        "streams": [{ "name": "events", "rows": 57, "partitions": 3, "batch_rows": 5 }],
    });
    let report = certify_source::<GeneratorSource>(config).await;
    report.assert_passed();
    assert_eq!(report.outcome("S-BARRIER"), Some(&Outcome::Passed));
}

#[tokio::test]
async fn the_memory_destination_is_certified() {
    certify_destination::<MemoryDestination>(
        json!({ "store": "certify" }),
        &MemoryProbe("certify"),
    )
    .await
    .assert_passed();
}

#[tokio::test]
async fn the_sqlite_destination_is_certified() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("certify.db");
    let config = json!({ "path": path });
    let probe = SqliteProbe(path.clone());
    certify_destination::<SqliteDestination>(config.clone(), &probe)
        .await
        .assert_passed();
    certify_destination::<SqliteDestination>(config, &probe)
        .await
        .assert_passed();
}
