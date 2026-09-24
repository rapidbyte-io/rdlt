use arrow_array::RecordBatch;
use rdlt_connector::testing::{Outcome, Probe, certify_destination, certify_source};
use rdlt_connector::{BoxFuture, Result, TableRef};
use rdlt_connector_reference::{
    FilesDestination, FilesSource, GeneratorSource, MemoryDestination, MemorySource,
    SqliteDestination, files, published, sqlite,
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

struct FilesProbe(std::path::PathBuf);

impl Probe for FilesProbe {
    fn published<'a>(&'a self, table: &'a TableRef) -> BoxFuture<'a, Result<Vec<RecordBatch>>> {
        let batches = files::published(&self.0, &table.name);
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

#[tokio::test]
async fn the_files_destination_is_certified_in_both_formats() {
    for format in ["jsonl", "arrow"] {
        let directory = tempfile::tempdir().unwrap();
        let config = json!({ "root": directory.path(), "format": format });
        let probe = FilesProbe(directory.path().to_owned());
        for _ in 0..2 {
            let report = certify_destination::<FilesDestination>(config.clone(), &probe).await;
            assert!(report.passed(), "{format}: {report}");
        }
    }
}

/// Writes ids `0..rows` as an Arrow IPC file of one-row batches at `path`.
fn arrow_file(path: &std::path::Path, rows: i64) {
    let schema = std::sync::Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int64,
        false,
    )]));
    let file = std::fs::File::create(path).expect("the file is created");
    let mut writer =
        arrow_ipc::writer::FileWriter::try_new(file, &schema).expect("the writer starts");
    for id in 0..rows {
        let column = std::sync::Arc::new(arrow_array::Int64Array::from(vec![id]));
        let batch = RecordBatch::try_new(schema.clone(), vec![column]).expect("the batch is valid");
        writer.write(&batch).expect("the batch is written");
    }
    writer.finish().expect("the file is finished");
}

#[tokio::test]
async fn the_files_source_is_certified() {
    let root = tempfile::tempdir().unwrap();
    let lines =
        "{\"id\": 0}\n{\"id\": 1, \"name\": \"b\"}\n{\"id\": 2}\n{\"id\": 3}\n{\"id\": 4}\n";
    std::fs::write(root.path().join("users.jsonl"), lines).unwrap();
    std::fs::create_dir(root.path().join("events")).unwrap();
    std::fs::write(
        root.path().join("events").join("a.jsonl"),
        "{\"x\": 1}\n\n{\"x\": 2}\n",
    )
    .unwrap();
    arrow_file(&root.path().join("events").join("b.arrow"), 3);
    std::fs::write(root.path().join("notes.txt"), "not a stream").unwrap();
    std::fs::create_dir(root.path().join("_rdlt")).unwrap();
    let config = json!({ "root": root.path(), "batch_rows": 2 });
    let report = certify_source::<FilesSource>(config).await;
    report.assert_passed();
    assert_eq!(report.outcome("S-BARRIER"), Some(&Outcome::Passed));
}
