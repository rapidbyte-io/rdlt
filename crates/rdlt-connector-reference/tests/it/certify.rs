use arrow_array::RecordBatch;
use rdlt_connector::testing::{Outcome, Probe, Unprobed, certify_destination, certify_source};
use rdlt_connector::{BoxFuture, Result, TableRef};
use rdlt_connector_reference::{
    ChangesSource, FilesDestination, FilesSource, GeneratorSource, LogSource, MemoryDestination,
    MemorySource, SqliteDestination, files, published, sqlite,
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
    assert_eq!(report.outcome("S-PARTITION"), Some(&Outcome::Passed));
}

#[tokio::test]
async fn a_destination_whose_published_data_cannot_be_read_skips_the_clauses_that_read_it() {
    let report =
        certify_destination::<MemoryDestination>(json!({ "store": "certify_unprobed" }), &Unprobed)
            .await;
    report.assert_passed();
    // Ownership is checked by the refusals, which need nothing read back.
    let unread = ["D-CHECK", "D-EPOCH", "D-STATE", "D-OWNED", "D-DROP"];
    for id in unread {
        assert_eq!(report.outcome(id), Some(&Outcome::Passed), "{report}");
    }
    let skipped = report
        .results
        .iter()
        .filter(|result| matches!(result.outcome, Outcome::Skipped(_)))
        .count();
    assert_eq!(skipped, report.results.len() - unread.len(), "{report}");
}

#[tokio::test]
async fn the_memory_destination_is_certified() {
    let report = certify_destination::<MemoryDestination>(
        json!({ "store": "certify" }),
        &MemoryProbe("certify"),
    )
    .await;
    report.assert_passed();
    assert_eq!(report.outcome("D-HIST"), Some(&Outcome::Passed), "{report}");
}

#[tokio::test]
async fn the_sqlite_destination_is_certified() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("certify.db");
    let config = json!({ "path": path });
    let probe = SqliteProbe(path.clone());
    // Certified again, the database it certified before passes alike.
    for config in [config.clone(), config] {
        let report = certify_destination::<SqliteDestination>(config, &probe).await;
        report.assert_passed();
        assert_eq!(report.outcome("D-HIST"), Some(&Outcome::Passed), "{report}");
    }
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
            let history = report.outcome("D-HIST");
            assert_eq!(history, Some(&Outcome::Passed), "{format}: {report}");
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

#[tokio::test]
async fn the_change_source_is_certified_and_moves_its_slot_only_when_committed() {
    let slots = [
        (None, true),
        (Some("certified"), true),
        (Some("forgets"), false),
    ];
    for (slot, replayable) in slots {
        let config = json!({
            "seed": 11,
            "streams": [{
                "name": "accounts",
                "keys": 12,
                "snapshot_partitions": 2,
                "changes": 9,
                "batch_rows": 3,
                "replayable": replayable,
            }],
            "slot": slot,
        });
        let report = certify_source::<ChangesSource>(config).await;
        report.assert_passed();
        assert_eq!(report.outcome("S-ACK"), Some(&Outcome::Passed), "{report}");
        let partition = report.outcome("S-PARTITION");
        assert_eq!(partition, Some(&Outcome::Passed), "{report}");
    }
}

#[tokio::test]
async fn the_change_source_is_certified_again_against_the_slot_an_earlier_run_moved() {
    let config = json!({
        "seed": 13,
        "streams": [{ "name": "accounts", "keys": 6, "changes": 9, "batch_rows": 3 }],
        "slot": "certified_twice",
    });
    for run in 1..=2 {
        let report = certify_source::<ChangesSource>(config.clone()).await;
        assert_eq!(
            report.outcome("S-ACK"),
            Some(&Outcome::Passed),
            "run {run}: {report}"
        );
    }
}

#[tokio::test]
async fn the_log_source_is_certified_and_commits_offsets_only_when_told() {
    for replayable in [true, false] {
        let config = json!({
            "seed": 17,
            "group": format!("certified_{replayable}"),
            "streams": [{
                "name": "events",
                "partitions": 3,
                "messages": 12,
                "batch_rows": 4,
                "replayable": replayable,
            }],
        });
        let report = certify_source::<LogSource>(config).await;
        report.assert_passed();
        assert_eq!(report.outcome("S-ACK"), Some(&Outcome::Passed), "{report}");
        // A log's partitions never end: no single read covers one.
        assert!(
            matches!(report.outcome("S-PARTITION"), Some(Outcome::Skipped(_))),
            "{report}"
        );
    }
}

#[tokio::test]
async fn a_log_whose_partitions_end_covers_each_once_however_it_is_planned() {
    let config = json!({
        "seed": 23,
        "group": "certified_bounded",
        "streams": [{
            "name": "events", "partitions": 3, "messages": 12, "batch_rows": 4, "bounded": true,
        }],
    });
    let report = certify_source::<LogSource>(config).await;
    report.assert_passed();
    let partition = report.outcome("S-PARTITION");
    assert_eq!(partition, Some(&Outcome::Passed), "{report}");
}

#[tokio::test(start_paused = true)]
async fn a_log_that_grows_as_it_is_read_is_certified() {
    // A following read of it never goes quiet: it is asked to stop while messages still arrive.
    let config = json!({
        "seed": 19,
        "group": "certified_growing",
        "streams": [{ "name": "events", "partitions": 2, "messages": 6, "per_second": 20 }],
    });
    let report = certify_source::<LogSource>(config).await;
    report.assert_passed();
    assert_eq!(report.outcome("S-STOP"), Some(&Outcome::Passed), "{report}");
}
