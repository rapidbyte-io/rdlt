//! T009: end-to-end jsonl → DuckDB through the ENGINE, incremental across two runs —
//! the flagship benchmark path as a supported connector (spec US1 / SC-003 path).

use rdlt_connector_duckdb::dest::DuckDb;
use rdlt_connector_file::FileSource;
use rdlt_engine::{Engine, EngineConfig};

fn source_for(dir: &std::path::Path) -> FileSource {
    FileSource::from_yaml(&format!(
        "streams:\n  - name: events\n    format: jsonl\n    path: \"{}/*.jsonl\"\n",
        dir.display()
    ))
    .expect("config")
}

#[tokio::test(flavor = "multi_thread")]
async fn incremental_jsonl_to_duckdb() {
    let data = tempfile::tempdir().expect("tempdir");
    let db = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        data.path().join("a.jsonl"),
        "{\"id\":1,\"tags\":[{\"t\":\"x\"}]}\n{\"id\":2,\"tags\":[]}\n",
    )
    .expect("write");

    let dest = DuckDb::open(db.path().join("out.duckdb")).expect("open db");
    let report = Engine::new(
        EngineConfig::new("files"),
        source_for(data.path()),
        dest.clone(),
    )
    .run()
    .await
    .expect("run 1");
    assert_eq!(report.total_rows(), 3, "2 roots + 1 child");
    assert_eq!(dest.count_rows("events").expect("count"), 2);

    // Append + new file; run 2 loads exactly the delta.
    use std::io::Write;
    let mut fh = std::fs::OpenOptions::new()
        .append(true)
        .open(data.path().join("a.jsonl"))
        .expect("open");
    writeln!(fh, "{{\"id\":3,\"tags\":[]}}").expect("append");
    drop(fh);
    std::fs::write(data.path().join("b.jsonl"), "{\"id\":4,\"tags\":[]}\n").expect("write");

    let report = Engine::new(
        EngineConfig::new("files"),
        source_for(data.path()),
        dest.clone(),
    )
    .run()
    .await
    .expect("run 2");
    assert_eq!(report.total_rows(), 2, "only ids 3 and 4");
    assert_eq!(
        dest.count_rows("events").expect("count"),
        4,
        "no duplicates"
    );
}
