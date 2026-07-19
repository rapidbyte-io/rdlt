//! T057: DuckDB destination — in-process integration (end-to-end through the engine)
//! plus the public destination conformance suite (the certification gate).

use async_trait::async_trait;
use rdlt_dest_duckdb::DuckDb;
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::conformance::dest::verify_destination;
use rdlt_testkit::{MemorySource, TableProbe, assert_conformant};
use serde_json::json;

struct DuckProbe(DuckDb);

#[async_trait]
impl TableProbe for DuckProbe {
    async fn count(&self, table: &rdlt_connector::TableName) -> u64 {
        match self.0.count_rows(table.as_str()) {
            Ok(count) => count,
            Err(e) => panic!("probe failed on {table}: {e}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn duckdb_destination_is_conformant() {
    let dir = tempfile::tempdir().expect("tempdir");
    let dest = DuckDb::open(dir.path().join("conf.duckdb")).expect("open db");
    let probe = DuckProbe(dest.clone());
    assert_conformant(verify_destination(&dest, &probe).await);
}

#[tokio::test(flavor = "multi_thread")]
async fn end_to_end_nested_sync_into_duckdb() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("e2e.duckdb");
    let dest = DuckDb::open(&db_path).expect("open db");

    let source = MemorySource::single_stream(
        rdlt_connector::StreamSpec::new("users"),
        vec![
            json!({"id": 1, "name": "ada", "profile": {"city": "NYC"},
                   "tags": [{"label": "x"}, {"label": "y"}]}),
            json!({"id": 2, "name": "grace", "profile": {"city": "LA"}, "tags": []}),
        ],
    );

    let mut config = EngineConfig::new("duck-e2e");
    config.workdir = Some(dir.path().join("work"));
    let report = Engine::new(config, source, dest.clone())
        .run()
        .await
        .expect("run");
    assert_eq!(report.total_rows(), 4, "2 users + 2 tag children");

    assert_eq!(dest.count_rows("users").expect("users"), 2);
    assert_eq!(dest.count_rows("users__tags").expect("tags"), 2);
    // Struct-native lowering: the nested field is queryable with dot syntax.
    let city = dest
        .query_string("SELECT profile.city FROM users WHERE id = 1")
        .expect("struct query");
    assert_eq!(city, "NYC");
    // Lineage: children join to parents on hex ids.
    let label = dest
        .query_string(
            "SELECT t.label FROM users__tags t JOIN users u ON t._rdlt_parent_id = u._rdlt_id \
             WHERE u.id = 1 ORDER BY t._rdlt_pos LIMIT 1",
        )
        .expect("join query");
    assert_eq!(label, "x");
}

/// A second incremental run against the same database file resumes and appends.
#[tokio::test(flavor = "multi_thread")]
async fn incremental_run_against_duckdb() {
    use rdlt_testkit::{MemoryBatch, MemoryStream};
    let dir = tempfile::tempdir().expect("tempdir");
    let dest = DuckDb::open(dir.path().join("incr.duckdb")).expect("open db");
    let batches = || {
        vec![
            MemoryBatch::new(vec![json!({"n": 1})]).with_checkpoint(1),
            MemoryBatch::new(vec![json!({"n": 2})]).with_checkpoint(2),
        ]
    };

    let source = MemorySource::new(vec![MemoryStream::new(
        rdlt_connector::StreamSpec::new("nums"),
        batches(),
    )]);
    Engine::new(EngineConfig::new("incr"), source, dest.clone())
        .run()
        .await
        .expect("run 1");
    assert_eq!(dest.count_rows("nums").expect("count"), 2);

    // Second run: resume from cursor 2 → nothing new, no duplicates.
    let source = MemorySource::new(vec![MemoryStream::new(
        rdlt_connector::StreamSpec::new("nums"),
        batches(),
    )]);
    let report = Engine::new(EngineConfig::new("incr"), source, dest.clone())
        .run()
        .await
        .expect("run 2");
    assert_eq!(report.total_rows(), 0);
    assert_eq!(dest.count_rows("nums").expect("count"), 2, "no duplicates");
}
