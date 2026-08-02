//! Certification and end-to-end: the sdk destination kit over the Shell
//! (certified = passes conformance), then the engine driving real JSON
//! feeds into a database file — shredding, resume, in-span dedup, and
//! cross-run column drift.

use async_trait::async_trait;
use rdlt_connector_duckdb::destination::{Config, Shell};
use rdlt_connector_sdk::spi::StreamSpec;
use rdlt_connector_sdk::spi::core::{TableName, WriteMode};
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::{
    MemoryBatch, MemorySource, MemoryStream, TableProbe, assert_conformant, verify_destination,
};
use serde_json::json;

use super::common::{rows_in, scalar};

/// Counts alongside the LIVE shell through a READ-ONLY instance per
/// probe. This cannot be `testhook::count_rows`: the kit probes while
/// the shell under test stays open, and a second READ-WRITE instance on
/// the same file replays-and-truncates the live instance's WAL (the
/// client.rs instance model), silently swallowing every commit the kit
/// makes afterwards — measured here as D4/D8 "found 0". A read-only
/// instance replays the WAL without touching it. A table the kit has
/// not created yet counts as 0.
struct FileCount(std::path::PathBuf);

#[async_trait]
impl TableProbe for FileCount {
    async fn count(&self, table: &TableName) -> u64 {
        let read_only = duckdb::Config::default()
            .access_mode(duckdb::AccessMode::ReadOnly)
            .expect("read-only config");
        let Ok(conn) = duckdb::Connection::open_with_flags(&self.0, read_only) else {
            return 0;
        };
        // The kit's table names pass through quoting to stay one rule.
        let ident = format!("\"{}\"", table.as_str().replace('"', "\"\""));
        conn.query_row(&format!("SELECT count(*) FROM {ident}"), [], |row| {
            row.get::<_, u64>(0)
        })
        .unwrap_or(0)
    }
}

#[tokio::test]
async fn the_sdk_kit_certifies_the_shell() {
    let dir = tempfile::tempdir().expect("dir");
    let file = dir.path().join("kit.duckdb");
    let shell = Shell::new(Config::new(&file)).expect("valid");
    assert_conformant(verify_destination(&shell, &FileCount(file)).await);
}

/// Nested JSON through the engine: structs stay struct-native (dot
/// syntax answers), arrays shred into a child table that joins its
/// parent on the lineage ids.
#[tokio::test(flavor = "multi_thread")]
async fn nested_documents_land_as_structs_and_child_tables() {
    let dir = tempfile::tempdir().expect("dir");
    let file = dir.path().join("nested.duckdb");
    let dest = Shell::new(Config::new(&file)).expect("valid");
    let source = MemorySource::single_stream(
        StreamSpec::new("users"),
        vec![
            json!({"id": 1, "name": "lin", "home": {"city": "Warsaw"},
                   "tags": [{"tag": "alpha"}, {"tag": "beta"}]}),
            json!({"id": 2, "name": "mo", "home": {"city": "Oslo"}, "tags": []}),
        ],
    );
    let config = EngineConfig::new("nested").with_workdir(dir.path().join("wal"));
    let report = Engine::new(config, source, dest).run().await.expect("run");

    assert_eq!(report.total_rows(), 4, "two parents + two children");
    assert_eq!(rows_in(&file, "users"), 2);
    assert_eq!(rows_in(&file, "users__tags"), 2);
    assert_eq!(
        scalar(&file, "SELECT home.city FROM users WHERE id = 1"),
        "Warsaw",
        "struct-native lowering answers dot syntax"
    );
    assert_eq!(
        scalar(
            &file,
            "SELECT c.tag FROM users__tags c \
             JOIN users p ON c._rdlt_parent_id = p._rdlt_id \
             WHERE p.id = 1 ORDER BY c._rdlt_pos LIMIT 1"
        ),
        "alpha",
        "children join their parent on the lineage ids"
    );
}

/// A second run of the same pipeline resumes from the persisted cursor:
/// zero new rows read, zero rows duplicated.
#[tokio::test(flavor = "multi_thread")]
async fn a_second_incremental_run_reads_nothing_new() {
    let dir = tempfile::tempdir().expect("dir");
    let file = dir.path().join("resume.duckdb");
    let feed = || {
        MemorySource::new(vec![MemoryStream::new(
            StreamSpec::new("ticks"),
            vec![
                MemoryBatch::new(vec![json!({"n": 1})]).with_checkpoint(1),
                MemoryBatch::new(vec![json!({"n": 2})]).with_checkpoint(2),
            ],
        )])
    };

    // Each run gets its own Shell so the instance is closed before the
    // count reads the file (sequential re-open — the documented-safe
    // pattern; an instance held open across a count loses later writes).
    let dest = Shell::new(Config::new(&file)).expect("valid");
    Engine::new(EngineConfig::new("resume"), feed(), dest)
        .run()
        .await
        .expect("run 1");
    assert_eq!(rows_in(&file, "ticks"), 2);

    let dest = Shell::new(Config::new(&file)).expect("valid");
    let second = Engine::new(EngineConfig::new("resume"), feed(), dest)
        .run()
        .await
        .expect("run 2");
    assert_eq!(second.total_rows(), 0, "the cursor already covers the feed");
    assert_eq!(rows_in(&file, "ticks"), 2, "and nothing duplicated");
}

/// Two versions of one key inside a single commit span: the LAST version
/// wins, deterministically.
#[tokio::test(flavor = "multi_thread")]
async fn in_span_merge_dedup_keeps_the_last_version() {
    let dir = tempfile::tempdir().expect("dir");
    let file = dir.path().join("lastwins.duckdb");
    let dest = Shell::new(Config::new(&file)).expect("valid");
    let source = MemorySource::single_stream(
        StreamSpec::new("kv").with_primary_key(["k"]),
        vec![json!({"k": 5, "v": "stale"}), json!({"k": 5, "v": "fresh"})],
    );
    let config = EngineConfig::new("lastwins").with_write_mode(WriteMode::Merge {
        key: vec!["k".into()],
    });
    Engine::new(config, source, dest).run().await.expect("run");

    assert_eq!(rows_in(&file, "kv"), 1);
    assert_eq!(scalar(&file, "SELECT v FROM kv WHERE k = 5"), "fresh");
}

/// Cross-run column drift publishes BY NAME: a later run that discovers
/// an extra column must land every value in its named column — and the
/// pre-drift row backfills NULL for the newcomer.
#[tokio::test(flavor = "multi_thread")]
async fn cross_run_column_drift_publishes_by_name() {
    let dir = tempfile::tempdir().expect("dir");
    let file = dir.path().join("drift.duckdb");
    let dest = Shell::new(Config::new(&file)).expect("valid");

    // Run 1: columns (a, b).
    let source =
        MemorySource::single_stream(StreamSpec::new("t"), vec![json!({"a": "a1", "b": "b1"})]);
    Engine::new(EngineConfig::new("drift"), source, dest.clone())
        .run()
        .await
        .expect("run 1");

    // Run 2: column c appears.
    let source = MemorySource::single_stream(
        StreamSpec::new("t"),
        vec![json!({"c": "c2", "b": "b2", "a": "a2"})],
    );
    Engine::new(EngineConfig::new("drift"), source, dest)
        .run()
        .await
        .expect("run 2");

    assert_eq!(rows_in(&file, "t"), 2);
    assert_eq!(
        scalar(&file, "SELECT a FROM t WHERE b = 'b2'"),
        "a2",
        "values land in their named columns"
    );
    assert_eq!(
        scalar(&file, "SELECT c FROM t WHERE b = 'b2'"),
        "c2",
        "the drift column carries its value"
    );
    assert_eq!(
        scalar(
            &file,
            "SELECT count(*)::VARCHAR FROM t WHERE b = 'b1' AND c IS NULL"
        ),
        "1",
        "the pre-drift row backfills NULL, never a shifted value"
    );
}
