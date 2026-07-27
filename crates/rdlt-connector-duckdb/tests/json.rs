//! `Json` columns land as NATIVE DuckDB JSON rather than as text.
//!
//! This is the behavioural half of the `json_type` capability: the probe in
//! `probes.rs` proves the bundled build HAS a JSON type, and these cells prove
//! the destination actually uses it — a capability declared but not exercised
//! would let the engine plan around a type that never materializes.

use rdlt_connector::StreamSpec;
use rdlt_connector_duckdb::dest::DuckDb;
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::MemorySource;
use serde_json::json;

/// Nested lists take the engine's v1 Json escape hatch → a Json-typed column
/// reaches the destination. It must land as native JSON: the declared column
/// type is JSON and DuckDB's json_extract works on it directly.
#[tokio::test(flavor = "multi_thread")]
async fn json_columns_land_native_and_extract() {
    let dir = tempfile::tempdir().unwrap();
    let dest = DuckDb::open(dir.path().join("json.duckdb")).unwrap();
    let source = MemorySource::single_stream(
        StreamSpec::new("docs").with_primary_key(["id"]),
        vec![
            // list-of-lists → ColState::Json (shred/infer.rs escape hatch)
            json!({"id": 1, "matrix": [[1, 2], [3, 4]]}),
            json!({"id": 2, "matrix": [[5]]}),
        ],
    );
    Engine::new(EngineConfig::new("json"), source, dest.clone())
        .run()
        .await
        .unwrap();

    // The DECLARED column type is native JSON, not VARCHAR.
    let declared = dest
        .query_string(
            "SELECT data_type FROM information_schema.columns \
             WHERE table_name = 'docs' AND column_name = 'matrix'",
        )
        .unwrap();
    assert_eq!(
        declared, "JSON",
        "capability json_type: the column is native"
    );

    // Content round-trips and DuckDB's JSON functions work directly on it.
    let cell = dest
        .query_string(
            "SELECT CAST(json_extract(matrix, '$[1][0]') AS VARCHAR) \
             FROM docs WHERE id = 1",
        )
        .unwrap();
    assert_eq!(cell, "3", "json_extract over the loaded document");
}

/// Merge composes with native JSON: redelivering a key replaces its document.
#[tokio::test(flavor = "multi_thread")]
async fn json_columns_merge_by_key() {
    let dir = tempfile::tempdir().unwrap();
    let dest = DuckDb::open(dir.path().join("jsonm.duckdb")).unwrap();
    let mut config = EngineConfig::new("jsonm");
    config.write_mode = rdlt_connector::WriteMode::Merge {
        key: vec!["id".into()],
    };
    let first = MemorySource::single_stream(
        StreamSpec::new("docs").with_primary_key(["id"]),
        vec![json!({"id": 1, "matrix": [[1]]})],
    );
    Engine::new(config.clone(), first, dest.clone())
        .run()
        .await
        .unwrap();

    let second = MemorySource::single_stream(
        StreamSpec::new("docs").with_primary_key(["id"]),
        vec![json!({"id": 1, "matrix": [[9, 9]]})],
    );
    Engine::new(config, second, dest.clone())
        .run()
        .await
        .unwrap();

    assert_eq!(dest.count_rows("docs").unwrap(), 1);
    let cell = dest
        .query_string("SELECT CAST(json_extract(matrix, '$[0][1]') AS VARCHAR) FROM docs")
        .unwrap();
    assert_eq!(cell, "9", "merged document is the redelivered one");
}
