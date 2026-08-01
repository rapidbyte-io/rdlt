//! One real load, end to end: rows leave as parquet parts, land through
//! COPY inside the unit, and the local staging directory is EMPTY once
//! the load settles — the cleanup claim checked rather than trusted.

use rdlt_connector_sdk::spi::StreamSpec;
use rdlt_connector_snowflake_v2::destination::{Shell, testhook};
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::{MemoryBatch, MemorySource, MemoryStream};
use serde_json::json;

use super::common::{config_for, credentials, scratch_schema};

#[tokio::test]
async fn an_append_load_lands_every_row_and_leaves_no_local_residue() {
    let Some(creds) = credentials() else { return };
    let schema = scratch_schema("ing");
    let doc = config_for(&creds, &schema);
    let shell = Shell::from_value(doc.clone()).expect("valid document");
    let config = {
        use rdlt_connector_sdk::config::Document;
        rdlt_connector_snowflake_v2::destination::SnowflakeConfig::from_value(doc).expect("valid")
    };

    let source = MemorySource::new(vec![MemoryStream::new(
        StreamSpec::new("events"),
        vec![
            MemoryBatch::new(vec![
                json!({"id": 1, "note": "a"}),
                json!({"id": 2, "note": "b"}),
            ])
            .with_checkpoint(1),
            MemoryBatch::new(vec![json!({"id": 3, "note": "c"})]).with_checkpoint(2),
        ],
    )]);

    let workdir = tempfile::tempdir().expect("workdir");
    let pipeline = "sf-ing";
    let report = Engine::new(
        EngineConfig::new(pipeline).with_workdir(workdir.path().join("wal")),
        source,
        shell,
    )
    .run()
    .await
    .expect("the load settles");
    assert!(!report.tables.is_empty(), "the run reports its table");

    let landed = testhook::rows(
        &config,
        &format!(
            "SELECT COUNT(*) AS N FROM \"{}\".\"{}\".\"EVENTS\"",
            config.database.to_uppercase(),
            schema.to_uppercase()
        ),
        &["n"],
    )
    .await
    .expect("read back");
    assert_eq!(landed[0][0], "3", "every row landed exactly once");

    // The local part directory for this load must be gone or empty:
    // parts are removed after upload, and the directory after settle.
    let local = testhook::local_part_dir(pipeline, report.load_id.as_str());
    let leftover = std::fs::read_dir(&local)
        .map(|entries| entries.count())
        .unwrap_or(0);
    assert_eq!(leftover, 0, "no local residue at {}", local.display());

    let _ = testhook::connect_and_run(
        &config,
        &format!(
            "DROP SCHEMA IF EXISTS \"{}\".\"{}\" CASCADE",
            config.database.to_uppercase(),
            schema.to_uppercase()
        ),
    )
    .await;
}
