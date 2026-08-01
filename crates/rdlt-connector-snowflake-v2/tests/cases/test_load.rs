//! Full-load semantics against the live service: Replace leaves exactly
//! the newest run's rows, and a repeat load at the same schema issues
//! no schema statements (the read-before-write economy, proven on the
//! real catalog).

use rdlt_connector_sdk::spi::StreamSpec;
use rdlt_connector_sdk::spi::core::WriteMode;
use rdlt_connector_snowflake_v2::destination::{Shell, testhook};
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::{MemoryBatch, MemorySource, MemoryStream};
use serde_json::json;

use super::common::{config_for, credentials, scratch_schema};

fn source_of(rows: Vec<serde_json::Value>) -> MemorySource {
    MemorySource::new(vec![MemoryStream::new(
        StreamSpec::new("events"),
        vec![MemoryBatch::new(rows).with_checkpoint(1)],
    )])
}

#[tokio::test]
async fn a_replace_load_leaves_only_the_newest_rows() {
    let Some(creds) = credentials() else { return };
    let schema = scratch_schema("rep");
    let doc = config_for(&creds, &schema);
    let config = {
        use rdlt_connector_sdk::config::Document;
        rdlt_connector_snowflake_v2::destination::SnowflakeConfig::from_value(doc.clone())
            .expect("valid")
    };

    for (pipeline, rows) in [
        (
            "sf-rep-1",
            vec![json!({"id": 1}), json!({"id": 2}), json!({"id": 3})],
        ),
        ("sf-rep-2", vec![json!({"id": 10}), json!({"id": 11})]),
    ] {
        let workdir = tempfile::tempdir().expect("workdir");
        Engine::new(
            EngineConfig::new(pipeline)
                .with_workdir(workdir.path().join("wal"))
                .with_write_mode(WriteMode::Replace),
            source_of(rows),
            Shell::from_value(doc.clone()).expect("valid"),
        )
        .run()
        .await
        .expect("the load settles");
    }

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
    assert_eq!(landed[0][0], "2", "only the second run's rows survive");

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

/// The economy, live: after a load created the table, the same ensure
/// against the REAL catalog renders zero statements.
#[tokio::test]
async fn a_repeat_ensure_against_the_live_catalog_emits_nothing() {
    let Some(creds) = credentials() else { return };
    let schema = scratch_schema("eco");
    let doc = config_for(&creds, &schema);
    let config = {
        use rdlt_connector_sdk::config::Document;
        rdlt_connector_snowflake_v2::destination::SnowflakeConfig::from_value(doc.clone())
            .expect("valid")
    };

    let workdir = tempfile::tempdir().expect("workdir");
    Engine::new(
        EngineConfig::new("sf-eco").with_workdir(workdir.path().join("wal")),
        source_of(vec![json!({"id": 1, "note": "a"})]),
        Shell::from_value(doc).expect("valid"),
    )
    .run()
    .await
    .expect("the load settles");

    // Read the real catalog and re-render the ensure against it.
    let columns = testhook::read_catalog(&config, "events")
        .await
        .expect("catalog read");
    assert!(!columns.is_empty(), "the load created the table");
    let mut catalog = testhook::Catalog::default();
    catalog.observe("events", columns.clone());
    let schema_shape = rdlt_connector_sdk::spi::core::TableSchema {
        table: rdlt_connector_sdk::spi::core::TableName::from("events"),
        parent: None,
        columns: columns
            .iter()
            .map(|name| rdlt_connector_sdk::spi::core::ColumnDef {
                name: name.to_lowercase(),
                column_type: rdlt_connector_sdk::spi::core::ColumnType::scalar(
                    rdlt_connector_sdk::spi::core::LogicalType::Utf8,
                ),
                nullable: true,
                provenance: rdlt_connector_sdk::spi::core::Provenance::Inferred,
            })
            .collect(),
    };
    let again = testhook::ensure_table_sql(
        "sf-eco",
        &schema_shape,
        &rdlt_connector_sdk::spi::core::WriteMode::Append,
        rdlt_connector_snowflake_v2::destination::TableType::Permanent,
        None,
        &catalog,
    );
    assert!(again.is_empty(), "steady state, live: {again:?}");

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
