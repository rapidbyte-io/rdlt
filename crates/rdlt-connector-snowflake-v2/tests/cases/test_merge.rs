//! Merge semantics against the live service — the suites that read
//! their results BACK, because nothing here enforces uniqueness and the
//! SQL alone carries correctness: upsert convergence, hard delete, and
//! the scd2 single-instant boundary.

use rdlt_connector_sdk::spi::core::WriteMode;
use rdlt_connector_snowflake_v2::destination::{Shell, testhook};
use rdlt_engine::{Engine, EngineConfig};
use serde_json::json;

use super::common::{KeyedSource, config_for, credentials, scratch_schema};

fn merge_mode() -> WriteMode {
    WriteMode::Merge {
        key: vec!["id".to_owned()],
    }
}

async fn run_load(doc: &serde_json::Value, pipeline: &str, rows: Vec<(i64, &'static str, bool)>) {
    let workdir = tempfile::tempdir().expect("workdir");
    Engine::new(
        EngineConfig::new(pipeline)
            .with_workdir(workdir.path().join("wal"))
            .with_write_mode(merge_mode()),
        KeyedSource { rows },
        Shell::from_value(doc.clone()).expect("valid"),
    )
    .run()
    .await
    .expect("the load settles");
}

fn qualified(
    config: &rdlt_connector_snowflake_v2::destination::SnowflakeConfig,
    schema: &str,
) -> String {
    format!(
        "\"{}\".\"{}\".\"EVENTS\"",
        config.database.to_uppercase(),
        schema.to_uppercase()
    )
}

async fn drop_schema(
    config: &rdlt_connector_snowflake_v2::destination::SnowflakeConfig,
    schema: &str,
) {
    let _ = testhook::connect_and_run(
        config,
        &format!(
            "DROP SCHEMA IF EXISTS \"{}\".\"{}\" CASCADE",
            config.database.to_uppercase(),
            schema.to_uppercase()
        ),
    )
    .await;
}

/// Two upsert loads over the same keys converge: the key set stays
/// unique and the newest values win.
#[tokio::test]
async fn an_upsert_converges_on_the_key_and_leaves_no_duplicates() {
    let Some(creds) = credentials() else { return };
    let schema = scratch_schema("ups");
    let mut doc = config_for(&creds, &schema);
    doc["merge_strategy"] = json!("upsert");
    let config = {
        use rdlt_connector_sdk::config::Document;
        rdlt_connector_snowflake_v2::destination::SnowflakeConfig::from_value(doc.clone())
            .expect("valid")
    };

    run_load(&doc, "sf-ups", vec![(1, "old", false), (2, "keep", false)]).await;
    run_load(&doc, "sf-ups", vec![(1, "new", false), (3, "add", false)]).await;

    let rows = testhook::rows(
        &config,
        &format!(
            "SELECT COUNT(*) AS N, COUNT(DISTINCT \"ID\") AS K FROM {}",
            qualified(&config, &schema)
        ),
        &["n", "k"],
    )
    .await
    .expect("read back");
    assert_eq!(rows[0][0], "3", "three keys, no duplicates");
    assert_eq!(rows[0][1], "3");

    let updated = testhook::rows(
        &config,
        &format!(
            "SELECT \"NOTE\" AS V FROM {} WHERE \"ID\" = 1",
            qualified(&config, &schema)
        ),
        &["v"],
    )
    .await
    .expect("read back");
    assert_eq!(updated[0][0], "new", "the newest value won");

    drop_schema(&config, &schema).await;
}

/// A hard-delete flag REMOVES the row — never a kept-but-flagged ghost.
#[tokio::test]
async fn a_hard_delete_flag_removes_the_row() {
    let Some(creds) = credentials() else { return };
    let schema = scratch_schema("hd");
    let mut doc = config_for(&creds, &schema);
    doc["merge_strategy"] = json!("upsert");
    doc["tables"] = json!({"events": {"hard_delete": "gone"}});
    let config = {
        use rdlt_connector_sdk::config::Document;
        rdlt_connector_snowflake_v2::destination::SnowflakeConfig::from_value(doc.clone())
            .expect("valid")
    };

    run_load(&doc, "sf-hd", vec![(1, "x", false), (2, "x", false)]).await;
    run_load(&doc, "sf-hd", vec![(1, "x", true)]).await;

    let rows = testhook::rows(
        &config,
        &format!("SELECT COUNT(*) AS N FROM {}", qualified(&config, &schema)),
        &["n"],
    )
    .await
    .expect("read back");
    assert_eq!(rows[0][0], "1", "the flagged row is gone, the other stays");

    drop_schema(&config, &schema).await;
}

/// scd2's retired and inserted versions meet at EXACTLY one instant —
/// the unit's captured timestamp, not a per-statement clock — so no
/// entity ever has a gap in which nothing is current.
#[tokio::test]
async fn scd2_versions_meet_exactly_because_the_unit_shares_one_instant() {
    let Some(creds) = credentials() else { return };
    let schema = scratch_schema("scd");
    let mut doc = config_for(&creds, &schema);
    doc["merge_strategy"] = json!("scd2");
    let config = {
        use rdlt_connector_sdk::config::Document;
        rdlt_connector_snowflake_v2::destination::SnowflakeConfig::from_value(doc.clone())
            .expect("valid")
    };

    run_load(&doc, "sf-scd", vec![(1, "v1", false)]).await;
    run_load(&doc, "sf-scd", vec![(1, "v2", false)]).await;

    // The retired version's valid_to must equal the new version's
    // valid_from, to the microsecond.
    let rows = testhook::rows(
        &config,
        &format!(
            "SELECT COUNT(*) AS MEETING FROM {t} a JOIN {t} b \
             ON a.\"_RDLT_VALID_TO\" = b.\"_RDLT_VALID_FROM\" \
             WHERE a.\"NOTE\" = 'v1' AND b.\"NOTE\" = 'v2'",
            t = qualified(&config, &schema)
        ),
        &["meeting"],
    )
    .await
    .expect("read back");
    assert_eq!(
        rows[0][0], "1",
        "retirement and insertion share one captured instant"
    );

    drop_schema(&config, &schema).await;
}
