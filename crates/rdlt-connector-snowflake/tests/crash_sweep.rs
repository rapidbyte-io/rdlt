//! Crash-point sweep against the real account.
//!
//! Every registered point × three actions × both write modes × both ingestion
//! paths: crash, crash again during recovery, then run clean and require
//! exactly-once totals. A destination whose commit protocol is only tested on
//! the happy path is a destination whose exactly-once claim rests on nothing.
//!
//! Credential-gated, and slow by nature — each cell runs three loads against a
//! SaaS warehouse. Run with `--features failpoints`.

#![cfg(feature = "failpoints")]

use rdlt_connector::StreamSpec;
use rdlt_connector::core::WriteMode;
use rdlt_connector::core::failpoint::fail;
use rdlt_connector_snowflake::dest::{S3Stage, Snowflake, SnowflakeConfig, Stage};
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::memory::{MemoryBatch, MemorySource, MemoryStream};
use rdlt_testkit::snowflake::{credentials, scratch_schema, stage_credentials};
use serde_json::json;

const TOTAL_ROWS: u64 = 40;

/// Four checkpointed batches, so a crash lands mid-load rather than only ever
/// at the single boundary a one-batch source has.
fn source() -> MemorySource {
    let batches = (0..4)
        .map(|b| {
            MemoryBatch::new(
                (0..10)
                    .map(|i| json!({"id": b * 10 + i, "note": format!("row-{b}-{i}")}))
                    .collect(),
            )
            .with_checkpoint(json!({"batch": b}))
        })
        .collect();
    MemorySource::new(vec![MemoryStream::new(StreamSpec::new("events"), batches)])
}

fn config_in(schema: &str, staged: bool) -> Option<SnowflakeConfig> {
    let creds = credentials()?;
    let mut doc = json!({
        "account": creds.account,
        "user": creds.user,
        "database": creds.database,
        "schema": schema,
        "warehouse": creds.warehouse,
        "role": creds.role,
        "auth": {"key_pair": {
            "private_key": creds.private_key_path,
            "passphrase": creds.passphrase,
        }},
    });
    if staged {
        let stage = stage_credentials()?;
        let mut s3 = S3Stage::new(stage.bucket, stage.access_key, stage.secret_key);
        s3.region = stage.region;
        s3.endpoint = stage.endpoint;
        s3.prefix = stage.prefix;
        doc["stage"] = serde_json::to_value(Stage::s3(s3)).expect("the stage serializes");
    }
    Some(SnowflakeConfig::from_value(doc).expect("valid config"))
}

async fn attempt(
    workdir: &std::path::Path,
    config: &SnowflakeConfig,
    mode: &WriteMode,
) -> Result<(), String> {
    let dest = Snowflake::new(config.clone()).expect("valid config");
    let mut engine_config = EngineConfig::new("sf-sweep");
    engine_config.workdir = Some(workdir.to_path_buf());
    engine_config.write_mode = mode.clone();
    match tokio::spawn(Engine::new(engine_config, source(), dest).run()).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(e.to_string()),
        Err(join) => Err(format!("panicked: {join}")),
    }
}

async fn count_rows(config: &SnowflakeConfig) -> u64 {
    rdlt_connector_snowflake::dest::testhook::connect_and_run(
        config,
        "SELECT count(*) FROM \"EVENTS\"",
    )
    .await
    .ok()
    .and_then(|text| text.parse().ok())
    .unwrap_or(0)
}

/// Registry discipline: the crate's exported list is pinned here, and the
/// sweep iterates exactly it.
#[test]
fn registry_is_pinned() {
    let mut registry: Vec<&str> = rdlt_connector_snowflake::dest::FAIL_POINTS.to_vec();
    registry.sort_unstable();
    let mut expected = vec!["sf.stage.write", "sf.unit.publish", "sf.receipt.visible"];
    expected.sort_unstable();
    assert_eq!(registry, expected, "update BOTH the const and this list");
}

/// Which points a given ingestion path can actually reach.
///
/// `sf.stage.write` brackets the object-store put, which the insert path never
/// performs. Encoding this keeps the armed-fire pin honest in both directions:
/// a dead crash site still fails the pin, and a point is not demanded from a
/// path whose code cannot contain it.
fn reachable(staged: bool) -> Vec<&'static str> {
    rdlt_connector_snowflake::dest::FAIL_POINTS
        .iter()
        .copied()
        .filter(|point| staged || *point != "sf.stage.write")
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn sweep_snowflake_destination() {
    let Some(admin) = config_in("PUBLIC", false) else {
        return;
    };
    let has_bucket = stage_credentials().is_some();

    let mut fired: std::collections::BTreeSet<(&str, bool)> = std::collections::BTreeSet::new();
    let mut schemas: Vec<String> = Vec::new();

    for &point in rdlt_connector_snowflake::dest::FAIL_POINTS {
        for action in ["return", "panic", "1*off->return"] {
            for staged in [false, true] {
                if staged && !has_bucket {
                    continue;
                }
                if !reachable(staged).contains(&point) {
                    continue;
                }
                for mode in [WriteMode::Append, WriteMode::Replace] {
                    // A fresh schema per cell: leftover state from a previous
                    // cell would make a broken recovery look like a working
                    // one, which is the failure this sweep exists to catch.
                    let schema = scratch_schema("sweep");
                    schemas.push(schema.clone());
                    let config = config_in(&schema, staged).expect("credentials");
                    let dir = tempfile::tempdir().expect("tempdir");
                    let workdir = dir.path().join("wal");

                    fail::cfg(point, action).expect("configure fail point");
                    let armed1 = attempt(&workdir, &config, &mode).await;
                    // Still armed: a crash during recovery itself.
                    let armed2 = attempt(&workdir, &config, &mode).await;
                    fail::remove(point);
                    if armed1.is_err() || armed2.is_err() {
                        fired.insert((point, staged));
                    }

                    let recovered = attempt(&workdir, &config, &mode).await;
                    let label = format!(
                        "[{point} / {action} / {mode:?} / {}]",
                        if staged { "staged" } else { "insert" }
                    );
                    assert!(recovered.is_ok(), "{label} recovery failed: {recovered:?}");
                    assert_eq!(
                        count_rows(&config).await,
                        TOTAL_ROWS,
                        "{label} exactly-once violated"
                    );
                }
            }
        }
    }

    for schema in schemas {
        let _ = rdlt_connector_snowflake::dest::testhook::apply(
            &admin,
            &format!(
                "DROP SCHEMA IF EXISTS \"{}\".\"{}\"",
                admin.database.to_uppercase(),
                schema
            ),
        )
        .await;
    }

    // Anti-vacuousness: every point reachable on a path must have failed at
    // least one armed attempt there. A crash site that went dead — moved,
    // renamed, or placed after the code it was meant to interrupt — fails
    // here instead of passing silently.
    let mut expected: std::collections::BTreeSet<(&str, bool)> = reachable(false)
        .into_iter()
        .map(|point| (point, false))
        .collect();
    if has_bucket {
        expected.extend(reachable(true).into_iter().map(|point| (point, true)));
    }
    assert_eq!(
        fired, expected,
        "armed-fire pin diverged — a missing entry means a crash site went dead"
    );
}
