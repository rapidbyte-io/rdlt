//! Crash-point sweep against the real account.
//!
//! Every registered point × three actions × the write modes that reach it:
//! crash, crash again during recovery, then run clean and require exactly-once
//! totals. A destination whose commit protocol is only tested on the happy path
//! is a destination whose exactly-once claim rests on nothing.
//!
//! With one ingestion mechanism there is no path axis left, so the matrix is
//! smaller than it was even though it now covers more: the points multiply by
//! modes alone, and the modes are chosen per point rather than crossed blindly.
//! Append and Replace differ in how the target is prepared, so both run
//! everywhere. Merge differs only in how the unit PUBLISHES — a staging table,
//! a merge statement and a deduplicating window — so it runs at that point and
//! not at the ones where it would re-test the same code under a third name.
//!
//! Credential-gated, and slow by nature — each cell runs three loads against a
//! SaaS warehouse. Run with `--features failpoints`.

#![cfg(feature = "failpoints")]

use rdlt_connector::StreamSpec;
use rdlt_connector::core::WriteMode;
use rdlt_connector::core::failpoint::fail;
use rdlt_connector_snowflake::dest::{Snowflake, SnowflakeConfig};
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::memory::{MemoryBatch, MemorySource, MemoryStream};
use rdlt_testkit::snowflake::{credentials, scratch_schema};
use serde_json::json;

const TOTAL_ROWS: u64 = 40;
const PIPELINE: &str = "sf-sweep";

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

fn config_in(schema: &str) -> Option<SnowflakeConfig> {
    let creds = credentials()?;
    Some(
        SnowflakeConfig::from_value(json!({
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
        }))
        .expect("valid config"),
    )
}

async fn attempt(
    workdir: &std::path::Path,
    config: &SnowflakeConfig,
    mode: &WriteMode,
) -> Result<(), String> {
    let dest = Snowflake::new(config.clone()).expect("valid config");
    let mut engine_config = EngineConfig::new(PIPELINE);
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
    let mut expected = vec![
        "sf.stage.write",
        "sf.stage.upload",
        "sf.unit.publish",
        "sf.receipt.visible",
    ];
    expected.sort_unstable();
    assert_eq!(registry, expected, "update BOTH the const and this list");
}

/// The write modes each point is swept with.
///
/// Not every mode times every point: Merge's protocol differs only at the
/// publish, and running it at the others would spend warehouse time re-proving
/// the code Append already covers there. Stated as a rule rather than left
/// implicit, so a point added later has to answer the question.
fn modes_for(point: &str) -> Vec<WriteMode> {
    let mut modes = vec![WriteMode::Append, WriteMode::Replace];
    if point == "sf.unit.publish" {
        modes.push(WriteMode::Merge {
            key: vec!["id".into()],
        });
    }
    modes
}

fn mode_label(mode: &WriteMode) -> &'static str {
    match mode {
        WriteMode::Append => "append",
        WriteMode::Replace => "replace",
        WriteMode::Merge { .. } => "merge",
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn sweep_snowflake_destination() {
    let Some(admin) = config_in("PUBLIC") else {
        return;
    };

    let mut fired: std::collections::BTreeSet<&str> = std::collections::BTreeSet::new();
    let mut schemas: Vec<String> = Vec::new();
    let mut cells = 0usize;
    let started = std::time::Instant::now();

    for &point in rdlt_connector_snowflake::dest::FAIL_POINTS {
        for action in ["return", "panic", "1*off->return"] {
            for mode in modes_for(point) {
                // A fresh schema per cell: leftover state from a previous cell
                // would make a broken recovery look like a working one, which
                // is the failure this sweep exists to catch.
                let schema = scratch_schema("sweep");
                schemas.push(schema.clone());
                let config = config_in(&schema).expect("credentials");
                let dir = tempfile::tempdir().expect("tempdir");
                let workdir = dir.path().join("wal");

                fail::cfg(point, action).expect("configure fail point");
                let armed1 = attempt(&workdir, &config, &mode).await;
                // Still armed: a crash during recovery itself.
                let armed2 = attempt(&workdir, &config, &mode).await;
                fail::remove(point);
                if armed1.is_err() || armed2.is_err() {
                    fired.insert(point);
                }

                let recovered = attempt(&workdir, &config, &mode).await;
                let label = format!("[{point} / {action} / {}]", mode_label(&mode));
                assert!(recovered.is_ok(), "{label} recovery failed: {recovered:?}");
                assert_eq!(
                    count_rows(&config).await,
                    TOTAL_ROWS,
                    "{label} exactly-once violated"
                );

                // The parts a crash left on this host are gone once the load
                // settles. Checked per cell rather than once at the end: the
                // directory is derived per load, so a single check would only
                // ever see the last one.
                let local = rdlt_connector_snowflake::dest::testhook::local_part_dir(
                    PIPELINE,
                    &last_load_id(&config).await,
                );
                let residue = std::fs::read_dir(&local)
                    .map(|entries| entries.count())
                    .unwrap_or(0);
                assert_eq!(
                    residue, 0,
                    "{label} left {residue} local part(s) in {local:?}"
                );

                cells += 1;
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

    // Anti-vacuousness: every point must have failed at least one armed
    // attempt. A crash site that went dead — moved, renamed, or placed after
    // the code it was meant to interrupt — fails here instead of passing
    // silently.
    let expected: std::collections::BTreeSet<&str> = rdlt_connector_snowflake::dest::FAIL_POINTS
        .iter()
        .copied()
        .collect();
    assert_eq!(
        fired, expected,
        "armed-fire pin diverged — a missing entry means a crash site went dead"
    );

    println!(
        "sweep: {cells} cells in {:.1} min",
        started.elapsed().as_secs_f64() / 60.0
    );
}

/// The load id the destination last committed for this pipeline.
///
/// Read back rather than guessed: the local part directory is derived from it,
/// and a guessed id would point the residue check at a directory that never
/// existed — which passes for the wrong reason.
async fn last_load_id(config: &SnowflakeConfig) -> String {
    rdlt_connector_snowflake::dest::testhook::connect_and_run(
        config,
        "SELECT \"load_id\" FROM \"_rdlt_commits\" ORDER BY \"commit_seq\" DESC LIMIT 1",
    )
    .await
    .unwrap_or_default()
}
