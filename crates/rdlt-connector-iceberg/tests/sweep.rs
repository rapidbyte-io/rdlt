//! Crash sweep over the Iceberg destination —
//! every `ICE_FAIL_POINTS` point × 3 actions against the LIVE catalog
//! fixture, armed-fire pinned, crash/rerun exactly-once with a
//! duplicate-free snapshot history.
//!
//! Run with `--features failpoints` (the `make test TARGET=sweep` gate).
//! Skip-not-fail without a container runtime.

#![cfg(feature = "failpoints")]

mod common;

use std::path::Path;

use common::CatalogFixture;
use rdlt_connector::StreamSpec;
use rdlt_connector::core::failpoint::fail;
use rdlt_connector_iceberg::IcebergDest;
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::{MemoryBatch, MemorySource, MemoryStream};
use serde_json::json;

const TOTAL_ROWS: u64 = 4;
const ACTIONS: [&str; 3] = ["return", "panic", "1*off->return"];

fn source() -> MemorySource {
    MemorySource::new(vec![MemoryStream::new(
        StreamSpec::new("events"),
        vec![
            MemoryBatch::new(vec![json!({"seq": 1}), json!({"seq": 2})]).with_checkpoint(2),
            MemoryBatch::new(vec![json!({"seq": 3}), json!({"seq": 4})]).with_checkpoint(4),
        ],
    )])
}

async fn attempt(fixture: &CatalogFixture, namespace: &str, workdir: &Path) -> Result<(), String> {
    let dest = IcebergDest::from_config(fixture.config(namespace)).map_err(|e| e.to_string())?;
    let mut config = EngineConfig::new("ice-sweep");
    config.workdir = Some(workdir.to_path_buf());
    match tokio::spawn(Engine::new(config, source(), dest).run()).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(e.to_string()),
        Err(join) => Err(format!("panicked: {join}")),
    }
}

/// DEST points × actions through the ENGINE into the live catalog:
/// crash armed twice, recover disarmed — exact totals, no duplicate
/// commit identity in the snapshot history.
#[tokio::test(flavor = "multi_thread")]
async fn iceberg_dest_survives_crash_sweep() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };

    let mut fired = std::collections::BTreeSet::new();
    for (i, &point) in rdlt_connector_iceberg::dest::ICE_FAIL_POINTS
        .iter()
        .enumerate()
    {
        for action in ACTIONS {
            let namespace = format!("sweep_{i}_{}", action.replace(['*', '-', '>'], "_"));
            let dir = tempfile::tempdir().expect("tempdir");
            let workdir = dir.path().join("wal");

            fail::cfg(point, action).expect("configure fail point");
            let armed1 = attempt(&fixture, &namespace, &workdir).await;
            let armed2 = attempt(&fixture, &namespace, &workdir).await;
            fail::remove(point);
            if armed1.is_err() || armed2.is_err() {
                fired.insert((point, action));
            }

            let recovered = attempt(&fixture, &namespace, &workdir).await;
            assert!(
                recovered.is_ok(),
                "[{point} / {action}] recovery failed: {recovered:?}"
            );

            let snapshots = fixture.snapshot_summaries(&namespace, "events").await;
            let total: u64 = snapshots
                .iter()
                .map(|s| s["added-records"].parse::<u64>().expect("count"))
                .sum();
            assert_eq!(
                total, TOTAL_ROWS,
                "[{point} / {action}] exactly-once violated"
            );
            let identities: Vec<(String, String, String)> = snapshots
                .iter()
                .map(|s| {
                    (
                        s["rdlt.pipeline"].clone(),
                        s["rdlt.load-id"].clone(),
                        s["rdlt.commit-seq"].clone(),
                    )
                })
                .collect();
            let unique: std::collections::BTreeSet<_> = identities.iter().collect();
            assert_eq!(
                unique.len(),
                identities.len(),
                "[{point} / {action}] duplicate commit identity in snapshot history"
            );
        }
    }

    let expected: std::collections::BTreeSet<_> = rdlt_connector_iceberg::dest::ICE_FAIL_POINTS
        .iter()
        .flat_map(|p| ACTIONS.iter().map(move |a| (*p, *a)))
        .collect();
    assert_eq!(fired, expected, "armed-fire pin diverged");
}
