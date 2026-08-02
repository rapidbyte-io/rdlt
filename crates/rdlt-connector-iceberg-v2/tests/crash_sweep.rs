#![cfg(feature = "failpoints")]
//! The crash sweep — every FAIL_POINTS point × 3 actions through the
//! ENGINE against the live fixture: armed twice, recovered disarmed,
//! exact totals with a duplicate-free identity history. Its own
//! binary, selected by name from the `make test TARGET=sweep` gate;
//! skip-not-fail without a runtime.

#[path = "cases/common.rs"]
mod common;

use std::path::Path;

use common::CatalogFixture;
use rdlt_connector_iceberg_v2::destination::{FAIL_POINTS, Shell};
use rdlt_connector_sdk::spi::StreamSpec;
use rdlt_connector_sdk::spi::core::failpoint::fail;
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
    let shell = Shell::from_value(fixture.doc(namespace)).map_err(|e| e.to_string())?;
    let config = EngineConfig::new("ice-sweep-v2").with_workdir(workdir.to_path_buf());
    match tokio::spawn(Engine::new(config, source(), shell).run()).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(e.to_string()),
        Err(join) => Err(format!("panicked: {join}")),
    }
}

/// Every point × action: crash armed twice, recover disarmed —
/// exactly-once proven by totals AND a duplicate-free identity set,
/// with the fired matrix pinned complete.
#[tokio::test(flavor = "multi_thread")]
async fn every_fail_point_recovers_exactly_once() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };

    let mut fired = std::collections::BTreeSet::new();
    for (i, &point) in FAIL_POINTS.iter().enumerate() {
        for action in ACTIONS {
            let namespace = format!("sweepv2_{i}_{}", action.replace(['*', '-', '>'], "_"));
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
                .filter_map(|s| s.get("added-records").and_then(|v| v.parse::<u64>().ok()))
                .sum();
            assert_eq!(
                total, TOTAL_ROWS,
                "[{point} / {action}] exactly-once violated"
            );

            let identities: Vec<(String, String, String)> = snapshots
                .iter()
                .filter_map(|s| {
                    Some((
                        s.get("rdlt.pipeline")?.clone(),
                        s.get("rdlt.load-id")?.clone(),
                        s.get("rdlt.commit-seq")?.clone(),
                    ))
                })
                .collect();
            let unique: std::collections::BTreeSet<_> = identities.iter().collect();
            assert_eq!(
                unique.len(),
                identities.len(),
                "[{point} / {action}] duplicate commit identity in history"
            );
        }
    }

    let expected: std::collections::BTreeSet<_> = FAIL_POINTS
        .iter()
        .flat_map(|p| ACTIONS.iter().map(move |a| (*p, *a)))
        .collect();
    assert_eq!(fired, expected, "the armed-fire matrix must be complete");
}

/// The registry names exactly the points armed in the sources — the
/// self-check before container minutes are spent (the ungated twin
/// lives in cases/test_gating.rs).
#[test]
fn the_registry_matches_the_sources() {
    rdlt_testkit::assert_registry_matches_sources(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .as_path(),
        &[FAIL_POINTS],
    );
}
