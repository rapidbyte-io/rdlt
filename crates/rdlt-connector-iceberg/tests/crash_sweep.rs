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
use rdlt_connector_iceberg::destination::{Config, FAIL_POINTS, Shell, testhook};
use rdlt_connector_sdk::config::Document;
use rdlt_connector_sdk::spi::StreamSpec;
use rdlt_connector_sdk::spi::core::failpoint::fail;
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::{MemoryBatch, MemorySource, MemoryStream};
use serde_json::json;

const TOTAL_ROWS: u64 = 4;
const ACTIONS: [&str; 3] = ["return", "panic", "1*off->return"];

/// ONE stream, deliberately (042 fix round 1's measurement): a
/// MULTI-stream sweep was built to drive the partial-publish shape
/// through the engine and it exposed a PRE-EXISTING ENGINE defect
/// instead — rdlt-engine's WAL recovery scan truncates the replay span
/// POSITIONALLY at the last checkpoint record
/// (wal/resume/scan.rs), so an interleaved co-stream's segment with no
/// checkpoint of its OWN stream after it is both REPLAYED and then
/// RE-EXTRACTED — double-apply, proven on main (3/4 control runs,
/// identical duplicate-identity histories) with this connector's code
/// untouched. Until the engine's scan is per-stream-coverage aware, any
/// multi-stream crash sweep is a coin flip; the connector's own
/// multi-table partial-publish convergence is pinned DETERMINISTICALLY
/// by the backend-direct cells in
/// `cases/test_exactly_once.rs` instead.
const TABLES: [&str; 1] = ["events"];

fn source() -> MemorySource {
    MemorySource::new(
        TABLES
            .into_iter()
            .map(|table| {
                MemoryStream::new(
                    StreamSpec::new(table),
                    vec![
                        MemoryBatch::new(vec![json!({"seq": 1}), json!({"seq": 2})])
                            .with_checkpoint(2),
                        MemoryBatch::new(vec![json!({"seq": 3}), json!({"seq": 4})])
                            .with_checkpoint(4),
                    ],
                )
            })
            .collect(),
    )
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

            for table in TABLES {
                let snapshots = fixture.snapshot_summaries(&namespace, table).await;
                let total: u64 = snapshots
                    .iter()
                    .filter_map(|s| s.get("added-records").and_then(|v| v.parse::<u64>().ok()))
                    .sum();
                assert_eq!(
                    total, TOTAL_ROWS,
                    "[{point} / {action}] exactly-once violated on `{table}`: {snapshots:?}"
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
                    "[{point} / {action}] duplicate commit identity in `{table}` history"
                );
            }

            // THE STATE DOC (042 fix round 1): recovery through the
            // receipt fast path must leave the cursor persisted —
            // publish writes state LAST, so the receipt.visible crash
            // leaves data without state, and a recovery that returns
            // receipts without ever re-writing it ends this green
            // while the NEXT run re-ingests everything.
            let state = testhook::read_raw_state(
                &Config::from_value(fixture.doc(&namespace)).expect("valid"),
                &[namespace.clone()],
                &testhook::scope_of("ice-sweep-v2"),
            )
            .await
            .expect("state readable")
            .unwrap_or_else(|| {
                panic!("[{point} / {action}] no state doc after recovery — the next run would re-ingest")
            });
            assert!(
                state.contains("ice-sweep-v2"),
                "[{point} / {action}] the state doc names its pipeline: {state}"
            );

            // The cross-run duplication observable itself: a FOURTH run
            // (fresh load id, disarmed) resumes from the persisted
            // cursor and must add NOTHING.
            let rerun = attempt(&fixture, &namespace, &workdir).await;
            assert!(
                rerun.is_ok(),
                "[{point} / {action}] re-run failed: {rerun:?}"
            );
            for table in TABLES {
                let total: u64 = fixture
                    .snapshot_summaries(&namespace, table)
                    .await
                    .iter()
                    .filter_map(|s| s.get("added-records").and_then(|v| v.parse::<u64>().ok()))
                    .sum();
                assert_eq!(
                    total, TOTAL_ROWS,
                    "[{point} / {action}] a fresh-load re-run re-ingested `{table}` — the \
                     recovered state doc carried a stale cursor"
                );
            }
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
