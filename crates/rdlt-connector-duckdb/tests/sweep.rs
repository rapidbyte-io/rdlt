//! Crash sweeps for the STRATEGY arms: every fail point × action × strategy,
//! with armed-fire pins and crash/rerun convergence proven per strategy.
//!
//! The armed-fire pins are the anti-vacuousness instrument — a sweep that
//! tolerates a crash point which never fires proves nothing, so each point must
//! demonstrably fail an armed attempt before its recovery is credited.
//!
//! ONE test function, deliberately: fail points are process-global, so
//! splitting these across tests lets one test's armed point fire inside
//! another running concurrently.
//!
//! Run with `--features failpoints` (the `make test TARGET=sweep` gate).

#![cfg(feature = "failpoints")]

mod common;

use std::path::Path;

use common::{StructuredSource, batch, bools, i64s, one_unit, strs};
use rdlt_connector::WriteMode;
use rdlt_connector::core::failpoint::fail;
use rdlt_connector_duckdb::dest::{DestOptions, DuckDb};
use rdlt_engine::{Engine, EngineConfig};

fn feed() -> StructuredSource {
    // One commit unit (single batch): scoped/scd2 tables need their full
    // feed in one unit — the sweep exercises crash edges, not the
    // split-feed rejection (tests/refinements.rs pins that).
    one_unit(
        "kv",
        &["k"],
        batch(&[
            ("k", i64s(&[Some(1), Some(1), Some(2), Some(3)])),
            ("seq", i64s(&[Some(1), Some(9), Some(5), None])),
            ("day", i64s(&[Some(1), Some(1), Some(1), Some(2)])),
            (
                "v",
                strs(&[Some("k1-old"), Some("k1-new"), Some("k2"), Some("k3")]),
            ),
            (
                "gone",
                bools(&[Some(false), Some(false), Some(false), Some(false)]),
            ),
        ]),
    )
}

async fn attempt(workdir: &Path, dest: &DuckDb) -> Result<(), String> {
    let mut config = EngineConfig::new("sweep");
    config.write_mode = WriteMode::Merge {
        key: vec!["k".into()],
    };
    config.workdir = Some(workdir.to_path_buf());
    // Spawned: the "panic" action must land as a JoinError, not abort the
    // test (the engine crash_sweep harness pattern).
    let engine = Engine::new(config, feed(), dest.clone());
    match tokio::spawn(engine.run()).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(e.to_string()),
        Err(join) => Err(format!("panicked: {join}")),
    }
}

/// Canonical converged state per strategy: (k, v) pairs sorted — dedup_sort
/// (seq desc) makes k=1 resolve to "k1-new" everywhere.
fn canon(dest: &DuckDb, scd2: bool) -> String {
    let filter = if scd2 {
        " WHERE _rdlt_valid_to IS NULL"
    } else {
        ""
    };
    dest.query_string(&format!(
        "SELECT string_agg(k::VARCHAR || ':' || v, ',' ORDER BY k) FROM kv{filter}"
    ))
    .expect("canon query")
}

#[tokio::test(flavor = "multi_thread")]
async fn strategy_arms_survive_crash_sweep() {
    let strategies: Vec<(&str, serde_json::Value, bool)> = vec![
        ("delete_insert", serde_json::json!({}), false),
        (
            "upsert",
            serde_json::json!({
                "merge_strategy": "upsert",
                "tables": {"kv": {"hard_delete": "gone",
                                   "dedup_sort": {"column": "seq", "order": "desc"}}}
            }),
            false,
        ),
        (
            "scd2_retire",
            serde_json::json!({
                "merge_strategy": "scd2",
                "tables": {"kv": {"scd2": {"absent": "retire"}}}
            }),
            true,
        ),
        (
            "merge_scope",
            serde_json::json!({
                "tables": {"kv": {"merge_scope": ["day"],
                                   "dedup_sort": {"column": "seq", "order": "desc"}}}
            }),
            false,
        ),
    ];
    // The uncrashed reference state per strategy, established first.
    let expected = "1:k1-new,2:k2,3:k3";

    let mut fired: std::collections::BTreeSet<(&str, &str)> = std::collections::BTreeSet::new();
    for (label, options, scd2) in &strategies {
        for &point in rdlt_connector_duckdb::dest::FAIL_POINTS {
            for action in ["return", "panic", "1*off->return"] {
                let dir = tempfile::tempdir().expect("tempdir");
                let workdir = dir.path().join("wal");
                let dest = DuckDb::open(dir.path().join("sweep.duckdb"))
                    .unwrap()
                    .options(DestOptions::from_value(options.clone()).unwrap())
                    .unwrap();

                fail::cfg(point, action).expect("configure fail point");
                let armed1 = attempt(&workdir, &dest).await;
                // Second run still armed: a crash during recovery itself.
                let armed2 = attempt(&workdir, &dest).await;
                fail::remove(point);
                if armed1.is_err() || armed2.is_err() {
                    fired.insert((label, point));
                }

                let recovered = attempt(&workdir, &dest).await;
                assert!(
                    recovered.is_ok(),
                    "[{label} / {point} / {action}] recovery failed: {recovered:?}"
                );
                assert_eq!(
                    canon(&dest, *scd2),
                    expected,
                    "[{label} / {point} / {action}] crash/rerun did not converge"
                );
                if *scd2 {
                    // Convergence for history: replayed loads must not
                    // double-close or duplicate versions — every key exactly
                    // one ACTIVE version, and no closed k=2/k=3 churn.
                    let closed = dest
                        .query_string(
                            "SELECT coalesce(count(*), 0)::VARCHAR FROM kv \
                             WHERE _rdlt_valid_to IS NOT NULL AND k <> 1",
                        )
                        .unwrap();
                    assert_eq!(
                        closed, "0",
                        "[{label} / {point} / {action}] unchanged keys churned history"
                    );
                }
            }
        }
    }

    // Armed-fire pin: every (strategy, point) must actually fire — a missing
    // entry means a crash_point! site went dead (vacuous sweep).
    let mut expected_fired = std::collections::BTreeSet::new();
    for (label, _, _) in &strategies {
        for &point in rdlt_connector_duckdb::dest::FAIL_POINTS {
            expected_fired.insert((*label, point));
        }
    }
    assert_eq!(fired, expected_fired, "armed-fire pin diverged");
}
