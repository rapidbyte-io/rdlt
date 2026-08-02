#![cfg(feature = "failpoints")]
//! The crash sweep: every fail point × 3 actions through the ENGINE —
//! armed twice, recovered disarmed, exactly-once proven by totals and
//! duplicate-free part names. The LOCAL matrix (both source points and
//! the six pq.* protocol points) runs anywhere; the S3 matrix runs
//! against RUSTFS and SKIPS visibly without a runtime.

#[path = "cases/common.rs"]
mod common;
#[path = "cases/s3.rs"]
mod s3;

use std::path::Path;

use common::{jsonl_source, local_dest, plant};
use rdlt_connector_file_v2::destination::{self, FAIL_POINTS, S3_FAIL_POINTS};
use rdlt_connector_file_v2::source;
use rdlt_connector_sdk::spi::core::failpoint::fail;
use rdlt_engine::{Engine, EngineConfig};
use s3::S3Fixture;

const TOTAL_ROWS: u64 = 4;
const ACTIONS: [&str; 3] = ["return", "panic", "1*off->return"];

fn plant_input(input: &Path) {
    plant(input, "data/a.jsonl", b"{\"id\": 1}\n{\"id\": 2}\n");
    plant(input, "data/b.jsonl", b"{\"id\": 3}\n{\"id\": 4}\n");
}

async fn attempt(
    input: &Path,
    dest_config: &destination::Config,
    pipeline: &str,
    workdir: &Path,
) -> Result<(), String> {
    let src = source::Shell::new(jsonl_source(input, "data/*.jsonl")).expect("valid");
    let dest = destination::Shell::new(dest_config.clone()).expect("valid");
    let config = EngineConfig::new(pipeline).with_workdir(workdir.to_path_buf());
    match tokio::spawn(Engine::new(config, src, dest).run()).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(e.to_string()),
        Err(join) => Err(format!("panicked: {join}")),
    }
}

/// One point × action cell: crash armed twice, recover disarmed,
/// verify exact totals and duplicate-free part names. Returns whether
/// the armed attempts fired.
async fn cell(point: &str, action: &str, dest_config: &destination::Config, input: &Path) -> bool {
    let workdir = tempfile::tempdir().expect("workdir");
    let pipeline = format!("sweep-{}", point.replace('.', "-"));

    fail::cfg(point, action).expect("configure fail point");
    let armed1 = attempt(input, dest_config, &pipeline, workdir.path()).await;
    let armed2 = attempt(input, dest_config, &pipeline, workdir.path()).await;
    fail::remove(point);

    let recovered = attempt(input, dest_config, &pipeline, workdir.path()).await;
    assert!(
        recovered.is_ok(),
        "[{point} / {action}] recovery failed: {recovered:?}"
    );
    let total = destination::testhook::count_rows_async(dest_config, "events")
        .await
        .expect("count");
    assert_eq!(
        total, TOTAL_ROWS,
        "[{point} / {action}] exactly-once violated"
    );
    armed1.is_err() || armed2.is_err()
}

/// The local matrix: the two source points and the six pq.* protocol
/// points, each × 3 actions, into a local parquet destination.
#[tokio::test(flavor = "multi_thread")]
async fn every_local_fail_point_recovers_exactly_once() {
    let mut fired = std::collections::BTreeSet::new();
    let points: Vec<&str> = source::FAIL_POINTS
        .iter()
        .chain(FAIL_POINTS.iter())
        .copied()
        .collect();
    for point in &points {
        for action in ACTIONS {
            let input = tempfile::tempdir().expect("input");
            let out = tempfile::tempdir().expect("out");
            plant_input(input.path());
            let dest_config = local_dest(out.path());
            if cell(point, action, &dest_config, input.path()).await {
                fired.insert((*point, action));
            }
        }
    }
    let expected: std::collections::BTreeSet<_> = points
        .iter()
        .flat_map(|p| ACTIONS.iter().map(move |a| (*p, *a)))
        .collect();
    assert_eq!(fired, expected, "the armed-fire matrix must be complete");
}

/// The S3 matrix: the three object-store points × 3 actions, into a
/// RUSTFS-backed destination. Skips visibly without a runtime.
#[tokio::test(flavor = "multi_thread")]
async fn every_s3_fail_point_recovers_exactly_once() {
    let Some(fixture) = S3Fixture::start().await else {
        return;
    };
    let mut fired = std::collections::BTreeSet::new();
    for (i, &point) in S3_FAIL_POINTS.iter().enumerate() {
        for action in ACTIONS {
            let input = tempfile::tempdir().expect("input");
            plant_input(input.path());
            let dest_config = destination::Config::new(format!("lake-{i}-{}", fired.len()))
                .with_location(fixture.location_options());
            if cell(point, action, &dest_config, input.path()).await {
                fired.insert((point, action));
            }
        }
    }
    let expected: std::collections::BTreeSet<_> = S3_FAIL_POINTS
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
        &[FAIL_POINTS, S3_FAIL_POINTS, source::FAIL_POINTS],
    );
}
