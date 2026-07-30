//! Feature 015 T013 (FF7): crash sweep over the file family — source
//! points on the local matrix, dest object-store points in the
//! container-gated arm. Armed-fire pinned, crash/rerun exactly-once.
//!
//! Run with `--features failpoints` (the `make test TARGET=sweep` gate).
//! Fail points are process-global; nextest runs each test in its own
//! process, so the two arms never interleave.

#![cfg(feature = "failpoints")]

mod common;

use std::path::Path;

use rdlt_connector::core::failpoint::fail;
use rdlt_connector_duckdb::dest::DuckDb;
use rdlt_connector_file::FileSource;
use rdlt_engine::{Engine, EngineConfig};

const TOTAL_ROWS: u64 = 4;
const ACTIONS: [&str; 3] = ["return", "panic", "1*off->return"];

fn seed_local(dir: &Path) {
    std::fs::write(
        dir.join("a.jsonl"),
        "{\"seq\":1,\"v\":\"r1\"}\n{\"seq\":2,\"v\":\"r2\"}\n",
    )
    .expect("seed a");
    std::fs::write(
        dir.join("b.jsonl"),
        "{\"seq\":3,\"v\":\"r3\"}\n{\"seq\":4,\"v\":\"r4\"}\n",
    )
    .expect("seed b");
}

fn source_yaml(dir: &Path) -> String {
    format!(
        "streams:\n  - name: events\n    format: jsonl\n    path: \"{}/*.jsonl\"\n    primary_key: [seq]\n",
        dir.display()
    )
}

/// SOURCE points × actions: local jsonl → duckdb, exactly-once totals.
#[tokio::test(flavor = "multi_thread")]
async fn file_source_read_path_survives_crash_sweep() {
    let data = tempfile::tempdir().expect("tempdir");
    seed_local(data.path());
    let yaml = source_yaml(data.path());

    async fn attempt(workdir: &Path, db: &Path, yaml: &str) -> Result<(), String> {
        let source = FileSource::from_yaml(yaml).expect("config");
        let dest = DuckDb::open(db).map_err(|e| e.to_string())?;
        let mut config = EngineConfig::new("file-sweep");
        config.workdir = Some(workdir.to_path_buf());
        match tokio::spawn(Engine::new(config, source, dest).run()).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(e.to_string()),
            Err(join) => Err(format!("panicked: {join}")),
        }
    }

    let mut fired = std::collections::BTreeSet::new();
    for &point in rdlt_connector_file::source::FAIL_POINTS {
        for action in ACTIONS {
            let dir = tempfile::tempdir().expect("tempdir");
            let workdir = dir.path().join("wal");
            let db = dir.path().join("sweep.duckdb");

            fail::cfg(point, action).expect("configure fail point");
            let armed1 = attempt(&workdir, &db, &yaml).await;
            let armed2 = attempt(&workdir, &db, &yaml).await;
            fail::remove(point);
            if armed1.is_err() || armed2.is_err() {
                fired.insert((point, action));
            }

            let recovered = attempt(&workdir, &db, &yaml).await;
            assert!(
                recovered.is_ok(),
                "[{point} / {action}] recovery failed: {recovered:?}"
            );
            let dest = DuckDb::open(&db).unwrap();
            assert_eq!(
                dest.count_rows("events").unwrap(),
                TOTAL_ROWS,
                "[{point} / {action}] exactly-once violated"
            );
        }
    }
    let expected: std::collections::BTreeSet<_> = rdlt_connector_file::source::FAIL_POINTS
        .iter()
        .flat_map(|p| ACTIONS.iter().map(move |a| (*p, *a)))
        .collect();
    assert_eq!(fired, expected, "armed-fire pin diverged");
}

/// DEST object-store points × actions: local jsonl → the bucket,
/// exactly-once totals in the bucket (container-gated, skip-not-fail).
#[tokio::test(flavor = "multi_thread")]
async fn file_dest_s3_path_survives_crash_sweep() {
    use common::s3::S3Fixture;
    use rdlt_connector_file::dest::{FileDest, FileDestConfig};

    let Some(fixture) = S3Fixture::start().await else {
        return;
    };
    let data = tempfile::tempdir().expect("tempdir");
    seed_local(data.path());
    let yaml = source_yaml(data.path());

    let dest_points = rdlt_connector_file::dest::S3_FAIL_POINTS;

    let mut fired = std::collections::BTreeSet::new();
    for (i, &point) in dest_points.iter().enumerate() {
        for action in ACTIONS {
            let dir = tempfile::tempdir().expect("tempdir");
            let workdir = dir.path().join("wal");
            let prefix = format!("sweep/{i}/{}", action.replace(['*', '-', '>'], "_"));

            let attempt = |armed_prefix: String| {
                let yaml = yaml.clone();
                let options = fixture.location_options();
                let workdir = workdir.clone();
                async move {
                    let source = FileSource::from_yaml(&yaml).expect("config");
                    let dest = FileDest::from_config(
                        FileDestConfig::new(armed_prefix).with_location(options),
                    )
                    .map_err(|e| e.to_string())?;
                    let mut config = EngineConfig::new("file-s3-sweep");
                    config.workdir = Some(workdir);
                    match tokio::spawn(Engine::new(config, source, dest).run()).await {
                        Ok(Ok(_)) => Ok(()),
                        Ok(Err(e)) => Err(e.to_string()),
                        Err(join) => Err(format!("panicked: {join}")),
                    }
                }
            };

            fail::cfg(point, action).expect("configure fail point");
            let armed1 = attempt(prefix.clone()).await;
            let armed2 = attempt(prefix.clone()).await;
            fail::remove(point);
            if armed1.is_err() || armed2.is_err() {
                fired.insert((point, action));
            }

            let recovered = attempt(prefix.clone()).await;
            assert!(
                recovered.is_ok(),
                "[{point} / {action}] recovery failed: {recovered:?}"
            );
            let dest = FileDest::from_config(
                FileDestConfig::new(prefix.clone()).with_location(fixture.location_options()),
            )
            .expect("connect");
            assert_eq!(
                dest.count_rows_async("events").await.expect("count"),
                TOTAL_ROWS,
                "[{point} / {action}] exactly-once violated in the bucket"
            );
        }
    }
    let expected: std::collections::BTreeSet<_> = dest_points
        .iter()
        .flat_map(|p| ACTIONS.iter().map(move |a| (*p, *a)))
        .collect();
    assert_eq!(fired, expected, "armed-fire pin diverged");
}

/// The registry names exactly the crash points armed in this crate's sources.
///
/// The sweep's own `fired == registry` check cannot establish this: it compares
/// the registry against itself, so deleting a point from BOTH the code and the
/// list leaves it true while the matrix quietly shrinks. This reads the sources
/// instead, which is the only way a dropped point becomes visible.
///
/// THREE registries over one source tree, checked against their union: a single
/// one compared against this scope would report its siblings' points as
/// undeclared, and the shape of that false failure invites widening the registry
/// being checked — which is the opposite of what this protects.
#[test]
fn the_registry_matches_the_sources() {
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    rdlt_testkit::assert_registry_is_armed(
        &src,
        &[
            rdlt_connector_file::dest::FAIL_POINTS,
            rdlt_connector_file::dest::S3_FAIL_POINTS,
            rdlt_connector_file::source::FAIL_POINTS,
        ],
    );
}
