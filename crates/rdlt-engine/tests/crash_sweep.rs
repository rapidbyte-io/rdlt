//! Deterministic crash-point sweep (feature 003 US1, research R20, gate G2).
//!
//! For EVERY registered fail point: arm it (error-return AND panic), run a
//! multi-commit pipeline until it dies, run again still-armed (a crash DURING
//! recovery), then disarm and recover — asserting exactly-once visibility of all
//! 100 source rows every single time, for every bundled in-process destination,
//! in both Append and Replace modes.
//!
//! Gate G2.2: `sweep_covers_entire_registry` pins the swept list to the union of
//! every crate's registry const — an instrumented-but-unswept boundary fails here.

#![cfg(feature = "failpoints")]

use std::path::Path;

use rdlt_connector::{Destination, StreamSpec};
use rdlt_core::WriteMode;
use rdlt_core::failpoint::fail;
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::{MemoryBatch, MemoryDestination, MemorySource, MemoryStream};
use serde_json::json;

const TOTAL_ROWS: u64 = 100;

/// Engine-owned fail points (registry discipline, gate G2.2): every
/// `crash_point!` site in rdlt-engine MUST appear here — grep-auditable, and
/// `sweep_covers_entire_registry` pins the union against the expected list.
const ENGINE_POINTS: &[&str] = &[
    "wal.segment.write",
    "wal.segment.fsync",
    "wal.manifest.append",
    "wal.manifest.fsync",
    "session.after_ensure",
    "session.after_write",
    "session.after_commit",
];

/// 4 checkpointed batches × 25 rows → 4 commits under EveryCheckpoints(1):
/// every sweep iteration exercises multi-commit recovery, not a single commit.
fn source() -> MemorySource {
    let batches = (0..4)
        .map(|b| {
            MemoryBatch::new(
                (0..25)
                    .map(|i| json!({"id": b * 25 + i, "name": format!("row-{b}-{i}")}))
                    .collect(),
            )
            .with_checkpoint(json!({"batch": b}))
        })
        .collect();
    MemorySource::new(vec![MemoryStream::new(StreamSpec::new("s"), batches)])
}

fn config(workdir: &Path, mode: &WriteMode) -> EngineConfig {
    let mut config = EngineConfig::new("sweep");
    config.workdir = Some(workdir.to_path_buf());
    config.write_mode = mode.clone();
    config
}

/// Run one attempt; a panic anywhere inside the engine is contained and reported
/// as a crash (that's the point of the panic-action sweep).
async fn attempt<D: Destination + Clone>(
    workdir: &Path,
    dest: &D,
    mode: &WriteMode,
) -> Result<(), String> {
    let engine = Engine::new(config(workdir, mode), source(), dest.clone());
    match tokio::spawn(engine.run()).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(e.to_string()),
        Err(join) => Err(format!("panicked: {join}")),
    }
}

/// The sweep core: (point × action) → crash, crash-during-recovery, recover, count.
async fn sweep<D, F, C>(points: &[&str], mode: WriteMode, make_dest: F, count: C)
where
    D: Destination + Clone,
    F: Fn(&Path) -> D,
    C: Fn(&D) -> u64,
{
    for &point in points {
        for action in ["return", "panic"] {
            let dir = tempfile::tempdir().expect("tempdir");
            let workdir = dir.path().join("wal");
            let dest = make_dest(dir.path());

            fail::cfg(point, action).expect("configure fail point");
            // First run: dies at the point (or completes if the point is
            // unreachable under this mode — vacuously fine, counts still checked).
            let _ = attempt(&workdir, &dest, &mode).await;
            // Second run STILL armed: a crash during recovery itself.
            let _ = attempt(&workdir, &dest, &mode).await;
            fail::remove(point);

            let recovered = attempt(&workdir, &dest, &mode).await;
            assert!(
                recovered.is_ok(),
                "[{point} / {action} / {mode:?}] recovery failed: {recovered:?}"
            );
            assert_eq!(
                count(&dest),
                TOTAL_ROWS,
                "[{point} / {action} / {mode:?}] exactly-once violated"
            );
        }
    }
}

fn engine_and<'a>(dest_points: &[&'a str]) -> Vec<&'a str> {
    ENGINE_POINTS.iter().chain(dest_points).copied().collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn sweep_memory_destination() {
    let points = engine_and(&[]);
    sweep(
        &points,
        WriteMode::Append,
        |_dir| MemoryDestination::new(),
        |dest| dest.committed_rows("s").len() as u64,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn sweep_parquet_destination() {
    let points = engine_and(rdlt_dest_parquet::FAIL_POINTS);
    for mode in [WriteMode::Append, WriteMode::Replace] {
        sweep(
            &points,
            mode,
            |dir| rdlt_dest_parquet::ParquetDir::open(dir.join("out")).expect("open"),
            |dest| dest.count_rows("s").expect("count"),
        )
        .await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn sweep_duckdb_destination() {
    let points = engine_and(rdlt_dest_duckdb::FAIL_POINTS);
    for mode in [WriteMode::Append, WriteMode::Replace] {
        sweep(
            &points,
            mode,
            |dir| rdlt_dest_duckdb::DuckDb::open(dir.join("out.duckdb")).expect("open"),
            |dest| dest.count_rows("s").expect("count"),
        )
        .await;
    }
}

/// Gate G2.2: the swept set IS the registry — no silently unswept boundary.
#[test]
fn sweep_covers_entire_registry() {
    let mut registry: Vec<&str> = ENGINE_POINTS
        .iter()
        .chain(rdlt_dest_parquet::FAIL_POINTS)
        .chain(rdlt_dest_duckdb::FAIL_POINTS)
        .copied()
        .collect();
    registry.sort_unstable();
    let mut expected = vec![
        "wal.segment.write",
        "wal.segment.fsync",
        "wal.manifest.append",
        "wal.manifest.fsync",
        "session.after_ensure",
        "session.after_write",
        "session.after_commit",
        "pq.replace.truncate",
        "pq.staged.sync",
        "pq.part.rename",
        "pq.dir.fsync",
        "pq.state.write",
        "pq.receipt.write",
        "duck.append",
        "duck.tx.commit",
    ];
    expected.sort_unstable();
    assert_eq!(
        registry, expected,
        "a crate registered a fail point the sweep does not know (update BOTH \
         the registry const and this list — gate G2.2)"
    );
}
