//! T130 — the WAL directory's lifecycle across a run boundary.
//!
//! `wal::clear` removing the directory is the only thing that stops a long-lived
//! pipeline's workdir from growing without bound, and it is called from exactly
//! one place: after `drain_loader` returns Ok. Replacing it with a no-op leaves
//! every run's manifest and segments behind forever while every test still
//! passes, because no test looked at the directory afterwards. The companion
//! assertion matters just as much: a FAILED run must keep its residue, or there
//! is nothing for recovery to replay.

use std::path::Path;

use rdlt_connector::StreamSpec;
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::{
    CrashDestination, FaultPoint, MemoryBatch, MemoryDestination, MemorySource, MemoryStream,
};
use serde_json::json;

fn source() -> MemorySource {
    let batches = (0..3)
        .map(|b| {
            MemoryBatch::new(vec![
                json!({"id": b * 2, "name": format!("r{b}a")}),
                json!({"id": b * 2 + 1, "name": format!("r{b}b")}),
            ])
            .with_checkpoint(json!({"batch": b}))
        })
        .collect();
    MemorySource::new(vec![MemoryStream::new(StreamSpec::new("s"), batches)])
}

fn config(workdir: &Path) -> EngineConfig {
    let mut config = EngineConfig::new("wal-lifecycle");
    config.workdir = Some(workdir.to_path_buf());
    config
}

#[tokio::test]
async fn a_clean_run_removes_the_wal_directory() {
    let dir = tempfile::tempdir().expect("tempdir");
    let workdir = dir.path().join("work");
    let wal_dir = workdir.join("wal");

    let report = Engine::new(config(&workdir), source(), MemoryDestination::new())
        .run()
        .await
        .expect("clean run");
    assert_eq!(report.total_rows(), 6, "the run really did work");

    assert!(
        !wal_dir.exists(),
        "a clean finish has nothing to replay, so the WAL directory must be gone \
         — otherwise a long-lived pipeline's workdir grows without bound: {wal_dir:?}"
    );
}

#[tokio::test]
async fn a_failed_run_keeps_its_wal_for_replay() {
    let dir = tempfile::tempdir().expect("tempdir");
    let workdir = dir.path().join("work");
    let wal_dir = workdir.join("wal");

    let dest = CrashDestination::new(MemoryDestination::new(), FaultPoint::BeforeWrite(1));
    let error = Engine::new(config(&workdir), source(), dest)
        .run()
        .await
        .expect_err("the fault point must fail the run");
    assert!(
        !error.to_string().is_empty(),
        "a failed run reports why: {error}"
    );

    assert!(
        wal_dir.exists(),
        "a failed run's WAL is the ONLY record of work the destination may not \
         have received — clearing it here would turn a replay into a re-extraction \
         at best, and silent loss at worst: {wal_dir:?}"
    );
    assert!(
        wal_dir.join("manifest.jsonl").exists(),
        "the manifest is what recovery scans"
    );
}
