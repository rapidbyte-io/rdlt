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

use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::{CrashDestination, FaultPoint, MemoryDestination, MemorySource};

use super::common::three_batch_source;

fn source() -> MemorySource {
    three_batch_source()
}

fn config(workdir: &Path) -> EngineConfig {
    let mut config = EngineConfig::new("wal-lifecycle");
    config = config.with_workdir(workdir.to_path_buf());
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

/// 037 final-review wave, item 1: a `Wal::open` failure strictly AFTER
/// `recover_wal` has already opened a destination session must not
/// abandon that session without closing it — the recorded sibling gap
/// this test pins closes. Forced cheaply: `Wal::open` calls
/// `std::fs::create_dir_all(workdir/wal)`, so planting a plain FILE at
/// that path makes it fail deterministically (`ErrorKind::AlreadyExists`
/// against a non-directory) without needing any fault-injection
/// machinery. `recover_wal` itself is unaffected — its own manifest scan
/// tolerates the same path being unreadable and simply finds nothing to
/// replay — so the session opens successfully and only the later
/// `Wal::open` call fails, isolating the fix's call site exactly.
#[tokio::test]
async fn a_wal_open_failure_after_session_recovery_still_closes_the_session() {
    let dir = tempfile::tempdir().expect("tempdir");
    let workdir = dir.path().join("work");
    std::fs::create_dir_all(&workdir).expect("workdir");
    // Occupy the WAL directory's path with a file, not a directory.
    std::fs::write(workdir.join("wal"), b"not a directory").expect("plant a blocking file");

    let dest = MemoryDestination::new();
    let error = Engine::new(config(&workdir), source(), dest.clone())
        .run()
        .await
        .expect_err("Wal::open must fail against a blocking file");
    assert!(
        matches!(error, rdlt_core::RdltError::Wal { .. }),
        "the failure is Wal::open's own, not something else: {error}"
    );
    assert_eq!(
        dest.closes(),
        1,
        "the session recover_wal opened must be closed best-effort, not \
         abandoned — otherwise a competitor TTL-waits for a holder that \
         already gave up"
    );
}
