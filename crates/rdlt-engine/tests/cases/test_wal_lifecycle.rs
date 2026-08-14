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

/// A v3 manifest line, re-derived independently of the engine's encoder:
/// `{json}|{blake3-hex-of-the-json-bytes}`.
fn v3_line(record: &serde_json::Value) -> String {
    let json = record.to_string();
    format!("{json}|{}\n", blake3::hash(json.as_bytes()).to_hex())
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

/// Plant an unclearable WAL directory: `manifest` and a matching
/// default-rules sidecar, plus a write-protected subdirectory that
/// makes `remove_dir_all` fail with EACCES. Returns the unlocker.
fn plant_unclearable_wal(wal_dir: &Path, manifest: String) -> impl FnOnce() {
    use std::os::unix::fs::PermissionsExt;
    std::fs::create_dir_all(wal_dir).expect("wal dir");
    std::fs::write(wal_dir.join("manifest.jsonl"), manifest).expect("manifest");
    std::fs::write(
        wal_dir.join("rules.json"),
        serde_json::json!({"max_len": 63}).to_string(),
    )
    .expect("sidecar");
    let locked = wal_dir.join("locked");
    std::fs::create_dir(&locked).expect("locked dir");
    std::fs::write(locked.join("pin"), b"residue").expect("pinned file");
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o555))
        .expect("write-protect");
    move || {
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755))
            .expect("unlock for cleanup");
    }
}

/// Round-12, narrowed in round 13 to the arms where residue is a
/// HAZARD: a Damaged-class WAL (here: an unparseable rules sidecar)
/// that cannot be cleared refuses the run naming the directory —
/// proceeding would re-degrade every following run over the same
/// residue.
#[tokio::test]
async fn an_uncleanable_damaged_wal_refuses_the_run_naming_the_clear_failure() {
    let dir = tempfile::tempdir().expect("tempdir");
    let workdir = dir.path().join("work");
    let wal_dir = workdir.join("wal");
    let unlock = plant_unclearable_wal(
        &wal_dir,
        v3_line(&serde_json::json!({
            "rec": "run",
            "format_version": 3,
            "load_id": "stale",
            "pipeline": "wal-lifecycle",
        })),
    );
    // Damaged-class: the sidecar does not parse.
    std::fs::write(wal_dir.join("rules.json"), b"not json").expect("corrupt sidecar");

    let error = Engine::new(config(&workdir), source(), MemoryDestination::new())
        .run()
        .await
        .expect_err("the run must refuse over damaged residue it cannot clear");
    let text = error.to_string();
    assert!(
        text.contains("clearing the WAL directory")
            && text.contains("unresolved residue")
            && text.contains(&wal_dir.display().to_string()),
        "the refusal names the clear failure and the directory: {text}"
    );

    unlock();
}

/// Round-13: the DISCARD arm softens — its manifest holds nothing
/// replayable (resolved state), so a failed clear WARNS and the run
/// proceeds (main's old best-effort posture), the new run's records
/// appending after the resolved span. Refusing here would wedge a
/// healthy pipeline on a permissions accident.
#[tokio::test]
async fn an_uncleanable_discard_class_wal_still_runs_with_a_warning() {
    let dir = tempfile::tempdir().expect("tempdir");
    let workdir = dir.path().join("work");
    let wal_dir = workdir.join("wal");
    // A current-version header with no checkpoint: the Discard shape.
    let unlock = plant_unclearable_wal(
        &wal_dir,
        v3_line(&serde_json::json!({
            "rec": "run",
            "format_version": 3,
            "load_id": "stale",
            "pipeline": "wal-lifecycle",
        })),
    );

    let report = Engine::new(config(&workdir), source(), MemoryDestination::new())
        .run()
        .await
        .expect("resolved residue must not wedge the run");
    assert_eq!(report.total_rows(), 6, "the run did real work");
    assert!(
        wal_dir.join("locked").exists(),
        "the unclearable residue is still there — tolerated, not silently gone"
    );

    unlock();
}
