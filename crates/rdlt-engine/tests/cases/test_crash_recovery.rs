//! US3 — Crash-safe, resumable runs (SC-002).
//!
//! Every crash-matrix row (design doc §6): kill the run at a precise stage, restart,
//! and assert the destination converges to EXACTLY the uninterrupted result — no
//! duplicated, lost, or partially visible rows, no cursor skips.

use std::collections::BTreeMap;

use rdlt_core::{CommitPolicy, Cursor, RdltError, ResumedFrom, TableName};
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::{
    CrashDestination, FaultPoint, MemoryBatch, MemoryDestination, MemorySource, MemoryStream, Row,
};
use serde_json::json;

use super::common::{stream_with_batches, three_batches, without_load_id};

fn batches() -> Vec<MemoryBatch> {
    vec![
        MemoryBatch::new(vec![json!({"seq": 1}), json!({"seq": 2})]).with_checkpoint(1),
        MemoryBatch::new(vec![json!({"seq": 3}), json!({"seq": 4})]).with_checkpoint(2),
        MemoryBatch::new(vec![json!({"seq": 5})]).with_checkpoint(3),
    ]
}

fn source() -> MemorySource {
    stream_with_batches(rdlt_connector::StreamSpec::new("events"), batches())
}

fn config(workdir: &std::path::Path) -> EngineConfig {
    let mut config = EngineConfig::new("crash");
    config = config.with_commit_policy(CommitPolicy::every_checkpoints(1));
    config = config.with_workdir(workdir.to_path_buf());
    config
}

type Snapshot = BTreeMap<TableName, Vec<Row>>;

/// The ground truth: what an uninterrupted run produces.
async fn uninterrupted() -> Snapshot {
    let dir = tempfile::tempdir().expect("tempdir");
    let dest = MemoryDestination::new();
    Engine::new(config(dir.path()), source(), dest.clone())
        .run()
        .await
        .expect("clean run");
    without_load_id(&dest)
}

/// Crash-matrix row 1: died during extraction. Restart resumes from the last
/// committed cursor; nothing is lost or duplicated.
#[tokio::test]
async fn row1_source_crash_mid_extraction() {
    let dir = tempfile::tempdir().expect("tempdir");
    let dest = MemoryDestination::new();
    let crashing = MemorySource::new(vec![
        MemoryStream::new(rdlt_connector::StreamSpec::new("events"), batches()).fatal_after(1),
    ]);
    let err = Engine::new(config(dir.path()), crashing, dest.clone())
        .run()
        .await
        .expect_err("injected source crash");
    assert!(matches!(err, RdltError::Source { .. }));

    // Restart with a healthy source: must resume from checkpoint 1 (no re-read).
    let healthy = source();
    let log = healthy.since_log();
    let report = Engine::new(config(dir.path()), healthy, dest.clone())
        .run()
        .await
        .expect("recovery run");
    assert_eq!(
        log.lock().unwrap()[0].1,
        Some(Cursor::new(1)),
        "resume from committed cursor"
    );
    assert_eq!(report.total_rows(), 3, "only seq 3..=5 re-extracted");
    assert_eq!(without_load_id(&dest), uninterrupted().await);
}

/// Row 2: WAL written, crash before the destination commit. Restart replays the WAL
/// into the destination — WITHOUT re-extracting from the source.
#[tokio::test]
async fn row2_crash_before_commit_replays_wal() {
    let dir = tempfile::tempdir().expect("tempdir");
    let inner = MemoryDestination::new();
    let flaky = CrashDestination::new(inner.clone(), FaultPoint::BeforeCommit(2));

    Engine::new(config(dir.path()), source(), flaky.clone())
        .run()
        .await
        .expect_err("injected commit crash");
    assert_eq!(flaky.fired(), 1);
    assert_eq!(
        inner.committed_rows("events").len(),
        2,
        "only span 1 visible pre-crash"
    );

    let healthy = source();
    let log = healthy.since_log();
    let report = Engine::new(config(dir.path()), healthy, flaky.clone())
        .run()
        .await
        .expect("recovery run");
    // The WAL held span 2; it must be REPLAYED, so the source resumes from cursor 2
    // (not 1) — proof there was no re-extraction of the WAL'd span.
    assert!(
        matches!(report.resumed_from, ResumedFrom::Wal { replayed_batches } if replayed_batches >= 1),
        "expected WAL resume, got {:?}",
        report.resumed_from
    );
    assert_eq!(
        log.lock().unwrap()[0].1,
        Some(Cursor::new(2)),
        "source resumes past WAL span"
    );
    assert_eq!(without_load_id(&inner), uninterrupted().await);
}

/// Row 3: crash MID-commit — the destination committed but the receipt was lost.
/// Recovery re-commits the same `(load_id, commit_seq)`; idempotence returns the
/// prior receipt and nothing is applied twice.
#[tokio::test]
async fn row3_crash_mid_commit_hits_idempotence() {
    let dir = tempfile::tempdir().expect("tempdir");
    let inner = MemoryDestination::new();
    let flaky = CrashDestination::new(inner.clone(), FaultPoint::AfterCommit(2));

    Engine::new(config(dir.path()), source(), flaky.clone())
        .run()
        .await
        .expect_err("injected mid-commit crash");
    assert_eq!(
        inner.committed_rows("events").len(),
        4,
        "span 2 IS committed inside"
    );

    let report = Engine::new(config(dir.path()), source(), flaky.clone())
        .run()
        .await
        .expect("recovery run");
    let _ = report;
    assert_eq!(
        without_load_id(&inner),
        uninterrupted().await,
        "no double-apply"
    );
}

/// Row 4: the WAL is lost entirely after a crash. Recovery degrades to
/// re-extraction from the last committed cursor — slower, never wrong.
#[tokio::test]
async fn row4_wal_lost_reextracts_from_cursor() {
    let dir = tempfile::tempdir().expect("tempdir");
    let inner = MemoryDestination::new();
    let flaky = CrashDestination::new(inner.clone(), FaultPoint::BeforeCommit(2));

    Engine::new(config(dir.path()), source(), flaky.clone())
        .run()
        .await
        .expect_err("injected commit crash");

    // Total loss of the work directory.
    std::fs::remove_dir_all(dir.path().join("wal")).expect("delete wal");

    let healthy = source();
    let log = healthy.since_log();
    let report = Engine::new(config(dir.path()), healthy, flaky.clone())
        .run()
        .await
        .expect("recovery run");
    assert!(matches!(report.resumed_from, ResumedFrom::Cursor));
    assert_eq!(
        log.lock().unwrap()[0].1,
        Some(Cursor::new(1)),
        "re-extract from cursor 1"
    );
    assert_eq!(without_load_id(&inner), uninterrupted().await);
}

/// Row 2 variant: crash between WAL append and the destination write.
#[tokio::test]
async fn row2_variant_crash_before_write() {
    let dir = tempfile::tempdir().expect("tempdir");
    let inner = MemoryDestination::new();
    let flaky = CrashDestination::new(inner.clone(), FaultPoint::BeforeWrite(2));

    Engine::new(config(dir.path()), source(), flaky.clone())
        .run()
        .await
        .expect_err("injected write crash");

    let report = Engine::new(config(dir.path()), source(), flaky.clone())
        .run()
        .await
        .expect("recovery run");
    let _ = report;
    assert_eq!(without_load_id(&inner), uninterrupted().await);
}

/// Cancellation is a crash we chose (invariant 4): cancel mid-run via the token,
/// restart, converge. One recovery path.
#[tokio::test]
async fn cancellation_recovers_like_a_crash() {
    let dir = tempfile::tempdir().expect("tempdir");
    let dest = MemoryDestination::new();
    let slow = MemorySource::new(vec![
        MemoryStream::new(rdlt_connector::StreamSpec::new("events"), batches())
            .batch_delay(std::time::Duration::from_millis(40)),
    ]);
    let engine = Engine::new(config(dir.path()), slow, dest.clone());
    let token = engine.cancellation_token();
    let run = tokio::spawn(engine.run());

    // Let some progress happen, then cancel mid-flight.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    token.cancel();
    let outcome = run.await.expect("task join");
    assert!(
        matches!(outcome, Err(RdltError::Cancelled)),
        "got {outcome:?}"
    );

    let report = Engine::new(config(dir.path()), source(), dest.clone())
        .run()
        .await
        .expect("recovery run");
    let _ = report;
    assert_eq!(without_load_id(&dest), uninterrupted().await);
}

/// Dropping the run future mid-flight (task abort) recovers identically.
#[tokio::test]
async fn dropped_run_recovers_like_a_crash() {
    let dir = tempfile::tempdir().expect("tempdir");
    let dest = MemoryDestination::new();
    let slow = MemorySource::new(vec![
        MemoryStream::new(rdlt_connector::StreamSpec::new("events"), batches())
            .batch_delay(std::time::Duration::from_millis(40)),
    ]);
    let run = tokio::spawn(Engine::new(config(dir.path()), slow, dest.clone()).run());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    run.abort();
    let _ = run.await; // JoinError (cancelled) — the "process died"

    let report = Engine::new(config(dir.path()), source(), dest.clone())
        .run()
        .await
        .expect("recovery run");
    let _ = report;
    assert_eq!(without_load_id(&dest), uninterrupted().await);
}

/// Review finding #2 regression: a destination failure while the byte-budget channel
/// is saturated must surface as an error, not deadlock the run (stream tasks parked
/// in `send` unblock when the loader drops the receiver).
#[tokio::test]
async fn loader_failure_with_saturated_channel_errors_instead_of_hanging() {
    let inner = MemoryDestination::new();
    let flaky = CrashDestination::new(inner, FaultPoint::BeforeWrite(1));
    // Many sizable batches, no checkpoints, and a tiny byte budget: the channel is
    // guaranteed saturated while the loader fails on the very first write.
    let big_rows: Vec<serde_json::Value> = (0..200)
        .map(|i| json!({"n": i, "pad": "x".repeat(200)}))
        .collect();
    let source = MemorySource::new(vec![MemoryStream::new(
        rdlt_connector::StreamSpec::new("events"),
        (0..20)
            .map(|_| MemoryBatch::new(big_rows.clone()))
            .collect(),
    )]);
    let mut config = EngineConfig::new("deadlock");
    config = config.with_byte_budget(4096);

    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        Engine::new(config, source, flaky).run(),
    )
    .await
    .expect("run must terminate, not deadlock");
    assert!(
        matches!(outcome, Err(RdltError::Destination { .. })),
        "expected the destination error to surface, got {outcome:?}"
    );
}

/// Kills: the cancellation error-precedence guard (`saw_cancelled`→false) —
/// a cancelled run must surface EXACTLY `RdltError::Cancelled`.
#[tokio::test(flavor = "multi_thread")]
async fn cancellation_surfaces_the_cancelled_error() {
    let dest = MemoryDestination::new();
    let source = MemorySource::new(vec![
        MemoryStream::new(rdlt_connector::StreamSpec::new("s"), three_batches())
            .batch_delay(std::time::Duration::from_millis(200)),
    ]);
    let engine = Engine::new(EngineConfig::new("cancel"), source, dest);
    let token = engine.cancellation_token();
    let run = tokio::spawn(engine.run());
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    token.cancel();
    let err = run.await.expect("join").expect_err("cancelled");
    assert!(matches!(err, RdltError::Cancelled), "got: {err:?}");
}
