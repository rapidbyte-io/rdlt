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
    stream_with_batches(rdlt_connector::source::StreamSpec::new("events"), batches())
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
        MemoryStream::new(rdlt_connector::source::StreamSpec::new("events"), batches())
            .fatal_after(1),
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

/// One `mkfifo <workdir>/wal/manifest.jsonl` after a crash: a plain
/// read-open of a writerless FIFO blocks until a writer appears — which is
/// never — and nothing up the recovery stack carries a timeout, so before
/// the read-side file-type gate this wedged every subsequent run of the
/// pipeline FOREVER. The scan must refuse the FIFO as damage instead, and
/// the run then converges by cursor re-extraction like any damaged WAL.
/// The engine runs on its own thread under a deadline because the RED state
/// of this pin is an eternal hang (a hung `spawn_blocking` open also hangs
/// the runtime's drop), and a test that hangs forever fails no gate; the
/// per-test process boundary reclaims the wedged thread of a red run.
#[test]
fn a_fifo_planted_as_the_manifest_after_a_crash_recovers_promptly() {
    let dir = tempfile::tempdir().expect("tempdir");
    let inner = MemoryDestination::new();
    let flaky = CrashDestination::new(inner.clone(), FaultPoint::BeforeCommit(2));

    let runtime = || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
    };
    runtime()
        .block_on(Engine::new(config(dir.path()), source(), flaky.clone()).run())
        .expect_err("injected commit crash");

    let manifest = dir.path().join("wal").join("manifest.jsonl");
    std::fs::remove_file(&manifest).expect("drop manifest");
    // mkfifo via the coreutils binary: the workspace denies `unsafe`, so
    // `libc::mkfifo` is not callable. Skip rather than fail on a host
    // without it.
    match std::process::Command::new("mkfifo").arg(&manifest).status() {
        Ok(status) if status.success() => {}
        _ => {
            eprintln!("skipping: no mkfifo binary on this host");
            return;
        }
    }

    let (tx, rx) = std::sync::mpsc::channel();
    let workdir = dir.path().to_path_buf();
    std::thread::spawn(move || {
        let outcome =
            runtime().block_on(Engine::new(config(&workdir), source(), inner.clone()).run());
        let _ = tx.send((outcome, without_load_id(&inner)));
    });
    let (outcome, rows) = match rx.recv_timeout(std::time::Duration::from_secs(60)) {
        Ok(result) => result,
        Err(_) => panic!("recovery hung on the planted FIFO instead of refusing it"),
    };
    let report = outcome.expect("the recovery run resolves the damaged WAL and completes");
    assert!(
        matches!(report.resumed_from, ResumedFrom::Cursor),
        "a refused manifest degrades to cursor re-extraction: {:?}",
        report.resumed_from
    );
    assert_eq!(
        rows,
        runtime().block_on(uninterrupted()),
        "the degraded run still converges to the uninterrupted result"
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
        MemoryStream::new(rdlt_connector::source::StreamSpec::new("events"), batches())
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
        MemoryStream::new(rdlt_connector::source::StreamSpec::new("events"), batches())
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
        rdlt_connector::source::StreamSpec::new("events"),
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
        MemoryStream::new(
            rdlt_connector::source::StreamSpec::new("s"),
            three_batches(),
        )
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

/// A source PARKED between frames observes no channel closure, so
/// cancellation must not wait on it (045 external findings, GROK 3):
/// the read task is aborted after a bounded grace, and the advertised
/// `cancellation_token().cancel()` returns promptly instead of hanging
/// an embedder forever on a wire source idle between frames.
#[tokio::test(flavor = "multi_thread")]
async fn cancelling_a_parked_source_returns_promptly() {
    use rdlt_connector::error::SourceError;
    use rdlt_connector::source::{ReadRequest, Source, StreamSpec};
    use rdlt_connector::spec::ConnectorSpec;

    /// `read` never yields, never pushes, and never watches its
    /// channel — the wire-source-idle-between-frames shape reduced to
    /// its essence.
    struct Parked;

    #[async_trait::async_trait]
    impl Source for Parked {
        fn spec(&self) -> ConnectorSpec {
            ConnectorSpec::new("parked", "0")
        }
        async fn check(&self) -> Result<(), SourceError> {
            Ok(())
        }
        async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
            Ok(vec![StreamSpec::new("s")])
        }
        async fn read(&self, _request: ReadRequest) -> Result<(), SourceError> {
            std::future::pending().await
        }
    }

    let engine = Engine::new(
        EngineConfig::new("parked"),
        Parked,
        MemoryDestination::new(),
    );
    let token = engine.cancellation_token();
    let run = tokio::spawn(engine.run());
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    token.cancel();
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(5), run)
        .await
        .expect("a cancelled run must not hang on a parked reader")
        .expect("join");
    assert!(
        matches!(outcome, Err(RdltError::Cancelled)),
        "got {outcome:?}"
    );
}

/// The subtler parked shape: the source first drops its output, making
/// the host observe SourceFinished, and only then parks inside read().
/// Cancellation must still win over the longer clean-finish grace.
#[tokio::test(flavor = "multi_thread")]
async fn cancelling_a_source_that_closed_then_parked_returns_promptly() {
    use rdlt_connector::error::SourceError;
    use rdlt_connector::source::{ReadRequest, Source, StreamSpec};
    use rdlt_connector::spec::ConnectorSpec;

    struct CloseThenPark(std::sync::Arc<tokio::sync::Notify>);

    #[async_trait::async_trait]
    impl Source for CloseThenPark {
        fn spec(&self) -> ConnectorSpec {
            ConnectorSpec::new("close-then-park", "0")
        }
        async fn check(&self) -> Result<(), SourceError> {
            Ok(())
        }
        async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
            Ok(vec![StreamSpec::new("s")])
        }
        async fn read(&self, request: ReadRequest) -> Result<(), SourceError> {
            drop(request);
            self.0.notify_one();
            std::future::pending().await
        }
    }

    let closed = std::sync::Arc::new(tokio::sync::Notify::new());
    let engine = Engine::new(
        EngineConfig::new("close-then-park"),
        CloseThenPark(std::sync::Arc::clone(&closed)),
        MemoryDestination::new(),
    );
    let token = engine.cancellation_token();
    let run = tokio::spawn(engine.run());
    closed.notified().await;
    token.cancel();
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(1), run)
        .await
        .expect("SourceFinished teardown remains cancellable")
        .expect("join");
    assert!(matches!(outcome, Err(RdltError::Cancelled)), "{outcome:?}");
}

/// Review round 2's regression pin for the RunStarted-first guarantee
/// on the WAL-REPLAY path — the one the fresh-run pins cannot see.
///
/// The wrapper destination reports a part on every commit through the
/// SPI listener, exactly like a file-writing connector. Run 1 crashes
/// mid-commit, leaving a pending WAL span; run 2 recovers by REPLAY.
/// The guarantee under test: the replay session is opened with NO
/// listener, so nothing reaches the feed before RunStarted — rewiring
/// the replay session's forwarder (the round-1 defect) turns this pin
/// red.
mod run_started_first_on_replay {
    use super::*;
    use rdlt_connector::arrow::RecordBatch;
    use rdlt_connector::core::{CommitMeta, CommitReceipt, StateDoc};
    use rdlt_connector::destination::{
        Capabilities, Destination, LoadSession, OpenContext, PartCloseReason, PartClosed,
        PartEventFn,
    };
    use rdlt_connector::error::DestinationError;
    use rdlt_core::PipelineEvent;

    /// MemoryDestination, plus a part report per commit — the shape of
    /// every file-writing destination, reduced to the seam under test.
    #[derive(Clone)]
    struct PartEmitting {
        inner: MemoryDestination,
    }

    struct PartEmittingSession {
        inner: Box<dyn LoadSession>,
        listener: Option<PartEventFn>,
    }

    #[async_trait::async_trait]
    impl Destination for PartEmitting {
        fn spec(&self) -> rdlt_connector::spec::ConnectorSpec {
            self.inner.spec()
        }
        fn capabilities(&self) -> Capabilities {
            self.inner.capabilities()
        }
        async fn open(
            &self,
            context: OpenContext,
        ) -> Result<Box<dyn LoadSession>, DestinationError> {
            let listener = context.part_events.clone();
            let inner = self.inner.open(context).await?;
            Ok(Box::new(PartEmittingSession { inner, listener }))
        }
    }

    #[async_trait::async_trait]
    impl LoadSession for PartEmittingSession {
        async fn ensure_table(
            &mut self,
            schema: &rdlt_connector::core::TableSchema,
            mode: &rdlt_connector::core::WriteMode,
        ) -> Result<(), DestinationError> {
            self.inner.ensure_table(schema, mode).await
        }
        async fn write(
            &mut self,
            table: &rdlt_connector::core::TableName,
            batch: RecordBatch,
        ) -> Result<(), DestinationError> {
            self.inner.write(table, batch).await
        }
        async fn commit(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestinationError> {
            if let Some(listener) = &self.listener {
                listener(PartClosed::new(
                    rdlt_connector::core::TableName::new("events"),
                    64,
                    PartCloseReason::Commit,
                ));
            }
            self.inner.commit(meta).await
        }
        async fn read_state(
            &mut self,
            pipeline: &rdlt_connector::core::PipelineId,
        ) -> Result<Option<StateDoc>, DestinationError> {
            self.inner.read_state(pipeline).await
        }
    }

    #[tokio::test]
    async fn replay_emits_nothing_before_run_started() {
        let dir = tempfile::tempdir().expect("tempdir");

        // Run 1: crash during the second commit — a WAL span is left
        // pending with committed work behind it.
        let crash = CrashDestination::new(MemoryDestination::new(), FaultPoint::BeforeCommit(2));
        let outcome = Engine::new(config(dir.path()), source(), crash).run().await;
        assert!(outcome.is_err(), "the fixture must crash");

        // Run 2: recover. The wrapper reports a part on EVERY commit —
        // including replay's, if replay were (wrongly) given a
        // listener.
        let dest = PartEmitting {
            inner: MemoryDestination::new(),
        };
        let engine = Engine::new(config(dir.path()), source(), dest);
        let mut events = engine.events();
        let report = engine.run().await.expect("recovery run");
        assert!(
            matches!(report.resumed_from, rdlt_core::ResumedFrom::Wal { .. }),
            "the pin must exercise the REPLAY path, got {:?}",
            report.resumed_from
        );

        let mut seen = Vec::new();
        while let Some(event) = events.recv().await {
            seen.push(event);
        }
        assert!(
            matches!(seen.first(), Some(PipelineEvent::RunStarted { .. })),
            "nothing precedes RunStarted, replay included — got {:?}",
            seen.first()
        );
        // The wrapper's own-session reports DO reach the feed — the
        // listener seam works; only replay is excluded.
        let started_at = 0;
        let part_positions: Vec<usize> = seen
            .iter()
            .enumerate()
            .filter(|(_, e)| matches!(e, PipelineEvent::PartClosed { .. }))
            .map(|(i, _)| i)
            .collect();
        assert!(
            !part_positions.is_empty(),
            "the run's own commits report parts"
        );
        assert!(
            part_positions.iter().all(|&i| i > started_at),
            "every part event follows RunStarted: {part_positions:?}"
        );
    }
}
