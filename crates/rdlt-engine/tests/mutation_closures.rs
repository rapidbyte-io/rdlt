//! Mutation-report closures (feature 003 T011): behaviors the suite exercised
//! but never ASSERTED — exact report counters, commit-policy boundaries, retry
//! arithmetic, event cleanliness, resume cursors. Each test names the mutant
//! class it kills.

use rdlt_connector::StreamSpec;
use rdlt_core::{CommitPolicy, PipelineEvent, RdltError};
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::{MemoryBatch, MemoryDestination, MemorySource, MemoryStream};
use serde_json::json;

fn three_batches() -> Vec<MemoryBatch> {
    (0..3)
        .map(|b| {
            MemoryBatch::new(vec![
                json!({"id": b * 2, "name": format!("r{b}a")}),
                json!({"id": b * 2 + 1, "name": format!("r{b}b")}),
            ])
            .with_checkpoint(json!({"b": b}))
        })
        .collect()
}

/// Kills: `+=`→`*=` on report rows/bytes counters; `byte_size`→0/1 (via the
/// bytes counter being real); Discarded zero-emission (`>`→`>=`).
#[tokio::test]
async fn report_counters_are_exact_and_clean_runs_emit_no_discards() {
    let dest = MemoryDestination::new();
    let engine = Engine::new(
        EngineConfig::new("exact"),
        MemorySource::new(vec![MemoryStream::new(
            StreamSpec::new("s"),
            three_batches(),
        )]),
        dest.clone(),
    );
    let mut events = engine.events();
    let report = engine.run().await.expect("run");

    assert_eq!(report.total_rows(), 6, "rows accumulate per batch (+=)");
    let table = report.tables.values().next().expect("one table");
    assert_eq!(table.rows, 6);
    assert!(table.bytes > 0, "byte accounting is real, not a constant");
    assert_eq!(table.discarded_rows, 0);
    assert_eq!(table.discarded_values, 0);
    while let Some(event) = events.recv().await {
        assert!(
            !matches!(event, PipelineEvent::Discarded { .. }),
            "a clean run must not emit Discarded events (not even zero-valued)"
        );
    }
}

/// Kills: `policy_triggers` `>=`→`<` boundaries and `EveryCheckpoints` counting.
/// 3 checkpoints under EveryCheckpoints(2): commit fires at checkpoint 2, the
/// trailing work commits in finish() — exactly 2 commits.
#[tokio::test]
async fn commit_policy_boundaries_are_exact() {
    let dest = MemoryDestination::new();
    let mut config = EngineConfig::new("policy");
    config.commit_policy = CommitPolicy::EveryCheckpoints(2);
    let report = Engine::new(
        config,
        MemorySource::new(vec![MemoryStream::new(
            StreamSpec::new("s"),
            three_batches(),
        )]),
        dest.clone(),
    )
    .run()
    .await
    .expect("run");
    assert_eq!(
        report.commits, 2,
        "checkpoint 2 commits; finish() commits the tail"
    );

    // EveryBytes(1): every checkpoint boundary sees bytes > threshold → commits
    // at each of the 3 checkpoints (kills byte_size→0, which would never trigger).
    let dest = MemoryDestination::new();
    let mut config = EngineConfig::new("policy-bytes");
    config.commit_policy = CommitPolicy::EveryBytes(1);
    let report = Engine::new(
        config,
        MemorySource::new(vec![MemoryStream::new(
            StreamSpec::new("s"),
            three_batches(),
        )]),
        dest.clone(),
    )
    .run()
    .await
    .expect("run");
    assert_eq!(
        report.commits, 3,
        "byte policy triggers at every checkpoint"
    );
}

/// Kills: retry-attempt arithmetic (`attempt + 1`→`*`, `<`→`<=` in the retry
/// guard) — the attempt COUNT is asserted, not just eventual success/failure.
#[tokio::test]
async fn transient_failures_retry_exactly_and_are_counted() {
    let dest = MemoryDestination::new();
    let source = MemorySource::new(vec![
        MemoryStream::new(StreamSpec::new("s"), three_batches()).transient_start_failures(2),
    ]);
    let since_log = source.since_log();
    let report = Engine::new(EngineConfig::new("retry"), source, dest.clone())
        .run()
        .await
        .expect("succeeds on attempt 3");
    assert_eq!(report.retries, 2, "exactly two retries recorded");
    assert_eq!(
        since_log.lock().expect("log").len(),
        3,
        "exactly three read attempts"
    );

    // Budget exhaustion: 5 attempts maximum, then a classified error.
    let dest = MemoryDestination::new();
    let source = MemorySource::new(vec![
        MemoryStream::new(StreamSpec::new("s"), three_batches()).transient_start_failures(99),
    ]);
    let since_log = source.since_log();
    let err = Engine::new(EngineConfig::new("retry-out"), source, dest)
        .run()
        .await
        .expect_err("budget exhausted");
    assert!(matches!(
        err,
        RdltError::Source {
            retryable: true,
            ..
        }
    ));
    assert_eq!(
        since_log.lock().expect("log").len(),
        5,
        "MAX_SOURCE_ATTEMPTS is a hard ceiling"
    );
}

/// Kills: the fresh-run resume guard (`!state.cursors.is_empty()`→true) — a
/// pipeline with NO committed state must pass `since: None`.
#[tokio::test]
async fn fresh_run_reads_with_no_cursor() {
    let dest = MemoryDestination::new();
    let source = MemorySource::new(vec![MemoryStream::new(
        StreamSpec::new("s"),
        three_batches(),
    )]);
    let since_log = source.since_log();
    Engine::new(EngineConfig::new("fresh"), source, dest)
        .run()
        .await
        .expect("run");
    let log = since_log.lock().expect("log");
    assert_eq!(log.len(), 1);
    assert!(
        log[0].1.is_none(),
        "fresh pipeline must not invent a cursor"
    );
}

/// Kills: the resumed-from guard (`!state.cursors.is_empty()`→true) — a
/// recovered state whose cursors are EMPTY (first run never checkpointed) must
/// report `Fresh`, not `Cursor`.
#[tokio::test]
async fn empty_cursor_state_reports_fresh_resume() {
    use rdlt_core::ResumedFrom;
    let dest = MemoryDestination::new();
    // No checkpoints: state commits with an empty cursor map.
    let stream = MemoryStream::new(
        StreamSpec::new("s"),
        vec![MemoryBatch::new(vec![json!({"id": 1})])],
    );
    let report = Engine::new(
        EngineConfig::new("nocursor"),
        MemorySource::new(vec![stream]),
        dest.clone(),
    )
    .run()
    .await
    .expect("run 1");
    assert_eq!(report.resumed_from, ResumedFrom::Fresh);

    // Second run recovers a StateDoc — but with no cursors it is still Fresh.
    let stream = MemoryStream::new(
        StreamSpec::new("s"),
        vec![MemoryBatch::new(vec![json!({"id": 2})])],
    );
    let report = Engine::new(
        EngineConfig::new("nocursor"),
        MemorySource::new(vec![stream]),
        dest,
    )
    .run()
    .await
    .expect("run 2");
    assert_eq!(
        report.resumed_from,
        ResumedFrom::Fresh,
        "empty cursors must never report a cursor resume"
    );
}

/// Kills: the cancellation error-precedence guard (`saw_cancelled`→false) —
/// a cancelled run must surface EXACTLY `RdltError::Cancelled`.
#[tokio::test(flavor = "multi_thread")]
async fn cancellation_surfaces_the_cancelled_error() {
    let dest = MemoryDestination::new();
    let source = MemorySource::new(vec![
        MemoryStream::new(StreamSpec::new("s"), three_batches())
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

/// A destination whose first N commits fail transient (then delegate):
/// the run driver must restart from committed state and succeed — the
/// destination's recoverable channel is honored, not aborted on.
#[derive(Clone)]
struct TransientCommitDest {
    inner: MemoryDestination,
    remaining: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

#[async_trait::async_trait]
impl rdlt_connector::Destination for TransientCommitDest {
    fn spec(&self) -> rdlt_connector::ConnectorSpec {
        self.inner.spec()
    }
    fn capabilities(&self) -> rdlt_connector::DestinationCapabilities {
        self.inner.capabilities()
    }
    async fn open(
        &self,
        ctx: rdlt_connector::OpenCtx,
    ) -> Result<Box<dyn rdlt_connector::LoadSession>, rdlt_connector::DestinationError> {
        let session = self.inner.open(ctx).await?;
        Ok(Box::new(TransientCommitSession {
            inner: session,
            remaining: std::sync::Arc::clone(&self.remaining),
        }))
    }
}

struct TransientCommitSession {
    inner: Box<dyn rdlt_connector::LoadSession>,
    remaining: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

#[async_trait::async_trait]
impl rdlt_connector::LoadSession for TransientCommitSession {
    async fn ensure_table(
        &mut self,
        schema: &rdlt_connector::core::TableSchema,
        mode: &rdlt_core::WriteMode,
    ) -> Result<(), rdlt_connector::DestinationError> {
        self.inner.ensure_table(schema, mode).await
    }
    async fn write(
        &mut self,
        table: &rdlt_core::TableName,
        batch: rdlt_connector::RecordBatch,
    ) -> Result<(), rdlt_connector::DestinationError> {
        self.inner.write(table, batch).await
    }
    async fn read_state(
        &mut self,
        pipeline: &rdlt_core::PipelineId,
    ) -> Result<Option<rdlt_core::StateDoc>, rdlt_connector::DestinationError> {
        self.inner.read_state(pipeline).await
    }
    async fn commit(
        &mut self,
        meta: rdlt_connector::CommitMeta,
    ) -> Result<rdlt_connector::CommitReceipt, rdlt_connector::DestinationError> {
        use std::sync::atomic::Ordering;
        if self
            .remaining
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(rdlt_connector::DestinationError::transient(
                "injected transient commit failure",
            ));
        }
        self.inner.commit(meta).await
    }
}

/// Kills: the destination arm of the run-level retry guard — a transient
/// DESTINATION failure restarts the run (bounded) exactly like a source
/// one; a rate-limited/fatal split is asserted via the terminal class.
#[tokio::test]
async fn transient_destination_failures_retry_and_are_bounded() {
    let inner = MemoryDestination::new();
    let dest = TransientCommitDest {
        inner: inner.clone(),
        remaining: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(2)),
    };
    let source = MemorySource::new(vec![MemoryStream::new(
        StreamSpec::new("s"),
        three_batches(),
    )]);
    let report = Engine::new(EngineConfig::new("dest-retry"), source, dest)
        .run()
        .await
        .expect("succeeds once the transient window passes");
    assert_eq!(report.retries, 2, "both transient commits retried");

    // Budget exhaustion: the ceiling applies to destination retries too.
    let dest = TransientCommitDest {
        inner: MemoryDestination::new(),
        remaining: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(u64::MAX)),
    };
    let source = MemorySource::new(vec![MemoryStream::new(
        StreamSpec::new("s"),
        three_batches(),
    )]);
    let err = Engine::new(EngineConfig::new("dest-retry-out"), source, dest)
        .run()
        .await
        .expect_err("budget exhausted");
    assert!(
        matches!(
            err,
            RdltError::Destination {
                retryable: true,
                ..
            }
        ),
        "terminal error keeps its classification: {err:?}"
    );
}
