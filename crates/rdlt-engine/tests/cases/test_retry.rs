//! The run-level retry driver: transient failures from EITHER side restart the
//! whole run from committed state, bounded by an exact attempt budget, without
//! ever publishing a staged row twice. Several tests here are mutation-report
//! closures; each names the mutant class it kills.

use rdlt_connector::StreamSpec;
use rdlt_core::{PipelineEvent, RdltError};
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::{MemoryBatch, MemoryDestination, MemorySource, MemoryStream};
use serde_json::json;

use super::common::{evolving_batches, three_batch_source, three_batches};

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
        "MAX_RUN_ATTEMPTS is a hard ceiling"
    );
}

/// FR-014: the engine retries transient failures with backoff; connectors never
/// retry. Retries surface in the report AND as events — never silent.
#[tokio::test]
async fn transient_source_failures_are_retried_and_counted() {
    let dest = MemoryDestination::new();
    let source = MemorySource::new(vec![
        MemoryStream::new(rdlt_connector::StreamSpec::new("s"), evolving_batches())
            .transient_start_failures(2),
    ]);

    let engine = Engine::new(EngineConfig::new("retry"), source, dest.clone());
    let mut events = engine.events();
    let report = engine.run().await.expect("run succeeds after retries");

    assert_eq!(report.retries, 2, "both transient failures counted");
    assert_eq!(report.total_rows(), 3, "all data arrived after retry");
    let mut retry_events = 0;
    while let Some(event) = events.recv().await {
        if matches!(event, PipelineEvent::Retried { .. }) {
            retry_events += 1;
        }
    }
    assert_eq!(retry_events, 2);
}

/// A source that keeps failing transiently eventually exhausts the retry budget and
/// surfaces as a classified source error.
#[tokio::test]
async fn retry_budget_exhaustion_is_a_classified_error() {
    let dest = MemoryDestination::new();
    let source = MemorySource::new(vec![
        MemoryStream::new(rdlt_connector::StreamSpec::new("s"), evolving_batches())
            .transient_start_failures(100),
    ]);
    let err = Engine::new(EngineConfig::new("retry-exhaust"), source, dest)
        .run()
        .await
        .expect_err("must eventually fail");
    assert!(matches!(err, rdlt_core::RdltError::Source { .. }));
}

/// 037 US2 T7 fix round 1, the mirror of the success-path proof in
/// `test_run_report.rs`: `close`'s SPI contract is success-path-only —
/// a run that never reaches a last successful commit must not close
/// its session, so a naive unconditional call at the end of
/// `run_once` (rather than gated behind `drain_loader`'s success
/// return) would violate it silently.
#[tokio::test]
async fn a_failed_run_never_closes_its_session() {
    let dest = MemoryDestination::new();
    let source = MemorySource::new(vec![
        MemoryStream::new(StreamSpec::new("s"), evolving_batches()).transient_start_failures(100),
    ]);
    Engine::new(
        EngineConfig::new("retry-exhaust-never-closes"),
        source,
        dest.clone(),
    )
    .run()
    .await
    .expect_err("must eventually fail");
    assert_eq!(dest.closes(), 0, "a failed run must not close its session");
}

/// Review finding #5 regression: a transient failure AFTER rows were staged past the
/// last checkpoint must not publish those rows twice. Run-level retry restarts
/// through the crash path (session re-open tears down staging), so re-extraction is
/// the ONLY delivery.
#[tokio::test]
async fn mid_stream_transient_retry_does_not_duplicate_staged_rows() {
    let dest = MemoryDestination::new();
    let source = MemorySource::new(vec![
        MemoryStream::new(
            rdlt_connector::StreamSpec::new("s"),
            vec![
                MemoryBatch::new(vec![json!({"seq": 1}), json!({"seq": 2})]).with_checkpoint(1),
                MemoryBatch::new(vec![json!({"seq": 3})]), // staged, NOT checkpointed…
                MemoryBatch::new(vec![json!({"seq": 4})]).with_checkpoint(3),
            ],
        )
        .transient_fail_after_once(2), // …then the source dies transiently
    ]);
    let mut config = EngineConfig::new("retry-nodup");
    config = config.with_commit_policy(rdlt_core::CommitPolicy::every_checkpoints(1));

    let report = Engine::new(config, source, dest.clone())
        .run()
        .await
        .expect("run");
    assert_eq!(report.retries, 1);

    let rows = dest.committed_rows("s");
    let mut seqs: Vec<i64> = rows.iter().map(|r| r["seq"].as_i64().unwrap()).collect();
    seqs.sort_unstable();
    assert_eq!(
        seqs,
        vec![1, 2, 3, 4],
        "row 3 must appear exactly once, got {seqs:?}"
    );
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
        ctx: rdlt_connector::OpenContext,
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
    let source = three_batch_source();
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
    let source = three_batch_source();
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

/// The retry budget must TERMINATE, and terminate at an exact count.
///
/// The two tests above prove a transient failure is retried and that the budget
/// eventually gives up with its classification intact. Neither pins the thing
/// the guard actually decides: HOW MANY attempts. That left every arithmetic
/// and boolean edge of `attempt + 1 < MAX_RUN_ATTEMPTS && !cancel.is_cancelled()`
/// free to drift — off-by-one, `&&` to `||`, and the increment itself — while
/// both tests still passed.
///
/// Two failure shapes, so this pin has to catch both:
///
/// - **Wrong count.** `<` to `<=`, or `attempt + 1` to `attempt * 1`, buys one
///   extra attempt. The run still exhausts and still returns the same error, so
///   only counting attempts can see it.
/// - **No termination.** Forcing the guard true, or `&&` to `||` (which makes
///   "not cancelled" alone sufficient), retries FOREVER. That does not fail a
///   test — it hangs it, and a hung test is indistinguishable from a slow
///   machine until someone kills it. The `timeout` is therefore load-bearing,
///   not decoration: it converts an unbounded loop into a fast, legible failure.
///
/// The bound is generous on purpose. Five attempts with the backoff curve
/// (100ms doubling, capped) cost roughly three seconds, so 45s cannot fire for
/// a correct run on any machine this suite runs on.
#[tokio::test]
async fn retry_budget_terminates_at_exactly_five_attempts() {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    const BOUND: Duration = Duration::from_secs(45);

    // ---- destination side: every commit fails transiently, forever ----
    let remaining = std::sync::Arc::new(AtomicU64::new(u64::MAX));
    let dest = TransientCommitDest {
        inner: MemoryDestination::new(),
        remaining: std::sync::Arc::clone(&remaining),
    };
    let source = three_batch_source();
    let err = tokio::time::timeout(
        BOUND,
        Engine::new(EngineConfig::new("dest-bounded"), source, dest).run(),
    )
    .await
    .expect("the retry budget must TERMINATE — an unbounded guard hangs here")
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
    // `remaining` counts down once per failed commit, so the distance travelled
    // IS the number of run attempts.
    assert_eq!(
        u64::MAX - remaining.load(Ordering::SeqCst),
        5,
        "exactly MAX_RUN_ATTEMPTS commit attempts, no more and no fewer"
    );

    // ---- source side: every read fails transiently, forever ----
    let source = MemorySource::new(vec![
        MemoryStream::new(StreamSpec::new("s"), three_batches()).transient_start_failures(u32::MAX),
    ]);
    let since_log = source.since_log();
    let err = tokio::time::timeout(
        BOUND,
        Engine::new(
            EngineConfig::new("source-bounded"),
            source,
            MemoryDestination::new(),
        )
        .run(),
    )
    .await
    .expect("the retry budget must TERMINATE on the source side too")
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
        "exactly MAX_RUN_ATTEMPTS read attempts"
    );
}
