//! The run-level retry driver: transient failures from EITHER side restart the
//! whole run from committed state, bounded by an exact attempt budget, without
//! ever publishing a staged row twice. Several tests here are mutation-report
//! closures; each names the mutant class it kills.

use rdlt_connector::source::StreamSpec;
use rdlt_core::error::Error;
use rdlt_core::event::PipelineEvent;
use rdlt_engine::config::Config;
use rdlt_engine::engine::Engine;
use rdlt_testkit::memory;
use serde_json::json;

use super::common::{evolving_batches, three_batch_source, three_batches};
use super::support::scripted;

/// Kills: retry-attempt arithmetic (`attempt + 1`→`*`, `<`→`<=` in the retry
/// guard) — the attempt COUNT is asserted, not just eventual success/failure.
#[tokio::test]
async fn transient_failures_retry_exactly_and_are_counted() {
    let dest = memory::Destination::new();
    let source = scripted::Source::new(vec![
        scripted::Stream::new(StreamSpec::new("s"), three_batches()).transient_start_failures(2),
    ]);
    let since_log = source.since_log();
    let report = Engine::new(Config::new("retry"), source, dest.clone())
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
    let dest = memory::Destination::new();
    let source = scripted::Source::new(vec![
        scripted::Stream::new(StreamSpec::new("s"), three_batches()).transient_start_failures(99),
    ]);
    let since_log = source.since_log();
    let err = Engine::new(Config::new("retry-out"), source, dest)
        .run()
        .await
        .expect_err("budget exhausted");
    assert!(matches!(
        err,
        Error::Source {
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

/// The engine retries transient failures with backoff; connectors never
/// retry. Retries surface in the report AND as events — never silent.
#[tokio::test]
async fn transient_source_failures_are_retried_and_counted() {
    let dest = memory::Destination::new();
    let source = scripted::Source::new(vec![
        scripted::Stream::new(
            rdlt_connector::source::StreamSpec::new("s"),
            evolving_batches(),
        )
        .transient_start_failures(2),
    ]);

    let engine = Engine::new(Config::new("retry"), source, dest.clone());
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
    let dest = memory::Destination::new();
    let source = scripted::Source::new(vec![
        scripted::Stream::new(
            rdlt_connector::source::StreamSpec::new("s"),
            evolving_batches(),
        )
        .transient_start_failures(100),
    ]);
    let err = Engine::new(Config::new("retry-exhaust"), source, dest)
        .run()
        .await
        .expect_err("must eventually fail");
    assert!(matches!(err, rdlt_core::error::Error::Source { .. }));
}

/// The lease (or whatever a destination's close releases) protects
/// CONCURRENT sessions, not dead ones, so a failed run DOES close its
/// session — best-effort, from the loader drive's abandonment path —
/// rather than leaving it for a foreign process's TTL wait.
///
/// NOT `closes() == 1`: `transient_start_failures(100)` fails the
/// source on EVERY read, so the run-level retry driver exhausts its
/// full `MAX_RUN_ATTEMPTS` budget (5, measured) before giving up, and
/// EACH attempt opens a fresh session that fails and gets best-effort
/// closed in turn — verified empirically before pinning this shape
/// rather than assumed. The invariant this test actually pins is
/// `opens() == closes()`: every session this run ever opened was also
/// closed, which holds regardless of how many attempts the retry
/// budget allows and so survives a future change to
/// `MAX_RUN_ATTEMPTS` without a hardcoded count.
#[tokio::test]
async fn a_failed_run_closes_best_effort() {
    let dest = memory::Destination::new();
    let source = scripted::Source::new(vec![
        scripted::Stream::new(StreamSpec::new("s"), evolving_batches())
            .transient_start_failures(100),
    ]);
    let err = Engine::new(
        Config::new("retry-exhaust-closes-best-effort"),
        source,
        dest.clone(),
    )
    .run()
    .await
    .expect_err("must eventually fail");
    // The run's own error is still the ORIGINAL failure — a close
    // artifact (impossible for `memory::Destination`, whose close cannot
    // fail, but the shape is pinned regardless) must never leak into
    // or replace it.
    assert!(
        matches!(err, rdlt_core::error::Error::Source { .. }),
        "the propagated error must be the source failure, not a close artifact: {err:?}"
    );
    // The vacuity guard: without this, a mutant that skipped opening a
    // session entirely would still pass a bare `closes() == 0`-shaped
    // assertion (nothing opened, nothing closed, `0 == 0`) — pairing it
    // with a genuine open count proves a session was actually opened
    // AND actually closed, not just that the counts happen to agree.
    assert!(
        dest.opens() > 0,
        "a session must have genuinely opened for this test to mean anything"
    );
    assert_eq!(
        dest.opens(),
        dest.closes(),
        "every session this failed run opened, across every retry attempt, was also closed"
    );
}

/// A transient failure AFTER rows were staged past the
/// last checkpoint must not publish those rows twice. Run-level retry restarts
/// through the crash path (session re-open tears down staging), so re-extraction is
/// the ONLY delivery.
#[tokio::test]
async fn mid_stream_transient_retry_does_not_duplicate_staged_rows() {
    let dest = memory::Destination::new();
    let source = scripted::Source::new(vec![
        scripted::Stream::new(
            rdlt_connector::source::StreamSpec::new("s"),
            vec![
                memory::Batch::new(vec![json!({"seq": 1}), json!({"seq": 2})]).with_checkpoint(1),
                memory::Batch::new(vec![json!({"seq": 3})]), // staged, NOT checkpointed…
                memory::Batch::new(vec![json!({"seq": 4})]).with_checkpoint(3),
            ],
        )
        .transient_fail_after_once(2), // …then the source dies transiently
    ]);
    let mut config = Config::new("retry-nodup");
    config = config.with_commit_policy(rdlt_core::commit::CommitPolicy::every_checkpoints(1));

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
    inner: memory::Destination,
    remaining: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

#[async_trait::async_trait]
impl rdlt_connector::destination::Destination for TransientCommitDest {
    fn spec(&self) -> rdlt_connector::spec::ConnectorSpec {
        self.inner.spec()
    }
    fn capabilities(&self) -> rdlt_connector::destination::Capabilities {
        self.inner.capabilities()
    }
    async fn open(
        &self,
        ctx: rdlt_connector::destination::OpenContext,
    ) -> Result<
        Box<dyn rdlt_connector::destination::LoadSession>,
        rdlt_connector::error::DestinationError,
    > {
        let session = self.inner.open(ctx).await?;
        Ok(Box::new(TransientCommitSession {
            inner: session,
            remaining: std::sync::Arc::clone(&self.remaining),
        }))
    }
}

struct TransientCommitSession {
    inner: Box<dyn rdlt_connector::destination::LoadSession>,
    remaining: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

#[async_trait::async_trait]
impl rdlt_connector::destination::LoadSession for TransientCommitSession {
    async fn ensure_table(
        &mut self,
        schema: &rdlt_connector::core::schema::TableSchema,
        mode: &rdlt_core::commit::WriteMode,
    ) -> Result<(), rdlt_connector::error::DestinationError> {
        self.inner.ensure_table(schema, mode).await
    }
    async fn write(
        &mut self,
        table: &rdlt_core::id::TableName,
        batch: rdlt_connector::arrow::RecordBatch,
    ) -> Result<(), rdlt_connector::error::DestinationError> {
        self.inner.write(table, batch).await
    }
    async fn read_state(
        &mut self,
        pipeline: &rdlt_core::id::PipelineId,
    ) -> Result<Option<rdlt_core::state::StateDoc>, rdlt_connector::error::DestinationError> {
        self.inner.read_state(pipeline).await
    }
    async fn commit(
        &mut self,
        meta: rdlt_connector::core::commit::CommitMeta,
    ) -> Result<rdlt_connector::core::commit::CommitReceipt, rdlt_connector::error::DestinationError>
    {
        use std::sync::atomic::Ordering;
        if self
            .remaining
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(rdlt_connector::error::DestinationError::transient(
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
    let inner = memory::Destination::new();
    let dest = TransientCommitDest {
        inner: inner.clone(),
        remaining: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(2)),
    };
    let source = three_batch_source();
    let report = Engine::new(Config::new("dest-retry"), source, dest)
        .run()
        .await
        .expect("succeeds once the transient window passes");
    assert_eq!(report.retries, 2, "both transient commits retried");

    // Budget exhaustion: the ceiling applies to destination retries too.
    let dest = TransientCommitDest {
        inner: memory::Destination::new(),
        remaining: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(u64::MAX)),
    };
    let source = three_batch_source();
    let err = Engine::new(Config::new("dest-retry-out"), source, dest)
        .run()
        .await
        .expect_err("budget exhausted");
    assert!(
        matches!(
            err,
            Error::Destination {
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
        inner: memory::Destination::new(),
        remaining: std::sync::Arc::clone(&remaining),
    };
    let source = three_batch_source();
    let err = tokio::time::timeout(
        BOUND,
        Engine::new(Config::new("dest-bounded"), source, dest).run(),
    )
    .await
    .expect("the retry budget must TERMINATE — an unbounded guard hangs here")
    .expect_err("budget exhausted");
    assert!(
        matches!(
            err,
            Error::Destination {
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
    let source = scripted::Source::new(vec![
        scripted::Stream::new(StreamSpec::new("s"), three_batches())
            .transient_start_failures(u32::MAX),
    ]);
    let since_log = source.since_log();
    let err = tokio::time::timeout(
        BOUND,
        Engine::new(
            Config::new("source-bounded"),
            source,
            memory::Destination::new(),
        )
        .run(),
    )
    .await
    .expect("the retry budget must TERMINATE on the source side too")
    .expect_err("budget exhausted");
    assert!(matches!(
        err,
        Error::Source {
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
