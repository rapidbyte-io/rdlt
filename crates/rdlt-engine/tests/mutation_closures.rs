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

/// Kills: `+=`→`*=` on the report's per-table rows/bytes counters; Discarded
/// zero-emission (`>`→`>=`).
///
/// Deliberately NOT `LoadItem::byte_size`: that trait method has exactly one
/// consumer — the stage channel's permit request — and `table.bytes` below is
/// read straight off the batch in `Loader::process`, never through the trait.
/// A constant `byte_size` leaves every counter here correct while removing
/// backpressure entirely, so it is pinned by its consequence in
/// `load::tests::byte_size_is_what_makes_backpressure_real`.
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
    // at each of the 3 checkpoints. `bytes_since_commit` is accumulated from the
    // batch in `Loader::process`, so this pins the POLICY arm, not
    // `LoadItem::byte_size` (see the note on the test above).
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
    let source = MemorySource::new(vec![MemoryStream::new(
        StreamSpec::new("s"),
        three_batches(),
    )]);
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

/// A destination that records the `CommitCounters` it is handed.
///
/// `CommitMeta.counters` is the per-commit-unit accounting the engine publishes
/// alongside the data, and NOTHING in the suite looked at it — the report's
/// per-table counters are asserted, but those are a different accumulator. So
/// every `+=` feeding the commit counters could become `*=`, leaving them
/// permanently zero, while every existing assertion still passed.
#[derive(Clone)]
struct CountersDest {
    inner: MemoryDestination,
    seen: std::sync::Arc<std::sync::Mutex<Vec<rdlt_core::CommitCounters>>>,
}

#[async_trait::async_trait]
impl rdlt_connector::Destination for CountersDest {
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
        Ok(Box::new(CountersSession {
            inner: self.inner.open(ctx).await?,
            seen: std::sync::Arc::clone(&self.seen),
        }))
    }
}

struct CountersSession {
    inner: Box<dyn rdlt_connector::LoadSession>,
    seen: std::sync::Arc<std::sync::Mutex<Vec<rdlt_core::CommitCounters>>>,
}

#[async_trait::async_trait]
impl rdlt_connector::LoadSession for CountersSession {
    async fn ensure_table(
        &mut self,
        schema: &rdlt_core::TableSchema,
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
    async fn commit(
        &mut self,
        meta: rdlt_connector::CommitMeta,
    ) -> Result<rdlt_connector::CommitReceipt, rdlt_connector::DestinationError> {
        self.seen.lock().expect("seen").push(meta.counters);
        self.inner.commit(meta).await
    }
    async fn read_state(
        &mut self,
        pipeline: &rdlt_core::PipelineId,
    ) -> Result<Option<rdlt_core::StateDoc>, rdlt_connector::DestinationError> {
        self.inner.read_state(pipeline).await
    }
}

/// The counters a destination is HANDED must describe the work it was handed.
///
/// A destination uses these to reconcile what it published — an accounting that
/// silently reads zero is worse than no accounting, because it looks like a
/// clean unit that moved nothing.
#[tokio::test]
async fn commit_counters_describe_the_unit_they_publish() {
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let dest = CountersDest {
        inner: MemoryDestination::new(),
        seen: std::sync::Arc::clone(&seen),
    };
    // One commit per checkpoint: three units, two rows each.
    let report = Engine::new(
        EngineConfig::new("counters"),
        MemorySource::new(vec![MemoryStream::new(
            StreamSpec::new("s"),
            three_batches(),
        )]),
        dest,
    )
    .run()
    .await
    .expect("run");

    let units = seen.lock().expect("seen").clone();
    assert!(!units.is_empty(), "at least one commit unit");
    let rows: u64 = units.iter().map(|c| c.rows).sum();
    let bytes: u64 = units.iter().map(|c| c.bytes).sum();
    assert_eq!(
        rows,
        report.total_rows(),
        "commit counters must total the rows the report says were loaded"
    );
    assert!(bytes > 0, "byte accounting is real, not a constant zero");
    // Every unit that carried rows carried bytes with them.
    for unit in units.iter().filter(|c| c.rows > 0) {
        assert!(unit.bytes > 0, "a unit with rows has bytes: {unit:?}");
    }
}

/// Discards are counted into the commit unit too, not just into the report.
#[tokio::test]
async fn discard_counters_reach_the_commit_unit() {
    use rdlt_core::{PolicyAction, SchemaPolicy};
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let dest = CountersDest {
        inner: MemoryDestination::new(),
        seen: std::sync::Arc::clone(&seen),
    };
    // Freeze the shape after the first batch, then send a row with a NEW column
    // under DiscardRow: the extra row is dropped and must be COUNTED.
    let mut config = EngineConfig::new("discard-counters");
    config.schema_policy = SchemaPolicy::with_default(PolicyAction::DiscardRow);
    let batches = vec![
        MemoryBatch::new(vec![json!({"id": 1})]).with_checkpoint(json!({"b": 0})),
        MemoryBatch::new(vec![json!({"id": 2, "surprise": "x"})]).with_checkpoint(json!({"b": 1})),
    ];
    let report = Engine::new(
        config,
        MemorySource::new(vec![MemoryStream::new(StreamSpec::new("s"), batches)]),
        dest,
    )
    .run()
    .await
    .expect("run");

    let units = seen.lock().expect("seen").clone();
    let discarded_rows: u64 = units.iter().map(|c| c.discarded_rows).sum();
    let discarded_values: u64 = units.iter().map(|c| c.discarded_values).sum();
    let reported_rows: u64 = report.tables.values().map(|t| t.discarded_rows).sum();
    let reported_values: u64 = report.tables.values().map(|t| t.discarded_values).sum();
    // ANTI-VACUOUS, and note the `&&`: an `||` here would let this pass on rows
    // alone while `discarded_values` stayed 0 == 0 — a comparison of two zeros,
    // which is exactly the tautology this guard exists to prevent. Both
    // counters must be exercised, so the policy below discards VALUES too.
    assert!(
        reported_rows > 0,
        "the fixture must discard whole rows: {reported_rows}"
    );
    assert_eq!(
        (discarded_rows, discarded_values),
        (reported_rows, reported_values),
        "the unit's discard counters must agree with the report's"
    );
}

/// The value-level discard counter, which `DiscardRow` never exercises.
#[tokio::test]
async fn discarded_value_counter_reaches_the_commit_unit() {
    use rdlt_core::{PolicyAction, SchemaPolicy};
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let dest = CountersDest {
        inner: MemoryDestination::new(),
        seen: std::sync::Arc::clone(&seen),
    };
    // DiscardValue keeps the row and NULLs the non-conforming value, so the
    // value counter moves while the row counter does not.
    let mut config = EngineConfig::new("discard-values");
    config.schema_policy = SchemaPolicy::with_default(PolicyAction::DiscardValue);
    let batches = vec![
        MemoryBatch::new(vec![json!({"id": 1})]).with_checkpoint(json!({"b": 0})),
        MemoryBatch::new(vec![json!({"id": 2, "surprise": "x"})]).with_checkpoint(json!({"b": 1})),
    ];
    let report = Engine::new(
        config,
        MemorySource::new(vec![MemoryStream::new(StreamSpec::new("s"), batches)]),
        dest,
    )
    .run()
    .await
    .expect("run");

    let units = seen.lock().expect("seen").clone();
    let unit_values: u64 = units.iter().map(|c| c.discarded_values).sum();
    let reported_values: u64 = report.tables.values().map(|t| t.discarded_values).sum();
    assert!(
        reported_values > 0,
        "the fixture must discard VALUES, not just rows: {reported_values}"
    );
    assert_eq!(
        unit_values, reported_values,
        "the unit's value-discard counter must agree with the report's"
    );
}

/// Only the table that actually discarded reports a discard.
///
/// The guards deciding whether to emit a `Discarded` item sit INSIDE blocks
/// that already require a discard to have happened somewhere — the cascade
/// block runs only when some upstream row was dropped, and `enforce_discards`
/// runs per table. So a fully conforming run never reaches them, and the
/// interesting state is a run where ONE table drops a row and ANOTHER, walked
/// in the same drain, drops nothing.
///
/// That is the case an always-true guard corrupts: the untouched child table
/// would report `Discarded { rows: 0, values: 0 }`, and a consumer watching
/// that event to alert on data loss cannot tell "nothing was dropped here" from
/// "something was". The fixture therefore discards a row that has NO children,
/// so the child table sees a non-empty discarded set and still cascades nothing.
#[tokio::test]
async fn only_the_table_that_discarded_reports_a_discard() {
    use rdlt_core::{PolicyAction, SchemaPolicy};
    let mut config = EngineConfig::new("discard-scoped");
    config.schema_policy = SchemaPolicy::with_default(PolicyAction::DiscardRow);
    let batches = vec![
        // Establishes the root shape AND the child table.
        MemoryBatch::new(vec![json!({"id": 1, "items": [{"a": 1}]})])
            .with_checkpoint(json!({"b": 0})),
        // `surprise` violates the established shape, so row 2 is discarded —
        // and it has no items, so the child table cascades NOTHING while the
        // discarded-id set is non-empty. Row 3 conforms and carries a child row.
        // Row 2 carries a NEW nested collection: the root sees an added column
        // and is discarded, and the child table `extra` that the collection
        // would create is therefore a refused CREATION whose rows have ALL
        // cascaded away — dropped == 0 while the discarded set is non-empty,
        // which is the only state the creation guard's mutant can corrupt.
        // Row 3 conforms and carries an ordinary child row.
        MemoryBatch::new(vec![
            json!({"id": 2, "surprise": "x", "extra": [{"z": 1}]}),
            json!({"id": 3, "items": [{"a": 3}]}),
        ])
        .with_checkpoint(json!({"b": 1})),
    ];
    let engine = Engine::new(
        config,
        MemorySource::new(vec![MemoryStream::new(StreamSpec::new("s"), batches)]),
        MemoryDestination::new(),
    );
    let mut events = engine.events();
    let report = engine.run().await.expect("run");

    let mut reported: Vec<(String, u64, u64)> = Vec::new();
    while let Some(event) = events.recv().await {
        if let PipelineEvent::Discarded {
            table,
            rows,
            values,
            ..
        } = event
        {
            reported.push((table.as_str().to_owned(), rows, values));
        }
    }

    // Anti-vacuous: a discard must actually have happened, and there must be a
    // second table for the guard to wrongly report on.
    assert!(
        report.tables.len() > 1,
        "the fixture needs a child table: {:?}",
        report.tables.keys().collect::<Vec<_>>()
    );
    assert!(
        reported.iter().any(|(_, rows, _)| *rows > 0),
        "the fixture must actually discard a row: {reported:?}"
    );
    // …and NOTHING may report a zero-valued discard.
    assert!(
        reported
            .iter()
            .all(|(_, rows, values)| *rows > 0 || *values > 0),
        "a table that discarded nothing must not report a discard: {reported:?}"
    );
}

/// A CONFORMING run under a Discard policy emits no Discarded event at all.
///
/// The clean-run test above uses the default Evolve policy, which never reaches
/// the discard paths — so the guards deciding whether to emit were free to
/// become always-true, and a zero-valued `Discarded { rows: 0, values: 0 }`
/// would flow for every batch of every conforming run.
///
/// That matters beyond tidiness. `Discarded` is how the engine reports data
/// loss; a consumer watching for it to alert, or to fail a pipeline, cannot
/// distinguish "nothing was dropped" from "something was dropped" if the event
/// arrives either way. An event that always fires carries no information.
///
/// Nested rows are used so the CHILD-table cascade paths run too: those decide
/// separately whether to emit, and a flat fixture never reaches them.
#[tokio::test]
async fn a_conforming_run_under_a_discard_policy_emits_no_discards() {
    use rdlt_core::{PolicyAction, SchemaPolicy};

    for action in [PolicyAction::DiscardRow, PolicyAction::DiscardValue] {
        let mut config = EngineConfig::new("discard-clean");
        config.schema_policy = SchemaPolicy::with_default(action);
        let batches = vec![
            MemoryBatch::new(vec![json!({"id": 1, "items": [{"a": 1}]})])
                .with_checkpoint(json!({"b": 0})),
            MemoryBatch::new(vec![json!({"id": 2, "items": [{"a": 2}]})])
                .with_checkpoint(json!({"b": 1})),
        ];
        let engine = Engine::new(
            config,
            MemorySource::new(vec![MemoryStream::new(StreamSpec::new("s"), batches)]),
            MemoryDestination::new(),
        );
        let mut events = engine.events();
        let report = engine.run().await.expect("run");

        // Anti-vacuous: the fixture has to actually load, and actually create
        // the child table whose cascade path this is here to reach.
        assert!(
            report.total_rows() > 0,
            "{action:?}: the fixture must load rows"
        );
        assert!(
            report.tables.len() > 1,
            "{action:?}: nested rows must create a child table, else the cascade \
             paths never run: {:?}",
            report.tables.keys().collect::<Vec<_>>()
        );

        while let Some(event) = events.recv().await {
            assert!(
                !matches!(event, PipelineEvent::Discarded { .. }),
                "{action:?}: a conforming run must emit NO Discarded event, not \
                 even a zero-valued one: {event:?}"
            );
        }
        for (table, entry) in &report.tables {
            assert_eq!(
                (entry.discarded_rows, entry.discarded_values),
                (0, 0),
                "{action:?}: {table} discarded nothing"
            );
        }
    }
}
