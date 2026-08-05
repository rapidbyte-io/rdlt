//! The run's accounting: events arrive in causal order, the report's totals
//! equal destination-visible reality, the `CommitCounters` handed to a
//! destination describe the unit they accompany, and `Discarded` — the
//! engine's data-loss signal — fires exactly when something was discarded.
//! Several tests here are mutation-report closures; each names the mutant
//! class it kills.

use rdlt_connector::StreamSpec;
use rdlt_core::{PipelineEvent, TableName};
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::{MemoryBatch, MemoryDestination};
use serde_json::json;

use super::common::{evolving_batches, stream_with_batches, three_batch_source};

#[tokio::test]
async fn events_are_causally_ordered_and_report_matches_reality() {
    let dest = MemoryDestination::new();
    let source = stream_with_batches(rdlt_connector::StreamSpec::new("s"), evolving_batches());
    let mut config = EngineConfig::new("obs");
    config = config.with_commit_policy(rdlt_core::CommitPolicy::every_checkpoints(1));

    let engine = Engine::new(config, source, dest.clone());
    let mut events = engine.events();
    let report = engine.run().await.expect("run");

    let mut seen = Vec::new();
    while let Some(event) = events.recv().await {
        seen.push(event);
    }

    // Causal order per table: SchemaEvolved before the first BatchLoaded at that
    // version; a Committed after everything it covers (clause R3).
    let first_evolve = seen
        .iter()
        .position(|e| matches!(e, PipelineEvent::SchemaEvolved { .. }))
        .expect("schema creation event");
    let first_batch = seen
        .iter()
        .position(|e| matches!(e, PipelineEvent::BatchLoaded { .. }))
        .expect("batch event");
    assert!(first_evolve < first_batch, "delta before batch");
    // 036: RunStarted identifies the run before anything else; the
    // first STREAM event follows it.
    assert!(
        matches!(seen.first(), Some(PipelineEvent::RunStarted { .. })),
        "run identity first, got {:?}",
        seen.first()
    );
    assert!(
        seen.iter()
            .position(|e| matches!(e, PipelineEvent::StreamStarted { .. }))
            < seen
                .iter()
                .position(|e| matches!(e, PipelineEvent::BatchLoaded { .. })),
        "stream start precedes its batches"
    );
    let commits = seen
        .iter()
        .filter(|e| matches!(e, PipelineEvent::Committed { .. }))
        .count();
    assert_eq!(commits as u64, report.commits);
    // The mid-run evolution (column `b`) appears as its own SchemaEvolved event.
    let evolves = seen
        .iter()
        .filter(|e| matches!(e, PipelineEvent::SchemaEvolved { .. }))
        .count();
    assert!(evolves >= 2, "create + add-column, got {evolves}");

    // Accounting invariant (SC-008): report totals == destination-visible reality.
    let table = TableName::new("s");
    assert_eq!(
        report.tables[&table].rows as usize,
        dest.committed_rows("s").len()
    );
    let event_rows: u64 = seen
        .iter()
        .filter_map(|e| match e {
            PipelineEvent::BatchLoaded { rows, .. } => Some(*rows),
            _ => None,
        })
        .sum();
    assert_eq!(event_rows, report.total_rows(), "events and report agree");
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
        three_batch_source(),
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

/// 037 US2 T7 fix round 1: the engine calls `LoadSession::close`
/// exactly once, on the SUCCESS path — after the run's last commit,
/// never per-open and never on failure (`MemoryDestination`'s default
/// close is a no-op, but the counter proves the CALL happened, not
/// just that nothing broke). `dest.opens()` alongside it is the
/// existing instrumentation this pairs with — one session, one close.
#[tokio::test]
async fn a_successful_run_closes_its_session_exactly_once() {
    let dest = MemoryDestination::new();
    let engine = Engine::new(
        EngineConfig::new("closes"),
        three_batch_source(),
        dest.clone(),
    );
    engine.run().await.expect("run");
    assert_eq!(dest.opens(), 1, "one session opened");
    assert_eq!(
        dest.closes(),
        1,
        "the session closes exactly once, on the success path"
    );
}

/// A destination wrapping `MemoryDestination` whose session `close`
/// ALWAYS fails, classified TRANSIENT by the destination itself — the
/// exact shape M4 exists to defeat: a destination has no way to know
/// its own close failure can never be helped by re-running a load that
/// already committed everything, so a naive
/// `classify_dest_error`-style forward (which trusts that
/// classification) would make the run driver retry a fully-committed
/// load.
#[derive(Clone)]
struct CloseFailsDest {
    inner: MemoryDestination,
}

#[async_trait::async_trait]
impl rdlt_connector::Destination for CloseFailsDest {
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
        Ok(Box::new(CloseFailsSession {
            inner: self.inner.open(ctx).await?,
        }))
    }
}

struct CloseFailsSession {
    inner: Box<dyn rdlt_connector::LoadSession>,
}

#[async_trait::async_trait]
impl rdlt_connector::LoadSession for CloseFailsSession {
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
        self.inner.commit(meta).await
    }
    async fn read_state(
        &mut self,
        pipeline: &rdlt_core::PipelineId,
    ) -> Result<Option<rdlt_core::StateDoc>, rdlt_connector::DestinationError> {
        self.inner.read_state(pipeline).await
    }
    async fn close(&mut self) -> Result<(), rdlt_connector::DestinationError> {
        self.inner.close().await?;
        Err(rdlt_connector::DestinationError::transient(
            "injected close failure",
        ))
    }
}

/// 037 US2 fix round 2, M4: a close failure on the SUCCESS path is
/// ALWAYS non-retryable, regardless of how the destination itself
/// classified it (here, TRANSIENT — the shape most likely to fool a
/// naive forward), and its message tells the operator the data is
/// safe.
#[tokio::test]
async fn a_close_failure_after_success_is_never_retried_and_says_data_is_durable() {
    let dest = CloseFailsDest {
        inner: MemoryDestination::new(),
    };
    let err = Engine::new(EngineConfig::new("close-fails"), three_batch_source(), dest)
        .run()
        .await
        .expect_err("the close failure must surface as the run's error");
    assert!(
        matches!(
            err,
            rdlt_core::RdltError::Destination {
                retryable: false,
                ..
            }
        ),
        "a close failure must be non-retryable even though the destination classified it \
         transient: {err:?}"
    );
    assert!(
        err.to_string().contains(
            "session close failed AFTER all commits were durable (the data is committed): "
        ),
        "{err}"
    );
    assert!(err.to_string().contains("injected close failure"), "{err}");
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
        ctx: rdlt_connector::OpenContext,
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
    let report = Engine::new(EngineConfig::new("counters"), three_batch_source(), dest)
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
    config = config.with_schema_policy(SchemaPolicy::with_default(PolicyAction::DiscardRow));
    let batches = vec![
        MemoryBatch::new(vec![json!({"id": 1})]).with_checkpoint(json!({"b": 0})),
        MemoryBatch::new(vec![json!({"id": 2, "surprise": "x"})]).with_checkpoint(json!({"b": 1})),
    ];
    let report = Engine::new(
        config,
        stream_with_batches(StreamSpec::new("s"), batches),
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
    config = config.with_schema_policy(SchemaPolicy::with_default(PolicyAction::DiscardValue));
    let batches = vec![
        MemoryBatch::new(vec![json!({"id": 1})]).with_checkpoint(json!({"b": 0})),
        MemoryBatch::new(vec![json!({"id": 2, "surprise": "x"})]).with_checkpoint(json!({"b": 1})),
    ];
    let report = Engine::new(
        config,
        stream_with_batches(StreamSpec::new("s"), batches),
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
    config = config.with_schema_policy(SchemaPolicy::with_default(PolicyAction::DiscardRow));
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
        stream_with_batches(StreamSpec::new("s"), batches),
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
        config = config.with_schema_policy(SchemaPolicy::with_default(action));
        let batches = vec![
            MemoryBatch::new(vec![json!({"id": 1, "items": [{"a": 1}]})])
                .with_checkpoint(json!({"b": 0})),
            MemoryBatch::new(vec![json!({"id": 2, "items": [{"a": 2}]})])
                .with_checkpoint(json!({"b": 1})),
        ];
        let engine = Engine::new(
            config,
            stream_with_batches(StreamSpec::new("s"), batches),
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

/// The 036 telemetry additions hold their causal contract: every
/// `BatchLoaded` row was announced by a `BatchRead` first (read
/// precedes write for the same rows), every `Committed` was preceded
/// by its `CommitStarted`, and the liveness heartbeat ticks even on a
/// short run (the first tick is immediate by design).
#[tokio::test]
async fn read_commit_and_heartbeat_events_hold_their_order() {
    let dest = MemoryDestination::new();
    let source = stream_with_batches(rdlt_connector::StreamSpec::new("s"), evolving_batches());
    let mut config = EngineConfig::new("obs-036");
    config = config.with_commit_policy(rdlt_core::CommitPolicy::every_checkpoints(1));

    let engine = Engine::new(config, source, dest.clone());
    let mut events = engine.events();
    engine.run().await.expect("run");

    let mut seen = Vec::new();
    while let Some(event) = events.recv().await {
        seen.push(event);
    }

    // Read precedes write, cumulatively: at every point in the feed,
    // rows announced as read are >= rows announced as loaded.
    let (mut read, mut loaded) = (0u64, 0u64);
    for event in &seen {
        match event {
            PipelineEvent::BatchRead { rows, .. } => read += rows,
            PipelineEvent::BatchLoaded { rows, .. } => {
                loaded += rows;
                assert!(
                    read >= loaded,
                    "rows were loaded before being announced as read: {read} < {loaded}"
                );
            }
            _ => {}
        }
    }
    assert!(read > 0, "the source's batches were announced as read");
    assert_eq!(
        read, loaded,
        "everything read was loaded — no discards here"
    );

    // Every Committed is preceded by ITS CommitStarted.
    let mut started = Vec::new();
    for event in &seen {
        match event {
            PipelineEvent::CommitStarted { commit_seq } => started.push(*commit_seq),
            PipelineEvent::Committed { commit_seq, .. } => assert!(
                started.contains(commit_seq),
                "commit {commit_seq} completed without starting"
            ),
            _ => {}
        }
    }
    assert!(!started.is_empty(), "commits announce their start");

    // Liveness: the ticker's first beat is immediate, so even a
    // fast run carries at least one.
    assert!(
        seen.iter()
            .any(|e| matches!(e, PipelineEvent::Heartbeat { .. })),
        "a heartbeat ticked"
    );
}

/// The canonical fold agrees with the run report on the totals the
/// report owns — one spot-check that `Metrics` and the exactly-once
/// numbers cannot silently diverge for a clean run.
#[tokio::test]
async fn the_metrics_fold_agrees_with_the_report_for_a_clean_run() {
    let dest = MemoryDestination::new();
    let source = stream_with_batches(rdlt_connector::StreamSpec::new("s"), evolving_batches());
    let engine = Engine::new(EngineConfig::new("obs-fold"), source, dest.clone());
    let mut events = engine.events();
    let report = engine.run().await.expect("run");

    let mut metrics = rdlt_core::Metrics::new();
    while let Some(event) = events.recv().await {
        metrics.apply(&event);
    }
    let snap = metrics.snapshot();
    let report_rows: u64 = report.tables.values().map(|t| t.rows).sum();
    assert_eq!(snap.rows_written, report_rows);
    assert_eq!(snap.commits, report.commits);
    assert_eq!(snap.retries, report.retries);
    assert_eq!(
        snap.schema_migrations,
        report.schema_migrations.len() as u64
    );
}

/// Review round 1's counting fix, pinned: `rows_read` counts what the
/// payload DECODED to — including whole rows a Discard policy then
/// dropped — so read-vs-loaded divergence exposes discards instead of
/// silently pre-subtracting them.
#[tokio::test]
async fn rows_read_includes_discarded_rows() {
    use rdlt_core::{PolicyAction, SchemaPolicy};
    let mut config = EngineConfig::new("discard-read");
    config = config.with_schema_policy(SchemaPolicy::with_default(PolicyAction::DiscardRow));
    let batches = vec![
        MemoryBatch::new(vec![json!({"id": 1})]).with_checkpoint(json!({"b": 0})),
        // The surprise column gets this whole row discarded.
        MemoryBatch::new(vec![json!({"id": 2, "surprise": "x"}), json!({"id": 3})])
            .with_checkpoint(json!({"b": 1})),
    ];
    let report = Engine::new(
        config,
        stream_with_batches(StreamSpec::new("s"), batches),
        MemoryDestination::new(),
    )
    .run()
    .await
    .expect("run");

    let read: u64 = report.streams.values().map(|s| s.rows_read).sum();
    let loaded: u64 = report.tables.values().map(|t| t.rows).sum();
    let discarded: u64 = report.tables.values().map(|t| t.discarded_rows).sum();
    assert!(discarded > 0, "the fixture must discard a row");
    assert_eq!(
        read,
        loaded + discarded,
        "reads = loads + discards, so the divergence is the discard"
    );
}
