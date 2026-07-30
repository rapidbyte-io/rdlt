//! US5 — Observable runs (spec acceptance scenario 1 + FR-012/FR-014/SC-008).
//!
//! Events arrive in causal order; the report's totals equal destination reality;
//! transient source failures are retried by the ENGINE and counted — no silent
//! failures anywhere.

use rdlt_core::{PipelineEvent, TableName};
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::{MemoryBatch, MemoryDestination, MemorySource, MemoryStream};
use serde_json::json;

fn batches() -> Vec<MemoryBatch> {
    vec![
        MemoryBatch::new(vec![json!({"a": 1}), json!({"a": 2})]).with_checkpoint(1),
        MemoryBatch::new(vec![json!({"a": 3, "b": "late"})]).with_checkpoint(2),
    ]
}

#[tokio::test]
async fn events_are_causally_ordered_and_report_matches_reality() {
    let dest = MemoryDestination::new();
    let source = MemorySource::new(vec![MemoryStream::new(
        rdlt_connector::StreamSpec::new("s"),
        batches(),
    )]);
    let mut config = EngineConfig::new("obs");
    config = config.with_commit_policy(rdlt_core::CommitPolicy::EveryCheckpoints(1));

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
    assert!(
        matches!(seen.first(), Some(PipelineEvent::StreamStarted { .. })),
        "stream start first, got {:?}",
        seen.first()
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

/// FR-014: the engine retries transient failures with backoff; connectors never
/// retry. Retries surface in the report AND as events — never silent.
#[tokio::test]
async fn transient_source_failures_are_retried_and_counted() {
    let dest = MemoryDestination::new();
    let source = MemorySource::new(vec![
        MemoryStream::new(rdlt_connector::StreamSpec::new("s"), batches())
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
        MemoryStream::new(rdlt_connector::StreamSpec::new("s"), batches())
            .transient_start_failures(100),
    ]);
    let err = Engine::new(EngineConfig::new("retry-exhaust"), source, dest)
        .run()
        .await
        .expect_err("must eventually fail");
    assert!(matches!(err, rdlt_core::RdltError::Source { .. }));
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
    config = config.with_commit_policy(rdlt_core::CommitPolicy::EveryCheckpoints(1));

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
