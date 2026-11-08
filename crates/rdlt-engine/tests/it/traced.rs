//! The engine's spans: a run, its attempts, each partition's read, each commit and its phases,
//! and every connector call but a write; none a batch.

use rdlt_connector::ReadMode;
use rdlt_engine::{RunStatus, WriteMode};
use rdlt_testkit::spans::Spans;

use crate::changes::{changes_of, orders};
use crate::support::{commit_every, engine, generator, memory, pipeline, stream};

#[tokio::test(start_paused = true)]
async fn a_run_traces_its_attempts_reads_commits_and_calls_but_no_batch() {
    let spans = Spans::default();
    let _traced = tracing::subscriber::set_default(spans.clone());
    let source = generator(&[("events", 300, 2, 10)]).await;
    let outcome = engine(commit_every(100))
        .run(
            pipeline("traced", [stream("events")]),
            source,
            memory("traced").await,
        )
        .await;
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    assert_eq!(spans.named("rdlt.run").len(), 1);
    assert!(spans.named("rdlt.run")[0].contains("pipeline=traced"));
    assert!(!spans.named("rdlt.attempt").is_empty());
    assert_eq!(
        spans.named("rdlt.stream").len(),
        2,
        "a span a partition's read"
    );
    let commits = usize::try_from(outcome.report.commits).unwrap();
    assert!(spans.named("rdlt.commit").len() >= commits);
    for phase in [
        "rdlt.commit.flush",
        "rdlt.commit.publish",
        "rdlt.commit.ack",
    ] {
        assert!(!spans.named(phase).is_empty(), "{phase}");
    }
    let calls = spans.named("rdlt.call");
    assert!(calls.iter().any(|fields| fields.contains("the commit")));
    assert!(
        calls.iter().all(|fields| !fields.contains("the write")),
        "no span a batch"
    );
}

#[tokio::test(start_paused = true)]
async fn a_run_is_one_trace_from_its_attempt_to_its_commits_and_writer_calls() {
    let spans = Spans::default();
    let _traced = tracing::subscriber::set_default(spans.clone());
    let source = generator(&[("events", 300, 2, 10)]).await;
    let outcome = engine(commit_every(100))
        .run(
            pipeline("traced", [stream("events")]),
            source,
            memory("one-trace").await,
        )
        .await;
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    assert!(spans.all_descend_from("rdlt.attempt", "rdlt.run"));
    for name in [
        "rdlt.stream",
        "rdlt.commit",
        "rdlt.commit.barrier",
        "rdlt.commit.flush",
        "rdlt.commit.publish",
        "rdlt.commit.ack",
        "rdlt.call",
    ] {
        assert!(spans.all_descend_from(name, "rdlt.attempt"), "{name}");
    }
    assert!(spans.all_descend_from("rdlt.commit.publish", "rdlt.commit"));
}

#[tokio::test]
async fn the_partitions_of_a_later_phase_are_read_in_spans_of_the_run() {
    let spans = Spans::default();
    let _traced = tracing::subscriber::set_default(spans.clone());
    let snapshot = orders(&[]);
    let partitions = usize::try_from(snapshot.snapshot_partitions).unwrap();
    let source = changes_of(7, &[snapshot]).await;
    let plan = pipeline(
        "traced",
        [stream("orders").read(ReadMode::Cdc).write(WriteMode::Merge)],
    );
    let outcome = engine(commit_every(8))
        .run(plan, source, memory("later-phase").await)
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert!(
        spans.named("rdlt.stream").len() > partitions,
        "a span for the changes read after the snapshot's"
    );
    assert!(spans.all_descend_from("rdlt.stream", "rdlt.attempt"));
}
