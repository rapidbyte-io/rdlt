//! A run's events, numbered and complete for a subscriber that keeps up; a stopped run.

use std::num::NonZeroUsize;

use rdlt::{
    Commit, Connector, ErrorKind, EventKind, Events, Origin, Pipeline, Run, RunStatus, Side,
    StopMode, Stream, Until,
};

use crate::support::{memory_host, memory_pipeline, within};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_subscriber_that_keeps_up_reads_every_event_numbered_from_one() {
    let pipeline = memory_pipeline("told", "events-told", &serde_json::json!([{ "id": 1 }]))
        .build(&memory_host())
        .await
        .expect("built");
    let run = pipeline.start();
    let mut events = run.events();
    let reading = tokio::spawn(async move {
        let mut told = Vec::new();
        while let Some(item) = events.next().await {
            told.push(item.expect("no lag"));
        }
        told
    });
    let outcome = within(run).await;
    assert_eq!(
        outcome.status(),
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    let told = within(reading).await.expect("it reads");
    let seqs: Vec<u64> = told.iter().map(|event| event.seq).collect();
    let numbered: Vec<u64> = (1..=u64::try_from(told.len()).unwrap()).collect();
    assert_eq!(seqs, numbered);
    assert_eq!(
        told.first().map(|event| &event.kind),
        Some(&EventKind::AttemptStarted)
    );
    let ended = told.last().map(|event| &event.kind);
    assert!(
        matches!(
            ended,
            Some(EventKind::RunEnded {
                status: RunStatus::Succeeded
            })
        ),
        "{ended:?}"
    );
    assert!(
        told.iter()
            .any(|event| matches!(event.kind, EventKind::Committed { rows: 1, .. }))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_run_s_status_says_what_it_is_doing_and_a_failed_run_its_error() {
    let pipeline = memory_pipeline(
        "failing",
        "events-failing",
        &serde_json::json!([{ "id": 1 }, "no object"]),
    )
    .build(&memory_host())
    .await
    .expect("built");
    let run = pipeline.start();
    assert_eq!(run.status().report.pipeline.as_str(), "failing");
    let outcome = within(run).await;
    assert_eq!(outcome.status(), RunStatus::Failed);
    let error = outcome.error.expect("the error that ended the run");
    assert_eq!(error.stream(), Some("users"));
    assert_eq!(
        (error.kind(), error.origin()),
        (ErrorKind::Source, Origin::Source)
    );
    assert_eq!(error.connector_code(), Some("json_not_object"));
    assert_eq!(error.code(), None, "the source's code is its own");
}

/// A pipeline following one stream of `rows` forever, a page of one row at a time, committing
/// after every row, into memory store `store`.
async fn following(name: &str, store: &str) -> Pipeline {
    let rows: Vec<serde_json::Value> = (0..1_000)
        .map(|id| serde_json::json!({ "id": id }))
        .collect();
    let source = Side::new(Connector::new("io.rapidbyte.memory"))
        .config(serde_json::json!({ "streams": { "users": rows }, "page_size": 1 }));
    let destination = Side::new(Connector::new("io.rapidbyte.memory"))
        .config(serde_json::json!({ "store": store }));
    Pipeline::builder(name, source, destination)
        .stream(Stream::new("users"))
        .commit(Commit::new().rows(1))
        .until(Until::Forever)
        .build(&memory_host())
        .await
        .expect("built")
}

/// Polls `run` until a commit lands, reading `events`; the run has not ended by then.
async fn committed(run: &mut Run, events: &mut Events) {
    let commit = async {
        loop {
            let told = events
                .next()
                .await
                .expect("the run goes on")
                .expect("no lag");
            if matches!(told.kind, EventKind::Committed { .. }) {
                return;
            }
        }
    };
    within(async {
        tokio::select! {
            biased;
            () = commit => {}
            outcome = &mut *run => panic!("the run ended before a commit: {outcome:?}"),
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_run_stopped_after_a_commit_commits_what_is_sealed_and_ends_stopped() {
    let pipeline = following("stopped", "events-stopped").await;
    let mut run = pipeline.start();
    let mut events = run.events();
    committed(&mut run, &mut events).await;
    run.stop(StopMode::AfterCommit);
    let outcome = within(run).await;
    assert_eq!(outcome.status(), RunStatus::Stopped, "{:?}", outcome.error);
    assert!(outcome.report.commits >= 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn another_task_stops_a_run_through_its_control() {
    let pipeline = following("controlled", "events-controlled").await;
    let mut run = pipeline.start();
    let mut events = run.events();
    committed(&mut run, &mut events).await;
    let control = run.control();
    let stopping = tokio::spawn(async move { control.stop(StopMode::AfterCommit) });
    within(stopping).await.expect("it stops");
    let outcome = within(run).await;
    assert_eq!(outcome.status(), RunStatus::Stopped, "{:?}", outcome.error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_run_stopped_now_ends_cancelled() {
    let pipeline = following("now", "events-now").await;
    let mut run = pipeline.start();
    let mut events = run.events();
    committed(&mut run, &mut events).await;
    run.stop(StopMode::Now);
    let outcome = within(run).await;
    assert_eq!(outcome.status(), RunStatus::Cancelled);
    let error = outcome.error.expect("what ended it");
    assert_eq!(error.kind(), ErrorKind::Cancelled);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_run_is_cancelled_and_its_events_end() {
    let pipeline = following("dropped", "events-dropped").await;
    let mut run = pipeline.start();
    let mut events = run.events();
    committed(&mut run, &mut events).await;
    assert!(format!("{run:?}").contains("subscribers: 1"));
    drop(run);
    let mut ended = false;
    while let Some(told) = within(events.next()).await {
        // Events the reader fell behind on before the drop are told as missed, and are no end.
        if let Ok(event) = told {
            ended |= matches!(event.kind, EventKind::RunEnded { .. });
        }
    }
    assert!(!ended, "a run dropped tells no end");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_run_s_events_end_with_its_end_though_the_run_is_kept() {
    let rows = serde_json::json!([{ "id": 1 }]);
    let pipeline = memory_pipeline("kept", "events-kept", &rows)
        .build(&memory_host())
        .await
        .expect("built");
    let mut run = pipeline.start();
    let mut events = run.events();
    let outcome = within(&mut run).await;
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    let mut last = None;
    while let Some(told) = within(events.next()).await {
        last = Some(told.expect("nothing fell behind").kind);
    }
    assert!(matches!(last, Some(EventKind::RunEnded { .. })), "{last:?}");
    assert!(events.next().await.is_none(), "it stays ended");
    drop(run);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_subscriber_come_after_the_run_ended_ends_and_takes_nothing_from_another() {
    let rows = serde_json::json!([{ "id": 1 }]);
    let pipeline = memory_pipeline("late", "events-late", &rows)
        .build(&memory_host())
        .await
        .expect("built");
    let mut run = pipeline.start();
    let mut early = run.events();
    let outcome = within(&mut run).await;
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    let mut late = run.events();
    assert!(
        within(late.next()).await.is_none(),
        "nothing more will come"
    );
    let mut read = Vec::new();
    while let Some(told) = within(early.next()).await {
        read.push(told.expect("nothing fell behind"));
    }
    assert_eq!(
        read.first().map(|event| event.seq),
        Some(1),
        "from the first event"
    );
    let last = read.last().map(|event| &event.kind);
    assert!(matches!(last, Some(EventKind::RunEnded { .. })), "{last:?}");
    drop(run);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_buffer_asked_beyond_the_most_a_ring_keeps_is_held_to_it() {
    let rows = serde_json::json!([{ "id": 1 }]);
    let pipeline = memory_pipeline("held", "events-held", &rows)
        .build(&memory_host())
        .await
        .expect("built")
        .with_event_buffer(NonZeroUsize::MAX);
    let outcome = within(pipeline.start()).await;
    assert_eq!(
        outcome.status(),
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
}
