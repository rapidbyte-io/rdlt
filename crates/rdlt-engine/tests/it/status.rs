//! What a run's status says while it runs: what it is in the middle of and since when, which
//! partition went quiet, what waits for memory or the destination, and each stream's freshness.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{ArrayRef, Int8Array};
use rdlt_connector::{CommitMeta, ConnectorErrorKind, Headroom, ReadMode};
use rdlt_engine::{
    Call, CommitPolicy, EngineConfig, EventKind, Phase, RunStatus, Status, Step as CommitStep,
    StopMode,
};

use crate::schema::batch;
use crate::support::destinations::{Step, failing};
use crate::support::faults::{self, Failure, failing_commits};
use crate::support::making::{Step as Made, making};
use crate::support::script::{Fault, Hang, Script, ScriptStream};
use crate::support::{Guarded, commit_every, generator, memory, pipeline, stream, told_engine};

/// Polls `run` until its status holds as `reached` says, failing the test where the run ends
/// first: the status then.
async fn when(run: &mut Guarded, reached: impl Fn(&Status) -> bool) -> Status {
    loop {
        let status = run.status();
        if reached(&status) {
            return status;
        }
        tokio::select! {
            biased;
            outcome = &mut *run => panic!("the run ended first: {:?}", outcome.error),
            () = tokio::time::sleep(Duration::from_millis(10)) => {}
        }
    }
}

/// Polls `run` while `time` passes, failing the test where the run ends.
async fn passing(run: &mut Guarded, time: Duration) {
    tokio::select! {
        biased;
        outcome = &mut *run => panic!("the run ended first: {:?}", outcome.error),
        () = tokio::time::sleep(time) => {}
    }
}

/// Stops `run` at once and waits for it to end.
async fn stopped(run: Guarded) {
    run.control().stop(StopMode::Now);
    assert_eq!(run.await.report.status, RunStatus::Cancelled);
}

#[tokio::test(start_paused = true)]
async fn a_flush_that_hangs_shows_as_its_commit_s_flush_step_and_its_lane_s_call() {
    let (engine, _) = told_engine(commit_every(10));
    let destination = failing(memory("status_flush").await, Step::StallFlushes);
    let source = generator(&[("events", 100, 1, 10)]).await;
    let mut run = engine.run(
        pipeline("status_flush", [stream("events")]),
        source,
        destination,
    );
    let flushing = |status: &Status| {
        matches!(
            status.phase,
            Phase::Committing {
                step: CommitStep::Flush,
                ..
            }
        )
    };
    let hung = when(&mut run, flushing).await;
    passing(&mut run, Duration::from_secs(30)).await;
    let later = run.status();
    assert_eq!(
        (later.phase, later.since),
        (hung.phase, hung.since),
        "the step and when it began stay while it hangs"
    );
    assert!(later.report.elapsed >= hung.report.elapsed + Duration::from_secs(30));
    assert_eq!(later.report.status, RunStatus::Running);
    let lane = later
        .lanes
        .iter()
        .flatten()
        .find(|call| call.call == Call::Flush)
        .expect("a lane flushes");
    assert_eq!(
        lane.since, hung.since,
        "the lane's flush began with the step"
    );
    stopped(run).await;
}

#[tokio::test(start_paused = true)]
async fn a_write_that_hangs_shows_as_its_lane_s_call_since_it_began() {
    let (engine, _) = told_engine(commit_every(1_000));
    let destination = failing(memory("status_write").await, Step::StallWrites);
    let source = generator(&[("events", 100, 1, 10)]).await;
    let mut run = engine.run(
        pipeline("status_write", [stream("events")]),
        source,
        destination,
    );
    let writing = |status: &Status| {
        let mut calls = status.lanes.iter().flatten();
        calls.any(|call| call.call == Call::Write)
    };
    let hung = when(&mut run, writing).await;
    passing(&mut run, Duration::from_secs(30)).await;
    let later = run.status();
    assert_eq!(
        later.lanes, hung.lanes,
        "the call and its start stay while it hangs"
    );
    assert_eq!(later.phase, Phase::Reading);
    let write = later.lanes.iter().flatten().next().expect("a lane writes");
    assert!(
        write.since >= later.since,
        "the write began once reading did"
    );
    stopped(run).await;
}

#[tokio::test(start_paused = true)]
async fn a_retry_s_delay_shows_as_backing_off_until_the_next_attempt() {
    let fault = Fault {
        batch: 1,
        kind: ConnectorErrorKind::RateLimited,
        retry_after: Some(Duration::from_secs(30)),
    };
    let (_, source) = Script::new(vec![ScriptStream::new("events", 1, 20, 5)])
        .fail(fault)
        .connect("status_backoff")
        .await;
    let (engine, told) = told_engine(commit_every(10));
    let mut run = engine.run(
        pipeline("status_backoff", [stream("events")]),
        source,
        memory("status_backoff").await,
    );
    let backing_off = |status: &Status| matches!(status.phase, Phase::BackingOff { .. });
    let waiting = when(&mut run, backing_off).await;
    let Phase::BackingOff { until } = waiting.phase else {
        unreachable!("the status was found backing off");
    };
    let retrying = told
        .events()
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::Retrying { retry_in, .. } => Some((event.at, retry_in)),
            _ => None,
        });
    let (at, retry_in) = retrying.expect("the retry is told");
    assert_eq!(retry_in, Duration::from_secs(30));
    assert_eq!(waiting.since, at, "the wait began as the retry was told");
    assert_eq!(until, at + retry_in);
    passing(&mut run, Duration::from_secs(20)).await;
    assert_eq!(run.status().phase, waiting.phase, "it still waits");
    let outcome = run.await;
    assert_eq!(outcome.report.status, RunStatus::Succeeded);
    assert_eq!(outcome.report.attempted, 2);
}

#[tokio::test(start_paused = true)]
async fn a_push_waiting_for_room_shows_its_wait_since_it_began() {
    // Each push keeps a buffer of 4 MiB alive, and no write returns to free one.
    let steps = Arc::new(|step: usize| {
        let whole = Int8Array::from(vec![1_i8; 4 << 20]);
        let column: ArrayRef = Arc::new(whole.slice(0, 3));
        (step < 64).then(|| Made::Batch(batch(vec![("value", column)])))
    });
    let source = making("status_intake", steps).await;
    let config = commit_every(1_000_000).memory(40 << 20).lanes(1);
    let (engine, _) = told_engine(config);
    let destination = failing(memory("status_intake").await, Step::StallWrites);
    let mut run = engine.run(
        pipeline("status_intake", [stream("events")]),
        source,
        destination,
    );
    let waiting = when(&mut run, |status| status.waiting.intake.is_some()).await;
    passing(&mut run, Duration::from_secs(30)).await;
    let later = run.status();
    assert_eq!(
        later.waiting.intake, waiting.waiting.intake,
        "the oldest wait and its start stay while it waits"
    );
    assert!(later.waiting.intake >= Some(later.since));
    stopped(run).await;
}

#[tokio::test(start_paused = true)]
async fn the_quietest_partition_is_the_one_whose_read_hangs_since_it_last_said_anything() {
    let mut hanging = ScriptStream::new("events", 2, 20, 5);
    hanging.hang = Hang::Partition(1);
    let (_, source) = Script::new(vec![hanging]).connect("status_quiet").await;
    let (engine, _) = told_engine(commit_every(10));
    let mut run = engine.run(
        pipeline("status_quiet", [stream("events")]),
        source,
        memory("status_quiet").await,
    );
    let one_ended = |status: &Status| status.partitions.ended == 1;
    let quiet = when(&mut run, one_ended).await;
    passing(&mut run, Duration::from_secs(30)).await;
    let later = run.status();
    let quietest = later.quietest.clone().expect("a partition reads");
    assert_eq!(quietest.partition, "p1");
    assert_eq!(quietest.stream, "events");
    assert_eq!(
        Some(quietest.clone()),
        quiet.quietest,
        "it stays quiet since"
    );
    assert!(quietest.since >= later.since);
    assert_eq!(
        (
            later.partitions.reading,
            later.partitions.ended,
            later.partitions.waiting
        ),
        (1, 1, 0)
    );
    stopped(run).await;
}

#[tokio::test(start_paused = true)]
async fn a_stream_s_landed_instant_moves_with_each_commit_and_not_without_one() {
    let mut idle = ScriptStream::new("events", 1, 5, 5);
    idle.idle = true;
    let (script, source) = Script::new(vec![idle]).connect("status_landed").await;
    let interval = CommitPolicy::new(Some(Duration::from_secs(10)), None, None).unwrap();
    let (engine, told) = told_engine(EngineConfig::builder().commit(interval));
    let plan = pipeline(
        "status_landed",
        [stream("events").read(ReadMode::Incremental)],
    );
    let mut run = engine.run(plan, source, memory("status_landed").await);
    let landed = |status: &Status| {
        status
            .streams
            .get("events")
            .and_then(|stream| stream.landed)
    };
    let committed = || {
        let events = told.events().into_iter();
        let commits = events.filter(|event| matches!(event.kind, EventKind::Committed { .. }));
        commits.map(|event| event.at).collect::<Vec<_>>()
    };
    passing(&mut run, Duration::from_secs(5)).await;
    assert_eq!(landed(&run.status()), None, "nothing landed yet");
    passing(&mut run, Duration::from_secs(10)).await;
    let first = committed();
    assert_eq!(first.len(), 1);
    assert_eq!(landed(&run.status()), first.first().copied());
    // The next interval finds nothing new to commit, so nothing lands.
    passing(&mut run, Duration::from_secs(10)).await;
    assert_eq!(committed(), first);
    assert_eq!(landed(&run.status()), first.first().copied());
    script.streams[0].grow(5);
    passing(&mut run, Duration::from_secs(10)).await;
    let second = committed();
    assert_eq!(second.len(), 2);
    assert!(second[1] > second[0]);
    assert_eq!(landed(&run.status()), second.last().copied());
    run.control().stop(StopMode::AfterCommit);
    assert_eq!(run.await.report.status, RunStatus::Stopped);
}

#[tokio::test(start_paused = true)]
async fn a_headroom_sent_with_a_lag_shows_in_its_stream_s_status() {
    let headroom = Headroom {
        bytes: Some(64 << 20),
        time: Some(Duration::from_secs(3_600)),
    };
    let steps = Arc::new(move |step: usize| match step {
        0 => {
            let column: ArrayRef = Arc::new(Int8Array::from(vec![1_i8; 3]));
            Some(Made::Batch(batch(vec![("value", column)])))
        }
        1 => Some(Made::Checkpoint(8)),
        2 => Some(Made::Behind(7, headroom)),
        3 => Some(Made::Wait(Box::pin(std::future::pending()))),
        _ => None,
    });
    let source = making("status_headroom", steps).await;
    let (engine, _) = told_engine(commit_every(1_000));
    let mut run = engine.run(
        pipeline("status_headroom", [stream("events")]),
        source,
        memory("status_headroom").await,
    );
    let lagging = |status: &Status| {
        let stream = status.streams.get("events");
        stream.is_some_and(|stream| stream.behind.is_some())
    };
    let status = when(&mut run, lagging).await;
    let stream = &status.streams["events"];
    assert_eq!((stream.behind, stream.headroom), (Some(7), headroom));
    assert_eq!(status.report.streams["events"].headroom, headroom);
    stopped(run).await;
}

#[tokio::test(start_paused = true)]
async fn the_status_of_a_run_that_ended_carries_its_outcome_s_report() {
    let (engine, _) = told_engine(commit_every(10));
    let source = generator(&[("events", 30, 2, 10)]).await;
    let mut run = engine.run(
        pipeline("status_ended", [stream("events")]),
        source,
        memory("status_ended").await,
    );
    let outcome = (&mut run).await;
    let status = run.status();
    assert_eq!(
        status.phase,
        Phase::Ended {
            status: RunStatus::Succeeded
        }
    );
    assert_eq!(status.report, outcome.report);
    assert_eq!(status.report.rows, 30);
}

#[tokio::test(start_paused = true)]
async fn a_commit_sent_again_in_place_shows_its_try_since_the_try_began() {
    let rule: Arc<faults::Rule> = Arc::new(|tried, _: &CommitMeta| match tried {
        1 => faults::Fault::Before(Failure::Limited(Duration::from_secs(30))),
        _ => faults::Fault::Before(Failure::Hang),
    });
    let (engine, _) = told_engine(commit_every(10));
    let destination = failing_commits(memory("status_retried").await, rule);
    let source = generator(&[("events", 10, 1, 10)]).await;
    let mut run = engine.run(
        pipeline("status_retried", [stream("events")]),
        source,
        destination,
    );
    let trying = |tries| Phase::Committing {
        seq: 1,
        step: CommitStep::Commit,
        tries,
    };
    let first = when(&mut run, |status| status.phase == trying(1)).await;
    let again = when(&mut run, |status| status.phase == trying(2)).await;
    assert_eq!(
        again.since,
        first.since + Duration::from_secs(30),
        "the second try began once the wait its first answer asked for passed"
    );
    stopped(run).await;
}
