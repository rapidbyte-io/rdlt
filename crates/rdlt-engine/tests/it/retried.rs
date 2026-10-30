//! A commit the destination answered transiently, sent again in place by the same key: the run
//! loads in one attempt, opening the destination once and reading nothing again; a commit whose
//! answer is lost, or whose wait ends, ends its attempt; and the attempt's end and a stop are
//! heard while a commit is in flight.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use rdlt_connector::wire::TRANSPORT;
use rdlt_connector::{CommitMeta, ConnectorErrorKind, ReadMode};
use rdlt_engine::{ErrorKind, Origin, PipelinePlan, RetryPolicy, RunStatus, StopMode, codes};

use crate::support::destinations::{Gate, counting, gated};
use crate::support::faults::{Failure, Fault, Rule, failing_commits, first_tries};
use crate::support::script::{self, Script, ScriptStream};
use crate::support::{commit_every, engine, memory, pipeline, published_ids, stream, until};

fn incremental(name: &str) -> PipelinePlan {
    pipeline(name, [stream("events").read(ReadMode::Incremental)])
}

/// Every id of two partitions of `rows` rows each, sorted.
fn ids(rows: u64) -> Vec<i64> {
    let mut ids: Vec<i64> = (0..2)
        .flat_map(|partition| (0..rows).map(move |offset| script::id(partition, offset)))
        .collect();
    ids.sort_unstable();
    ids
}

/// The fault of the `n`th commit of a run (from 1): `failure` before it lands where `n` is even,
/// once it landed where `n` is odd.
fn alternating(n: usize, failure: Failure) -> Fault {
    if n.is_multiple_of(2) {
        Fault::Before(failure)
    } else {
        Fault::After(failure)
    }
}

#[tokio::test(start_paused = true)]
async fn a_run_whose_every_commit_is_first_answered_transiently_loads_in_one_attempt() {
    let commits = AtomicUsize::new(0);
    let rule = first_tries(move |_: &CommitMeta| {
        let n = commits.fetch_add(1, Ordering::SeqCst) + 1;
        alternating(n, Failure::Transient)
    });
    let (destination, sessions) = counting(failing_commits(memory("retried_in_place").await, rule));
    let (script, source) = Script::new(vec![ScriptStream::new("events", 2, 30, 5)])
        .connect("retried_in_place")
        .await;
    let outcome = engine(commit_every(10))
        .run(incremental("retried-in-place"), source, destination)
        .await;
    let report = &outcome.report;
    assert_eq!(report.status, RunStatus::Succeeded, "{:?}", outcome.error);
    assert_eq!(report.attempted, 1);
    assert_eq!(sessions.opened.load(Ordering::SeqCst), 1);
    assert_eq!(
        script.pushed.load(Ordering::SeqCst),
        60,
        "nothing read again"
    );
    assert_eq!(published_ids("retried_in_place", "events"), ids(30));
    assert!(report.commits >= 2, "{report:?}");
    assert_eq!(report.counters.commits.retried, report.commits);
}

#[tokio::test(start_paused = true)]
async fn a_commit_whose_answer_is_lost_or_whose_wait_ends_ends_its_attempt_each_time() {
    for (failure, name) in [
        (Failure::Host(rdlt_host::CONNECTOR_LOST), "lost_connector"),
        (Failure::Host(TRANSPORT), "lost_transport"),
        (Failure::Hang, "lost_deadline"),
    ] {
        // The first commit lands and then fails, the second fails before it lands, and every
        // later one lands.
        let commits = AtomicUsize::new(0);
        let rule = first_tries(move |_: &CommitMeta| {
            let n = commits.fetch_add(1, Ordering::SeqCst) + 1;
            if n <= 2 {
                alternating(n, failure)
            } else {
                Fault::None
            }
        });
        let (destination, sessions) = counting(failing_commits(memory(name).await, rule));
        let (_, source) = Script::new(vec![ScriptStream::new("events", 2, 30, 5)])
            .connect(name)
            .await;
        let retry = RetryPolicy::default()
            .initial(Duration::from_millis(10))
            .max_delay(Duration::from_millis(100));
        let config = commit_every(10)
            .retry(retry)
            .connector_wait(Duration::from_secs(1));
        let outcome = engine(config)
            .run(incremental(name), source, destination)
            .await;
        let report = &outcome.report;
        assert_eq!(report.status, RunStatus::Succeeded, "{failure:?}");
        assert_eq!(report.counters.commits.retried, 0, "{failure:?}");
        assert_eq!(report.attempted, 3, "{failure:?}");
        assert_eq!(sessions.opened.load(Ordering::SeqCst), 3, "{failure:?}");
        let (last, failed) = report.attempts.split_last().expect("an attempt");
        assert!(last.error.is_none(), "{failure:?}");
        for attempt in failed {
            let error = attempt
                .error
                .as_ref()
                .expect("an attempt before the last failed");
            let found = (error.origin, error.code, error.connector_code.as_deref());
            let expected = match failure {
                Failure::Host(code) => (Origin::Destination, None, Some(code)),
                _ => (Origin::Engine, Some(codes::DEADLINE_EXCEEDED), None),
            };
            assert_eq!(found, expected, "{failure:?}");
        }
        assert_eq!(published_ids(name, "events"), ids(30), "{failure:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_partition_failing_while_a_commit_hangs_ends_the_attempt_within_a_second() {
    let gate = Gate::closed();
    let mut events = ScriptStream::new("events", 1, 20, 10);
    events.idle = true;
    let (script, source) = Script::new(vec![events]).connect("hung_commit").await;
    let config = commit_every(10).retry(RetryPolicy::default().max_attempts(1));
    let destination = gated(memory("hung_commit").await, Arc::clone(&gate));
    let run = engine(config).run(incremental("hung-commit"), source, destination);
    let failing = async {
        until(|| gate.started.load(Ordering::SeqCst) == 1).await;
        // The read waits at its end; the batch it reads next, the run's third, fails.
        script.faults.lock().push(script::Fault {
            batch: 3,
            kind: ConnectorErrorKind::Data,
            retry_after: None,
        });
        script.streams[0].grow(10);
        tokio::time::Instant::now()
    };
    let (outcome, failed_at) = tokio::join!(run, failing);
    let ended = failed_at.elapsed();
    assert!(ended < Duration::from_secs(1), "{ended:?}");
    let error = outcome.error.expect("the partition's failure ends the run");
    assert_eq!(error.kind(), ErrorKind::Source);
}

#[tokio::test(start_paused = true)]
async fn a_stop_after_commit_while_a_commit_waits_to_be_sent_again_stops_the_run_first() {
    let tries = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&tries);
    let rule: Arc<Rule> = Arc::new(move |n: usize, _: &CommitMeta| {
        counted.store(n, Ordering::SeqCst);
        Fault::Before(Failure::Limited(Duration::from_secs(60)))
    });
    let destination = failing_commits(memory("stopped_retry").await, rule);
    let (_, source) = Script::new(vec![ScriptStream::new("events", 1, 20, 5)])
        .connect("stopped_retry")
        .await;
    let started = tokio::time::Instant::now();
    let run = engine(commit_every(10)).run(incremental("stopped-retry"), source, destination);
    let control = run.control();
    let stop = async {
        until(|| tries.load(Ordering::SeqCst) == 1).await;
        control.stop(StopMode::AfterCommit);
    };
    let (outcome, ()) = tokio::join!(run, stop);
    assert_eq!(outcome.report.status, RunStatus::Stopped);
    assert_eq!(tries.load(Ordering::SeqCst), 1, "no try after the stop");
    assert!(started.elapsed() < Duration::from_secs(60));
    let error = outcome.error.expect("the commit's error");
    assert_eq!(
        (error.kind(), error.origin(), error.is_retryable()),
        (ErrorKind::Destination, Origin::Destination, true)
    );
}
