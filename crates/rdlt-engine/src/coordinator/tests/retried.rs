//! A commit the destination answered transiently, sent again in place by the same key; every
//! other failure ending the attempt; and the attempt's cancel and stop while a commit is in flight.

use std::time::Duration;

use rdlt_connector::{COMMIT_STALE, ConnectorErrorKind, PartitionState};

use super::{Answer, Harness, Setup, Try, cursor, ended, partition, stream, until};
use crate::config::RetryPolicy;
use crate::error::{ErrorKind, Origin};
use crate::plan::WriteMode;

/// The coordinator of one partition whose destination answers its first tries as `tries` says,
/// under `retry`, with a seal of three rows sent and the partition ended.
async fn sealed(
    tries: Vec<Try>,
    retry: RetryPolicy,
) -> (
    tokio::task::JoinHandle<Result<(), crate::error::Error>>,
    Harness,
) {
    let mut setup = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", false)],
    );
    setup.tries = tries;
    setup.retry = retry;
    let (task, harness) = setup.start().await;
    harness.seal(0, 1, 3, PartitionState::Cursor(cursor(3)), None);
    harness.end(0, false);
    (task, harness)
}

/// Waits until the destination was sent `tries` tries of commits.
async fn tried(harness: &Harness, tries: usize) {
    until(|| harness.sent.lock().len() >= tries).await;
}

#[tokio::test(start_paused = true)]
async fn a_commit_answered_transiently_is_sent_again_in_place_and_lands_once() {
    for first in [
        Try::Fail(ConnectorErrorKind::Transient, None),
        Try::LandThenFail(ConnectorErrorKind::Transient),
        Try::Fail(ConnectorErrorKind::RateLimited, None),
        Try::LandThenFail(ConnectorErrorKind::RateLimited),
    ] {
        let (task, harness) = sealed(vec![first], RetryPolicy::default()).await;
        ended(task).await.unwrap();
        let sent = harness.sent.lock();
        assert_eq!(sent.len(), 2, "{first:?}");
        assert_eq!(
            sent[0].0, sent[1].0,
            "{first:?}: the same commit, by the same key"
        );
        assert_eq!(harness.commit_count(), 1, "{first:?}: landed once");
        assert_eq!(harness.acks.lock().len(), 1, "{first:?}");
        assert_eq!(harness.log.lock().committed.commits, 1, "{first:?}");
        assert_eq!(harness.tally.counters().commits.retried, 1, "{first:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_rate_limited_commit_is_sent_again_after_the_wait_it_asked_within_the_policy() {
    let policy = RetryPolicy::default()
        .initial(Duration::from_secs(1))
        .max_delay(Duration::from_secs(5));
    for (asked, waited) in [
        (Duration::from_secs(3), Duration::from_secs(3)),
        (Duration::from_millis(1), Duration::from_secs(1)),
        (Duration::from_hours(1), Duration::from_secs(5)),
    ] {
        let (task, harness) = sealed(vec![Try::Limited(asked)], policy).await;
        ended(task).await.unwrap();
        let sent = harness.sent.lock();
        assert_eq!(sent.len(), 2);
        assert_eq!(sent[1].1 - sent[0].1, waited, "asked for {asked:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn waits_between_tries_are_drawn_no_longer_than_the_policy_s_doubling_delays() {
    let transient = Try::Fail(ConnectorErrorKind::Transient, None);
    let (task, harness) = sealed(vec![transient; 3], RetryPolicy::default()).await;
    ended(task).await.unwrap();
    let sent = harness.sent.lock();
    assert_eq!(sent.len(), 4);
    for (gap, longest) in sent.windows(2).zip([1, 2, 4]) {
        let waited = gap[1].1 - gap[0].1;
        assert!(waited <= Duration::from_secs(longest), "{waited:?}");
    }
    assert_eq!(harness.tally.counters().commits.retried, 3);
}

#[tokio::test(start_paused = true)]
async fn a_commit_answered_transiently_every_try_ends_the_attempt_after_as_many_tries_as_attempts()
{
    let transient = Try::Fail(ConnectorErrorKind::Transient, None);
    let retry = RetryPolicy::default().max_attempts(3);
    let (task, harness) = sealed(vec![transient; 10], retry).await;
    let error = ended(task).await.unwrap_err();
    assert_eq!(
        (error.kind(), error.origin(), error.is_retryable()),
        (ErrorKind::Destination, Origin::Destination, true)
    );
    assert_eq!(harness.sent.lock().len(), 3);
    assert_eq!(harness.tally.counters().commits.retried, 2);
    assert!(harness.acks.lock().is_empty());
    assert!(
        harness.log.lock().pending.is_some(),
        "the commit given up stays in flight for the next open to credit"
    );
}

#[tokio::test(start_paused = true)]
async fn a_commit_failed_for_good_ends_the_attempt_at_its_first_try_and_tells_nothing() {
    let host = |code| Try::Fail(ConnectorErrorKind::Transient, Some(code));
    for (first, kind) in [
        (
            Try::Fail(ConnectorErrorKind::Data, None),
            ErrorKind::Destination,
        ),
        (
            Try::Fail(ConnectorErrorKind::Internal, None),
            ErrorKind::Destination,
        ),
        (
            Try::Fail(ConnectorErrorKind::Fenced, None),
            ErrorKind::Fenced,
        ),
        (
            Try::Fail(ConnectorErrorKind::Fenced, Some(COMMIT_STALE)),
            ErrorKind::Fenced,
        ),
        // The host's own findings about the connection: whether the commit applied is unknown.
        (host("connector_lost"), ErrorKind::Destination),
        (host("transport"), ErrorKind::Destination),
        (host("deadline_exceeded"), ErrorKind::Destination),
        (host("tls"), ErrorKind::Destination),
        (
            Try::LandThenFail(ConnectorErrorKind::Internal),
            ErrorKind::Destination,
        ),
    ] {
        let (task, harness) = sealed(vec![first], RetryPolicy::default()).await;
        let error = ended(task).await.unwrap_err();
        assert_eq!(error.kind(), kind, "{first:?}");
        assert_eq!(harness.sent.lock().len(), 1, "{first:?}: sent once");
        assert!(harness.acks.lock().is_empty(), "{first:?}");
        assert_eq!(harness.log.lock().committed.commits, 0, "{first:?}");
        assert_eq!(harness.tally.counters().commits.retried, 0, "{first:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn the_session_takes_other_calls_while_a_commit_waits_to_be_sent_again() {
    let limited = Try::Limited(Duration::from_secs(60));
    let (task, harness) = sealed(vec![limited], RetryPolicy::default()).await;
    tried(&harness, 1).await;
    let applied = tokio::time::timeout(Duration::from_secs(1), harness.session.apply_schema(&[]))
        .await
        .expect("the session is free between tries");
    assert!(applied.unwrap().is_ok());
    assert_eq!(harness.sent.lock().len(), 1);
    ended(task).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn the_cancel_ends_an_attempt_whose_commit_hangs_at_once() {
    let (task, harness) = sealed(vec![Try::Hang], RetryPolicy::default()).await;
    tried(&harness, 1).await;
    harness.cancel.cancel();
    let error = tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("the attempt ends within a second of its cancel")
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Cancelled);
}

#[tokio::test(start_paused = true)]
async fn the_cancel_ends_an_attempt_whose_commit_waits_to_be_sent_again_at_once() {
    let limited = Try::Limited(Duration::from_secs(60));
    let (task, harness) = sealed(vec![limited], RetryPolicy::default()).await;
    tried(&harness, 1).await;
    harness.cancel.cancel();
    let error = tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("the attempt ends within a second of its cancel")
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Cancelled);
    assert_eq!(harness.sent.lock().len(), 1, "no try after the cancel");
}

#[tokio::test(start_paused = true)]
async fn the_cancel_ends_an_attempt_whose_report_hangs_at_once() {
    let mut setup = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", false)],
    );
    setup.retry = RetryPolicy::default();
    let (task, harness) = setup.start().await;
    harness.answers.lock().push_back(Answer::Hang);
    harness.seal(0, 1, 3, PartitionState::Cursor(cursor(3)), None);
    harness.end(0, false);
    until(|| !harness.asked.lock().is_empty()).await;
    harness.cancel.cancel();
    let error = tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("the attempt ends within a second of its cancel")
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Cancelled);
}

#[tokio::test(start_paused = true)]
async fn a_stop_after_commit_during_a_wait_to_send_again_ends_with_the_commit_s_error() {
    let limited = Try::Limited(Duration::from_secs(60));
    let (task, harness) = sealed(vec![limited], RetryPolicy::default()).await;
    tried(&harness, 1).await;
    harness.stop.cancel();
    let error = tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("the attempt ends within a second of the stop")
        .unwrap()
        .unwrap_err();
    assert_eq!(
        (error.kind(), error.origin(), error.is_retryable()),
        (ErrorKind::Destination, Origin::Destination, true)
    );
    assert_eq!(harness.sent.lock().len(), 1, "no try after the stop");
    assert_eq!(harness.tally.counters().commits.retried, 0);
}
