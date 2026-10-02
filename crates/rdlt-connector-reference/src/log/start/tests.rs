// Logs kept nowhere grow from the process's first log source: each test needs a process of its
// own, which nextest, the runner of `just test`, gives it.

use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::{
    ConnectContext, ConnectorError, ConnectorErrorKind, Source, acknowledging_source_factory,
    source_factory,
};
use serde_json::{Value, json};

use super::super::tests::{Sent, events, offset, p, read};
use super::super::{LogSource, Logged, LoggedStream, Offset};
use crate::kept::Kept;

/// A stream of one partition holding forty messages and growing `per_second` a second.
fn logged(per_second: u64, replayable: bool, retention: Option<u64>) -> Logged {
    let stream = json!({
        "name": "events", "partitions": 1, "messages": 40, "per_second": per_second,
        "replayable": replayable, "retention": retention,
    });
    Logged(serde_json::from_value::<LoggedStream>(stream).unwrap())
}

/// A source of a new group, kept in a file where `lasting`.
fn grouped(lasting: bool) -> LogSource {
    LogSource {
        seed: 3,
        streams: Vec::new(),
        group: Arc::new(Kept::default()),
        lasting,
    }
}

/// The kind and code `refused` carries, and whether a retry may pass.
fn refusal(refused: &ConnectorError) -> (ConnectorErrorKind, Option<&str>, bool) {
    (refused.kind(), refused.code(), refused.is_retryable())
}

const UNISSUED: (ConnectorErrorKind, Option<&str>, bool) =
    (ConnectorErrorKind::Data, Some("cursor_unissued"), false);

#[test]
fn a_start_is_accepted_up_to_the_head_every_process_agrees_on_and_refused_one_past_it() {
    let at = |next| Offset { next };
    // A group kept in a file, and a log that does not grow, have a head every process agrees
    // on: the head is issued, one past it never was.
    for (lasting, per_second) in [(true, 0), (true, 7), (false, 0)] {
        let (stream, source) = (logged(per_second, true, None), grouped(lasting));
        for issued in [0, 1, 39, 40] {
            let start = stream.accept(&source, &p(0), at(issued), 40);
            assert_eq!(start.unwrap(), issued, "{lasting} {per_second}");
        }
        for forged in [41, 42, u64::MAX] {
            let refused = stream.accept(&source, &p(0), at(forged), 40).unwrap_err();
            assert_eq!(
                refusal(&refused),
                UNISSUED,
                "{lasting} {per_second} {forged}"
            );
        }
    }
    // A log that grows from each process's start may have issued any offset before this one.
    let (stream, source) = (logged(7, true, None), grouped(false));
    for issued in [40, 41, u64::MAX] {
        assert_eq!(
            stream.accept(&source, &p(0), at(issued), 40).unwrap(),
            issued
        );
    }
}

#[test]
fn a_start_at_or_below_what_the_group_committed_is_always_issued() {
    let at = |next| Offset { next };
    let (stream, source) = (logged(7, true, None), grouped(true));
    source.group.advance("events", &p(0), 50).unwrap();
    for issued in [40, 41, 50] {
        assert_eq!(
            stream.accept(&source, &p(0), at(issued), 40).unwrap(),
            issued
        );
    }
    let refused = stream.accept(&source, &p(0), at(51), 40).unwrap_err();
    assert_eq!(refusal(&refused), UNISSUED);
    // Another partition's commit, and another stream's, issue nothing here.
    let other = grouped(true);
    other.group.advance("events", &p(1), 50).unwrap();
    other.group.advance("orders", &p(0), 50).unwrap();
    let stream = Logged(LoggedStream {
        partitions: 2,
        ..logged(7, true, None).0
    });
    let refused = stream.accept(&other, &p(0), at(41), 40).unwrap_err();
    assert_eq!(refusal(&refused), UNISSUED);
    // A head past what was committed is the bound.
    assert_eq!(stream.accept(&other, &p(1), at(60), 60).unwrap(), 60);
    let refused = stream.accept(&other, &p(1), at(61), 60).unwrap_err();
    assert_eq!(refusal(&refused), UNISSUED);
}

#[test]
fn a_start_before_what_a_forgetting_log_committed_or_still_holds_is_gone_not_unissued() {
    let at = |next| Offset { next };
    let (stream, forgetting) = (logged(0, false, None), grouped(true));
    forgetting.group.advance("events", &p(0), 20).unwrap();
    let gone = stream.accept(&forgetting, &p(0), at(19), 40).unwrap_err();
    assert_eq!(refusal(&gone), (ConnectorErrorKind::Transient, None, true));
    assert_eq!(stream.accept(&forgetting, &p(0), at(20), 40).unwrap(), 20);
    // A log that serves again reads from before what was committed.
    let replayable = logged(0, true, None);
    assert_eq!(
        replayable.accept(&forgetting, &p(0), at(19), 40).unwrap(),
        19
    );
    // Retention: the log holds its last ten; a start from nothing begins at the earliest.
    let kept = logged(0, true, Some(10));
    let source = grouped(true);
    assert_eq!(kept.accept(&source, &p(0), at(0), 40).unwrap(), 30);
    assert_eq!(kept.accept(&source, &p(0), at(30), 40).unwrap(), 30);
    let lost = kept.accept(&source, &p(0), at(29), 40).unwrap_err();
    assert_eq!(lost.code(), Some("retention_lost"));
    // A partition the stream never has is refused before its cursor is looked at.
    let none = kept.accept(&source, &p(1), at(u64::MAX), 40).unwrap_err();
    assert_eq!(none.kind(), ConnectorErrorKind::Data);
    assert_eq!(none.code(), None);
}

fn growing(group: &Value, replayable: bool) -> Value {
    let stream = json!({
        "name": "events", "partitions": 1, "messages": 5, "per_second": 10,
        "replayable": replayable,
    });
    let mut config = json!({ "seed": 3, "streams": [stream] });
    for (field, value) in group.as_object().unwrap() {
        config[field] = value.clone();
    }
    config
}

async fn connected(config: &Value) -> Box<dyn Source> {
    let factory = source_factory::<LogSource>();
    let connected = factory.connect(config.clone(), ConnectContext::new());
    connected.await.expect("the source connects")
}

/// A source of `config` connected for a host named to a listening connector.
async fn hosted(config: &Value) -> Box<dyn Source> {
    let context = ConnectContext::serving("a.example");
    let factory = source_factory::<LogSource>();
    let connected = factory.connect(config.clone(), context);
    connected.await.expect("the source connects")
}

/// A read from `next` that does not follow, or follows for `follow`.
async fn read_from(
    source: &dyn Source,
    next: u64,
    follow: Option<Duration>,
) -> (rdlt_connector::Result<()>, Sent) {
    read(source, Some(offset(next)), follow).await
}

const AHEAD: (ConnectorErrorKind, Option<&str>, bool) =
    (ConnectorErrorKind::Transient, Some("cursor_ahead"), true);

#[tokio::test(start_paused = true)]
async fn a_start_a_growing_log_kept_nowhere_has_yet_to_reach_sends_nothing_before_it_is_there() {
    let source = connected(&growing(&json!({ "group": "ahead" }), true)).await;
    // The head is five; thirty arrives after two and a half seconds.
    let (ended, sent) = read_from(source.as_ref(), 30, None).await;
    assert_eq!(refusal(&ended.unwrap_err()), AHEAD);
    assert!(sent.offsets.is_empty() && sent.checkpoints.is_empty());
    // A following read waits; stopped before the head is there, it ends as a failure too.
    let (ended, sent) = read_from(source.as_ref(), 30, Some(Duration::from_millis(2_400))).await;
    assert_eq!(refusal(&ended.unwrap_err()), AHEAD);
    assert!(sent.offsets.is_empty() && sent.checkpoints.is_empty());
    // Stopped once the head is there, it ends cleanly, and what it sent starts where it asked.
    let (ended, sent) = read_from(source.as_ref(), 30, Some(Duration::from_millis(1_050))).await;
    ended.expect("the head passed the start");
    assert_eq!(sent.offsets.first(), Some(&30));
    assert!(sent.offsets.windows(2).all(|pair| pair[1] == pair[0] + 1));
    assert_eq!(
        sent.checkpoints.last().copied(),
        sent.offsets.last().map(|last| last + 1)
    );
    // A start at the head, which the log issued, waits and ends cleanly having sent nothing.
    let head = sent.checkpoints.last().copied().unwrap();
    let (ended, sent) = read_from(source.as_ref(), head, None).await;
    ended.expect("a read at the head ends");
    assert!(sent.offsets.is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_bounded_log_that_grows_ends_a_read_from_past_its_head_as_one_to_try_again() {
    let mut config = growing(&json!({ "group": "bounded_ahead" }), true);
    config["streams"][0]["bounded"] = json!(true);
    let source = connected(&config).await;
    // A bounded log's read ends at the head though it was asked to follow.
    for follow in [None, Some(Duration::from_secs(60))] {
        let (ended, sent) = read_from(source.as_ref(), 30, follow).await;
        assert_eq!(refusal(&ended.unwrap_err()), AHEAD, "{follow:?}");
        assert!(sent.offsets.is_empty() && sent.checkpoints.is_empty());
    }
    let (ended, sent) = read_from(source.as_ref(), 5, Some(Duration::from_secs(60))).await;
    ended.expect("a read at the head ends");
    assert!(sent.offsets.is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_start_exactly_reached_when_a_following_read_is_stopped_ends_cleanly() {
    let source = connected(&growing(&json!({ "group": "exactly" }), true)).await;
    // Thirty is the head after two and a half seconds, to the millisecond.
    let (ended, sent) = read_from(source.as_ref(), 30, Some(Duration::from_millis(2_499))).await;
    assert_eq!(refusal(&ended.unwrap_err()), AHEAD);
    assert!(sent.offsets.is_empty());
    let (ended, sent) = read_from(source.as_ref(), 30, Some(Duration::from_millis(1))).await;
    ended.expect("the head is at the start");
    assert!(sent.offsets.is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_log_kept_nowhere_does_not_fall_back_when_its_group_s_last_source_goes() {
    for replayable in [true, false] {
        let group = json!({ "group": format!("goes_{replayable}") });
        let config = growing(&group, replayable);
        let source = hosted(&config).await;
        tokio::time::advance(Duration::from_secs(3)).await;
        let (ended, sent) = read(source.as_ref(), None, None).await;
        ended.expect("the read ends");
        let issued = sent.checkpoints.last().copied().unwrap();
        assert!(issued >= 35, "{issued}");
        let at = [(p(0), offset(issued))];
        source.committed(&events(), &at).await.unwrap();
        // The last source of the group goes, and the group with it; the process's logs go on
        // from when its first log source was connected, so the offset is under the head still.
        drop(source);
        let source = hosted(&config).await;
        let (ended, sent) = read_from(source.as_ref(), issued, None).await;
        ended.unwrap_or_else(|error| panic!("{replayable}: {error}"));
        assert!(sent.offsets.is_empty(), "{replayable}");
        tokio::time::advance(Duration::from_secs(1)).await;
        let (ended, sent) = read_from(source.as_ref(), issued, None).await;
        ended.expect("the read ends");
        assert_eq!(sent.offsets, (issued..issued + 10).collect::<Vec<u64>>());
        // The group forgot what it was told with its last source.
        let (ended, sent) = read(source.as_ref(), None, None).await;
        ended.expect("a read from the start");
        assert_eq!(sent.offsets.first(), Some(&0), "{replayable}");
    }
}

#[tokio::test]
async fn an_offset_issued_before_a_restart_of_a_growing_log_kept_in_a_file_is_at_or_under_its_head()
{
    let dir = crate::scratch::tempdir().unwrap();
    for replayable in [true, false] {
        let path = dir.path().join(format!("{replayable}.group"));
        let mut config = growing(&json!({ "group_path": path }), replayable);
        config["streams"][0]["per_second"] = json!(1_000);
        let source = connected(&config).await;
        tokio::time::sleep(Duration::from_millis(40)).await;
        let (ended, sent) = read(source.as_ref(), None, None).await;
        ended.expect("the read ends");
        let issued = sent.checkpoints.last().copied().unwrap();
        assert!(issued >= 45, "{issued}");
        if !replayable {
            source
                .committed(&events(), &[(p(0), offset(issued))])
                .await
                .unwrap();
        }
        // The last source of the group goes, and its keeper with it, as with its process; the
        // next finds in the file when the logs began.
        drop(source);
        let source = connected(&config).await;
        let (ended, sent) = read_from(source.as_ref(), issued, None).await;
        ended.unwrap_or_else(|error| panic!("{replayable}: {error}"));
        assert!(sent.offsets.first().is_none_or(|first| *first == issued));
        // Every process counts the head from the same beginning, so one far past it is forged.
        let (ended, sent) = read_from(source.as_ref(), issued + 1_000_000, None).await;
        assert_eq!(refusal(&ended.unwrap_err()), UNISSUED, "{replayable}");
        assert!(sent.offsets.is_empty() && sent.checkpoints.is_empty());
        let (ended, _) = read_from(source.as_ref(), u64::MAX, Some(Duration::from_millis(5))).await;
        assert_eq!(refusal(&ended.unwrap_err()), UNISSUED, "{replayable}");
    }
}

#[tokio::test(start_paused = true)]
async fn logs_kept_nowhere_grow_from_when_the_process_first_connected_a_log_source() {
    let first = connected(&growing(&json!({ "group": "first_connected" }), true)).await;
    let (ended, sent) = read(first.as_ref(), None, None).await;
    ended.expect("the read ends");
    let then = sent.checkpoints.last().copied().unwrap();
    tokio::time::advance(Duration::from_secs(2)).await;
    // A source connected later, of that group or another, finds the logs as far as the first
    // does: twenty messages on.
    let same = connected(&growing(&json!({ "group": "first_connected" }), true)).await;
    let other = connected(&growing(&json!({ "group": "connected_later" }), true)).await;
    for source in [&first, &same, &other] {
        let (ended, sent) = read(source.as_ref(), None, None).await;
        ended.expect("the read ends");
        assert_eq!(sent.checkpoints.last(), Some(&(then + 20)));
    }
}

/// A source of `config` connected for `host`, with the reader of what its group committed.
async fn serving(
    host: Option<&str>,
    config: &Value,
) -> rdlt_connector::Result<(Arc<dyn Source>, Arc<dyn rdlt_connector::AcknowledgedReader>)> {
    let context = host.map_or_else(ConnectContext::new, ConnectContext::serving);
    let factory = acknowledging_source_factory::<LogSource>();
    let (source, reader) = factory
        .connect_acknowledging(config.clone(), context)
        .await?;
    Ok((source, reader))
}

#[tokio::test]
async fn two_hosts_that_name_one_group_share_nothing_and_a_group_goes_with_its_last_source() {
    let config = growing(&json!({ "group": "of_two_hosts" }), false);
    let (ours, told) = serving(Some("a.example"), &config).await.unwrap();
    let (again, told_again) = serving(Some("a.example"), &config).await.unwrap();
    let (theirs, told_them) = serving(Some("b.example"), &config).await.unwrap();
    let (own, told_own) = serving(None, &config).await.unwrap();
    ours.committed(&events(), &[(p(0), offset(4))])
        .await
        .unwrap();
    let at = |next| Some(offset(next));
    assert_eq!(told.acknowledged(&events(), &p(0)).await.unwrap(), at(4));
    assert_eq!(
        told_again.acknowledged(&events(), &p(0)).await.unwrap(),
        at(4)
    );
    assert_eq!(
        told_them.acknowledged(&events(), &p(0)).await.unwrap(),
        None
    );
    assert_eq!(told_own.acknowledged(&events(), &p(0)).await.unwrap(), None);
    // What one host committed is not gone for another: its forgetting log serves it the start.
    let (ended, sent) = read(theirs.as_ref(), None, None).await;
    ended.expect("the other host reads from the start");
    assert_eq!(sent.offsets.first(), Some(&0));
    let (ended, _) = read(again.as_ref(), None, None).await;
    assert_eq!(ended.unwrap_err().kind(), ConnectorErrorKind::Transient);
    // The group stays while a source of its host holds it, and goes with the last.
    drop((ours, told));
    assert_eq!(
        told_again.acknowledged(&events(), &p(0)).await.unwrap(),
        at(4)
    );
    own.committed(&events(), &[(p(0), offset(2))])
        .await
        .unwrap();
    drop((again, told_again, theirs, told_them, own, told_own));
    let (_anew, told) = serving(Some("a.example"), &config).await.unwrap();
    assert_eq!(told.acknowledged(&events(), &p(0)).await.unwrap(), None);
    // The process's own host's group is kept between its sources, for the process.
    let (_own, told) = serving(None, &config).await.unwrap();
    assert_eq!(told.acknowledged(&events(), &p(0)).await.unwrap(), at(2));
}

#[tokio::test]
async fn a_group_file_one_host_keeps_is_refused_another_and_kept_for_the_next() {
    let dir = crate::scratch::tempdir().unwrap();
    let config = growing(
        &json!({ "group_path": dir.path().join("events.group") }),
        false,
    );
    let (ours, told) = serving(Some("a.example"), &config).await.unwrap();
    ours.committed(&events(), &[(p(0), offset(4))])
        .await
        .unwrap();
    for host in [Some("b.example"), None] {
        let Err(refused) = serving(host, &config).await else {
            panic!("{host:?} shares the file");
        };
        assert_eq!(refused.kind(), ConnectorErrorKind::Config, "{host:?}");
    }
    drop((ours, told));
    let (_theirs, told) = serving(Some("b.example"), &config).await.unwrap();
    let kept = told.acknowledged(&events(), &p(0)).await.unwrap();
    assert_eq!(kept, Some(offset(4)));
}

#[tokio::test(start_paused = true)]
async fn a_checkpoint_follows_every_so_many_batches_as_the_stream_says() {
    let stream = |every: u64| {
        json!({ "seed": 3, "group": format!("every_{every}"), "streams": [{
            "name": "events", "partitions": 1, "messages": 70, "checkpoint_batches": every,
        }]})
    };
    // Seven batches of ten.
    for (every, expected) in [
        (1, vec![10, 20, 30, 40, 50, 60, 70]),
        (3, vec![30, 60]),
        (7, vec![70]),
        (8, Vec::new()),
        (0, vec![10, 20, 30, 40, 50, 60, 70]),
    ] {
        let source = connected(&stream(every)).await;
        let (ended, sent) = read(source.as_ref(), None, None).await;
        ended.expect("the read ends");
        assert_eq!(sent.offsets, (0..70).collect::<Vec<u64>>(), "{every}");
        assert_eq!(sent.checkpoints, expected, "{every}");
    }
}
