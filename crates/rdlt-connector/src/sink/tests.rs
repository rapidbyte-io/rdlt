use std::num::NonZeroUsize;

use bytes::Bytes;

use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use super::{
    Admission, Permit, Push, Requested, SourceEvent, admitted_partition_channel, partition_channel,
};
use crate::cursor::Cursor;
use crate::error::ConnectorErrorKind;
use crate::spec::BoxFuture;

fn json(text: &'static str) -> SourceEvent {
    SourceEvent::Push(Push::Json(Bytes::from_static(text.as_bytes())))
}

#[tokio::test]
async fn events_arrive_in_order() {
    let (mut sink, mut feed) = partition_channel(NonZeroUsize::new(4).unwrap());
    sink.send(json("[1]")).await.unwrap();
    sink.send(json("[2]")).await.unwrap();
    drop(sink);
    assert_eq!(feed.recv().await, Some(json("[1]")));
    assert_eq!(feed.recv().await, Some(json("[2]")));
    assert_eq!(feed.recv().await, None);
}

#[tokio::test]
async fn a_stop_request_fails_the_next_send_even_with_room() {
    let (mut sink, feed) = partition_channel(NonZeroUsize::new(4).unwrap());
    feed.stop();
    assert_eq!(
        sink.send(json("[1]")).await.unwrap_err().kind(),
        ConnectorErrorKind::Stopped
    );
}

#[tokio::test(start_paused = true)]
async fn a_stop_request_releases_a_send_blocked_on_a_full_channel() {
    let (mut sink, feed) = partition_channel(NonZeroUsize::MIN);
    sink.send(json("[1]")).await.unwrap();
    let blocked = tokio::spawn(async move { sink.send(json("[2]")).await });
    tokio::task::yield_now().await;
    feed.stop();
    assert_eq!(
        blocked.await.unwrap().unwrap_err().kind(),
        ConnectorErrorKind::Stopped
    );
}

#[tokio::test]
async fn dropping_the_feed_stops_the_sink() {
    let (mut sink, feed) = partition_channel(NonZeroUsize::MIN);
    drop(feed);
    assert_eq!(
        sink.send(json("[1]")).await.unwrap_err().kind(),
        ConnectorErrorKind::Stopped
    );
}

#[tokio::test]
async fn a_checkpoint_answering_a_barrier_clears_it() {
    let (mut sink, feed) = partition_channel(NonZeroUsize::new(4).unwrap());
    assert_eq!(sink.pending_barrier(), None);
    feed.request_checkpoint(2);
    feed.request_checkpoint(1);
    assert_eq!(sink.pending_barrier(), Some(2));
    let cursor = Cursor::encode(1, &0u64).unwrap();
    sink.send(SourceEvent::Checkpoint {
        cursor,
        answers: Some(2),
    })
    .await
    .unwrap();
    assert_eq!(sink.pending_barrier(), None);
    feed.request_checkpoint(3);
    assert_eq!(sink.pending_barrier(), Some(3));
}

/// Admits pushes and checkpoints once opened, recording the bytes each holds, and charges what a
/// read reserves at once.
#[derive(Default)]
struct Gate {
    open: Notify,
    asked: Mutex<Vec<u64>>,
}

impl Admission for Gate {
    fn admit<'a>(&'a self, event: &'a SourceEvent) -> BoxFuture<'a, crate::Result<Option<Permit>>> {
        Box::pin(async move {
            let bytes = match event {
                SourceEvent::Push(Push::Json(json)) => json.len(),
                SourceEvent::Checkpoint { cursor, .. } => cursor.bytes().len(),
                _ => return Ok(None),
            };
            let bytes = u64::try_from(bytes).unwrap();
            self.asked.lock().unwrap().push(bytes);
            self.open.notified().await;
            Ok(Some(Box::new(bytes) as Permit))
        })
    }

    fn charge(&self, bytes: u64) -> Permit {
        self.asked.lock().unwrap().push(bytes);
        Box::new(bytes)
    }
}

#[tokio::test(start_paused = true)]
async fn an_admitted_push_waits_for_admission_and_carries_its_permit() {
    let gate = Arc::new(Gate::default());
    let (mut sink, mut feed) =
        admitted_partition_channel(NonZeroUsize::new(4).unwrap(), gate.clone());
    let sending = tokio::spawn(async move { sink.send(json("[1]")).await });
    tokio::task::yield_now().await;
    assert_eq!(
        *gate.asked.lock().unwrap(),
        [3],
        "the push asks for its bytes"
    );
    assert!(
        !sending.is_finished(),
        "the push waits until it is admitted"
    );
    gate.open.notify_one();
    sending.await.unwrap().unwrap();
    let (event, permit) = feed.recv_admitted().await.unwrap();
    assert_eq!(event, json("[1]"));
    assert_eq!(
        permit
            .and_then(|permit| permit.downcast::<u64>().ok())
            .map(|bytes| *bytes),
        Some(3)
    );
}

#[tokio::test(start_paused = true)]
async fn a_checkpoint_waits_for_admission_and_carries_its_permit() {
    let gate = Arc::new(Gate::default());
    let (mut sink, mut feed) =
        admitted_partition_channel(NonZeroUsize::new(4).unwrap(), gate.clone());
    let cursor = Cursor::new(1, b"12345").unwrap();
    let checkpoint = SourceEvent::Checkpoint {
        cursor,
        answers: None,
    };
    let sent = checkpoint.clone();
    let sending = tokio::spawn(async move { sink.send(sent).await });
    tokio::task::yield_now().await;
    assert_eq!(*gate.asked.lock().unwrap(), [5]);
    assert!(!sending.is_finished());
    gate.open.notify_one();
    sending.await.unwrap().unwrap();
    let (event, permit) = feed.recv_admitted().await.unwrap();
    assert_eq!(event, checkpoint);
    assert_eq!(
        permit
            .and_then(|permit| permit.downcast::<u64>().ok())
            .map(|bytes| *bytes),
        Some(5)
    );
}

#[tokio::test]
async fn an_event_its_admission_charges_nothing_enters_without_a_permit() {
    let gate = Arc::new(Gate::default());
    let (mut sink, mut feed) =
        admitted_partition_channel(NonZeroUsize::new(4).unwrap(), gate.clone());
    for event in [SourceEvent::Replan, SourceEvent::Behind { records: 7 }] {
        sink.send(event.clone()).await.unwrap();
        let (received, permit) = feed.recv_admitted().await.unwrap();
        assert_eq!(received, event);
        assert!(permit.is_none());
    }
    assert!(gate.asked.lock().unwrap().is_empty());
}

#[test]
fn a_sink_reserves_bytes_from_whoever_admits_its_events() {
    let gate = Arc::new(Gate::default());
    let (sink, _feed) = admitted_partition_channel(NonZeroUsize::new(4).unwrap(), gate.clone());
    let permit = sink.reserve(64).unwrap();
    assert_eq!(permit.downcast::<u64>().ok().map(|bytes| *bytes), Some(64));
    assert_eq!(*gate.asked.lock().unwrap(), [64]);
    let (plain, _feed) = partition_channel(NonZeroUsize::new(4).unwrap());
    assert!(plain.reserve(64).is_none());
}

#[tokio::test(start_paused = true)]
async fn a_stop_request_releases_a_send_waiting_for_admission() {
    let gate = Arc::new(Gate::default());
    let (mut sink, feed) = admitted_partition_channel(NonZeroUsize::new(4).unwrap(), gate);
    let waiting = tokio::spawn(async move { sink.send(json("[1]")).await });
    tokio::task::yield_now().await;
    feed.stop();
    assert_eq!(
        waiting.await.unwrap().unwrap_err().kind(),
        ConnectorErrorKind::Stopped
    );
}

#[tokio::test]
async fn a_plain_feed_drops_permits_as_it_receives() {
    let (mut sink, mut feed) = partition_channel(NonZeroUsize::new(4).unwrap());
    sink.send(json("[1]")).await.unwrap();
    let (event, permit) = feed.recv_admitted().await.unwrap();
    assert_eq!(event, json("[1]"));
    assert!(
        permit.is_none(),
        "a channel without admission carries no permits"
    );
}

#[test]
fn a_sink_debugs_its_answered_barrier_and_whether_it_admits() {
    let (plain, _feed) = partition_channel(NonZeroUsize::new(4).unwrap());
    assert_eq!(
        format!("{plain:?}"),
        "PartitionSink { answered: 0, admitted: false, .. }"
    );
    let (admitted, _feed) =
        admitted_partition_channel(NonZeroUsize::new(4).unwrap(), Arc::new(Gate::default()));
    assert_eq!(
        format!("{admitted:?}"),
        "PartitionSink { answered: 0, admitted: true, .. }"
    );
}

#[tokio::test]
async fn a_forwarded_read_learns_of_each_newer_barrier_once() {
    let (mut sink, feed) = partition_channel(NonZeroUsize::new(4).expect("not zero"));
    feed.request_checkpoint(3);
    assert_eq!(sink.requested(0).await, Requested::Checkpoint(3));
    // A barrier already forwarded is not asked for again; the next newer one is.
    let waiting = tokio::spawn(async move { sink.requested(3).await });
    tokio::task::yield_now().await;
    feed.request_checkpoint(5);
    assert_eq!(
        waiting.await.expect("the wait completes"),
        Requested::Checkpoint(5)
    );
}

#[tokio::test]
async fn a_forwarded_read_does_not_ask_for_a_barrier_its_checkpoint_answered() {
    let (mut sink, mut feed) = partition_channel(NonZeroUsize::new(4).expect("not zero"));
    feed.request_checkpoint(2);
    let cursor = Cursor::new(1, b"c").expect("a small cursor");
    sink.send(SourceEvent::Checkpoint {
        cursor,
        answers: Some(2),
    })
    .await
    .expect("the send goes");
    feed.recv().await.expect("the checkpoint arrives");
    feed.stop();
    assert_eq!(sink.requested(0).await, Requested::Stop);
}

#[tokio::test]
async fn a_checkpoint_answering_a_barrier_never_requested_is_refused() {
    let (mut sink, mut feed) = partition_channel(NonZeroUsize::new(4).expect("not zero"));
    feed.request_checkpoint(2);
    let cursor = || Cursor::new(1, b"c").expect("a small cursor");
    let refused = sink
        .send(SourceEvent::Checkpoint {
            cursor: cursor(),
            answers: Some(u64::MAX),
        })
        .await
        .expect_err("no such barrier was requested");
    assert_eq!(refused.kind(), ConnectorErrorKind::Internal);
    assert_eq!(refused.code(), Some("barrier_unrequested"));
    // An answer to an older barrier after a newer one leaves the newer answered.
    for answers in [2, 1] {
        sink.send(SourceEvent::Checkpoint {
            cursor: cursor(),
            answers: Some(answers),
        })
        .await
        .expect("the send goes");
        feed.recv().await.expect("the checkpoint arrives");
    }
    assert_eq!(sink.pending_barrier(), None);
}
