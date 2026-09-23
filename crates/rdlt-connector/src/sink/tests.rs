use std::num::NonZeroUsize;

use bytes::Bytes;

use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use super::{Admission, Permit, Push, SourceEvent, admitted_partition_channel, partition_channel};
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

/// Admits pushes once opened, recording the bytes each asked for.
#[derive(Default)]
struct Gate {
    open: Notify,
    asked: Mutex<Vec<u64>>,
}

impl Admission for Gate {
    fn admit(&self, bytes: u64) -> BoxFuture<'_, Permit> {
        Box::pin(async move {
            self.asked.lock().unwrap().push(bytes);
            self.open.notified().await;
            Box::new(bytes) as Permit
        })
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

#[tokio::test]
async fn events_other_than_pushes_need_no_admission() {
    let gate = Arc::new(Gate::default());
    let (mut sink, mut feed) =
        admitted_partition_channel(NonZeroUsize::new(4).unwrap(), gate.clone());
    let checkpoint = SourceEvent::Checkpoint {
        cursor: Cursor::encode(1, &1u32).unwrap(),
        answers: None,
    };
    sink.send(checkpoint.clone()).await.unwrap();
    let (event, permit) = feed.recv_admitted().await.unwrap();
    assert_eq!(event, checkpoint);
    assert!(permit.is_none());
    assert!(gate.asked.lock().unwrap().is_empty());
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
