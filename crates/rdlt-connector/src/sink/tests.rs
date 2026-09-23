use std::num::NonZeroUsize;

use bytes::Bytes;

use super::{Push, SourceEvent, partition_channel};
use crate::cursor::Cursor;
use crate::error::ConnectorErrorKind;

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
