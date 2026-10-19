//! A partition sink's sends and requests, and an emitter's checkpoints over one, cut at every
//! point they wait and then made again.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use rdlt_testkit::cancel::{self, Ended};
use tokio::sync::mpsc;

use super::{answering, json};
use crate::Emitter;
use crate::cursor::Cursor;
use crate::sink::{
    Admission, PartitionFeed, PartitionSink, Permit, Requested, SourceEvent,
    admitted_partition_channel, partition_channel,
};
use crate::spec::BoxFuture;

/// How long each admission takes, and the engine before it asks for a barrier.
const PACE: Duration = Duration::from_millis(10);

/// How long the engine takes before it receives each event.
const SLOW: Duration = Duration::from_millis(30);

/// Bounds every wait of a check.
const WAIT: Duration = Duration::from_secs(60);

/// Admits every event `PACE` after it asks, holding nothing.
struct Paced;

impl Admission for Paced {
    fn admit(&self, _: &SourceEvent) -> BoxFuture<'_, crate::Result<Option<Permit>>> {
        Box::pin(async {
            tokio::time::sleep(PACE).await;
            Ok(None)
        })
    }

    fn charge(&self, bytes: u64) -> crate::Result<Permit> {
        Ok(Box::new(bytes))
    }
}

/// Receives `feed`'s events as the engine does, each `SLOW` after the last, handing them on.
fn engine(mut feed: PartitionFeed) -> mpsc::UnboundedReceiver<SourceEvent> {
    let (engine, received) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(SLOW).await;
            let Some(event) = feed.recv().await else {
                return;
            };
            if engine.send(event).is_err() {
                return;
            }
        }
    });
    received
}

/// The next `count` events the engine received, each within `WAIT`, and then no more.
async fn received(
    events: &mut mpsc::UnboundedReceiver<SourceEvent>,
    count: usize,
) -> Vec<SourceEvent> {
    let mut received = Vec::with_capacity(count);
    for _ in 0..count {
        let event = tokio::time::timeout(WAIT, events.recv())
            .await
            .expect("an event arrives")
            .expect("the read goes on");
        received.push(event);
    }
    tokio::time::sleep(WAIT).await;
    assert!(
        events.try_recv().is_err(),
        "no event arrives twice: {received:?}"
    );
    received
}

/// A sink whose one-event channel holds a push the engine takes `SLOW` from now, each of whose
/// events is admitted `PACE` after it asks, and whose engine asked for barrier 1.
struct Sending {
    sink: PartitionSink,
    events: mpsc::UnboundedReceiver<SourceEvent>,
}

async fn sending() -> Sending {
    let (mut sink, feed) = admitted_partition_channel(NonZeroUsize::MIN, Arc::new(Paced));
    feed.request_checkpoint(1);
    sink.send(json("[1]")).await.expect("the push goes");
    Sending {
        sink,
        events: engine(feed),
    }
}

#[test]
fn a_checkpoint_cut_wherever_it_waits_leaves_its_barrier_pending_and_goes_once_retried() {
    let points = cancel::at_every_point(
        async || sending().await,
        async |sending: &mut Sending| sending.sink.send(answering(1)).await,
        async |sending: &mut Sending, ended: Ended<crate::Result<()>>| {
            match ended {
                Ended::Returned(sent) => sent.expect("the checkpoint goes"),
                Ended::Cancelled { at } => {
                    assert_eq!(
                        sending.sink.pending_barrier(),
                        Some(1),
                        "a checkpoint cut at wait {at} answers no barrier"
                    );
                    tokio::time::timeout(WAIT, sending.sink.send(answering(1)))
                        .await
                        .expect("the retry ends")
                        .expect("the retry goes");
                }
            }
            assert_eq!(sending.sink.pending_barrier(), None);
            let events = received(&mut sending.events, 2).await;
            assert_eq!(events, [json("[1]"), answering(1)]);
        },
    );
    // The admission, then room in the channel.
    assert_eq!(points, 2);
}

/// An emitter whose one-event channel holds a push the engine takes `SLOW` from now, and whose
/// engine asked for barrier 1.
struct Emitting {
    out: Emitter<u64>,
    events: mpsc::UnboundedReceiver<SourceEvent>,
}

async fn emitting() -> Emitting {
    let (sink, feed) = partition_channel(NonZeroUsize::MIN);
    feed.request_checkpoint(1);
    let mut out = Emitter::new(sink, 7, false);
    out.json(Bytes::from_static(b"[1]"))
        .await
        .expect("the push goes");
    Emitting {
        out,
        events: engine(feed),
    }
}

#[test]
fn an_emitter_s_checkpoint_cut_wherever_it_waits_answers_its_barrier_when_retried() {
    let points = cancel::at_every_point(
        async || emitting().await,
        async |emitting: &mut Emitting| emitting.out.checkpoint(&6).await,
        async |emitting: &mut Emitting, ended: Ended<crate::Result<()>>| {
            match ended {
                Ended::Returned(sent) => sent.expect("the checkpoint goes"),
                Ended::Cancelled { at } => {
                    assert!(
                        emitting.out.checkpoint_due(),
                        "a checkpoint cut at wait {at} leaves its barrier due"
                    );
                    tokio::time::timeout(WAIT, emitting.out.checkpoint(&6))
                        .await
                        .expect("the retry ends")
                        .expect("the retry goes");
                }
            }
            assert!(!emitting.out.checkpoint_due());
            let checkpoint = SourceEvent::Checkpoint {
                cursor: Cursor::encode(7, &6_u64).expect("a small cursor"),
                answers: Some(1),
            };
            let events = received(&mut emitting.events, 2).await;
            assert_eq!(events, [json("[1]"), checkpoint]);
        },
    );
    // Room in the channel.
    assert_eq!(points, 1);
}

/// A sink whose engine asks for barrier 1 `PACE` from now, and keeps its end.
fn requesting() -> PartitionSink {
    let (sink, feed) = partition_channel(NonZeroUsize::MIN);
    tokio::spawn(async move {
        tokio::time::sleep(PACE).await;
        feed.request_checkpoint(1);
        std::future::pending::<()>().await;
    });
    sink
}

#[test]
fn a_request_cut_wherever_it_waits_is_seen_when_asked_for_again() {
    let points = cancel::at_every_point(
        async || requesting(),
        async |sink: &mut PartitionSink| sink.requested(0).await,
        async |sink: &mut PartitionSink, ended: Ended<Requested>| {
            let requested = match ended {
                Ended::Returned(requested) => requested,
                Ended::Cancelled { at } => {
                    assert_eq!(
                        sink.pending_barrier(),
                        None,
                        "a request cut at wait {at} answers and asks for no barrier"
                    );
                    tokio::time::timeout(WAIT, sink.requested(0))
                        .await
                        .expect("the barrier is seen again")
                }
            };
            assert_eq!(requested, Requested::Checkpoint(1));
            assert_eq!(sink.pending_barrier(), Some(1));
        },
    );
    // The wait for the engine's barrier.
    assert_eq!(points, 1);
}
