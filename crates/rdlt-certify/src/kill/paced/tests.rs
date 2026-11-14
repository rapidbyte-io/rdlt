use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use rdlt_connector::{
    BoxFuture, Catalog, ConnectorError, ConnectorErrorKind, Cursor, Partition, PartitionFeed,
    PartitionId, PartitionPlan, PartitionSink, Push, ReadRequest, Result, Source, SourceEvent,
    StreamName, StreamState, partition_channel,
};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::{Pace, Paced, WINDOW};

/// The rows of each push the scripted source sends.
const BATCH: u64 = 8;

/// The pushes a read sends in a window.
const PER_WINDOW: u64 = WINDOW / BATCH;

/// How long the engine's end waits for an event before it takes the read to be holding.
const QUIET: Duration = Duration::from_secs(1);

/// A source each read of which sends `pushes` pushes of [`BATCH`] rows, checkpointing as
/// `checkpoints` says; it carries `calls` at once.
struct Scripted {
    pushes: u64,
    checkpoints: Checkpoints,
    calls: Option<NonZeroUsize>,
}

/// Where a scripted read checkpoints.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Checkpoints {
    /// After each push, answering the barrier pending.
    Answering,
    /// After each push, answering no barrier.
    Deaf,
    /// After the last push alone.
    Last,
}

/// A source sending `pushes` pushes, checkpointing as `checkpoints` says.
fn scripted(pushes: u64, checkpoints: Checkpoints) -> Scripted {
    Scripted {
        pushes,
        checkpoints,
        calls: None,
    }
}

impl Source for Scripted {
    fn check(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn discover(&self) -> BoxFuture<'_, Result<Catalog>> {
        Box::pin(async { Ok(Catalog::default()) })
    }

    fn plan<'a>(
        &'a self,
        _: &'a StreamName,
        _: &'a StreamState,
    ) -> BoxFuture<'a, Result<PartitionPlan>> {
        Box::pin(async { Err(ConnectorError::internal("the test plans nothing")) })
    }

    fn read(&self, _: ReadRequest, mut sink: PartitionSink) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            for push in 0..self.pushes {
                let ids: ArrayRef = Arc::new(Int64Array::from(vec![0; 8]));
                let batch = RecordBatch::try_from_iter([("id", ids)]).expect("a valid batch");
                sink.send(SourceEvent::Push(Push::Arrow(batch))).await?;
                if self.checkpoints != Checkpoints::Last || push + 1 == self.pushes {
                    let answers = match self.checkpoints {
                        Checkpoints::Deaf => None,
                        Checkpoints::Answering | Checkpoints::Last => sink.pending_barrier(),
                    };
                    let cursor = cursor(push);
                    sink.send(SourceEvent::Checkpoint { cursor, answers })
                        .await?;
                }
            }
            Ok(())
        })
    }

    fn committed<'a>(
        &'a self,
        _: &'a StreamName,
        _: &'a [(PartitionId, Cursor)],
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn max_calls(&self) -> Option<NonZeroUsize> {
        self.calls
    }
}

/// The cursor the scripted source checkpoints at after its `push`th push.
fn cursor(push: u64) -> Cursor {
    Cursor::new(1, &push.to_be_bytes()).expect("a small cursor")
}

/// A read of a paced [`Scripted`] source, from the engine's end.
struct Read {
    feed: PartitionFeed,
    pace: Arc<watch::Sender<Pace>>,
    reading: JoinHandle<Result<()>>,
}

/// What reaches the engine's end next.
#[derive(Debug, PartialEq)]
enum Next {
    Event(SourceEvent),
    /// The read sent everything.
    Ended,
    /// Nothing arrived within [`QUIET`].
    Quiet,
}

/// Starts a read of `source`, paced by a pace `paced` sets first.
fn read(source: Scripted, paced: impl FnOnce(&mut Pace)) -> Read {
    let mut start = Pace::default();
    paced(&mut start);
    let pace = Arc::new(watch::Sender::new(start));
    let source = Paced::new(Arc::new(source), Arc::clone(&pace));
    let (sink, feed) = partition_channel(NonZeroUsize::MIN);
    let stream = StreamName::new("rows").expect("a valid name");
    let partition = Partition::new(PartitionId::parse("0").expect("a valid id"));
    let request = ReadRequest::new(stream, partition, None);
    let reading = tokio::spawn(async move { source.read(request, sink).await });
    Read {
        feed,
        pace,
        reading,
    }
}

impl Read {
    async fn next(&mut self) -> Next {
        match tokio::time::timeout(QUIET, self.feed.recv()).await {
            Ok(Some(event)) => Next::Event(event),
            Ok(None) => Next::Ended,
            Err(_) => Next::Quiet,
        }
    }

    /// The pushes and checkpoints that arrive until nothing more does, and how that ends.
    async fn drained(&mut self) -> (u64, u64, Next) {
        let (mut pushes, mut checkpoints) = (0, 0);
        loop {
            match self.next().await {
                Next::Event(SourceEvent::Push(_)) => pushes += 1,
                Next::Event(SourceEvent::Checkpoint { .. }) => checkpoints += 1,
                Next::Event(event) => panic!("the source sends no {event:?}"),
                end => return (pushes, checkpoints, end),
            }
        }
    }

    fn pace(&self) -> Pace {
        *self.pace.borrow()
    }

    /// Starts the `commit`th commit of a load whose last point counts `last`.
    fn start(&self, commit: u64, last: u64) {
        self.pace.send_modify(|pace| pace.start(commit, last));
    }
}

#[tokio::test(start_paused = true)]
async fn a_read_holds_its_push_once_it_forwarded_a_window_after_a_checkpoint() {
    let mut read = read(scripted(3 * PER_WINDOW, Checkpoints::Answering), |_| {});
    assert_eq!(read.drained().await, (PER_WINDOW, PER_WINDOW, Next::Quiet));
    let pace = read.pace();
    assert_eq!((pace.relaying, pace.holding), (1, 1));
    assert!(pace.stalled(), "{pace:?}");
}

#[tokio::test(start_paused = true)]
async fn a_commit_starting_opens_another_window() {
    let mut read = read(scripted(3 * PER_WINDOW, Checkpoints::Answering), |_| {});
    assert_eq!(read.drained().await.0, PER_WINDOW);
    read.start(1, 5);
    assert!(!read.pace().stalled(), "a start releases the held push");
    assert_eq!(read.drained().await, (PER_WINDOW, PER_WINDOW, Next::Quiet));
    assert!(read.pace().stalled(), "{:?}", read.pace());
    read.start(2, 5);
    assert_eq!(read.drained().await, (PER_WINDOW, PER_WINDOW, Next::Ended));
    let pace = read.pace();
    assert_eq!((pace.relaying, pace.holding), (0, 0));
    tokio::time::timeout(QUIET, read.reading)
        .await
        .expect("an ended read returns")
        .expect("the read ran")
        .expect("the read ended");
}

#[tokio::test(start_paused = true)]
async fn a_holding_read_answers_a_barrier_at_its_last_checkpoint() {
    let mut read = read(scripted(2 * PER_WINDOW, Checkpoints::Answering), |_| {});
    assert_eq!(read.drained().await.2, Next::Quiet);
    read.feed.request_checkpoint(1);
    let answer = SourceEvent::Checkpoint {
        cursor: cursor(PER_WINDOW - 1),
        answers: Some(1),
    };
    assert_eq!(read.next().await, Next::Event(answer));
    assert_eq!(read.next().await, Next::Quiet, "the held push stays held");
}

#[tokio::test(start_paused = true)]
async fn a_read_that_holds_answers_at_once_a_barrier_its_source_left_pending() {
    let mut read = read(scripted(2 * PER_WINDOW, Checkpoints::Deaf), |_| {});
    read.feed.request_checkpoint(1);
    for _ in 0..PER_WINDOW {
        assert!(matches!(
            read.next().await,
            Next::Event(SourceEvent::Push(_))
        ));
        let unanswered = matches!(
            read.next().await,
            Next::Event(SourceEvent::Checkpoint { answers: None, .. })
        );
        assert!(unanswered, "the source answers no barrier");
    }
    let answer = SourceEvent::Checkpoint {
        cursor: cursor(PER_WINDOW - 1),
        answers: Some(1),
    };
    assert_eq!(read.next().await, Next::Event(answer));
    assert_eq!(read.next().await, Next::Quiet, "the held push stays held");
}

#[tokio::test(start_paused = true)]
async fn a_read_not_holding_passes_a_barrier_on_to_its_source() {
    let mut read = read(scripted(2, Checkpoints::Answering), |_| {});
    read.feed.request_checkpoint(1);
    let mut answers = Vec::new();
    loop {
        match read.next().await {
            Next::Event(SourceEvent::Checkpoint {
                answers: answer, ..
            }) => answers.push(answer),
            Next::Event(_) => {}
            end => {
                assert_eq!(end, Next::Ended);
                break;
            }
        }
    }
    // The source's two checkpoints, one of them answering the barrier, and none of the read's.
    assert_eq!(answers.len(), 2, "{answers:?}");
    assert!(answers.contains(&Some(1)), "{answers:?}");
}

#[tokio::test(start_paused = true)]
async fn a_stop_ends_a_holding_read() {
    let mut read = read(scripted(3 * PER_WINDOW, Checkpoints::Answering), |_| {});
    assert_eq!(read.drained().await.2, Next::Quiet);
    read.feed.stop();
    let ended = tokio::time::timeout(QUIET, read.reading)
        .await
        .expect("a stopped read ends")
        .expect("the read ran");
    let error = ended.expect_err("the source's read was stopped");
    assert_eq!(error.kind(), ConnectorErrorKind::Stopped);
    let pace = *read.pace.borrow();
    assert_eq!((pace.relaying, pace.holding), (0, 0));
}

#[tokio::test(start_paused = true)]
async fn a_holding_read_dropped_is_counted_gone() {
    let mut read = read(scripted(3 * PER_WINDOW, Checkpoints::Answering), |_| {});
    assert_eq!(read.drained().await.2, Next::Quiet);
    let pace = read.pace();
    assert_eq!((pace.relaying, pace.holding), (1, 1));
    read.reading.abort();
    let dropped = tokio::time::timeout(QUIET, read.reading)
        .await
        .expect("an aborted read returns");
    assert!(dropped.is_err_and(|error| error.is_cancelled()));
    let pace = *read.pace.borrow();
    assert_eq!((pace.relaying, pace.holding), (0, 0));
}

#[tokio::test(start_paused = true)]
async fn nothing_is_held_once_the_last_point_has_started() {
    let mut read = read(scripted(3 * PER_WINDOW, Checkpoints::Answering), |pace| {
        pace.start(5, 5);
    });
    let all = 3 * PER_WINDOW;
    assert_eq!(read.drained().await, (all, all, Next::Ended));
}

#[tokio::test(start_paused = true)]
async fn a_push_that_follows_no_checkpoint_is_never_held() {
    let mut read = read(scripted(3 * PER_WINDOW, Checkpoints::Last), |_| {});
    assert_eq!(read.drained().await, (3 * PER_WINDOW, 1, Next::Ended));
}

#[test]
fn a_paced_source_carries_the_calls_its_source_carries() {
    let pace = Arc::new(watch::Sender::new(Pace::default()));
    for calls in [None, NonZeroUsize::new(7)] {
        let source = Scripted {
            calls,
            ..scripted(1, Checkpoints::Answering)
        };
        let paced = Paced::new(Arc::new(source), Arc::clone(&pace));
        assert_eq!(paced.max_calls(), calls);
    }
}
