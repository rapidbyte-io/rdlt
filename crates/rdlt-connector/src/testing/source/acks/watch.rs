//! Reading a partition for `S-ACK` while asking, at each checkpoint, where every partition stands.

use std::num::NonZeroUsize;
use std::time::Duration;

use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::Probed;
use crate::catalog::Checkpointing;
use crate::cursor::Cursor;
use crate::sink::{PartitionFeed, SourceEvent, partition_channel};
use crate::source::{Partition, ReadRequest};
use crate::state::PartitionState;
use crate::testing::source::STOP_WINDOW;
use crate::testing::{Violation, bounded};

/// How many checkpoints a read of an unbounded partition sends before it is asked to stop.
const CHECKPOINTS: usize = 3;

/// How long a read of an unbounded partition may go without a checkpoint before it is asked to
/// stop.
const QUIET: Duration = Duration::from_secs(5);

/// What a read sent.
#[derive(Default)]
pub(super) struct Read {
    /// Its checkpoints, in order.
    pub(super) checkpoints: Vec<Cursor>,
    /// Whether rows followed its last checkpoint.
    tail: bool,
}

impl Read {
    /// Where the engine records the end of this read, which started at `from`, as it does once a
    /// bounded partition reads to its end: rows after the last checkpoint leave it done, and a
    /// read that sent nothing leaves it where it started.
    ///
    /// The clause reads no phase after one with an unbounded partition, so what such a partition
    /// would record never matters.
    pub(super) fn end(&self, from: Option<&Cursor>) -> Option<PartitionState> {
        match (self.checkpoints.last(), self.tail) {
            (_, true) => Some(PartitionState::Done),
            (Some(last), false) => Some(PartitionState::Cursor(last.clone())),
            (None, false) => from.cloned().map(PartitionState::Cursor),
        }
    }
}

impl Probed<'_> {
    /// What a read of `partition` from `cursor` sent, asking at each checkpoint and once it ends
    /// whether every partition seen still stands where it was found.
    ///
    /// A read of an unbounded partition is asked to stop after a few checkpoints, or once it goes
    /// without one for a while; one asked to stop that is still quiet a moment later waits for
    /// data, and is dropped.
    pub(super) async fn watched(
        &self,
        partition: &Partition,
        cursor: Option<Cursor>,
    ) -> Result<Read, Violation> {
        // A one-event channel keeps the read at most an event ahead of the questions; the
        // question once it ends covers what it sent last.
        let (sink, feed) = partition_channel(NonZeroUsize::MIN);
        let request = ReadRequest {
            stream: self.stream.name().clone(),
            partition: partition.clone(),
            cursor,
        };
        let stopped = CancellationToken::new();
        let read = async {
            tokio::select! {
                biased;
                read = self.source.read(request, sink) => Some(read),
                () = async {
                    stopped.cancelled().await;
                    tokio::time::sleep(STOP_WINDOW).await;
                } => None,
            }
        };
        let watch = async {
            let watched = self.watch(feed, partition).await;
            stopped.cancel();
            watched
        };
        let what = format!(
            "reading {} partition {}",
            self.stream.name(),
            partition.id()
        );
        let (ended, watched) = bounded(&what, async { tokio::join!(read, watch) }).await?;
        let watched = watched?;
        if let Some(ended) = ended {
            ended.map_err(|error| Violation::from(format!("{what}: {error}")))?;
        }
        self.unmoved().await?;
        Ok(watched)
    }

    /// What `feed` sends, asking at each checkpoint whether every partition seen still stands
    /// where it was found; however the watch ends, the read is asked to stop.
    ///
    /// A stream that checkpoints on demand is asked for a checkpoint as the read starts and after
    /// each it sends.
    async fn watch(
        &self,
        mut feed: PartitionFeed,
        partition: &Partition,
    ) -> Result<Read, Violation> {
        let unbounded = partition.is_unbounded();
        let on_demand = self.stream.checkpointing() == Checkpointing::OnDemand;
        let mut barrier = 0;
        let mut ask = |feed: &PartitionFeed| {
            if on_demand {
                barrier += 1;
                feed.request_checkpoint(barrier);
            }
        };
        ask(&feed);
        let mut read = Read::default();
        let mut quiet = Instant::now() + QUIET;
        let watched = loop {
            if unbounded && read.checkpoints.len() >= CHECKPOINTS {
                break Ok(());
            }
            let event = if unbounded {
                match tokio::time::timeout_at(quiet, feed.recv()).await {
                    Ok(event) => event,
                    Err(_) => break Ok(()),
                }
            } else {
                feed.recv().await
            };
            match event {
                None => break Ok(()),
                Some(SourceEvent::Checkpoint { cursor, .. }) => {
                    read.checkpoints.push(cursor);
                    read.tail = false;
                    quiet = Instant::now() + QUIET;
                    if let Err(violation) = self.unmoved().await {
                        break Err(violation);
                    }
                    ask(&feed);
                }
                Some(SourceEvent::Push(_)) => read.tail = true,
                Some(SourceEvent::Log { .. } | SourceEvent::Metric { .. }) => {}
            }
        };
        // A stop request wins over any send, so the read's next event ends it.
        feed.stop();
        watched.map(|()| read)
    }

    /// A violation unless every partition seen stands where the clause last found it.
    pub(super) async fn unmoved(&self) -> Result<(), Violation> {
        for (partition, stood) in &self.seen {
            if self.position(partition).await? != *stood {
                return Err(
                    self.violation(partition, "a read moved where it stands, nothing committed")
                );
            }
        }
        Ok(())
    }
}
