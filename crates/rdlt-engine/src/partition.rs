//! One partition's pipeline: read, coalesce pushes, shred JSON, fit each batch to its table,
//! prepare it, and hand it to a lane.

mod coalesce;
#[cfg(test)]
mod tests;
mod write;

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

use rdlt_connector::{
    Cursor, LoadId, Partition, PartitionFeed, PartitionState, Permit, Push, ReadRequest, SegmentId,
    Source, SourceEvent, StreamName, admitted_partition_channel,
};
use tokio::sync::{Semaphore, mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::budget::MemoryBudget;
use crate::config::BatchPolicy;
use crate::env::Env;
use crate::error::{Error, ErrorKind, Side};
use crate::lane::Lanes;
use crate::table::Tables;

use coalesce::{Coalescer, Pushed};
use write::write_flushed;

/// What a partition tells the commit coordinator.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Progress {
    /// The partition started reading.
    Started {
        /// The partition's index in the attempt.
        partition: usize,
    },
    /// Rows were queued for staging.
    Written {
        /// Rows queued.
        rows: u64,
        /// Their bytes in memory.
        bytes: u64,
    },
    /// A segment was sealed.
    Sealed(Seal),
    /// The partition stopped reading; a partition that was not stopped sealed its end first.
    Ended {
        /// The partition's index in the attempt.
        partition: usize,
        /// Whether the read ended because the engine asked it to stop.
        stopped: bool,
    },
}

/// A sealed segment: every row written to it, and where to resume after it.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Seal {
    /// The partition's index in the attempt.
    pub(crate) partition: usize,
    /// The segment.
    pub(crate) segment: SegmentId,
    /// Rows written to the segment.
    pub(crate) rows: u64,
    /// Their bytes in memory.
    pub(crate) bytes: u64,
    /// Where the partition resumes once the segment is committed.
    pub(crate) state: PartitionState,
    /// The barrier this seal answers.
    pub(crate) answers: Option<u64>,
    /// Rows the schema policy dropped from the segment.
    pub(crate) discarded_rows: u64,
    /// Values the schema policy nulled in the segment.
    pub(crate) discarded_values: u64,
}

/// One partition to read.
#[derive(Clone, Debug)]
pub(crate) struct PartitionJob {
    /// The partition's index in the attempt.
    pub(crate) index: usize,
    /// The stream.
    pub(crate) stream: StreamName,
    /// The stream's table index.
    pub(crate) table: usize,
    /// The partition.
    pub(crate) partition: Partition,
    /// Where to resume.
    pub(crate) cursor: Option<Cursor>,
    /// Whether the partition checkpoints when asked, so barriers are forwarded to it.
    pub(crate) on_demand: bool,
}

/// Everything the partitions of an attempt share.
#[derive(Clone)]
pub(crate) struct PartitionContext {
    pub(crate) source: Arc<dyn Source>,
    pub(crate) lanes: Lanes,
    pub(crate) tables: Arc<Tables>,
    pub(crate) budget: MemoryBudget,
    pub(crate) progress: mpsc::UnboundedSender<Progress>,
    pub(crate) barrier: watch::Receiver<u64>,
    /// Fires when reads must stop; the partitions end without sealing their open segments.
    pub(crate) stop: CancellationToken,
    /// Fires when the attempt is cancelled.
    pub(crate) cancel: CancellationToken,
    /// Limits how many partitions read at once.
    pub(crate) slots: Arc<Semaphore>,
    /// The next segment id of the load.
    pub(crate) segments: Arc<AtomicU64>,
    pub(crate) buffer: NonZeroUsize,
    pub(crate) load_id: LoadId,
    /// When the attempt started, which every row it loads carries.
    pub(crate) loaded_at: SystemTime,
    /// The clock the coalescer's deadlines follow, and the pool JSON is shredded on.
    pub(crate) env: Arc<dyn Env>,
    /// How pushes are coalesced and JSON is shredded.
    pub(crate) batch: BatchPolicy,
}

impl PartitionContext {
    fn report(&self, progress: Progress) -> Result<(), Error> {
        self.progress
            .send(progress)
            .map_err(|_| Error::cancelled("the commit coordinator stopped"))
    }

    fn next_segment(&self) -> SegmentId {
        SegmentId(self.segments.fetch_add(1, Ordering::Relaxed))
    }
}

/// Reads `job` to its end, or until the attempt stops or is cancelled.
pub(crate) async fn run(job: PartitionJob, context: PartitionContext) -> Result<(), Error> {
    let _slot = tokio::select! {
        biased;
        // Cancellation wins: a partition that has not started never needs to.
        () = context.cancel.cancelled() => return Err(Error::cancelled("the attempt was cancelled")),
        // A stop request comes next: a partition still waiting for a slot ends without reading.
        () = context.stop.cancelled() => None,
        slot = context.slots.acquire() => Some(slot.map_err(|_| Error::internal("partition slots closed"))?),
    };
    if context.stop.is_cancelled() {
        return context.report(Progress::Ended {
            partition: job.index,
            stopped: true,
        });
    }
    context.report(Progress::Started {
        partition: job.index,
    })?;
    let ingested = read_and_ingest(&job, &context).await?;
    if !ingested.stopped
        && let Some(state) = end_state(&ingested)
    {
        context.report(Progress::Sealed(ingested.open.seal(job.index, state, None)))?;
    }
    context.report(Progress::Ended {
        partition: job.index,
        stopped: ingested.stopped,
    })
}

/// Reads `job` while ingesting what the read emits, until both end or the attempt is cancelled.
async fn read_and_ingest(
    job: &PartitionJob,
    context: &PartitionContext,
) -> Result<Ingested, Error> {
    // Each push reserves its bytes before it enters the channel, so a source buffers nothing
    // outside the budget (spec §7.5).
    let admission = Arc::new(context.budget.clone());
    let (sink, feed) = admitted_partition_channel(context.buffer, admission);
    let request = ReadRequest {
        stream: job.stream.clone(),
        partition: job.partition.clone(),
        cursor: job.cursor.clone(),
    };
    // An ingest failure ends the read rather than waiting for a source that may not emit again
    // for a long time. A read failure lets ingest drain what the source already sent.
    let ingest_failed = CancellationToken::new();
    let read = async {
        tokio::select! {
            biased;
            () = ingest_failed.cancelled() => Ok(()),
            read = context.source.read(request, sink) => read,
        }
    };
    let ingest = async {
        let ingested = ingest(job, context, feed).await;
        if ingested.is_err() {
            ingest_failed.cancel();
        }
        ingested
    };
    let both = async { tokio::join!(read, ingest) };
    let (read, ingested) = tokio::select! {
        biased;
        // Cancellation wins: dropping the read and ingest futures ends both.
        () = context.cancel.cancelled() => return Err(Error::cancelled("the attempt was cancelled")),
        both = both => both,
    };
    // An ingest failure ends the read, so it is the cause when both fail.
    let ingested = ingested?;
    read.map_err(|error| {
        Error::connector(
            Side::Source,
            format!("reading stream {}", job.stream),
            error,
        )
        .with_stream(&job.stream)
    })?;
    Ok(ingested)
}

/// Where a partition that read to its end resumes, if anywhere new.
///
/// Rows received after the last checkpoint have no cursor that resumes past them, so committing
/// them marks the partition `Done`, whether they were written or its policy discarded them all,
/// and the seal carries the discards. Otherwise the partition resumes from its last cursor, so an
/// incremental read picks up rows the source adds later. A partition that read nothing and never
/// checkpointed records no position, so its next read starts from the beginning again.
fn end_state(ingested: &Ingested) -> Option<PartitionState> {
    match (&ingested.last_cursor, ingested.open.received) {
        (Some(cursor), 0) => Some(PartitionState::Cursor(cursor.clone())),
        (None, 0) => None,
        _ => Some(PartitionState::Done),
    }
}

/// What an ingest loop leaves once the read has ended.
struct Ingested {
    open: OpenSegment,
    last_cursor: Option<Cursor>,
    stopped: bool,
    /// Pushes gathered and not yet written.
    coalescer: Coalescer,
}

/// The segment rows are written to until the next checkpoint seals it.
#[derive(Debug, Default)]
struct OpenSegment {
    id: SegmentId,
    /// Rows written.
    rows: u64,
    bytes: u64,
    /// Rows received, before discards and compaction, which number each row's sequence.
    received: u64,
    discarded_rows: u64,
    discarded_values: u64,
}

impl OpenSegment {
    fn new(id: SegmentId) -> Self {
        Self {
            id,
            ..Self::default()
        }
    }

    fn seal(self, partition: usize, state: PartitionState, answers: Option<u64>) -> Seal {
        Seal {
            partition,
            segment: self.id,
            rows: self.rows,
            bytes: self.bytes,
            state,
            answers,
            discarded_rows: self.discarded_rows,
            discarded_values: self.discarded_values,
        }
    }
}

/// The barriers the coordinator raises, forwarded to an on-demand partition's read.
struct Barriers {
    receiver: watch::Receiver<u64>,
    open: bool,
}

impl Barriers {
    /// Barriers for a partition, forwarding one already raised to `feed` at once.
    fn new(mut receiver: watch::Receiver<u64>, on_demand: bool, feed: &PartitionFeed) -> Self {
        if on_demand {
            let raised = *receiver.borrow_and_update();
            if raised > 0 {
                feed.request_checkpoint(raised);
            }
        }
        Self {
            receiver,
            open: on_demand,
        }
    }

    /// The next barrier raised; `None` once the coordinator has gone.
    async fn next(&mut self) -> Option<u64> {
        if self.receiver.changed().await.is_ok() {
            Some(*self.receiver.borrow_and_update())
        } else {
            self.open = false;
            None
        }
    }
}

async fn ingest(
    job: &PartitionJob,
    context: &PartitionContext,
    mut feed: PartitionFeed,
) -> Result<Ingested, Error> {
    let mut barriers = Barriers::new(context.barrier.clone(), job.on_demand, &feed);
    let mut ingested = Ingested {
        open: OpenSegment::new(context.next_segment()),
        last_cursor: job.cursor.clone(),
        stopped: false,
        coalescer: Coalescer::new(context.batch),
    };
    loop {
        let deadline = ingested.coalescer.deadline();
        let pending = deadline.is_some();
        let pressed = context.budget.pressed();
        let due = async {
            match deadline {
                Some(deadline) => {
                    let wait = deadline.saturating_duration_since(context.env.instant());
                    context.env.sleep(wait).await;
                }
                None => std::future::pending().await,
            }
        };
        let event = tokio::select! {
            biased;
            // A stop request goes to the read first, even while events keep arriving.
            () = context.stop.cancelled(), if !ingested.stopped => {
                feed.stop();
                ingested.stopped = true;
                continue;
            }
            raised = barriers.next(), if barriers.open => {
                if let Some(barrier) = raised {
                    feed.request_checkpoint(barrier);
                }
                continue;
            }
            // A request waiting for the budget may be waiting for the gathered pushes' bytes, so
            // they are written at once rather than after their latency.
            () = pressed, if pending => {
                ingested.flush(job, context).await?;
                continue;
            }
            // Pushes that waited long enough are written even while more keep arriving.
            () = due => {
                ingested.flush(job, context).await?;
                continue;
            }
            event = feed.recv_admitted() => event,
        };
        let Some((event, permit)) = event else {
            ingested.flush(job, context).await?;
            return Ok(ingested);
        };
        ingested.handle(job, context, event, permit).await?;
    }
}

impl Ingested {
    async fn handle(
        &mut self,
        job: &PartitionJob,
        context: &PartitionContext,
        event: SourceEvent,
        permit: Option<Permit>,
    ) -> Result<(), Error> {
        let pushed = match event {
            SourceEvent::Push(Push::Arrow(batch)) => Pushed::Arrow(batch),
            SourceEvent::Push(Push::Json(json)) => Pushed::Json(json),
            SourceEvent::Push(Push::Changes(_)) => {
                return Err(Error::new(
                    ErrorKind::Source,
                    format!(
                        "stream {} pushed changes, which the engine does not load yet",
                        job.stream
                    ),
                )
                .with_code("push_unsupported")
                .with_stream(&job.stream));
            }
            SourceEvent::Checkpoint { cursor, answers } => {
                // Coalescing never carries rows past a checkpoint, so segments are never split.
                self.flush(job, context).await?;
                let next = OpenSegment::new(context.next_segment());
                let sealed = std::mem::replace(&mut self.open, next);
                let state = PartitionState::Cursor(cursor.clone());
                self.last_cursor = Some(cursor);
                return context.report(Progress::Sealed(sealed.seal(job.index, state, answers)));
            }
            SourceEvent::Log { .. } | SourceEvent::Metric { .. } => return Ok(()),
        };
        // Every push on an admitted channel carries the permit that reserved its bytes.
        let permit = permit.ok_or_else(|| Error::internal("a push arrived without its permit"))?;
        for flushed in self.coalescer.add(pushed, permit, context.env.instant()) {
            write_flushed(job, context, &mut self.open, flushed).await?;
        }
        Ok(())
    }

    /// Writes every push gathered.
    async fn flush(&mut self, job: &PartitionJob, context: &PartitionContext) -> Result<(), Error> {
        match self.coalescer.flush() {
            Some(flushed) => write_flushed(job, context, &mut self.open, flushed).await,
            None => Ok(()),
        }
    }
}
