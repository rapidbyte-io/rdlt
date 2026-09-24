//! One partition's pipeline: read, fit each batch to its table, prepare it, and hand it to a lane.

#[cfg(test)]
mod tests;

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

use arrow_array::RecordBatch;
use rdlt_connector::{
    Cursor, LoadId, Partition, PartitionFeed, PartitionState, Permit, Push, ReadRequest, SegmentId,
    Source, SourceEvent, StreamName, TableSchema, admitted_partition_channel,
};
use tokio::sync::{Semaphore, mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::budget::MemoryBudget;
use crate::error::{Error, Side};
use crate::lane::{Lanes, Write};
use crate::table::{Stamp, Tables, prepare};

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
/// Rows written after the last checkpoint have no cursor that resumes past them, so committing
/// them marks the partition `Done`. Otherwise the partition resumes from its last cursor, so an
/// incremental read picks up rows the source adds later. A partition that read nothing and never
/// checkpointed records no position, so its next read starts from the beginning again.
fn end_state(ingested: &Ingested) -> Option<PartitionState> {
    match (&ingested.last_cursor, ingested.open.rows) {
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
    };
    loop {
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
            event = feed.recv_admitted() => event,
        };
        let Some((event, permit)) = event else {
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
        match event {
            SourceEvent::Push(Push::Arrow(batch)) => {
                write(job, context, &mut self.open, &batch, permit).await
            }
            SourceEvent::Push(Push::Json(_) | Push::Changes(_)) => Err(Error::new(
                crate::ErrorKind::Source,
                format!(
                    "stream {} pushed JSON or changes, which the engine does not load yet",
                    job.stream
                ),
            )
            .with_code("push_unsupported")
            .with_stream(&job.stream)),
            SourceEvent::Checkpoint { cursor, answers } => {
                let next = OpenSegment::new(context.next_segment());
                let sealed = std::mem::replace(&mut self.open, next);
                let state = PartitionState::Cursor(cursor.clone());
                self.last_cursor = Some(cursor);
                context.report(Progress::Sealed(sealed.seal(job.index, state, answers)))
            }
            SourceEvent::Log { .. } | SourceEvent::Metric { .. } => Ok(()),
        }
    }
}

/// Fits `batch` to its table, prepares it, and queues it on its lane.
///
/// The permit holding the push's bytes travels with the batch, reserved here when the push
/// arrived without one. Growth from preparing is charged at once; later pushes pay it back by
/// waiting.
async fn write(
    job: &PartitionJob,
    context: &PartitionContext,
    open: &mut OpenSegment,
    batch: &RecordBatch,
    permit: Option<Permit>,
) -> Result<(), Error> {
    let received = u64::try_from(batch.num_rows()).unwrap_or(u64::MAX);
    if received == 0 {
        return Ok(());
    }
    let incoming = TableSchema::from_arrow(&batch.schema()).map_err(|error| {
        Error::schema(format!(
            "stream {}: a batch has no table schema: {error}",
            job.stream
        ))
        .with_code("batch_schema_invalid")
        .with_stream(&job.stream)
    })?;
    let (view, routes) = context.tables.fit(job.table, &incoming).await?;
    let stamp = Stamp {
        load_id: context.load_id,
        loaded_at: context.loaded_at,
        segment: open.id,
        first_row: open.received,
    };
    let prepared = prepare(&job.stream, &view, &incoming, batch, &routes, &stamp)?;
    open.received += received;
    open.discarded_rows += prepared.discarded_rows;
    open.discarded_values += prepared.discarded_values;
    let rows = u64::try_from(prepared.batch.num_rows()).unwrap_or(u64::MAX);
    if rows == 0 {
        return Ok(());
    }
    let pushed = u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX);
    let bytes = u64::try_from(prepared.batch.get_array_memory_size()).unwrap_or(u64::MAX);
    let permit = match permit {
        Some(permit) => permit,
        None => Box::new(context.budget.acquire(pushed).await),
    };
    let growth = context.budget.charge(bytes.saturating_sub(pushed));
    let lane = context.lanes.route(job.table, job.partition.id());
    context
        .lanes
        .write(
            lane,
            Write {
                table: job.table,
                segment: open.id,
                batch: prepared.batch,
                reservation: Box::new((permit, growth)),
            },
        )
        .await?;
    open.rows += rows;
    open.bytes += bytes;
    context.report(Progress::Written { rows, bytes })
}
