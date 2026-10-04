//! One partition's pipeline: read, coalesce pushes, shred JSON, fit each batch to its table,
//! prepare it, and hand it to a lane.

mod barriers;
mod clock;
mod coalesce;
mod latest;
mod progress;
mod retention;
mod slots;
#[cfg(test)]
mod tests;
mod write;

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use rdlt_connector::cost::Rendering;
use rdlt_connector::{
    Cursor, LoadId, Partition, PartitionFeed, PartitionState, Permit, Push, ReadRequest, SegmentId,
    Source, SourceEvent, StreamName, admitted_partition_channel,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::budget::MemoryBudget;
use crate::config::BatchPolicy;
use crate::cost::{Admitted, Charging};
use crate::env::Env;
use crate::error::{Error, ErrorKind, Side};
use crate::lane::Lanes;
use crate::table::Tables;
use crate::wal::LoadLog;
use crate::watch;

use barriers::Barriers;
pub(crate) use clock::LoadClock;
use coalesce::{Coalescer, Pushed};
pub(crate) use latest::Latest;
pub(crate) use progress::{CursorHold, Progress, Seal};
use retention::read_resetting;
pub(crate) use slots::Slots;
use write::write_flushed;

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
    /// How a change stream's pushes load; `None` for a stream not read as changes.
    pub(crate) changes: Option<ChangeMode>,
    /// Fires when this partition must stop reading: as every partition does when the attempt
    /// stops, or alone when a plan no longer names it.
    pub(crate) stop: CancellationToken,
    /// Whether a read of the unbounded partition follows it once caught up: in a following run,
    /// for a stream the run plans again as it reads; a full read ends at its head.
    pub(crate) follow: bool,
    /// Whether the partition reads again from its source's earliest where the source's retention
    /// dropped where it would resume, rather than failing.
    pub(crate) reset_retention: bool,
}

/// How a change stream's pushes load.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ChangeMode {
    /// Whether its table merges by key; otherwise it is a log of every change.
    pub(crate) merge: bool,
    pub(crate) deletes: crate::plan::DeleteMode,
    pub(crate) truncates: crate::plan::OnTruncate,
    /// Whether the destination keeps a column's value an update flags unchanged.
    pub(crate) partial_updates: bool,
}

/// Everything the partitions of an attempt share.
#[derive(Clone)]
pub(crate) struct PartitionContext {
    pub(crate) source: Arc<dyn Source>,
    pub(crate) lanes: Lanes,
    pub(crate) tables: Arc<Tables>,
    pub(crate) budget: MemoryBudget,
    /// How the destination renders values, which decides what a batch costs.
    pub(crate) rendering: Arc<Rendering>,
    pub(crate) progress: mpsc::UnboundedSender<Progress>,
    /// What each partition last said of which only the newest matters.
    pub(crate) latest: Arc<Latest>,
    pub(crate) barrier: watch::Receiver<u64>,
    /// Fires when reads must stop; the partitions end without sealing their open segments.
    pub(crate) stop: CancellationToken,
    /// Fires when the attempt is cancelled.
    pub(crate) cancel: CancellationToken,
    /// Limits how many partitions read at once.
    pub(crate) slots: Slots,
    /// The next segment id of the load.
    pub(crate) segments: Arc<AtomicU64>,
    pub(crate) buffer: NonZeroUsize,
    pub(crate) load_id: LoadId,
    /// When the attempt started, which every row it loads carries, and when each batch is
    /// received.
    pub(crate) clock: Arc<LoadClock>,
    /// The clock the coalescer's deadlines follow, and the pool JSON is shredded on.
    pub(crate) env: Arc<dyn Env>,
    /// How pushes are coalesced and JSON is shredded.
    pub(crate) batch: BatchPolicy,
    /// How long a read asked to stop may take to end.
    pub(crate) stop_wait: std::time::Duration,
    /// The load's write-ahead log, where it keeps one.
    pub(crate) wal: Option<LoadLog>,
}

impl PartitionContext {
    fn report(&self, progress: Progress) -> Result<(), Error> {
        self.progress
            .send(progress)
            .map_err(|_| Error::cancelled("the commit coordinator stopped"))
    }

    /// Tells the coordinator `partition` has a signal waiting, where `tell` says no message in
    /// its queue says so yet.
    fn signalled(&self, partition: usize, tell: bool) -> Result<(), Error> {
        if tell {
            self.report(Progress::Signalled { partition })
        } else {
            Ok(())
        }
    }

    fn next_segment(&self) -> SegmentId {
        SegmentId(self.segments.fetch_add(1, Ordering::Relaxed))
    }
}

/// Reads `job` to its end, or until the attempt stops or is cancelled.
pub(crate) async fn run(job: PartitionJob, context: Arc<PartitionContext>) -> Result<(), Error> {
    // Every read holds a slot, so no more reads keep bytes than the reads' share is divided
    // among. A followed unbounded read holds one for as long as the run; fewer of them than
    // slots run at once, so the other reads always have a slot to take in turn.
    let _endless = context.slots.endless(&job)?;
    let _slot = tokio::select! {
        biased;
        // Cancellation wins: a partition that has not started never needs to.
        () = context.cancel.cancelled() => return Err(Error::cancelled("the attempt was cancelled")),
        // A stop request comes next: a partition still waiting for a slot ends without reading.
        () = job.stop.cancelled() => None,
        // A read waits for a slot as long as a request waits for bytes.
        slot = context.slots.read(&context.budget, &job) => Some(slot?),
    };
    if job.stop.is_cancelled() {
        return context.report(Progress::Ended {
            partition: job.index,
            stopped: true,
        });
    }
    // The read's state is allocated once it starts: a partition waiting for a slot holds its
    // job alone.
    Box::pin(read(job, &context)).await
}

/// Reads `job`, which holds its slot, to its end, sealing or abandoning what it read last.
async fn read(mut job: PartitionJob, context: &PartitionContext) -> Result<(), Error> {
    context.report(Progress::Started {
        partition: job.index,
    })?;
    let ingested = read_resetting(&mut job, context).await?;
    let end = (!ingested.stopped)
        .then(|| end_state(&ingested, job.partition.is_unbounded()))
        .flatten();
    match end {
        Some(state) => {
            // A cursor the read ended at waits for its commit as a checkpoint's does.
            let held = CursorHold::reserve(&context.budget, &context.cancel, &state).await;
            let seal = ingested.open.seal(job.index, state, None, held?);
            if let Some(log) = &context.wal {
                log.checkpointed();
            }
            context.report(Progress::Sealed(seal))?;
        }
        None => abandon(job.index, &ingested.open, context).await?,
    }
    context.report(Progress::Ended {
        partition: job.index,
        stopped: ingested.stopped,
    })
}

/// Lets `open` go uncommitted: its rows no longer make a commit due, and the write-ahead log may
/// drop what it holds of it.
async fn abandon(
    partition: usize,
    open: &OpenSegment,
    context: &PartitionContext,
) -> Result<(), Error> {
    context.report(Progress::Abandoned {
        partition,
        segment: open.id,
        rows: open.rows,
        bytes: open.bytes,
    })?;
    if let Some(log) = &context.wal {
        log.abandon(open.id).await?;
    }
    Ok(())
}

/// Reads `job` while ingesting what the read emits, until both end or the attempt is cancelled:
/// what was ingested, and how the read ended.
async fn read_and_ingest(
    job: &PartitionJob,
    context: &PartitionContext,
) -> Result<(Ingested, rdlt_connector::Result<()>), Error> {
    // Each push and checkpoint reserves what it costs before it enters the channel, so a source
    // buffers nothing outside the budget (spec §7.5).
    let admission = Arc::new(Charging::new(context.budget.clone()));
    let charging = Arc::clone(&admission) as Arc<dyn rdlt_connector::Admission>;
    let (sink, feed) = admitted_partition_channel(context.buffer, charging);
    let request = ReadRequest::new(
        job.stream.clone(),
        job.partition.clone(),
        job.cursor.clone(),
    )
    .following(job.follow);
    // An ingest failure ends the read rather than waiting for a source that may not emit again
    // for a long time. A read failure lets ingest drain what the source already sent. A read
    // asked to stop that has not ended within the stop wait is dropped, which ends its feed.
    let ingest_failed = CancellationToken::new();
    let overdue = async {
        job.stop.cancelled().await;
        context.env.sleep(context.stop_wait).await;
    };
    let read = async {
        tokio::select! {
            biased;
            () = ingest_failed.cancelled() => Ok(()),
            read = context.source.read(request, sink) => read,
            () = overdue => Ok(()),
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
    // A read that failed once the budget refused one of its events failed for the budget,
    // whatever error its source answered the refusal with.
    if let (Err(_), Some(exhausted)) = (&read, admission.exhausted()) {
        return Err(Error::memory(exhausted).with_stream(&job.stream));
    }
    Ok((ingested, read))
}

/// Where a partition that read to its end resumes, if anywhere new.
///
/// Rows received after the last checkpoint have no cursor that resumes past them, so committing
/// them marks the partition `Done`, whether they were written or its policy discarded them all,
/// and the seal carries the discards. Otherwise the partition resumes from its last cursor, so an
/// incremental read picks up rows the source adds later. A partition that read nothing and never
/// checkpointed records no position, so its next read starts from the beginning again.
///
/// An unbounded partition is never done: rows it pushed after its last checkpoint are not
/// sealed, so its next read, from that checkpoint, reads them again.
fn end_state(ingested: &Ingested, unbounded: bool) -> Option<PartitionState> {
    match (&ingested.last_cursor, ingested.open.received) {
        (Some(cursor), 0) => Some(PartitionState::Cursor(cursor.clone())),
        (None, 0) => None,
        _ if unbounded => None,
        _ => Some(PartitionState::Done),
    }
}

/// What an ingest loop leaves once the read has ended.
struct Ingested {
    open: OpenSegment,
    last_cursor: Option<Cursor>,
    /// Rows of the segments the read sealed.
    sealed_rows: u64,
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
    /// Deletes and truncates the stream ignores, dropped from the segment.
    deletes_ignored: u64,
    truncates_ignored: u64,
}

impl OpenSegment {
    fn new(id: SegmentId) -> Self {
        Self {
            id,
            ..Self::default()
        }
    }

    fn seal(
        self,
        partition: usize,
        state: PartitionState,
        answers: Option<u64>,
        held: CursorHold,
    ) -> Seal {
        Seal {
            partition,
            segment: self.id,
            rows: self.rows,
            bytes: self.bytes,
            state,
            answers,
            discarded_rows: self.discarded_rows,
            discarded_values: self.discarded_values,
            deletes_ignored: self.deletes_ignored,
            truncates_ignored: self.truncates_ignored,
            held,
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
        sealed_rows: 0,
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
            () = job.stop.cancelled(), if !ingested.stopped => {
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
            SourceEvent::Push(Push::Arrow(_) | Push::Json(_)) if job.changes.is_some() => {
                return Err(pushed_wrongly(job, "rows, where it is read as changes"));
            }
            SourceEvent::Push(Push::Arrow(batch)) => Pushed::Arrow(batch),
            SourceEvent::Push(Push::Json(json)) => Pushed::Json(json),
            SourceEvent::Push(Push::Changes(_)) if job.changes.is_none() => {
                return Err(pushed_wrongly(
                    job,
                    "changes, where it is not read as changes",
                ));
            }
            SourceEvent::Push(Push::Changes(batch)) => {
                rdlt_connector::validate_change_batch(&batch).map_err(|error| {
                    Error::connector(
                        Side::Source,
                        format!("reading stream {}", job.stream),
                        error,
                    )
                    .with_stream(&job.stream)
                })?;
                Pushed::Arrow(batch)
            }
            SourceEvent::Checkpoint { cursor, answers } => {
                // Coalescing never carries rows past a checkpoint, so segments are never split.
                self.flush(job, context).await?;
                let next = OpenSegment::new(context.next_segment());
                let sealed = std::mem::replace(&mut self.open, next);
                self.sealed_rows = self.sealed_rows.saturating_add(sealed.rows);
                let state = PartitionState::Cursor(cursor.clone());
                self.last_cursor = Some(cursor);
                // The permit that admitted the checkpoint holds its cursor until its commit.
                let seal = sealed.seal(job.index, state, answers, CursorHold::new(permit));
                return seal_segment(job, context, seal);
            }
            SourceEvent::Log { .. } | SourceEvent::Metric { .. } => return Ok(()),
            SourceEvent::Replan => {
                return context.signalled(job.index, context.latest.replan(job.index));
            }
            SourceEvent::Behind { records } => {
                return context.signalled(job.index, context.latest.behind(job.index, records));
            }
        };
        // Every push on an admitted channel carries the permit that reserved its bytes.
        let admitted = permit
            .and_then(Admitted::of)
            .ok_or_else(|| Error::internal("a push arrived without its permit"))?;
        let held: (u64, Permit) = (admitted.bytes, admitted);
        for flushed in self.coalescer.add(pushed, held, context.env.instant()) {
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

/// Reports `seal`, a checkpoint's: a seal of no rows waits as its partition's newest, in place
/// of any before it, and a seal with rows follows whatever it sealed in the coordinator's queue.
fn seal_segment(
    job: &PartitionJob,
    context: &PartitionContext,
    mut seal: Seal,
) -> Result<(), Error> {
    let partition = job.index;
    if seal.moves_only() {
        return match context.latest.moved(seal) {
            Some(epoch) => context.report(Progress::Moved { partition, epoch }),
            None => Ok(()),
        };
    }
    // The position it carries is newer than a waiting seal's of no rows, which it replaces.
    seal.answers = seal.answers.max(context.latest.superseded(partition));
    if let Some(log) = &context.wal {
        log.checkpointed();
    }
    context.report(Progress::Sealed(seal))
}

/// The error for a push the stream's read mode does not take: `what` it pushed, and why not.
fn pushed_wrongly(job: &PartitionJob, what: &str) -> Error {
    Error::new(
        ErrorKind::Source,
        format!("stream {} pushed {what}", job.stream),
    )
    .with_code("push_unexpected")
    .with_stream(&job.stream)
}
