//! The commit coordinator: one task per attempt that owns the destination session.
//!
//! It tracks every partition's sealed segments, decides when a commit is due, asks on-demand
//! partitions to checkpoint (a barrier), flushes the lanes, commits the sealed segments with their
//! state, and acknowledges the committed cursors to the source. At most one commit is in flight;
//! partitions keep reading while it runs.

mod delta;
#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use rdlt_connector::{
    CommitMeta, CommitSeq, Cursor, Epoch, GenerationId, LoadId, PartitionId, PartitionState,
    Source, StateChange, StateEntry, StreamName,
};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::config::CommitPolicy;
use crate::env::{Env, Sleep};
use crate::error::{Error, Side};
use crate::lane::Lanes;
use crate::partition::{Progress, Seal};
use crate::plan::WriteMode;
use crate::report::{AttemptEnd, AttemptLog, CommitRecord};
use crate::table::Tables;

/// A stream as one attempt loads it.
#[derive(Debug)]
pub(crate) struct StreamRun {
    pub(crate) name: StreamName,
    pub(crate) write: WriteMode,
    /// The index of the stream's table among the attempt's tables.
    pub(crate) table: usize,
    /// The full read in progress; `None` for incremental reads.
    pub(crate) cycle: Option<Cycle>,
    /// Partitions still reading.
    pub(crate) remaining: usize,
    /// Whether any partition stopped before its end.
    pub(crate) stopped: bool,
}

/// A full read of a stream, from its first partition to its last; a replace stream fills the
/// cycle's generation.
#[derive(Debug)]
pub(crate) struct Cycle {
    pub(crate) generation: GenerationId,
    /// Whether state records the cycle.
    pub(crate) recorded: bool,
    /// Partition entries of the previous cycle, deleted when this cycle is first recorded.
    pub(crate) stale: Vec<PartitionId>,
    /// Whether a commit has already ended the cycle.
    pub(crate) finished: bool,
    /// The stream's recently completed reads, which this one joins when it completes.
    pub(crate) completed: Vec<GenerationId>,
}

/// How many completed reads state keeps per stream: enough that a run's retry still finds its own
/// read among them after other runs of the pipeline completed theirs.
const KEPT_COMPLETIONS: usize = 16;

impl StreamRun {
    /// Whether the next commit ends the stream's cycle: every partition read to its end.
    fn completes(&self) -> bool {
        self.remaining == 0
            && !self.stopped
            && self.cycle.as_ref().is_some_and(|cycle| !cycle.finished)
    }
}

/// A partition as the coordinator sees it.
#[derive(Debug)]
pub(crate) struct PartitionRun {
    pub(crate) stream: usize,
    pub(crate) id: PartitionId,
    /// Whether the partition checkpoints when asked.
    pub(crate) on_demand: bool,
    started: bool,
    ended: bool,
    answered: u64,
}

impl PartitionRun {
    pub(crate) fn new(stream: usize, id: PartitionId, on_demand: bool) -> Self {
        Self {
            stream,
            id,
            on_demand,
            started: false,
            ended: false,
            answered: 0,
        }
    }

    /// Whether barrier `barrier` is waiting for this partition.
    fn owes(&self, barrier: u64) -> bool {
        self.on_demand && self.started && !self.ended && self.answered < barrier
    }
}

/// Everything a coordinator starts with.
pub(crate) struct CoordinatorParts {
    pub(crate) env: Arc<dyn Env>,
    pub(crate) policy: CommitPolicy,
    pub(crate) barrier_wait: Duration,
    /// The tables, and through them the destination session.
    pub(crate) tables: Arc<Tables>,
    pub(crate) source: Arc<dyn Source>,
    pub(crate) lanes: Lanes,
    pub(crate) load_id: LoadId,
    pub(crate) epoch: Epoch,
    pub(crate) streams: Vec<StreamRun>,
    pub(crate) partitions: Vec<PartitionRun>,
    pub(crate) progress: mpsc::UnboundedReceiver<Progress>,
    pub(crate) barrier: watch::Sender<u64>,
    /// Asks every partition to stop reading.
    pub(crate) stop_reads: CancellationToken,
    /// Fires when the run is asked to stop after committing.
    pub(crate) stop: CancellationToken,
    /// Fires when the attempt is cancelled.
    pub(crate) cancel: CancellationToken,
    pub(crate) log: Arc<Mutex<AttemptLog>>,
}

pub(crate) struct Coordinator {
    parts: CoordinatorParts,
    seq: CommitSeq,
    sealed: Vec<Seal>,
    /// Rows and bytes written but not yet committed.
    pending_rows: u64,
    pending_bytes: u64,
    barrier: u64,
    stopping: bool,
}

impl Coordinator {
    pub(crate) fn new(parts: CoordinatorParts) -> Self {
        Self {
            parts,
            seq: CommitSeq::FIRST,
            sealed: Vec::new(),
            pending_rows: 0,
            pending_bytes: 0,
            barrier: 0,
            stopping: false,
        }
    }

    /// Commits as the policy says until every partition has ended, then commits what is left
    /// and closes the session.
    pub(crate) async fn run(mut self) -> Result<(), Error> {
        let mut timer = self.timer();
        while !self.all_ended() {
            tokio::select! {
                biased;
                // Cancellation wins: the attempt is ending and nothing more may commit.
                () = self.parts.cancel.cancelled() => return Err(cancelled()),
                // A stop request comes before more data, so reading stops promptly.
                () = self.parts.stop.cancelled(), if !self.stopping => {
                    self.stopping = true;
                    self.raise_barrier().await?;
                    self.parts.stop_reads.cancel();
                }
                // The interval comes before data, so a busy source still commits on time.
                () = &mut timer => {
                    self.raise_barrier().await?;
                    self.commit().await?;
                    timer = self.timer();
                }
                progress = self.parts.progress.recv() => {
                    self.observe(progress.ok_or_else(cancelled)?);
                    if self.parts.policy.is_due(self.pending_rows, self.pending_bytes) {
                        self.raise_barrier().await?;
                        self.commit().await?;
                        timer = self.timer();
                    }
                }
            }
        }
        self.commit().await?;
        let end = if self.stopping {
            AttemptEnd::Stopped
        } else {
            AttemptEnd::Exhausted
        };
        self.parts.tables.session().close().await?;
        self.parts.log.lock().end = Some(end);
        Ok(())
    }

    fn timer(&self) -> Sleep {
        match self.parts.policy.every() {
            Some(every) => self.parts.env.sleep(every),
            None => Box::pin(std::future::pending()),
        }
    }

    fn all_ended(&self) -> bool {
        self.parts
            .partitions
            .iter()
            .all(|partition| partition.ended)
    }

    fn observe(&mut self, progress: Progress) {
        match progress {
            Progress::Started { partition } => self.parts.partitions[partition].started = true,
            Progress::Written { rows, bytes } => {
                self.pending_rows += rows;
                self.pending_bytes += bytes;
            }
            Progress::Sealed(seal) => {
                if let Some(barrier) = seal.answers {
                    let partition = &mut self.parts.partitions[seal.partition];
                    partition.answered = partition.answered.max(barrier);
                }
                self.sealed.push(seal);
            }
            Progress::Ended { partition, stopped } => {
                let partition = &mut self.parts.partitions[partition];
                partition.ended = true;
                let stream = &mut self.parts.streams[partition.stream];
                stream.remaining -= 1;
                stream.stopped |= stopped;
            }
        }
    }

    /// Asks every reading on-demand partition to checkpoint, and waits until each has answered,
    /// ended, or `barrier_wait` has passed.
    async fn raise_barrier(&mut self) -> Result<(), Error> {
        self.barrier += 1;
        let barrier = self.barrier;
        self.parts.barrier.send_replace(barrier);
        let mut deadline = self.parts.env.sleep(self.parts.barrier_wait);
        while self
            .parts
            .partitions
            .iter()
            .any(|partition| partition.owes(barrier))
        {
            tokio::select! {
                biased;
                () = self.parts.cancel.cancelled() => return Err(cancelled()),
                // Once the wait is over, the commit takes whatever is sealed.
                () = &mut deadline => break,
                progress = self.parts.progress.recv() => self.observe(progress.ok_or_else(cancelled)?),
            }
        }
        Ok(())
    }

    /// Commits every sealed segment with the state that goes with it, then acknowledges the
    /// committed cursors to the source.
    ///
    /// Commits nothing when there is nothing to publish or record.
    async fn commit(&mut self) -> Result<(), Error> {
        let collected = self.collect();
        let completing: Vec<usize> = (0..self.parts.streams.len())
            .filter(|index| self.parts.streams[*index].completes())
            .collect();
        let tables = self.parts.tables.delta();
        let mut delta = self.state_delta(&collected.positions, &completing);
        delta.extend(tables.changes);
        let finish_generations = self.finish_generations(&completing);
        if collected.segments.is_empty() && delta.is_empty() {
            return Ok(());
        }
        let streams = self.stream_reports(collected.streams, &completing);
        // The commit records its own receipt, so an attempt that loses the response can still be
        // credited with it once a later attempt reads it back.
        let marker = self.marker(&streams);
        delta.push(StateChange::Put(
            StateEntry::Receipt(marker.clone()).to_record(),
        ));
        self.parts.log.lock().pending = Some(CommitRecord {
            receipt: marker,
            streams: streams.clone(),
        });
        self.parts.lanes.flush().await?;
        let meta = CommitMeta {
            load_id: self.parts.load_id,
            commit_seq: self.seq,
            epoch: self.parts.epoch,
            segments: collected.segments,
            state_delta: delta,
            finish_generations,
        };
        let receipt = self
            .parts
            .tables
            .session()
            .commit(&meta)
            .await?
            .map_err(|error| Error::connector(Side::Destination, "committing", error))?;
        self.parts.tables.recorded(&tables.versions);
        self.record(receipt, streams, &completing);
        self.acknowledge(collected.positions).await
    }

    /// Advances past a landed commit: what state now records, and the commit in the log.
    fn record(
        &mut self,
        receipt: rdlt_connector::Receipt,
        streams: BTreeMap<StreamName, crate::report::StreamReport>,
        completing: &[usize],
    ) {
        self.seq = self.seq.next();
        // Rows still unsealed stay pending, so they make the next commit due as soon as they seal.
        for counts in streams.values() {
            self.pending_rows = self.pending_rows.saturating_sub(counts.rows);
            self.pending_bytes = self.pending_bytes.saturating_sub(counts.bytes);
        }
        for stream in &mut self.parts.streams {
            if let Some(cycle) = &mut stream.cycle {
                cycle.recorded = true;
                cycle.stale.clear();
            }
        }
        for index in completing {
            if let Some(cycle) = &mut self.parts.streams[*index].cycle {
                cycle.finished = true;
            }
        }
        let mut log = self.parts.log.lock();
        log.pending = None;
        log.commits.push(CommitRecord { receipt, streams });
    }

    /// Tells the source which cursors are committed, per stream.
    async fn acknowledge(
        &mut self,
        positions: BTreeMap<usize, PartitionState>,
    ) -> Result<(), Error> {
        let mut cursors: BTreeMap<usize, Vec<(PartitionId, Cursor)>> = BTreeMap::new();
        for (partition, state) in positions {
            if let PartitionState::Cursor(cursor) = state {
                let partition = &self.parts.partitions[partition];
                cursors
                    .entry(partition.stream)
                    .or_default()
                    .push((partition.id.clone(), cursor));
            }
        }
        for (stream, cursors) in cursors {
            let name = &self.parts.streams[stream].name;
            self.parts
                .source
                .committed(name, &cursors)
                .await
                .map_err(|error| {
                    Error::connector(Side::Source, format!("acknowledging stream {name}"), error)
                        .with_stream(name)
                })?;
        }
        Ok(())
    }
}

fn cancelled() -> Error {
    Error::cancelled("the attempt was cancelled")
}
