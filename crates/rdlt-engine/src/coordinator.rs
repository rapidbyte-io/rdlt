//! The commit coordinator: one task per attempt that owns the destination session.
//!
//! It tracks every partition's sealed segments, decides when a commit is due, asks on-demand
//! partitions to checkpoint (a barrier), flushes the lanes, commits the sealed segments with their
//! state, and acknowledges the committed cursors to the source. At most one commit is in flight;
//! partitions keep reading while it runs.

mod acks;
mod delta;
mod phases;
mod replan;
mod signals;
#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use rdlt_connector::{
    CommitMeta, CommitSeq, Epoch, GenerationId, LoadId, PartitionId, Sequences, Source,
    StateChange, StateEntry, StreamName, TablePath,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::config::CommitPolicy;
use crate::env::{Env, Sleep};
use crate::error::{Error, Side};
use crate::lane::Lanes;
use crate::partition::{Progress, Seal};
use crate::plan::WriteMode;
use crate::report::{AttemptEnd, AttemptLog, CommitRecord};
use crate::table::Tables;
use crate::wal::{LoadLog, Positions};
use crate::watch;
pub(crate) use phases::{Begun, Launcher, Phases, Template, launcher};

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
    /// For a stream read in phases, its place in them.
    pub(crate) phases: Option<Phases>,
    /// Who made the sequences of the stream's table, where state records otherwise; the next
    /// commit records them.
    pub(crate) sequences: Option<(TablePath, Sequences)>,
    /// Whether the stream's source can read again what it acknowledged; one that cannot learns
    /// its position once the load's log holds it, before the destination commits.
    pub(crate) replayable: bool,
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
    /// Stops this partition alone.
    stop: CancellationToken,
}

impl PartitionRun {
    pub(crate) fn new(
        stream: usize,
        id: PartitionId,
        on_demand: bool,
        stop: CancellationToken,
    ) -> Self {
        Self {
            stream,
            id,
            on_demand,
            started: false,
            ended: false,
            answered: 0,
            stop,
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
    /// Starts the partitions of a stream's next phase.
    pub(crate) launcher: Launcher,
    /// The load's write-ahead log, where it keeps one.
    pub(crate) wal: Option<LoadLog>,
    /// The partitions' positions as the destination holds them, through the commits that landed.
    pub(crate) positions: Positions,
    /// Whether the run follows its source: it reads until stopped, and plans its streams again
    /// every `replan`.
    pub(crate) follow: bool,
    pub(crate) replan: Duration,
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
    /// Streams whose sources said their partitions changed, to plan again.
    replans: BTreeSet<usize>,
    /// How many records each stream's partitions last said their reads are behind, by stream.
    lag: BTreeMap<usize, BTreeMap<PartitionId, u64>>,
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
            replans: BTreeSet::new(),
            lag: BTreeMap::new(),
        }
    }

    /// Commits as the policy says until every partition has ended and no stream starts another
    /// phase, then closes the session.
    pub(crate) async fn run(mut self) -> Result<(), Error> {
        loop {
            self.load().await?;
            self.commit().await?;
            self.advance_phases().await?;
            if self.done() {
                break;
            }
        }
        let end = if self.stopping {
            AttemptEnd::Stopped
        } else {
            AttemptEnd::Exhausted
        };
        // Writes of rows no commit took, as an unbounded partition's after its last checkpoint,
        // finish before the session closes; the next session discards them.
        self.parts.lanes.flush().await?;
        if let Some(log) = &self.parts.wal {
            // Every commit landed: a log that fails to close stays, and a replay finds each of its
            // commits received, which the destination answers with its stored receipt.
            drop(log.close().await);
        }
        self.parts.tables.session().close().await?;
        self.parts.log.lock().end = Some(end);
        Ok(())
    }

    /// Commits as the policy says until every partition has ended, and in a following run the
    /// attempt stops; a following run plans its streams again as it reads.
    async fn load(&mut self) -> Result<(), Error> {
        let mut timer = self.timer();
        let mut replan = self.replan_timer();
        while !self.done() {
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
                // Planning again starts from the positions the last commit made durable: a
                // partition whose end is not yet committed waits for a later plan.
                () = &mut replan => {
                    self.replan().await?;
                    replan = self.replan_timer();
                }
                // The interval comes before data, so a busy source still commits on time.
                () = &mut timer => {
                    self.raise_barrier().await?;
                    self.commit().await?;
                    self.advance_phases().await?;
                    timer = self.timer();
                }
                progress = self.parts.progress.recv() => {
                    self.observe(progress.ok_or_else(cancelled)?);
                    self.replan_signalled().await?;
                    if self.parts.policy.is_due(self.pending_rows, self.pending_bytes) {
                        self.raise_barrier().await?;
                        self.commit().await?;
                        self.advance_phases().await?;
                        timer = self.timer();
                    }
                }
            }
        }
        Ok(())
    }

    fn timer(&self) -> Sleep {
        match self.parts.policy.every() {
            Some(every) => self.parts.env.sleep(every),
            None => Box::pin(std::future::pending()),
        }
    }

    fn replan_timer(&self) -> Sleep {
        if self.parts.follow {
            self.parts.env.sleep(self.parts.replan)
        } else {
            Box::pin(std::future::pending())
        }
    }

    /// Whether the attempt has read all it will: every partition ended, and a following run
    /// asked to stop.
    fn done(&self) -> bool {
        self.all_ended() && (!self.parts.follow || self.stopping)
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
            Progress::Replan { partition } => self.signalled(partition),
            Progress::Behind { partition, records } => self.behind(partition, records),
            Progress::RetentionReset { partition } => self.reset(partition),
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
        // A new phase's stale entries go before its partitions' positions, which may reuse ids.
        let mut delta = self.phase_delta();
        delta.extend(self.sequences_delta());
        delta.extend(self.state_delta(&collected.positions, &completing));
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
            child_tables: self.parts.tables.child_tables(),
        };
        if let Some(log) = &self.parts.wal {
            // Every batch of the commit's segments was queued for the log before its partition
            // sealed it: the commit's frame, queued now, follows them all.
            log.commit(collected.sealed, &meta).await?;
            self.acknowledge(&collected.positions, false).await?;
        }
        let receipt = self
            .parts
            .tables
            .session()
            .commit(&meta)
            .await?
            .map_err(|error| Error::connector(Side::Destination, "committing", error))?;
        if let Some(log) = &self.parts.wal {
            log.committed(&receipt).await?;
            self.parts.positions.apply(&meta.state_delta);
        }
        self.parts.tables.recorded(&tables.revisions);
        self.record(receipt, streams, &completing);
        self.record_positions(&collected.positions);
        self.acknowledge(&collected.positions, true).await
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
        log.committed.add(CommitRecord { receipt, streams });
    }
}

fn cancelled() -> Error {
    Error::cancelled("the attempt was cancelled")
}
