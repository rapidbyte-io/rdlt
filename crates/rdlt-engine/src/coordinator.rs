//! The commit coordinator: one task per attempt that owns the destination session.
//!
//! It tracks every partition's sealed segments, decides when a commit is due, asks on-demand
//! partitions to checkpoint (a barrier), flushes the lanes, commits the sealed segments with their
//! state, sending the commit again in place where the destination answers it transiently, and
//! acknowledges the committed cursors to the source, owing it those of a report that failed until
//! a later commit's report carries them. At most one commit is in flight; partitions keep reading
//! while it runs.

mod acks;
mod barrier;
mod commit;
mod delta;
mod done;
mod due;
mod phases;
mod pressure;
mod replan;
mod retried;
mod signals;
#[cfg(test)]
mod tests;
mod waiting;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use rdlt_connector::{
    CommitKey, CommitMeta, Epoch, GenerationId, PartitionId, PartitionState, Receipt, SegmentSet,
    Source, StreamName, TablePath,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::attempt::Keying;
use crate::budget::MemoryBudget;
use crate::config::{CommitPolicy, RetryPolicy};
use crate::crash::crash_point;
use crate::env::{Env, Sleep};
use crate::error::Error;
use crate::lane::Lanes;
use crate::partition::{Latest, Progress};
use crate::plan::WriteMode;
use crate::report::{AttemptEnd, AttemptLog, CommitRecord, Tally, Trigger};
use crate::stored::{Measured, Stored};
use crate::table::Tables;
use crate::watch;
pub(crate) use done::DoneMarkers;
use done::Recorded;
pub(crate) use phases::{Begun, Launcher, Phases, Template, launcher, plan_of};
use waiting::WaitingSeals;

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
    /// How the stream's table sequences and matches its rows, where state records otherwise; the
    /// next commit records it.
    pub(crate) sequences: Option<(TablePath, Keying)>,
    /// Every partition the stream's latest plan names: the done ones it leaves unread among
    /// them, whose markers stay under state pressure.
    pub(crate) named: BTreeSet<PartitionId>,
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
    /// Where the partition stood when its read started, then the position of its last commit
    /// that landed: a commit that records it there again moves it nowhere.
    stands: Option<PartitionState>,
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
            stands: None,
        }
    }

    /// The partition, whose read starts from `cursor`, where the destination holds it.
    pub(crate) fn starting(mut self, cursor: Option<&rdlt_connector::Cursor>) -> Self {
        self.stands = cursor.cloned().map(PartitionState::Cursor);
        self
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
    pub(crate) epoch: Epoch,
    /// The key of the pipeline's last commit that had landed when the session opened, which its
    /// first commit expects landed.
    pub(crate) after: Option<CommitKey>,
    pub(crate) streams: Vec<StreamRun>,
    pub(crate) partitions: Vec<PartitionRun>,
    pub(crate) progress: mpsc::UnboundedReceiver<Progress>,
    /// What each partition last said of which only the newest matters.
    pub(crate) latest: Arc<Latest>,
    /// The memory budget, whose cursors of waiting seals make a commit due once they hold their
    /// share of it.
    pub(crate) budget: MemoryBudget,
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
    /// The partitions the destination holds done, through the commits that landed.
    pub(crate) done: DoneMarkers,
    /// What the destination's stored state takes, through the commits that landed.
    pub(crate) stored: Stored,
    /// Whether the run follows its source: it reads until stopped, and plans its streams again
    /// every `replan`.
    pub(crate) follow: bool,
    pub(crate) replan: Duration,
    /// Where the commits count what made each due and the time their phases took.
    pub(crate) tally: Arc<Tally>,
    /// The run's retry policy: a commit the destination answers transiently is sent as many
    /// times as the run makes attempts, the waits between drawn as the run's are, and as many
    /// failed reports in a row of a partition's cursor end the attempt, or give up the cursor of a
    /// partition recorded done.
    pub(crate) retry: RetryPolicy,
    /// How long a commit in flight has to answer once the attempt is cancelled, so one that lands
    /// as another task fails is still recorded: the run's close wait.
    pub(crate) close_wait: Duration,
}

pub(crate) struct Coordinator {
    parts: CoordinatorParts,
    /// The next commit's key: the session's epoch, and its place among the session's commits.
    key: CommitKey,
    /// The key of the commit the next one expects landed: the last that landed.
    after: Option<CommitKey>,
    sealed: WaitingSeals,
    /// Whether something arrived since the last commit.
    cursors_may_free: bool,
    /// The rows and bytes a commit could take.
    due: due::Due,
    barrier: u64,
    stopping: bool,
    /// Streams whose sources said their partitions changed, to plan again.
    replans: BTreeSet<usize>,
    /// How many records each stream's partitions last said their reads are behind, by stream.
    lag: BTreeMap<usize, signals::Lag>,
    /// How many tracked partitions have not ended.
    unended: usize,
    /// The partitions that owe the newest barrier an answer.
    owing: BTreeSet<usize>,
    /// The partitions with seals no commit has taken yet.
    sealing: BTreeSet<usize>,
    /// The places of ended partitions no stream reads any more, which a partition a plan names
    /// takes once no seal of theirs waits for a commit.
    retired: BTreeSet<usize>,
    /// The segments partitions abandoned since the last commit, whose staging the next commit
    /// removes.
    abandoned: SegmentSet,
    /// The cursors the source has yet to be told are committed.
    owed: acks::Owed,
}

impl Coordinator {
    pub(crate) fn new(parts: CoordinatorParts) -> Self {
        let unended = parts.partitions.iter().filter(|run| !run.ended).count();
        Self {
            cursors_may_free: true,
            key: CommitKey {
                epoch: parts.epoch,
                seq: 1,
            },
            after: parts.after,
            parts,
            sealed: WaitingSeals::default(),
            due: due::Due::default(),
            barrier: 0,
            stopping: false,
            replans: BTreeSet::new(),
            lag: BTreeMap::new(),
            unended,
            owing: BTreeSet::new(),
            sealing: BTreeSet::new(),
            retired: BTreeSet::new(),
            abandoned: SegmentSet::new(),
            owed: acks::Owed::default(),
        }
    }

    /// Commits as the policy says until every partition has ended and no stream starts another
    /// phase, then closes the session.
    pub(crate) async fn run(mut self) -> Result<(), Error> {
        loop {
            self.load().await?;
            self.commit(Trigger::End).await?;
            self.advance_phases().await?;
            if self.done() {
                break;
            }
        }
        self.settle().await?;
        let end = if self.stopping {
            AttemptEnd::Stopped
        } else {
            AttemptEnd::Exhausted
        };
        // Writes of rows no commit took, as an unbounded partition's after its last checkpoint,
        // finish before the session closes; the next session discards them.
        self.parts.lanes.flush().await?;
        self.parts.tables.session().close().await?;
        self.parts.log.lock().end = Some(end);
        Ok(())
    }

    /// Commits as the policy says until every partition has ended, and in a following run the
    /// attempt stops; a following run plans its streams again as it reads.
    async fn load(&mut self) -> Result<(), Error> {
        let mut timer = self.timer();
        let mut replan = self.replan_timer();
        let mut commits = self.parts.budget.cursor_waits();
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
                () = &mut timer => timer = self.commit_now(Trigger::Interval).await?,
                // A cursor that finds its share full waits for a commit, which is then due:
                // once, since a commit that frees it nothing is not tried again before more
                // arrives.
                () = commits.raised(), if self.cursors_may_free => {
                    timer = self.commit_now(Trigger::Cursors).await?;
                }
                progress = self.parts.progress.recv() => {
                    self.observe(progress.ok_or_else(cancelled)?);
                    self.replan_signalled().await?;
                    if let Some(trigger) = self.commit_due() {
                        timer = self.commit_now(trigger).await?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Commits what is sealed behind a barrier, as `trigger` made due, and advances the phases:
    /// the timer of the commit after it.
    async fn commit_now(&mut self, trigger: Trigger) -> Result<Sleep, Error> {
        self.raise_barrier().await?;
        self.commit(trigger).await?;
        self.advance_phases().await?;
        self.cursors_may_free = false;
        Ok(self.timer())
    }

    /// What makes a commit due, where something does: the policy's rows and bytes, or the
    /// cursors of the seals waiting, which hold budget only a commit releases, once they take
    /// half their share.
    fn commit_due(&self) -> Option<Trigger> {
        let cursors = (self.parts.budget.shares().cursors / 2).max(1);
        if self.parts.policy.is_due(self.due.rows(), self.due.bytes()) {
            Some(Trigger::Size)
        } else if self.sealed.cursor_bytes() >= cursors {
            Some(Trigger::Cursors)
        } else {
            None
        }
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
        self.unended == 0
    }

    fn observe(&mut self, progress: Progress) {
        // Whatever arrives, wherever it is heard, may be a seal a commit frees cursors with.
        self.cursors_may_free = true;
        match progress {
            Progress::Started { partition } => {
                let run = &mut self.parts.partitions[partition];
                run.started = true;
                if run.owes(self.barrier) {
                    self.owing.insert(partition);
                }
            }
            Progress::Written {
                partition,
                rows,
                bytes,
            } => {
                debug_assert!(
                    self.parts
                        .partitions
                        .get(partition)
                        .is_some_and(|run| run.on_demand),
                    "only a partition that seals when asked tells its writes"
                );
                self.due.written(partition, rows, bytes);
            }
            Progress::Abandoned { partition, segment } => {
                self.due.abandoned(partition);
                self.abandoned.insert(segment);
            }
            Progress::Sealed(seal) => self.seal(seal),
            Progress::Moved { partition, epoch } => {
                if let Some(seal) = self.parts.latest.seal(partition, epoch) {
                    self.seal(seal);
                }
            }
            Progress::Signalled { partition } => {
                let (behind, replan) = self.parts.latest.signals(partition);
                if replan {
                    self.signalled(partition);
                }
                if let Some(records) = behind {
                    self.behind(partition, records);
                }
            }
            Progress::RetentionReset { partition } => self.reset(partition),
            Progress::Ended { partition, stopped } => {
                self.owing.remove(&partition);
                let partition = &mut self.parts.partitions[partition];
                partition.ended = true;
                self.unended -= 1;
                let stream = &mut self.parts.streams[partition.stream];
                stream.remaining -= 1;
                stream.stopped |= stopped;
            }
        }
    }

    /// Keeps `seal` for the next commit, and notes the barrier it answers.
    fn seal(&mut self, seal: crate::partition::Seal) {
        if let Some(barrier) = seal.answers {
            let partition = &mut self.parts.partitions[seal.partition];
            partition.answered = partition.answered.max(barrier);
            if !partition.owes(self.barrier) {
                self.owing.remove(&seal.partition);
            }
        }
        self.sealing.insert(seal.partition);
        self.due.sealed(seal.partition, seal.rows, seal.bytes);
        self.sealed.push(seal);
    }

    /// Commits `meta`, whose state changes are `measured` and whose partitions stand at
    /// `positions`, in the destination's session, sent again in place where the destination
    /// answers it transiently.
    async fn committed(
        &mut self,
        meta: &CommitMeta,
        measured: Measured,
        positions: &BTreeMap<usize, PartitionState>,
    ) -> Result<Receipt, Error> {
        crash_point!("engine.commit.before");
        let receipt = self.tried(meta).await?;
        crash_point!("engine.commit.after");
        self.parts.stored.apply(measured);
        let states: BTreeMap<(StreamName, PartitionId), PartitionState> = positions
            .iter()
            .map(|(partition, state)| {
                let partition = &self.parts.partitions[*partition];
                let stream = self.parts.streams[partition.stream].name.clone();
                ((stream, partition.id.clone()), state.clone())
            })
            .collect();
        let recorded = Recorded {
            epoch: self.parts.epoch,
            states: &states,
        };
        self.parts.done.landed(&meta.state_delta, &recorded);
        Ok(receipt)
    }

    /// Advances past a landed commit: what state now records.
    ///
    /// # Errors
    ///
    /// `receipt_overflow` where the receipt counts more than the attempt's totals hold.
    fn record(
        &mut self,
        receipt: Receipt,
        streams: BTreeMap<StreamName, crate::report::StreamReport>,
        completing: &[usize],
    ) -> Result<(), Error> {
        self.after = Some(receipt.key);
        self.key.seq = self
            .key
            .seq
            .checked_add(1)
            .expect("a session commits fewer than u64::MAX times");
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
        log.committed.add(CommitRecord { receipt, streams })
    }
}

fn cancelled() -> Error {
    Error::cancelled("the attempt was cancelled")
}
