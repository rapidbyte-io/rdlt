//! What a run counts and times as it works: written as it happens, summed into its report, and
//! read by no decision.

use std::time::Duration;

use parking_lot::Mutex;
use serde::Serialize;

/// What a run waited for and spent its time on, across its attempts, measured on its
/// environment's clock.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Counters {
    /// The run's waits for room in its memory budget, by share.
    pub waits: Waits,
    /// The jobs the run gave its compute pool.
    pub pool: PoolCounters,
    /// The run's commits, by what made each due, and the tries it sent again.
    pub commits: Commits,
    /// The run's reports to its source of the cursors its commits landed with that failed, and
    /// the cursors it gave up.
    pub acks: Acks,
    /// The time the run's commits took, phase by phase.
    pub phases: CommitPhases,
    /// What each lane of the run waited for and spent its time on, by the lane's place.
    pub lanes: Vec<LaneCounters>,
    /// Changes the run's budget sent to the tasks watching for pressure and for cursors' waits,
    /// each waking every task waiting on that value.
    pub signals: u64,
    /// Bytes: the most the run's reads kept beside their events at once, of the budget's share
    /// for what reads keep.
    pub kept_peak: u64,
}

/// Jobs on the compute pool, and how long they waited to start, ran and took to come back.
///
/// A job counts once its caller takes its result, so a job a cancelled caller left is not counted.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct PoolCounters {
    /// Jobs run.
    pub jobs: u64,
    /// The time from each job's queueing to its start, summed.
    pub queued: Duration,
    /// The time each job ran, summed.
    pub running: Duration,
    /// The time from each job's end to its caller taking its result, summed: the way back to
    /// the caller, and for a call of several jobs the time a finished job waits for those
    /// before it.
    pub returned: Duration,
}

/// Commits that landed, by what made each due, and the tries of commits sent again.
///
/// A commit counts once it lands, whatever becomes of the report of its cursors.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Commits {
    /// Made due by the commit policy's interval.
    pub interval: u64,
    /// Made due by the rows or bytes the commit policy allows.
    pub size: u64,
    /// Made due by checkpoints' cursors, waiting for room or holding half their share.
    pub cursors: u64,
    /// Of what was sealed once every partition reading had ended.
    pub end: u64,
    /// Tries of commits sent again in place, each after the destination answered the try before
    /// it transiently.
    pub retried: u64,
}

/// Reports of committed cursors to the source that failed, and owed cursors given up.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Acks {
    /// Reports the source refused, or whose answer was lost: each leaves the cursors it carried
    /// owed to the next commit's report.
    pub failed: u64,
    /// Owed cursors of partitions recorded done that no report told: no later attempt reads
    /// such a partition, so its source trails where the destination holds it.
    pub dropped: u64,
}

/// The time commits that landed took, phase by phase, summed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct CommitPhases {
    /// Flushing the lanes, so the destination holds every write the commit publishes.
    pub flush: Duration,
    /// The destination's commit.
    pub commit: Duration,
    /// Acknowledging the committed cursors to the source.
    pub ack: Duration,
}

/// What made a commit due.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Trigger {
    Interval,
    Size,
    Cursors,
    End,
}

/// What one lane waited for and spent its time on.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct LaneCounters {
    /// The time partitions waited for room in the lane's queue, summed.
    pub blocked: Duration,
    /// The time its destination writers took to write batches, a failed write's included, summed.
    pub writing: Duration,
    /// The time its destination writers took to flush, a failed flush's included, summed.
    pub flushing: Duration,
    /// Its destination writers' flushes, by what asked for each.
    pub flushes: Flushes,
}

/// A lane's flushes of its destination writers, a failed flush's included, by what asked for
/// each.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Flushes {
    /// A request waiting for bytes the lane's writes held.
    pub pressure: u64,
    /// A commit, before it publishes what the lanes staged, or the attempt's end, for writes no
    /// commit took.
    pub commit: u64,
    /// The writer's retirement: of a schema version a newer one superseded, or of the writer
    /// written longest ago where the lane holds as many open as it may.
    pub retired: u64,
}

/// What asked a lane to flush a writer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Flush {
    Pressure,
    Commit,
    Retirement,
}

/// What shredding JSON took, in passes over chunks of its records.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct ShredCounts {
    /// Passes that parsed a chunk building its columns as they went; a chunk parsed again with
    /// exact numbers counts again.
    pub parsed: u64,
    /// Of those, the passes whose builders would have taken more than the chunk was admitted
    /// for.
    pub tripped: u64,
    /// Passes that read a chunk only to observe its values, once its parse tripped.
    pub observed: u64,
    /// Chunks built again against their flush's shape.
    pub rebuilt: u64,
    /// Of every pass above, those that read numbers exactly.
    pub exact: u64,
}

impl ShredCounts {
    /// Adds what `other` counts.
    pub(crate) fn add(&mut self, other: &Self) {
        self.parsed = self.parsed.saturating_add(other.parsed);
        self.tripped = self.tripped.saturating_add(other.tripped);
        self.observed = self.observed.saturating_add(other.observed);
        self.rebuilt = self.rebuilt.saturating_add(other.rebuilt);
        self.exact = self.exact.saturating_add(other.exact);
    }

    /// Counts one more pass that parsed, exactly where `exact` says.
    pub(crate) fn parse(&mut self, exact: bool) {
        self.parsed = self.parsed.saturating_add(1);
        self.exact = self.exact.saturating_add(u64::from(exact));
    }

    /// Counts one more pass that only observed, exactly where `exact` says.
    pub(crate) fn observe(&mut self, exact: bool) {
        self.observed = self.observed.saturating_add(1);
        self.exact = self.exact.saturating_add(u64::from(exact));
    }

    /// Counts one more chunk built again, exactly where `exact` says.
    pub(crate) fn rebuild(&mut self, exact: bool) {
        self.rebuilt = self.rebuilt.saturating_add(1);
        self.exact = self.exact.saturating_add(u64::from(exact));
    }
}

/// Waits for room in a memory budget, by the share waited for.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Waits {
    /// Pushes waiting for room to be admitted.
    pub intake: Waited,
    /// Frames of served reads waiting for room among pushes to be decoded, apart from the
    /// pushes they carry.
    pub decode: Waited,
    /// Pieces waiting for room to be lowered.
    pub work: Waited,
    /// Checkpoints' cursors waiting for a commit to make room for them.
    pub cursors: Waited,
    /// Tables' records waiting for a commit to make room for them.
    pub tables: Waited,
    /// Connectors' answers waiting for room to be decoded.
    pub control: Waited,
    /// Reads waiting for a slot among those what reads keep is divided among.
    pub reads: Waited,
}

/// How many requests waited, and for how long together.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Waited {
    /// Requests that waited.
    pub count: u64,
    /// The time they waited, summed.
    pub time: Duration,
}

impl Waited {
    /// Notes one more wait, of `time`.
    pub(crate) fn add(&mut self, time: Duration) {
        self.count = self.count.saturating_add(1);
        self.time = self.time.saturating_add(time);
    }
}

impl Commits {
    /// Notes one more commit that `trigger` made due.
    pub(crate) fn add(&mut self, trigger: Trigger) {
        let count = match trigger {
            Trigger::Interval => &mut self.interval,
            Trigger::Size => &mut self.size,
            Trigger::Cursors => &mut self.cursors,
            Trigger::End => &mut self.end,
        };
        *count = count.saturating_add(1);
    }
}

impl LaneCounters {
    /// Adds what `other` counted.
    pub(crate) fn add(&mut self, other: &Self) {
        self.blocked = self.blocked.saturating_add(other.blocked);
        self.writing = self.writing.saturating_add(other.writing);
        self.flushing = self.flushing.saturating_add(other.flushing);
        self.flushes.add(&other.flushes);
    }
}

impl Flushes {
    /// Adds what `other` counted.
    pub(crate) fn add(&mut self, other: &Self) {
        self.pressure = self.pressure.saturating_add(other.pressure);
        self.commit = self.commit.saturating_add(other.commit);
        self.retired = self.retired.saturating_add(other.retired);
    }

    /// Counts one more flush `flush` asked for.
    pub(crate) fn count(&mut self, flush: Flush) {
        let count = match flush {
            Flush::Pressure => &mut self.pressure,
            Flush::Commit => &mut self.commit,
            Flush::Retirement => &mut self.retired,
        };
        *count = count.saturating_add(1);
    }
}

impl CommitPhases {
    /// Adds what `other` took.
    pub(crate) fn add(&mut self, other: &Self) {
        self.flush = self.flush.saturating_add(other.flush);
        self.commit = self.commit.saturating_add(other.commit);
        self.ack = self.ack.saturating_add(other.ack);
    }
}

impl Counters {
    /// What lane `lane` counted, the lanes before it listed as none where they counted nothing.
    pub(crate) fn lane(&mut self, lane: usize) -> &mut LaneCounters {
        if self.lanes.len() <= lane {
            self.lanes.resize(lane + 1, LaneCounters::default());
        }
        &mut self.lanes[lane]
    }
}

/// Where a run's work writes its counters as it goes, and its report reads them once it ends.
#[derive(Debug, Default)]
pub(crate) struct Tally(Mutex<Counters>);

impl Tally {
    /// Counts what `count` writes.
    pub(crate) fn add(&self, count: impl FnOnce(&mut Counters)) {
        count(&mut self.0.lock());
    }

    /// What was counted.
    pub(crate) fn counters(&self) -> Counters {
        self.0.lock().clone()
    }
}
