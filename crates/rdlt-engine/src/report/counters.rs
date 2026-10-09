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
    /// The run's commits, by what made each due.
    pub commits: Commits,
    /// The time the run's commits took, phase by phase.
    pub phases: CommitPhases,
    /// What each lane of the run waited for and spent its time on, by the lane's place.
    pub lanes: Vec<LaneCounters>,
    /// What the run asked of its write-ahead log store, and the bytes it wrote and read.
    pub log: LogCounters,
}

/// What a run asked of its write-ahead log store, and the bytes that moved.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct LogCounters {
    /// Bytes appended to chunks staged, the carried among them.
    pub appended: u64,
    /// Bytes of open segments' frames copied out of older chunks into the chunk staged.
    pub carried: u64,
    /// Bytes read from published chunks: a replay's, and a carry's.
    pub read: u64,
    /// Chunks a full log published between commits to hold less.
    pub reliefs: u64,
    /// The requests made of the store, by operation.
    pub requests: StoreRequests,
}

/// Requests made of a write-ahead log store, by operation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct StoreRequests {
    /// Asks for the store's identity.
    pub identity: u64,
    /// Logs opened.
    pub open_log: u64,
    /// Chunks staged.
    pub stage: u64,
    /// Appends to chunks staged.
    pub append: u64,
    /// Chunks published.
    pub publish: u64,
    /// Chunks staged and discarded.
    pub discard: u64,
    /// Listings of a pipeline's logs.
    pub loads: u64,
    /// Listings of what removed logs left behind.
    pub leftovers: u64,
    /// Listings of a log's chunks.
    pub chunks: u64,
    /// Reads of a published chunk.
    pub read: u64,
    /// Deletions of what a log staged.
    pub remove_staged: u64,
    /// Deletions of a published chunk.
    pub remove: u64,
    /// Removals of a whole log.
    pub remove_log: u64,
}

/// Jobs on the compute pool, and how long they waited to start and ran.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct PoolCounters {
    /// Jobs run.
    pub jobs: u64,
    /// The time from each job's queueing to its start, summed.
    pub queued: Duration,
    /// The time each job ran, summed.
    pub running: Duration,
}

/// Commits that landed, by what made each due.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Commits {
    /// Made due by the commit policy's interval.
    pub interval: u64,
    /// Made due by the rows or bytes the commit policy allows.
    pub size: u64,
    /// Made due by checkpoints' cursors, waiting for room or holding half their share.
    pub cursors: u64,
    /// Made due by the load's log, full or holding what a commit should let go.
    pub log: u64,
    /// Of what was sealed once every partition reading had ended.
    pub end: u64,
}

/// The time commits that landed took, phase by phase, summed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct CommitPhases {
    /// Flushing the lanes, so the destination holds every write the commit publishes.
    pub flush: Duration,
    /// Listing the logs of the pipeline for the oldest commit a replay may repeat.
    pub horizon: Duration,
    /// Writing the commit into the load's log, where it keeps one.
    pub log: Duration,
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
    Log,
    End,
}

/// What one lane waited for and spent its time on.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct LaneCounters {
    /// The time partitions waited for room in the lane's queue, summed.
    pub blocked: Duration,
    /// The time its destination writers took to write batches, summed.
    pub writing: Duration,
    /// The time its destination writers took to flush, summed.
    pub flushing: Duration,
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
    /// Pieces waiting for room to be lowered.
    pub work: Waited,
    /// Checkpoints' cursors waiting for a commit to make room for them.
    pub cursors: Waited,
    /// The log's frames and staging waiting for room.
    pub log: Waited,
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
            Trigger::Log => &mut self.log,
            Trigger::End => &mut self.end,
        };
        *count = count.saturating_add(1);
    }
}

impl CommitPhases {
    /// Adds what `other` took.
    pub(crate) fn add(&mut self, other: &Self) {
        self.flush = self.flush.saturating_add(other.flush);
        self.horizon = self.horizon.saturating_add(other.horizon);
        self.log = self.log.saturating_add(other.log);
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
