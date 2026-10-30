//! What a run is doing now, pulled through [`RunHandle::status`](crate::RunHandle::status): what
//! it is in the middle of and since when, which partitions read, wait or went quiet, what waits
//! for memory or for the destination, how fresh each stream is, and the run's report so far.
//!
//! No decision of the engine reads any of it, so pulling a status never changes what a run does.

mod clock;
mod live;
#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::time::SystemTime;

use rdlt_connector::Headroom;
use serde::Serialize;

use crate::report::{Report, RunStatus};

pub(crate) use clock::{RunClock, stamp};
pub(crate) use live::{Ended, LaneCalls, Live, Place};

/// What a run is doing now.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Status {
    /// What the run is in the middle of.
    pub phase: Phase,
    /// When it began it.
    pub since: SystemTime,
    /// The attempt's partitions, by where they stand.
    pub partitions: Partitions,
    /// The partition reading that has told the engine nothing for longest, and since when; none
    /// where no partition reads.
    pub quietest: Option<Quiet>,
    /// The oldest request waiting for room in each share of the memory budget, by when it began
    /// to wait.
    pub waiting: OpenWaits,
    /// Each lane's call into the destination in flight, by the lane's place; none for a lane
    /// between calls.
    pub lanes: Vec<Option<LaneCall>>,
    /// Each stream's freshness and how far its source says its reads are behind, by stream name.
    pub streams: BTreeMap<String, StreamStatus>,
    /// The run so far, as its report counts it: the attempt in progress listed as ending now.
    pub report: Report,
}

/// What a run is in the middle of.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    /// An attempt opens the destination and plans its streams.
    Opening,
    /// The attempt's partitions read.
    Reading,
    /// A commit is under way.
    Committing {
        /// The commit's place among its attempt's commits, the first being 1; a report of
        /// cursors owed after a commit that published nothing takes the next commit's.
        seq: u64,
        /// Its step.
        step: Step,
        /// How many times the destination was sent the commit, the try under way counted: none
        /// before its commit step, more than one where it answered a try transiently and the
        /// commit is sent again in place.
        tries: u32,
    },
    /// An attempt failed, and the run waits to try again.
    BackingOff {
        /// When the next attempt starts.
        until: SystemTime,
    },
    /// The run was asked to stop: the attempt commits what is sealed and ends.
    Stopping,
    /// The run ended.
    Ended {
        /// How.
        status: RunStatus,
    },
}

/// A step of a commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Step {
    /// Asking every partition that checkpoints when asked to do so, and waiting for their answers.
    Barrier,
    /// Flushing the lanes, so the destination holds every write the commit publishes.
    Flush,
    /// The destination's commit.
    Commit,
    /// Reporting the committed cursors to the source.
    Ack,
}

/// An attempt's partitions, by where they stand.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Partitions {
    /// Partitions reading.
    pub reading: u64,
    /// Partitions waiting for a slot to read in.
    pub waiting: u64,
    /// Partitions whose reads ended.
    pub ended: u64,
}

/// The partition reading that has told the engine nothing for longest.
///
/// A read that follows its source still tells where it stands while it waits for more, so a
/// quiet partition is a read that hangs or one with a long stretch between its checkpoints.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Quiet {
    /// Its stream.
    pub stream: String,
    /// The partition.
    pub partition: String,
    /// When it last told the engine anything: that it started, sealed, moved, signalled or ended.
    pub since: SystemTime,
}

/// The oldest request waiting for room in each share of a memory budget, by when it began to
/// wait; none in a share where none waits.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct OpenWaits {
    /// A push waiting to be admitted.
    pub intake: Option<SystemTime>,
    /// A frame of a served read waiting to be decoded.
    pub decode: Option<SystemTime>,
    /// A piece waiting to be lowered.
    pub work: Option<SystemTime>,
    /// A checkpoint's cursor waiting for a commit to make room for it.
    pub cursors: Option<SystemTime>,
    /// A table's records waiting for a commit to make room for them.
    pub tables: Option<SystemTime>,
    /// A connector's answer waiting to be decoded.
    pub control: Option<SystemTime>,
    /// A read waiting for a slot.
    pub reads: Option<SystemTime>,
}

/// A lane's call into the destination in flight.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct LaneCall {
    /// The call.
    pub call: Call,
    /// When it was made.
    pub since: SystemTime,
}

/// A lane's call into the destination.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Call {
    /// Opening a writer for a table.
    Open,
    /// Writing a batch.
    Write,
    /// Flushing a writer.
    Flush,
}

/// How fresh a stream is, and how far its source says its reads are behind.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct StreamStatus {
    /// When the last commit that landed its rows or its partitions' positions landed; none
    /// before one has.
    pub landed: Option<SystemTime>,
    /// How many records its reads were last behind their source's newest, over its partitions,
    /// as its source measures them; none where it never said.
    pub behind: Option<u64>,
    /// The least retention headroom its partitions last said, of each measure.
    pub headroom: Headroom,
}
