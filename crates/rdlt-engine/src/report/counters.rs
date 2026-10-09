//! What a run counts and times as it works: written as it happens, summed into its report, and
//! read by no decision.

use std::time::Duration;

use serde::Serialize;

/// What a run waited for and spent its time on, across its attempts, measured on its
/// environment's clock.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Counters {
    /// The run's waits for room in its memory budget, by share.
    pub waits: Waits,
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
