//! What a partition tells the coordinator of which only the newest matters: kept a partition,
//! where a queue would hold one of every checkpoint and signal a source sends.
//!
//! A checkpoint that sealed no rows moves its partition's position and nothing else, so the
//! next checkpoint replaces it. How far a read is behind, and that a stream's partitions changed,
//! are states. Each partition has at most one of each waiting, and a message in the
//! coordinator's queue saying so.
//!
//! A seal of no rows must reach the coordinator after every seal with rows its partition sent
//! before it: taken sooner, a commit could record its position without the rows before it. So
//! each seal with rows starts a new epoch, and a message takes a waiting seal only in the epoch
//! it was queued in; a seal of no rows that follows has a message of its own, queued after.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;

use parking_lot::Mutex;

use super::Seal;

/// What each partition last said, until the coordinator takes it.
#[derive(Debug, Default)]
pub(crate) struct Latest {
    partitions: Mutex<BTreeMap<usize, Waiting>>,
}

/// What one partition last said and the coordinator has not taken.
#[derive(Debug, Default)]
struct Waiting {
    /// The newest seal of no rows.
    seal: Option<Seal>,
    /// How many seals with rows the partition has sent.
    epoch: u64,
    /// The epoch of the queued message telling the coordinator a seal waits.
    seal_told: Option<u64>,
    /// How many records the read last said it is behind.
    behind: Option<u64>,
    /// Whether the source said the stream's partitions changed.
    replan: bool,
    /// Whether a message telling the coordinator of a signal is in its queue.
    signal_told: bool,
}

impl Latest {
    fn with<T>(&self, partition: usize, change: impl FnOnce(&mut Waiting) -> T) -> T {
        change(self.partitions.lock().entry(partition).or_default())
    }

    /// Keeps `seal`, which sealed no rows, as its partition's newest, in place of any that
    /// waits, whose barrier it answers too; returns the epoch to tell the coordinator of, where
    /// no message queued in it says a seal waits.
    pub(crate) fn moved(&self, mut seal: Seal) -> Option<u64> {
        self.with(seal.partition, |waiting| {
            let answered = waiting.seal.take().and_then(|replaced| replaced.answers);
            seal.answers = seal.answers.max(answered);
            waiting.seal = Some(seal);
            let epoch = Some(waiting.epoch);
            (waiting.seal_told != epoch).then(|| {
                waiting.seal_told = epoch;
                waiting.epoch
            })
        })
    }

    /// Starts `partition`'s next epoch, as it sends a seal with rows: drops the seal of no rows
    /// it has waiting, whose position the seal with rows passes, and returns the barrier that
    /// one answered.
    pub(crate) fn superseded(&self, partition: usize) -> Option<u64> {
        self.with(partition, |waiting| {
            waiting.epoch += 1;
            waiting.seal.take().and_then(|replaced| replaced.answers)
        })
    }

    /// Takes the seal of no rows `partition` has waiting, for a message queued in `epoch`; none
    /// where the partition sent a seal with rows since, as a later message then says what waits.
    pub(crate) fn seal(&self, partition: usize, epoch: u64) -> Option<Seal> {
        self.with(partition, |waiting| {
            if waiting.epoch != epoch {
                return None;
            }
            waiting.seal_told = None;
            waiting.seal.take()
        })
    }

    /// Keeps `records` as how far `partition`'s read is behind; returns whether the coordinator
    /// must be told a signal waits.
    pub(crate) fn behind(&self, partition: usize, records: u64) -> bool {
        self.with(partition, |waiting| {
            waiting.behind = Some(records);
            !std::mem::replace(&mut waiting.signal_told, true)
        })
    }

    /// Keeps that `partition`'s source said its stream's partitions changed; returns whether the
    /// coordinator must be told a signal waits.
    pub(crate) fn replan(&self, partition: usize) -> bool {
        self.with(partition, |waiting| {
            waiting.replan = true;
            !std::mem::replace(&mut waiting.signal_told, true)
        })
    }

    /// Takes the signals `partition` has waiting: how far its read is behind, and whether its
    /// stream's partitions changed.
    pub(crate) fn signals(&self, partition: usize) -> (Option<u64>, bool) {
        self.with(partition, |waiting| {
            waiting.signal_told = false;
            (waiting.behind.take(), std::mem::take(&mut waiting.replan))
        })
    }
}
