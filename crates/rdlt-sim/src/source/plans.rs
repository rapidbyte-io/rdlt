//! What one connection's plans named and what its reads did meanwhile, which the oracle holds the
//! connection's run to.

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use parking_lot::Mutex;
use tokio::time::Instant;

/// One connection's plans and reads, each partition by its stream's index and its own.
#[derive(Debug, Default)]
pub(crate) struct Plans(Mutex<Planned>);

#[derive(Debug, Default)]
struct Planned {
    /// How many plans the connection answered.
    answered: u64,
    /// How many reads began on it.
    began: u64,
    /// The partitions each stream's last plan named.
    named: BTreeMap<usize, BTreeSet<usize>>,
    /// The number of the last plan that stopped naming each partition, kept once a later plan
    /// names it again: a read that plan stopped still answers to it.
    dropped: BTreeMap<(usize, usize), u64>,
    /// How many reads of each partition run.
    reading: BTreeMap<(usize, usize), usize>,
    /// What each running read sent once a plan answered since it began dropped its partition, by
    /// the read's number.
    beyond: BTreeMap<u64, u64>,
    /// The furthest checkpoint the engine took from the reads of each partition, by how many plans
    /// the connection had answered when they began.
    taken: BTreeMap<(usize, usize), BTreeMap<u64, u64>>,
    /// Since when each partition its stream's last plan names has had no read running, where it
    /// has none: since it was named, or since its last read ended.
    idle: BTreeMap<(usize, usize), Instant>,
    /// When each partition its stream's last plan does not name stopped being named.
    gone: BTreeMap<(usize, usize), Instant>,
    findings: Findings,
}

/// What a connection's reads did beside what its plans named.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Findings {
    /// Whether a plan named other partitions of a stream than the plan before it.
    pub(crate) replanned: bool,
    /// Whether a plan of a stream stopped naming a partition a read of which ran, and named one
    /// the plan before it did not.
    pub(crate) swapped: bool,
    /// The partitions two reads read at once.
    pub(crate) doubled: BTreeSet<(usize, usize)>,
    /// The most pushes and checkpoints the engine took from one read of each partition once a
    /// plan answered since the read began stopped naming the partition.
    pub(crate) late: BTreeMap<(usize, usize), u64>,
    /// The longest each partition its plans named waited for a read to begin, of the waits that
    /// ended.
    pub(crate) waited: BTreeMap<(usize, usize), Duration>,
}

impl Plans {
    /// Notes that a plan of stream `stream` named the partitions `named`.
    pub(crate) fn answered(&self, stream: usize, named: BTreeSet<usize>) {
        let now = Instant::now();
        let mut guard = self.0.lock();
        let planned = &mut *guard;
        planned.answered += 1;
        let before = planned.named.remove(&stream);
        if let Some(before) = &before {
            planned.findings.replanned |= *before != named;
            let running = |partition: &usize| {
                let reads = planned.reading.get(&(stream, *partition));
                reads.is_some_and(|reads| *reads > 0)
            };
            let stopped = before.difference(&named).any(running);
            planned.findings.swapped |= stopped && !named.is_subset(before);
        }
        let before = before.unwrap_or_default();
        for gone in before.difference(&named) {
            let key = (stream, *gone);
            planned.dropped.insert(key, planned.answered);
            planned.idle.remove(&key);
            planned.gone.insert(key, now);
        }
        for partition in named.difference(&before) {
            let key = (stream, *partition);
            planned.gone.remove(&key);
            if planned
                .reading
                .get(&key)
                .is_none_or(|running| *running == 0)
            {
                planned.idle.entry(key).or_insert(now);
            }
        }
        planned.named.insert(stream, named);
    }

    /// A read of partition `partition` of stream `stream` that begins now, counted among the
    /// connection's until it drops.
    pub(crate) fn read(&self, stream: usize, partition: usize) -> PlannedRead<'_> {
        let now = Instant::now();
        let mut guard = self.0.lock();
        let planned = &mut *guard;
        let key = (stream, partition);
        let running = planned.reading.entry(key).or_default();
        *running += 1;
        if *running > 1 {
            planned.findings.doubled.insert(key);
        }
        if let Some(since) = planned.idle.remove(&key) {
            let waited = planned.findings.waited.entry(key).or_default();
            *waited = (*waited).max(now.saturating_duration_since(since));
        }
        planned.began += 1;
        PlannedRead {
            plans: self,
            key,
            number: planned.began,
            since: planned.answered,
            checkpointed: Mutex::default(),
        }
    }

    /// What the connection's reads did beside what its plans named.
    pub(crate) fn findings(&self) -> Findings {
        self.0.lock().findings.clone()
    }

    /// The furthest checkpoint the engine took from a read of each partition a plan stopped
    /// naming, of the reads begun before the last plan that stopped naming it: a read begun once
    /// the partition was named again answers to the run, not to the drop.
    pub(crate) fn drained(&self) -> Vec<((usize, usize), u64)> {
        let planned = self.0.lock();
        planned
            .dropped
            .iter()
            .filter_map(|(key, plan)| {
                let reads = planned.taken.get(key)?;
                let furthest = reads.range(..*plan).map(|(_, next)| *next).max()?;
                Some((*key, furthest))
            })
            .collect()
    }

    /// How long each partition its plans named waited for a read to begin, by `now`: the longest
    /// wait that ended, or the wait still open where that is longer; and each the source has named
    /// since as `named` says, of a stream its plans named, that no plan of it named since.
    pub(crate) fn waits(
        &self,
        now: Instant,
        named: &BTreeMap<(usize, usize), Instant>,
    ) -> BTreeMap<(usize, usize), Duration> {
        let planned = self.0.lock();
        let mut waits = planned.findings.waited.clone();
        let unplanned = named.iter().filter(|((stream, partition), _)| {
            let last = planned.named.get(stream);
            last.is_some_and(|last| !last.contains(partition))
        });
        for (key, since) in planned.idle.iter().chain(unplanned) {
            let waited = waits.entry(*key).or_default();
            *waited = (*waited).max(now.saturating_duration_since(*since));
        }
        waits
    }

    /// Each partition a plan stopped naming that no later plan named again: since when, and the
    /// furthest checkpoint the engine took from a read of it begun before.
    pub(crate) fn gone(&self) -> Vec<((usize, usize), Instant, u64)> {
        let drained: BTreeMap<(usize, usize), u64> = self.drained().into_iter().collect();
        let planned = self.0.lock();
        planned
            .gone
            .iter()
            .filter_map(|(key, since)| Some((*key, *since, *drained.get(key)?)))
            .collect()
    }
}

/// A read of one partition, counted among its connection's until it drops.
#[derive(Debug)]
pub(crate) struct PlannedRead<'a> {
    plans: &'a Plans,
    /// The partition, by its stream's index and its own.
    key: (usize, usize),
    /// The read's number among the connection's.
    number: u64,
    /// How many plans the connection had answered when the read began.
    since: u64,
    /// The offset of the row after the last checkpoint the engine took from the read, if any.
    checkpointed: Mutex<Option<u64>>,
}

impl PlannedRead<'_> {
    /// The index of the partition read.
    pub(crate) fn partition(&self) -> usize {
        self.key.1
    }

    /// Notes that the engine took a push or a checkpoint of the read's.
    pub(crate) fn pushed(&self) {
        let mut guard = self.plans.0.lock();
        let planned = &mut *guard;
        let dropped = planned.dropped.get(&self.key);
        if dropped.is_some_and(|plan| *plan > self.since) {
            let beyond = planned.beyond.entry(self.number).or_default();
            *beyond += 1;
            let late = planned.findings.late.entry(self.key).or_default();
            *late = (*late).max(*beyond);
        }
    }

    /// Notes that the engine took the read's checkpoint before the row at offset `next`.
    pub(crate) fn took(&self, next: u64) {
        *self.checkpointed.lock() = Some(next);
        {
            let mut planned = self.plans.0.lock();
            let reads = planned.taken.entry(self.key).or_default();
            let taken = reads.entry(self.since).or_default();
            *taken = (*taken).max(next);
        }
        self.pushed();
    }

    /// The offset of the row after the last checkpoint the engine took from the read, if any.
    pub(crate) fn checkpointed(&self) -> Option<u64> {
        *self.checkpointed.lock()
    }
}

impl Drop for PlannedRead<'_> {
    fn drop(&mut self) {
        let now = Instant::now();
        let mut guard = self.plans.0.lock();
        let planned = &mut *guard;
        planned.beyond.remove(&self.number);
        let (stream, partition) = self.key;
        let named = planned
            .named
            .get(&stream)
            .is_some_and(|named| named.contains(&partition));
        if let Some(running) = planned.reading.get_mut(&self.key) {
            *running = running.saturating_sub(1);
            if *running == 0 && named {
                planned.idle.entry(self.key).or_insert(now);
            }
        }
    }
}
