//! What one connection's plans named and what its reads did meanwhile, which the oracle holds the
//! connection's run to.

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};

use parking_lot::Mutex;

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
    /// The number of the last plan that stopped naming each partition.
    dropped: BTreeMap<(usize, usize), u64>,
    /// How many reads of each partition run.
    reading: BTreeMap<(usize, usize), usize>,
    /// What each running read sent once a plan answered since it began dropped its partition, by
    /// the read's number.
    beyond: BTreeMap<u64, u64>,
    /// The furthest checkpoint the engine took from a read of each partition.
    taken: BTreeMap<(usize, usize), u64>,
    findings: Findings,
}

/// What a connection's reads did beside what its plans named.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Findings {
    /// Whether a plan named other partitions of a stream than the plan before it.
    pub(crate) replanned: bool,
    /// The partitions two reads read at once.
    pub(crate) doubled: BTreeSet<(usize, usize)>,
    /// The most pushes and checkpoints the engine took from one read of each partition once a
    /// plan answered since the read began stopped naming the partition.
    pub(crate) late: BTreeMap<(usize, usize), u64>,
}

impl Plans {
    /// Notes that a plan of stream `stream` named the partitions `named`.
    pub(crate) fn answered(&self, stream: usize, named: BTreeSet<usize>) {
        let mut guard = self.0.lock();
        let planned = &mut *guard;
        planned.answered += 1;
        if let Some(before) = planned.named.get(&stream) {
            planned.findings.replanned |= *before != named;
            for gone in before.difference(&named) {
                planned.dropped.insert((stream, *gone), planned.answered);
            }
        }
        planned.named.insert(stream, named);
    }

    /// A read of partition `partition` of stream `stream` that begins now, counted among the
    /// connection's until it drops.
    pub(crate) fn read(&self, stream: usize, partition: usize) -> PlannedRead<'_> {
        let mut guard = self.0.lock();
        let planned = &mut *guard;
        let key = (stream, partition);
        let running = planned.reading.entry(key).or_default();
        *running += 1;
        if *running > 1 {
            planned.findings.doubled.insert(key);
        }
        planned.began += 1;
        PlannedRead {
            plans: self,
            key,
            number: planned.began,
            since: planned.answered,
        }
    }

    /// What the connection's reads did beside what its plans named.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the oracle reads it once it judges a run against the records"
        )
    )]
    pub(crate) fn findings(&self) -> Findings {
        self.0.lock().findings.clone()
    }

    /// The furthest checkpoint the engine took from a read of each partition a plan stopped
    /// naming.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the oracle reads it once it judges a run against the records"
        )
    )]
    pub(crate) fn drained(&self) -> Vec<((usize, usize), u64)> {
        let planned = self.0.lock();
        planned
            .taken
            .iter()
            .filter(|(key, _)| planned.dropped.contains_key(*key))
            .map(|(key, next)| (*key, *next))
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
        {
            let mut planned = self.plans.0.lock();
            let taken = planned.taken.entry(self.key).or_default();
            *taken = (*taken).max(next);
        }
        self.pushed();
    }
}

impl Drop for PlannedRead<'_> {
    fn drop(&mut self) {
        let mut guard = self.plans.0.lock();
        let planned = &mut *guard;
        planned.beyond.remove(&self.number);
        if let Some(running) = planned.reading.get_mut(&self.key) {
            *running = running.saturating_sub(1);
        }
    }
}
