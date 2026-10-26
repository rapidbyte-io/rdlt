//! The slots partitions read in: every read holds one, so no more read at once than their source
//! carries, and a read that is not followed holds a part of what reads keep beside it.

mod parts;
#[cfg(test)]
mod tests;

use std::num::NonZeroUsize;
use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, SemaphorePermit};

pub(crate) use self::parts::Parts;
use super::PartitionJob;
use crate::budget::MemoryBudget;
use crate::cost::Keeping;
use crate::error::{Error, codes};
use crate::limits;

/// The slots of an attempt's reads, the places among them a followed unbounded read takes, and
/// the parts of what reads keep.
///
/// A followed unbounded read holds its slot for as long as the run, so fewer of them than slots
/// read at once: the other reads always have a slot to take in turn, and a part, which followed
/// reads never all hold.
#[derive(Clone, Debug)]
pub(crate) struct Slots {
    reads: Arc<Semaphore>,
    endless: Arc<Semaphore>,
    /// How many followed unbounded reads may read at once.
    places: usize,
    /// What sets how many slots there are.
    bound: Bound,
    /// One for each read that is not followed, the rest drawn on by the followed ones together.
    parts: Arc<Parts>,
}

/// A read's slot, and the part of what reads keep a read that is not followed holds with it.
pub(super) struct Slot<'a> {
    _slot: SemaphorePermit<'a>,
    _part: Option<OwnedSemaphorePermit>,
}

/// What sets an attempt's read slots.
#[derive(Clone, Copy, Debug)]
enum Bound {
    /// The partitions the run reads at once.
    Partitions,
    /// The calls the source carries at once, which leave fewer reads.
    Calls(NonZeroUsize),
}

impl Slots {
    /// The slots of `partitions` reads at once, or of fewer where a source carrying `calls` at
    /// once leaves fewer reads beside the engine's other call, and the parts of what `budget`'s
    /// reads keep.
    pub(crate) fn new(
        partitions: NonZeroUsize,
        calls: Option<NonZeroUsize>,
        budget: &MemoryBudget,
    ) -> Self {
        let (slots, bound) = match calls {
            Some(calls) if limits::carried(calls) < partitions => {
                (limits::carried(calls), Bound::Calls(calls))
            }
            _ => (partitions, Bound::Partitions),
        };
        Self {
            reads: Arc::new(Semaphore::new(slots.get())),
            endless: Arc::new(Semaphore::new(slots.get() - 1)),
            places: slots.get() - 1,
            bound,
            parts: Arc::new(Parts::new(budget.parts(), budget.part())),
        }
    }

    /// Where `job`'s read keeps what it keeps beside its events: in the part it holds, or for a
    /// followed unbounded read, in the parts no other read holds.
    pub(super) fn keeping(&self, job: &PartitionJob) -> Keeping {
        if job.followed() {
            Keeping::Pooled(Arc::clone(&self.parts))
        } else {
            Keeping::Part
        }
    }

    /// For a followed unbounded read, one of the places such reads have; nothing for another.
    ///
    /// # Errors
    ///
    /// A `Config` error where as many followed unbounded reads run as there are slots less one:
    /// the slots must outnumber them. It is coded `source_calls_exceeded` where the source's calls
    /// set the slots, and `partitions_too_few` where the run's partitions do.
    pub(super) fn endless(
        &self,
        job: &PartitionJob,
    ) -> Result<Option<OwnedSemaphorePermit>, Error> {
        if !job.followed() {
            return Ok(None);
        }
        let place = Arc::clone(&self.endless).try_acquire_owned();
        place.map(Some).map_err(|_| self.refused(job))
    }

    /// The refusal of `job`, a followed unbounded read beyond the places, naming what sets them.
    fn refused(&self, job: &PartitionJob) -> Error {
        let (stream, partition, places) = (&job.stream, job.partition.id(), self.places);
        let refused = match self.bound {
            Bound::Partitions => Error::config(format!(
                "stream {stream}: partition {partition} would be one more followed unbounded read \
                 than the {places} the run's partitions leave room for: partitions must be more \
                 than the unbounded partitions a following run reads"
            ))
            .with_code(codes::PARTITIONS_TOO_FEW),
            Bound::Calls(calls) => Error::config(format!(
                "stream {stream}: partition {partition} would be one more followed unbounded read \
                 than the {places} its source's {calls} calls at once leave room for, beside the \
                 engine's other call and a slot for every other read"
            ))
            .with_code(codes::SOURCE_CALLS_EXCEEDED),
        };
        refused.with_stream(stream)
    }

    /// A slot for `job`, and for a read that is not followed a part of what reads keep, each at
    /// once where one is free, or once one frees, the waits counted by `budget`.
    ///
    /// No deadline ends a wait: a read that is not followed ends, freeing its slot and its part,
    /// and the followed unbounded reads leave a slot and a part, so each always comes round.
    pub(super) async fn read(
        &self,
        job: &PartitionJob,
        budget: &MemoryBudget,
    ) -> Result<Slot<'_>, Error> {
        let slot = match self.reads.try_acquire() {
            Ok(slot) => slot,
            Err(_) => budget
                .read_slot(self.reads.acquire())
                .await
                .map_err(|_| Error::internal("partition slots closed"))?,
        };
        let part = if job.followed() {
            None
        } else if let Some(part) = self.parts.try_take() {
            Some(part)
        } else {
            Some(budget.read_slot(self.parts.take()).await)
        };
        Ok(Slot {
            _slot: slot,
            _part: part,
        })
    }
}
