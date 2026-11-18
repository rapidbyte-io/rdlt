//! The slots partitions read in: every read holds one, so no more read at once than their source
//! carries, and a read that is not followed holds a part of what reads keep beside it.

mod parts;
#[cfg(test)]
mod tests;

use std::num::NonZeroUsize;
use std::sync::Arc;

use rdlt_connector::{PartitionId, StreamName};
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
/// reads never all hold. A run whose plans name more is refused, and a read the plans name waits
/// for its place while one they no longer name still holds it.
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

/// A read's slot, with the place a followed unbounded read holds, or the part of what reads keep
/// another holds.
pub(super) struct Slot<'a> {
    _place: Option<OwnedSemaphorePermit>,
    _read: SemaphorePermit<'a>,
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

    /// Refuses `partition` of `stream`, read as a followed unbounded partition, where it is one of
    /// `followed` such reads its run's plans name, more than the places.
    ///
    /// # Errors
    ///
    /// A `Config` error where the followed unbounded reads are as many as the slots or more: the
    /// slots must outnumber them. It is coded `source_calls_exceeded` where the source's calls set
    /// the slots, and `partitions_too_few` where the run's partitions do.
    pub(crate) fn admit(
        &self,
        stream: &StreamName,
        partition: &PartitionId,
        followed: usize,
    ) -> Result<(), Error> {
        if followed > self.places {
            return Err(self.refused(stream, partition));
        }
        Ok(())
    }

    /// The refusal of `partition` of `stream`, a followed unbounded read beyond the places, naming
    /// what sets them.
    fn refused(&self, stream: &StreamName, partition: &PartitionId) -> Error {
        let places = self.places;
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

    /// A slot for `job`, for a followed unbounded read first a place, and for a read that is not
    /// followed a part of what reads keep, each at once where one is free, or once one frees, the
    /// waits for slots and parts counted by `budget`.
    ///
    /// No deadline ends a wait: a read that is not followed ends, freeing its slot and its part,
    /// and the followed unbounded reads leave a slot and a part, so each always comes round. A
    /// place is waited for only while a read the plans no longer name holds it, which its stop
    /// ends.
    pub(super) async fn read(
        &self,
        job: &PartitionJob,
        budget: &MemoryBudget,
    ) -> Result<Slot<'_>, Error> {
        let place = if job.followed() {
            let place = Arc::clone(&self.endless).acquire_owned().await;
            Some(place.map_err(|_| Error::internal("partition places closed"))?)
        } else {
            None
        };
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
            _place: place,
            _read: slot,
            _part: part,
        })
    }
}
