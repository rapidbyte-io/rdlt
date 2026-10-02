//! The slots partitions read in: every read holds one, so no more reads keep bytes than what
//! reads keep is divided among.

use std::num::NonZeroUsize;
use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, SemaphorePermit};

use super::PartitionJob;
use crate::budget::MemoryBudget;
use crate::error::Error;
use crate::limits::PARTITIONS_TOO_FEW;

/// The slots of an attempt's reads, and the places among them a followed unbounded read takes.
///
/// A followed unbounded read holds its slot for as long as the run, so fewer of them than slots
/// read at once: the other reads always have a slot to take in turn.
#[derive(Clone, Debug)]
pub(crate) struct Slots {
    reads: Arc<Semaphore>,
    endless: Arc<Semaphore>,
    /// How many followed unbounded reads may read at once.
    places: usize,
}

impl Slots {
    /// The slots of `partitions` reads at once.
    pub(crate) fn new(partitions: NonZeroUsize) -> Self {
        Self {
            reads: Arc::new(Semaphore::new(partitions.get())),
            endless: Arc::new(Semaphore::new(partitions.get() - 1)),
            places: partitions.get() - 1,
        }
    }

    /// For a followed unbounded read, one of the places such reads have; nothing for another.
    ///
    /// # Errors
    ///
    /// A `Config` error coded `partitions_too_few` where as many followed unbounded reads run as
    /// there are slots less one: the slots must outnumber them.
    pub(super) fn endless(
        &self,
        job: &PartitionJob,
    ) -> Result<Option<OwnedSemaphorePermit>, Error> {
        if !(job.follow && job.partition.is_unbounded()) {
            return Ok(None);
        }
        let place = Arc::clone(&self.endless).try_acquire_owned();
        place.map(Some).map_err(|_| {
            Error::config(format!(
                "stream {}: partition {} would be one more followed unbounded read than the {} \
                 the run's partitions leave room for: partitions must be more than the \
                 unbounded partitions a following run reads",
                job.stream,
                job.partition.id(),
                self.places
            ))
            .with_code(PARTITIONS_TOO_FEW)
            .with_stream(&job.stream)
        })
    }

    /// A slot, waited for as long as `budget` waits for bytes.
    ///
    /// # Errors
    ///
    /// The budget's error, naming what reads keep, once the wait reaches its deadline.
    pub(super) async fn read(
        &self,
        budget: &MemoryBudget,
        job: &PartitionJob,
    ) -> Result<SemaphorePermit<'_>, Error> {
        let slot = budget.read_slot(self.reads.acquire()).await;
        let slot = slot.map_err(|exhausted| Error::memory(exhausted).with_stream(&job.stream))?;
        slot.map_err(|_| Error::internal("partition slots closed"))
    }
}
