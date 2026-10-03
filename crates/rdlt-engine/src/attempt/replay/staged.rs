//! The writers a replayed commit's batches are staged through, held open within the limit a
//! lane keeps, and what their batches hold charged to the memory budget until they flush.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow_array::RecordBatch;
use rdlt_connector::{DestinationWriter, GenerationId, SchemaVersion, SegmentId, TableRef};

use super::failed;
use crate::budget::{Denied, MemoryBudget, Reservation};
use crate::error::Error;
use crate::limits::REPLAY_EXCEEDS_BUDGET;

/// A destination table and the schema version a writer of it writes.
type Key = (Arc<str>, Option<GenerationId>, SchemaVersion);

/// The open writers, at most `limit` of them; a table's writer of a version older than one
/// written is closed, as is the writer written longest ago when another must open.
///
/// Each open writer is a call into a served destination, and replay stages the batches of every
/// table version its commit's attempt described. What a writer holds of the batches it was given
/// stays reserved until it flushes, and every writer flushes once the budget has no room for the
/// next batch, as a lane does under pressure.
pub(super) struct Staged {
    open: BTreeMap<Key, Open>,
    limit: NonZeroUsize,
    budget: MemoryBudget,
    /// Counts the writes, so the writer written longest ago is found.
    writes: u64,
}

/// An open writer, the count of writes when it was last written, and what the batches it holds
/// reserved.
struct Open {
    writer: Box<dyn DestinationWriter>,
    used: u64,
    held: Vec<Reservation>,
}

fn key(table: &TableRef) -> Key {
    (Arc::clone(&table.name), table.generation, table.version)
}

impl Staged {
    pub(super) fn new(limit: NonZeroUsize, budget: MemoryBudget) -> Self {
        Self {
            open: BTreeMap::new(),
            limit,
            budget,
            writes: 0,
        }
    }

    /// Reserves `bytes` of a batch about to be read or held: where the budget has no room, every
    /// writer flushes first, releasing what its batches held.
    ///
    /// # Errors
    ///
    /// `replay_exceeds_budget` for more than one request for lowering may take, which the
    /// memory a log was written under admitted and this engine's does not; a wait on the budget
    /// that reaches its deadline.
    pub(super) async fn reserve(&mut self, bytes: u64) -> Result<Reservation, Error> {
        if let Some(reserved) = self.budget.try_acquire_working(bytes) {
            return Ok(reserved);
        }
        self.flush_all().await?;
        self.budget
            .acquire_working(bytes)
            .await
            .map_err(|denied| match denied {
                Denied::Exhausted(exhausted) => Error::memory(exhausted),
                Denied::TooLarge(large) => Error::wal(format!(
                    "a logged batch takes more than the memory budget lets a replay hold: {large}"
                ))
                .with_code(REPLAY_EXCEEDS_BUDGET),
            })
    }

    /// Writes `batch` of `segment` with `table`'s writer, opened by `open` where none is.
    ///
    /// Its table's writers of older versions close first, each flushed so what it staged stays
    /// staged.
    pub(super) async fn write<Opening>(
        &mut self,
        table: &TableRef,
        open: impl FnOnce() -> Opening,
        (segment, batch): (SegmentId, RecordBatch),
        held: Reservation,
    ) -> Result<(), Error>
    where
        Opening: Future<Output = Result<Box<dyn DestinationWriter>, Error>>,
    {
        self.writes = self.writes.saturating_add(1);
        let key = key(table);
        let older: Vec<Key> = self
            .open
            .keys()
            .filter(|open| open.0 == key.0 && open.1 == key.1 && open.2 < key.2)
            .cloned()
            .collect();
        for older in older {
            self.close(&older).await?;
        }
        if !self.open.contains_key(&key) && self.open.len() >= self.limit.get() {
            let oldest = self.open.iter().min_by_key(|(_, open)| open.used);
            if let Some(oldest) = oldest.map(|(key, _)| key.clone()) {
                self.close(&oldest).await?;
            }
        }
        let used = self.writes;
        let writer = match self.open.entry(key) {
            Entry::Occupied(open) => {
                let open = open.into_mut();
                open.used = used;
                open
            }
            Entry::Vacant(vacant) => vacant.insert(Open {
                writer: open().await?,
                used,
                held: Vec::new(),
            }),
        };
        writer.held.push(held);
        writer
            .writer
            .write(segment, batch)
            .await
            .map_err(|error| failed("staging a replayed batch", error))
    }

    /// Flushes and closes every open writer.
    pub(super) async fn flush(mut self) -> Result<(), Error> {
        self.flush_all().await
    }

    /// Flushes and closes every open writer, releasing what their batches held.
    async fn flush_all(&mut self) -> Result<(), Error> {
        let keys: Vec<Key> = self.open.keys().cloned().collect();
        for key in keys {
            self.close(&key).await?;
        }
        Ok(())
    }

    async fn close(&mut self, key: &Key) -> Result<(), Error> {
        let Some(mut open) = self.open.remove(key) else {
            return Ok(());
        };
        // What it holds is released once it flushed, its writer closed with it.
        open.writer
            .flush()
            .await
            .map(drop)
            .map_err(|error| failed("staging replayed batches", error))
    }
}

#[cfg(test)]
mod tests;
