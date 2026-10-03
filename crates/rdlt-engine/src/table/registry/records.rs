//! What a commit records of the tables, reserved from the tables' share of the budget from the
//! schema change that makes it until the commit recording it lands.

use rdlt_connector::{SchemaVersion, StateChange, StateEntry};
use tokio_util::sync::CancellationToken;

use super::{TableView, Tables};
use crate::budget::{Denied, MemoryBudget, Reservation};
use crate::error::Error;
use crate::limits::{RECORDED, TABLE_EXCEEDS_BUDGET};

/// The budget a table's records are reserved from, and when a wait for room ends.
#[derive(Debug)]
pub(super) struct Charge {
    budget: MemoryBudget,
    cancel: CancellationToken,
}

/// The records a commit makes of `view`: its schema, and its names.
pub(super) fn records(view: &TableView) -> [StateChange; 2] {
    let path = view.table.path.clone();
    let schema = StateEntry::Schema {
        table: path.clone(),
        version: SchemaVersion(view.model.version),
        schema: view.model.schema(),
        exact: view.model.exact.clone(),
    };
    let names = StateEntry::Names {
        table: path,
        physical: std::sync::Arc::clone(&view.table.name),
        names: view.model.names.clone(),
    };
    [
        StateChange::Put(schema.to_record()),
        StateChange::Put(names.to_record()),
    ]
}

/// Bytes: what `changes` take in a commit's frame in the log, where each record's bytes are
/// written twice over.
pub(super) fn recorded_bytes(changes: &[StateChange]) -> u64 {
    let record = |change: &StateChange| match change {
        StateChange::Put(record) => record.key.len().saturating_add(record.value.len()),
        StateChange::Delete(key) => key.len(),
    };
    let bytes = changes
        .iter()
        .map(record)
        .fold(0_usize, usize::saturating_add);
    RECORDED.saturating_mul(u64::try_from(bytes).unwrap_or(u64::MAX))
}

impl Tables {
    /// From now on, each change of a table's schema reserves what the commit recording it makes
    /// from `budget` first.
    ///
    /// A wait for room ends when `cancel` fires. Only the first call counts.
    pub(crate) fn charge(&self, budget: MemoryBudget, cancel: CancellationToken) {
        self.charge.set(Charge { budget, cancel }).ok();
    }

    /// Reserves what a commit records of `next`, the view `table` changes to, in place of what
    /// its last change reserved.
    ///
    /// # Errors
    ///
    /// A `Schema` error coded `table_exceeds_budget` where the records take more than the
    /// tables' share, before the destination sees the change; the budget's error where the wait
    /// reaches its deadline, and a cancelled one where the attempt ends.
    pub(super) async fn reserve_records(
        &self,
        table: usize,
        next: &TableView,
    ) -> Result<(), Error> {
        let Some(charge) = self.charge.get() else {
            return Ok(());
        };
        let slot = self.slot(table);
        let bytes = recorded_bytes(&records(next));
        // What the last change reserved is released first: the commit records only the latest.
        drop(slot.records.lock().take());
        let reserved = tokio::select! {
            biased;
            () = charge.cancel.cancelled() => return Err(Error::cancelled("the attempt was cancelled")),
            reserved = charge.budget.acquire_tables(bytes) => reserved,
        };
        let stream = &slot.resolver.lock().stream.clone();
        let reserved = reserved.map_err(|denied| match denied {
            Denied::Exhausted(exhausted) => Error::memory(exhausted).with_stream(stream),
            Denied::TooLarge(large) => Error::schema(format!(
                "stream {stream}: table {} records {bytes} bytes of its schema and names in a \
                 commit, more than the {} the memory budget's share for them holds",
                next.table.path, large.limit
            ))
            .with_code(TABLE_EXCEEDS_BUDGET)
            .with_stream(stream),
        })?;
        *slot.records.lock() = Some((next.model.revision, reserved));
        Ok(())
    }

    /// Releases what `table`'s changes reserved, where a commit now records its model at
    /// `revision`.
    pub(super) fn release_records(&self, table: usize, revision: u32) {
        let slot = self.slot(table);
        let mut records = slot.records.lock();
        if records
            .as_ref()
            .is_some_and(|(reserved, _)| *reserved <= revision)
        {
            drop(records.take());
        }
    }
}

/// What holds a table's records: the model revision they were reserved for, and the bytes.
pub(super) type Held = Option<(u32, Reservation)>;
