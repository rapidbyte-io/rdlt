//! Loads of mixed rows through the engine into a destination the caller connects: appends, and
//! merges of updates into the rows a load before them wrote, committing every so many rows or
//! once.

#[cfg(test)]
mod tests;

use std::num::NonZeroU64;
use std::sync::Arc;

use arrow_array::RecordBatch;
use rdlt_connector::{ConnectContext, Destination, DestinationFactory};
use serde_json::Value;

use super::runner::{Runner, stream};
use super::{Replayed, replay};
use crate::fixtures::events_of;
use crate::{ComputePoolError, Cores, Report, WriteMode};

/// Columns of each row a load writes.
const COLUMNS: usize = 10;

/// Rows of ten mixed columns, in batches, and an engine to load them.
#[derive(Debug)]
pub struct Loading {
    runner: Runner,
    batches: Vec<RecordBatch>,
}

/// Which rows a load writes, and how.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Load {
    /// Appends rows whose ids run from zero.
    Append,
    /// Merges by id rows whose ids run from zero.
    Merge,
    /// Merges by id over the rows a merge of as many wrote: their first tenth again, their
    /// values changed, and new rows for the rest, each batch holding both.
    Update,
}

impl Loading {
    /// `rows` rows of `load` in batches of `per_batch`, committed every `commit` rows or once,
    /// and a runtime of `cores`' workers and an engine within `cores` to load them.
    ///
    /// # Errors
    ///
    /// When the compute pool's threads do not start.
    ///
    /// # Panics
    ///
    /// Panics where the runtime does not start, or the rows do not fit in 63 bits.
    pub fn try_new(
        cores: Cores,
        load: Load,
        rows: u64,
        per_batch: NonZeroU64,
        commit: Option<NonZeroU64>,
    ) -> Result<Self, ComputePoolError> {
        let stream = match load {
            Load::Append => stream().write(WriteMode::Append),
            Load::Merge | Load::Update => stream().write(WriteMode::Merge).key(["id"]),
        };
        let rows = i64::try_from(rows).expect("rows fit in 63 bits");
        let ids = ids(load, rows);
        let per_batch = usize::try_from(per_batch.get()).unwrap_or(usize::MAX);
        let shift = i64::from(load == Load::Update);
        Ok(Self {
            runner: Runner::try_new(cores, "loading", stream, commit)?,
            batches: ids
                .chunks(per_batch)
                .map(|ids| events_of(ids, COLUMNS, shift))
                .collect(),
        })
    }

    /// The batches each run loads.
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    /// The rows each run loads.
    pub fn rows(&self) -> u64 {
        self.batches
            .iter()
            .map(|batch| batch.num_rows() as u64)
            .sum()
    }

    /// A destination `factory` connects with `config`.
    ///
    /// # Panics
    ///
    /// Panics where it does not connect.
    pub fn connect(&self, factory: &dyn DestinationFactory, config: Value) -> Arc<dyn Destination> {
        let connected = self
            .runner
            .block_on(factory.connect(config, ConnectContext::new()))
            .expect("the destination connects");
        Arc::from(connected)
    }

    /// Runs the engine from a source replaying the batches into `destination`; its report.
    ///
    /// # Panics
    ///
    /// Panics where the run fails or loads other rows than the batches hold.
    pub fn run(&self, destination: Arc<dyn Destination>) -> Report {
        let replayed = Replayed::Batches(self.batches.clone());
        let source = self.runner.block_on(replay("loading", replayed));
        let report = self.runner.load(source, destination);
        assert_eq!(report.rows, self.rows());
        report
    }
}

/// The ids of `rows` rows of `load`, in the order it pushes them.
fn ids(load: Load, rows: i64) -> Vec<i64> {
    match load {
        Load::Append | Load::Merge => (0..rows).collect(),
        Load::Update => {
            let updates = rows / 10;
            let mut ids: Vec<i64> = (0..updates).chain(rows..2 * rows - updates).collect();
            // A fixed scatter, so every batch holds updates and new rows alike.
            ids.sort_by_key(|id| (id * 2_654_435_761) % 1_000_003);
            ids
        }
    }
}
