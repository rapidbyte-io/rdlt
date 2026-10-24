//! Loads of mixed rows through the engine into a destination the caller connects: appends, and
//! merges of updates into the rows a load before them wrote, committing every so many rows or
//! once; and appends from a source that cannot read its rows again, logged ahead.

#[cfg(test)]
mod tests;

use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;

use arrow_array::RecordBatch;
use rdlt_connector::{ConnectContext, Destination, DestinationFactory, ReadMode};
use serde_json::Value;

use super::Replayed;
use super::replay::mixed_replay;
use super::runner::{Runner, stream};
use crate::fixtures::events_of;
use crate::{ComputePoolError, Cores, GrowthLimits, Report, WalStore, WriteMode};

/// Columns of each row a load writes.
const COLUMNS: usize = 10;

/// Rows of ten mixed columns, in batches, and an engine to load them.
#[derive(Debug)]
pub struct Loading {
    runner: Runner,
    batches: Vec<RecordBatch>,
    /// The load's streams its source cannot read again: none, or its one stream.
    unreplayable: usize,
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
        Ok(Self {
            runner: Runner::try_new(cores, "loading", stream, commit)?,
            batches: batches(load, rows, per_batch),
            unreplayable: 0,
        })
    }

    /// `rows` rows appended in batches of `per_batch`, committed every `commit` rows or once,
    /// from a source that cannot read them again, so the engine logs each load ahead into
    /// `store`; and a runtime of `cores`' workers and an engine within `cores` to load them.
    ///
    /// # Errors
    ///
    /// When the compute pool's threads do not start.
    ///
    /// # Panics
    ///
    /// Panics where the runtime does not start, or the rows do not fit in 63 bits.
    pub fn logged(
        cores: Cores,
        rows: u64,
        per_batch: NonZeroU64,
        commit: Option<NonZeroU64>,
        store: Arc<dyn WalStore>,
    ) -> Result<Self, ComputePoolError> {
        let stream = stream()
            .write(WriteMode::Append)
            .read(ReadMode::Incremental);
        let log = (store, GrowthLimits::default().log_bytes());
        let streams = ("loading", vec![stream]);
        Ok(Self {
            runner: Runner::mixed(cores, streams, commit, Runner::MEMORY, log)?,
            batches: batches(Load::Append, rows, per_batch),
            unreplayable: 1,
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
        let one = NonZeroUsize::MIN;
        let replay = mixed_replay("loading", replayed, one, one, self.unreplayable);
        let source = self.runner.block_on(replay);
        let report = self.runner.load(source, destination);
        assert_eq!(report.rows, self.rows());
        report
    }
}

/// `rows` rows of `load` in batches of `per_batch`.
fn batches(load: Load, rows: u64, per_batch: NonZeroU64) -> Vec<RecordBatch> {
    let rows = i64::try_from(rows).expect("rows fit in 63 bits");
    let per_batch = usize::try_from(per_batch.get()).unwrap_or(usize::MAX);
    let shift = i64::from(load == Load::Update);
    ids(load, rows)
        .chunks(per_batch)
        .map(|ids| events_of(ids, COLUMNS, shift))
        .collect()
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
