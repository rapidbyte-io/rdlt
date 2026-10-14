//! The passthrough workload from streams of two kinds in one pipeline: those whose source can
//! read them again and the last, which it cannot, so that each load logs every stream's batches
//! ahead; and the same pipeline with every stream readable again, which logs nothing.

#[cfg(test)]
mod tests;

use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow_array::RecordBatch;
use rdlt_connector::ReadMode;

use super::connectors::{mixed_replay, share};
use super::runner::{Runner, streams};
use super::{Cadence, Cell, Passthrough, Replayed, Sinking, batch_frame, sink};
use crate::{ComputePoolError, Cores, EngineConfig, GrowthLimits, Report, WalStore};

/// The batches of a cell read from streams of two kinds, and an engine to move them.
#[derive(Debug)]
pub struct Mixed {
    runner: Runner,
    cell: Cell,
    unreplayable: usize,
    batches: Vec<RecordBatch>,
}

impl Mixed {
    /// The cell timed: batches of the matrix's middle size from one partition of each of two
    /// streams, committed every sixteenth of the rows, into the IPC sink.
    pub const CELL: Cell = Cell::of(
        8_000,
        NonZeroUsize::MIN,
        NonZeroUsize::new(2).expect("two is not zero"),
        Cadence::Sixteenths,
        Sinking::Ipc,
    );

    /// The batches of `cell`, the last of whose streams its source cannot read again, so that
    /// each load logs every batch into `store`; on a runtime of `cores`' workers and an engine
    /// within `cores` and the default budget.
    ///
    /// # Errors
    ///
    /// When the compute pool's threads do not start.
    ///
    /// # Panics
    ///
    /// Panics where `cell` has a single stream, which leaves no stream of the other kind.
    pub fn logged(
        cores: Cores,
        cell: Cell,
        store: Arc<dyn WalStore>,
    ) -> Result<Self, ComputePoolError> {
        assert!(
            cell.streams.get() > 1,
            "a mix of streams needs two at least"
        );
        let mut plans = streams(cell.streams);
        if let Some(last) = plans.pop() {
            plans.push(last.read(ReadMode::Incremental));
        }
        let log = (store, GrowthLimits::default().log_bytes());
        let memory = EngineConfig::default().memory().get();
        let runner = Runner::mixed(cores, ("mixed", plans), cell.commit_rows(), memory, log)?;
        Ok(Self::of(runner, cell, 1))
    }

    /// The batches of `cell` as [`Mixed::logged`] makes them, from streams its source can each
    /// read again, so that no load logs anything.
    ///
    /// # Errors
    ///
    /// When the compute pool's threads do not start.
    pub fn unlogged(cores: Cores, cell: Cell) -> Result<Self, ComputePoolError> {
        let memory = EngineConfig::default().memory().get();
        let runner = Runner::within(
            cores,
            "mixed",
            streams(cell.streams),
            cell.commit_rows(),
            memory,
        )?;
        Ok(Self::of(runner, cell, 0))
    }

    fn of(runner: Runner, cell: Cell, unreplayable: usize) -> Self {
        Self {
            runner,
            cell,
            unreplayable,
            batches: Passthrough::run_of(cell.batches, cell.rows),
        }
    }

    /// Runs the engine from a source replaying the batches into the cell's sink; its report.
    ///
    /// # Panics
    ///
    /// Panics where the run fails, or reports other rows than the batches hold.
    pub fn run(&self) -> Report {
        let replayed = Replayed::Batches(self.batches.clone());
        let (partitions, streams) = (self.cell.partitions, self.cell.streams);
        let source = self.runner.block_on(mixed_replay(
            "mixed",
            replayed,
            partitions,
            streams,
            self.unreplayable,
        ));
        let destination = self
            .runner
            .block_on(sink(self.cell.sinking, Passthrough::WRITERS));
        let report = self.runner.load(source, destination);
        let rows: usize = self.batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(report.rows, rows as u64);
        report
    }

    /// Bytes: the batch frames of every stream but the last, which the source of a logged run
    /// can read again, and of every stream, each batch's frame as a load logs it.
    ///
    /// # Panics
    ///
    /// Panics where a frame does not encode, which a valid batch always does.
    pub fn frame_bytes(&self) -> (u64, u64) {
        let (partitions, streams) = (self.cell.partitions, self.cell.streams);
        let of = |stream: usize| -> u64 {
            (0..partitions.get())
                .flat_map(|partition| {
                    share(self.batches.len(), (partitions, streams), stream, partition)
                })
                .map(|index| batch_frame(self.batches[index].clone()).len() as u64)
                .sum()
        };
        let last = streams.get() - 1;
        ((0..last).map(of).sum(), (0..=last).map(of).sum())
    }
}
