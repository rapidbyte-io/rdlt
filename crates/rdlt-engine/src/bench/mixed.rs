//! The passthrough workload from streams of two kinds in one pipeline: those whose source can
//! read them again and the last, which it cannot, every stream told its cursor once its commit
//! lands; and the same pipeline with every stream readable again.

#[cfg(test)]
mod tests;

use std::num::NonZeroUsize;

use arrow_array::RecordBatch;
use rdlt_connector::ReadMode;

use super::replay::mixed_replay;
use super::runner::{Runner, streams};
use super::{Cadence, Cell, Passthrough, Replayed, Sinking, sink};
use crate::{ComputePoolError, Cores, EngineConfig, Report};

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

    /// The batches of `cell`, the last of whose streams its source cannot read again; on a
    /// runtime of `cores`' workers and an engine within `cores` and the default budget.
    ///
    /// # Errors
    ///
    /// When the compute pool's threads do not start.
    ///
    /// # Panics
    ///
    /// Panics where `cell` has a single stream, which leaves no stream of the other kind.
    pub fn new(cores: Cores, cell: Cell) -> Result<Self, ComputePoolError> {
        assert!(
            cell.streams.get() > 1,
            "a mix of streams needs two at least"
        );
        let mut plans = streams(cell.streams);
        if let Some(last) = plans.pop() {
            plans.push(last.read(ReadMode::Incremental));
        }
        let memory = EngineConfig::default().memory().get();
        let runner = Runner::within(cores, "mixed", plans, cell.commit_rows(), memory)?;
        Ok(Self::of(runner, cell, 1))
    }

    /// The batches of `cell` as [`Mixed::new`] makes them, from streams its source can each
    /// read again.
    ///
    /// # Errors
    ///
    /// When the compute pool's threads do not start.
    pub fn replayable(cores: Cores, cell: Cell) -> Result<Self, ComputePoolError> {
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
}
