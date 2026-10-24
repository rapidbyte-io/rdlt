//! The passthrough workload: the batches of a cell of the matrix written to an Arrow IPC
//! destination, or dropped, by a bare loop and by the engine.

#[cfg(test)]
mod tests;

use std::num::NonZeroU16;
use std::sync::Arc;

use arrow_array::RecordBatch;
use rdlt_connector::{Destination, SegmentId, Source, TableWriter};

use super::replay::connect_replay;
use super::runner::{Runner, streams};
use super::{Cell, Replayed, SinkWriter, logical_bytes, register, sink};
use crate::fixtures::events;
use crate::{ComputePoolError, Cores, EngineConfig, Report};

/// Batches of a cell of the matrix written to the same destination by a bare loop and by the
/// engine, on a runtime and a compute pool that split the same cores.
#[derive(Debug)]
pub struct Passthrough {
    runner: Runner,
    cell: Cell,
    writers: NonZeroU16,
    batches: Vec<RecordBatch>,
}

impl Passthrough {
    /// Columns of each batch.
    pub const COLUMNS: usize = 10;
    /// Writers the engine's sink declares it runs at once, so a run has a lane a core.
    pub const WRITERS: NonZeroU16 = NonZeroU16::new(16).expect("not zero");

    /// The batches of `cell`, their ids running on from one batch to the next, and a runtime of
    /// `cores`' workers and an engine within `cores` and the default budget to move them.
    ///
    /// # Errors
    ///
    /// When the compute pool's threads do not start.
    ///
    /// # Panics
    ///
    /// Panics where the runtime does not start.
    pub fn try_new(cores: Cores, cell: Cell) -> Result<Self, ComputePoolError> {
        let memory = EngineConfig::default().memory().get();
        Self::within(cores, cell, memory, Self::WRITERS)
    }

    /// The workload of `cell` as [`Passthrough::try_new`] makes it, the engine within a budget of
    /// 1 GiB and writing through a single writer, as the bound on the paired cell's overhead is
    /// stated.
    ///
    /// # Errors
    ///
    /// When the compute pool's threads do not start.
    ///
    /// # Panics
    ///
    /// Panics where the runtime does not start.
    pub fn paired(cores: Cores, cell: Cell) -> Result<Self, ComputePoolError> {
        Self::within(cores, cell, Runner::MEMORY, NonZeroU16::MIN)
    }

    fn within(
        cores: Cores,
        cell: Cell,
        memory: u64,
        writers: NonZeroU16,
    ) -> Result<Self, ComputePoolError> {
        let streams = streams(cell.streams);
        Ok(Self {
            runner: Runner::within(cores, "passthrough", streams, cell.commit_rows(), memory)?,
            cell,
            writers,
            batches: Self::run_of(cell.batches, cell.rows),
        })
    }

    /// The cell the workload moves.
    pub fn cell(&self) -> Cell {
        self.cell
    }

    /// `batches` batches of `rows` rows of the workload's columns, their ids running on from one
    /// batch to the next.
    pub(super) fn run_of(batches: u32, rows: u32) -> Vec<RecordBatch> {
        (0..batches)
            .map(|index| Self::batch(i64::from(index) * i64::from(rows), rows))
            .collect()
    }

    /// One batch of `rows` rows of the workload's columns, its ids running from `first`: the
    /// batch a workload of `rows` rows a batch moves at that id.
    pub fn batch(first: i64, rows: u32) -> RecordBatch {
        events(first, rows, Self::COLUMNS)
    }

    /// The batches the workload moves.
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    /// Writes the batches to the destination's writer one after another; the rows written.
    ///
    /// # Panics
    ///
    /// Panics where the writer refuses a batch.
    pub fn bare_loop(&self) -> u64 {
        self.runner.block_on(async {
            let mut writer = SinkWriter::new(Arc::default(), self.cell.sinking);
            for batch in &self.batches {
                writer
                    .write(SegmentId(1), batch.clone())
                    .await
                    .expect("the sink writes");
            }
            writer.flush().await.expect("the sink flushes").rows
        })
    }

    /// Runs the engine from a source replaying the batches into the destination; the rows it
    /// reports.
    ///
    /// # Panics
    ///
    /// Panics where the run fails.
    pub fn engine_run(&self) -> u64 {
        self.engine_report().rows
    }

    /// Runs the engine as [`Passthrough::engine_run`] does; its report.
    ///
    /// # Panics
    ///
    /// Panics where the run fails, or reports other rows than the batches hold.
    pub fn engine_report(&self) -> Report {
        let source = self.block_on(self.source());
        let destination = self.block_on(sink(self.cell.sinking, self.writers));
        self.run(source, destination)
    }

    /// A source replaying the batches from the cell's partitions and streams, checkpointing as
    /// the cell says.
    pub async fn source(&self) -> Arc<dyn Source> {
        register("passthrough", Replayed::Batches(self.batches.clone()));
        connect_replay(self.cell.replay_config("passthrough")).await
    }

    /// Bytes: the logical bytes of a run of `cell`, known before its batches are made: every
    /// batch holds as many as the first.
    pub fn bytes(cell: Cell) -> u64 {
        logical_bytes(&[Self::batch(0, cell.rows)]) * u64::from(cell.batches)
    }

    /// The rows of the batches.
    pub fn rows(&self) -> u64 {
        self.batches
            .iter()
            .map(|batch| batch.num_rows() as u64)
            .sum()
    }

    /// Runs `future` on the workload's runtime, where connectors served from this process run.
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.runner.block_on(future)
    }

    /// Runs the engine from `source`, which pushes the batches, into `destination`; its report.
    ///
    /// # Panics
    ///
    /// Panics where the run fails, or reports other rows than the batches hold.
    pub fn run(&self, source: Arc<dyn Source>, destination: Arc<dyn Destination>) -> Report {
        let report = self.runner.load(source, destination);
        assert_eq!(report.rows, self.rows());
        report
    }
}
