//! The passthrough workload: the same batches written to an Arrow IPC destination by a bare loop
//! and by the engine.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use arrow_array::RecordBatch;
use rdlt_connector::{Destination, SegmentId, Source, TableWriter};

use super::runner::{Runner, stream};
use super::{Replayed, SinkWriter, Sinking, ipc_sink, logical_bytes, replay};
use crate::fixtures::events;
use crate::{ComputePoolError, Cores};

/// Batches written to the same destination by a bare loop and by the engine, on a runtime and a
/// compute pool that split the same cores.
#[derive(Debug)]
pub struct Passthrough {
    runner: Runner,
    batches: Vec<RecordBatch>,
}

impl Passthrough {
    /// Rows per batch the bench moves: about 7 MB of ten mixed columns.
    pub const ROWS: u32 = 80_000;
    /// Batches the bench moves a run.
    pub const BATCHES: u32 = 64;
    /// Columns of each batch.
    pub const COLUMNS: usize = 10;

    /// `batches` batches of `rows` rows, their ids running on from one batch to the next, and a
    /// runtime of `cores`' workers and an engine within `cores` to move them.
    ///
    /// # Errors
    ///
    /// When the compute pool's threads do not start.
    ///
    /// # Panics
    ///
    /// Panics where the runtime does not start.
    pub fn try_new(cores: Cores, batches: u32, rows: u32) -> Result<Self, ComputePoolError> {
        Ok(Self {
            runner: Runner::try_new(cores, "passthrough", stream(), None)?,
            batches: (0..batches)
                .map(|index| Self::batch(i64::from(index) * i64::from(rows), rows))
                .collect(),
        })
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
            let mut writer = SinkWriter::new(Arc::default(), Sinking::Ipc);
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
        let replayed = Replayed::Batches(self.batches.clone());
        let source = self.block_on(replay("passthrough", replayed));
        self.run(source, self.block_on(ipc_sink()))
    }

    /// The logical bytes of `batches` batches of `rows` rows, known before they are made: every
    /// batch holds as many as the first.
    pub fn bytes(batches: u32, rows: u32) -> u64 {
        logical_bytes(&[Self::batch(0, rows)]) * u64::from(batches)
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

    /// Runs the engine from `source`, which pushes the batches, into `destination`; the rows it
    /// reports.
    ///
    /// # Panics
    ///
    /// Panics where the run fails, or reports other rows than the batches hold.
    pub fn run(&self, source: Arc<dyn Source>, destination: Arc<dyn Destination>) -> u64 {
        let report = self.runner.load(source, destination);
        assert_eq!(report.rows, self.rows());
        report.rows
    }
}
