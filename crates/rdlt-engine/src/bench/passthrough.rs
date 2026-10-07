//! The passthrough workload: the same batches written to an Arrow IPC destination by a bare loop
//! and by the engine.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use arrow_array::RecordBatch;
use rdlt_connector::{PipelineId, SegmentId, StreamName, TableWriter};
use tokio::runtime::Runtime;

use super::{Replayed, SinkWriter, Sinking, ipc_sink, replay};
use crate::fixtures::events;
use crate::{
    CommitPolicy, ComputePoolError, Cores, Engine, EngineConfig, PipelinePlan, StreamPlan,
    SystemEnv,
};

/// Batches written to the same destination by a bare loop and by the engine, on a runtime and a
/// compute pool that split the same cores.
#[derive(Debug)]
pub struct Passthrough {
    runtime: Runtime,
    engine: Engine,
    plan: PipelinePlan,
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
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(cores.workers().get())
            .enable_all()
            .build()
            .expect("a runtime starts");
        let config = EngineConfig::builder()
            .memory(1 << 30)
            .commit(CommitPolicy::new(None, None, Some(1 << 40)).expect("a valid policy"))
            .build()
            .expect("a valid configuration");
        let plan = PipelinePlan::new(
            PipelineId::parse("passthrough").expect("a valid id"),
            [StreamPlan::new(
                StreamName::new("events").expect("a valid name"),
            )],
        )
        .expect("a valid plan");
        Ok(Self {
            runtime,
            engine: Engine::new(config, Arc::new(SystemEnv::try_new(cores)?)),
            plan,
            batches: (0..batches)
                .map(|index| events(i64::from(index) * i64::from(rows), rows, Self::COLUMNS))
                .collect(),
        })
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
        self.runtime.block_on(async {
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
        self.runtime.block_on(async {
            let source = replay("passthrough", Replayed::Batches(self.batches.clone())).await;
            let outcome = self
                .engine
                .run(self.plan.clone(), source, ipc_sink().await)
                .await;
            assert!(outcome.error.is_none(), "{:?}", outcome.error);
            outcome.report.rows
        })
    }
}
