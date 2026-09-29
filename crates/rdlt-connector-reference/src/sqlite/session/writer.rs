//! A SQLite session's writer, which stages a table's batches.

use std::sync::Arc;

use arrow_array::RecordBatch;
use rdlt_connector::prelude::*;
use rdlt_connector::sqlgen::{self, SqlPlanner, Sqlite};
use rdlt_connector::{Epoch, PipelineId, SegmentId};

use super::super::database::{Database, columns, run};
use super::super::values;

/// Stages a table's batches: buffers them, and writes them to its staging table on flush.
#[derive(Debug)]
pub struct SqliteWriter {
    pub(super) database: Database,
    pub(super) planner: Arc<SqlPlanner<Sqlite>>,
    pub(super) pipeline: PipelineId,
    pub(super) epoch: Epoch,
    pub(super) table: TableRef,
    pub(super) buffered: Vec<(SegmentId, RecordBatch)>,
}

impl TableWriter for SqliteWriter {
    async fn write(&mut self, segment: SegmentId, batch: RecordBatch) -> Result<()> {
        self.buffered.push((segment, batch));
        Ok(())
    }

    async fn flush(&mut self) -> Result<WriteStats> {
        let buffered = std::mem::take(&mut self.buffered);
        let (planner, pipeline, epoch, table) = (
            Arc::clone(&self.planner),
            self.pipeline.clone(),
            self.epoch,
            self.table.clone(),
        );
        self.database
            .transaction(move |transaction| {
                let mut stats = WriteStats::default();
                let target = columns(transaction, planner.dialect(), &planner.target(&table))?;
                for (segment, batch) in &buffered {
                    let batch = &sqlgen::staged_changes(batch, &table, &target)?;
                    let schema = batch.schema();
                    let names: Vec<&str> = schema
                        .fields()
                        .iter()
                        .map(|field| field.name().as_str())
                        .collect();
                    let statement = planner.stage(&table, &pipeline, epoch, *segment, &names);
                    values::stage(transaction, &statement, batch)?;
                    let rows = batch.num_rows() as u64;
                    let bytes = batch.get_array_memory_size() as u64;
                    let record =
                        planner.record_segment(&table, &pipeline, epoch, *segment, [rows, bytes]);
                    run(transaction, &record)?;
                    stats.rows += rows;
                    stats.bytes += bytes;
                }
                Ok(stats)
            })
            .await
    }
}
