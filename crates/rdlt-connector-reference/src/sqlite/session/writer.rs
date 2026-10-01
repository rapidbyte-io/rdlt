//! A SQLite session's writer, which stages a table's batches.

use std::sync::Arc;

use arrow_array::RecordBatch;
use rdlt_connector::prelude::*;
use rdlt_connector::sqlgen::{self, SqlPlanner, Sqlite};
use rdlt_connector::{Epoch, PipelineId, SegmentId};

use rusqlite::Transaction;

use super::super::database::{Database, columns, run, run_all};
use super::super::values;
use super::owners::{claim, distinct, owned};

/// Readies `table` for a writer of `pipeline`'s session at `epoch`: claims it, readies a change
/// stream's tables, checks that it holds its merge key, creates the generation table it fills
/// where that is missing, and indexes what a commit finds rows in, which a commit never does
/// itself.
pub(super) fn ready(
    transaction: &Transaction<'_>,
    planner: &SqlPlanner<Sqlite>,
    pipeline: &PipelineId,
    epoch: Epoch,
    table: &TableRef,
) -> Result<()> {
    let table_owner = claim(transaction, planner, pipeline, epoch, &table.name)?;
    let dialect = planner.dialect();
    let [target, staging, tombstones] = [
        planner.target(table),
        planner.staging_table(&table.name),
        planner.tombstone_table(&table.name),
    ]
    .map(|name| columns(transaction, dialect, &name));
    let (target, tables) = (target?, [&staging?[..], &tombstones?[..]]);
    let tables = [&target[..], tables[0], tables[1]];
    run_all(
        transaction,
        &planner.change_tables(&table_owner, table, tables)?,
    )?;
    let base = columns(transaction, dialect, &table.name)?;
    if !base.is_empty() {
        planner.merges(table, &base)?;
    }
    if table.generation.is_some() && target.is_empty() {
        distinct(transaction, planner, table)?;
        run_all(
            transaction,
            &planner.generation(&table_owner, table, &base)?,
        )?;
    }
    let mut indexes = planner.key_indexes(&table_owner, table)?;
    indexes.extend(planner.root_index(&table_owner, table)?);
    run_all(transaction, &indexes)
}

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
                // The table may have been dropped and claimed by another pipeline since the
                // writer opened.
                let staging = owned(transaction, &planner, &pipeline, &table.name)?;
                let target = columns(transaction, planner.dialect(), &planner.target(&table))?;
                for (segment, batch) in &buffered {
                    let batch = &sqlgen::staged_changes(batch, &table, &target)?;
                    let schema = batch.schema();
                    let names: Vec<&str> = schema
                        .fields()
                        .iter()
                        .map(|field| field.name().as_str())
                        .collect();
                    let statement = planner.stage(&staging, &table, epoch, *segment, &names)?;
                    values::stage(transaction, &statement, batch)?;
                    let rows = batch.num_rows() as u64;
                    let bytes = batch.get_array_memory_size() as u64;
                    let record =
                        planner.record_segment(&staging, &table, epoch, *segment, [rows, bytes])?;
                    run(transaction, &record)?;
                    stats.rows += rows;
                    stats.bytes += bytes;
                }
                Ok(stats)
            })
            .await
    }
}
