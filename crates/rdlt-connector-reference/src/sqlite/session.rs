//! A SQLite session: schema changes, staging writers and commits, each one transaction.

mod commit;
pub(super) mod owners;
#[cfg(test)]
mod tests;
mod writer;

use std::sync::Arc;
use std::time::SystemTime;

use rdlt_connector::prelude::*;
use rdlt_connector::sqlgen::{self, SqlPlanner, Sqlite};
use rdlt_connector::{Epoch, PipelineId, StateRecord};
use rusqlite::types::Value;

use super::database::{Database, columns, integer, query, run, run_all, text};
use commit::{drop_tables, finish, publish, stored};
use owners::{catalog, changed, created, discard, distinct};

pub use writer::SqliteWriter;

/// A [`SqliteDestination`](super::SqliteDestination) session, on its own connection.
#[derive(Debug)]
pub struct SqliteSession {
    database: Database,
    planner: Arc<SqlPlanner<Sqlite>>,
    pipeline: PipelineId,
    epoch: Epoch,
}

impl SqliteSession {
    /// Creates the catalog where it is missing, increments `pipeline`'s epoch and reads its state.
    pub(super) async fn open(
        database: Database,
        planner: Arc<SqlPlanner<Sqlite>>,
        pipeline: PipelineId,
    ) -> Result<Opened<Self>> {
        let (plan, name) = (Arc::clone(&planner), pipeline.clone());
        let (epoch, state) = database
            .transaction(move |transaction| {
                catalog(transaction, &plan)?;
                run_all(transaction, &plan.bootstrap())?;
                run_all(transaction, &plan.open(&name))?;
                let epoch = query(transaction, &plan.epoch(&name))?;
                let epoch = epoch
                    .first()
                    .and_then(|row| row.first())
                    .map(integer)
                    .transpose()?
                    .unwrap_or_default();
                let state = query(transaction, &plan.state(&name))?
                    .into_iter()
                    .map(|row| match &row[..] {
                        [key, Value::Blob(value)] => Ok(StateRecord {
                            key: text(key)?,
                            value: value.clone().into(),
                        }),
                        _ => Err(ConnectorError::internal("a state record is not bytes")),
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok((epoch, state))
            })
            .await?;
        let epoch = Epoch(sqlgen::unsigned(epoch));
        Ok(Opened {
            session: Self {
                database,
                planner,
                pipeline,
                epoch,
            },
            epoch,
            state,
        })
    }
}

impl Session for SqliteSession {
    type Writer = SqliteWriter;

    async fn apply_schema(&mut self, change: &TableChange) -> Result<()> {
        let (planner, change) = (Arc::clone(&self.planner), change.clone());
        let (pipeline, epoch) = (self.pipeline.clone(), self.epoch);
        self.database
            .transaction(move |transaction| {
                let table = change.table();
                let owned = if matches!(change, TableChange::Create { .. }) {
                    let created = created(transaction, &planner, &pipeline, epoch, table)?;
                    distinct(transaction, &planner, table)?;
                    created
                } else {
                    changed(transaction, &planner, &pipeline, epoch, &table.name)?
                };
                let target = columns(transaction, planner.dialect(), &planner.target(table))?;
                let staging = columns(
                    transaction,
                    planner.dialect(),
                    &planner.staging_table(&table.name),
                )?;
                let tombstones = columns(
                    transaction,
                    planner.dialect(),
                    &planner.tombstone_table(&table.name),
                )?;
                let plan = planner.change(&owned, &change, [&target, &staging, &tombstones])?;
                run_all(transaction, &plan)
            })
            .await
    }

    async fn writer(&mut self, table: &TableRef) -> Result<SqliteWriter> {
        let (planner, pipeline, epoch, written) = (
            Arc::clone(&self.planner),
            self.pipeline.clone(),
            self.epoch,
            table.clone(),
        );
        self.database
            .transaction(move |transaction| {
                writer::ready(transaction, &planner, &pipeline, epoch, &written)
            })
            .await?;
        Ok(SqliteWriter {
            database: self.database.clone(),
            planner: Arc::clone(&self.planner),
            pipeline: self.pipeline.clone(),
            epoch: self.epoch,
            table: table.clone(),
            buffered: Vec::new(),
        })
    }

    async fn discard_staged(&mut self) -> Result<()> {
        let (planner, pipeline, epoch) =
            (Arc::clone(&self.planner), self.pipeline.clone(), self.epoch);
        self.database
            .transaction(move |transaction| discard(transaction, &planner, &pipeline, epoch))
            .await
    }

    async fn commit(&mut self, meta: &CommitMeta) -> Result<Receipt> {
        let (planner, pipeline, epoch) =
            (Arc::clone(&self.planner), self.pipeline.clone(), self.epoch);
        let meta = meta.clone();
        self.database
            .transaction(move |transaction| {
                let fenced =
                    meta.epoch != epoch || run(transaction, &planner.fence(&pipeline, epoch))? == 0;
                if fenced {
                    return Err(ConnectorError::fenced(format!(
                        "pipeline {pipeline} has a session newer than epoch {epoch}"
                    )));
                }
                if let Some(receipt) = stored(
                    transaction,
                    &planner,
                    &pipeline,
                    meta.load_id,
                    meta.commit_seq,
                )? {
                    return Ok(receipt);
                }
                let (rows, bytes) = publish(transaction, &planner, &pipeline, epoch, &meta)?;
                finish(transaction, &planner, &pipeline, &meta)?;
                drop_tables(transaction, &planner, &pipeline, &meta)?;
                run_all(
                    transaction,
                    &planner.state_changes(&pipeline, &meta.state_delta),
                )?;
                let committed_at = sqlgen::micros(SystemTime::now());
                let receipt = sqlgen::receipt(
                    meta.load_id,
                    meta.commit_seq,
                    i64::try_from(committed_at).unwrap_or(i64::MAX),
                    rows,
                    bytes,
                );
                run(transaction, &planner.record_receipt(&pipeline, &receipt))?;
                Ok(receipt)
            })
            .await
    }

    async fn close(self) -> Result<()> {
        Ok(())
    }
}
