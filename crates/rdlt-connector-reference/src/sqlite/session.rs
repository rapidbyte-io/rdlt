//! A SQLite session: schema changes, staging writers and commits, each one transaction.

mod commit;
pub(super) mod owners;
#[cfg(test)]
mod tests;
mod writer;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::SystemTime;

use parking_lot::Mutex;
use rdlt_connector::prelude::*;
use rdlt_connector::sqlgen::{self, SqlPlanner, Sqlite};
use rdlt_connector::{Epoch, GenerationId, PipelineId, SegmentId, SegmentSet, StateRecord};
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
    written: Written,
}

/// The columns the session's writers staged rows with, by segment, then by table and the
/// generation of it the rows fill.
///
/// A commit publishes only what its own session staged, and the catalog records no columns of
/// it.
#[derive(Clone, Debug, Default)]
pub(super) struct Written(Arc<Mutex<BTreeMap<SegmentId, Columns>>>);

/// The columns rows of each table, and of each generation of it, were staged with.
pub(super) type Columns = BTreeMap<(String, Option<GenerationId>), BTreeSet<String>>;

impl Written {
    /// Notes that rows of `table` were staged in `segment` with `columns`.
    pub(super) fn note(
        &self,
        table: &TableRef,
        segment: SegmentId,
        columns: impl IntoIterator<Item = String>,
    ) {
        let staging = (table.name.to_string(), table.generation);
        let mut noted = self.0.lock();
        let noted = noted
            .entry(segment)
            .or_default()
            .entry(staging)
            .or_default();
        noted.extend(columns);
    }

    /// The columns noted of each table and generation in `segments`.
    pub(super) fn of(&self, segments: &SegmentSet) -> Columns {
        let mut columns = Columns::new();
        let noted = self.0.lock();
        for (_, of_segment) in noted.iter().filter(|(id, _)| segments.contains(**id)) {
            for (staging, names) in of_segment {
                let held = columns.entry(staging.clone()).or_default();
                held.extend(names.iter().cloned());
            }
        }
        columns
    }

    /// Forgets what was noted in `segments`, which a commit published or abandoned.
    pub(super) fn forget(&self, segments: &SegmentSet) {
        self.0
            .lock()
            .retain(|segment, _| !segments.contains(*segment));
    }
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
                written: Written::default(),
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
            written: self.written.clone(),
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
        let ended = [meta.segments.clone(), meta.abandoned.clone()];
        let (meta, noted) = (meta.clone(), self.written.of(&meta.segments));
        let receipt = self
            .database
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
                let (rows, bytes) =
                    publish(transaction, &planner, (&pipeline, epoch), &meta, &noted)?;
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
                if let Some(horizon) = &meta.horizon {
                    run(transaction, &planner.forget_receipts(&pipeline, horizon))?;
                }
                run(transaction, &planner.record_receipt(&pipeline, &receipt))?;
                Ok(receipt)
            })
            .await?;
        for segments in &ended {
            self.written.forget(segments);
        }
        Ok(receipt)
    }

    async fn close(self) -> Result<()> {
        Ok(())
    }
}
