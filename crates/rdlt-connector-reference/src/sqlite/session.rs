//! A SQLite session: schema changes, staging writers and commits, each one transaction.

use std::sync::Arc;
use std::time::SystemTime;

use arrow_array::RecordBatch;
use rdlt_connector::prelude::*;
use rdlt_connector::sqlgen::{self, SqlPlanner, Sqlite, Staged, staging_table};
use rdlt_connector::{CommitSeq, Epoch, GenerationId, LoadId, PipelineId, SegmentId, StateRecord};
use rusqlite::Transaction;
use rusqlite::types::Value;

use super::database::{Database, columns, integer, query, run, run_all, text};
use super::values;

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
        self.database
            .transaction(move |transaction| {
                let table = change.table();
                let target = columns(transaction, planner.dialect(), &planner.target(table))?;
                let staging = columns(transaction, planner.dialect(), &staging_table(&table.name))?;
                run_all(transaction, &planner.change(&change, &target, &staging)?)?;
                if matches!(change, TableChange::Create { .. }) {
                    run_all(transaction, &planner.register(table))?;
                }
                Ok(())
            })
            .await
    }

    async fn writer(&mut self, table: &TableRef) -> Result<SqliteWriter> {
        if table.generation.is_some() {
            let (planner, generation) = (Arc::clone(&self.planner), table.clone());
            self.database
                .transaction(move |transaction| {
                    let dialect = planner.dialect();
                    if !columns(transaction, dialect, &planner.target(&generation))?.is_empty() {
                        return Ok(());
                    }
                    let base = columns(transaction, dialect, &generation.name)?;
                    run_all(transaction, &planner.generation(&generation, &base))
                })
                .await?;
        }
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
            .transaction(move |transaction| {
                let names = query(transaction, &planner.tables())?
                    .iter()
                    .map(|row| row.first().map_or(Ok(String::new()), text))
                    .collect::<Result<Vec<_>>>()?;
                run_all(transaction, &planner.discard(&pipeline, epoch, &names))
            })
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
                for (path, generation) in &meta.finish_generations {
                    let found = query(transaction, &planner.table_name(path))?;
                    let Some(name) = found.first().and_then(|row| row.first()) else {
                        continue;
                    };
                    let name = text(name)?;
                    swap(transaction, &planner, &name, *generation)?;
                }
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

/// The receipt stored for `(load_id, commit_seq)`, if that commit already happened.
fn stored(
    transaction: &Transaction<'_>,
    planner: &SqlPlanner<Sqlite>,
    pipeline: &PipelineId,
    load_id: LoadId,
    commit_seq: CommitSeq,
) -> Result<Option<Receipt>> {
    let rows = query(transaction, &planner.receipt(pipeline, load_id, commit_seq))?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    match &row[..] {
        [at, count, bytes] => Ok(Some(sqlgen::receipt(
            load_id,
            commit_seq,
            integer(at)?,
            integer(count)?,
            integer(bytes)?,
        ))),
        _ => Err(ConnectorError::internal(
            "a stored receipt has missing fields",
        )),
    }
}

/// Publishes what this session staged in `meta`'s segments into each table; returns the rows and
/// bytes published.
fn publish(
    transaction: &Transaction<'_>,
    planner: &SqlPlanner<Sqlite>,
    pipeline: &PipelineId,
    epoch: Epoch,
    meta: &CommitMeta,
) -> Result<(i64, i64)> {
    let (mut rows, mut bytes) = (0, 0);
    let mut staged = query(
        transaction,
        &planner.staged(pipeline, epoch, &meta.segments),
    )?
    .iter()
    .map(|row| staged_segment(row))
    .collect::<Result<Vec<_>>>()?;
    // A child table the commit lists follows its root even where it staged nothing.
    for child in &meta.child_tables {
        let root = child.merge.root.as_ref().map(|root| root.table.to_string());
        let root_staged = staged
            .iter()
            .any(|(staged, ..)| Some(&staged.name) == root.as_ref());
        let own_staged = staged
            .iter()
            .any(|(staged, ..)| *staged.name == *child.table);
        if root_staged && !own_staged {
            let listed = Staged {
                name: child.table.to_string(),
                generation: None,
                merge: Some(child.merge.clone()),
            };
            staged.push((listed, 0, 0));
        }
    }
    // Child tables publish first: they read their roots' staged rows, which a root's publish
    // removes.
    staged.sort_by_key(|(staged, ..)| staged.merge.as_ref().is_none_or(|key| key.root.is_none()));
    for (staged, count, size) in staged {
        let target = match staged.generation {
            Some(generation) => sqlgen::generation_table(&staged.name, generation),
            None => staged.name.clone(),
        };
        let columns = columns(transaction, planner.dialect(), &target)?;
        run_all(
            transaction,
            &planner.publish(&staged, &columns, pipeline, epoch, &meta.segments)?,
        )?;
        rows += count;
        bytes += size;
    }
    run(
        transaction,
        &planner.forget(pipeline, epoch, &meta.segments),
    )?;
    Ok((rows, bytes))
}

/// A row of [`SqlPlanner::staged`]: what a table staged, with its rows and bytes.
fn staged_segment(row: &[Value]) -> Result<(Staged, i64, i64)> {
    let [name, generation, key, seq, count, size] = row else {
        return Err(ConnectorError::internal(
            "a staged segment has missing fields",
        ));
    };
    let generation = match generation {
        Value::Null => None,
        other => Some(GenerationId(sqlgen::unsigned(integer(other)?))),
    };
    let merge = match (key, seq) {
        (Value::Text(key), Value::Text(seq)) => Some(sqlgen::merge_key(key, seq)?),
        _ => None,
    };
    let staged = Staged {
        name: text(name)?,
        generation,
        merge,
    };
    Ok((staged, integer(count)?, integer(size)?))
}

/// Swaps `generation` in as the table `name`.
fn swap(
    transaction: &Transaction<'_>,
    planner: &SqlPlanner<Sqlite>,
    name: &str,
    generation: GenerationId,
) -> Result<()> {
    let generations = query(transaction, &planner.generations(name))?
        .iter()
        .map(|row| match &row[..] {
            [table, found] => Ok((
                text(table)?,
                GenerationId(sqlgen::unsigned(integer(found)?)),
            )),
            _ => Err(ConnectorError::internal("a generation has missing fields")),
        })
        .collect::<Result<Vec<_>>>()?;
    let exists = !columns(transaction, planner.dialect(), name)?.is_empty();
    run_all(
        transaction,
        &planner.swap(name, exists, generation, &generations),
    )
}

/// Stages a table's batches: buffers them, and writes them to its staging table on flush.
#[derive(Debug)]
pub struct SqliteWriter {
    database: Database,
    planner: Arc<SqlPlanner<Sqlite>>,
    pipeline: PipelineId,
    epoch: Epoch,
    table: TableRef,
    buffered: Vec<(SegmentId, RecordBatch)>,
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
                for (segment, batch) in &buffered {
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
