//! One load's conversation with Snowflake: ensure, write, commit.
//!
//! The commit protocol is the same one every SQL destination runs — the plan
//! comes from the shared planner and this executes it — with one constraint
//! the others do not have. **DDL auto-commits here.** A schema statement
//! issued inside the unit transaction silently commits everything written
//! before it, turning a half-written unit into a durable one with no error
//! anywhere. So all schema work happens before the unit opens, and the
//! executor that runs the unit refuses DDL rather than trusting this file to
//! remember.

use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use rdlt_connector::{
    CommitMeta, CommitReceipt, DestinationError, LoadSession, RecordBatch,
    core::{LoadId, PipelineId, StateDoc, TableName, TableSchema, WriteMode},
};
use rdlt_connector_sqlcore::protocol::{
    CommitCtx, FullLoadPublish, Step, commit_script, prepare_target, unit,
};

use super::client::{DmlOnly, Executor};
use super::config::SnowflakeConfig;
use super::ddl::{self, Catalog, quote};
use super::encode;
use super::stage::{self, Part, Stage};

/// This destination publishes straight into the target.
///
/// Append and Replace rows go into the target inside the unit transaction, so
/// nothing is written twice and no staging twin exists for them. Merge still
/// stages, because its arms join delivered rows against the target.
const PUBLISH: FullLoadPublish = FullLoadPublish::DirectToTarget;

/// The three literal statements bounding a unit.
///
/// Named because the transaction IS the atomicity: publish, receipt and state
/// commit together or not at all, and a reader never sees a cleared target
/// that has not been refilled.
const UNIT_BEGIN: &str = "BEGIN";
const UNIT_COMMIT: &str = "COMMIT";
const UNIT_ROLLBACK: &str = "ROLLBACK";

/// The column a `COPY` result reports its per-file loaded rowcount in.
const COPY_ROWS_LOADED: &str = "rows_loaded";

pub(super) struct SnowflakeSession {
    pub(super) config: SnowflakeConfig,
    pub(super) executor: Box<dyn Executor>,
    pub(super) pipeline: PipelineId,
    pub(super) load_id: LoadId,
    /// What the catalog holds, read once per table per session.
    pub(super) catalog: Catalog,
    /// Ensured tables and the write mode each was ensured under.
    pub(super) tables: BTreeMap<TableName, (TableSchema, WriteMode)>,
    /// Targets already cleared for this load — the once-per-load Replace
    /// guard's in-memory half.
    pub(super) cleared: BTreeSet<TableName>,
    /// Whether a unit transaction is currently open.
    pub(super) unit_open: bool,
    /// The bulk path, when a bucket is configured. Absent, rows travel inside
    /// statements.
    pub(super) stage: Option<Stage>,
    /// Parts written but not yet loaded, per table.
    ///
    /// The `COPY` waits for the commit rather than running per batch: one
    /// statement can name every part a table accumulated, and the round trip
    /// to a SaaS warehouse is the cost that matters here.
    pub(super) pending: BTreeMap<TableName, Vec<Part>>,
    /// Parts loaded by units this session already committed, awaiting removal.
    pub(super) spent: Vec<Part>,
}

impl SnowflakeSession {
    /// The fully-qualified, quoted name of a table in this session's schema.
    ///
    /// Always three-part: a changed server-side default must not be able to
    /// retarget a pipeline mid-load.
    fn qualified(&self, table: &str) -> String {
        format!(
            "{}.{}.{}",
            quote(&self.config.database),
            quote(&self.config.schema),
            quote(table)
        )
    }

    /// Open the unit transaction if it is not already open.
    ///
    /// A unit with no writes still publishes a receipt and state, so this is
    /// called from commit as well as from the first write.
    async fn begin_unit(&mut self) -> Result<(), DestinationError> {
        if !self.unit_open {
            self.executor.execute(UNIT_BEGIN).await?;
            self.unit_open = true;
        }
        Ok(())
    }

    /// Abandon the unit, discarding everything it wrote.
    ///
    /// Failure to roll back is deliberately swallowed: the caller is already
    /// reporting a failure, the transaction dies with the session anyway, and
    /// a second error would replace the useful one.
    async fn rollback_unit(&mut self) {
        if self.unit_open {
            let _ = self.executor.execute(UNIT_ROLLBACK).await;
            self.unit_open = false;
        }
    }

    /// Load every staged part into its table, inside the open unit.
    ///
    /// One `COPY` per table rather than one per batch: the statement names
    /// every part the table accumulated, and a round trip to a SaaS warehouse
    /// is the cost worth spending once.
    ///
    /// The loaded rowcount is checked against what was written. Nothing should
    /// be able to make them differ — the parts are named explicitly and errors
    /// abort the statement — which is exactly why a difference means an
    /// assumption is wrong and the unit must not commit on it.
    async fn load_staged_parts(&mut self) -> Result<(), DestinationError> {
        let Some(stage) = &self.stage else {
            return Ok(());
        };
        let pending = std::mem::take(&mut self.pending);
        let mut loaded_parts = Vec::new();
        for (table, parts) in pending {
            if parts.is_empty() {
                continue;
            }
            let sql = stage::copy_sql(
                &self.qualified(table.as_str()),
                &self.qualified(stage.name()),
                &parts,
            );
            let expected: u64 = parts.iter().map(|part| part.rows).sum();
            let loaded = DmlOnly(&*self.executor)
                .sum_column(&sql, COPY_ROWS_LOADED)
                .await?;
            if loaded != expected {
                return Err(DestinationError::fatal(format!(
                    "snowflake: loading `{table}` staged {expected} rows in {} part(s) but the \
                     service reported {loaded} loaded; the unit is abandoned rather than \
                     committed short",
                    parts.len()
                )));
            }
            loaded_parts.extend(parts);
        }
        self.spent.extend(loaded_parts);
        Ok(())
    }

    /// Remove every part this session staged, loaded or not.
    ///
    /// Best effort by design: the objects are dead either way — a part is
    /// named by exactly one `COPY`, and a unit that did not commit will never
    /// name them again — and a cleanup failure must not fail a load that
    /// committed. The scope wipe at the next open collects the remainder.
    async fn discard_staged(&mut self) {
        let mut parts: Vec<Part> = std::mem::take(&mut self.spent);
        for table_parts in std::mem::take(&mut self.pending).into_values() {
            parts.extend(table_parts);
        }
        if let Some(stage) = &self.stage {
            stage.remove(&parts).await;
        }
    }

    /// Read a table's columns unless this session already has.
    async fn observe(&mut self, table: &str) -> Result<(), DestinationError> {
        if self.catalog.is_known(table) {
            return Ok(());
        }
        let columns = ddl::observe_table(
            &*self.executor,
            &self.config.database,
            &self.config.schema,
            table,
        )
        .await?;
        self.catalog.observe(table, columns);
        Ok(())
    }

    /// Run one step of the commit program.
    ///
    /// Every statement here is DML by construction — the planner emits no
    /// schema work — and it runs through the unit executor, which enforces
    /// that rather than assuming it.
    async fn execute_step(
        &self,
        unit_executor: &DmlOnly<'_>,
        meta: &CommitMeta,
        step: &Step,
    ) -> Result<(), DestinationError> {
        match step {
            Step::ClearTarget { table } => {
                // DELETE, never TRUNCATE: truncation is DDL here and would
                // commit the unit, publishing a cleared table before its
                // replacement rows landed.
                unit_executor
                    .execute(&format!("DELETE FROM {}", self.qualified(table.as_str())))
                    .await
            }
            Step::UpsertState => {
                let doc = serde_json::to_string(&meta.state).map_err(DestinationError::fatal)?;
                unit_executor
                    .execute(&format!(
                        "MERGE INTO {state} t USING (SELECT '{pipeline}' AS PIPELINE) s \
                         ON t.{pipeline_col} = s.PIPELINE \
                         WHEN MATCHED THEN UPDATE SET {doc_col} = '{doc}' \
                         WHEN NOT MATCHED THEN INSERT ({pipeline_col}, {doc_col}) \
                         VALUES (s.PIPELINE, '{doc}')",
                        state = self.qualified(rdlt_connector_sqlcore::names::STATE_TABLE),
                        pipeline = encode::sql_literal_body(meta.state.pipeline.as_str()),
                        pipeline_col = quote("pipeline"),
                        doc_col = quote("doc"),
                        doc = encode::sql_literal_body(&doc),
                    ))
                    .await
            }
            Step::InsertReceipt => {
                unit_executor
                    .execute(&format!(
                        "INSERT INTO {commits} ({load_col}, {seq_col}) VALUES ('{load}', {seq})",
                        commits = self.qualified(rdlt_connector_sqlcore::names::COMMITS_TABLE),
                        load_col = quote("load_id"),
                        seq_col = quote("commit_seq"),
                        load = encode::sql_literal_body(meta.load_id.as_str()),
                        seq = meta.commit_seq,
                    ))
                    .await
            }
            // The staged-publish steps cannot arise on a direct-publish
            // destination, and the merge arms belong to the merge dialect,
            // which this increment does not yet ship.
            other => Err(DestinationError::fatal(format!(
                "snowflake: the commit planner emitted {other:?}, which this \
                 destination does not execute yet"
            ))),
        }
    }
}

#[async_trait]
impl LoadSession for SnowflakeSession {
    async fn ensure_table(
        &mut self,
        schema: &TableSchema,
        mode: &WriteMode,
    ) -> Result<(), DestinationError> {
        // Schema work happens HERE, outside any unit — see the module doc.
        debug_assert!(
            !self.unit_open,
            "ensure_table must not run inside a unit: DDL would commit it"
        );
        let table = schema.table.as_str().to_owned();
        self.observe(&table).await?;
        if matches!(mode, WriteMode::Merge { .. }) {
            let stage = ddl::stage_name(self.pipeline.as_str(), &schema.table);
            self.observe(&stage).await?;
        }

        let previous = self.tables.get(&schema.table).map(|(s, _)| s.clone());
        for sql in ddl::table_ddl_stmts(
            self.pipeline.as_str(),
            schema,
            mode,
            self.config.table_type,
            previous.as_ref(),
            &self.catalog,
        ) {
            self.executor.execute(&self.qualify_ddl(&sql)).await?;
        }
        // Fold what was just applied into the image, so a second ensure at the
        // same schema version emits nothing.
        self.catalog.record_created(
            &table,
            &schema
                .columns
                .iter()
                .map(|c| c.name.clone())
                .collect::<Vec<_>>(),
        );

        for sql in ddl::merge_ensure_stmts(&self.config.options, schema, mode, &self.catalog)
            .map_err(DestinationError::fatal)?
        {
            self.executor.execute(&self.qualify_ddl(&sql)).await?;
            if let Some(column) = column_of_add(&sql) {
                self.catalog.record_column(&table, &column);
            }
        }

        self.tables
            .insert(schema.table.clone(), (schema.clone(), mode.clone()));
        Ok(())
    }

    async fn write(
        &mut self,
        table: &TableName,
        batch: RecordBatch,
    ) -> Result<(), DestinationError> {
        let Some(schema) = self.tables.get(table).map(|(schema, _)| schema.clone()) else {
            return Err(DestinationError::fatal(format!(
                "snowflake: `{table}` was written before it was ensured"
            )));
        };
        self.begin_unit().await?;

        // Replace clears its target once per load, inside the unit and ahead
        // of the first row — the direct-publish counterpart of the staged
        // path's clear step. The planner decides whether one is owed; this
        // only runs what it returns.
        let empty = BTreeSet::new();
        let steps = prepare_target(
            &self.tables,
            &CommitCtx {
                replayed: false,
                load_committed_before: false,
                single_unit_done: &empty,
                staged_nonempty: &empty,
                full_load_publish: PUBLISH,
                cleared_targets: &self.cleared,
            },
            table,
        );
        for step in steps {
            let executor = DmlOnly(&*self.executor);
            self.execute_step(&executor, &clear_meta(&self.load_id, &self.pipeline), &step)
                .await?;
            if let Step::ClearTarget { table } = step {
                self.cleared.insert(table);
            }
        }

        // With a bucket the rows leave as parquet and the service loads them
        // at commit; without one they travel inside the statements themselves.
        // The choice is the user's configuration, not a size heuristic: a
        // threshold would be a guessed constant, and the crossover is a
        // measurement this feature takes rather than assumes.
        if self.stage.is_some() {
            let rows = batch.num_rows() as u64;
            if rows == 0 {
                return Ok(());
            }
            let bytes = encode::parquet_part(&schema, &batch)?;
            let part = self
                .stage
                .as_mut()
                .expect("checked")
                .put_part(table.as_str(), bytes, rows)
                .await?;
            self.pending.entry(table.clone()).or_default().push(part);
            return Ok(());
        }

        let target = self.qualified(table.as_str());
        for sql in encode::insert_statements(&target, &schema, &batch)? {
            DmlOnly(&*self.executor).execute(&sql).await?;
        }
        Ok(())
    }

    async fn commit(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestinationError> {
        // A session opened for one load must not commit another: the receipt
        // it would write is one no recovery could ever match.
        if let Some(message) = unit::load_mismatch(&self.load_id, &meta.load_id) {
            return Err(DestinationError::fatal(format!("snowflake: {message}")));
        }
        self.begin_unit().await?;

        let receipt = CommitReceipt {
            load_id: meta.load_id.clone(),
            commit_seq: meta.commit_seq,
        };
        let replayed = {
            let executor = DmlOnly(&*self.executor);
            executor
                .scalar_u64(
                    &unit::receipt_exists_sql(|_| "?".to_owned()),
                    &[meta.load_id.as_str(), &meta.commit_seq.to_string()],
                )
                .await?
                > 0
        };

        let script = commit_script(
            &self.tables,
            &self.config.options,
            &CommitCtx {
                replayed,
                load_committed_before: false,
                single_unit_done: &BTreeSet::new(),
                staged_nonempty: &BTreeSet::new(),
                full_load_publish: PUBLISH,
                cleared_targets: &self.cleared,
            },
        )
        .map_err(DestinationError::fatal)?;

        // A redelivered unit's rows are already durable in the target, and
        // this attempt's copies sit in the transaction still open. What to do
        // about that is the shared planner's decision, not this executor's
        // memory — and on a direct-publish path the answer is to abandon the
        // unit, which discards exactly what this attempt wrote.
        if replayed && unit::replay_disposition(PUBLISH) == unit::ReplayDisposition::DiscardUnit {
            self.rollback_unit().await;
            self.discard_staged().await;
            return Ok(receipt);
        }

        // The staged parts land BEFORE the publish steps, in the same
        // transaction: the receipt this unit is about to write claims those
        // rows are durable, so it must not be written first.
        if let Err(e) = self.load_staged_parts().await {
            self.rollback_unit().await;
            self.discard_staged().await;
            return Err(e);
        }

        for step in &script.steps {
            let executor = DmlOnly(&*self.executor);
            if let Err(e) = self.execute_step(&executor, &meta, step).await {
                self.rollback_unit().await;
                self.discard_staged().await;
                return Err(e);
            }
        }
        self.executor.execute(UNIT_COMMIT).await?;
        self.unit_open = false;
        // Only after the commit: a part removed before it is durable in the
        // target is a part no recovery could re-read.
        self.discard_staged().await;
        Ok(receipt)
    }

    async fn read_state(
        &mut self,
        pipeline: &PipelineId,
    ) -> Result<Option<StateDoc>, DestinationError> {
        let sql = format!(
            "SELECT {doc} FROM {state} WHERE {pipeline_col} = ?",
            doc = quote("doc"),
            state = self.qualified(rdlt_connector_sqlcore::names::STATE_TABLE),
            pipeline_col = quote("pipeline"),
        );
        let docs = self
            .executor
            .column_names(&sql, &[pipeline.as_str()])
            .await?;
        let Some(doc) = docs.into_iter().next() else {
            return Ok(None);
        };
        serde_json::from_str(&doc)
            .map(Some)
            .map_err(DestinationError::fatal)
    }
}

impl SnowflakeSession {
    /// Qualify a rendered DDL statement's table name.
    ///
    /// The ddl module renders names quoted but unqualified, because qualifying
    /// belongs to the session that knows the database and schema. The
    /// substitution is anchored on the quoted name the renderer produced, so
    /// it cannot match a column or a literal.
    fn qualify_ddl(&self, sql: &str) -> String {
        let prefix = format!(
            "{}.{}.",
            quote(&self.config.database),
            quote(&self.config.schema)
        );
        for verb in [
            "CREATE TABLE IF NOT EXISTS ",
            "CREATE TRANSIENT TABLE IF NOT EXISTS ",
            "ALTER TABLE ",
        ] {
            if let Some(rest) = sql.strip_prefix(verb) {
                return format!("{verb}{prefix}{rest}");
            }
        }
        sql.to_owned()
    }
}

/// The table name an `ADD COLUMN` statement adds, so the catalog image can
/// record it without re-reading.
fn column_of_add(sql: &str) -> Option<String> {
    let rest = sql.split("ADD COLUMN IF NOT EXISTS ").nth(1)?;
    let name = rest.split_whitespace().next()?;
    Some(name.trim_matches('"').to_owned())
}

/// A meta for the clear step, which reads none of it.
///
/// `ClearTarget` names its own table and needs no load identity or state; the
/// step executor takes a `CommitMeta` because the receipt and state steps do,
/// and inventing one here is cheaper than splitting the signature for a case
/// that ignores it.
fn clear_meta(load_id: &LoadId, pipeline: &PipelineId) -> CommitMeta {
    CommitMeta {
        load_id: load_id.clone(),
        commit_seq: 0,
        state: StateDoc::new(pipeline.clone(), ""),
        counters: Default::default(),
    }
}
