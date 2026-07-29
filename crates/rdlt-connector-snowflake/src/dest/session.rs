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
use rdlt_connector_sqlcore::plan::scope_replace_sql;
use rdlt_connector_sqlcore::protocol::{
    CommitCtx, FullLoadPublish, MergeArm, Step, build_merge_plan, commit_script, insert_select_sql,
    prepare_target, render_arm, staged_probe_targets, unit,
};
use rdlt_connector_sqlcore::{MergeDialect as _, column_list_with};

use super::client::{self, DmlOnly, Executor};
use super::config::SnowflakeConfig;
use super::ddl::{self, Catalog, quote};
use super::dialect::{self, SnowflakeDialect};
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

/// An injected crash, as a VALUE rather than an early return.
///
/// The usual macro returns straight out of the enclosing function, which is
/// wrong at the two unit edges: a crash there has cleanup to do first — the
/// transaction to abandon, the staged parts to drop — and a bare return would
/// leave the session holding an open transaction the test then blames on the
/// protocol rather than on the injection.
#[cfg(feature = "failpoints")]
fn crash_at(name: &str) -> Option<DestinationError> {
    rdlt_connector::core::failpoint::fail::fail_point!(name, |_| {
        Some(DestinationError::fatal(format!("injected crash at {name}")))
    });
    None
}

#[cfg(not(feature = "failpoints"))]
fn crash_at(_name: &str) -> Option<DestinationError> {
    None
}

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
    /// Tables whose full-feed unit has already committed.
    ///
    /// The single-unit discipline is per table: a full-feed merge delivers a
    /// table's whole contents in ONE unit, and a second non-empty unit for the
    /// same table means the feed disagrees with its own declaration. Marked
    /// only AFTER the unit commits.
    pub(super) single_unit_done: BTreeSet<TableName>,
    /// Where rows travel: parquet parts uploaded to storage the service
    /// provides. Not optional — there is no second mechanism to fall back to.
    pub(super) stage: Stage,
    /// Parts written but not yet loaded, per table.
    ///
    /// The `COPY` waits for the commit rather than running per batch: one
    /// statement can name every part a table accumulated, and the round trip
    /// to a SaaS warehouse is the cost that matters here.
    /// Parts written but not yet loaded, per destination table, together with
    /// the columns they carry — recorded when the part is built, because that
    /// is where the schema is known and reverse-deriving it later from a
    /// derived stage-table name would be guesswork.
    pub(super) pending: BTreeMap<TableName, (Vec<String>, Vec<Part>)>,
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
            // The unit's instant, captured once. Every statement that needs a
            // boundary reads this rather than calling the clock again, which
            // moves between statements here.
            self.executor
                .execute(&dialect::capture_tx_timestamp())
                .await?;
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
        let stage = &self.stage;
        let pending = std::mem::take(&mut self.pending);
        let mut loaded_parts = Vec::new();
        for (table, (columns, parts)) in pending {
            if parts.is_empty() {
                continue;
            }
            let sql = stage::copy_sql(
                &self.qualified(table.as_str()),
                &self.qualified(stage.name()),
                &columns,
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
        for (_, table_parts) in std::mem::take(&mut self.pending).into_values() {
            parts.extend(table_parts);
        }
        let qualified_stage = self.qualified(self.stage.name());
        self.stage.remove(&*self.executor, &qualified_stage).await;
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
            Step::InsertSelect { table } => {
                let (schema, _) = &self.tables[table];
                unit_executor
                    .execute(&insert_select_sql(
                        &self.qualified(table.as_str()),
                        &column_list_with(schema, quote),
                        &self.qualified(&ddl::stage_name(self.pipeline.as_str(), table)),
                    ))
                    .await
            }
            Step::ScopeReplace { table, scope } => {
                unit_executor
                    .execute(&scope_replace_sql(
                        &SnowflakeDialect,
                        &self.qualified(table.as_str()),
                        &self.qualified(&ddl::stage_name(self.pipeline.as_str(), table)),
                        scope,
                    ))
                    .await
            }
            Step::MergeArm { table, arm } => {
                for sql in self.merge_statements(table, arm)? {
                    if let Err(e) = unit_executor.execute(&sql).await {
                        return Err(self.explain_merge_failure(table, e));
                    }
                }
                Ok(())
            }
            Step::TruncateStage { table } => {
                unit_executor
                    .execute(&SnowflakeDialect.clear_table(
                        &self.qualified(&ddl::stage_name(self.pipeline.as_str(), table)),
                    ))
                    .await
            }
        }
    }

    /// Turn a merge failure into advice where the service gave a reason.
    ///
    /// A merge whose source holds two rows for one target key is the
    /// unique-violation analogue here — the difference being that no
    /// constraint could have caught it earlier, because this service enforces
    /// none. The service reports it as a structured code; the ADVICE is the
    /// shared one every SQL destination gives, so an operator meeting this on
    /// Snowflake reads the same sentence they would have read on Postgres.
    ///
    /// Recognised by CODE, never by message text: the wording is the service's
    /// to change.
    fn explain_merge_failure(
        &self,
        table: &TableName,
        error: DestinationError,
    ) -> DestinationError {
        let key = match self.tables.get(table) {
            Some((_, WriteMode::Merge { key })) => key.as_slice(),
            _ => &[],
        };
        match merge_diagnosis(table.as_str(), key, client::code_in(&error).as_deref()) {
            Some(diagnosis) => DestinationError::fatal(diagnosis),
            None => error,
        }
    }

    /// The statements one merge arm becomes.
    ///
    /// Every decision — which arm, which survivor, which columns — is the
    /// shared planner's; only the spelling is this dialect's. Building the plan
    /// here rather than in the executor keeps the borrow of `self.tables`
    /// contained.
    fn merge_statements(
        &self,
        table: &TableName,
        arm: &MergeArm,
    ) -> Result<Vec<String>, DestinationError> {
        let (schema, mode) = self.tables.get(table).ok_or_else(|| {
            DestinationError::fatal(format!(
                "snowflake: merge arm planned for unknown `{table}`"
            ))
        })?;
        let WriteMode::Merge { key } = mode else {
            return Err(DestinationError::fatal(format!(
                "snowflake: merge arm planned for non-merge table `{table}`"
            )));
        };
        let roots = unit::roots_of(&self.tables);
        let root = roots.get(table).unwrap_or(table).clone();
        let pipeline = self.pipeline.as_str();
        // Bound to locals: the plan borrows every one of these, so building
        // them inline would leave it holding references to temporaries.
        let target = self.qualified(table.as_str());
        let stage = self.qualified(&ddl::stage_name(pipeline, table));
        let columns = column_list_with(schema, quote);
        let plan = build_merge_plan(
            &SnowflakeDialect,
            &self.config.options,
            table,
            schema,
            key,
            &target,
            &stage,
            &columns,
            &root,
            self.qualified(&ddl::stage_name(pipeline, &root)),
            self.tables.get(&root).map(|(s, _)| s),
        );
        Ok(render_arm(&plan, arm))
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
        let Some((schema, mode)) = self.tables.get(table).cloned() else {
            return Err(DestinationError::fatal(format!(
                "snowflake: `{table}` was written before it was ensured"
            )));
        };
        self.begin_unit().await?;

        // A merge's rows land in the STAGE table, never the target: its arms
        // join the delivered rows against what is already there, and rows
        // written straight to the target would be both sides of that join.
        let staged_merge = matches!(mode, WriteMode::Merge { .. });
        let destination_table = if staged_merge {
            ddl::stage_name(self.pipeline.as_str(), table)
        } else {
            table.as_str().to_owned()
        };

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

        // The rows leave as a parquet part, uploaded to storage the service
        // provides. The part is built and uploaded inside a borrow of the
        // stage and recorded outside it: `pending` belongs to the session, and
        // holding the stage across that insert would borrow the session twice.
        //
        // The fields are split rather than reached through `self`, because the
        // upload needs the executor and the staging state at the same time and
        // they are both the session's.
        let rows = batch.num_rows() as u64;
        if rows == 0 {
            return Ok(());
        }
        let part = {
            let Self {
                stage,
                executor,
                config,
                ..
            } = self;
            let qualified_stage = format!(
                "{}.{}.{}",
                quote(&config.database),
                quote(&config.schema),
                quote(stage.name())
            );
            let bytes = encode::parquet_part(&schema, &batch)?;
            stage
                .put_part(
                    &**executor,
                    &qualified_stage,
                    &destination_table,
                    bytes,
                    rows,
                )
                .await?
        };
        let columns = schema.columns.iter().map(|c| c.name.clone()).collect();
        self.pending
            .entry(TableName::from(destination_table.as_str()))
            .or_insert_with(|| (columns, Vec::new()))
            .1
            .push(part);
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

        // The staged parts land BEFORE anything asks what the stages hold —
        // and before the publish steps, in the same transaction, because the
        // receipt this unit is about to write claims those rows are durable.
        if let Err(e) = self.load_staged_parts().await {
            self.rollback_unit().await;
            self.discard_staged().await;
            return Err(e);
        }

        // Which full-feed stages actually received rows. Probed here rather
        // than remembered: a merge's rows may have arrived through either
        // ingestion path, and the stage table is the one place both agree.
        let mut staged_nonempty = BTreeSet::new();
        for table in staged_probe_targets(&self.tables, &self.config.options) {
            let stage = self.qualified(&ddl::stage_name(self.pipeline.as_str(), table));
            let nonempty = DmlOnly(&*self.executor)
                .scalar_u64(&unit::stage_nonempty_sql(&stage), &[])
                .await?;
            if nonempty > 0 {
                staged_nonempty.insert(table.clone());
            }
        }

        let script = commit_script(
            &self.tables,
            &self.config.options,
            &CommitCtx {
                replayed,
                load_committed_before: false,
                single_unit_done: &self.single_unit_done,
                staged_nonempty: &staged_nonempty,
                full_load_publish: PUBLISH,
                cleared_targets: &self.cleared,
            },
        )
        .map_err(DestinationError::fatal)?;

        for step in &script.steps {
            let executor = DmlOnly(&*self.executor);
            if let Err(e) = self.execute_step(&executor, &meta, step).await {
                self.rollback_unit().await;
                self.discard_staged().await;
                return Err(e);
            }
        }
        // Everything the unit publishes is written but nothing is durable: the
        // transaction is still open, so recovery must find the target exactly
        // as it was before this attempt.
        if let Some(injected) = crash_at("sf.unit.publish") {
            self.rollback_unit().await;
            self.discard_staged().await;
            return Err(injected);
        }

        self.executor.execute(UNIT_COMMIT).await?;
        self.unit_open = false;
        // Marked only now: a table whose unit did not commit has not had its
        // one full feed, and marking before the commit would refuse the retry.
        self.single_unit_done.extend(staged_nonempty);

        // The redelivery window: the data IS durable and the caller is about
        // to be told it failed. Recovery has to find the receipt and publish
        // nothing rather than re-running the unit — the one crash that cannot
        // be handled by undoing anything.
        if let Some(injected) = crash_at("sf.receipt.visible") {
            self.discard_staged().await;
            return Err(injected);
        }

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

/// The shared advice for a merge failure, when the code says it applies.
///
/// Split from the error plumbing so the DECISION — which code earns the
/// diagnosis, and what the diagnosis says — is testable without a service
/// error to carry it. Nothing here reads a message: the code is the signal,
/// and the wording around it is the service's to change.
fn merge_diagnosis(table: &str, key: &[String], code: Option<&str>) -> Option<String> {
    (code? == client::DUPLICATE_ROW_IN_DML).then(|| {
        rdlt_connector_sqlcore::names::duplicate_merge_key_diagnosis(
            table,
            key,
            &format!("Snowflake error {}", client::DUPLICATE_ROW_IN_DML),
        )
    })
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A duplicate merge key earns the SHARED advice, keyed on the code.
    ///
    /// The same sentence every SQL destination gives: which strategy needs the
    /// uniqueness, which columns collide, and the two ways out. An operator
    /// meeting this on Snowflake reads what they would have read on Postgres —
    /// and the service's own wording is kept as the cause rather than replacing
    /// the advice.
    #[test]
    fn the_duplicate_key_code_becomes_the_shared_diagnosis() {
        let key = vec!["id".to_string(), "day".to_string()];
        let diagnosis = merge_diagnosis("orders", &key, Some(client::DUPLICATE_ROW_IN_DML))
            .expect("the duplicate code earns advice");
        assert_eq!(
            diagnosis,
            rdlt_connector_sqlcore::names::duplicate_merge_key_diagnosis(
                "orders",
                &key,
                &format!("Snowflake error {}", client::DUPLICATE_ROW_IN_DML)
            ),
            "the advice must be the shared one, not a Snowflake paraphrase"
        );
        assert!(diagnosis.contains("id, day"), "{diagnosis}");
        assert!(diagnosis.contains("delete_insert"), "{diagnosis}");
    }

    /// Every other failure passes through untouched.
    ///
    /// Replacing an unrelated error with merge advice would send an operator
    /// looking for a duplicate key that is not there — worse than saying
    /// nothing, because it reads as a diagnosis.
    #[test]
    fn an_unrelated_failure_keeps_its_own_error() {
        for code in [None, Some("000904"), Some("002003")] {
            assert!(
                merge_diagnosis("orders", &["id".to_string()], code).is_none(),
                "{code:?} must not be rewritten as a duplicate-key diagnosis"
            );
        }
    }
}
