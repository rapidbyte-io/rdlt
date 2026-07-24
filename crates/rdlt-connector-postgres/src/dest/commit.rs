//! The load-session protocol: staging COPY, the publish transaction, merge
//! arms, receipts, state.

use std::collections::BTreeMap;

use async_trait::async_trait;
use rdlt_connector::{
    CommitMeta, CommitReceipt, DestinationError, LoadSession, RecordBatch, WriteMode,
    core::{PipelineId, StateDoc, TableName, TableSchema, crash_point, schema::system_columns},
};
use tokio_postgres::Client;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::{ToSql, Type};

use rdlt_connector_sqlcore::plan::{self as sqlplan, IndexSpec, TableFacts, scope_replace_sql};
use rdlt_connector_sqlcore::{
    CommitCtx, MergeDialect, Step, build_merge_plan, commit_script, insert_select_sql, render_arm,
    staged_probe_targets,
};

use super::config::MergeStrategy;
use super::dialect::PgDialect;
use super::{classify_stmt, encode, fatal, quote, transient};

/// Arrival-order column on STAGE tables only: makes merge dedup deterministic
/// ("last wins" for real). Excluded from publish column lists because it
/// is not part of the logical schema.
pub const ARRIVAL_COL: &str = "__rdlt_arrival";

/// Stage names are pipeline-scoped and hashed: scoping stops one pipeline's `open`
/// from truncating another's live staged rows in a shared schema, and
/// hashing bounds the identifier under Postgres's 63-byte limit, where silent
/// truncation would otherwise cut off exactly the disambiguation suffix.
pub(super) fn stage_prefix(pipeline: &PipelineId) -> String {
    format!(
        "{}{}_",
        rdlt_connector_sqlcore::names::STAGE_PREFIX,
        rdlt_connector::core::naming::ident_hash(pipeline.as_str(), 8)
    )
}

pub(super) fn stage_name(pipeline: &PipelineId, table: &TableName) -> String {
    format!(
        "{}{}",
        stage_prefix(pipeline),
        rdlt_connector::core::naming::ident_hash(table.as_str(), 16)
    )
}

/// Quoted, comma-joined logical columns — the shared sqlcore rule; publishes
/// are ALWAYS by name.
pub(super) use rdlt_connector_sqlcore::column_list;

pub(super) struct PgSession {
    pub(super) client: Client,
    pub(super) pipeline: PipelineId,
    pub(super) tables: BTreeMap<TableName, (TableSchema, WriteMode)>,
    pub(super) options: super::config::DestOptions,
    /// Single-unit discipline, PER TABLE: tables whose stage has already
    /// published non-empty in an earlier commit unit of THIS load.
    /// Session-scoped is load-scoped: a session spans one engine run = one
    /// load; a crash starts both afresh. Marked only AFTER the unit's
    /// transaction commits (a rolled-back unit never counts), and re-marked
    /// on the replay branch (a committed unit whose outcome the client never
    /// learned still counts).
    pub(super) single_unit_done: std::collections::BTreeSet<TableName>,
}

impl PgSession {
    fn root_of(&self, table: &TableName) -> TableName {
        rdlt_connector_sqlcore::root_of(&self.tables, table)
    }
}

/// Execute one planned [`Step`] in the publish transaction. Free-standing so it
/// can borrow the session fields disjointly from `client`, which the live
/// transaction holds mutably. Every decision + the order come from the planner;
/// this renders each step's SQL through the PgDialect seam + shared renderers.
/// Execute one planned step. Failures CLASSIFY by SQLSTATE (shared rule
/// with the duckdb executor): environmental errors ride the engine's
/// retry budget; deterministic classes (22/23/42) — a duplicate receipt's
/// unique violation included, the idempotence-anomaly signal — fail
/// loudly instead of burning retries.
async fn execute_step(
    tx: &tokio_postgres::Transaction<'_>,
    pipeline: &PipelineId,
    tables: &BTreeMap<TableName, (TableSchema, WriteMode)>,
    options: &super::config::DestOptions,
    roots: &BTreeMap<TableName, TableName>,
    meta: &CommitMeta,
    step: &Step,
) -> Result<(), DestinationError> {
    match step {
        Step::ClearTarget { table } => {
            tx.batch_execute(&PgDialect.clear_table(&quote(table.as_str())))
                .await
                .map_err(classify_stmt)?;
        }
        Step::InsertSelect { table } => {
            let (schema, _) = &tables[table];
            let target = quote(table.as_str());
            let stage = quote(&stage_name(pipeline, table));
            tx.batch_execute(&insert_select_sql(&target, &column_list(schema), &stage))
                .await
                .map_err(classify_stmt)?;
        }
        Step::ScopeReplace { table, scope } => {
            let target = quote(table.as_str());
            let stage = quote(&stage_name(pipeline, table));
            tx.batch_execute(&scope_replace_sql(&PgDialect, &target, &stage, scope))
                .await
                .map_err(classify_stmt)?;
        }
        Step::MergeArm { table, arm } => {
            let (schema, mode) = &tables[table];
            let WriteMode::Merge { key } = mode else {
                // The planner emits MergeArm only for merge tables.
                return Err(fatal(format!(
                    "internal: merge arm planned for non-merge table `{table}`"
                )));
            };
            let root = &roots[table];
            let target = quote(table.as_str());
            let stage = quote(&stage_name(pipeline, table));
            let cols = column_list(schema);
            let root_stage = quote(&stage_name(pipeline, root));
            let root_schema = tables.get(root).map(|(s, _)| s);
            let dialect = PgDialect;
            let plan = build_merge_plan(
                &dialect,
                options,
                table,
                schema,
                key,
                &target,
                &stage,
                &cols,
                root,
                root_stage,
                root_schema,
            );
            for sql in render_arm(&plan, arm) {
                tx.batch_execute(&sql).await.map_err(classify_stmt)?;
            }
        }
        Step::TruncateStage { table } => {
            tx.batch_execute(&PgDialect.clear_table(&quote(&stage_name(pipeline, table))))
                .await
                .map_err(classify_stmt)?;
        }
        Step::UpsertState => {
            // State travels in the SAME transaction as the data.
            let doc = serde_json::to_string(&meta.state).map_err(fatal)?;
            tx.execute(
                &format!(
                    "INSERT INTO {} VALUES ($1, $2)
             ON CONFLICT (pipeline) DO UPDATE SET doc = EXCLUDED.doc",
                    rdlt_connector_sqlcore::names::STATE_TABLE
                ),
                &[&meta.state.pipeline.as_str(), &doc],
            )
            .await
            .map_err(classify_stmt)?;
        }
        Step::InsertReceipt => {
            tx.execute(
                &format!(
                    "INSERT INTO {} VALUES ($1, $2)",
                    rdlt_connector_sqlcore::names::COMMITS_TABLE
                ),
                &[&meta.load_id.as_str(), &(meta.commit_seq as i64)],
            )
            .await
            .map_err(classify_stmt)?;
        }
    }
    Ok(())
}

#[async_trait]
impl LoadSession for PgSession {
    async fn ensure_table(
        &mut self,
        schema: &TableSchema,
        mode: &WriteMode,
    ) -> Result<(), DestinationError> {
        let previous = self.tables.get(&schema.table).map(|(s, _)| s.clone());
        for (name, is_stage) in [
            (schema.table.as_str().to_owned(), false),
            (stage_name(&self.pipeline, &schema.table), true),
        ] {
            let mut columns = schema
                .columns
                .iter()
                .map(|c| super::ddl::column_def(c, !is_stage))
                .collect::<Vec<_>>()
                .join(", ");
            if is_stage {
                // Arrival order for deterministic merge dedup.
                columns.push_str(&format!(", {} BIGSERIAL", quote(ARRIVAL_COL)));
            }
            let unlogged = if is_stage { "UNLOGGED " } else { "" };
            self.client
                .batch_execute(&format!(
                    "CREATE {unlogged}TABLE IF NOT EXISTS {} ({columns})",
                    quote(&name)
                ))
                .await
                .map_err(classify_stmt)?;
            // Migrations: add new columns; widen with a USING cast.
            for column in &schema.columns {
                self.client
                    .batch_execute(&format!(
                        "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}",
                        quote(&name),
                        quote(&column.name),
                        super::ddl::sql_type(&column.column_type)
                    ))
                    .await
                    .map_err(classify_stmt)?;
                if let Some(prev) = &previous
                    && let Some(old) = prev.column(&column.name)
                    && old.column_type != column.column_type
                {
                    self.client
                        .batch_execute(&format!(
                            "ALTER TABLE {} ALTER COLUMN {} TYPE {} USING {}::{}",
                            quote(&name),
                            quote(&column.name),
                            super::ddl::sql_type(&column.column_type),
                            quote(&column.name),
                            super::ddl::sql_type(&column.column_type)
                        ))
                        .await
                        .map_err(classify_stmt)?;
                }
            }
        }
        // The option-vs-mode rules live in sqlcore — one rule set, identical
        // typed errors on every SQL destination.
        if !matches!(mode, WriteMode::Merge { .. }) {
            sqlplan::validate_non_merge(&self.options, schema.table.as_str()).map_err(fatal)?;
        }
        // Strategy validation + the supporting/unique indexes, ensured WITH
        // the table.
        if let WriteMode::Merge { key } = mode {
            let table = schema.table.as_str();
            let strategy = self.options.strategy_for(table);
            let has_identity = schema.columns.iter().any(|c| c.name == system_columns::ID);
            let is_child = schema.parent.is_some();
            // The option checks live in sqlcore; errors keep their exact
            // pinned text.
            sqlplan::validate_merge(
                &self.options,
                table,
                key,
                &TableFacts {
                    schema,
                    has_identity,
                    is_child,
                },
            )
            .map_err(fatal)?;
            if strategy == MergeStrategy::Scd2 {
                let scd2 = self.options.scd2_for(table);
                // Validity columns on the TARGET only (the stage carries the
                // stream's shape); additive for pre-existing scd2 tables.
                for (col, extra) in [
                    (&scd2.valid_from, "TIMESTAMPTZ NOT NULL DEFAULT now()"),
                    (&scd2.valid_to, "TIMESTAMPTZ"),
                ] {
                    self.client
                        .batch_execute(&format!(
                            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {extra}",
                            quote(table),
                            quote(col)
                        ))
                        .await
                        .map_err(classify_stmt)?;
                }
            }
            // Index plan — shared shape from sqlcore; this destination owns
            // only the SQL text.
            for IndexSpec { unique, columns } in
                sqlplan::index_plan(&self.options, table, key, has_identity, is_child)
            {
                let sql = super::ddl::create_index_sql(unique, table, &columns);
                if let Err(e) = self.client.batch_execute(&sql).await {
                    // Pre-existing duplicate keys under upsert — typed,
                    // naming the key columns.
                    if unique
                        && e.as_db_error()
                            .is_some_and(|db| db.code().code() == "23505")
                    {
                        return Err(fatal(format!(
                            "table `{table}`: cannot create the unique index the upsert \
                             strategy requires — existing rows duplicate the merge key \
                             ({}); deduplicate the table or use delete_insert: {}",
                            columns.join(", "),
                            super::describe(&e)
                        )));
                    }
                    return Err(classify_stmt(e));
                }
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
        crash_point!(
            "pg.stage.copy",
            Err(DestinationError::fatal("injected crash at pg.stage.copy"))
        );
        let stage = stage_name(&self.pipeline, table);
        let arrow_schema = batch.schema();
        let column_names = arrow_schema
            .fields()
            .iter()
            .map(|f| quote(f.name()))
            .collect::<Vec<_>>()
            .join(", ");
        // Wire decisions come from the ENSURED schema's logical types;
        // arrow representation only fills in where the schema is silent.
        let table_schema = self.tables.get(table).map(|(s, _)| s);
        let wires: Vec<encode::ColumnWire> = arrow_schema
            .fields()
            .iter()
            .map(|f| {
                let logical = table_schema
                    .and_then(|s| s.column(f.name()))
                    .map(|c| &c.column_type);
                encode::column_wire(logical, f.data_type())
            })
            .collect::<Result<_, _>>()?;
        let types: Vec<Type> = wires.iter().map(|w| encode::wire_type(*w)).collect();

        let sink = self
            .client
            .copy_in(&format!(
                "COPY {} ({column_names}) FROM STDIN BINARY",
                quote(&stage)
            ))
            .await
            .map_err(classify_stmt)?;
        let writer = BinaryCopyInWriter::new(sink, &types);
        futures::pin_mut!(writer);

        for row_idx in 0..batch.num_rows() {
            let mut owned: Vec<Box<dyn ToSql + Sync + Send>> =
                Vec::with_capacity(batch.num_columns());
            for (col_idx, field) in arrow_schema.fields().iter().enumerate() {
                let array = batch.column(col_idx);
                owned.push(encode::cell_value(
                    wires[col_idx],
                    array.as_ref(),
                    row_idx,
                    field.name(),
                )?);
            }
            let refs: Vec<&(dyn ToSql + Sync)> = owned
                .iter()
                .map(|b| b.as_ref() as &(dyn ToSql + Sync))
                .collect();
            writer.as_mut().write(&refs).await.map_err(classify_stmt)?;
        }
        writer.finish().await.map_err(classify_stmt)?;
        Ok(())
    }

    async fn commit(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestinationError> {
        let receipt = CommitReceipt {
            load_id: meta.load_id.clone(),
            commit_seq: meta.commit_seq,
        };
        let roots: BTreeMap<TableName, TableName> = self
            .tables
            .keys()
            .map(|t| (t.clone(), self.root_of(t)))
            .collect();

        crash_point!(
            "pg.publish.begin",
            Err(DestinationError::fatal(
                "injected crash at pg.publish.begin"
            ))
        );
        let tx = self.client.transaction().await.map_err(transient)?;
        // Idempotence by (load_id, commit_seq).
        let replayed = tx
            .query_one(
                &format!(
                    "SELECT count(*) FROM {} WHERE load_id = $1 AND commit_seq = $2",
                    rdlt_connector_sqlcore::names::COMMITS_TABLE
                ),
                &[&meta.load_id.as_str(), &(meta.commit_seq as i64)],
            )
            .await
            .map_err(transient)?
            .get::<_, i64>(0)
            > 0;
        // Replace truncates at most once per LOAD, guarded DURABLY from the
        // receipt log — a crash-recovery session (fresh memory, same load) must
        // never re-truncate rows an earlier commit already published (the
        // parquet destination had the same latent data-loss bug; same fix,
        // same reasoning).
        let load_committed_before = tx
            .query_one(
                &format!(
                    "SELECT count(*) FROM {} WHERE load_id = $1",
                    rdlt_connector_sqlcore::names::COMMITS_TABLE
                ),
                &[&meta.load_id.as_str()],
            )
            .await
            .map_err(transient)?
            .get::<_, i64>(0)
            > 0;

        // Probe the full-feed stages the planner needs. Staged-row counts are
        // INVARIANT across the publish (no stage is written during it — merges
        // read stages, publishes write targets), so probing up front matches the
        // former lazy per-table check.
        let mut staged_nonempty = std::collections::BTreeSet::new();
        for table in staged_probe_targets(&self.tables, &self.options) {
            let stage = quote(&stage_name(&self.pipeline, table));
            let staged: bool = tx
                .query_one(&format!("SELECT EXISTS (SELECT 1 FROM {stage})"), &[])
                .await
                .map_err(transient)?
                .get(0);
            if staged {
                staged_nonempty.insert(table.clone());
            }
        }

        // The planner owns every decision + the ordering; this session executes.
        let script = commit_script(
            &self.tables,
            &self.options,
            &CommitCtx {
                replayed,
                load_committed_before,
                single_unit_done: &self.single_unit_done,
                staged_nonempty: &staged_nonempty,
            },
        )
        .map_err(fatal)?;

        for step in &script.steps {
            execute_step(
                &tx,
                &self.pipeline,
                &self.tables,
                &self.options,
                &roots,
                &meta,
                step,
            )
            .await?;
        }

        // The canonical redelivery window: on a fresh unit everything is
        // published in ONE server-side transaction, so a crash at either edge of
        // tx.commit() must replay idempotently — the injected error models the
        // client dying without learning the outcome. A replay unit only
        // truncated stages and carried no receipt/state edge (it was never
        // instrumented), so the crash point stays confined to the fresh path.
        if !replayed {
            crash_point!(
                "pg.tx.commit",
                Err(DestinationError::fatal("injected crash at pg.tx.commit"))
            );
        }
        tx.commit().await.map_err(transient)?;
        // Applied only after the unit's transaction committed (a rolled-back
        // unit never counts).
        self.single_unit_done.extend(script.marks);
        Ok(receipt)
    }

    async fn read_state(
        &mut self,
        pipeline: &PipelineId,
    ) -> Result<Option<StateDoc>, DestinationError> {
        let row = self
            .client
            .query_opt(
                &format!(
                    "SELECT doc FROM {} WHERE pipeline = $1",
                    rdlt_connector_sqlcore::names::STATE_TABLE
                ),
                &[&pipeline.as_str()],
            )
            .await
            .map_err(transient)?;
        match row {
            Some(row) => {
                let doc: String = row.get(0);
                Ok(Some(serde_json::from_str(&doc).map_err(fatal)?))
            }
            None => Ok(None),
        }
    }
}
