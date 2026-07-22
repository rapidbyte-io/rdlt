//! The load-session protocol: staging COPY, the publish transaction, merge
//! arms, receipts, state. (Feature 008 T001: relocated verbatim; strategy
//! arms and the describe() error helper land in later tasks.)

use std::collections::BTreeMap;

use async_trait::async_trait;
use rdlt_connector::{
    CommitMeta, CommitReceipt, DestError, LoadSession, RecordBatch, WriteMode,
    core::{PipelineId, StateDoc, TableName, TableSchema, crash_point, schema::system_columns},
};
use tokio_postgres::Client;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::{ToSql, Type};

use rdlt_connector_sqlcore::plan::{
    self as sqlplan, TableFacts, identity_delete_insert_sql, keyed_delete_insert_sql,
    keyed_upsert_sql, scd2_merge_sql, scope_replace_sql,
};
use rdlt_connector_sqlcore::{HardDelete, MergePlan};

use super::config::MergeStrategy;
use super::dialect::PgDialect;
use super::{copy_error, encode, fatal, quote, transient};

/// Arrival-order column on STAGE tables only: makes merge dedup deterministic
/// ("last wins" for real — finding #7). Excluded from publish column lists because it
/// is not part of the logical schema.
pub const ARRIVAL_COL: &str = "__rdlt_arrival";

/// Stage names are pipeline-scoped and hashed: scoping stops one pipeline's `open`
/// from truncating another's live staged rows in a shared schema (finding #3), and
/// hashing bounds the identifier under Postgres's 63-byte limit, where silent
/// truncation used to cut off exactly the disambiguation suffix (finding #8).
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

/// Quoted, comma-joined logical columns — publishes are ALWAYS by name (finding #4).
pub(super) fn column_list(schema: &TableSchema) -> String {
    schema
        .columns
        .iter()
        .map(|c| quote(&c.name))
        .collect::<Vec<_>>()
        .join(", ")
}

pub(super) struct PgSession {
    pub(super) client: Client,
    pub(super) pipeline: PipelineId,
    pub(super) tables: BTreeMap<TableName, (TableSchema, WriteMode)>,
    pub(super) options: super::config::PgDestOptions,
    /// Single-unit discipline, PER TABLE (MR5 / scd2 S6, 010 review round):
    /// tables whose stage has already published non-empty in an earlier
    /// commit unit of THIS load. Session-scoped is load-scoped: a session
    /// spans one engine run = one load; a crash starts both afresh. Marked
    /// only AFTER the unit's transaction commits (a rolled-back unit never
    /// counts), and re-marked on the D3 replay branch (a committed unit
    /// whose outcome the client never learned still counts).
    pub(super) single_unit_done: std::collections::BTreeSet<TableName>,
}

impl PgSession {
    fn root_of(&self, table: &TableName) -> TableName {
        let mut current = table.clone();
        for _ in 0..64 {
            match self
                .tables
                .get(&current)
                .and_then(|(s, _)| s.parent.as_ref())
            {
                Some(link) => current = link.parent.clone(),
                None => break,
            }
        }
        current
    }
}

#[async_trait]
impl LoadSession for PgSession {
    async fn ensure_table(
        &mut self,
        schema: &TableSchema,
        mode: &WriteMode,
    ) -> Result<(), DestError> {
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
                // Arrival order for deterministic merge dedup (finding #7).
                columns.push_str(&format!(", {} BIGSERIAL", quote(ARRIVAL_COL)));
            }
            let unlogged = if is_stage { "UNLOGGED " } else { "" };
            self.client
                .batch_execute(&format!(
                    "CREATE {unlogged}TABLE IF NOT EXISTS {} ({columns})",
                    quote(&name)
                ))
                .await
                .map_err(transient)?;
            // Migrations (clause D5): add new columns; widen with a USING cast.
            for column in &schema.columns {
                self.client
                    .batch_execute(&format!(
                        "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}",
                        quote(&name),
                        quote(&column.name),
                        super::ddl::sql_type(&column.ty)
                    ))
                    .await
                    .map_err(transient)?;
                if let Some(prev) = &previous
                    && let Some(old) = prev.column(&column.name)
                    && old.ty != column.ty
                {
                    self.client
                        .batch_execute(&format!(
                            "ALTER TABLE {} ALTER COLUMN {} TYPE {} USING {}::{}",
                            quote(&name),
                            quote(&column.name),
                            super::ddl::sql_type(&column.ty),
                            quote(&column.name),
                            super::ddl::sql_type(&column.ty)
                        ))
                        .await
                        .map_err(transient)?;
                }
            }
        }
        // Feature 013: the option-vs-mode rules live in sqlcore (SM1) — one
        // rule set, identical typed errors on every SQL destination.
        if !matches!(mode, WriteMode::Merge { .. }) {
            sqlplan::validate_non_merge(&self.options, schema.table.as_str()).map_err(fatal)?;
        }
        // Feature 008 US2 (merge-strategies.md): strategy validation + the
        // supporting/unique indexes, ensured WITH the table (M3/M5).
        if let WriteMode::Merge { key } = mode {
            let table = schema.table.as_str();
            let strategy = self.options.strategy_for(table);
            let has_identity = schema.columns.iter().any(|c| c.name == system_columns::ID);
            let is_child = schema.parent.is_some();
            // Feature 013: the 008/010 option checks live in sqlcore (SM1);
            // errors keep their exact pinned text.
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
                        .map_err(transient)?;
                }
            }
            // Index plan (008 data-model + 010 scope index) — shared shape
            // (feature 013 SM1); this destination owns only the SQL text.
            for (unique, columns) in
                sqlplan::index_plan(&self.options, table, key, has_identity, is_child)
            {
                let sql = super::ddl::create_index_sql(unique, table, &columns);
                if let Err(e) = self.client.batch_execute(&sql).await {
                    // M3: pre-existing duplicate keys under upsert — typed,
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
                    return Err(transient(e));
                }
            }
        }
        self.tables
            .insert(schema.table.clone(), (schema.clone(), mode.clone()));
        Ok(())
    }

    async fn write(&mut self, table: &TableName, batch: RecordBatch) -> Result<(), DestError> {
        crash_point!(
            "pg.stage.copy",
            Err(DestError::fatal("injected crash at pg.stage.copy"))
        );
        let stage = stage_name(&self.pipeline, table);
        let arrow_schema = batch.schema();
        let column_names = arrow_schema
            .fields()
            .iter()
            .map(|f| quote(f.name()))
            .collect::<Vec<_>>()
            .join(", ");
        // Wire decisions come from the ENSURED schema's logical types (T6);
        // arrow representation only fills in where the schema is silent.
        let table_schema = self.tables.get(table).map(|(s, _)| s);
        let wires: Vec<encode::ColumnWire> = arrow_schema
            .fields()
            .iter()
            .map(|f| {
                let logical = table_schema.and_then(|s| s.column(f.name())).map(|c| &c.ty);
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
            .map_err(copy_error)?;
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
            writer.as_mut().write(&refs).await.map_err(copy_error)?;
        }
        writer.finish().await.map_err(copy_error)?;
        Ok(())
    }

    async fn commit(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestError> {
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
            Err(DestError::fatal("injected crash at pg.publish.begin"))
        );
        let tx = self.client.transaction().await.map_err(transient)?;
        // Clause D3: idempotence by (load_id, commit_seq).
        let already = tx
            .query_one(
                &format!(
                    "SELECT count(*) FROM {} WHERE load_id = $1 AND commit_seq = $2",
                    rdlt_connector_sqlcore::names::COMMITS_TABLE
                ),
                &[&meta.load_id.as_str(), &(meta.commit_seq as i64)],
            )
            .await
            .map_err(transient)?
            .get::<_, i64>(0);
        // Replace truncates at most once per LOAD, guarded DURABLY from the
        // receipt log — a crash-recovery session (fresh memory, same load) must
        // never re-truncate rows an earlier commit already published (the
        // parquet twin of this bug was the feature-002 review's confirmed
        // data-loss finding; same fix, same reasoning).
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
        if already > 0 {
            // D3 replay of a unit that DID commit server-side: the merge SQL
            // never re-runs, but the single-unit discipline must still count
            // this unit — the redelivered stage carries the same rows the
            // committed one did.
            for (table, (_, mode)) in &self.tables {
                if !matches!(mode, WriteMode::Merge { .. }) {
                    continue;
                }
                let scoped = self.options.merge_key_for(table.as_str()).is_some();
                let retire = self.options.strategy_for(table.as_str()) == MergeStrategy::Scd2
                    && self.options.scd2_for(table.as_str()).absent
                        == super::config::AbsentPolicy::Retire;
                if (scoped || retire) && !self.single_unit_done.contains(table) {
                    let stage = quote(&stage_name(&self.pipeline, table));
                    let staged: bool = tx
                        .query_one(&format!("SELECT EXISTS (SELECT 1 FROM {stage})"), &[])
                        .await
                        .map_err(transient)?
                        .get(0);
                    if staged {
                        self.single_unit_done.insert(table.clone());
                    }
                }
            }
            for table in self.tables.keys() {
                tx.batch_execute(&format!(
                    "TRUNCATE TABLE {}",
                    quote(&stage_name(&self.pipeline, table))
                ))
                .await
                .map_err(transient)?;
            }
            tx.commit().await.map_err(transient)?;
            return Ok(receipt);
        }
        // Applied only after THIS unit's transaction commits (see
        // `single_unit_done`).
        let mut single_unit_marks: Vec<TableName> = Vec::new();

        for (table, (schema, mode)) in &self.tables {
            // Feature 006: a schema without the per-row identity column is a
            // STRUCTURED stream's table — merge (if requested) goes by key.
            let schema_has_identity = schema.columns.iter().any(|c| c.name == system_columns::ID);
            let target = quote(table.as_str());
            let stage = quote(&stage_name(&self.pipeline, table));
            // Publishes are ALWAYS by name (finding #4) — and the list excludes the
            // stage-only arrival column.
            let cols = column_list(schema);
            match mode {
                WriteMode::Append => {
                    tx.batch_execute(&format!(
                        "INSERT INTO {target} ({cols}) SELECT {cols} FROM {stage}"
                    ))
                    .await
                    .map_err(transient)?;
                }
                WriteMode::Replace => {
                    if !load_committed_before {
                        tx.batch_execute(&format!("TRUNCATE TABLE {target}"))
                            .await
                            .map_err(transient)?;
                    }
                    tx.batch_execute(&format!(
                        "INSERT INTO {target} ({cols}) SELECT {cols} FROM {stage}"
                    ))
                    .await
                    .map_err(transient)?;
                }
                WriteMode::Merge { key } => {
                    // Feature 008 (merge-strategies.md): the strategy is
                    // destination config; the engine's mode stays frozen.
                    let strategy = self.options.strategy_for(table.as_str());
                    let scoped = self.options.merge_key_for(table.as_str());
                    let scd2 = (strategy == MergeStrategy::Scd2)
                        .then(|| self.options.scd2_for(table.as_str()));
                    let retire = scd2
                        .as_ref()
                        .is_some_and(|s| s.absent == super::config::AbsentPolicy::Retire);
                    // Single-unit discipline, PER TABLE (MR5 + scd2 S6, one
                    // shared rule): scope replacement and absent-retire each
                    // interpret the stage as "the complete truth" — sound only
                    // when THIS TABLE's full feed arrives in one commit unit.
                    // Per-table tracking (not `load_committed_before`): other
                    // streams' checkpoints legitimately split the LOAD into
                    // units without splitting this table's feed. A unit where
                    // this table stages NOTHING is skipped outright — which
                    // also stops an empty stage from reading as "every key
                    // absent" (retire = mass retirement). The crash residual
                    // is recorded in MR5: a scoped/retire stream that
                    // checkpoints MID-feed and crashes in the window resumes
                    // as a new load with a partial feed, which no
                    // destination-side bookkeeping can distinguish from a
                    // fresh load (this feature's own sweep killed the receipts
                    // scheme that tried).
                    if scoped.is_some() || retire {
                        let staged: bool = tx
                            .query_one(&format!("SELECT EXISTS (SELECT 1 FROM {stage})"), &[])
                            .await
                            .map_err(transient)?
                            .get(0);
                        if !staged {
                            continue; // nothing delivered for THIS table this unit
                        }
                        if self.single_unit_done.contains(table) {
                            // 013 review finding 9: cite the rule that FIRED —
                            // under scd2 the retire rule (S6) governs even
                            // when a merge_key scopes it.
                            return Err(fatal(sqlplan::single_unit_violation(
                                table.as_str(),
                                scoped.is_some() && !retire,
                            )));
                        }
                        single_unit_marks.push(table.clone());
                    }
                    // Feature 010 (MR3/MR4): scope replacement runs BEFORE
                    // the strategy arm, inside the same transaction. NOT for
                    // scd2 (013 G1): there the merge_key scopes RETIREMENT
                    // inside the strategy arm — deleting scope rows would
                    // destroy history.
                    if let Some(scope) = scoped
                        && strategy != MergeStrategy::Scd2
                    {
                        tx.batch_execute(&scope_replace_sql(&PgDialect, &target, &stage, scope))
                            .await
                            .map_err(transient)?;
                    }
                    let plan = MergePlan {
                        dialect: &PgDialect,
                        target: &target,
                        stage: &stage,
                        cols: &cols,
                        schema,
                        key,
                        root_stage: quote(&stage_name(&self.pipeline, &roots[table])),
                        is_child: table != &roots[table],
                        hard_delete: self
                            .options
                            .hard_delete_for(roots[table].as_str())
                            .and_then(|col| {
                                let root_schema = self.tables.get(&roots[table]).map(|(s, _)| s)?;
                                Some(HardDelete::new(col, root_schema, &PgDialect))
                            }),
                        dedup_sort: self.options.dedup_sort_for(table.as_str()),
                        merge_scope: scoped,
                    };
                    match (schema_has_identity, strategy) {
                        (false, MergeStrategy::DeleteInsert) => {
                            keyed_delete_insert(&tx, &plan).await?
                        }
                        (false, MergeStrategy::Upsert) => keyed_upsert(&tx, &plan).await?,
                        (true, MergeStrategy::DeleteInsert) => {
                            identity_delete_insert(&tx, &plan).await?
                        }
                        (true, MergeStrategy::Upsert) => {
                            // Unreachable: ensure_table rejected it (M7).
                            return Err(fatal(format!(
                                "table `{table}`: upsert on a shredded stream"
                            )));
                        }
                        (false, MergeStrategy::Scd2) => {
                            // 008 review F2's single-unit guard for
                            // absent-retire now lives in the shared per-table
                            // discipline above (010 review round).
                            let scd2 = scd2.expect("scd2 options resolved with the strategy");
                            scd2_merge(&tx, &plan, &scd2).await?
                        }
                        (true, MergeStrategy::Scd2) => {
                            // Unreachable: ensure_table rejected it (S1).
                            return Err(fatal(format!(
                                "table `{table}`: scd2 on a shredded stream"
                            )));
                        }
                    }
                }
            }
        }
        // Truncate stages only after ALL tables published: child-table merges read the
        // ROOT's stage for their delete-by-root-id subquery.
        for table in self.tables.keys() {
            tx.batch_execute(&format!(
                "TRUNCATE TABLE {}",
                quote(&stage_name(&self.pipeline, table))
            ))
            .await
            .map_err(transient)?;
        }

        // Clause D2: state travels in the SAME transaction as the data.
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
        .map_err(transient)?;
        tx.execute(
            &format!(
                "INSERT INTO {} VALUES ($1, $2)",
                rdlt_connector_sqlcore::names::COMMITS_TABLE
            ),
            &[&meta.load_id.as_str(), &(meta.commit_seq as i64)],
        )
        .await
        .map_err(transient)?;
        // The canonical redelivery window: everything published in ONE server-side
        // transaction; a crash at either edge of tx.commit() must replay
        // idempotently (D3) — the injected error models the client dying without
        // learning the outcome.
        crash_point!(
            "pg.tx.commit",
            Err(DestError::fatal("injected crash at pg.tx.commit"))
        );
        tx.commit().await.map_err(transient)?;
        self.single_unit_done.extend(single_unit_marks);
        Ok(receipt)
    }

    async fn read_state(&mut self, pipeline: &PipelineId) -> Result<Option<StateDoc>, DestError> {
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

// ---- Strategy executors (feature 013): the SQL layer lives in
// rdlt-connector-sqlcore (contract SM1/SM2); this destination executes the
// shared shapes' statements through the PgDialect and owns nothing else.

async fn keyed_delete_insert(
    tx: &tokio_postgres::Transaction<'_>,
    plan: &MergePlan<'_>,
) -> Result<(), DestError> {
    for sql in keyed_delete_insert_sql(plan) {
        tx.batch_execute(&sql).await.map_err(transient)?;
    }
    Ok(())
}

async fn keyed_upsert(
    tx: &tokio_postgres::Transaction<'_>,
    plan: &MergePlan<'_>,
) -> Result<(), DestError> {
    for sql in keyed_upsert_sql(plan) {
        tx.batch_execute(&sql).await.map_err(transient)?;
    }
    Ok(())
}

async fn identity_delete_insert(
    tx: &tokio_postgres::Transaction<'_>,
    plan: &MergePlan<'_>,
) -> Result<(), DestError> {
    for sql in identity_delete_insert_sql(plan) {
        tx.batch_execute(&sql).await.map_err(transient)?;
    }
    Ok(())
}

async fn scd2_merge(
    tx: &tokio_postgres::Transaction<'_>,
    plan: &MergePlan<'_>,
    scd2: &super::config::Scd2Options,
) -> Result<(), DestError> {
    for sql in scd2_merge_sql(plan, scd2) {
        tx.batch_execute(&sql).await.map_err(transient)?;
    }
    Ok(())
}
