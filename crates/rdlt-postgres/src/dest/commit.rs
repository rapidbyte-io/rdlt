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

use super::config::MergeStrategy;
use super::{encode, fatal, quote, transient};

/// Arrival-order column on STAGE tables only: makes merge dedup deterministic
/// ("last wins" for real — finding #7). Excluded from publish column lists because it
/// is not part of the logical schema.
pub(super) const ARRIVAL_COL: &str = "__rdlt_arrival";

/// Stage names are pipeline-scoped and hashed: scoping stops one pipeline's `open`
/// from truncating another's live staged rows in a shared schema (finding #3), and
/// hashing bounds the identifier under Postgres's 63-byte limit, where silent
/// truncation used to cut off exactly the disambiguation suffix (finding #8).
pub(super) fn stage_prefix(pipeline: &PipelineId) -> String {
    format!(
        "_rdlt_stage_{}_",
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
        // Feature 008 US2 (merge-strategies.md): strategy validation + the
        // supporting/unique indexes, ensured WITH the table (M3/M5).
        if let WriteMode::Merge { key } = mode {
            let table = schema.table.as_str();
            let strategy = self.options.strategy_for(table);
            let has_identity = schema.columns.iter().any(|c| c.name == system_columns::ID);
            let is_child = schema.parent.is_some();
            if let Some(col) = self.options.hard_delete_for(table) {
                // M4: the flag column must exist on THIS table's schema.
                if schema.column(col).is_none() && !is_child {
                    return Err(fatal(format!(
                        "hard_delete column `{col}` is not a column of table `{table}`"
                    )));
                }
            }
            if strategy == MergeStrategy::Scd2 && has_identity {
                return Err(fatal(format!(
                    "table `{table}`: scd2 requires a KEYED structured stream \
                     (contract scd2.md S1) — shredded streams have no declared key"
                )));
            }
            // Index plan (data-model.md): identity per table kind.
            let mut indexes: Vec<(bool, Vec<String>)> = Vec::new();
            if has_identity {
                let unique = strategy == MergeStrategy::Upsert;
                indexes.push((unique, vec![system_columns::ID.to_string()]));
                if is_child {
                    indexes.push((false, vec![system_columns::ROOT_ID.to_string()]));
                }
            } else {
                match strategy {
                    MergeStrategy::Upsert => indexes.push((true, key.clone())),
                    MergeStrategy::DeleteInsert => indexes.push((false, key.clone())),
                    MergeStrategy::Scd2 => {} // scd2 index lands with its arm (T010)
                }
            }
            for (unique, columns) in indexes {
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
            .map_err(transient)?;
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
            writer.as_mut().write(&refs).await.map_err(transient)?;
        }
        writer.finish().await.map_err(transient)?;
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
                "SELECT count(*) FROM _rdlt_commits WHERE load_id = $1 AND commit_seq = $2",
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
                "SELECT count(*) FROM _rdlt_commits WHERE load_id = $1",
                &[&meta.load_id.as_str()],
            )
            .await
            .map_err(transient)?
            .get::<_, i64>(0)
            > 0;
        if already > 0 {
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
                    let plan = MergePlan {
                        target: &target,
                        stage: &stage,
                        cols: &cols,
                        schema,
                        key,
                        root_table: &roots[table],
                        root_stage: quote(&stage_name(&self.pipeline, &roots[table])),
                        is_child: table != &roots[table],
                        hard_delete: self
                            .options
                            .hard_delete_for(roots[table].as_str())
                            .and_then(|col| {
                                let root_schema = self.tables.get(&roots[table]).map(|(s, _)| s)?;
                                Some(HardDelete::new(col, root_schema))
                            }),
                    };
                    match (schema_has_identity, strategy) {
                        (false, MergeStrategy::DeleteInsert) => {
                            keyed_delete_insert(&tx, &plan).await?
                        }
                        (false, MergeStrategy::Upsert) => keyed_upsert(&tx, &plan).await?,
                        (true, MergeStrategy::DeleteInsert) => {
                            identity_delete_insert(&tx, &plan).await?
                        }
                        (true, MergeStrategy::Upsert) => identity_upsert(&tx, &plan).await?,
                        (_, MergeStrategy::Scd2) => {
                            // Arm lands with T010 (contract scd2.md); the
                            // ensure-time validation already rejected the
                            // shredded case (S1).
                            return Err(fatal(format!(
                                "table `{table}`: scd2 strategy not yet available \
                                 in this build"
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
            "INSERT INTO _rdlt_state VALUES ($1, $2)
             ON CONFLICT (pipeline) DO UPDATE SET doc = EXCLUDED.doc",
            &[&meta.state.pipeline.as_str(), &doc],
        )
        .await
        .map_err(transient)?;
        tx.execute(
            "INSERT INTO _rdlt_commits VALUES ($1, $2)",
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
        Ok(receipt)
    }

    async fn read_state(&mut self, pipeline: &PipelineId) -> Result<Option<StateDoc>, DestError> {
        let row = self
            .client
            .query_opt(
                "SELECT doc FROM _rdlt_state WHERE pipeline = $1",
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

// ---- Feature 008 US2: merge-strategy SQL (contract merge-strategies.md) ----

/// Hard-delete flag semantics (M4): boolean columns compare `IS TRUE`,
/// other types `IS NOT NULL` — both NULL-safe on the KEEP side.
struct HardDelete {
    flagged: String,
    keep: String,
}

impl HardDelete {
    fn new(column: &str, root_schema: &TableSchema) -> Self {
        use rdlt_connector::core::{ColumnType, LogicalType};
        let is_bool = matches!(
            root_schema.column(column).map(|c| &c.ty),
            Some(ColumnType::Scalar {
                scalar: LogicalType::Bool
            })
        );
        let col = quote(column);
        if is_bool {
            Self {
                flagged: format!("{col} IS TRUE"),
                keep: format!("{col} IS NOT TRUE"),
            }
        } else {
            Self {
                flagged: format!("{col} IS NOT NULL"),
                keep: format!("{col} IS NULL"),
            }
        }
    }
}

/// Everything a strategy arm needs about one table's publish.
struct MergePlan<'a> {
    target: &'a str,
    stage: &'a str,
    cols: &'a str,
    schema: &'a TableSchema,
    key: &'a [String],
    root_table: &'a TableName,
    root_stage: String,
    is_child: bool,
    hard_delete: Option<HardDelete>,
}

impl MergePlan<'_> {
    fn key_list(&self) -> String {
        self.key
            .iter()
            .map(|k| quote(k))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Last-wins in-batch dedup over the stage (finding #7).
    fn deduped(&self, identity: &str) -> String {
        format!(
            "(SELECT DISTINCT ON ({identity}) * FROM {} ORDER BY {identity}, {} DESC)",
            self.stage,
            quote(ARRIVAL_COL)
        )
    }

    /// `SET c = EXCLUDED.c, …` over the non-identity columns.
    fn update_set(&self, identity: &[String]) -> String {
        self.schema
            .columns
            .iter()
            .filter(|c| !identity.contains(&c.name))
            .map(|c| format!("{q} = EXCLUDED.{q}", q = quote(&c.name)))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Roots flagged for hard deletion in THIS load's root stage.
    fn flagged_roots(&self) -> Option<String> {
        self.hard_delete.as_ref().map(|hd| {
            format!(
                "(SELECT {} FROM {} WHERE {})",
                quote(system_columns::ID),
                self.root_stage,
                hd.flagged
            )
        })
    }
}

/// Keyed structured delete-insert (the 006 arm + M4 hard delete).
async fn keyed_delete_insert(
    tx: &tokio_postgres::Transaction<'_>,
    plan: &MergePlan<'_>,
) -> Result<(), DestError> {
    let (target, stage, cols) = (plan.target, plan.stage, plan.cols);
    let key_list = plan.key_list();
    tx.batch_execute(&format!(
        "DELETE FROM {target} WHERE ({key_list}) IN (SELECT {key_list} FROM {stage})"
    ))
    .await
    .map_err(transient)?;
    let keep = plan
        .hard_delete
        .as_ref()
        .map(|hd| format!(" WHERE {}", hd.keep))
        .unwrap_or_default();
    tx.batch_execute(&format!(
        "INSERT INTO {target} ({cols}) SELECT {cols} FROM {} deduped{keep}",
        plan.deduped(&key_list),
    ))
    .await
    .map_err(transient)?;
    Ok(())
}

/// Keyed structured upsert (M2): conflict-update on the merge key.
async fn keyed_upsert(
    tx: &tokio_postgres::Transaction<'_>,
    plan: &MergePlan<'_>,
) -> Result<(), DestError> {
    let (target, cols) = (plan.target, plan.cols);
    let key_list = plan.key_list();
    if let Some(hd) = &plan.hard_delete {
        tx.batch_execute(&format!(
            "DELETE FROM {target} WHERE ({key_list}) IN \
             (SELECT {key_list} FROM {} d WHERE {})",
            plan.deduped(&key_list),
            hd.flagged
        ))
        .await
        .map_err(transient)?;
    }
    let keep = plan
        .hard_delete
        .as_ref()
        .map(|hd| format!(" WHERE {}", hd.keep))
        .unwrap_or_default();
    let set = plan.update_set(plan.key);
    let action = if set.is_empty() {
        "DO NOTHING".to_string()
    } else {
        format!("DO UPDATE SET {set}")
    };
    tx.batch_execute(&format!(
        "INSERT INTO {target} ({cols}) SELECT {cols} FROM {} deduped{keep} \
         ON CONFLICT ({key_list}) {action}",
        plan.deduped(&key_list),
    ))
    .await
    .map_err(transient)?;
    Ok(())
}

/// Shredded identity delete-insert (the original arm + M4 hard delete).
async fn identity_delete_insert(
    tx: &tokio_postgres::Transaction<'_>,
    plan: &MergePlan<'_>,
) -> Result<(), DestError> {
    let (target, cols) = (plan.target, plan.cols);
    let id = quote(system_columns::ID);
    let id_col = if plan.is_child {
        quote(system_columns::ROOT_ID)
    } else {
        id.clone()
    };
    // Subtree replacement by root id + DETERMINISTIC in-batch dedup:
    // arrival order breaks ties, so "last wins" is real (finding #7).
    tx.batch_execute(&format!(
        "DELETE FROM {target} WHERE {id_col} IN (SELECT {id} FROM {})",
        plan.root_stage
    ))
    .await
    .map_err(transient)?;
    // Hard delete (M4): flagged ROOTS drop from the root insert; their
    // children drop by root-id membership.
    let keep = match (&plan.hard_delete, plan.is_child) {
        (Some(hd), false) => format!(" WHERE {}", hd.keep),
        (Some(_), true) => format!(
            " WHERE {} NOT IN {}",
            quote(system_columns::ROOT_ID),
            plan.flagged_roots().expect("hard_delete present")
        ),
        (None, _) => String::new(),
    };
    tx.batch_execute(&format!(
        "INSERT INTO {target} ({cols}) SELECT {cols} FROM {} deduped{keep}",
        plan.deduped(&id),
    ))
    .await
    .map_err(transient)?;
    Ok(())
}

/// Shredded identity upsert (M2): per-row conflict-update on `_rdlt_id`,
/// plus subtree hygiene — orphaned children of staged roots are removed
/// (a re-shredded root may carry fewer children than before).
async fn identity_upsert(
    tx: &tokio_postgres::Transaction<'_>,
    plan: &MergePlan<'_>,
) -> Result<(), DestError> {
    let (target, cols) = (plan.target, plan.cols);
    let id = quote(system_columns::ID);
    if plan.is_child {
        // Orphan + hard-delete hygiene: children of staged roots that are
        // not re-staged disappear (flagged roots re-stage nothing).
        tx.batch_execute(&format!(
            "DELETE FROM {target} WHERE {root_id} IN (SELECT {id} FROM {root_stage}) \
             AND {id} NOT IN (SELECT {id} FROM {stage})",
            root_id = quote(system_columns::ROOT_ID),
            root_stage = plan.root_stage,
            stage = plan.stage,
        ))
        .await
        .map_err(transient)?;
    } else if let Some(flagged) = plan.flagged_roots() {
        tx.batch_execute(&format!("DELETE FROM {target} WHERE {id} IN {flagged}"))
            .await
            .map_err(transient)?;
    }
    let keep = match (&plan.hard_delete, plan.is_child) {
        (Some(hd), false) => format!(" WHERE {}", hd.keep),
        (Some(_), true) => format!(
            " WHERE {} NOT IN {}",
            quote(system_columns::ROOT_ID),
            plan.flagged_roots().expect("hard_delete present")
        ),
        (None, _) => String::new(),
    };
    let identity = vec![system_columns::ID.to_string()];
    let set = plan.update_set(&identity);
    let action = if set.is_empty() {
        "DO NOTHING".to_string()
    } else {
        format!("DO UPDATE SET {set}")
    };
    tx.batch_execute(&format!(
        "INSERT INTO {target} ({cols}) SELECT {cols} FROM {} deduped{keep} \
         ON CONFLICT ({id}) {action}",
        plan.deduped(&id),
    ))
    .await
    .map_err(transient)?;
    Ok(())
}
