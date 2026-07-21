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
use super::{copy_error, encode, fatal, quote, transient};

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
                // Review F6: configuring hard_delete on a CHILD table was
                // silently inert — reject typed instead (M4: flags live on
                // the ROOT row of a shredded stream).
                if is_child {
                    return Err(fatal(format!(
                        "table `{table}`: hard_delete applies to the ROOT table of a \
                         shredded stream — configure it on the root, not the child"
                    )));
                }
                // M4: the flag column must exist on THIS table's schema.
                if schema.column(col).is_none() {
                    return Err(fatal(format!(
                        "hard_delete column `{col}` is not a column of table `{table}`"
                    )));
                }
            }
            // Feature 010 (MR6): both refinement options are keyed-structured
            // only, their columns must exist, and they may not repurpose the
            // hard_delete flag. (A collision with scd2 validity columns is
            // unreachable: validity names may not be stream columns [S1] while
            // these options' columns MUST be — S1 fires first.)
            if let Some(dedup) = self.options.dedup_sort_for(table) {
                if has_identity {
                    return Err(fatal(format!(
                        "table `{table}`: dedup_sort requires a KEYED structured \
                         stream (contract merge-refinements.md MR6) — a shredded \
                         stream's identity is a content hash, ordered survivors \
                         are meaningless there"
                    )));
                }
                if schema.column(&dedup.column).is_none() {
                    return Err(fatal(format!(
                        "dedup_sort column `{}` is not a column of table `{table}`",
                        dedup.column
                    )));
                }
                if self.options.hard_delete_for(table) == Some(dedup.column.as_str()) {
                    return Err(fatal(format!(
                        "table `{table}`: dedup_sort column `{}` is the hard_delete \
                         flag — use a distinct ordering column (contract \
                         merge-refinements.md MR6)",
                        dedup.column
                    )));
                }
            }
            if let Some(scope) = self.options.merge_key_for(table) {
                if has_identity {
                    return Err(fatal(format!(
                        "table `{table}`: merge_key requires a KEYED structured \
                         stream (contract merge-refinements.md MR6) — shredded \
                         streams replace by root subtree"
                    )));
                }
                if strategy == MergeStrategy::Scd2 {
                    // Belt: parse-time validation already rejects this; direct
                    // struct construction must not slip past it.
                    return Err(fatal(format!(
                        "table `{table}`: merge_key is not valid with scd2 \
                         (contract merge-refinements.md MR6)"
                    )));
                }
                for col in scope {
                    if schema.column(col).is_none() {
                        return Err(fatal(format!(
                            "merge_key column `{col}` is not a column of table `{table}`"
                        )));
                    }
                    if self.options.hard_delete_for(table) == Some(col.as_str()) {
                        return Err(fatal(format!(
                            "table `{table}`: merge_key column `{col}` is the \
                             hard_delete flag — a deletion flag is not a scope \
                             (contract merge-refinements.md MR6)"
                        )));
                    }
                }
            }
            if strategy == MergeStrategy::Upsert && has_identity {
                // Review F4 / contract M7 (amended): a shredded stream's
                // _rdlt_id is a CONTENT hash for keyless streams — updates
                // mint new ids and ON CONFLICT never fires, silently
                // duplicating. The destination cannot distinguish keyed from
                // keyless shredded streams, so upsert is keyed-structured
                // only; shredded streams keep delete_insert (subtree
                // replacement).
                return Err(fatal(format!(
                    "table `{table}`: the upsert strategy requires a KEYED \
                     structured stream (contract merge-strategies.md M2/M7) — \
                     shredded streams use delete_insert"
                )));
            }
            if strategy == MergeStrategy::Scd2 {
                if has_identity {
                    return Err(fatal(format!(
                        "table `{table}`: scd2 requires a KEYED structured stream \
                         (contract scd2.md S1) — shredded streams have no declared key"
                    )));
                }
                let scd2 = self.options.scd2_for(table);
                for name in [&scd2.valid_from, &scd2.valid_to] {
                    if schema.column(name).is_some() {
                        return Err(fatal(format!(
                            "table `{table}`: scd2 validity column `{name}` collides \
                             with a stream column (contract scd2.md S1) — configure \
                             different names"
                        )));
                    }
                }
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
            // Index plan (data-model.md): identity per table kind.
            let mut indexes: Vec<(bool, Vec<String>)> = Vec::new();
            if has_identity {
                indexes.push((false, vec![system_columns::ID.to_string()]));
                if is_child {
                    indexes.push((false, vec![system_columns::ROOT_ID.to_string()]));
                }
            } else {
                match strategy {
                    MergeStrategy::Upsert => indexes.push((true, key.clone())),
                    MergeStrategy::DeleteInsert => indexes.push((false, key.clone())),
                    MergeStrategy::Scd2 => {
                        // (key…, valid_to): active-version lookups + retire.
                        let scd2 = self.options.scd2_for(table);
                        let mut cols = key.clone();
                        cols.push(scd2.valid_to.clone());
                        indexes.push((false, cols));
                    }
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
                    // Feature 010 (MR3–MR5): scope replacement runs BEFORE
                    // the strategy arm, first-touch-per-load.
                    if let Some(scope) = self.options.merge_key_for(table.as_str()) {
                        scope_replace(
                            &tx,
                            meta.load_id.as_str(),
                            table.as_str(),
                            &target,
                            &stage,
                            scope,
                            !load_committed_before,
                        )
                        .await?;
                    }
                    let plan = MergePlan {
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
                                Some(HardDelete::new(col, root_schema))
                            }),
                        dedup_sort: self.options.dedup_sort_for(table.as_str()),
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
                            let scd2 = self.options.scd2_for(table.as_str());
                            // Review F2: absent-retire compares against ONE
                            // commit unit's stage; a load split across units
                            // would mass-retire keys published by earlier
                            // units. Sound without an end-of-load hook (SPI
                            // frozen): retire only in a load's FIRST unit,
                            // and fail typed if a later unit arrives.
                            if scd2.absent == super::config::AbsentPolicy::Retire
                                && load_committed_before
                            {
                                return Err(fatal(format!(
                                    "table `{table}`: scd2 `absent: retire` requires the \
                                     load's full feed in a SINGLE commit unit (contract \
                                     scd2.md S6) — raise the engine commit thresholds"
                                )));
                            }
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
    root_stage: String,
    is_child: bool,
    hard_delete: Option<HardDelete>,
    /// Feature 010 (MR1): ordered in-load survivor selection; None keeps
    /// arrival-order last-wins.
    dedup_sort: Option<&'a super::config::DedupSort>,
}

impl MergePlan<'_> {
    fn key_list(&self) -> String {
        self.key
            .iter()
            .map(|k| quote(k))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// In-batch dedup over the stage: arrival-order last-wins (finding #7),
    /// or — feature 010 MR1 — ordered survivor selection when `dedup_sort`
    /// is declared. Values beat NULL (`NULLS LAST` both directions); the
    /// arrival column stays as the trailing tie-breaker, so ties and
    /// all-NULL groups keep the deterministic last-wins. EVERY strategy's
    /// survivor decision flows through here (MR2).
    fn deduped(&self, identity: &str) -> String {
        let sort = match self.dedup_sort {
            Some(d) => {
                let dir = match d.order {
                    super::config::SortOrder::Asc => "ASC",
                    super::config::SortOrder::Desc => "DESC",
                };
                format!("{} {dir} NULLS LAST, ", quote(&d.column))
            }
            None => String::new(),
        };
        format!(
            "(SELECT DISTINCT ON ({identity}) * FROM {} ORDER BY {identity}, {sort}{} DESC)",
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

    /// Roots flagged for hard deletion — decided from the DEDUPED last-wins
    /// root row (review F3: reading the RAW stage disagreed with survival
    /// when a root was flagged then re-created in the same load).
    fn flagged_roots(&self) -> Option<String> {
        self.hard_delete.as_ref().map(|hd| {
            let id = quote(system_columns::ID);
            format!(
                "(SELECT {id} FROM (SELECT DISTINCT ON ({id}) * FROM {} \
                 ORDER BY {id}, {} DESC) d WHERE {})",
                self.root_stage,
                quote(ARRIVAL_COL),
                hd.flagged
            )
        })
    }
}

/// Scope replacement (feature 010, contract merge-refinements.md MR3–MR5):
/// delete every target row whose scope matches a DELIVERED, UNRECEIPTED
/// stage scope, then receipt the delivered scopes — all inside the publish
/// transaction. Receipts make multi-commit-unit loads sound (each scope
/// replaced at most ONCE per load; later units never destroy earlier
/// units' rows — the 008 S6/F2 bug class, designed out). NULL is not a
/// scope: stage rows with any NULL scope column are excluded explicitly,
/// and target-side row comparison is never TRUE against NULL (MR4).
/// Committed-unit redelivery exits before merge SQL (D3), so a receipt
/// can never double-fire.
async fn scope_replace(
    tx: &tokio_postgres::Transaction<'_>,
    load_id: &str,
    table_name: &str,
    target: &str,
    stage: &str,
    scope: &[String],
    first_unit: bool,
) -> Result<(), DestError> {
    let cols = scope
        .iter()
        .map(|c| quote(c))
        .collect::<Vec<_>>()
        .join(", ");
    let not_null = scope
        .iter()
        .map(|c| format!("{} IS NOT NULL", quote(c)))
        .collect::<Vec<_>>()
        .join(" AND ");
    if first_unit {
        // Hygiene (MR5): other loads' receipts for this table are stale the
        // moment a new load first touches it — same moment replace's
        // truncate-once guard uses.
        tx.execute(
            "DELETE FROM _rdlt_scope_receipts WHERE table_name = $1 AND load_id <> $2",
            &[&table_name, &load_id],
        )
        .await
        .map_err(transient)?;
    }
    tx.execute(
        &format!(
            "DELETE FROM {target} WHERE ({cols}) IN (
                 SELECT {cols} FROM {stage}
                 WHERE {not_null}
                   AND ROW({cols})::text NOT IN (
                       SELECT scope FROM _rdlt_scope_receipts
                       WHERE load_id = $1 AND table_name = $2))"
        ),
        &[&load_id, &table_name],
    )
    .await
    .map_err(transient)?;
    tx.execute(
        &format!(
            "INSERT INTO _rdlt_scope_receipts
             SELECT DISTINCT $1, $2, ROW({cols})::text FROM {stage} WHERE {not_null}
             ON CONFLICT DO NOTHING"
        ),
        &[&load_id, &table_name],
    )
    .await
    .map_err(transient)?;
    Ok(())
}

/// Keyed structured delete-insert (the 006 arm + M4 hard delete).
///
/// NULL keys: the `(key) IN (...)` predicate is NULL-blind by SQL semantics,
/// but NULL merge-key VALUES cannot reach this code — the engine rejects
/// them typed at write time (feature 006, `rdlt-engine/src/load/mod.rs`
/// structured_merge_keys guard, conformance-pinned). Direct SPI drivers
/// bypassing the engine inherit that contract obligation.
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

// ---- Feature 008 US3: SCD2 (contract scd2.md) ----

/// Retire-changed-then-insert with NULL-safe column-wise change detection.
/// One boundary per commit unit: `now()` is the TRANSACTION timestamp, so
/// every statement in this publish sees the same instant (S5); redelivery
/// re-executes nothing (D3 receipts).
async fn scd2_merge(
    tx: &tokio_postgres::Transaction<'_>,
    plan: &MergePlan<'_>,
    scd2: &super::config::Scd2Options,
) -> Result<(), DestError> {
    let (target, cols) = (plan.target, plan.cols);
    let key_list = plan.key_list();
    let deduped = plan.deduped(&key_list);
    let vf = quote(&scd2.valid_from);
    let vt = quote(&scd2.valid_to);
    let key_match = plan
        .key
        .iter()
        .map(|k| format!("t.{q} = d.{q}", q = quote(k)))
        .collect::<Vec<_>>()
        .join(" AND ");
    // Change detection (S3): NULL-safe, over the DATA columns — the key is
    // the identity and the load-id changes every load by construction.
    let changed = plan
        .schema
        .columns
        .iter()
        .filter(|c| !plan.key.contains(&c.name) && c.name != system_columns::LOAD_ID)
        .map(|c| format!("t.{q} IS DISTINCT FROM d.{q}", q = quote(&c.name)))
        .collect::<Vec<_>>()
        .join(" OR ");

    // S3 retire: active versions whose key arrives with DIFFERENT values.
    if !changed.is_empty() {
        tx.batch_execute(&format!(
            "UPDATE {target} t SET {vt} = now() \
             FROM {deduped} d WHERE t.{vt} IS NULL AND {key_match} AND ({changed})"
        ))
        .await
        .map_err(transient)?;
    }
    // S2/S3 insert: staged rows with NO remaining active version (changed
    // keys were just retired; unchanged keys still hold their identical
    // active version and are SKIPPED — no churn).
    tx.batch_execute(&format!(
        "INSERT INTO {target} ({cols}, {vf}, {vt}) \
         SELECT {cols}, now(), NULL FROM {deduped} d \
         WHERE NOT EXISTS ( \
             SELECT 1 FROM {target} t WHERE t.{vt} IS NULL AND {key_match})"
    ))
    .await
    .map_err(transient)?;
    // S6: full-feed absence semantics on request.
    if scd2.absent == super::config::AbsentPolicy::Retire {
        tx.batch_execute(&format!(
            "UPDATE {target} t SET {vt} = now() \
             WHERE t.{vt} IS NULL AND ({key_list}) NOT IN \
                   (SELECT {key_list} FROM {} d)",
            plan.deduped(&key_list),
        ))
        .await
        .map_err(transient)?;
    }
    Ok(())
}
