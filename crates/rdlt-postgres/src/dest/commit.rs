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
                .map(|c| format!("{} {}", quote(&c.name), super::ddl::sql_type(&c.ty)))
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
        let types: Vec<Type> = arrow_schema
            .fields()
            .iter()
            .map(|f| encode::copy_type(f.data_type()))
            .collect::<Result<_, _>>()?;

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
                    field.data_type(),
                    array.as_ref(),
                    row_idx,
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
                WriteMode::Merge { key } if !schema_has_identity => {
                    // Keyed STRUCTURED merge (feature 006, merge-structured.md):
                    // delete-by-declared-key, then insert with deterministic
                    // last-wins in-batch dedup by the same key.
                    let key_list = key.iter().map(|k| quote(k)).collect::<Vec<_>>().join(", ");
                    tx.batch_execute(&format!(
                        "DELETE FROM {target} WHERE ({key_list}) IN (SELECT {key_list} FROM {stage})"
                    ))
                    .await
                    .map_err(transient)?;
                    tx.batch_execute(&format!(
                        "INSERT INTO {target} ({cols}) \
                         SELECT {cols} FROM ( \
                             SELECT DISTINCT ON ({key_list}) * FROM {stage} \
                             ORDER BY {key_list}, {arrival} DESC \
                         ) deduped",
                        arrival = quote(ARRIVAL_COL),
                    ))
                    .await
                    .map_err(transient)?;
                }
                WriteMode::Merge { .. } => {
                    let root = &roots[table];
                    let root_stage = quote(&stage_name(&self.pipeline, root));
                    let id_col = if table == root {
                        system_columns::ID
                    } else {
                        system_columns::ROOT_ID
                    };
                    // Subtree replacement by root id + DETERMINISTIC in-batch dedup:
                    // arrival order breaks ties, so "last wins" is real (finding #7).
                    tx.batch_execute(&format!(
                        "DELETE FROM {target} WHERE {} IN (SELECT {} FROM {root_stage})",
                        quote(id_col),
                        quote(system_columns::ID),
                    ))
                    .await
                    .map_err(transient)?;
                    tx.batch_execute(&format!(
                        "INSERT INTO {target} ({cols}) \
                         SELECT {cols} FROM ( \
                             SELECT DISTINCT ON ({id}) * FROM {stage} \
                             ORDER BY {id}, {arrival} DESC \
                         ) deduped",
                        id = quote(system_columns::ID),
                        arrival = quote(ARRIVAL_COL),
                    ))
                    .await
                    .map_err(transient)?;
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
