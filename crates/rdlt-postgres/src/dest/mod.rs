//! # rdlt-dest-postgres — PostgreSQL destination
//!
//! Binary-protocol COPY into unlogged staging tables; publication is one transaction
//! moving stage → target, upserting the state document, and recording the commit
//! receipt (clauses D1–D4). Receives FLATTENED schemas — `structs: false` makes the
//! engine lower nested objects at the seam. Depends on the SPI only.

use std::collections::BTreeMap;

use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Float64Array, Int64Array, StringArray,
    TimestampMicrosecondArray,
};
use arrow_schema::{DataType, TimeUnit};
use async_trait::async_trait;
use rdlt_connector::{
    CommitMeta, CommitReceipt, ConnectorSpec, DestCapabilities, DestError, Destination,
    LoadSession, OpenCtx, RecordBatch, WriteMode,
    core::{
        ColumnType, LogicalType, PipelineId, StateDoc, TableName, TableSchema, naming::IdentRules,
        schema::system_columns,
    },
};
use tokio_postgres::Client;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::{ToSql, Type};

#[derive(Debug, Clone)]
pub struct Postgres {
    conn_string: String,
    schema: String,
    tls: Option<crate::tls::TlsPolicy>,
}

impl Postgres {
    /// `conn_string`: e.g. `host=localhost user=postgres password=pw dbname=raw`.
    pub fn connect(conn_string: impl Into<String>) -> Self {
        Self {
            conn_string: conn_string.into(),
            schema: "public".into(),
            tls: None,
        }
    }

    /// Target schema (dataset); created if missing.
    pub fn dataset(mut self, schema: impl Into<String>) -> Self {
        self.schema = schema.into();
        self
    }

    /// TLS posture (feature 006, contract tls-policy.md) — the SAME policy
    /// type the source uses; the two directions share one connect path.
    pub fn tls(mut self, policy: crate::tls::TlsPolicy) -> Self {
        self.tls = Some(policy);
        self
    }

    async fn client(&self) -> Result<Client, DestError> {
        let parsed: tokio_postgres::Config = self
            .conn_string
            .parse()
            .map_err(|e| DestError::fatal(format!("conn string does not parse: {e}")))?;
        let policy = crate::tls::resolve_policy(&parsed, self.tls.as_ref())
            .map_err(|e| DestError::fatal(e.to_string()))?;
        match crate::tls::connect(&parsed, &policy).await {
            Ok(client) => Ok(client),
            Err(crate::tls::ConnectResult::Config(e)) => Err(DestError::fatal(e.to_string())),
            Err(crate::tls::ConnectResult::Connect(e)) if e.transient => Err(transient(e)),
            Err(crate::tls::ConnectResult::Connect(e)) => Err(DestError::fatal(e.to_string())),
        }
    }
}

use rdlt_connector::core::crash_point;

/// Fail-point registry (gate G2.2): every `crash_point!` site in this crate —
/// the ENGINE-OWNED protocol boundaries (stage writes, the publish transaction
/// edges, the D3 redelivery window). Postgres' internal transaction atomicity
/// is the database's own guarantee and is deliberately NOT instrumented
/// (research R20 scope guard).
#[cfg(feature = "failpoints")]
#[doc(hidden)]
pub const FAIL_POINTS: &[&str] = &["pg.stage.copy", "pg.publish.begin", "pg.tx.commit"];

fn transient(e: impl std::fmt::Display) -> DestError {
    DestError::transient(e.to_string())
}

fn fatal(e: impl std::fmt::Display) -> DestError {
    DestError::fatal(e.to_string())
}

fn quote(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Arrival-order column on STAGE tables only: makes merge dedup deterministic
/// ("last wins" for real — finding #7). Excluded from publish column lists because it
/// is not part of the logical schema.
const ARRIVAL_COL: &str = "__rdlt_arrival";

/// Stage names are pipeline-scoped and hashed: scoping stops one pipeline's `open`
/// from truncating another's live staged rows in a shared schema (finding #3), and
/// hashing bounds the identifier under Postgres's 63-byte limit, where silent
/// truncation used to cut off exactly the disambiguation suffix (finding #8).
fn stage_prefix(pipeline: &PipelineId) -> String {
    format!(
        "_rdlt_stage_{}_",
        rdlt_connector::core::naming::ident_hash(pipeline.as_str(), 8)
    )
}

fn stage_name(pipeline: &PipelineId, table: &TableName) -> String {
    format!(
        "{}{}",
        stage_prefix(pipeline),
        rdlt_connector::core::naming::ident_hash(table.as_str(), 16)
    )
}

/// Quoted, comma-joined logical columns — publishes are ALWAYS by name (finding #4).
fn column_list(schema: &TableSchema) -> String {
    schema
        .columns
        .iter()
        .map(|c| quote(&c.name))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Postgres SQL type per (already lowered) column type.
fn sql_type(ty: &ColumnType) -> &'static str {
    match ty {
        ColumnType::Scalar { scalar } => match scalar {
            LogicalType::Bool => "BOOLEAN",
            LogicalType::Int64 => "BIGINT",
            LogicalType::Float64 => "DOUBLE PRECISION",
            LogicalType::Utf8 | LogicalType::Uuid => "TEXT",
            LogicalType::Json => "TEXT", // engine ships Json as text; JSONB is follow-up
            LogicalType::Binary => "BYTEA",
            LogicalType::TimestampTz => "TIMESTAMPTZ",
            LogicalType::TimestampNaive => "TIMESTAMP",
            LogicalType::Date => "DATE",
            LogicalType::Time => "TIME",
            // decimal: false — the engine lowers decimals to text before we see them.
            LogicalType::Decimal { .. } => "TEXT",
        },
        // structs/lists never reach a structless destination (engine-lowered).
        ColumnType::Struct { .. } | ColumnType::ScalarList { .. } => "TEXT",
    }
}

/// Postgres wire type for binary COPY, per arrow column type.
fn copy_type(dt: &DataType) -> Result<Type, DestError> {
    Ok(match dt {
        DataType::Boolean => Type::BOOL,
        DataType::Int64 => Type::INT8,
        DataType::Float64 => Type::FLOAT8,
        DataType::Utf8 => Type::TEXT,
        DataType::Binary => Type::BYTEA,
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => Type::TIMESTAMPTZ,
        DataType::Timestamp(TimeUnit::Microsecond, None) => Type::TIMESTAMP,
        DataType::Date32 => Type::DATE,
        other => return Err(fatal(format!("unsupported arrow type for COPY: {other}"))),
    })
}

#[async_trait]
impl Destination for Postgres {
    fn spec(&self) -> ConnectorSpec {
        ConnectorSpec::new("postgres", env!("CARGO_PKG_VERSION"))
    }

    fn capabilities(&self) -> DestCapabilities {
        DestCapabilities {
            merge: true,
            structs: false,      // → engine flattens collision-safely at the seam
            scalar_lists: false, // → scalar lists become child tables at shred planning
            json_type: false,
            decimal: false, // → engine lowers decimals to canonical text
            ident_rules: IdentRules { max_len: 63 },
        }
    }

    async fn open(&self, _ctx: OpenCtx) -> Result<Box<dyn LoadSession>, DestError> {
        let client = self.client().await?;
        let schema = quote(&self.schema);
        client
            .batch_execute(&format!(
                "CREATE SCHEMA IF NOT EXISTS {schema};
                 SET search_path TO {schema};
                 CREATE TABLE IF NOT EXISTS _rdlt_state (pipeline TEXT PRIMARY KEY, doc TEXT);
                 CREATE TABLE IF NOT EXISTS _rdlt_commits (
                     load_id TEXT, commit_seq BIGINT, PRIMARY KEY (load_id, commit_seq));"
            ))
            .await
            .map_err(transient)?;

        // Clause D4: staged data from THIS PIPELINE's dead sessions becomes
        // invisible/reclaimable. Scoped by pipeline-hash prefix: other pipelines
        // sharing the schema keep their live staged rows (finding #3).
        let prefix_pattern = format!("{}%", stage_prefix(&_ctx.pipeline).replace('_', "\\_"));
        let stale: Vec<String> = client
            .query(
                "SELECT tablename FROM pg_tables
                 WHERE schemaname = $1 AND tablename LIKE $2",
                &[&self.schema, &prefix_pattern],
            )
            .await
            .map_err(transient)?
            .into_iter()
            .map(|row| row.get::<_, String>(0))
            .collect();
        for table in stale {
            client
                .batch_execute(&format!("TRUNCATE TABLE {}", quote(&table)))
                .await
                .map_err(transient)?;
        }

        Ok(Box::new(PgSession {
            client,
            pipeline: _ctx.pipeline,
            tables: BTreeMap::new(),
        }))
    }
}

struct PgSession {
    client: Client,
    pipeline: PipelineId,
    tables: BTreeMap<TableName, (TableSchema, WriteMode)>,
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
                .map(|c| format!("{} {}", quote(&c.name), sql_type(&c.ty)))
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
                        sql_type(&column.ty)
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
                            sql_type(&column.ty),
                            quote(&column.name),
                            sql_type(&column.ty)
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
            .map(|f| copy_type(f.data_type()))
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
                owned.push(cell_value(field.data_type(), array.as_ref(), row_idx)?);
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

/// One cell as an owned ToSql value for binary COPY.
fn cell_value(
    dt: &DataType,
    array: &dyn Array,
    row: usize,
) -> Result<Box<dyn ToSql + Sync + Send>, DestError> {
    macro_rules! cast {
        ($ty:ty) => {
            array
                .as_any()
                .downcast_ref::<$ty>()
                .ok_or_else(|| fatal("array type mismatch"))?
        };
    }
    if array.is_null(row) {
        // Binary COPY checks the ToSql type against the column's wire type — a NULL
        // must still be typed correctly.
        return Ok(match dt {
            DataType::Boolean => Box::new(Option::<bool>::None),
            DataType::Int64 => Box::new(Option::<i64>::None),
            DataType::Float64 => Box::new(Option::<f64>::None),
            DataType::Utf8 => Box::new(Option::<String>::None),
            DataType::Binary => Box::new(Option::<Vec<u8>>::None),
            DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => {
                Box::new(Option::<chrono::DateTime<chrono::Utc>>::None)
            }
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                Box::new(Option::<chrono::NaiveDateTime>::None)
            }
            DataType::Date32 => Box::new(Option::<chrono::NaiveDate>::None),
            other => return Err(fatal(format!("unsupported arrow type for COPY: {other}"))),
        });
    }
    Ok(match dt {
        DataType::Boolean => Box::new(cast!(BooleanArray).value(row)),
        DataType::Int64 => Box::new(cast!(Int64Array).value(row)),
        DataType::Float64 => Box::new(cast!(Float64Array).value(row)),
        DataType::Utf8 => Box::new(cast!(StringArray).value(row).to_owned()),
        DataType::Binary => Box::new(cast!(BinaryArray).value(row).to_owned()),
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => {
            let micros = cast!(TimestampMicrosecondArray).value(row);
            Box::new(
                chrono::DateTime::<chrono::Utc>::from_timestamp_micros(micros)
                    .ok_or_else(|| fatal("timestamp out of range"))?,
            )
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let micros = cast!(TimestampMicrosecondArray).value(row);
            Box::new(
                chrono::DateTime::from_timestamp_micros(micros)
                    .ok_or_else(|| fatal("timestamp out of range"))?
                    .naive_utc(),
            )
        }
        DataType::Date32 => {
            let days = cast!(Date32Array).value(row);
            Box::new(
                chrono::NaiveDate::from_num_days_from_ce_opt(days + 719_163)
                    .ok_or_else(|| fatal("date out of range"))?,
            )
        }
        other => return Err(fatal(format!("unsupported arrow type for COPY: {other}"))),
    })
}
