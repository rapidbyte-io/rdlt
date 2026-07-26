//! # PostgreSQL destination
//!
//! Binary-protocol COPY into unlogged staging tables; publication is one transaction
//! moving stage → target, upserting the state document, and recording the commit
//! receipt. Receives FLATTENED schemas — `structs: false` makes the
//! engine lower nested objects at the seam. Depends on the SPI only.
//!
//! Module layout (source-mirroring, all crate-private): `config` the
//! handle/builder, `ddl` type mapping + table DDL, `encode` the binary-COPY
//! wire encoding, `commit` the load-session protocol.
//!
//! # What a unit transaction costs
//!
//! Append and Replace rows COPY straight into their target inside one
//! transaction per commit unit, instead of landing in a stage table and being
//! moved by `INSERT … SELECT` at publish. Every row is written once rather
//! than twice. Merge is unchanged: its arms join delivered rows against the
//! target, so it genuinely needs the stage.
//!
//! That trade has four consequences worth knowing before running rdlt against
//! a busy database. None of them blocks a load; all of them are about what
//! ELSE the database can do while one is running.
//!
//! - **A Replace target is locked for the whole load, not just the publish.**
//!   `TRUNCATE` takes ACCESS EXCLUSIVE and holds it until the unit commits.
//!   Under the old staged publish the target was locked for the publish alone
//!   — on a 1M-row load, roughly 740 ms. It is now locked from the first batch
//!   to the commit. Readers of that table block for that whole window.
//! - **Vacuum falls behind while a unit is open.** The transaction holds its
//!   `xmin` for its lifetime, and that pins the oldest row version the whole
//!   DATABASE may reclaim — not just this table's. A long load therefore
//!   delays cleanup everywhere.
//! - **A stalled load holds both at once.** A load blocked on a slow source
//!   keeps the target's ACCESS EXCLUSIVE lock and the vacuum horizon, having
//!   written nothing recently. Commit cadence is the control: more frequent
//!   commit units mean shorter transactions and shorter locks.
//! - **Constraint violations surface at `write`, not at publish.** The server
//!   enforces the target's constraints during the COPY, so a bad row fails at
//!   the batch that carried it and names the row. Under staging the row landed
//!   in a permissive stage first and failed later, at `INSERT … SELECT`.

mod commit;
mod config;
mod ddl;
mod dialect;
mod encode;

use async_trait::async_trait;
use rdlt_connector::{
    ConnectorSpec, Destination, DestinationCapabilities, DestinationError, LoadSession, OpenCtx,
    core::naming::IdentRules,
};

pub use config::{
    AbsentPolicy, DedupSort, DestOptions, MergeStrategy, Postgres, Scd2Options, SortOrder,
    TableOptions,
};

/// SQL-generation seam, exposed ONLY for the golden-SQL pin suite: the pins
/// bind the exact statement text across the sqlcore extraction. Not a public
/// API.
#[doc(hidden)]
pub mod sqlgen {
    pub use super::commit::{ARRIVAL_COL, UNIT_BEGIN, UNIT_COMMIT, UNIT_ROLLBACK, UNIT_WORK_MEM};
    pub use super::dialect::PgDialect;
    pub use rdlt_connector_sqlcore::plan::{
        identity_delete_insert_sql, keyed_delete_insert_sql, keyed_upsert_sql, scd2_merge_sql,
        scope_replace_sql,
    };
    pub use rdlt_connector_sqlcore::{HardDelete, MergePlan};
}

/// Encoding seam, exposed ONLY for the byte-identity pin and the gated
/// encoder bench. Not a public API.
///
/// The pin exists because the wire encoder is replaceable but its OUTPUT is
/// not: Postgres binary COPY carries no per-field type tag, so a value encoded
/// one byte differently is either a loud server-side format error or, worse, a
/// silently different value. The fixture captures what the encoder emits for
/// every wire kind at boundary values, so any rewrite is checked against bytes
/// rather than against intent.
#[doc(hidden)]
pub mod testhook {
    use arrow_array::{
        Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array,
        RecordBatch, StringArray, Time64MicrosecondArray, TimestampMicrosecondArray, UInt32Array,
    };
    use arrow_schema::{Field, Schema};
    use bytes::BytesMut;
    use rdlt_connector::core::{ColumnType, LogicalType};
    use std::sync::Arc;

    use super::encode::{ColumnEncoder, column_wire};

    /// One column of the pin/bench batch: name, the LOGICAL type when the
    /// arrow representation alone would not reach the wire kind (Utf8 covers
    /// text, jsonb and uuid), and values chosen to include NULL plus whatever
    /// the type's edges are.
    struct PinColumn {
        name: &'static str,
        logical: Option<ColumnType>,
        array: Arc<dyn Array>,
    }

    fn scalar(t: LogicalType) -> Option<ColumnType> {
        Some(ColumnType::Scalar { scalar: t })
    }

    /// Every `ColumnWire` variant, in declaration order.
    fn columns() -> Vec<PinColumn> {
        let col = |name, logical, array| PinColumn {
            name,
            logical,
            array,
        };
        vec![
            col(
                "c_bool",
                None,
                Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])) as Arc<dyn Array>,
            ),
            col(
                "c_int8",
                None,
                Arc::new(Int64Array::from(vec![Some(i64::MIN), Some(i64::MAX), None])),
            ),
            col(
                "c_float8",
                None,
                Arc::new(Float64Array::from(vec![
                    Some(f64::MIN_POSITIVE),
                    Some(-0.0),
                    None,
                ])),
            ),
            col(
                "c_text",
                None,
                Arc::new(StringArray::from(vec![
                    Some(""),
                    Some("héllo\u{1F600}"),
                    None,
                ])),
            ),
            col(
                "c_bytea",
                None,
                Arc::new(BinaryArray::from_opt_vec(vec![
                    Some(&b""[..]),
                    Some(&b"\x00\xff\x7f"[..]),
                    None,
                ])),
            ),
            col(
                "c_timestamptz",
                None,
                Arc::new(
                    TimestampMicrosecondArray::from(vec![Some(0), Some(-1), None])
                        .with_timezone("UTC"),
                ),
            ),
            col(
                "c_timestamp",
                None,
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(0),
                    Some(-1),
                    None,
                ])),
            ),
            col(
                "c_date",
                None,
                Arc::new(Date32Array::from(vec![Some(0), Some(-1), None])),
            ),
            col(
                "c_time",
                None,
                Arc::new(Time64MicrosecondArray::from(vec![
                    Some(0),
                    Some(86_399_999_999),
                    None,
                ])),
            ),
            col(
                "c_numeric",
                scalar(LogicalType::Decimal {
                    precision: 38,
                    scale: 9,
                }),
                Arc::new(
                    Decimal128Array::from(vec![Some(0i128), Some(-123_456_789_012_345_678), None])
                        .with_precision_and_scale(38, 9)
                        .expect("precision/scale"),
                ),
            ),
            col(
                "c_jsonb",
                scalar(LogicalType::Json),
                Arc::new(StringArray::from(vec![
                    Some("{}"),
                    Some(r#"{"a":[1,null,"é"]}"#),
                    None,
                ])),
            ),
            col(
                "c_uuid",
                scalar(LogicalType::Uuid),
                Arc::new(StringArray::from(vec![
                    Some("00000000-0000-0000-0000-000000000000"),
                    Some("ffffffff-ffff-ffff-ffff-ffffffffffff"),
                    None,
                ])),
            ),
        ]
    }

    /// Arrow field for a pin column: the field the destination would see.
    fn field_of(c: &PinColumn) -> Field {
        Field::new(c.name, c.array.data_type().clone(), true)
    }

    /// The batch the pin and the bench both encode. `rows` cycles the value
    /// vectors so the bench has volume; the pin uses the natural length.
    pub fn bench_batch(rows: usize) -> RecordBatch {
        let cols = columns();
        let schema = Arc::new(Schema::new(cols.iter().map(field_of).collect::<Vec<_>>()));
        let arrays: Vec<Arc<dyn Array>> = cols
            .iter()
            .map(|c| {
                let idx = UInt32Array::from(
                    (0..rows)
                        .map(|i| u32::try_from(i % c.array.len()).expect("index fits"))
                        .collect::<Vec<u32>>(),
                );
                arrow_select::take::take(c.array.as_ref(), &idx, None).expect("take")
            })
            .collect();
        RecordBatch::try_new(schema, arrays).expect("bench batch")
    }

    /// Encode the pin batch's VALUES through the shipping path, returning
    /// `(column, per-row wire bytes or NULL)`.
    ///
    /// The encoder emits length-prefixed FIELDS, so the prefix is read back
    /// off each one here: `-1` is NULL, otherwise the declared length must
    /// account for exactly the bytes that follow — which pins the prefix as
    /// well as the value. What is rendered stays value-bytes-only, so the
    /// fixture is a stable oracle across the encoder rewrite.
    pub fn encode_pin_values() -> Vec<(String, Vec<Option<Vec<u8>>>)> {
        let cols = columns();
        let rows = cols[0].array.len();
        cols.iter()
            .map(|c| {
                let wire =
                    column_wire(c.logical.as_ref(), c.array.data_type()).expect("supported wire");
                let encoder =
                    ColumnEncoder::new(wire, c.array.as_ref(), c.name).expect("column encodable");
                let cells = (0..rows)
                    .map(|row| {
                        let mut buf = BytesMut::new();
                        encoder
                            .encode_field(row, c.name, &mut buf)
                            .expect("encodable");
                        let (prefix, value) = buf.split_at(4);
                        let declared = i32::from_be_bytes(prefix.try_into().expect("4 bytes"));
                        if declared < 0 {
                            assert_eq!(declared, -1, "{}: NULL is spelled -1", c.name);
                            assert!(value.is_empty(), "{}: NULL carries no bytes", c.name);
                            return None;
                        }
                        assert_eq!(
                            declared as usize,
                            value.len(),
                            "{}: field length prefix disagrees with the bytes written",
                            c.name
                        );
                        Some(value.to_vec())
                    })
                    .collect();
                (c.name.to_string(), cells)
            })
            .collect()
    }

    /// The gated encoder hot path (bench body): a whole batch through the
    /// production encoding path — downcast once per column, fields appended
    /// into one reused buffer. Returns the byte count so the work cannot be
    /// optimized away.
    ///
    /// Note when comparing against the recorded pre-rewrite baseline: this
    /// body also writes the 4-byte length prefix the old one did not, so it
    /// does slightly MORE work per cell. Any improvement it shows is a
    /// conservative lower bound.
    pub fn bench_encode(batch: &RecordBatch) -> u64 {
        let schema = batch.schema();
        let mut buf = BytesMut::with_capacity(64 * 1024);
        let mut bytes = 0u64;
        // Wires come from the SAME logical types the pin uses. Resolving them
        // from the arrow type alone would bench `c_jsonb` and `c_uuid` as
        // plain text — so the jsonb version byte and the uuid parser, both
        // per-cell work in production, would never appear in the instruction
        // count and a regression in either would pass the 3% gate untouched.
        let logical = columns();
        let encoders: Vec<ColumnEncoder<'_>> = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, field)| {
                let column = logical
                    .iter()
                    .find(|c| c.name == field.name())
                    .expect("bench batch columns come from `columns()`");
                let wire = column_wire(column.logical.as_ref(), field.data_type())
                    .expect("supported wire");
                ColumnEncoder::new(wire, batch.column(idx).as_ref(), field.name())
                    .expect("column encodable")
            })
            .collect();
        for row in 0..batch.num_rows() {
            for (idx, encoder) in encoders.iter().enumerate() {
                buf.clear();
                encoder
                    .encode_field(row, schema.field(idx).name(), &mut buf)
                    .expect("encodable");
                bytes += buf.len() as u64;
            }
        }
        bytes
    }
}

/// Fail-point registry: every `crash_point!` site in this crate — the
/// ENGINE-OWNED protocol boundaries of a commit unit, in the order a unit
/// reaches them. Postgres' internal transaction atomicity is the database's
/// own guarantee and is deliberately NOT instrumented.
///
/// - `pg.unit.begin` — before the unit transaction opens
/// - `pg.target.clear` — before a Replace target is cleared, inside that
///   transaction; a crash here must leave the target's OLD rows intact
/// - `pg.unit.write` — before a batch is written (into the target directly,
///   or into a stage for merge)
/// - `pg.publish.begin` — at `commit`, before the first publish step
/// - `pg.tx.commit` — the redelivery window: the client dies without learning
///   whether the transaction committed
/// - `pg.tx.acked` — the same window from the other side: the transaction HAS
///   committed durably and the client dies before acting on it, so recovery
///   must return the existing receipt rather than publish a second time
#[cfg(feature = "failpoints")]
#[doc(hidden)]
pub const FAIL_POINTS: &[&str] = &[
    "pg.unit.begin",
    "pg.target.clear",
    "pg.unit.write",
    "pg.publish.begin",
    "pg.tx.commit",
    "pg.tx.acked",
];

/// Render a driver error with its server message + SQLSTATE — the shared
/// rendering both connectors use (tokio-postgres's own Display for a db error
/// is just "db error"; non-db errors render their full source chain).
pub(crate) fn describe(e: &tokio_postgres::Error) -> String {
    crate::pgerror::pg_error_detail(e)
}

pub(crate) fn transient(e: tokio_postgres::Error) -> DestinationError {
    DestinationError::transient(describe(&e))
}

/// Statement-error classification shared by the COPY write path AND table DDL:
/// data-shaped SQLSTATE classes (22 data exception, 23 integrity, 42
/// syntax/access) are PERMANENT — a poisoned batch or an unwinnable 42xxx DDL
/// statement must not burn the engine's retry budget on retries that cannot
/// win. Everything else stays transient (connection-shaped).
pub(crate) fn classify_stmt(e: tokio_postgres::Error) -> DestinationError {
    match e.as_db_error() {
        Some(db) if crate::pgerror::is_permanent_statement_sqlstate(db.code().code()) => {
            DestinationError::fatal(describe(&e))
        }
        _ => DestinationError::transient(describe(&e)),
    }
}

pub(crate) fn fatal(e: impl std::fmt::Display) -> DestinationError {
    DestinationError::fatal(e.to_string())
}

pub(crate) fn quote(ident: &str) -> String {
    // The one injection-safe quoting rule, shared with every SQL destination
    // (and the dialect seam's default). Kept as a thin local alias so the many
    // DDL/publish call sites read `quote(...)`.
    rdlt_connector_sqlcore::quote_ident(ident)
}

#[async_trait]
impl Destination for Postgres {
    fn spec(&self) -> ConnectorSpec {
        ConnectorSpec::new("postgres", env!("CARGO_PKG_VERSION"))
    }

    fn capabilities(&self) -> DestinationCapabilities {
        DestinationCapabilities {
            merge: true,
            structs: false,      // → engine flattens collision-safely at the seam
            scalar_lists: false, // → scalar lists become child tables at shred planning
            // Native JSONB + NUMERIC(p,s) — engine lowering passes
            // Json/Decimal128 through untouched. These are CODE-LEVEL
            // declarations; no user configuration exists.
            json_type: true,
            decimal: true,
            ident_rules: IdentRules { max_len: 63 },
        }
    }

    async fn open(&self, _ctx: OpenCtx) -> Result<Box<dyn LoadSession>, DestinationError> {
        let client = self.client().await?;
        let schema = quote(&self.schema);
        client
            .batch_execute(&format!(
                "CREATE SCHEMA IF NOT EXISTS {schema};
                 SET search_path TO {schema};
                 CREATE TABLE IF NOT EXISTS {state} (pipeline TEXT PRIMARY KEY, doc TEXT);
                 CREATE TABLE IF NOT EXISTS {commits} (
                     load_id TEXT, commit_seq BIGINT, PRIMARY KEY (load_id, commit_seq));
                 CREATE TABLE IF NOT EXISTS {cleared} (
                     load_id TEXT, table_name TEXT, PRIMARY KEY (load_id, table_name));",
                state = rdlt_connector_sqlcore::names::STATE_TABLE,
                commits = rdlt_connector_sqlcore::names::COMMITS_TABLE,
                cleared = rdlt_connector_sqlcore::names::CLEARED_TABLE
            ))
            .await
            .map_err(transient)?;

        // Staged data from THIS PIPELINE's dead sessions becomes
        // invisible/reclaimable. Scoped by pipeline-hash prefix: other pipelines
        // sharing the schema keep their live staged rows.
        let prefix_pattern = format!(
            "{}%",
            commit::stage_prefix(&_ctx.pipeline).replace('_', "\\_")
        );
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

        Ok(Box::new(commit::PgSession {
            client,
            pipeline: _ctx.pipeline,
            load_id: _ctx.load_id,
            tables: std::collections::BTreeMap::new(),
            options: self.options.clone(),
            unit: None,
            cleared_targets: std::collections::BTreeSet::new(),
            single_unit_done: std::collections::BTreeSet::new(),
        }))
    }
}
