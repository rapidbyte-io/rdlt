//! Type mapping and table DDL decisions.
//!
//! Native NUMERIC(p,s)/JSONB/UUID + NOT NULL. Decisions come from the LOGICAL
//! column type — a plain text column and a json/uuid-logical column (same
//! arrow repr) can never confuse.

use rdlt_connector::core::{ColumnDef, ColumnType, LogicalType, TableSchema, WriteMode};
use rdlt_connector_sqlcore::plan::{self as sqlplan, IndexSpec, TableFacts, ValidateError};

use super::commit::{ARRIVAL_COL, stage_name};
use super::config::{DestOptions, MergeStrategy};

/// Postgres SQL type per logical column type.
pub(super) fn sql_type(ty: &ColumnType) -> String {
    match ty {
        ColumnType::Scalar { scalar } => match scalar {
            LogicalType::Bool => "BOOLEAN".into(),
            LogicalType::Int64 => "BIGINT".into(),
            LogicalType::Float64 => "DOUBLE PRECISION".into(),
            LogicalType::Utf8 => "TEXT".into(),
            LogicalType::Uuid => "UUID".into(),
            LogicalType::Json => "JSONB".into(),
            LogicalType::Binary => "BYTEA".into(),
            LogicalType::TimestampTz => "TIMESTAMPTZ".into(),
            LogicalType::TimestampNaive => "TIMESTAMP".into(),
            LogicalType::Date => "DATE".into(),
            LogicalType::Time => "TIME".into(),
            LogicalType::Decimal { precision, scale } => {
                format!("NUMERIC({precision},{scale})")
            }
        },
        // structs/lists never reach a structless destination (engine-lowered).
        ColumnType::Struct { .. } | ColumnType::ScalarList { .. } => "TEXT".into(),
    }
}

/// One column definition for CREATE TABLE. NOT NULL applies to the TARGET
/// table only (CREATE-time; migrations stay additive) — stage tables
/// keep everything nullable.
pub(super) fn column_def(column: &ColumnDef, target: bool) -> String {
    let not_null = if target && !column.nullable {
        " NOT NULL"
    } else {
        ""
    };
    format!(
        "{} {}{}",
        super::quote(&column.name),
        sql_type(&column.column_type),
        not_null
    )
}

// ---- Supporting indexes ----

pub(super) fn create_index_sql(unique: bool, table: &str, columns: &[String]) -> String {
    // The statement is sqlcore's; only the quoting is this destination's.
    rdlt_connector_sqlcore::names::create_index_sql(unique, table, columns, super::quote)
}

// ---- Ensure: rendering, separated from execution ----
//
// These build the exact statement sequence `ensure_table` runs, in order, and
// touch no connection. Separating them is what makes the emitted DDL testable
// at all: executing it needs a live server, but comparing it needs nothing.

/// One ensure statement plus what the caller must know to classify its
/// failure. A unique-index failure is the only one carrying a special
/// diagnosis, so it is the only kind distinguished here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct EnsureStmt {
    pub(super) sql: String,
    pub(super) unique_index: Option<Vec<String>>,
}

impl EnsureStmt {
    fn plain(sql: String) -> Self {
        Self {
            sql,
            unique_index: None,
        }
    }
}

/// Phase 1 — the table statements: create each leg, then add and widen its
/// columns. Infallible by construction, and deliberately independent of the
/// option validation that follows: the tables are created BEFORE the options
/// are checked today, and reordering that would move where a bad config
/// fails.
///
/// `previous` is the schema THIS SESSION last ensured, never the live
/// catalog — that is what makes the widen a within-run rule.
pub(super) fn table_ddl_stmts(
    pipeline: &rdlt_connector::core::PipelineId,
    schema: &TableSchema,
    mode: &WriteMode,
    previous: Option<&TableSchema>,
) -> Vec<String> {
    // Only merge tables get a stage leg. Append and Replace COPY straight
    // into the target inside the unit transaction, so an UNLOGGED twin —
    // and the sequence that comes with it — would be built, written and
    // truncated for nothing.
    let mut legs = vec![(schema.table.as_str().to_owned(), false)];
    if matches!(mode, WriteMode::Merge { .. }) {
        legs.push((stage_name(pipeline, &schema.table), true));
    }
    let mut out = Vec::new();
    for (name, is_stage) in legs {
        let mut columns = schema
            .columns
            .iter()
            .map(|c| column_def(c, !is_stage))
            .collect::<Vec<_>>()
            .join(", ");
        if is_stage {
            // Arrival order for deterministic merge dedup.
            //
            // `CACHE 32` rather than a plain BIGSERIAL's cache of 1, and it
            // is worth real time: the sequence is drawn once per staged row,
            // and at a million rows that draw was measured at 215 ms of a
            // 642 ms stage COPY — a third of it. Caching recovers 122 ms of
            // that; the number plateaus at 32, with 1000 no better.
            //
            // Caching costs only CONTIGUITY, which nothing here reads. The
            // column exists to order rows within one stage table, and
            // `DISTINCT ON … ORDER BY id, arrival DESC` asks which value is
            // larger, never whether values are adjacent. A session's block
            // is still handed out in increasing order, and the engine opens
            // its destination sessions one after another — the replay
            // session completes before the run's own session opens — so
            // there is no concurrent writer to interleave blocks with.
            //
            // Spelled as an identity column because that is one statement
            // that both creates the sequence and sets its cache. An
            // already-existing stage table keeps whatever it was created
            // with; `IF NOT EXISTS` leaves it alone, and a stage table
            // holding uncached arrival values is correct, merely slower.
            columns.push_str(&format!(
                ", {} bigint GENERATED BY DEFAULT AS IDENTITY (CACHE 32)",
                super::quote(ARRIVAL_COL)
            ));
        }
        let unlogged = if is_stage { "UNLOGGED " } else { "" };
        out.push(format!(
            "CREATE {unlogged}TABLE IF NOT EXISTS {} ({columns})",
            super::quote(&name)
        ));
        // Migrations: add new columns; widen with a USING cast.
        for column in &schema.columns {
            out.push(format!(
                "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}",
                super::quote(&name),
                super::quote(&column.name),
                sql_type(&column.column_type)
            ));
            if let Some(prev) = previous
                && let Some(old) = prev.column(&column.name)
                && old.column_type != column.column_type
            {
                out.push(format!(
                    "ALTER TABLE {} ALTER COLUMN {} TYPE {} USING {}::{}",
                    super::quote(&name),
                    super::quote(&column.name),
                    sql_type(&column.column_type),
                    super::quote(&column.name),
                    sql_type(&column.column_type)
                ));
            }
        }
    }
    out
}

/// Phase 2 — option validation, then the scd2 validity columns and the index
/// plan. Runs AFTER phase 1 has been applied, preserving today's failure
/// point. Non-merge modes validate and return nothing to execute.
pub(super) fn merge_ensure_stmts(
    options: &DestOptions,
    schema: &TableSchema,
    mode: &WriteMode,
) -> Result<Vec<EnsureStmt>, ValidateError> {
    let table = schema.table.as_str();
    // The option-vs-mode rules live in sqlcore — one rule set, identical
    // typed errors on every SQL destination.
    let WriteMode::Merge { key } = mode else {
        sqlplan::validate_non_merge(options, table)?;
        return Ok(Vec::new());
    };
    let has_identity = schema
        .columns
        .iter()
        .any(|c| c.name == rdlt_connector::core::schema::system_columns::ID);
    let is_child = schema.parent.is_some();
    sqlplan::validate_merge(
        options,
        table,
        key,
        &TableFacts {
            schema,
            has_identity,
            is_child,
        },
    )?;
    let mut out = Vec::new();
    if options.strategy_for(table) == MergeStrategy::Scd2 {
        let scd2 = options.scd2_for(table);
        // Validity columns on the TARGET only (the stage carries the
        // stream's shape); additive for pre-existing scd2 tables.
        for (col, extra) in [
            (&scd2.valid_from, "TIMESTAMPTZ NOT NULL DEFAULT now()"),
            (&scd2.valid_to, "TIMESTAMPTZ"),
        ] {
            out.push(EnsureStmt::plain(format!(
                "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {extra}",
                super::quote(table),
                super::quote(col)
            )));
        }
    }
    // Index plan — shared shape from sqlcore; this destination owns only the
    // SQL text.
    for IndexSpec { unique, columns } in
        sqlplan::index_plan(options, table, key, has_identity, is_child)
    {
        let sql = create_index_sql(unique, table, &columns);
        out.push(EnsureStmt {
            sql,
            unique_index: unique.then_some(columns),
        });
    }
    Ok(out)
}
