//! Type mapping and table DDL decisions.
//!
//! Feature 008 US1: native NUMERIC(p,s)/JSONB/UUID + NOT NULL (contract
//! dest-types.md T1–T4). Decisions come from the LOGICAL column type — a
//! plain text column and a json/uuid-logical column (same arrow repr) can
//! never confuse (T6).

use rdlt_connector::core::{ColumnDef, ColumnType, LogicalType};

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
/// table only (T4: CREATE-time; migrations stay additive) — stage tables
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
        sql_type(&column.ty),
        not_null
    )
}
