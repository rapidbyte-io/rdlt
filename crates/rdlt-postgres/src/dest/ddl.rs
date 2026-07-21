//! Type mapping and table DDL decisions.
//! (Feature 008 T001: relocated verbatim; create/migrate/index helpers land
//! in later tasks.)

use rdlt_connector::core::{ColumnType, LogicalType};

/// Postgres SQL type per (already lowered) column type.
pub(super) fn sql_type(ty: &ColumnType) -> &'static str {
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
