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

// ---- Feature 008 US2: supporting indexes (contract merge-strategies.md M5) ----

/// Deterministic index name: idempotent `IF NOT EXISTS` across sessions.
pub(super) fn index_name(unique: bool, table: &str, columns: &[String]) -> String {
    let prefix = if unique { "rdlt_ux" } else { "rdlt_ix" };
    format!(
        "{prefix}_{}",
        rdlt_connector::core::naming::ident_hash(&format!("{table}:{}", columns.join(",")), 16)
    )
}

pub(super) fn create_index_sql(unique: bool, table: &str, columns: &[String]) -> String {
    let cols = columns
        .iter()
        .map(|c| super::quote(c))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "CREATE {}INDEX IF NOT EXISTS {} ON {} ({cols})",
        if unique { "UNIQUE " } else { "" },
        super::quote(&index_name(unique, table, columns)),
        super::quote(table),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_names_are_deterministic_and_distinct() {
        let a = index_name(false, "orders", &["id".into()]);
        let b = index_name(false, "orders", &["id".into()]);
        assert_eq!(a, b, "same inputs, same name (idempotency)");
        assert_ne!(
            a,
            index_name(true, "orders", &["id".into()]),
            "unique differs"
        );
        assert_ne!(a, index_name(false, "orders", &["other".into()]));
        assert_ne!(a, index_name(false, "other", &["id".into()]));
        assert!(a.len() <= 63, "under the identifier limit");
        assert!(a.starts_with("rdlt_ix_"));
        assert!(index_name(true, "t", &["k".into()]).starts_with("rdlt_ux_"));
    }
}
