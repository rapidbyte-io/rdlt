//! Contract enforcement helpers (spec FR-010, design doc §5.4).
//!
//! `Freeze` turns a would-be delta into a typed error before any row of the violating
//! batch is written; `Discard*` filters data down to the frozen shape — counted, never
//! silent.

use rdlt_core::types::int64_fits_in_f64;
use rdlt_core::{ColumnType, ContractViolation, LogicalType, SchemaChange, TableName};
use serde_json::Value;

use crate::shred::canon::parse_timestamp_tz;

/// Does `value` conform to `ty` without requiring any schema change?
/// (`true` = storable as-is; `false` = this value is what forced the widening.)
pub(crate) fn value_fits(value: &Value, ty: &ColumnType) -> bool {
    match (value, ty) {
        (Value::Null, _) => true,
        (_, ColumnType::Scalar { scalar }) => scalar_fits(value, *scalar),
        (Value::Object(map), ColumnType::Struct { fields }) => map.iter().all(|(key, item)| {
            match fields.iter().find(|f| &f.name == key) {
                Some(field) => value_fits(item, &field.ty),
                None => item.is_null(), // a new nested field forced an evolution
            }
        }),
        (Value::Array(items), ColumnType::ScalarList { item }) => {
            let ty = ColumnType::scalar(*item);
            items.iter().all(|i| value_fits(i, &ty))
        }
        _ => false,
    }
}

fn scalar_fits(value: &Value, scalar: LogicalType) -> bool {
    match scalar {
        LogicalType::Json => true, // Json holds anything
        LogicalType::Bool => value.is_boolean(),
        LogicalType::Int64 => value.as_i64().is_some(),
        LogicalType::Float64 => match value {
            Value::Number(n) => n.as_i64().map(int64_fits_in_f64).unwrap_or(n.is_f64()),
            _ => false,
        },
        // Utf8 absorbs every textable scalar (canonical renderings).
        LogicalType::Utf8 | LogicalType::Uuid => {
            matches!(value, Value::String(_) | Value::Number(_) | Value::Bool(_))
        }
        LogicalType::TimestampTz => value
            .as_str()
            .map(|s| parse_timestamp_tz(s).is_some())
            .unwrap_or(false),
        LogicalType::TimestampNaive | LogicalType::Date | LogicalType::Time => value.is_string(),
        LogicalType::Decimal { .. } => value.as_i64().is_some() || value.is_string(),
        LogicalType::Binary => false, // not producible from JSON
    }
}

/// Build the typed violation for a frozen change.
pub(crate) fn violation_for(table: &TableName, change: &SchemaChange) -> ContractViolation {
    match change {
        SchemaChange::CreateTable { .. } => ContractViolation {
            table: table.clone(),
            column: None,
            change: "table creation".to_owned(),
            from: None,
            to: None,
        },
        SchemaChange::AddColumn { column } => ContractViolation {
            table: table.clone(),
            column: Some(column.name.clone()),
            change: format!("new column `{}` would be added", column.name),
            from: None,
            to: scalar_of(&column.ty),
        },
        SchemaChange::WidenColumn { name, from, to } => ContractViolation {
            table: table.clone(),
            column: Some(name.clone()),
            change: format!("column `{name}` would widen from {from:?} to {to:?}"),
            from: scalar_of(from),
            to: scalar_of(to),
        },
    }
}

fn scalar_of(ty: &ColumnType) -> Option<LogicalType> {
    match ty {
        ColumnType::Scalar { scalar } => Some(*scalar),
        _ => None,
    }
}

/// The column a change concerns, for policy resolution (None = table-level).
pub(crate) fn change_column(change: &SchemaChange) -> Option<&str> {
    match change {
        SchemaChange::CreateTable { .. } => None,
        SchemaChange::AddColumn { column } => Some(&column.name),
        SchemaChange::WidenColumn { name, .. } => Some(name),
    }
}
