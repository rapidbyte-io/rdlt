//! Contract enforcement helpers (spec FR-010, design doc §5.4).
//!
//! `Freeze` turns a would-be delta into a typed error before any row of the violating
//! batch is written; `Discard*` filters data down to the frozen shape — counted, never
//! silent.

use rdlt_core::types::int64_fits_in_f64;
use rdlt_core::{ColumnType, ContractViolation, LogicalType, SchemaChange, TableName};

use crate::shred::canon::parse_timestamp_tz;
use crate::shred::view::{JsonView, Kind};

/// Does `value` conform to `ty` without requiring any schema change?
/// (`true` = storable as-is; `false` = this value is what forced the widening.)
pub(crate) fn value_fits<'a, V: JsonView<'a>>(value: V, ty: &ColumnType) -> bool {
    match (value.kind(), ty) {
        (Kind::Null, _) => true,
        (_, ColumnType::Scalar { scalar }) => scalar_fits(value, *scalar),
        (Kind::Object, ColumnType::Struct { fields }) => {
            value
                .obj_entries()
                .all(|(key, item)| match fields.iter().find(|f| f.name == key) {
                    Some(field) => value_fits(item, &field.ty),
                    None => item.is_null(), // a new nested field forced an evolution
                })
        }
        (Kind::Array, ColumnType::ScalarList { item }) => {
            let ty = ColumnType::scalar(*item);
            value.arr_items().all(|i| value_fits(i, &ty))
        }
        _ => false,
    }
}

fn scalar_fits<'a, V: JsonView<'a>>(value: V, scalar: LogicalType) -> bool {
    let kind = value.kind();
    match scalar {
        LogicalType::Json => true, // Json holds anything
        LogicalType::Bool => matches!(kind, Kind::Bool(_)),
        LogicalType::Int64 => matches!(kind, Kind::Int(_)),
        LogicalType::Float64 => match kind {
            Kind::Int(i) => int64_fits_in_f64(i),
            Kind::Float(_) => true,
            _ => false,
        },
        // Utf8 absorbs every textable scalar (canonical renderings).
        LogicalType::Utf8 | LogicalType::Uuid => matches!(
            kind,
            Kind::Str(_) | Kind::Int(_) | Kind::UInt(_) | Kind::Float(_) | Kind::Bool(_)
        ),
        LogicalType::TimestampTz => match kind {
            Kind::Str(s) => parse_timestamp_tz(s).is_some(),
            _ => false,
        },
        LogicalType::TimestampNaive | LogicalType::Date | LogicalType::Time => {
            matches!(kind, Kind::Str(_))
        }
        LogicalType::Decimal { .. } => matches!(kind, Kind::Int(_) | Kind::Str(_)),
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

#[cfg(test)]
mod tests {
    // Mutation-report closure: value_fits arms were only reachable through
    // Discard policies, which few tests exercise. Direct table.
    use super::*;
    use serde_json::json;

    fn fits(value: &serde_json::Value, ty: LogicalType) -> bool {
        value_fits(value, &ColumnType::scalar(ty))
    }

    #[test]
    fn value_fits_scalar_table() {
        use LogicalType::*;
        assert!(fits(&json!(null), Bool));
        assert!(fits(&json!(true), Bool) && !fits(&json!(1), Bool));
        assert!(fits(&json!(5), Int64) && !fits(&json!(5.5), Int64));
        assert!(fits(&json!(5.5), Float64) && fits(&json!(5), Float64));
        assert!(!fits(&json!(9007199254740993i64), Float64), "beyond 2^53");
        assert!(fits(&json!("x"), Utf8) && fits(&json!(5), Utf8) && fits(&json!(true), Utf8));
        assert!(!fits(&json!({"a": 1}), Utf8));
        assert!(fits(&json!("2026-07-19T10:00:00Z"), TimestampTz));
        assert!(!fits(&json!("not a time"), TimestampTz));
        assert!(fits(&json!("anything"), TimestampNaive) && !fits(&json!(5), TimestampNaive));
        assert!(fits(
            &json!(5),
            Decimal {
                precision: 10,
                scale: 2
            }
        ));
        assert!(fits(
            &json!("5.10"),
            Decimal {
                precision: 10,
                scale: 2
            }
        ));
        assert!(!fits(
            &json!(5.1),
            Decimal {
                precision: 10,
                scale: 2
            }
        ));
        assert!(!fits(&json!("x"), Binary), "Binary unproducible from JSON");
        assert!(fits(&json!({"free": ["form"]}), Json));
    }

    #[test]
    fn value_fits_struct_and_list() {
        let struct_ty = ColumnType::Struct {
            fields: vec![rdlt_core::ColumnDef {
                name: "a".into(),
                ty: ColumnType::scalar(LogicalType::Int64),
                nullable: true,
                provenance: rdlt_core::Provenance::Inferred,
            }],
        };
        assert!(value_fits(&json!({"a": 1}), &struct_ty));
        assert!(
            !value_fits(&json!({"a": "text"}), &struct_ty),
            "field type mismatch"
        );
        assert!(
            !value_fits(&json!({"a": 1, "new": 2}), &struct_ty),
            "new field forced evolution"
        );
        assert!(
            value_fits(&json!({"a": 1, "new": null}), &struct_ty),
            "null new field is fine"
        );

        let list_ty = ColumnType::ScalarList {
            item: LogicalType::Int64,
        };
        assert!(value_fits(&json!([1, 2, null]), &list_ty));
        assert!(!value_fits(&json!([1, "x"]), &list_ty));
    }
}
