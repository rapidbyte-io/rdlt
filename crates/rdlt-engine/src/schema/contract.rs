//! Contract enforcement: what a policy does with a would-be change. `Freeze`
//! turns it into a typed error before any row of the violating batch is
//! written; `Discard*` filters data down to the frozen shape — counted, never
//! silent; and a table without an entry of its own answers to its ancestry.

use rdlt_core::error::ContractViolation;
use rdlt_core::id::TableName;
use rdlt_core::schema::{self, ColumnType, TableSchema};
use rdlt_core::types::LogicalType;

use super::registry::SchemaRegistry;
use crate::policy::{PolicyAction, SchemaPolicy};
use crate::shred::canonical::parse_timestamp_tz;
use crate::shred::infer::int64_fits_in_f64;
use crate::shred::view::{JsonView, ValueKind};

/// Does `value` conform to `ty` without requiring any schema change?
/// (`true` = storable as-is; `false` = this value is what forced the widening.)
///
/// Compiles `ty` for one consultation — the test-side spelling; the
/// discard walk, which asks the same type of every row, compiles it
/// ONCE as a [`Fit`] and asks that.
#[cfg(test)]
pub(crate) fn value_fits<'a, V: JsonView<'a>>(value: V, ty: &ColumnType) -> bool {
    Fit::of(ty).admits(value)
}

/// A column type compiled for fit checks: its struct fields indexed by
/// name at every depth, ONCE, so each value asked costs its own
/// entries (each a lookup) and nothing of the type's width. The
/// uncompiled walk rebuilt a field index per object per value, which
/// priced every row — an empty object included — at the type's width,
/// and a discard policy asks this of every row a push carries against
/// a type as wide as the wire chose.
#[derive(Debug)]
pub(crate) enum Fit {
    Scalar(LogicalType),
    List(LogicalType),
    Struct(std::collections::BTreeMap<String, Fit>),
}

impl Fit {
    pub(crate) fn of(ty: &ColumnType) -> Self {
        match ty {
            ColumnType::Scalar { scalar } => Self::Scalar(*scalar),
            ColumnType::ScalarList { item } => Self::List(*item),
            ColumnType::Struct { fields } => Self::Struct(
                fields
                    .iter()
                    .map(|field| (field.name.clone(), Self::of(&field.column_type)))
                    .collect(),
            ),
        }
    }

    pub(crate) fn admits<'a, V: JsonView<'a>>(&self, value: V) -> bool {
        match (value.kind(), self) {
            (ValueKind::Null, _) => true,
            (_, Self::Scalar(scalar)) => scalar_fits(value, *scalar),
            (ValueKind::Object, Self::Struct(fields)) => {
                value
                    .obj_entries()
                    .all(|(key, item)| match fields.get(key) {
                        Some(field) => field.admits(item),
                        None => item.is_null(), // a new nested field forced an evolution
                    })
            }
            (ValueKind::Array, Self::List(item)) => value
                .arr_items()
                .all(|i| scalar_fits(i, *item) || i.is_null()),
            _ => false,
        }
    }
}

fn scalar_fits<'a, V: JsonView<'a>>(value: V, scalar: LogicalType) -> bool {
    let kind = value.kind();
    match scalar {
        LogicalType::Json => true, // Json holds anything
        LogicalType::Bool => matches!(kind, ValueKind::Bool(_)),
        LogicalType::Int64 => matches!(kind, ValueKind::Int(_)),
        LogicalType::Float64 => match kind {
            ValueKind::Int(i) => int64_fits_in_f64(i),
            ValueKind::Float(_) => true,
            _ => false,
        },
        // Utf8 absorbs every textable scalar (canonical renderings).
        LogicalType::Utf8 | LogicalType::Uuid => matches!(
            kind,
            ValueKind::Str(_)
                | ValueKind::Int(_)
                | ValueKind::UInt(_)
                | ValueKind::Float(_)
                | ValueKind::Bool(_)
        ),
        LogicalType::TimestampTz => match kind {
            ValueKind::Str(s) => parse_timestamp_tz(s).is_some(),
            _ => false,
        },
        LogicalType::TimestampNaive | LogicalType::Date | LogicalType::Time => {
            matches!(kind, ValueKind::Str(_))
        }
        LogicalType::Decimal { .. } => matches!(kind, ValueKind::Int(_) | ValueKind::Str(_)),
        LogicalType::Binary => false, // not producible from JSON
    }
}

/// Build the typed violation for a frozen change.
pub(crate) fn violation_for(table: &TableName, change: &schema::Change) -> ContractViolation {
    match change {
        schema::Change::CreateTable { .. } => ContractViolation {
            table: table.clone(),
            column: None,
            change: "table creation".to_owned(),
            from: None,
            to: None,
        },
        schema::Change::AddColumn { column } => ContractViolation {
            table: table.clone(),
            column: Some(column.name.clone()),
            change: format!("new column `{}` would be added", column.name),
            from: None,
            to: scalar_of(&column.column_type),
        },
        schema::Change::WidenColumn { name, from, to } => ContractViolation {
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
pub(crate) fn change_column(change: &schema::Change) -> Option<&str> {
    match change {
        schema::Change::CreateTable { .. } => None,
        schema::Change::AddColumn { column } => Some(&column.name),
        schema::Change::WidenColumn { name, .. } => Some(name),
    }
}

/// The policy action governing a table, resolved through its ANCESTRY.
///
/// A child table has no configuration of its own unless the operator wrote one:
/// its existence and shape come from the parent's data, so the parent's contract
/// is the only one that means anything about it. Freezing `t` has to freeze what
/// `t`'s own records create, or the contract is only as strong as the shape of
/// the first batch.
///
/// The NEAREST explicit entry wins, walking from the table upward, so an explicit
/// entry on a child still overrides its parent — inheritance fills a gap, it does
/// not override a decision. Only when no ancestor has an entry does the default
/// apply.
pub(crate) fn inherited_action(
    policy: &SchemaPolicy,
    registry: &SchemaRegistry,
    observed: &TableSchema,
    column: Option<&str>,
) -> PolicyAction {
    if let Some(action) = explicit_action(policy, &observed.table, column) {
        return action;
    }
    // The observed schema carries only its immediate parent; the registry holds
    // the ancestors, so the chain is walked through it. Bounded by the nesting
    // depth the shredder produced.
    let mut next = observed.parent.as_ref().map(|link| link.parent.clone());
    while let Some(ancestor) = next {
        if let Some(action) = explicit_action(policy, &ancestor, None) {
            return action;
        }
        next = registry
            .get(&ancestor)
            .and_then(|schema| schema.parent.as_ref())
            .map(|link| link.parent.clone());
    }
    policy.default
}

/// An action the operator wrote for this exact table or column, if any.
/// Distinct from `SchemaPolicy::action_for`, which cannot tell an explicit entry
/// from the default it falls back to — the difference inheritance turns on.
fn explicit_action(
    policy: &SchemaPolicy,
    table: &TableName,
    column: Option<&str>,
) -> Option<PolicyAction> {
    if let Some(column) = column {
        let key = crate::policy::ColumnRef {
            table: table.clone(),
            column: column.to_owned(),
        };
        if let Some(action) = policy.per_column.get(&key) {
            return Some(*action);
        }
    }
    policy.per_table.get(table).copied()
}

#[cfg(test)]
mod tests {
    // The `value_fits` arms are otherwise reachable only through Discard
    // policies, which few tests exercise: a direct table.
    use rdlt_core::schema::{Column, Provenance};
    use serde_json::json;

    use super::*;

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
            fields: vec![rdlt_core::schema::Column {
                name: "a".into(),
                column_type: ColumnType::scalar(LogicalType::Int64),
                nullable: true,
                provenance: rdlt_core::schema::Provenance::Inferred,
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

    /// The `from`/`to` fields are asserted BY VALUE, not through the rendered
    /// message — a violation is consumed programmatically (an operator tool
    /// reads which type widened to which), so behaviour is never pinned to
    /// rendered text; without this pin `scalar_of` could answer `None` for
    /// everything unnoticed.
    #[test]
    fn violation_for_carries_typed_from_and_to() {
        let table = TableName::new("t");

        // CreateTable is table-level: no column, no types.
        let created = violation_for(
            &table,
            &schema::Change::CreateTable {
                schema: TableSchema {
                    table: table.clone(),
                    parent: None,
                    columns: vec![],
                },
            },
        );
        assert_eq!(created.table, table);
        assert_eq!(created.column, None);
        assert_eq!(created.from, None);
        assert_eq!(created.to, None);

        // AddColumn: no `from` (the column did not exist), `to` is its type.
        let added = violation_for(
            &table,
            &schema::Change::AddColumn {
                column: Column {
                    name: "email".into(),
                    column_type: ColumnType::scalar(LogicalType::Utf8),
                    nullable: true,
                    provenance: Provenance::Inferred,
                },
            },
        );
        assert_eq!(added.column.as_deref(), Some("email"));
        assert_eq!(added.from, None);
        assert_eq!(
            added.to,
            Some(LogicalType::Utf8),
            "the added column's type must reach the caller as a value"
        );

        // WidenColumn: both ends present and distinct.
        let widened = violation_for(
            &table,
            &schema::Change::WidenColumn {
                name: "id".into(),
                from: ColumnType::scalar(LogicalType::Int64),
                to: ColumnType::scalar(LogicalType::Utf8),
            },
        );
        assert_eq!(widened.column.as_deref(), Some("id"));
        assert_eq!(widened.from, Some(LogicalType::Int64));
        assert_eq!(widened.to, Some(LogicalType::Utf8));
    }

    /// A non-scalar end has no `LogicalType` to report, and that `None` is a
    /// real answer rather than a missing one — asserted so the scalar arm and
    /// the fallback arm are distinguishable.
    #[test]
    fn violation_for_reports_none_for_non_scalar_ends() {
        let table = TableName::new("t");
        let struct_ty = ColumnType::Struct {
            fields: vec![Column {
                name: "city".into(),
                column_type: ColumnType::scalar(LogicalType::Utf8),
                nullable: true,
                provenance: Provenance::Inferred,
            }],
        };
        let widened = violation_for(
            &table,
            &schema::Change::WidenColumn {
                name: "profile".into(),
                from: struct_ty.clone(),
                to: ColumnType::scalar(LogicalType::Json),
            },
        );
        assert_eq!(widened.from, None, "a struct has no single logical type");
        assert_eq!(widened.to, Some(LogicalType::Json));
    }

    #[test]
    fn change_column_addresses_the_right_column() {
        assert_eq!(
            change_column(&schema::Change::CreateTable {
                schema: TableSchema {
                    table: TableName::new("t"),
                    parent: None,
                    columns: vec![],
                },
            }),
            None,
            "table creation is table-level, not column-level"
        );
        assert_eq!(
            change_column(&schema::Change::AddColumn {
                column: Column {
                    name: "email".into(),
                    column_type: ColumnType::scalar(LogicalType::Utf8),
                    nullable: true,
                    provenance: Provenance::Inferred,
                },
            }),
            Some("email")
        );
        assert_eq!(
            change_column(&schema::Change::WidenColumn {
                name: "id".into(),
                from: ColumnType::scalar(LogicalType::Int64),
                to: ColumnType::scalar(LogicalType::Utf8),
            }),
            Some("id")
        );
    }
}
