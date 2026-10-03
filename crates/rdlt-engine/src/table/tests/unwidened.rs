//! Columns the destination refused to widen in place: their values go to variant columns.

use rdlt_connector::{ColumnKey, ColumnPath, LogicalType, TypeKind};

use super::{capabilities, columns, created, decimal, plan, resolver, schema};
use crate::table::resolve::Change;

#[test]
fn a_column_kept_from_widening_takes_a_variant_for_values_it_cannot_hold() {
    let resolver = resolver(capabilities(), plan(), &[]);
    let model = created(&resolver, &[("n", LogicalType::Int32)]);
    let wider = schema(&[("n", LogicalType::Int64)]);
    let widened = resolver.resolve(&model, &wider).unwrap();
    assert!(matches!(widened.changes[..], [Change::Widen { .. }]));
    let kept = resolver.unwidening([ColumnKey::Source(ColumnPath::from("n"))]);
    let routed = kept.resolve(&model, &wider).unwrap();
    assert!(matches!(routed.changes[..], [Change::Add { .. }]));
    assert_eq!(
        columns(&routed.model),
        [
            ("n".to_owned(), LogicalType::Int32),
            ("n__int64".to_owned(), LogicalType::Int64)
        ]
    );
}

#[test]
fn a_variant_kept_from_widening_leaves_its_values_to_the_json_variant() {
    let own = ColumnKey::Source(ColumnPath::from("d"));
    let resolver = resolver(capabilities(), plan(), &[]).unwidening([own]);
    let model = created(&resolver, &[("d", decimal(5, 2))]);
    // Kept as it is, the column's wider decimals go to its decimal variant.
    let model = resolver
        .resolve(&model, &schema(&[("d", decimal(10, 2))]))
        .unwrap()
        .model;
    assert_eq!(
        columns(&model)[1],
        ("d__decimal".to_owned(), decimal(10, 2))
    );
    let wider = schema(&[("d", decimal(20, 2))]);
    let widened = resolver.resolve(&model, &wider).unwrap();
    assert_eq!(columns(&widened.model)[1].1, decimal(20, 2));
    let variant = ColumnKey::Variant {
        column: ColumnPath::from("d"),
        kind: TypeKind::Decimal,
    };
    let routed = resolver
        .unwidening([variant])
        .resolve(&model, &wider)
        .unwrap();
    assert_eq!(columns(&routed.model)[1].1, decimal(10, 2));
    assert_eq!(
        columns(&routed.model)[2],
        ("d__json".to_owned(), LogicalType::Json)
    );
}

#[test]
fn a_child_table_widens_its_own_column_of_a_name_its_root_keeps_from_widening() {
    let kept = resolver(capabilities(), plan(), &[])
        .unwidening([ColumnKey::Source(ColumnPath::from("n"))]);
    let child = kept.child(None, ColumnPath::from("items"));
    let model = created(&child, &[("n", LogicalType::Int32)]);
    let widened = child
        .resolve(&model, &schema(&[("n", LogicalType::Int64)]))
        .unwrap();
    assert!(matches!(widened.changes[..], [Change::Widen { .. }]));
}
