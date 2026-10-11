//! Columns whose schema policy decides each value: converted once on the way to their table.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{ArrayRef, Int32Array, Int64Array, TimestampSecondArray};
use rdlt_connector::{LogicalType, TableSchema};

use super::{batch, capabilities, created, plan, resolver, stamp, table};
use crate::policy::SchemaPolicy;
use crate::table::TableView;
use crate::table::convert::CONVERSIONS;
use crate::table::lowering::LoweringPlan;
use crate::table::resolve::Incoming;

/// How many whole columns preparing a batch of an `Int32` column `n` and a key `id` converts
/// into a table holding `n` as `Int64`, each column following `policy`; for a history table
/// where `history` says, its versions beginning at a column `at` of instants each version can
/// begin at.
fn conversions(policy: SchemaPolicy, history: bool) -> usize {
    let mut resolver = resolver(capabilities(), plan(), &["id"]);
    if history {
        let meta = resolver.meta.clone();
        resolver.meta = meta.with_history(&resolver.naming, Some(Arc::from("at")));
    }
    let ns: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let ids: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let at: ArrayRef = Arc::new(TimestampSecondArray::from(vec![10, 20, 30]));
    let rows = batch(vec![("id", ids), ("n", ns), ("at", at)]);
    let incoming = Incoming::declared(TableSchema::from_arrow(&rows.schema()).unwrap());
    let model = created(&resolver, &[("n", LogicalType::Int64)]);
    let resolution = resolver.resolve(&model, &incoming).unwrap();
    let view = Arc::new(TableView::new(&table("t"), resolution.model, &resolver).unwrap());
    let plan = LoweringPlan::new(resolver.stream.clone(), view, incoming, resolution.routes)
        .with_policies(vec![policy; 3]);
    CONVERSIONS.with(|conversions| conversions.set(0));
    plan.prepare(&rows, None, &stamp(), None).unwrap();
    CONVERSIONS.with(std::cell::Cell::get)
}

#[test]
fn a_column_a_discarding_policy_decides_converts_once() {
    for history in [false, true] {
        let once = conversions(SchemaPolicy::Evolve, history);
        for policy in [SchemaPolicy::DiscardRow, SchemaPolicy::DiscardValue] {
            assert_eq!(conversions(policy, history), once, "{policy:?} {history}");
        }
    }
}

/// Every column of a batch of five thousand whose policy discards values converts once, and
/// lowering finds each conversion where it is: each column holds its own values.
#[test]
fn five_thousand_discarded_columns_convert_once_each() {
    use arrow_array::RecordBatch;
    const COLUMNS: usize = 5_000;
    let resolver = resolver(capabilities(), plan(), &[]);
    let names: Vec<String> = (0..COLUMNS).map(|column| format!("c{column}")).collect();
    let created_as: Vec<(&str, LogicalType)> = names
        .iter()
        .map(|name| (name.as_str(), LogicalType::Int64))
        .collect();
    let model = created(&resolver, &created_as);
    // Column `c` holds `c`, `c + 1` and `c + 2`.
    let values = |column: usize| {
        let first = i32::try_from(column).unwrap();
        vec![first, first + 1, first + 2]
    };
    let rows = RecordBatch::try_from_iter(names.iter().enumerate().map(|(column, name)| {
        let values: ArrayRef = Arc::new(Int32Array::from(values(column)));
        (name.as_str(), values)
    }))
    .unwrap();
    let incoming = Incoming::declared(TableSchema::from_arrow(&rows.schema()).unwrap());
    let resolution = resolver.resolve(&model, &incoming).unwrap();
    let view = Arc::new(TableView::new(&table("t"), resolution.model, &resolver).unwrap());
    let plan = LoweringPlan::new(resolver.stream.clone(), view, incoming, resolution.routes)
        .with_policies(vec![SchemaPolicy::DiscardValue; COLUMNS]);
    CONVERSIONS.with(|conversions| conversions.set(0));
    let prepared = plan.prepare(&rows, None, &stamp(), None).unwrap();
    assert_eq!(CONVERSIONS.with(std::cell::Cell::get), COLUMNS);
    assert_eq!(prepared.batch.num_rows(), 3);
    for column in [0, COLUMNS / 2, COLUMNS - 1] {
        let held = prepared.batch.column_by_name(&names[column]).unwrap();
        let held = held.as_primitive::<Int64Type>().values().to_vec();
        let expected: Vec<i64> = values(column).into_iter().map(i64::from).collect();
        assert_eq!(held, expected, "{}", names[column]);
    }
}
