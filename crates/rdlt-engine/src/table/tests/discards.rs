//! Columns whose schema policy decides each value: converted once on the way to their table.

use std::sync::Arc;

use arrow_array::{ArrayRef, Int32Array, Int64Array};
use rdlt_connector::{LogicalType, TableSchema};

use super::{batch, capabilities, created, plan, resolver, stamp, table};
use crate::policy::SchemaPolicy;
use crate::table::TableView;
use crate::table::convert::CONVERSIONS;
use crate::table::lowering::LoweringPlan;
use crate::table::resolve::Incoming;

/// How many whole columns preparing a batch of an `Int32` column `n` and a key `id` converts
/// into a table holding `n` as `Int64`, each column following `policy`.
fn conversions(policy: SchemaPolicy) -> usize {
    let resolver = resolver(capabilities(), plan(), &["id"]);
    let ns: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let ids: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let rows = batch(vec![("id", ids), ("n", ns)]);
    let incoming = Incoming::declared(TableSchema::from_arrow(&rows.schema()).unwrap());
    let model = created(&resolver, &[("n", LogicalType::Int64)]);
    let resolution = resolver.resolve(&model, &incoming).unwrap();
    let view = Arc::new(TableView::new(&table("t"), resolution.model, &resolver).unwrap());
    let plan = LoweringPlan::new(resolver.stream.clone(), view, incoming, resolution.routes)
        .with_policies(vec![policy; 2]);
    CONVERSIONS.with(|conversions| conversions.set(0));
    plan.prepare(&rows, None, &stamp(), None).unwrap();
    CONVERSIONS.with(std::cell::Cell::get)
}

#[test]
fn a_column_a_discarding_policy_decides_converts_once() {
    let once = conversions(SchemaPolicy::Evolve);
    for policy in [SchemaPolicy::DiscardRow, SchemaPolicy::DiscardValue] {
        assert_eq!(conversions(policy), once, "{policy:?}");
    }
}
