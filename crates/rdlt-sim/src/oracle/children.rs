//! The child tables of normalized streams against the reference model.

use rdlt_connector::{LogicalType, TablePath};
use rdlt_engine::SchemaPolicy;
use serde_json::Value;

use crate::destination::{Meta, published_table};
use crate::seed::Seed;
use crate::workload::{Extra, Row, SimStream};
use crate::world::World;

/// One item of an array: the id of the row holding it, its position and its value.
type Item = (i64, i64, Value);

/// The values of a row's binary metadata: its lineage ids, and a merge table's sequence.
///
/// Destinations rename metadata columns, so lineage is found by type: the ids and the sequence
/// are a row's only binary metadata, a child's position its only integer metadata.
fn ids(meta: &Meta) -> Vec<&Value> {
    meta.iter()
        .filter(|(logical, _)| *logical == LogicalType::Binary)
        .map(|(_, value)| value)
        .collect()
}

/// Each published row of `stream`'s own table: its binary metadata, and its source row's id.
fn roots(world: &World, stream: &SimStream) -> Vec<(Vec<Value>, i64)> {
    let root = TablePath::new([stream.name.as_str()]).expect("stream names are valid paths");
    published_table(world, &root)
        .into_iter()
        .filter_map(|(source, meta)| {
            let ids = ids(&meta).into_iter().cloned().collect();
            Some((ids, source.get("id")?.as_i64()?))
        })
        .collect()
}

/// Checks that each array column of `stream` has a child table holding exactly the items of
/// `rows`, the rows the stream's table holds, each naming the row it belongs to by that row's id.
pub(super) fn check(world: &World, stream: &SimStream, rows: &[Row], seed: Seed) {
    let roots = roots(world, stream);
    let row_of = |parent: &Value| {
        roots
            .iter()
            .find(|(ids, _)| ids.contains(parent))
            .map(|(_, row)| *row)
    };
    for (column, drift) in stream.drift.iter().enumerate() {
        let mut expected: Vec<Item> = rows
            .iter()
            .filter_map(|row| match row.extras.get(column) {
                // Drift is never declared, so an array is always new: a stream that discards
                // values discards its child table.
                Some(Some(Extra::List(items))) if stream.policy != SchemaPolicy::DiscardValue => {
                    Some((row.id, items))
                }
                _ => None,
            })
            .flat_map(|(id, items)| {
                items.iter().enumerate().map(move |(idx, value)| {
                    (
                        id,
                        i64::try_from(idx).unwrap_or(i64::MAX),
                        Value::from(*value),
                    )
                })
            })
            .collect();
        let path = TablePath::new([stream.name.as_str(), drift.name.as_str()])
            .expect("drift names are valid paths");
        let mut actual: Vec<Item> = published_table(world, &path)
            .into_iter()
            .map(|(source, meta)| {
                // A child of a stream's row has that row as both its parent and its root.
                let lineage = ids(&meta);
                let parent = lineage
                    .iter()
                    .find(|id| lineage.iter().filter(|other| other == id).count() == 2)
                    .and_then(|parent| row_of(parent));
                let idx = meta
                    .iter()
                    .find(|(logical, _)| *logical == LogicalType::Int64)
                    .and_then(|(_, idx)| idx.as_i64());
                let value = source.get("value").cloned().unwrap_or(Value::Null);
                (parent.unwrap_or(-1), idx.unwrap_or(-1), value)
            })
            .collect();
        let order = |item: &Item| (item.0, item.1, item.2.to_string());
        expected.sort_by_key(order);
        actual.sort_by_key(order);
        assert_eq!(
            actual, expected,
            "seed {seed}: the child table of stream {}'s array {} holds other items than the \
             model's",
            stream.name, drift.name
        );
    }
}
