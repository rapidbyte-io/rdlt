//! The child tables of normalized streams against the reference model.

use rdlt_connector::{LogicalType, TablePath};
use serde_json::Value;

use crate::destination::{Meta, published_table};
use crate::seed::Seed;
use crate::workload::{Extra, Row, SimStream};
use crate::world::World;

/// One item of an array: the id of the row holding it, its position and its value.
type Item = (i64, i64, Value);

/// The values of a row's lineage ids.
///
/// Destinations rename metadata columns, so lineage is found by type: the ids are a row's only
/// binary metadata, a child's position its only integer metadata.
fn ids(meta: &Meta) -> Vec<&Value> {
    meta.iter()
        .filter(|(logical, _)| *logical == LogicalType::Binary)
        .map(|(_, value)| value)
        .collect()
}

/// Checks that each array column of `stream` has a child table holding exactly the items of
/// `rows`, which the stream's table holds, each naming the row it belongs to by that row's id.
pub(super) fn check(world: &World, stream: &SimStream, rows: &[Row], seed: Seed) {
    let root = TablePath::new([stream.name.as_str()]).expect("stream names are valid paths");
    let roots: Vec<(Value, i64)> = published_table(world, &root)
        .into_iter()
        .filter_map(|(source, meta)| {
            let [id] = ids(&meta).try_into().ok()?;
            Some((id.clone(), source.get("id")?.as_i64()?))
        })
        .collect();
    let row_of = |parent: &Value| {
        roots
            .iter()
            .find(|(id, _)| id == parent)
            .map(|(_, row)| *row)
    };
    for (column, drift) in stream.drift.iter().enumerate() {
        let mut expected: Vec<Item> = rows
            .iter()
            .filter_map(|row| match row.extras.get(column) {
                Some(Some(Extra::List(items))) => Some((row.id, items)),
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
