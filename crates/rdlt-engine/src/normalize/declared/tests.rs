use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use proptest::prelude::*;
use rdlt_connector::{ColumnPath, Field, Fields, LogicalType, TableSchema};

use super::{children, root_columns};
use crate::normalize::encodings::{batch, drawn, plainly};
use crate::normalize::{Shape, normalize};
use crate::table::Incoming;

/// A column's path within its table and its type.
type Column = (Vec<String>, LogicalType);

fn shape(max_depth: u8) -> Shape {
    Shape {
        max_depth,
        whole: BTreeSet::new(),
        key: Vec::new(),
    }
}

fn list(item: LogicalType) -> LogicalType {
    LogicalType::List(Box::new(Field::new("item", item, true)))
}

fn object(fields: &[(&str, LogicalType)]) -> LogicalType {
    let fields = fields
        .iter()
        .map(|(name, logical)| Field::new(*name, logical.clone(), true))
        .collect();
    LogicalType::Struct(Fields::new(fields).expect("distinct names"))
}

/// A stream's schema of an `id` and the column `held`, of type `logical`.
fn schema(held: &str, logical: LogicalType) -> TableSchema {
    TableSchema::new(vec![
        Field::new("id", LogicalType::Int64, false),
        Field::new(held, logical, true),
    ])
    .expect("a valid schema")
}

fn columns(incoming: &Incoming) -> Vec<Column> {
    incoming
        .paths
        .iter()
        .zip(incoming.schema.fields().iter())
        .map(|(path, field)| {
            let path = path.segments().map(str::to_owned).collect();
            (path, field.logical_type().clone())
        })
        .collect()
}

fn path(segments: &[&str]) -> Vec<String> {
    segments
        .iter()
        .map(|segment| (*segment).to_owned())
        .collect()
}

/// Each child table `schema` makes at `max_depth`: its path and its columns.
fn tables(schema: &TableSchema, max_depth: u8) -> Vec<(Vec<String>, Vec<Column>)> {
    children(schema, &shape(max_depth))
        .expect("declared children")
        .iter()
        .map(|(path, incoming)| {
            let path = path.iter().map(ToString::to_string).collect();
            (path, columns(incoming))
        })
        .collect()
}

fn root(schema: &TableSchema, max_depth: u8) -> Vec<Column> {
    columns(&root_columns(schema, &shape(max_depth)).expect("declared root columns"))
}

fn grid() -> TableSchema {
    schema("grid", list(list(LogicalType::Int64)))
}

/// The tables an array of arrays of integers makes when both arrays are within depth: the outer
/// array's table holds no column, and the inner array's items are a grandchild table under
/// `value`.
fn grid_as_tables() -> Vec<(Vec<String>, Vec<Column>)> {
    vec![
        (path(&["grid"]), Vec::new()),
        (
            path(&["grid", "value"]),
            vec![(path(&["value"]), LogicalType::Int64)],
        ),
    ]
}

#[test]
fn an_array_of_arrays_within_depth_makes_a_grandchild_table_under_value() {
    assert_eq!(tables(&grid(), 3), grid_as_tables());
    assert_eq!(root(&grid(), 3), [(path(&["id"]), LogicalType::Int64)]);
}

#[test]
fn an_array_of_arrays_at_the_depth_limit_makes_a_grandchild_table_under_value() {
    assert_eq!(tables(&grid(), 2), grid_as_tables());
    assert_eq!(root(&grid(), 2), [(path(&["id"]), LogicalType::Int64)]);
}

#[test]
fn an_array_of_arrays_beyond_the_depth_limit_keeps_its_inner_arrays_as_the_column_value() {
    assert_eq!(
        tables(&grid(), 1),
        [(
            path(&["grid"]),
            vec![(path(&["value"]), list(LogicalType::Int64))],
        )],
    );
    assert_eq!(root(&grid(), 1), [(path(&["id"]), LogicalType::Int64)]);
}

fn size() -> LogicalType {
    object(&[("w", LogicalType::Int64)])
}

fn line() -> LogicalType {
    object(&[("sku", LogicalType::Utf8), ("size", size())])
}

fn lines() -> TableSchema {
    schema("lines", list(line()))
}

#[test]
fn an_array_of_objects_within_depth_flattens_its_items_into_columns() {
    assert_eq!(
        tables(&lines(), 3),
        [(
            path(&["lines"]),
            vec![
                (path(&["sku"]), LogicalType::Utf8),
                (path(&["size", "w"]), LogicalType::Int64),
            ],
        )],
    );
    assert_eq!(root(&lines(), 3), [(path(&["id"]), LogicalType::Int64)]);
}

#[test]
fn an_array_of_objects_at_the_depth_limit_flattens_its_items_and_keeps_their_objects_whole() {
    assert_eq!(
        tables(&lines(), 2),
        [(
            path(&["lines"]),
            vec![
                (path(&["sku"]), LogicalType::Utf8),
                (path(&["size"]), size()),
            ],
        )],
    );
    assert_eq!(root(&lines(), 2), [(path(&["id"]), LogicalType::Int64)]);
}

#[test]
fn an_array_of_objects_beyond_the_depth_limit_keeps_its_items_as_the_column_value() {
    assert_eq!(
        tables(&lines(), 1),
        [(path(&["lines"]), vec![(path(&["value"]), line())])],
    );
    assert_eq!(root(&lines(), 1), [(path(&["id"]), LogicalType::Int64)]);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(256)))]

    #[test]
    fn a_declared_schema_makes_each_table_and_column_its_plain_batch_normalizes_into(
        drawn in drawn(),
        max_depth in 0_u8..4,
    ) {
        let plain = batch(&drawn, plainly);
        let schema = TableSchema::from_arrow(&plain.schema()).expect("a drawn schema");
        let shape = shape(max_depth);
        let mut declared: BTreeMap<Vec<Arc<str>>, Vec<ColumnPath>> = children(&schema, &shape)
            .expect("a drawn schema's child tables")
            .into_iter()
            .take(64)
            .map(|(path, incoming)| (path, incoming.paths))
            .collect();
        let root = root_columns(&schema, &shape).expect("a drawn schema's own table");
        declared.insert(Vec::new(), root.paths);
        for part in normalize(&plain, &shape).expect("a plain batch normalizes") {
            prop_assert_eq!(declared.get(&part.path), Some(&part.columns), "{:?}", part.path);
        }
    }
}
