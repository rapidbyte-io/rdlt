use rdlt_connector::{Field, Fields, LogicalType};
use rdlt_testkit::drawn::Scalar;
use serde_json::{Value, json};

use std::collections::BTreeMap;

use super::{Node, Pending, Tables, child_rows, counted};

fn objects() -> LogicalType {
    let fields = Fields::new(vec![
        Field::new("x", LogicalType::Int64, true),
        Field::new("y", LogicalType::Json, true),
    ])
    .unwrap();
    let item = Field::new("item", LogicalType::Struct(fields), true);
    LogicalType::List(Box::new(item))
}

fn object(x: i64, y: Value) -> Scalar {
    Scalar::Struct(vec![
        ("x".to_owned(), Scalar::Int(x)),
        ("y".to_owned(), Scalar::Json(y)),
    ])
}

/// The tables one row holding `node` in column `a` normalizes into, to `max_depth`.
fn normalized(node: Node, max_depth: u8) -> Tables {
    let mut pending = Pending::default();
    pending.place(vec!["a".to_owned()], node, 1, max_depth);
    let mut tables = Tables::new();
    pending.emit(
        &["s".to_owned()],
        "1:2",
        max_depth,
        BTreeMap::new(),
        &mut tables,
    );
    tables
}

#[test]
fn an_array_of_objects_becomes_child_rows_of_their_fields() {
    let value = Scalar::List(vec![object(7, json!({"k": 1})), object(8, Value::Null)]);
    let tables = normalized(Node::Typed(value, objects()), 8);
    let children = &tables[&vec!["s".to_owned(), "a".to_owned()]];
    let idents: Vec<&str> = children.iter().map(|row| row.ident.as_str()).collect();
    assert_eq!(idents, ["1:2/s.a[0]", "1:2/s.a[1]"]);
    let columns: Vec<Vec<&str>> = children
        .iter()
        .map(|row| row.columns.keys().map(String::as_str).collect())
        .collect();
    assert_eq!(
        columns,
        [vec!["x", "y"], vec!["x"]],
        "JSON's null means no value once stored"
    );
    assert!(tables[&vec!["s".to_owned()]][0].columns.is_empty());
}

#[test]
fn containers_deeper_than_the_limit_are_stored_whole() {
    let value = Scalar::List(vec![object(7, Value::Null)]);
    let tables = normalized(Node::Typed(value, objects()), 1);
    let item = &tables[&vec!["s".to_owned(), "a".to_owned()]][0];
    assert_eq!(item.columns.keys().collect::<Vec<_>>(), ["value"]);
}

#[test]
fn pushed_arrays_of_arrays_become_grandchild_tables() {
    let tables = normalized(Node::Json(json!([[1, 2], []])), 8);
    let grandchildren = &tables[&vec!["s".to_owned(), "a".to_owned(), "value".to_owned()]];
    let idents: Vec<&str> = grandchildren.iter().map(|row| row.ident.as_str()).collect();
    assert_eq!(
        idents,
        ["1:2/s.a[0]/s.a.value[0]", "1:2/s.a[0]/s.a.value[1]"]
    );
}

#[test]
fn a_json_null_counts_as_a_value_its_policy_discards() {
    let null = || {
        vec![(
            "a".to_owned(),
            Node::Typed(Scalar::Json(Value::Null), LogicalType::Json),
        )]
    };
    assert_eq!(counted(null(), None), 1);
    assert_eq!(counted(null(), Some(8)), 1, "normalized, it is a leaf too");
}

#[test]
fn an_empty_array_or_an_object_of_nulls_counts_only_where_it_is_stored_whole() {
    let empty = || vec![("a".to_owned(), Node::Json(json!([])))];
    assert_eq!(counted(empty(), None), 1);
    assert_eq!(counted(empty(), Some(8)), 0, "normalized, it holds no item");
    let nulls = || vec![("a".to_owned(), Node::Json(json!({"x": null})))];
    assert_eq!(counted(nulls(), None), 1);
    assert_eq!(
        counted(nulls(), Some(8)),
        0,
        "normalized, it holds no value column"
    );
    let items = vec![("a".to_owned(), Node::Json(json!([{"x": 1}, 2])))];
    assert_eq!(counted(items, Some(8)), 2, "each item counts");
}

#[test]
fn a_value_adds_a_child_row_for_each_array_item_at_any_depth_within_the_limit() {
    let value = || Node::Json(json!([{"x": 1}, {"x": 2, "y": [1, 2]}, 3]));
    assert_eq!(
        child_rows("a", value(), 8),
        5,
        "three items and the inner array's two"
    );
    assert_eq!(
        child_rows("a", value(), 1),
        3,
        "the inner array is stored whole"
    );
    let object = Node::Json(json!({"x": 1, "y": {"z": 2}}));
    assert_eq!(
        child_rows("a", object, 8),
        0,
        "objects flatten into columns"
    );
}
