use rdlt_connector::{Field, Fields, LogicalType};
use rdlt_testkit::drawn::Scalar;
use serde_json::{Value, json};

use super::{Node, Pending, Tables};

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
    pending.emit(&["s".to_owned()], "1:2", max_depth, &mut tables);
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
