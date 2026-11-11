use rdlt_connector::{Checkpointing, Field, Fields, LogicalType, ReadMode};
use rdlt_engine::{Nested, SchemaPolicy};
use rdlt_testkit::drawn::{Encoding, Scalar, Shape};
use serde_json::{Value, json};

use std::collections::BTreeMap;

use super::{Node, Pending, Tables, child_rows, counted, owned};
use crate::rng::SplitMix64;
use crate::swarm::Features;
use crate::workload::{Level, Reshaping, Row, SimStream, Workload};

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

/// `value`, pushed as JSON.
fn pushed(value: Value) -> Node {
    fn scalar(value: Value) -> Scalar {
        match value {
            Value::Null => Scalar::Null,
            Value::Bool(value) => Scalar::Bool(value),
            Value::Number(number) => number
                .as_i64()
                .map_or_else(|| Scalar::Float64(number.as_f64().unwrap()), Scalar::Int),
            Value::String(text) => Scalar::Utf8(text),
            Value::Array(items) => Scalar::List(items.into_iter().map(scalar).collect()),
            Value::Object(members) => Scalar::Struct(
                members
                    .into_iter()
                    .map(|(name, inner)| (name, scalar(inner)))
                    .collect(),
            ),
        }
    }
    Node::Json(scalar(value), LogicalType::Json)
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
    let tables = normalized(pushed(json!([[1, 2], []])), 8);
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
    let empty = || vec![("a".to_owned(), pushed(json!([])))];
    assert_eq!(counted(empty(), None), 1);
    assert_eq!(counted(empty(), Some(8)), 0, "normalized, it holds no item");
    let nulls = || vec![("a".to_owned(), pushed(json!({"x": null})))];
    assert_eq!(counted(nulls(), None), 1);
    assert_eq!(
        counted(nulls(), Some(8)),
        0,
        "normalized, it holds no value column"
    );
    let items = vec![("a".to_owned(), pushed(json!([{"x": 1}, 2])))];
    assert_eq!(counted(items, Some(8)), 2, "each item counts");
}

#[test]
fn a_value_adds_a_child_row_for_each_array_item_at_any_depth_within_the_limit() {
    let value = || pushed(json!([{"x": 1}, {"x": 2, "y": [1, 2]}, 3]));
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
    let object = pushed(json!({"x": 1, "y": {"z": 2}}));
    assert_eq!(
        child_rows("a", object, 8),
        0,
        "objects flatten into columns"
    );
}

#[test]
fn a_pushed_integer_a_float_column_would_round_is_told_apart() {
    let json = |text: &str| super::Sent::Json(text.to_owned());
    let edge = 1_i64 << 53;
    assert!(!json(&edge.to_string()).rounds_into(&LogicalType::Float64));
    assert!(!json(&(-edge).to_string()).rounds_into(&LogicalType::Float64));
    assert!(json(&(edge + 1).to_string()).rounds_into(&LogicalType::Float64));
    assert!(json(&"9".repeat(40)).rounds_into(&LogicalType::Float64));
    assert!(json(&((1 << 24) + 1).to_string()).rounds_into(&LogicalType::Float32));
    assert!(
        !json("9007199254740993.5").rounds_into(&LogicalType::Float64),
        "a float is no integer"
    );
    assert!(!json(&(edge + 1).to_string()).rounds_into(&LogicalType::Int64));
}

/// The row at `offset` of partition `partition`, holding `value` in its stream's one drift column.
fn row(partition: i64, offset: i64, value: Scalar) -> Row {
    Row {
        id: partition * 1_000_000 + offset,
        partition,
        offset,
        value: 0,
        key: None,
        tag: None,
        extras: vec![Some(value)],
        delivered: 0,
    }
}

fn shape(logical: LogicalType) -> Shape {
    Shape {
        logical,
        encoding: Encoding::Plain,
        children: Vec::new(),
    }
}

/// A JSON stream, pushing a row each and gathering its whole read, whose filter keeps odd ids:
/// partition 0 holds lists in the rows it drops and nulls in those it keeps, partition 1 text.
fn lists_filtered_from_nulls() -> SimStream {
    let drawn = (0..400)
        .flat_map(|seed| {
            let features = Features {
                transform: true,
                ..Features::ALL
            };
            Workload::generate(&mut SplitMix64::new(seed), features).streams
        })
        .find(|stream| stream.json && !stream.drift.is_empty())
        .expect("a generated stream pushing JSON with a drift column");
    let list = LogicalType::List(Box::new(Field::new("item", LogicalType::Utf8, true)));
    let mut stream = drawn;
    stream.reads = [ReadMode::Full; 2];
    stream.checkpointing = Checkpointing::OnDemand;
    stream.batch_rows = 1;
    stream.partitions = vec![[4, 0], [4, 0]];
    stream.schema = Level {
        policy: Some(SchemaPolicy::Evolve),
        on_unsupported: None,
        nested: Some(Nested::Native),
    };
    stream.pipeline = Level::default();
    stream.reshaping = Reshaping {
        filter: Some(2),
        ..Reshaping::default()
    };
    stream.drift.truncate(1);
    stream.drift[0].shapes = vec![
        [Some(shape(list)), None],
        [Some(shape(LogicalType::Utf8)), None],
    ];
    stream.drift[0].settings = Level::default();
    stream.drift[0].hint = None;
    stream.drift[0].declared = None;
    let text = || Scalar::Utf8("x".to_owned());
    let lists = || Scalar::List(vec![text()]);
    stream.set_rows(vec![
        [
            vec![
                row(0, 0, lists()),
                row(0, 1, Scalar::Null),
                row(0, 2, lists()),
                row(0, 3, Scalar::Null),
            ],
            Vec::new(),
        ],
        [
            (0..4).map(|offset| row(1, offset, text())).collect(),
            Vec::new(),
        ],
    ]);
    stream
}

#[test]
fn a_column_a_filtered_push_widens_has_no_sure_type() {
    let stream = lists_filtered_from_nulls();
    let kept: Vec<Row> = (0..2)
        .flat_map(|partition| stream.rows(partition, 0))
        .filter(|row| stream.reshaping.keeps(row))
        .cloned()
        .collect();
    assert_eq!(
        owned(&stream, &kept, 0),
        None,
        "the lists its filter drops shred with the nulls of its first partition"
    );
}
