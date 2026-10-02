use serde_json::{Value, json};

use crate::limits::MAX_NESTING_DEPTH;
use crate::types::{Field, Fields, LogicalType};

/// A list type nested `levels` deep, counting itself as the first, its innermost item an integer.
fn lists(levels: u64) -> LogicalType {
    (1..levels).fold(LogicalType::Int64, |item, _| {
        LogicalType::List(Box::new(Field::new("item", item, true)))
    })
}

/// How deep `value` nests as JSON text.
fn json_depth(value: &Value) -> usize {
    match value {
        Value::Array(items) => 1 + items.iter().map(json_depth).max().unwrap_or(0),
        Value::Object(fields) => 1 + fields.values().map(json_depth).max().unwrap_or(0),
        _ => 0,
    }
}

#[test]
fn a_type_is_stored_as_its_nodes_in_preorder() {
    let fields = Fields::new(vec![
        Field::new("a", LogicalType::Int8, false),
        Field::new("b", lists(2), true),
    ])
    .unwrap();
    let stored = serde_json::to_value(LogicalType::Struct(fields)).unwrap();
    assert_eq!(
        stored,
        json!([
            {"type": {"struct": 2}},
            {"name": "a", "nullable": false, "type": "int8"},
            {"name": "b", "nullable": true, "type": "list"},
            {"name": "item", "nullable": true, "type": "int64"},
        ])
    );
}

#[test]
fn a_stored_type_nests_no_deeper_however_deep_the_type() {
    for levels in [1, 2, MAX_NESTING_DEPTH] {
        let field = Field::new("deep", lists(levels), true);
        let stored = serde_json::to_value(&field).unwrap();
        assert!(json_depth(&stored) <= 3, "{levels} levels");
        assert_eq!(serde_json::from_value::<Field>(stored).unwrap(), field);
    }
}

#[test]
fn a_stored_type_nested_past_the_limit_is_refused() {
    let stored = serde_json::to_value(lists(MAX_NESTING_DEPTH + 1)).unwrap();
    assert!(serde_json::from_value::<LogicalType>(stored).is_err());
    let field = Field::new("deep", lists(MAX_NESTING_DEPTH + 1), true);
    assert!(serde_json::from_value::<Field>(serde_json::to_value(field).unwrap()).is_err());
}

#[test]
fn a_stored_type_that_is_not_one_type_is_refused() {
    let int = json!({"name": "a", "nullable": true, "type": "int8"});
    let refused = [
        // No node at all.
        json!([]),
        // A struct naming more fields than follow, as many as a word holds among them.
        json!([{"type": {"struct": 2}}, int]),
        json!([{"type": {"struct": usize::MAX}}, int]),
        // A list without its item.
        json!([{"type": "list"}]),
        // Nodes beyond the type's own.
        json!([{"type": "int8"}, int]),
        // A field named by half, and a field without a name.
        json!([{"type": "list"}, {"name": "item", "type": "int8"}]),
        json!([{"type": "list"}, {"type": "int8"}]),
        // Two fields of one name.
        json!([{"type": {"struct": 2}}, int, int]),
        // A field the reader does not know, and a kind it does not know.
        json!([{"type": "int8", "width": 8}]),
        json!([{"type": "int128"}]),
    ];
    for stored in refused {
        assert!(
            serde_json::from_value::<LogicalType>(stored.clone()).is_err(),
            "{stored}"
        );
    }
    // A type's root names no field, and a field's root names one.
    assert!(serde_json::from_value::<LogicalType>(json!([int])).is_err());
    assert!(serde_json::from_value::<Field>(json!([{"type": "int8"}])).is_err());
    assert_eq!(
        serde_json::from_value::<Field>(json!([int])).unwrap(),
        Field::new("a", LogicalType::Int8, true)
    );
}
