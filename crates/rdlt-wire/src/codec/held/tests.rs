use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit, UnionFields, UnionMode};

use super::schema_held_bytes;

fn named(name: &str, data_type: DataType) -> Field {
    Field::new(name, data_type, true)
}

/// A schema of one column nested `levels` deep in structs of one field, a top-level column being
/// the first level.
fn nested(levels: usize) -> Schema {
    let mut data_type = DataType::Int64;
    for _ in 1..levels {
        data_type = DataType::Struct(Fields::from(vec![named("a", data_type)]));
    }
    Schema::new(vec![named("a", data_type)])
}

#[test]
fn a_schema_counts_its_names_and_metadata_at_any_depth() {
    let plain = Schema::new(vec![named("ab", DataType::Int8)]);
    assert_eq!(schema_held_bytes(&plain), 128 + 2);
    // A megabyte of name is a megabyte held, wherever the field lies.
    let long = "n".repeat(1 << 20);
    let inner = named(&long, DataType::Int8);
    let item = Arc::new(named("item", DataType::Struct(Fields::from(vec![inner]))));
    let keyed = DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::List(item)));
    let nested_keyed = Schema::new(vec![named("c", keyed)]);
    assert_eq!(
        schema_held_bytes(&nested_keyed),
        3 * 128 + 1 + 4 + (1 << 20)
    );
    // Metadata of the schema and of a field, and a time zone.
    let zoned = DataType::Timestamp(TimeUnit::Second, Some("Europe/Warsaw".into()));
    let field = named("t", zoned).with_metadata(HashMap::from([("k".to_owned(), "vv".to_owned())]));
    let schema = Schema::new(vec![field])
        .with_metadata(HashMap::from([("key".to_owned(), "value".to_owned())]));
    assert_eq!(
        schema_held_bytes(&schema),
        (48 + 8) + 128 + 1 + (48 + 3) + 13
    );
    // A type nested deeper than any stack walks is counted to its end.
    assert_eq!(schema_held_bytes(&nested(3_000)), 3_000 * 128 + 3_000);
    // A union's members are fields of it, and so are a run-end encoding's ends and values.
    let members = [named("i", DataType::Int8), named("s", DataType::Utf8)];
    let members = UnionFields::try_new([0, 1], members).unwrap();
    let union = DataType::Union(members, UnionMode::Dense);
    let ends = Arc::new(Field::new("run_ends", DataType::Int16, false));
    let runs = DataType::RunEndEncoded(ends, Arc::new(named("values", DataType::Int8)));
    let encoded = Schema::new(vec![named("u", union), named("r", runs)]);
    let texts = 1 + 1 + 1 + 1 + "run_ends".len() + "values".len();
    assert_eq!(
        schema_held_bytes(&encoded),
        6 * 128 + u64::try_from(texts).unwrap()
    );
}
