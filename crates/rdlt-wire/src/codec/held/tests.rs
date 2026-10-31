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

/// Bytes: a schema's `Arc` and its list of `fields` fields.
const fn top(fields: u64) -> u64 {
    80 + 16 + 8 * fields
}

#[test]
fn a_schema_counts_its_lists_fields_names_and_what_its_types_box() {
    let plain = Schema::new(vec![named("ab", DataType::Int8)]);
    assert_eq!(schema_held_bytes(&plain), top(1) + 128 + 2);
    // A megabyte of name is a megabyte held, wherever the field lies; a dictionary boxes its
    // two types, and a struct holds a list of its fields.
    let long = "n".repeat(1 << 20);
    let inner = named(&long, DataType::Int8);
    let item = Arc::new(named("item", DataType::Struct(Fields::from(vec![inner]))));
    let keyed = DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::List(item)));
    let nested_keyed = Schema::new(vec![named("c", keyed)]);
    let fields = 3 * 128 + 1 + 4 + (1 << 20);
    assert_eq!(
        schema_held_bytes(&nested_keyed),
        top(1) + fields + 2 * 24 + (16 + 8)
    );
    // A dictionary of dictionaries boxes each one's types.
    let twice = DataType::Dictionary(
        Box::new(DataType::Int8),
        Box::new(DataType::Dictionary(
            Box::new(DataType::Int16),
            Box::new(DataType::Utf8),
        )),
    );
    let twice = Schema::new(vec![named("d", twice)]);
    assert_eq!(schema_held_bytes(&twice), top(1) + 128 + 1 + 4 * 24);
    // A type nested deeper than any stack walks is counted to its end.
    let deep = 80 + 3_000 * (128 + 1 + 16 + 8);
    assert_eq!(schema_held_bytes(&nested(3_000)), deep);
    // A union's members are fields of it, each beside its id, and so are a run-end encoding's
    // ends and values.
    let members = [named("i", DataType::Int8), named("s", DataType::Utf8)];
    let members = UnionFields::try_new([0, 1], members).unwrap();
    let union = DataType::Union(members, UnionMode::Dense);
    let ends = Arc::new(Field::new("run_ends", DataType::Int16, false));
    let runs = DataType::RunEndEncoded(ends, Arc::new(named("values", DataType::Int8)));
    let encoded = Schema::new(vec![named("u", union), named("r", runs)]);
    let texts = 1 + 1 + 1 + 1 + "run_ends".len() + "values".len();
    let texts = u64::try_from(texts).unwrap();
    assert_eq!(
        schema_held_bytes(&encoded),
        top(2) + 6 * 128 + texts + (16 + 2 * 16)
    );
}

#[test]
fn a_schema_counts_its_metadata_and_time_zones_by_the_room_they_hold() {
    // A map of one entry has room for three; a time zone's text is padded to eight bytes.
    let one = |key: &str, value: &str| HashMap::from([(key.to_owned(), value.to_owned())]);
    let zoned = DataType::Timestamp(TimeUnit::Second, Some("Europe/Warsaw".into()));
    let field = named("t", zoned).with_metadata(one("k", "vv"));
    let schema = Schema::new(vec![field]).with_metadata(one("key", "value"));
    let map = 44 + 3 * 56;
    let held = top(1) + (map + 8) + 128 + 1 + (map + 3) + (16 + 16);
    assert_eq!(schema_held_bytes(&schema), held);
    // Text and a map count the room they hold, not only what fills it.
    let mut name = String::with_capacity(64);
    name.push('c');
    let mut room = HashMap::with_capacity(100);
    room.insert("k".to_owned(), String::with_capacity(32));
    let slots = u64::try_from(room.capacity()).unwrap();
    let spare = Schema::new(vec![
        Field::new(name, DataType::Int8, true).with_metadata(room),
    ]);
    let held = top(1) + 128 + 64 + (44 + slots * 56) + 1 + 32;
    assert_eq!(schema_held_bytes(&spare), held);
    // A map emptied keeps the room it held.
    let mut cleared = one("k", "v");
    cleared.clear();
    let emptied = Schema::new(vec![named("e", DataType::Int8).with_metadata(cleared)]);
    assert_eq!(schema_held_bytes(&emptied), top(1) + 128 + 1 + map);
}
