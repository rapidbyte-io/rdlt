//! What a schema holds, in the `Arc` a batch keeps it in, is within what its count says, at every
//! shape the count walks.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit, UnionFields, UnionMode};
use rdlt_wire::schema_held_bytes;

use super::HEAP;

/// How to make a schema, again for each window it is measured in.
type Make = Box<dyn Fn() -> Schema>;

/// A schema's shape, by what it holds, and how to make one.
type Shaped = (&'static str, fn() -> Schema);

/// What `make` keeps on the heap while what it made is alive, the least over eight windows that
/// leave the heap as it began once that is dropped, and what a ninth makes.
fn held_alone<T>(make: impl Fn() -> T) -> (u64, T) {
    let mut least: Option<usize> = None;
    for _ in 0..8 {
        let before = HEAP.current_usage();
        let alive = make();
        let held = HEAP.current_usage().saturating_sub(before);
        drop(alive);
        if HEAP.current_usage() == before && least.is_none_or(|low| held < low) {
            least = Some(held);
        }
    }
    let held = least.expect("a window ended with the heap holding what it began with");
    (u64::try_from(held).expect("a size"), make())
}

/// A schema of `fields` fields, each made by `field` from its place.
fn many(fields: usize, field: impl Fn(usize) -> Field) -> Schema {
    Schema::new((0..fields).map(field).collect::<Vec<_>>())
}

/// A field named for its `place`, as `format!` names it.
fn named(place: usize, data_type: DataType) -> Field {
    Field::new(format!("c{place}"), data_type, true)
}

fn item(data_type: DataType) -> Arc<Field> {
    Arc::new(Field::new("item", data_type, true))
}

fn keyed(values: DataType) -> DataType {
    DataType::Dictionary(Box::new(DataType::Int32), Box::new(values))
}

/// `entries` entries of metadata, of short keys and values.
fn metadata(entries: usize) -> HashMap<String, String> {
    let entry = |entry| (format!("k{entry}"), "v".to_owned());
    (0..entries).map(entry).collect()
}

fn integers() -> Schema {
    many(1_000, |place| named(place, DataType::Int64))
}

fn text() -> Schema {
    many(1_000, |place| named(place, DataType::Utf8))
}

fn structs_of_ten() -> Schema {
    many(100, |place| {
        let inner = (0..10).map(|inner| named(inner, DataType::Int64));
        named(place, DataType::Struct(inner.collect()))
    })
}

fn empty_structs() -> Schema {
    many(1_000, |place| {
        named(place, DataType::Struct(Fields::empty()))
    })
}

fn lists_of_each_kind() -> Schema {
    many(1_000, |place| {
        let items = item(DataType::Int64);
        let data_type = match place % 5 {
            0 => DataType::List(items),
            1 => DataType::LargeList(items),
            2 => DataType::ListView(items),
            3 => DataType::LargeListView(items),
            _ => DataType::FixedSizeList(items, 4),
        };
        named(place, data_type)
    })
}

fn maps() -> Schema {
    many(500, |place| {
        let key = Field::new("key", DataType::Utf8, false);
        let value = Field::new("value", DataType::Int64, true);
        let entries = DataType::Struct(Fields::from(vec![key, value]));
        let entries = Arc::new(Field::new("entries", entries, false));
        named(place, DataType::Map(entries, false))
    })
}

fn unions() -> Schema {
    many(600, |place| {
        let members = [DataType::Int64, DataType::Utf8, DataType::Boolean];
        let members = members.into_iter().enumerate();
        let members = members.map(|(member, data_type)| named(member, data_type));
        let members = UnionFields::try_new([0, 1, 2], members).expect("an id a member");
        let mode = [UnionMode::Dense, UnionMode::Sparse][place % 2];
        named(place, DataType::Union(members, mode))
    })
}

fn run_ends() -> Schema {
    many(1_000, |place| {
        let ends = Arc::new(Field::new("run_ends", DataType::Int32, false));
        let values = Arc::new(Field::new("values", DataType::Utf8, true));
        named(place, DataType::RunEndEncoded(ends, values))
    })
}

fn dictionaries() -> Schema {
    many(1_000, |place| {
        let data_type = match place % 3 {
            0 => keyed(DataType::Utf8),
            1 => keyed(DataType::List(item(DataType::Utf8))),
            _ => DataType::List(item(keyed(DataType::Utf8))),
        };
        named(place, data_type)
    })
}

fn time_zones() -> Schema {
    many(1_000, |place| {
        let zone = ["UTC", "Europe/Warsaw", "+08:00"][place % 3];
        named(
            place,
            DataType::Timestamp(TimeUnit::Second, Some(zone.into())),
        )
    })
}

fn schema_metadata() -> Schema {
    many(10, |place| named(place, DataType::Int64)).with_metadata(metadata(10))
}

/// Names, maps and values with room beyond what fills them.
fn room_to_spare() -> Schema {
    many(100, |_| {
        let mut name = String::with_capacity(100);
        name.push('c');
        let mut room = HashMap::with_capacity(64);
        room.insert("k".to_owned(), String::with_capacity(32));
        Field::new(name, DataType::Int64, true).with_metadata(room)
    })
}

fn nested_sixty_four_deep() -> Schema {
    let mut data_type = DataType::Int64;
    for level in 1..64 {
        data_type = DataType::Struct(Fields::from(vec![named(level, data_type)]));
    }
    Schema::new(vec![named(0, data_type)])
}

/// Schemas of every shape the count walks, each of many fields, by what they hold.
fn shapes() -> Vec<(String, Make)> {
    let made: [Shaped; 13] = [
        ("integers", integers),
        ("text", text),
        ("structs of ten", structs_of_ten),
        ("empty structs", empty_structs),
        ("lists of each kind", lists_of_each_kind),
        ("maps", maps),
        ("unions", unions),
        ("run ends", run_ends),
        ("dictionaries", dictionaries),
        ("time zones", time_zones),
        ("schema metadata", schema_metadata),
        ("room to spare", room_to_spare),
        ("nested sixty-four deep", nested_sixty_four_deep),
    ];
    let mut shapes: Vec<(String, Make)> = made
        .into_iter()
        .map(|(what, make)| (what.to_owned(), Box::new(make) as Make))
        .collect();
    for entries in [1, 2, 3, 4, 7, 8, 15, 100] {
        let described = move || {
            many(100, |place| {
                named(place, DataType::Int64).with_metadata(metadata(entries))
            })
        };
        shapes.push((format!("metadata of {entries}"), Box::new(described)));
    }
    shapes
}

#[test]
fn a_schema_holds_no_more_than_its_count_and_not_much_less() {
    let mut found = Vec::new();
    for (what, make) in shapes() {
        let (held, schema) = held_alone(|| Arc::new(make()));
        let counted = schema_held_bytes(&schema);
        // A map's table is counted at most a few bytes a slot beyond what it takes.
        if held > counted || counted > held + held / 16 {
            found.push(format!("{what}: {counted} counted, {held} held"));
        }
    }
    assert!(found.is_empty(), "{}", found.join("\n"));
}
