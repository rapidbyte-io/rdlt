use std::collections::HashMap;
use std::sync::Arc;

use arrow_ipc::writer::IpcWriteOptions;
use arrow_ipc::{Endianness, MessageHeader, MetadataVersion};
use arrow_schema::{DataType, Field, Fields, Schema, UnionFields, UnionMode};
use bytes::Bytes;
use flatbuffers::{FlatBufferBuilder, WIPOffset};

use crate::codec::tests::frames::{problem, refusal};
use crate::codec::{Decoder, Encoder};
use crate::error::Problem;
use crate::limits::{CONTROL_STRING_BYTES, Limits, NESTING_DEPTH, SCHEMA_COLUMNS};

/// The text every field of a hand-built schema shares.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Shared {
    /// Each field's name.
    Name,
    /// Each field's time zone.
    Zone,
    /// The key of each field's one metadata entry.
    Key,
    /// The value of each field's one metadata entry.
    Value,
    /// The value of each of the schema's own metadata entries, one a field.
    SchemaValue,
}

/// A hand-built schema message: `fields` fields that share one text of `bytes` bytes, and one
/// field table when `one_table`.
struct Sharing {
    fields: usize,
    shared: Shared,
    bytes: usize,
    one_table: bool,
    endianness: Endianness,
}

impl Sharing {
    fn names(fields: usize, bytes: usize) -> Self {
        Self {
            fields,
            shared: Shared::Name,
            bytes,
            one_table: false,
            endianness: Endianness::Little,
        }
    }

    fn message(&self) -> Bytes {
        let mut fbb = FlatBufferBuilder::new();
        let long = fbb.create_string(&"n".repeat(self.bytes));
        let short = fbb.create_string("k");
        let empty = fbb.create_string("");
        let entry = |fbb: &mut FlatBufferBuilder<'_>, key, value| {
            let mut entry = arrow_ipc::KeyValueBuilder::new(fbb);
            entry.add_key(key);
            entry.add_value(value);
            entry.finish().value()
        };
        let entry = match self.shared {
            Shared::Key => Some(entry(&mut fbb, long, short)),
            Shared::Value | Shared::SchemaValue => Some(entry(&mut fbb, short, long)),
            Shared::Name | Shared::Zone => None,
        };
        let entry = entry.map(WIPOffset::<arrow_ipc::KeyValue<'_>>::new);
        let entries = entry.map(|entry| fbb.create_vector(&[entry]));
        let of_schema = entry.map(|entry| fbb.create_vector(&vec![entry; self.fields]));
        let (kind, type_) = if self.shared == Shared::Zone {
            let mut time = arrow_ipc::TimestampBuilder::new(&mut fbb);
            time.add_timezone(long);
            (arrow_ipc::Type::Timestamp, time.finish().as_union_value())
        } else {
            let null = arrow_ipc::NullBuilder::new(&mut fbb).finish();
            (arrow_ipc::Type::Null, null.as_union_value())
        };
        let field = |fbb: &mut FlatBufferBuilder<'_>| {
            let mut field = arrow_ipc::FieldBuilder::new(fbb);
            field.add_name(if self.shared == Shared::Name {
                long
            } else {
                empty
            });
            field.add_type_type(kind);
            field.add_type_(type_);
            if let Some(entries) = entries.filter(|_| self.shared != Shared::SchemaValue) {
                field.add_custom_metadata(entries);
            }
            field.finish().value()
        };
        let tables: Vec<_> = if self.one_table {
            vec![field(&mut fbb); self.fields]
        } else {
            (0..self.fields).map(|_| field(&mut fbb)).collect()
        };
        let tables: Vec<_> = tables
            .into_iter()
            .map(WIPOffset::<arrow_ipc::Field<'_>>::new)
            .collect();
        let tables = fbb.create_vector(&tables);
        let mut schema = arrow_ipc::SchemaBuilder::new(&mut fbb);
        schema.add_endianness(self.endianness);
        schema.add_fields(tables);
        if let Some(entries) = of_schema.filter(|_| self.shared == Shared::SchemaValue) {
            schema.add_custom_metadata(entries);
        }
        let schema = schema.finish().as_union_value();
        finished(fbb, schema)
    }
}

/// The schema message of `schema`, built in `fbb`.
fn finished(
    mut fbb: FlatBufferBuilder<'_>,
    schema: WIPOffset<flatbuffers::UnionWIPOffset>,
) -> Bytes {
    let mut message = arrow_ipc::MessageBuilder::new(&mut fbb);
    message.add_version(MetadataVersion::V5);
    message.add_header_type(MessageHeader::Schema);
    message.add_header(schema);
    let message = message.finish();
    fbb.finish(message, None);
    Bytes::copy_from_slice(fbb.finished_data())
}

#[test]
fn a_schema_whose_fields_share_one_long_name_is_refused() {
    // Within the column limit and under half a mebibyte long, yet two gibibytes of names once
    // each field holds its own.
    let message = Sharing::names(10_000, 200_000).message();
    assert!(message.len() < 1 << 19);
    let decoded = Decoder::new(Limits::default()).schema(&message);
    assert_eq!(problem(decoded), Problem::Inflated);
}

#[test]
fn a_schema_repeating_one_field_table_is_refused() {
    // Twice the tables a message of its length can hold: a field and its type, each time the
    // one table repeats.
    let message = Sharing {
        one_table: true,
        ..Sharing::names(5_000, 1)
    }
    .message();
    assert!(message.len() / 4 < 2 * 5_000);
    let decoded = Decoder::new(Limits::default()).schema(&message);
    assert_eq!(problem(decoded), Problem::Inflated);
    // Repeated within what its length can hold, the fields count as any others.
    let message = Sharing {
        one_table: true,
        ..Sharing::names(8, 1)
    }
    .message();
    let schema = Decoder::new(Limits::default()).schema(&message).unwrap();
    assert_eq!(schema.fields().len(), 8);
}

#[test]
fn text_a_schema_repeats_counts_wherever_a_field_repeats_it() {
    let (fields, bytes) = (10, 1_000);
    let cases = [
        (Shared::Name, 10 * 1_000),
        (Shared::Zone, 10 * 1_000),
        (Shared::Key, 10 * (1_000 + 1)),
        (Shared::Value, 10 * (1 + 1_000)),
        (Shared::SchemaValue, 10 * (1 + 1_000)),
    ];
    for (shared, text) in cases {
        let message = Sharing {
            shared,
            ..Sharing::names(fields, bytes)
        }
        .message();
        // The message itself is well within the limit its text exceeds.
        assert!(message.len() < 2_000, "{shared:?}");
        let within = |schema_bytes| {
            Decoder::new(Limits {
                schema_bytes,
                ..Limits::default()
            })
            .schema(&message)
        };
        let schema = within(text).unwrap();
        assert_eq!(schema.fields().len(), fields, "{shared:?}");
        let refusal = refusal(within(text - 1));
        assert_eq!(
            (refusal.field, refusal.limit, refusal.actual),
            ("schema bytes", text - 1, text),
            "{shared:?}"
        );
    }
}

#[test]
fn a_schema_message_beyond_its_byte_limit_is_refused() {
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
    let message = Encoder::default().schema(&schema);
    let bytes = u64::try_from(message.len()).unwrap();
    let within = |schema_bytes| {
        Decoder::new(Limits {
            schema_bytes,
            ..Limits::default()
        })
        .schema(&message)
    };
    assert_eq!(within(bytes).unwrap().as_ref(), &schema);
    let refusal = refusal(within(bytes - 1));
    assert_eq!(
        (refusal.field, refusal.limit, refusal.actual),
        ("schema bytes", bytes - 1, bytes)
    );
    // A frame's limit does not bound a schema message.
    let small = Limits {
        frame_bytes: 16,
        ..Limits::default()
    };
    assert!(Decoder::new(small).schema(&message).is_ok());
}

#[test]
fn a_field_name_beyond_the_control_string_limit_is_refused_wherever_it_nests() {
    let long = "n".repeat(100);
    let top = Schema::new(vec![Field::new(&long, DataType::Int32, true)]);
    let inner = Fields::from(vec![Field::new(&long, DataType::Int32, true)]);
    let nested = Schema::new(vec![Field::new("a", DataType::Struct(inner), true)]);
    for schema in [top, nested] {
        let message = Encoder::default().schema(&schema);
        let within = |control_string_bytes| {
            Decoder::new(Limits {
                control_string_bytes,
                ..Limits::default()
            })
            .schema(&message)
        };
        assert_eq!(within(100).unwrap().as_ref(), &schema);
        let refusal = refusal(within(99));
        assert_eq!(
            (refusal.field, refusal.limit, refusal.actual),
            ("control string bytes", 99, 100)
        );
    }
    let beyond = usize::try_from(CONTROL_STRING_BYTES).unwrap() + 1;
    let message = Sharing::names(1, beyond).message();
    let refusal = refusal(Decoder::new(Limits::default()).schema(&message));
    assert_eq!(refusal.field, "control string bytes");
}

#[test]
fn a_schema_beyond_the_column_limit_is_refused_before_it_is_converted() {
    let fields: Vec<_> = (0..2 * SCHEMA_COLUMNS)
        .map(|column| Field::new(format!("c{column}"), DataType::Null, true))
        .collect();
    let message = Encoder::default().schema(&Schema::new(fields));
    let refusal = refusal(Decoder::new(Limits::default()).schema(&message));
    // The count stops at the first column beyond the limit.
    assert_eq!(
        (refusal.field, refusal.actual),
        ("schema columns", SCHEMA_COLUMNS + 1)
    );
}

#[test]
fn a_schema_at_the_column_and_depth_limits_is_admitted() {
    let depth = usize::try_from(NESTING_DEPTH).unwrap();
    let columns = usize::try_from(SCHEMA_COLUMNS).unwrap();
    let deep = (1..depth).fold(DataType::Int32, |item, _| {
        DataType::List(Arc::new(Field::new("item", item, true)))
    });
    let described = HashMap::from([
        ("ARROW:extension:name".to_owned(), "arrow.json".to_owned()),
        ("comment".to_owned(), "what the column holds".repeat(4)),
    ]);
    let mut fields = vec![Field::new("deep", deep, true)];
    fields.extend((depth..columns).map(|column| {
        let name = format!("a_column_with_a_name_of_some_length_{column}");
        let zoned = DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into()));
        Field::new(name, zoned, true).with_metadata(described.clone())
    }));
    let schema = Schema::new(fields).with_metadata(described);
    let message = Encoder::default().schema(&schema);
    let decoded = Decoder::new(Limits::default()).schema(&message).unwrap();
    assert_eq!(decoded.as_ref(), &schema);
}

#[test]
fn every_kind_of_nested_type_counts_its_columns_and_levels() {
    let item = || Arc::new(Field::new("item", DataType::Int32, true));
    let pair = Fields::from(vec![
        Field::new("k", DataType::Utf8, false),
        Field::new("v", DataType::Int32, true),
    ]);
    let union = UnionFields::try_new(
        vec![0, 1],
        vec![
            Field::new("a", DataType::Int8, true),
            Field::new("b", DataType::Utf8, true),
        ],
    );
    let entries = Arc::new(Field::new("entries", DataType::Struct(pair.clone()), false));
    let ends = Arc::new(Field::new("run_ends", DataType::Int32, false));
    let cases: Vec<(DataType, u64, u64)> = vec![
        (DataType::Int32, 1, 1),
        (DataType::List(item()), 2, 2),
        (DataType::LargeList(item()), 2, 2),
        (DataType::ListView(item()), 2, 2),
        (DataType::LargeListView(item()), 2, 2),
        (DataType::FixedSizeList(item(), 2), 2, 2),
        (DataType::Map(entries, false), 4, 3),
        (DataType::Struct(pair), 3, 2),
        (DataType::Union(union.unwrap(), UnionMode::Dense), 3, 2),
        (DataType::RunEndEncoded(ends, item()), 3, 2),
        (
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::List(item()))),
            2,
            2,
        ),
    ];
    for (data_type, columns, depth) in cases {
        let schema = Schema::new(vec![Field::new("c", data_type.clone(), true)]);
        let message = Encoder::default().schema(&schema);
        let within = |schema_columns, nesting_depth| {
            Decoder::new(Limits {
                schema_columns,
                nesting_depth,
                ..Limits::default()
            })
            .schema(&message)
        };
        assert_eq!(within(columns, depth).unwrap().as_ref(), &schema);
        if columns > 1 {
            let refusal = refusal(within(columns - 1, depth));
            assert_eq!(
                (refusal.field, refusal.actual),
                ("schema columns", columns),
                "{data_type}"
            );
        }
        let refusal = refusal(within(columns, depth - 1));
        assert_eq!(
            (refusal.field, refusal.actual),
            ("nesting depth", depth),
            "{data_type}"
        );
    }
}

#[test]
fn a_big_endian_schema_is_refused() {
    let message = Sharing {
        endianness: Endianness::Big,
        ..Sharing::names(1, 1)
    }
    .message();
    let decoded = Decoder::new(Limits::default()).schema(&message);
    assert_eq!(problem(decoded), Problem::BigEndian);
}

#[test]
fn a_schema_message_of_another_metadata_version_or_kind_is_refused() {
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
    let old = Encoder {
        options: IpcWriteOptions::try_new(8, false, MetadataVersion::V4).unwrap(),
        ..Encoder::default()
    }
    .schema(&schema);
    let decoded = Decoder::new(Limits::default()).schema(&old);
    assert_eq!(
        problem(decoded),
        Problem::Version {
            found: MetadataVersion::V4.0
        }
    );
    let batch = arrow_array::RecordBatch::new_empty(Arc::new(schema));
    let frame = Encoder::default().batch(&batch).unwrap().remove(0);
    let decoded = Decoder::new(Limits::default()).schema(&frame.header);
    assert_eq!(
        problem(decoded),
        Problem::Unexpected {
            found: "RecordBatch"
        }
    );
}
