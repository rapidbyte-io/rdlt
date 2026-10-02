//! The invariant tying the cost model to lowering: converting a column to the type its table
//! holds it in, and rendering it as the destination stores it, allocates no more at its peak
//! than the column was charged for that table.

use std::sync::Arc;

use arrow_array::types::Int32Type;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, DictionaryArray,
    FixedSizeBinaryArray, FixedSizeListArray, Int8Array, Int32Array, Int64Array, LargeBinaryArray,
    LargeStringArray, ListArray, ListViewArray, MapArray, NullArray, RecordBatch, RunArray,
    StringArray, StringViewArray, StructArray,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field as ArrowField, Fields, TimeUnit as U};
use rdlt_connector::cost::{Rendering, Stored};
use rdlt_connector::{DecimalType, Field, LogicalType, TimeUnit};

use super::HEAP;
use crate::table::convert::{convert, text};

/// Rows a column of this test holds: enough that what an array takes beside its values is
/// little of it.
const ROWS: usize = 2_048;

/// Bytes: what a conversion allocates beside its arrays' values, whatever their rows.
const SLACK: u64 = 8 << 10;

fn int64s(value: i64) -> ArrayRef {
    Arc::new(Int64Array::from(vec![value; ROWS]))
}

/// `value` cast to `data_type`, through its storage where Arrow casts no integer to it.
fn cast(value: i64, data_type: &DataType) -> Option<ArrayRef> {
    let stored = match data_type {
        DataType::Date32 | DataType::Time32(_) => {
            arrow_cast::cast(&int64s(value), &DataType::Int32).ok()?
        }
        _ => int64s(value),
    };
    let options = arrow_cast::CastOptions {
        safe: false,
        ..Default::default()
    };
    arrow_cast::cast_with_options(&stored, data_type, &options).ok()
}

/// A column of every type whose values Arrow casts an integer to: the least value its type
/// holds of those given, then a small one.
fn numbers() -> Vec<ArrayRef> {
    let zoned = |unit, zone: &str| DataType::Timestamp(unit, Some(zone.into()));
    let types = [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Float32,
        DataType::Float64,
        DataType::Decimal32(9, 2),
        DataType::Decimal64(18, 0),
        DataType::Decimal128(38, 10),
        DataType::Decimal128(5, 0),
        DataType::Decimal256(76, 0),
        DataType::Decimal256(60, 20),
        DataType::Date32,
        DataType::Date64,
        DataType::Time32(U::Second),
        DataType::Time32(U::Millisecond),
        DataType::Time64(U::Microsecond),
        DataType::Time64(U::Nanosecond),
        DataType::Timestamp(U::Second, None),
        DataType::Timestamp(U::Millisecond, None),
        DataType::Timestamp(U::Microsecond, None),
        DataType::Timestamp(U::Nanosecond, None),
        zoned(U::Second, "UTC"),
        zoned(U::Millisecond, "+02:00"),
        zoned(U::Microsecond, "Europe/Warsaw"),
        zoned(U::Nanosecond, "UTC"),
        DataType::Duration(U::Second),
        DataType::Duration(U::Millisecond),
        DataType::Duration(U::Microsecond),
        DataType::Duration(U::Nanosecond),
    ];
    let values = [i64::MIN, i64::from(i32::MIN), -32_768, -128, 86_399, 1];
    let mut columns = Vec::new();
    for data_type in &types {
        let casts = values.iter().filter_map(|value| cast(*value, data_type));
        let casts: Vec<ArrayRef> = casts.collect();
        columns.extend(casts.first().cloned());
        columns.extend(casts.last().cloned());
    }
    columns
}

fn list(items: ArrayRef) -> ArrayRef {
    let field = Arc::new(ArrowField::new("item", items.data_type().clone(), true));
    let lengths = vec![items.len() / ROWS; ROWS];
    Arc::new(ListArray::new(
        field,
        OffsetBuffer::from_lengths(lengths),
        items,
        None,
    ))
}

/// A column of every other type: strings, bytes, flags, nulls and nested values.
fn others() -> Vec<ArrayRef> {
    let control = "\u{1}\"\\".repeat(30);
    let words: ArrayRef = Arc::new(StringArray::from(vec![control.as_str(); ROWS]));
    let bytes = vec![&[0xab_u8; 40][..]; ROWS];
    let small: ArrayRef = Arc::new(Int8Array::from(vec![-128_i8; ROWS]));
    let fields = Fields::from(vec![
        ArrowField::new("a", DataType::Int8, true),
        ArrowField::new("s", DataType::Utf8, true),
    ]);
    let structs = StructArray::new(fields, vec![Arc::clone(&small), Arc::clone(&words)], None);
    // A field and items typed null, which a table holding them widely converts to nulls of its
    // own types.
    let nulls: ArrayRef = Arc::new(NullArray::new(ROWS));
    let with_null = StructArray::new(
        Fields::from(vec![
            ArrowField::new("a", DataType::Null, true),
            ArrowField::new("s", DataType::Utf8, true),
        ]),
        vec![Arc::clone(&nulls), Arc::clone(&words)],
        None,
    );
    let twice = |array: &ArrayRef| arrow_select::concat::concat(&[array, array]).expect("joined");
    vec![
        Arc::new(BooleanArray::from(vec![false; ROWS])),
        Arc::new(NullArray::new(ROWS)),
        Arc::clone(&words),
        Arc::new(StringArray::from(vec!["a"; ROWS])),
        Arc::new(LargeStringArray::from(vec![control.as_str(); ROWS])),
        Arc::new(StringViewArray::from(vec![control.as_str(); ROWS])),
        Arc::new(BinaryArray::from(bytes.clone())),
        Arc::new(LargeBinaryArray::from(bytes)),
        Arc::new(FixedSizeBinaryArray::try_from_iter(vec![[7_u8; 16]; ROWS].into_iter()).unwrap()),
        Arc::new(structs.clone()),
        list(twice(&small)),
        list(twice(&words)),
        list(twice(&(Arc::new(structs) as ArrayRef))),
        Arc::new(with_null),
        list(twice(&nulls)),
        Arc::new(BinaryViewArray::from(vec![&[0xab_u8; 40][..]; ROWS])),
        views(&small),
        views(&words),
        arrow_cast::cast(&list(twice(&small)), &DataType::LargeList(item(&small))).expect("lists"),
        Arc::new(FixedSizeListArray::new(
            item(&small),
            2,
            twice(&small),
            None,
        )),
        map(&words, &small),
    ]
}

fn item(items: &ArrayRef) -> Arc<ArrowField> {
    Arc::new(ArrowField::new("item", items.data_type().clone(), true))
}

/// List views of `items`, every row naming the same three.
fn views(items: &ArrayRef) -> ArrayRef {
    Arc::new(ListViewArray::new(
        item(items),
        ScalarBuffer::from(vec![5_i32; ROWS]),
        ScalarBuffer::from(vec![3_i32; ROWS]),
        Arc::clone(items),
        None,
    ))
}

/// A map of one entry a row, its key from `keys` and its value from `values`.
fn map(keys: &ArrayRef, values: &ArrayRef) -> ArrayRef {
    let fields = Fields::from(vec![
        ArrowField::new("key", keys.data_type().clone(), false),
        ArrowField::new("value", values.data_type().clone(), true),
    ]);
    let entries = StructArray::new(fields, vec![Arc::clone(keys), Arc::clone(values)], None);
    let field = Arc::new(ArrowField::new(
        "entries",
        entries.data_type().clone(),
        false,
    ));
    let offsets = OffsetBuffer::from_lengths(vec![1; ROWS]);
    Arc::new(MapArray::new(field, offsets, entries, None, false))
}

/// A column of each type only its field's extension names: UUIDs and JSON text.
fn extensions() -> Vec<(ArrowField, ArrayRef)> {
    let named = |name: &str, column: ArrayRef| {
        let extension = [("ARROW:extension:name".to_owned(), name.to_owned())];
        let field = ArrowField::new("c", column.data_type().clone(), true);
        (field.with_metadata(extension.into()), column)
    };
    let uuids = FixedSizeBinaryArray::try_from_iter(vec![[0xff_u8; 16]; ROWS].into_iter());
    let json = StringArray::from(vec![r#"{"a":[1,2,3],"b":"\u0001"}"#; ROWS]);
    vec![
        named("arrow.uuid", Arc::new(uuids.unwrap())),
        named("arrow.json", Arc::new(json)),
    ]
}

/// `column` as it is, behind keys and as runs.
fn encodings(column: &ArrayRef) -> Vec<ArrayRef> {
    let keys = Int32Array::from_iter_values((0..ROWS).rev().map(|row| i32::try_from(row).unwrap()));
    let ends = Int32Array::from_iter_values((1..=ROWS).map(|row| i32::try_from(row).unwrap()));
    let mut encoded = vec![Arc::clone(column)];
    if let Ok(keyed) = DictionaryArray::<Int32Type>::try_new(keys, Arc::clone(column)) {
        encoded.push(Arc::new(keyed));
    }
    if let Ok(runs) = RunArray::<Int32Type>::try_new(&ends, column.as_ref()) {
        encoded.push(Arc::new(runs));
    }
    encoded
}

/// A type of every kind, unit and width a table's column may be joined with.
fn joined() -> Vec<LogicalType> {
    let decimal =
        |precision, scale| LogicalType::Decimal(DecimalType::new(precision, scale).unwrap());
    let units = [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ];
    let field = |name: &str, logical| Field::new(name, logical, true);
    let fields = |fields| LogicalType::Struct(rdlt_connector::Fields::new(fields).unwrap());
    let mut types = vec![
        LogicalType::Null,
        LogicalType::Bool,
        LogicalType::Int8,
        LogicalType::Int16,
        LogicalType::Int32,
        LogicalType::Int64,
        LogicalType::Float32,
        LogicalType::Float64,
        decimal(76, 0),
        decimal(38, 10),
        decimal(9, 2),
        LogicalType::Utf8,
        LogicalType::Binary,
        LogicalType::Date,
        LogicalType::Uuid,
        LogicalType::Json,
        fields(vec![
            field("a", decimal(76, 0)),
            field("z", LogicalType::Int64),
        ]),
        fields(vec![
            field("a", LogicalType::Utf8),
            field("s", LogicalType::Int64),
        ]),
        LogicalType::List(Box::new(field("item", decimal(76, 0)))),
        LogicalType::List(Box::new(field("item", LogicalType::Json))),
    ];
    // Nested values wider than a scalar: a struct of decimals, in a struct and in a list.
    let wide = fields(
        (0..8)
            .map(|index| field(&format!("d{index}"), decimal(76, 0)))
            .collect(),
    );
    types.push(fields(vec![field("a", wide.clone())]));
    types.push(LogicalType::List(Box::new(field("item", wide))));
    for unit in units {
        types.push(LogicalType::Time(unit));
        types.push(LogicalType::Duration(unit));
        types.push(LogicalType::Timestamp(unit, None));
        types.push(LogicalType::Timestamp(unit, Some("UTC".into())));
        types.push(LogicalType::Timestamp(unit, Some("+02:00".into())));
    }
    types
}

/// Every type a table may hold a column of `from` in, each once.
fn held_in(from: &LogicalType) -> Vec<LogicalType> {
    let mut held: Vec<LogicalType> = Vec::new();
    for to in joined().iter().map(|other| from.join(other)) {
        if !held.contains(&to) {
            held.push(to);
        }
    }
    held
}

/// What `run` allocates at its peak, beyond what was allocated when it began.
fn peak<T>(run: impl FnOnce() -> T) -> (T, u64) {
    HEAP.reset_peak_usage();
    let before = HEAP.current_usage();
    let out = run();
    let peak = HEAP.peak_usage().saturating_sub(before);
    (out, u64::try_from(peak).unwrap())
}

/// Lowers `column`, of `from`, into a table column of `to`, as text where `as_text`: what it
/// was charged for that table and what lowering it allocated at its peak, where it could be
/// lowered so.
fn lowered(
    column: &ArrayRef,
    from: &LogicalType,
    to: &LogicalType,
    as_text: bool,
) -> Option<(u64, u64)> {
    let batch = RecordBatch::try_from_iter([("c", Arc::clone(column))]).unwrap();
    let stored = Stored {
        column: to.clone(),
        text: as_text,
    };
    let mut measure = Rendering::native().lowering(&batch, vec![Some(stored)], 0, u64::MAX);
    let charge = measure.expanded(0..column.len());
    let (made, peak) = peak(|| {
        let converted = convert(column, from, to)?;
        if as_text {
            text(&converted, to)
        } else {
            Ok(converted)
        }
    });
    made.is_ok().then_some((charge, peak))
}

/// Runs `$check` over each tenth of the columns, in a test of its own, under a module named as
/// the check proves: together they cover every column, and each runs in about a second.
macro_rules! in_tenths {
    ($module:ident: $check:ident) => {
        mod $module {
            #[test]
            fn the_first_tenth_of_the_columns() {
                super::$check(0);
            }

            #[test]
            fn the_second_tenth_of_the_columns() {
                super::$check(1);
            }

            #[test]
            fn the_third_tenth_of_the_columns() {
                super::$check(2);
            }

            #[test]
            fn the_fourth_tenth_of_the_columns() {
                super::$check(3);
            }

            #[test]
            fn the_fifth_tenth_of_the_columns() {
                super::$check(4);
            }

            #[test]
            fn the_sixth_tenth_of_the_columns() {
                super::$check(5);
            }

            #[test]
            fn the_seventh_tenth_of_the_columns() {
                super::$check(6);
            }

            #[test]
            fn the_eighth_tenth_of_the_columns() {
                super::$check(7);
            }

            #[test]
            fn the_ninth_tenth_of_the_columns() {
                super::$check(8);
            }

            #[test]
            fn the_last_tenth_of_the_columns() {
                super::$check(9);
            }
        }
    };
}

/// Every column this file lowers, before its encodings, with the field that names its type.
fn plain_columns() -> Vec<(ArrowField, ArrayRef)> {
    let plain = |column: ArrayRef| {
        (
            ArrowField::new("c", column.data_type().clone(), true),
            column,
        )
    };
    let columns = numbers().into_iter().chain(others()).map(plain);
    columns.chain(extensions()).collect()
}

/// The columns of `tenth`, of the ten every column falls in by its place.
fn in_tenth<T>(columns: Vec<T>, tenth: usize) -> impl Iterator<Item = T> {
    let place = move |(index, column): (usize, T)| (index % 10 == tenth).then_some(column);
    columns.into_iter().enumerate().filter_map(place)
}

#[test]
fn the_columns_lowered_are_of_every_kind_of_logical_type() {
    let kinds: std::collections::BTreeSet<_> = plain_columns()
        .iter()
        .map(|(field, _)| {
            Field::from_arrow(field)
                .expect("a logical type")
                .logical_type()
                .kind()
        })
        .collect();
    let every: std::collections::BTreeSet<_> = rdlt_testkit::drawn::KINDS.into_iter().collect();
    // Nulls among them, which are built as their table stores them rather than converted.
    assert_eq!(kinds, every);
    // And some of them a normalizing plan splits into tables of their own.
    let shape = crate::normalize::Shape {
        max_depth: 8,
        whole: std::collections::BTreeSet::new(),
        key: Vec::new(),
    };
    let splits = every_column().into_iter().any(|(field, column)| {
        let schema = Arc::new(arrow_schema::Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema, vec![column]).unwrap();
        crate::normalize::normalize(&batch, &shape).unwrap().len() > 1
    });
    assert!(splits, "no column splits into tables of its own");
}

in_tenths!(lowering_a_column_into_any_type_its_table_may_hold_it_in_allocates_no_more_than_its_charge: lowers_within_its_charge);

/// Lowers each column of `tenth` in every encoding into every type its table may hold it in,
/// as it is and as text, failing where one allocates more than it was charged.
fn lowers_within_its_charge(tenth: usize) {
    let (mut lowerings, mut beyond) = (0, Vec::new());
    for (field, column) in in_tenth(plain_columns(), tenth) {
        let from = Field::from_arrow(&field).expect("a logical type");
        let from = from.logical_type();
        // A column of nulls is built as its table stores it, charged as the rows the batch
        // holds nothing in are.
        if *from == LogicalType::Null {
            continue;
        }
        let held = held_in(from);
        for encoded in encodings(&column) {
            for to in &held {
                for as_text in [false, true] {
                    let Some((charge, peak)) = lowered(&encoded, from, to, as_text) else {
                        continue;
                    };
                    lowerings += 1;
                    if peak > charge + SLACK {
                        let kind = encoded.data_type();
                        beyond.push(format!(
                            "{kind} into {to:?}, as text {as_text}: charged {charge}, allocated {peak}"
                        ));
                    }
                }
            }
        }
    }
    assert!(lowerings > 100, "{lowerings} lowerings were measured");
    beyond.sort();
    beyond.dedup();
    assert!(
        beyond.is_empty(),
        "{} of {lowerings} lowerings allocated beyond their charge:\n{}",
        beyond.len(),
        beyond.join("\n")
    );
}

/// Every column this file lowers, with the field that names its type, in every encoding.
fn every_column() -> Vec<(ArrowField, ArrayRef)> {
    let mut every = Vec::new();
    for (field, column) in plain_columns() {
        for encoded in encodings(&column) {
            let field = field.clone().with_data_type(encoded.data_type().clone());
            every.push((field, encoded));
        }
    }
    every
}

/// What a piece of `batch` reserves before it is split: its rows as they arrive, with the
/// lineage of each row and each item, for every copy the split may hold.
fn split_estimate(batch: &RecordBatch) -> u64 {
    use crate::cost::{LINEAGE_ITEM, LINEAGE_ROW, SPLIT_COPIES};
    let measure = Rendering::native().lowering(batch, Vec::new(), LINEAGE_ROW, u64::MAX);
    let mut measure = measure.with_items(LINEAGE_ITEM);
    measure
        .expanded(0..batch.num_rows())
        .saturating_mul(SPLIT_COPIES)
}

in_tenths!(a_column_through_a_normalizing_plan_is_split_and_lowered_within_what_is_reserved_for_it: splits_and_lowers_within_what_is_reserved);

/// Splits each column of `tenth` in every encoding through a normalizing plan, then lowers its
/// parts as a plain batch's, failing where either allocates more than was reserved.
fn splits_and_lowers_within_what_is_reserved(tenth: usize) {
    use crate::normalize::{Shape, normalize};
    let shape = Shape {
        max_depth: 8,
        whole: std::collections::BTreeSet::new(),
        key: Vec::new(),
    };
    let (mut splits, mut lowerings, mut tables, mut beyond) = (0, 0, 0, Vec::new());
    for (field, column) in in_tenth(every_column(), tenth) {
        let kind = column.data_type().clone();
        let schema = Arc::new(arrow_schema::Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema, vec![column]).unwrap();
        let estimate = split_estimate(&batch);
        let (parts, peak) = peak(|| normalize(&batch, &shape));
        let parts = parts.expect("every column normalizes");
        splits += 1;
        tables += parts.len();
        if peak > estimate + SLACK {
            beyond.push(format!(
                "splitting {kind}: reserved {estimate}, allocated {peak}"
            ));
        }
        // Each part is then cut and reserved by its own table's types, as a plain batch is.
        for part in parts {
            let fields = part.batch.schema();
            for (field, column) in fields.fields().iter().zip(part.batch.columns()) {
                let from = Field::from_arrow(field).expect("a logical type");
                let from = from.logical_type();
                if *from == LogicalType::Null {
                    continue;
                }
                let held = held_in(from);
                for (to, as_text) in held.iter().flat_map(|to| [(to, false), (to, true)]) {
                    let Some((charge, peak)) = lowered(column, from, to, as_text) else {
                        continue;
                    };
                    lowerings += 1;
                    if peak > charge + SLACK {
                        let part = column.data_type();
                        beyond.push(format!(
                            "{kind} split to {part} into {to:?}, as text {as_text}: charged \
                             {charge}, allocated {peak}"
                        ));
                    }
                }
            }
        }
    }
    assert!(splits > 10, "{splits} columns were split");
    assert!(tables >= splits, "{tables} tables of {splits} columns");
    assert!(lowerings > 100, "{lowerings} lowerings were measured");
    beyond.sort();
    beyond.dedup();
    assert!(
        beyond.is_empty(),
        "{} of {splits} splits and {lowerings} lowerings allocated beyond what was reserved:\n{}",
        beyond.len(),
        beyond.join("\n")
    );
}
