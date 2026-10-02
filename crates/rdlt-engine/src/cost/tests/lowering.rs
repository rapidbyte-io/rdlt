//! The invariant tying the cost model to lowering: converting a column to the type its table
//! holds it in, and rendering it as the destination stores it, allocates no more at its peak
//! than the column was charged for that table.

use std::sync::Arc;

use arrow_array::types::Int32Type;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, DictionaryArray, FixedSizeBinaryArray, Int8Array,
    Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, ListArray, NullArray, RecordBatch,
    RunArray, StringArray, StringViewArray, StructArray,
};
use arrow_buffer::OffsetBuffer;
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
    for unit in units {
        types.push(LogicalType::Time(unit));
        types.push(LogicalType::Duration(unit));
        types.push(LogicalType::Timestamp(unit, None));
        types.push(LogicalType::Timestamp(unit, Some("UTC".into())));
        types.push(LogicalType::Timestamp(unit, Some("+02:00".into())));
    }
    types
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

#[test]
fn lowering_a_column_into_any_type_its_table_may_hold_it_in_allocates_no_more_than_its_charge() {
    let (mut lowerings, mut beyond) = (0, Vec::new());
    for column in numbers().into_iter().chain(others()) {
        let field = ArrowField::new("c", column.data_type().clone(), true);
        let from = Field::from_arrow(&field).expect("a logical type");
        let from = from.logical_type();
        // A column of nulls is built as its table stores it, charged as the rows the batch
        // holds nothing in are.
        if *from == LogicalType::Null {
            continue;
        }
        // Every type the column's table may hold it in, each once.
        let mut held: Vec<LogicalType> = Vec::new();
        for to in joined().iter().map(|other| from.join(other)) {
            if !held.contains(&to) {
                held.push(to);
            }
        }
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
    assert!(lowerings > 1_000, "{lowerings} lowerings were measured");
    beyond.sort();
    beyond.dedup();
    assert!(
        beyond.is_empty(),
        "{} of {lowerings} lowerings allocated beyond their charge:\n{}",
        beyond.len(),
        beyond.join("\n")
    );
}
