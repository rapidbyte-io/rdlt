use std::sync::Arc;

use arrow_array::builder::{ListBuilder, StringDictionaryBuilder};
use arrow_array::types::{Int16Type, Int32Type};
use arrow_array::{
    Array, ArrayRef, DictionaryArray, Int16Array, Int32Array, Int64Array, RecordBatch, RunArray,
    StringArray, StructArray,
};
use arrow_schema::{DataType, Field};

use super::decoded_bytes;

const ROWS: usize = 10_000;

fn value() -> String {
    "x".repeat(1_000)
}

fn batch(column: ArrayRef) -> RecordBatch {
    RecordBatch::try_from_iter([("column", column)]).unwrap()
}

/// At least every row's copy of the value, and not twice that.
fn decodes_to_every_row(bytes: u64, rows: usize) {
    let each = u64::try_from(value().len()).unwrap();
    let rows = u64::try_from(rows).unwrap();
    assert!(bytes >= rows * each, "{bytes}");
    assert!(bytes < 2 * rows * each, "{bytes}");
}

#[test]
fn a_plain_batch_takes_the_memory_its_rows_hold() {
    let ids = batch(Arc::new(Int64Array::from(vec![7; ROWS])));
    assert_eq!(decoded_bytes(&ids), 8 * u64::try_from(ROWS).unwrap());
    assert_eq!(decoded_bytes(&ids.slice(0, 10)), 80);
}

#[test]
fn run_end_encoded_columns_take_a_copy_of_their_value_per_row() {
    let ends = i32::try_from(ROWS).unwrap();
    let runs = RunArray::<Int32Type>::try_new(
        &Int32Array::from(vec![ends]),
        &StringArray::from(vec![value()]),
    )
    .unwrap();
    let whole = batch(Arc::new(runs));
    decodes_to_every_row(decoded_bytes(&whole), ROWS);
    decodes_to_every_row(decoded_bytes(&whole.slice(0, ROWS / 4)), ROWS / 4);
    // Every width of run end.
    let short = RunArray::<Int16Type>::try_new(
        &Int16Array::from(vec![1_000]),
        &StringArray::from(vec![value()]),
    )
    .unwrap();
    decodes_to_every_row(decoded_bytes(&batch(Arc::new(short))), 1_000);
}

#[test]
fn dictionary_columns_take_a_copy_of_their_value_per_row_at_any_depth() {
    let words = || {
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![0; ROWS]),
            Arc::new(StringArray::from(vec![value()])),
        )
        .unwrap()
    };
    decodes_to_every_row(decoded_bytes(&batch(Arc::new(words()))), ROWS);
    let nested = StructArray::from(vec![(
        Arc::new(Field::new("word", words().data_type().clone(), false)),
        Arc::new(words()) as ArrayRef,
    )]);
    decodes_to_every_row(decoded_bytes(&batch(Arc::new(nested))), ROWS);
    let mut listed = ListBuilder::new(StringDictionaryBuilder::<Int32Type>::new());
    for _ in 0..ROWS {
        listed.values().append_value(value());
    }
    listed.append(true);
    let listed = listed.finish();
    assert!(matches!(listed.data_type(), DataType::List(_)));
    decodes_to_every_row(decoded_bytes(&batch(Arc::new(listed))), ROWS);
}

#[test]
fn a_dictionary_of_many_values_takes_their_average_per_row() {
    // Two values of a thousand bytes, keyed alternately: a copy of one value a row, not two.
    let words = DictionaryArray::<Int32Type>::try_new(
        Int32Array::from_iter_values((0..ROWS).map(|row| i32::from(row % 2 == 1))),
        Arc::new(StringArray::from(vec![
            "a".repeat(1_000),
            "b".repeat(1_000),
        ])),
    )
    .unwrap();
    decodes_to_every_row(decoded_bytes(&batch(Arc::new(words))), ROWS);
}

#[test]
fn every_nested_array_counts_its_offsets_and_decodes_its_items() {
    use arrow_array::builder::{FixedSizeListBuilder, LargeListBuilder, MapBuilder, StringBuilder};
    // Empty lists hold nothing but their offsets.
    let mut empty = ListBuilder::new(Int32Array::builder(0));
    for _ in 0..1_000 {
        empty.append(true);
    }
    assert!(decoded_bytes(&batch(Arc::new(empty.finish()))) >= 8_000);
    let mut large = LargeListBuilder::new(StringDictionaryBuilder::<Int32Type>::new());
    for _ in 0..ROWS {
        large.values().append_value(value());
    }
    large.append(true);
    decodes_to_every_row(decoded_bytes(&batch(Arc::new(large.finish()))), ROWS);
    let mut fixed = FixedSizeListBuilder::new(StringDictionaryBuilder::<Int32Type>::new(), 2);
    for _ in 0..ROWS / 2 {
        fixed.values().append_value(value());
        fixed.values().append_value(value());
        fixed.append(true);
    }
    decodes_to_every_row(decoded_bytes(&batch(Arc::new(fixed.finish()))), ROWS);
    let mut map = MapBuilder::new(
        None,
        StringBuilder::new(),
        StringDictionaryBuilder::<Int32Type>::new(),
    );
    for row in 0..ROWS {
        map.keys().append_value(row.to_string());
        map.values().append_value(value());
        map.append(true).unwrap();
    }
    assert!(decoded_bytes(&batch(Arc::new(map.finish()))) >= u64::try_from(ROWS * 1_000).unwrap());
}
