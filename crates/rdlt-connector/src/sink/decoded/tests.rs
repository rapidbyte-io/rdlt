use std::sync::Arc;

use arrow_array::builder::{ListBuilder, StringDictionaryBuilder};
use arrow_array::types::{Int16Type, Int32Type};
use arrow_array::{
    Array, ArrayRef, DictionaryArray, Int16Array, Int32Array, Int64Array, RecordBatch, RunArray,
    StringArray, StructArray,
};
use arrow_schema::{DataType, Field};

use super::{decoded_bytes, decoded_rows};

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

/// One large value, then a thousand of one byte each.
fn skewed_values() -> StringArray {
    StringArray::from_iter_values(
        std::iter::once(value()).chain((0..1_000).map(|_| "y".to_owned())),
    )
}

#[test]
fn skewed_encodings_take_each_row_s_own_value() {
    // A long run over the large value, beside a thousand one-row runs over one byte each: an
    // average of the values would count a byte or two a row.
    let long = i32::try_from(ROWS).unwrap();
    let ends = Int32Array::from_iter_values((0..=1_000).map(|run| long + run));
    let runs = batch(Arc::new(
        RunArray::<Int32Type>::try_new(&ends, &skewed_values()).unwrap(),
    ));
    decodes_to_every_row(decoded_bytes(&runs), ROWS);
    // The short runs alone are a byte or so a row, the long run's rows alone its copies.
    assert!(decoded_bytes(&runs.slice(ROWS, 1_000)) < 64_000);
    decodes_to_every_row(decoded_bytes(&runs.slice(ROWS / 2, ROWS / 4)), ROWS / 4);
    // Every key naming the large value among the small ones.
    let keyed = DictionaryArray::<Int32Type>::try_new(
        Int32Array::from(vec![0; ROWS]),
        Arc::new(skewed_values()),
    )
    .unwrap();
    decodes_to_every_row(decoded_bytes(&batch(Arc::new(keyed))), ROWS);
}

#[test]
fn a_dictionary_of_many_values_takes_one_copy_per_row() {
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

#[test]
fn a_dictionary_of_no_values_decodes_its_null_rows_to_nothing() {
    let nulls = DictionaryArray::<Int32Type>::try_new(
        Int32Array::from(vec![None; 10]),
        Arc::new(StringArray::from(Vec::<String>::new())),
    )
    .unwrap();
    let keys = u64::try_from(nulls.keys().to_data().get_slice_memory_size().unwrap()).unwrap();
    assert_eq!(decoded_bytes(&batch(Arc::new(nulls))), keys);
}

/// A map of each of `keys` to one short value.
fn map_of(keys: [&str; 2]) -> ArrayRef {
    use arrow_array::builder::{MapBuilder, StringBuilder};
    let mut map = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    for key in keys {
        map.keys().append_value(key);
        map.values().append_value("v");
        map.append(true).unwrap();
    }
    Arc::new(map.finish())
}

/// Two values of every kind a column may hold: the first of about a thousand bytes, the second
/// of a few.
fn large_then_small() -> Vec<(&'static str, ArrayRef)> {
    use arrow_array::types::Int64Type;
    use arrow_array::{
        BinaryArray, BinaryViewArray, FixedSizeListArray, LargeBinaryArray, LargeListArray,
        LargeStringArray, ListArray, StringViewArray,
    };
    let (large, small) = (value(), "y".to_owned());
    let texts = [large.as_str(), small.as_str()];
    let bytes = [large.as_bytes(), small.as_bytes()];
    let items = [Some(vec![Some(7_i64); 125]), Some(vec![Some(7)])];
    let pairs = StringArray::from(vec![large.as_str(), "y", "y", "y"]);
    let item = Arc::new(Field::new("item", DataType::Utf8, false));
    vec![
        (
            "utf8",
            Arc::new(StringArray::from(texts.to_vec())) as ArrayRef,
        ),
        (
            "large utf8",
            Arc::new(LargeStringArray::from(texts.to_vec())),
        ),
        ("utf8 view", Arc::new(StringViewArray::from(texts.to_vec()))),
        ("binary", Arc::new(BinaryArray::from(bytes.to_vec()))),
        (
            "large binary",
            Arc::new(LargeBinaryArray::from(bytes.to_vec())),
        ),
        (
            "binary view",
            Arc::new(BinaryViewArray::from(bytes.to_vec())),
        ),
        (
            "list",
            Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(
                items.clone(),
            )),
        ),
        (
            "large list",
            Arc::new(LargeListArray::from_iter_primitive::<Int64Type, _, _>(
                items,
            )),
        ),
        (
            "fixed-size list",
            Arc::new(FixedSizeListArray::try_new(item, 2, Arc::new(pairs), None).unwrap()),
        ),
        ("map", map_of(texts)),
        (
            "struct",
            Arc::new(StructArray::from(vec![(
                Arc::new(Field::new("text", DataType::Utf8, false)),
                Arc::new(StringArray::from(texts.to_vec())) as ArrayRef,
            )])),
        ),
    ]
}

#[test]
fn every_kind_of_value_counts_each_row_s_own_value_behind_a_key_or_a_run() {
    let rows = 1_000;
    for (kind, values) in large_then_small() {
        let keys = Int32Array::from(vec![0; rows]);
        let keyed = DictionaryArray::<Int32Type>::try_new(keys, Arc::clone(&values)).unwrap();
        decodes_to_every_row(decoded_bytes(&batch(Arc::new(keyed))), rows);
        let ends = Int32Array::from(vec![i32::try_from(rows).unwrap(), 1_001]);
        let runs = RunArray::<Int32Type>::try_new(&ends, values.as_ref()).unwrap();
        let runs = batch(Arc::new(runs));
        decodes_to_every_row(decoded_bytes(&runs), rows);
        // The small value's row alone takes far less than the large one's.
        assert!(decoded_bytes(&runs.slice(rows, 1)) < 200, "{kind}");
    }
}

#[test]
fn each_row_counts_its_own_value_and_a_view_its_value_once_too_long_to_inline() {
    use arrow_array::StringViewArray;
    // A view inlines up to twelve bytes.
    let views = StringViewArray::from(vec!["x".repeat(12), "x".repeat(13)]);
    assert_eq!(decoded_rows(&batch(Arc::new(views))), [16, 16 + 13]);
    let keyed = DictionaryArray::<Int32Type>::try_new(
        Int32Array::from(vec![0, 1]),
        Arc::new(StringArray::from(vec![value(), "y".to_owned()])),
    )
    .unwrap();
    let rows = decoded_rows(&batch(Arc::new(keyed)));
    assert!(rows[0] >= 1_000, "{rows:?}");
    assert!(rows[1] < 100, "{rows:?}");
}
