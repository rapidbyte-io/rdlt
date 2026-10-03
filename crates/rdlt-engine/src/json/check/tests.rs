use std::sync::Arc;

use arrow_array::types::{
    Int8Type, Int16Type, Int32Type, Int64Type, RunEndIndexType, UInt8Type, UInt16Type, UInt32Type,
    UInt64Type,
};
use arrow_array::{
    Array, ArrayRef, DictionaryArray, FixedSizeListArray, LargeListArray, LargeListViewArray,
    LargeStringArray, ListArray, ListViewArray, MapArray, PrimitiveArray, RecordBatch, RunArray,
    StringArray, StringViewArray, StructArray,
};
use arrow_buffer::{ArrowNativeType, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Fields, Schema};
use rdlt_connector::limits::MAX_NESTING_DEPTH;

use super::{NotJson, check_batch};
use crate::json::JsonError;

/// `field` marked as a column of JSON.
fn json(field: Field) -> Field {
    field.with_metadata([("ARROW:extension:name".to_owned(), "arrow.json".to_owned())].into())
}

/// What checking a batch whose only column is `array`, of `field`, finds.
fn checked(field: Field, array: ArrayRef) -> Result<(), JsonError> {
    let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array]).unwrap();
    check_batch(&batch).map_err(|NotJson { column, error }| {
        assert_eq!(column, "c", "the refusal names the batch's column");
        error
    })
}

fn texts(values: &[Option<&str>]) -> ArrayRef {
    Arc::new(StringArray::from(values.to_vec()))
}

/// Whether a check refused a value as not JSON.
fn invalid(checked: &Result<(), JsonError>) -> bool {
    matches!(checked, Err(JsonError::Invalid(_)))
}

#[test]
fn a_column_of_json_in_every_text_type_is_checked_a_value_its_rows_name_at_a_time() {
    let arrays: [(DataType, ArrayRef, ArrayRef); 3] = [
        (
            DataType::Utf8,
            texts(&[Some("{}"), None]),
            texts(&[Some("[1]"), Some("x")]),
        ),
        (
            DataType::LargeUtf8,
            Arc::new(LargeStringArray::from(vec![Some("1"), None])),
            Arc::new(LargeStringArray::from(vec![Some("x")])),
        ),
        (
            DataType::Utf8View,
            Arc::new(StringViewArray::from(vec![
                Some("\"a long enough string\""),
                None,
            ])),
            Arc::new(StringViewArray::from(vec![Some(
                "not JSON, and long enough",
            )])),
        ),
    ];
    for (data_type, good, bad) in arrays {
        let field = json(Field::new("c", data_type.clone(), true));
        assert_eq!(checked(field.clone(), good), Ok(()), "{data_type}");
        assert!(invalid(&checked(field, bad.clone())), "{data_type}");
        // The same text in a column that is not JSON is no concern of the check.
        assert_eq!(checked(Field::new("c", data_type, true), bad), Ok(()));
    }
    // A row the batch slices off is not named.
    let sliced = texts(&[Some("x"), Some("1")]).slice(1, 1);
    assert_eq!(
        checked(json(Field::new("c", DataType::Utf8, true)), sliced),
        Ok(())
    );
}

fn keyed<K: arrow_array::types::ArrowDictionaryKeyType>(key: &DataType) {
    let field = |values: DataType| {
        json(Field::new(
            "c",
            DataType::Dictionary(Box::new(key.clone()), Box::new(values)),
            true,
        ))
    };
    let values = texts(&[Some("x"), Some("1")]);
    let array = |keys: &[Option<u8>]| -> ArrayRef {
        let keys = PrimitiveArray::<K>::from_iter(
            keys.iter()
                .map(|key| key.map(|key| K::Native::from_usize(usize::from(key)).unwrap())),
        );
        Arc::new(DictionaryArray::<K>::try_new(keys, Arc::clone(&values)).unwrap())
    };
    // Only the values a key names are read: the dictionary's other values may hold anything.
    assert_eq!(
        checked(field(DataType::Utf8), array(&[Some(1), None])),
        Ok(()),
        "{key}"
    );
    assert!(
        invalid(&checked(field(DataType::Utf8), array(&[Some(1), Some(0)]))),
        "{key}"
    );
}

#[test]
fn a_dictionary_of_json_is_checked_where_keys_of_every_type_name_its_values() {
    keyed::<Int8Type>(&DataType::Int8);
    keyed::<Int16Type>(&DataType::Int16);
    keyed::<Int32Type>(&DataType::Int32);
    keyed::<Int64Type>(&DataType::Int64);
    keyed::<UInt8Type>(&DataType::UInt8);
    keyed::<UInt16Type>(&DataType::UInt16);
    keyed::<UInt32Type>(&DataType::UInt32);
    keyed::<UInt64Type>(&DataType::UInt64);
}

fn runs<R: RunEndIndexType>() {
    let ends = PrimitiveArray::<R>::from_iter_values(
        [2_usize, 3].map(|end| R::Native::from_usize(end).unwrap()),
    );
    let array = RunArray::<R>::try_new(&ends, &texts(&[Some("1"), Some("x")])).unwrap();
    let field = json(Field::new("c", array.data_type().clone(), true));
    let array: ArrayRef = Arc::new(array);
    // The first two rows are the first run's; the third, the second's.
    assert_eq!(
        checked(field.clone(), array.slice(0, 2)),
        Ok(()),
        "{}",
        R::DATA_TYPE
    );
    assert!(invalid(&checked(field, array)), "{}", R::DATA_TYPE);
}

#[test]
fn runs_of_json_are_checked_where_runs_of_every_end_type_hold_them() {
    runs::<Int16Type>();
    runs::<Int32Type>();
    runs::<Int64Type>();
}

/// A struct of one field, `j`, of JSON, null where `valid` says.
fn structs(values: ArrayRef, valid: &[bool]) -> (Field, ArrayRef) {
    let inner = json(Field::new("j", values.data_type().clone(), true));
    let fields = Fields::from(vec![inner]);
    let array = StructArray::new(
        fields.clone(),
        vec![values],
        Some(NullBuffer::from(valid.to_vec())),
    );
    (
        Field::new("c", DataType::Struct(fields), true),
        Arc::new(array),
    )
}

#[test]
fn json_in_a_struct_is_checked_where_the_struct_is_not_null() {
    let (field, array) = structs(texts(&[Some("x"), Some("1")]), &[false, true]);
    assert_eq!(checked(field.clone(), array), Ok(()));
    let (field, array) = structs(texts(&[Some("x"), Some("1")]), &[true, true]);
    assert!(invalid(&checked(field, array)));
}

fn item() -> Arc<Field> {
    Arc::new(json(Field::new("item", DataType::Utf8, true)))
}

/// Items `1`, `x` and `2`: the second is named only where a test says.
fn items() -> ArrayRef {
    texts(&[Some("1"), Some("x"), Some("2")])
}

fn column(data_type: &DataType) -> Field {
    Field::new("c", data_type.clone(), true)
}

#[test]
fn json_in_lists_of_every_layout_is_checked_where_their_rows_hold_it() {
    let (item, items) = (item(), items());
    // Rows hold items 0 and 2; item 1 lies between them, named by no row.
    let null_between = NullBuffer::from(vec![true, false, true]);
    let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 1, 2, 3]));
    let list = ListArray::new(
        Arc::clone(&item),
        offsets.clone(),
        Arc::clone(&items),
        Some(null_between.clone()),
    );
    let field = |data_type: DataType| column(&data_type);
    assert_eq!(
        checked(field(list.data_type().clone()), Arc::new(list.clone())),
        Ok(())
    );
    let named = ListArray::new(Arc::clone(&item), offsets, Arc::clone(&items), None);
    assert!(invalid(&checked(
        field(named.data_type().clone()),
        Arc::new(named)
    )));

    let large = LargeListArray::new(
        Arc::clone(&item),
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i64, 1, 2, 3])),
        Arc::clone(&items),
        Some(null_between.clone()),
    );
    assert_eq!(
        checked(field(large.data_type().clone()), Arc::new(large)),
        Ok(())
    );

    let fixed =
        FixedSizeListArray::new(Arc::clone(&item), 1, Arc::clone(&items), Some(null_between));
    assert_eq!(
        checked(field(fixed.data_type().clone()), Arc::new(fixed)),
        Ok(())
    );
    let fixed = FixedSizeListArray::new(Arc::clone(&item), 3, Arc::clone(&items), None);
    assert!(invalid(&checked(
        field(fixed.data_type().clone()),
        Arc::new(fixed)
    )));
}

#[test]
fn json_in_list_views_is_checked_where_a_view_names_it() {
    let (item, items) = (item(), items());
    let field = |data_type: DataType| column(&data_type);
    // List views name items in any order, again and again: item 1 only where a view says.
    let views = |sizes: Vec<i32>| {
        ListViewArray::new(
            Arc::clone(&item),
            ScalarBuffer::from(vec![2_i32, 0, 2]),
            ScalarBuffer::from(sizes),
            Arc::clone(&items),
            None,
        )
    };
    assert_eq!(
        checked(
            field(views(vec![1, 1, 1]).data_type().clone()),
            Arc::new(views(vec![1, 1, 1]))
        ),
        Ok(())
    );
    assert!(invalid(&checked(
        field(views(vec![1, 2, 1]).data_type().clone()),
        Arc::new(views(vec![1, 2, 1]))
    )));
    let large_views = LargeListViewArray::new(
        Arc::clone(&item),
        ScalarBuffer::from(vec![0_i64, 1]),
        ScalarBuffer::from(vec![1_i64, 2]),
        Arc::clone(&items),
        None,
    );
    assert!(invalid(&checked(
        field(large_views.data_type().clone()),
        Arc::new(large_views)
    )));
}

#[test]
fn json_in_a_map_is_checked_where_its_entries_hold_it() {
    let field = |data_type: DataType| column(&data_type);
    // A map's values are its entries' second field.
    let entries = Fields::from(vec![
        Field::new("key", DataType::Utf8, false),
        json(Field::new("value", DataType::Utf8, true)),
    ]);
    let map = |value: &str| -> ArrayRef {
        let entries = StructArray::new(
            entries.clone(),
            vec![texts(&[Some("k")]), texts(&[Some(value)])],
            None,
        );
        let entry = Arc::new(Field::new(
            "entries",
            DataType::Struct(entries.fields().clone()),
            false,
        ));
        Arc::new(MapArray::new(
            entry,
            OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 1])),
            entries,
            None,
            false,
        ))
    };
    assert_eq!(
        checked(field(map("1").data_type().clone()), map("[]")),
        Ok(())
    );
    assert!(invalid(&checked(
        field(map("x").data_type().clone()),
        map("x")
    )));
}

#[test]
fn json_nested_past_the_limit_is_refused() {
    let limit = usize::try_from(MAX_NESTING_DEPTH).unwrap();
    let nested = |depth: usize| format!("{}{}", "[".repeat(depth), "]".repeat(depth));
    let field = json(Field::new("c", DataType::Utf8, true));
    assert_eq!(
        checked(field.clone(), texts(&[Some(&nested(limit))])),
        Ok(())
    );
    assert_eq!(
        checked(field, texts(&[Some(&nested(limit + 1))])),
        Err(JsonError::TooDeep)
    );
}

/// What checking `batch` allocates at its peak, beside what was allocated before.
fn check_peak(batch: &RecordBatch) -> (Result<(), NotJson>, u64) {
    let heap = &crate::cost::tests::HEAP;
    heap.reset_peak_usage();
    let before = heap.current_usage();
    let checked = check_batch(batch);
    let peak = heap.peak_usage().saturating_sub(before);
    (checked, u64::try_from(peak).unwrap())
}

/// One row of a list of `items` items, `values` naming them, a column of JSON.
fn one_long_list(items: i32, values: ArrayRef) -> RecordBatch {
    let item = json(Field::new("item", values.data_type().clone(), true));
    let list = ListArray::new(
        Arc::new(item),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, items])),
        values,
        None,
    );
    let field = Field::new("c", list.data_type().clone(), true);
    RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![Arc::new(list)]).unwrap()
}

#[test]
fn checking_runs_allocates_by_what_the_batch_holds_not_by_its_items() {
    const ITEMS: i32 = 60 << 20;
    let one: ArrayRef = Arc::new(StringArray::from(vec!["1"]));
    let ends = PrimitiveArray::<Int32Type>::from(vec![ITEMS]);
    let runs: ArrayRef = Arc::new(RunArray::<Int32Type>::try_new(&ends, &one).unwrap());
    let batch = one_long_list(ITEMS, runs);
    let (checked, peak) = check_peak(&batch);
    assert_eq!(checked, Ok(()));
    let held = u64::try_from(batch.get_array_memory_size()).unwrap();
    assert!(
        peak <= 64 << 10,
        "a batch of {held} bytes checked with {peak} bytes"
    );
}

#[test]
fn checking_keys_allocates_by_the_values_they_name_not_by_the_keys() {
    let one: ArrayRef = Arc::new(StringArray::from(vec!["1"]));
    // A key a byte: the keys are the batch, and the check holds no more than a bit a value.
    let keys = PrimitiveArray::<Int8Type>::from(vec![0_i8; 4 << 20]);
    let keyed: ArrayRef = Arc::new(DictionaryArray::try_new(keys, one).unwrap());
    let batch = one_long_list(4 << 20, keyed);
    let (checked, peak) = check_peak(&batch);
    assert_eq!(checked, Ok(()));
    let held = u64::try_from(batch.get_array_memory_size()).unwrap();
    assert!(
        peak <= 64 << 10,
        "a batch of {held} bytes checked with {peak} bytes"
    );
}

/// One row of a list view of JSON naming `values` through `views`, each `(offset, size)`.
fn one_view(views: &[(i32, i32)], values: ArrayRef) -> RecordBatch {
    let item = json(Field::new("item", values.data_type().clone(), true));
    let list = ListViewArray::new(
        Arc::new(item),
        ScalarBuffer::from(views.iter().map(|(offset, _)| *offset).collect::<Vec<_>>()),
        ScalarBuffer::from(views.iter().map(|(_, size)| *size).collect::<Vec<_>>()),
        values,
        None,
    );
    let field = Field::new("c", list.data_type().clone(), true);
    RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![Arc::new(list)]).unwrap()
}

#[test]
fn what_checking_holds_beside_a_batch_is_no_more_than_its_charge() {
    const ROWS: i32 = 1 << 18;
    let texts = |count: i32| -> ArrayRef {
        Arc::new(StringArray::from_iter_values(
            (0..count).map(|value| value.to_string()),
        ))
    };
    // Views in row order hold nothing; views out of order are gathered, a span a row.
    let ordered: Vec<(i32, i32)> = (0..ROWS).map(|row| (row, 1)).collect();
    let reversed: Vec<(i32, i32)> = (0..ROWS).map(|row| (ROWS - 1 - row, 1)).collect();
    // A dictionary of few values, held as a bit a value; one of many values that few keys name,
    // held as the keys.
    let few = PrimitiveArray::<Int32Type>::from_iter_values((0..ROWS).map(|row| row % 64));
    let few: ArrayRef = Arc::new(DictionaryArray::try_new(few, texts(64)).unwrap());
    let sparse = PrimitiveArray::<Int32Type>::from_iter_values((0..16).map(|key| key << 14));
    let sparse: ArrayRef = Arc::new(DictionaryArray::try_new(sparse, texts(ROWS)).unwrap());
    let batches = [
        (one_view(&ordered, texts(ROWS)), false),
        (one_view(&reversed, texts(ROWS)), true),
        (one_long_list(ROWS, few), true),
        (one_long_list(16, sparse), true),
    ];
    for (batch, holds) in batches {
        let charged = super::held(&batch);
        let (checked, peak) = check_peak(&batch);
        assert_eq!(checked, Ok(()));
        assert_eq!(charged > 0, holds, "charged {charged} bytes");
        assert!(
            peak <= charged + (16 << 10),
            "charged {charged} bytes, held {peak}"
        );
    }
}

#[test]
fn columns_that_hold_no_json_are_neither_checked_nor_charged() {
    // A million text values a million keys name, none of it JSON.
    let values: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..1 << 20).map(|value| format!("not json {value}")),
    ));
    let keys = PrimitiveArray::<Int32Type>::from_iter_values(0..1 << 20);
    let keyed: ArrayRef = Arc::new(DictionaryArray::try_new(keys, values).unwrap());
    let field = Field::new("c", keyed.data_type().clone(), true);
    let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![keyed]).unwrap();
    assert_eq!(super::held(&batch), 0);
    let (result, peak) = check_peak(&batch);
    assert_eq!(result, Ok(()));
    assert!(peak <= 1 << 10, "checked with {peak} bytes");
    // Text beside JSON in a struct is not JSON, in any text type.
    let plain: [ArrayRef; 2] = [
        Arc::new(StringArray::from(vec!["not json"])),
        Arc::new(StringViewArray::from(vec!["not json"])),
    ];
    for text in plain {
        let fields = Fields::from(vec![
            Field::new("t", text.data_type().clone(), true),
            json(Field::new("j", DataType::Utf8, true)),
        ]);
        let json_text: ArrayRef = Arc::new(StringArray::from(vec!["1"]));
        let row = StructArray::new(fields.clone(), vec![text, json_text], None);
        let field = Field::new("c", DataType::Struct(fields), true);
        assert_eq!(checked(field, Arc::new(row)), Ok(()));
    }
}

#[test]
fn json_is_checked_in_runs_of_structs_large_lists_fixed_lists_and_sparse_keys() {
    let bad = || -> ArrayRef { Arc::new(StringArray::from(vec!["{"])) };
    // A run of a struct whose field is JSON.
    let fields = Fields::from(vec![json(Field::new("j", DataType::Utf8, true))]);
    let row: ArrayRef = Arc::new(StructArray::new(fields, vec![bad()], None));
    let ends = PrimitiveArray::<Int32Type>::from(vec![3]);
    let runs = RunArray::<Int32Type>::try_new(&ends, &row).unwrap();
    let field = Field::new("c", runs.data_type().clone(), true);
    assert!(invalid(&checked(field, Arc::new(runs))));
    // A large list and a fixed-size list of JSON.
    let item = Arc::new(json(Field::new("item", DataType::Utf8, true)));
    let large = LargeListArray::new(
        Arc::clone(&item),
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i64, 1])),
        bad(),
        None,
    );
    let field = Field::new("c", large.data_type().clone(), true);
    assert!(invalid(&checked(field, Arc::new(large))));
    let fixed = FixedSizeListArray::new(item, 1, bad(), None);
    let field = Field::new("c", fixed.data_type().clone(), true);
    assert!(invalid(&checked(field, Arc::new(fixed))));
    // A key naming one of a thousand values, the bad one: its keys are listed, not a bitmap.
    let values: ArrayRef = Arc::new(StringArray::from_iter_values((0..1_000).map(|value| {
        if value == 5 {
            "{".to_owned()
        } else {
            value.to_string()
        }
    })));
    let keys = PrimitiveArray::<Int32Type>::from(vec![5]);
    let keyed = DictionaryArray::try_new(keys, values).unwrap();
    let field = json(Field::new("c", keyed.data_type().clone(), true));
    assert!(invalid(&checked(field, Arc::new(keyed))));
}

#[test]
fn what_checking_holds_counts_maps_and_large_list_views() {
    // A map whose values are JSON a dictionary names.
    let values: ArrayRef = Arc::new(StringArray::from(vec!["1"]));
    let keys = PrimitiveArray::<Int32Type>::from(vec![0, 0]);
    let keyed: ArrayRef = Arc::new(DictionaryArray::try_new(keys, values).unwrap());
    let entries = Fields::from(vec![
        Field::new("key", DataType::Utf8, false),
        json(Field::new("value", keyed.data_type().clone(), true)),
    ]);
    let names: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
    let entries = StructArray::new(entries, vec![names, keyed], None);
    let entry = Arc::new(Field::new("entries", entries.data_type().clone(), false));
    let map = MapArray::new(
        entry,
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2])),
        entries,
        None,
        false,
    );
    let field = Field::new("c", map.data_type().clone(), true);
    let batch =
        RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![Arc::new(map)]).unwrap();
    assert!(super::held(&batch) > 0);
    // A large list view naming its items in reverse.
    let item = Arc::new(json(Field::new("item", DataType::Utf8, true)));
    let views = LargeListViewArray::new(
        item,
        ScalarBuffer::from(vec![1_i64, 0]),
        ScalarBuffer::from(vec![1_i64, 1]),
        Arc::new(StringArray::from(vec!["1", "2"])),
        None,
    );
    let field = Field::new("c", views.data_type().clone(), true);
    let batch =
        RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![Arc::new(views)]).unwrap();
    assert_eq!(super::held(&batch), 32);
}

#[test]
fn a_dictionary_s_named_values_are_a_bitmap_while_it_takes_no_more_than_its_keys_listed() {
    // Two keys listed take sixteen bytes: a bitmap of 128 values as many, of 129 one more.
    assert!(super::held::bitmapped(120, 2));
    assert!(super::held::bitmapped(128, 2));
    assert!(!super::held::bitmapped(129, 2));
    assert!(super::held::bitmapped(0, 0));
    assert!(!super::held::bitmapped(1, 0));
}

/// A batch whose only column, `c`, is `array`.
fn alone(array: ArrayRef) -> RecordBatch {
    let field = Field::new("c", array.data_type().clone(), true);
    RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array]).unwrap()
}

/// A dictionary of `values` values of JSON that keys 0 and 1 of type `K` name.
fn two_keys<K: arrow_array::types::ArrowDictionaryKeyType>(values: usize) -> ArrayRef {
    let keys = PrimitiveArray::<K>::from_iter_values(
        [0_usize, 1].map(|key| K::Native::from_usize(key).unwrap()),
    );
    let values: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..values).map(|value| value.to_string()),
    ));
    Arc::new(DictionaryArray::<K>::try_new(keys, values).unwrap())
}

/// What checking a dictionary of JSON, keyed by `K`, holds: a bit a value where that is no
/// more than its keys listed, else eight bytes a key.
fn held_by_keys<K: arrow_array::types::ArrowDictionaryKeyType>() {
    let held = |values: usize| {
        let dictionary = two_keys::<K>(values);
        let field = json(Field::new("c", dictionary.data_type().clone(), true));
        let batch =
            RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![dictionary]).unwrap();
        assert_eq!(check_batch(&batch), Ok(()));
        super::held(&batch)
    };
    assert_eq!(held(100), 13, "{}", K::DATA_TYPE);
    assert_eq!(held(128), 16, "{}", K::DATA_TYPE);
    assert_eq!(held(129), 16, "{}", K::DATA_TYPE);
    assert_eq!(held(1_000), 16, "{}", K::DATA_TYPE);
}

#[test]
fn what_checking_a_dictionary_holds_is_charged_exactly_for_every_key_type() {
    held_by_keys::<Int8Type>();
    held_by_keys::<Int16Type>();
    held_by_keys::<Int32Type>();
    held_by_keys::<Int64Type>();
    held_by_keys::<UInt8Type>();
    held_by_keys::<UInt16Type>();
    held_by_keys::<UInt32Type>();
    held_by_keys::<UInt64Type>();
}

/// A dictionary of a thousand values of JSON two keys name, which checking holds as its keys,
/// sixteen bytes, in a field `item` of JSON.
fn keyed_item() -> (Arc<Field>, ArrayRef) {
    let dictionary = two_keys::<Int32Type>(1_000);
    let item = json(Field::new("item", dictionary.data_type().clone(), true));
    (Arc::new(item), dictionary)
}

/// What checking a run-end encoded column of a struct of [`keyed_item`], ends of type `R`, holds.
fn held_in_runs<R: RunEndIndexType>() -> u64 {
    let (item, dictionary) = keyed_item();
    let row: ArrayRef = Arc::new(StructArray::new(
        Fields::from(vec![item]),
        vec![dictionary],
        None,
    ));
    let ends = PrimitiveArray::<R>::from_iter_values(
        [1_usize, 2].map(|end| R::Native::from_usize(end).unwrap()),
    );
    let runs = RunArray::<R>::try_new(&ends, &row).unwrap();
    let batch = alone(Arc::new(runs));
    assert_eq!(check_batch(&batch), Ok(()));
    super::held(&batch)
}

#[test]
fn what_checking_holds_is_charged_exactly_in_runs_of_every_end_type_and_lists_of_every_layout() {
    assert_eq!(held_in_runs::<Int16Type>(), 16);
    assert_eq!(held_in_runs::<Int32Type>(), 16);
    assert_eq!(held_in_runs::<Int64Type>(), 16);
    let (item, items) = keyed_item();
    let lists: [ArrayRef; 6] = [
        Arc::new(ListArray::new(
            Arc::clone(&item),
            OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 2])),
            Arc::clone(&items),
            None,
        )),
        Arc::new(LargeListArray::new(
            Arc::clone(&item),
            OffsetBuffer::new(ScalarBuffer::from(vec![0_i64, 2])),
            Arc::clone(&items),
            None,
        )),
        Arc::new(FixedSizeListArray::new(
            Arc::clone(&item),
            2,
            Arc::clone(&items),
            None,
        )),
        Arc::new(StructArray::new(
            Fields::from(vec![Arc::clone(&item)]),
            vec![Arc::clone(&items)],
            None,
        )),
        Arc::new(ListViewArray::new(
            Arc::clone(&item),
            ScalarBuffer::from(vec![0_i32]),
            ScalarBuffer::from(vec![2_i32]),
            Arc::clone(&items),
            None,
        )),
        Arc::new(LargeListViewArray::new(
            Arc::clone(&item),
            ScalarBuffer::from(vec![0_i64]),
            ScalarBuffer::from(vec![2_i64]),
            Arc::clone(&items),
            None,
        )),
    ];
    for list in lists {
        let batch = alone(list);
        assert_eq!(check_batch(&batch), Ok(()));
        assert_eq!(super::held(&batch), 16, "{}", batch.schema().field(0));
    }
    // Views naming their items out of order hold a span a row beside the items' keys.
    let reversed = ListViewArray::new(
        Arc::clone(&item),
        ScalarBuffer::from(vec![1_i32, 0]),
        ScalarBuffer::from(vec![1_i32, 1]),
        items,
        None,
    );
    assert_eq!(super::held(&alone(Arc::new(reversed))), 2 * 16 + 16);
}

#[test]
fn json_in_a_dictionary_of_structs_is_checked_and_charged_where_its_keys_name_it() {
    let fields = Fields::from(vec![json(Field::new("j", DataType::Utf8, true))]);
    let rows: ArrayRef = Arc::new(StructArray::new(
        fields,
        vec![texts(&[Some("1"), Some("x")])],
        None,
    ));
    let keyed = |keys: Vec<i32>| -> RecordBatch {
        let keys = PrimitiveArray::<Int32Type>::from(keys);
        alone(Arc::new(
            DictionaryArray::try_new(keys, Arc::clone(&rows)).unwrap(),
        ))
    };
    // Only the first value named: a bit for each of the two values.
    let first = keyed(vec![0, 0]);
    assert_eq!(check_batch(&first).map_err(|not| not.error), Ok(()));
    assert_eq!(super::held(&first), 1);
    assert!(matches!(
        check_batch(&keyed(vec![0, 1])),
        Err(NotJson {
            error: JsonError::Invalid(_),
            ..
        })
    ));
}

#[test]
fn keys_listed_from_a_dictionary_of_many_values_hold_no_more_than_their_charge() {
    // One key a row, past a power of two, naming every 65th of 65 values a key: a bitmap of the
    // values would take more than the keys listed, which a vector grown by doubling passes.
    const KEYS: usize = (1 << 16) + 1;
    let values: ArrayRef = Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
        "1",
        65 * KEYS,
    )));
    let keys = PrimitiveArray::<Int32Type>::from_iter_values(
        (0..KEYS).map(|key| i32::try_from(65 * key).unwrap()),
    );
    let keyed: ArrayRef = Arc::new(DictionaryArray::try_new(keys, values).unwrap());
    let field = json(Field::new("c", keyed.data_type().clone(), true));
    let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![keyed]).unwrap();
    assert!(!super::held::bitmapped(65 * KEYS, KEYS));
    let charged = super::held(&batch);
    assert_eq!(charged, 8 * u64::try_from(KEYS).unwrap());
    let (checked, peak) = check_peak(&batch);
    assert_eq!(checked, Ok(()));
    assert!(
        peak <= charged + (16 << 10),
        "charged {charged} bytes, held {peak}"
    );
}
