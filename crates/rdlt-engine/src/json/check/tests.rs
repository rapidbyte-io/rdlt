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
