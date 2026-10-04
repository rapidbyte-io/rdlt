use std::sync::Arc;

use arrow_array::builder::{Int64Builder, MapBuilder, StringBuilder};
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, Int64Array, LargeListArray, ListArray, StructArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, Fields};

use super::unhidden;

/// Rows 0 to 2 whose middle row is null.
#[expect(
    clippy::unnecessary_wraps,
    reason = "Arrow takes a row's nulls as an option"
)]
fn middle_null() -> Option<NullBuffer> {
    Some(NullBuffer::from(vec![true, false, true]))
}

/// `values`, three rows, in every nesting whose middle row is null: a struct's field, a list's,
/// a large list's and a fixed-size list's items, a map's values, and a list within a struct.
fn nestings(values: &ArrayRef) -> Vec<ArrayRef> {
    let item = Arc::new(Field::new("item", DataType::Int64, false));
    let fields = Fields::from(vec![Field::new("x", DataType::Int64, false)]);
    let within_struct: ArrayRef =
        Arc::new(StructArray::try_new(fields, vec![Arc::clone(values)], middle_null()).unwrap());
    let lengths = [1, 1, 1];
    let within_list: ArrayRef = Arc::new(ListArray::new(
        Arc::clone(&item),
        OffsetBuffer::from_lengths(lengths),
        Arc::clone(values),
        middle_null(),
    ));
    let within_large: ArrayRef = Arc::new(LargeListArray::new(
        Arc::clone(&item),
        OffsetBuffer::from_lengths(lengths),
        Arc::clone(values),
        middle_null(),
    ));
    let within_fixed: ArrayRef = Arc::new(FixedSizeListArray::new(
        item,
        1,
        Arc::clone(values),
        middle_null(),
    ));
    let mut map = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
    for (row, valid) in [(1, true), (2, false), (3, true)] {
        map.keys().append_value("k");
        map.values().append_value(row);
        map.append(valid).unwrap();
    }
    let within_map: ArrayRef = Arc::new(map.finish());
    let nested: ArrayRef = Arc::new(
        StructArray::try_new(
            Fields::from(vec![Field::new("l", within_list.data_type().clone(), true)]),
            vec![Arc::clone(&within_list)],
            None,
        )
        .unwrap(),
    );
    vec![
        within_struct,
        within_list,
        within_large,
        within_fixed,
        within_map,
        nested,
    ]
}

/// The integers `array` holds at any depth that are not null.
fn held(array: &dyn Array) -> Vec<i64> {
    match array.data_type() {
        DataType::Int64 => array.as_primitive::<Int64Type>().iter().flatten().collect(),
        DataType::Struct(_) => array
            .as_struct()
            .columns()
            .iter()
            .flat_map(|column| held(column.as_ref()))
            .collect(),
        DataType::List(_) => held(array.as_list::<i32>().values().as_ref()),
        DataType::LargeList(_) => held(array.as_list::<i64>().values().as_ref()),
        DataType::FixedSizeList(..) => held(array.as_fixed_size_list().values().as_ref()),
        DataType::Map(..) => held(array.as_map().entries()),
        _ => Vec::new(),
    }
}

#[test]
fn nothing_beneath_a_null_row_is_held_at_any_depth() {
    let values: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    for array in nestings(&values) {
        let kind = array.data_type().to_string();
        assert_eq!(
            held(array.as_ref()),
            [1, 2, 3],
            "{kind} holds the value as it arrived"
        );
        let unhidden = unhidden(&array).unwrap();
        assert_eq!(held(unhidden.as_ref()), [1, 3], "{kind}");
        assert_eq!(unhidden.len(), 3, "{kind}");
        assert_eq!(
            unhidden.logical_null_count(),
            array.logical_null_count(),
            "{kind}"
        );
    }
    // An array with nothing beneath a null row is itself.
    let fields = Fields::from(vec![Field::new("x", DataType::Int64, false)]);
    let plain: ArrayRef =
        Arc::new(StructArray::try_new(fields, vec![Arc::clone(&values)], None).unwrap());
    assert!(Arc::ptr_eq(&unhidden(&plain).unwrap(), &plain));
    // A list whose null rows span no item is itself too.
    let item = Arc::new(Field::new("item", DataType::Int64, false));
    let spanning_none: ArrayRef = Arc::new(ListArray::new(
        item,
        OffsetBuffer::from_lengths([2, 0, 1]),
        values,
        middle_null(),
    ));
    assert!(Arc::ptr_eq(
        &unhidden(&spanning_none).unwrap(),
        &spanning_none
    ));
}
