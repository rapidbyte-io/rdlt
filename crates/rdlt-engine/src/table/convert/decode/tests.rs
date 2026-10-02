use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int8Type, Int32Type};
use arrow_array::{
    Array, ArrayRef, DictionaryArray, FixedSizeListArray, Int8Array, Int32Array, LargeListArray,
    LargeListViewArray, ListArray, ListViewArray, MapArray, RunArray, StringArray, StructArray,
    UnionArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, Fields, UnionFields};

use super::decoded;

/// Five rows over three runs: `a a b b c`.
fn runs() -> ArrayRef {
    let ends = Int32Array::from(vec![2, 4, 5]);
    let values = StringArray::from(vec!["a", "b", "c"]);
    Arc::new(RunArray::<Int32Type>::try_new(&ends, &values).unwrap())
}

/// `values` keyed by `keys`, a null where a key is `None`, whose bytes then hold `99`: no
/// value's place.
fn keyed(keys: &[Option<i8>], values: ArrayRef) -> ArrayRef {
    let raw = Int8Array::from_iter_values(keys.iter().map(|key| key.unwrap_or(99)));
    let nulls = NullBuffer::from(keys.iter().map(Option::is_some).collect::<Vec<_>>());
    let keys = Int8Array::new(raw.values().clone(), Some(nulls));
    Arc::new(DictionaryArray::<Int8Type>::try_new(keys, values).unwrap())
}

/// The rows of `array`, each as Arrow displays it.
fn shown(array: &ArrayRef) -> Vec<String> {
    let options = arrow_cast::display::FormatOptions::default().with_null("null");
    let formatter = arrow_cast::display::ArrayFormatter::try_new(array.as_ref(), &options).unwrap();
    (0..array.len())
        .map(|row| formatter.value(row).to_string())
        .collect()
}

#[test]
fn keys_into_runs_name_each_its_run() {
    let values = runs();
    let named = decoded(&keyed(
        &[Some(0), Some(3), None, Some(4)],
        Arc::clone(&values),
    ))
    .unwrap();
    assert_eq!(shown(&named), ["a", "b", "null", "c"]);
    // The first row and the last, each a run's end.
    let ends = keyed(&[Some(1), Some(4), Some(0)], values);
    assert_eq!(shown(&decoded(&ends).unwrap()), ["a", "c", "a"]);
}

/// An item field of `values`' type.
fn item(values: &ArrayRef) -> Arc<Field> {
    Arc::new(Field::new("item", values.data_type().clone(), true))
}

/// One row of each kind of list over `values` whole.
fn lists(values: &ArrayRef) -> Vec<(&'static str, ArrayRef)> {
    let one = || OffsetBuffer::from_lengths([values.len()]);
    let (first, size) = (vec![0_i32], vec![5_i32]);
    let list = ListArray::new(item(values), one(), Arc::clone(values), None);
    let large = LargeListArray::new(
        item(values),
        OffsetBuffer::from_lengths([5]),
        Arc::clone(values),
        None,
    );
    let view = ListViewArray::new(
        item(values),
        first.into(),
        size.into(),
        Arc::clone(values),
        None,
    );
    let (first, size) = (vec![0_i64], vec![5_i64]);
    let large_view = LargeListViewArray::new(
        item(values),
        first.into(),
        size.into(),
        Arc::clone(values),
        None,
    );
    let fixed = FixedSizeListArray::new(item(values), 5, Arc::clone(values), None);
    let keys: ArrayRef = Arc::new(StringArray::from(vec!["k1", "k2", "k3", "k4", "k5"]));
    let fields = Fields::from(vec![
        Field::new("keys", DataType::Utf8, false),
        Field::new("values", values.data_type().clone(), true),
    ]);
    let entries = StructArray::new(fields, vec![keys, Arc::clone(values)], None);
    let entry = Arc::new(Field::new("entries", entries.data_type().clone(), false));
    let map = MapArray::new(entry, one(), entries, None, false);
    vec![
        ("list", Arc::new(list)),
        ("large list", Arc::new(large)),
        ("list view", Arc::new(view)),
        ("large list view", Arc::new(large_view)),
        ("fixed-size list", Arc::new(fixed)),
        ("map", Arc::new(map)),
    ]
}

/// One row of each nesting a run-end encoding may lie in, over [`runs`].
fn nestings() -> Vec<(&'static str, ArrayRef)> {
    let values = runs();
    let field = Field::new("run", values.data_type().clone(), true);
    let one_field = || Fields::from(vec![field.clone()]);
    let row = StructArray::new(one_field(), vec![values.slice(2, 1)], None);
    let members = UnionFields::try_new([0], [field.clone()]).unwrap();
    let union = UnionArray::try_new(members, vec![0_i8].into(), None, vec![values.slice(4, 1)]);
    let rows: ArrayRef = Arc::new(StructArray::new(
        one_field(),
        vec![Arc::clone(&values)],
        None,
    ));
    let listed_rows = ListArray::new(item(&rows), OffsetBuffer::from_lengths([5]), rows, None);
    let mut nestings = lists(&values);
    nestings.extend([
        ("struct", Arc::new(row) as ArrayRef),
        ("union", Arc::new(union.unwrap())),
        ("dictionary", keyed(&[Some(1)], Arc::clone(&values))),
        ("list of structs", Arc::new(listed_rows)),
        ("run-end encoding", values.slice(2, 1)),
    ]);
    nestings
}

#[test]
fn a_null_key_into_values_holding_runs_at_any_depth_is_a_null_and_the_others_their_values() {
    for (nesting, value) in nestings() {
        // The value twice, between null keys whose bytes name no value.
        let column = keyed(&[None, Some(0), None, Some(0), None], Arc::clone(&value));
        let named = decoded(&column).unwrap_or_else(|error| panic!("{nesting}: {error}"));
        let value = shown(&decoded(&value).unwrap())[0].clone();
        let rows = shown(&named);
        assert_eq!([&rows[1], &rows[3]], [&value, &value], "{nesting}");
        // A union has no nulls of its own: its null rows are its members'.
        let null = if nesting == "union" {
            "{run=c}"
        } else {
            "null"
        };
        assert_eq!([&rows[0], &rows[2], &rows[4]], [null; 3], "{nesting}");
        // Every encoding is decoded but a union's members, which decoding leaves as they are.
        let encoded = format!("{:?}", named.data_type()).contains("RunEndEncoded");
        assert_eq!(encoded, nesting == "union", "{nesting}");
    }
}

/// Lists of four, two and four items keyed into `x y z`: the items `x y z x | y z | x y z x`.
fn lists_of_keys() -> ArrayRef {
    let keys = Int8Array::from_iter_values((0..10).map(|item| item % 3));
    let words: ArrayRef = Arc::new(StringArray::from(vec!["x", "y", "z"]));
    let items: ArrayRef = Arc::new(DictionaryArray::<Int8Type>::try_new(keys, words).unwrap());
    let item = Arc::new(Field::new("item", items.data_type().clone(), true));
    Arc::new(ListArray::new(
        item,
        OffsetBuffer::from_lengths([4, 2, 4]),
        items,
        None,
    ))
}

#[test]
fn a_lists_items_are_only_those_its_rows_name_from_wherever_they_start() {
    let lists = lists_of_keys();
    for (first, rows, items, shown_rows) in [
        // From the first item, naming fewer than the list holds.
        (0, 1, 4, vec!["[x, y, z, x]"]),
        // From the fifth, to the last.
        (1, 2, 6, vec!["[y, z]", "[x, y, z, x]"]),
        (2, 1, 4, vec!["[x, y, z, x]"]),
        // All of them.
        (0, 3, 10, vec!["[x, y, z, x]", "[y, z]", "[x, y, z, x]"]),
    ] {
        let named = decoded(&lists.slice(first, rows)).unwrap();
        let list = named.as_list::<i32>();
        assert_eq!(list.values().len(), items, "rows {first}..{}", first + rows);
        assert_eq!(list.value_offsets()[0], 0);
        assert_eq!(list.values().data_type(), &DataType::Utf8);
        assert_eq!(shown(&named), shown_rows, "rows {first}..{}", first + rows);
    }
}

#[test]
fn a_list_holding_only_what_its_rows_name_is_returned_as_it_is() {
    let items: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));
    let item = Arc::new(Field::new("item", DataType::Utf8, true));
    let list: ArrayRef = Arc::new(ListArray::new(
        item,
        OffsetBuffer::from_lengths([1, 2]),
        items,
        None,
    ));
    assert!(Arc::ptr_eq(&decoded(&list).unwrap(), &list));
}
