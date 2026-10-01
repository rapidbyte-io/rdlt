//! Columns laid out in the ways Arrow allows and writers rarely choose, and their rows rendered
//! from the arrays themselves: what compacting, weighing and cutting a batch must not change.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::cast::AsArray as _;
use arrow_array::types::{
    Decimal256Type, Int8Type, Int16Type, Int32Type, Int64Type, RunEndIndexType,
};
use arrow_array::{
    Array, ArrayRef, DictionaryArray, FixedSizeListArray, Int8Array, Int32Array,
    LargeListViewArray, ListArray, ListViewArray, MapArray, RecordBatch, RunArray, StringArray,
    StringViewArray, StructArray, UnionArray, make_array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Fields, UnionFields};

/// The value `row` of a union, run-end or dictionary column names, rendered.
fn named(array: &dyn Array, row: usize) -> Option<String> {
    use DataType as T;
    fn ree<R: RunEndIndexType>(array: &dyn Array, row: usize) -> String {
        let runs = array.as_run::<R>();
        render(runs.values().as_ref(), runs.get_physical_index(row))
    }
    Some(match array.data_type() {
        T::Union(..) => {
            let union = array.as_union();
            let id = union.type_id(row);
            let at = union.value_offset(row);
            format!("U{id}:{}", render(union.child(id).as_ref(), at))
        }
        T::RunEndEncoded(ends, _) => match ends.data_type() {
            T::Int16 => ree::<Int16Type>(array, row),
            T::Int32 => ree::<Int32Type>(array, row),
            _ => ree::<Int64Type>(array, row),
        },
        T::Dictionary(..) => {
            let keyed = array.as_any_dictionary();
            if keyed.keys().is_null(row) {
                return Some("null".to_owned());
            }
            render(keyed.values().as_ref(), keyed.normalized_keys()[row])
        }
        _ => return None,
    })
}

/// The logical value of `row` of `array`, by walking the array itself.
pub(crate) fn render(array: &dyn Array, row: usize) -> String {
    use DataType as T;
    fn each(values: &dyn Array) -> String {
        let items: Vec<_> = (0..values.len()).map(|at| render(values, at)).collect();
        format!("[{}]", items.join(","))
    }
    if let Some(named) = named(array, row) {
        return named;
    }
    if array.is_null(row) {
        return "null".to_owned();
    }
    match array.data_type() {
        T::Null => "null".to_owned(),
        T::Boolean => array.as_boolean().value(row).to_string(),
        T::Int8 => array.as_primitive::<Int8Type>().value(row).to_string(),
        T::Int32 => array.as_primitive::<Int32Type>().value(row).to_string(),
        T::Int64 => array.as_primitive::<Int64Type>().value(row).to_string(),
        T::Utf8 => format!("{:?}", array.as_string::<i32>().value(row)),
        T::Binary => format!("{:?}", array.as_binary::<i32>().value(row)),
        T::LargeUtf8 => format!("{:?}", array.as_string::<i64>().value(row)),
        T::LargeBinary => format!("{:?}", array.as_binary::<i64>().value(row)),
        T::FixedSizeBinary(_) => format!("{:?}", array.as_fixed_size_binary().value(row)),
        T::Decimal256(..) => array
            .as_primitive::<Decimal256Type>()
            .value(row)
            .to_string(),
        T::Utf8View => format!("{:?}", array.as_string_view().value(row)),
        T::BinaryView => format!("{:?}", array.as_binary_view().value(row)),
        T::List(_) => each(array.as_list::<i32>().value(row).as_ref()),
        T::LargeList(_) => each(array.as_list::<i64>().value(row).as_ref()),
        T::ListView(_) => each(array.as_list_view::<i32>().value(row).as_ref()),
        T::LargeListView(_) => each(array.as_list_view::<i64>().value(row).as_ref()),
        T::FixedSizeList(..) => each(array.as_fixed_size_list().value(row).as_ref()),
        T::Map(..) => each(&array.as_map().value(row)),
        T::Struct(_) => {
            let parent = array.as_struct();
            let fields: Vec<_> = parent
                .columns()
                .iter()
                .map(|column| render(column.as_ref(), row))
                .collect();
            format!("{{{}}}", fields.join(","))
        }
        other => panic!("no renderer for {other}"),
    }
}

/// Every row of `batch`'s one column, rendered.
pub(crate) fn rendered(batch: &RecordBatch) -> Vec<String> {
    (0..batch.num_rows())
        .map(|row| render(batch.column(0).as_ref(), row))
        .collect()
}

fn item(data_type: &DataType) -> Arc<Field> {
    Arc::new(Field::new("item", data_type.clone(), true))
}

fn long_views() -> ArrayRef {
    Arc::new(StringViewArray::from(vec![
        Some("a string long enough to leave its view, number zero"),
        None,
        Some("tiny"),
        Some("a string long enough to leave its view, number three"),
        Some(""),
        Some("a string long enough to leave its view, number five"),
        Some("x"),
        Some("a string long enough to leave its view, number seven"),
    ]))
}

fn ints(count: i32) -> ArrayRef {
    Arc::new(Int32Array::from_iter(
        (0..count).map(|at| (at % 4 != 3).then_some(at)),
    ))
}

/// List views naming items out of order, more than once, and under a null row.
fn odd_list_views(values: ArrayRef) -> ArrayRef {
    let offsets: ScalarBuffer<i32> = vec![6, 0, 3, 0, 5, 2, 7].into();
    let sizes: ScalarBuffer<i32> = vec![2, 5, 4, 0, 3, 0, 1].into();
    let nulls = NullBuffer::from(vec![true, true, false, true, true, true, true]);
    Arc::new(ListViewArray::new(
        item(values.data_type()),
        offsets,
        sizes,
        values,
        Some(nulls),
    ))
}

fn odd_large_list_views(values: ArrayRef) -> ArrayRef {
    let offsets: ScalarBuffer<i64> = vec![6, 0, 3, 0, 5, 2, 7].into();
    let sizes: ScalarBuffer<i64> = vec![2, 5, 4, 0, 3, 0, 1].into();
    Arc::new(LargeListViewArray::new(
        item(values.data_type()),
        offsets,
        sizes,
        values,
        None,
    ))
}

/// A dense union of type ids 5 and 7 whose offsets go back and forth and repeat.
fn odd_dense(first: ArrayRef, second: ArrayRef) -> ArrayRef {
    let fields = UnionFields::try_new(
        vec![5, 7],
        vec![
            Field::new("a", first.data_type().clone(), true),
            Field::new("b", second.data_type().clone(), true),
        ],
    )
    .unwrap();
    let ids: ScalarBuffer<i8> = vec![7, 5, 7, 7, 5, 5, 7].into();
    let offsets: ScalarBuffer<i32> = vec![5, 3, 0, 5, 0, 3, 1].into();
    Arc::new(UnionArray::try_new(fields, ids, Some(offsets), vec![first, second]).unwrap())
}

fn sparse(first: ArrayRef, second: ArrayRef) -> ArrayRef {
    let fields = UnionFields::try_new(
        vec![5, 7],
        vec![
            Field::new("a", first.data_type().clone(), true),
            Field::new("b", second.data_type().clone(), true),
        ],
    )
    .unwrap();
    let ids: ScalarBuffer<i8> = vec![7, 5, 7, 7, 5, 5, 7].into();
    Arc::new(UnionArray::try_new(fields, ids, None, vec![first, second]).unwrap())
}

fn runs_of(values: &ArrayRef) -> ArrayRef {
    // Eight values: runs ending at 2, 3, 3+..: lengths 2,1,3,1,2,1,4,1 => 15 rows.
    let ends = Int32Array::from(vec![2, 3, 6, 7, 9, 10, 14, 15]);
    Arc::new(RunArray::<Int32Type>::try_new(&ends, values).unwrap())
}

/// A run-end column whose fields are not as `RunArray::try_new` names them.
fn runs_typed(values_nullable: bool, metadata: bool) -> ArrayRef {
    let values = StringArray::from(vec!["x", "y", "z"]);
    let ends = Int32Array::from(vec![2, 3, 5]);
    let mut field = Field::new("values", DataType::Utf8, values_nullable);
    if metadata {
        field = field.with_metadata(HashMap::from([("k".to_owned(), "v".to_owned())]));
    }
    let data_type = DataType::RunEndEncoded(
        Arc::new(Field::new("run_ends", DataType::Int32, false)),
        Arc::new(field),
    );
    let data = ends
        .to_data()
        .into_builder()
        .data_type(data_type)
        .len(5)
        .buffers(vec![])
        .child_data(vec![ends.to_data(), values.to_data()])
        .build()
        .unwrap();
    make_array(data)
}

fn masked_struct(children: Vec<(&str, ArrayRef)>) -> ArrayRef {
    let rows = children[0].1.len();
    let fields: Fields = children
        .iter()
        .map(|(name, child)| Field::new(*name, child.data_type().clone(), true))
        .collect();
    let nulls = NullBuffer::from((0..rows).map(|row| row % 3 != 1).collect::<Vec<_>>());
    let columns = children.into_iter().map(|(_, child)| child).collect();
    Arc::new(StructArray::new(fields, columns, Some(nulls)))
}

/// List views, alone and under other columns.
fn list_views(views: &ArrayRef) -> Vec<(&'static str, ArrayRef)> {
    vec![
        ("views sliced", views.slice(1, 6)),
        ("list view odd ints", odd_list_views(ints(8))),
        ("list view odd views", odd_list_views(Arc::clone(views))),
        (
            "large list view odd views",
            odd_large_list_views(Arc::clone(views)),
        ),
        (
            "list view of sliced values",
            odd_list_views(ints(12).slice(3, 8)),
        ),
        (
            "list of list view of views",
            Arc::new(ListArray::new(
                item(odd_list_views(Arc::clone(views)).data_type()),
                OffsetBuffer::new(vec![1, 1, 3, 6, 7].into()),
                odd_list_views(Arc::clone(views)),
                Some(NullBuffer::from(vec![true, true, false, true])),
            )),
        ),
    ]
}

/// Unions, alone and over other columns.
fn unions(views: &ArrayRef) -> Vec<(&'static str, ArrayRef)> {
    let keys = Int8Array::from(vec![Some(2), None, Some(0), Some(2), Some(1), Some(1)]);
    let tags = Arc::new(StringArray::from(vec!["p", "q", "r"]));
    vec![
        ("dense odd", odd_dense(ints(4), Arc::clone(views))),
        (
            "dense of list views",
            odd_dense(odd_list_views(Arc::clone(views)), Arc::clone(views)),
        ),
        (
            "sparse of list views",
            sparse(odd_list_views(Arc::clone(views)), ints(7)),
        ),
        (
            "dense of dictionary",
            odd_dense(
                ints(4),
                Arc::new(DictionaryArray::try_new(keys, tags).unwrap()),
            ),
        ),
    ]
}

/// Run-end columns, alone, sliced, over and under other columns, and of fields named otherwise
/// than `RunArray::try_new` names them.
fn runs(views: &ArrayRef) -> Vec<(&'static str, ArrayRef)> {
    let seven = Int32Array::from(vec![2, 3, 6, 7, 9, 10, 14]);
    let of_list_views: ArrayRef = Arc::new(
        RunArray::<Int32Type>::try_new(&seven, &odd_list_views(Arc::clone(views))).unwrap(),
    );
    let dense = odd_dense(ints(4), Arc::clone(views));
    let all = runs_of(views);
    let nulls = NullBuffer::from(vec![true, false, true, true, true, false, true]);
    vec![
        ("runs of views", Arc::clone(&all)),
        ("runs of views sliced", all.slice(4, 9)),
        ("runs of list views", Arc::clone(&of_list_views)),
        (
            "runs of dense",
            Arc::new(RunArray::<Int32Type>::try_new(&seven, &dense).unwrap()),
        ),
        (
            "list of runs",
            Arc::new(ListArray::new(
                item(all.data_type()),
                OffsetBuffer::new(vec![0, 1, 1, 5, 8, 8, 13, 15].into()),
                Arc::clone(&all),
                Some(nulls),
            )),
        ),
        (
            "struct of runs and dense and list views, masked",
            masked_struct(vec![
                ("r", of_list_views.slice(3, 7)),
                ("u", odd_dense(odd_list_views(ints(8)), Arc::clone(views))),
                ("l", odd_list_views(Arc::clone(views))),
            ])
            .slice(1, 5),
        ),
        ("runs, values not nullable", runs_typed(false, false)),
        ("runs, values with metadata", runs_typed(true, true)),
        ("runs, as try_new names them", runs_typed(true, false)),
    ]
}

/// Fixed-size lists and a sorted map, of views.
fn fixed_and_maps(views: &ArrayRef) -> Vec<(&'static str, ArrayRef)> {
    let nulls = || Some(NullBuffer::from(vec![true, false, true, true]));
    let dense = odd_dense(ints(4), Arc::clone(views));
    let of_dense = FixedSizeListArray::new(item(dense.data_type()), 2, dense.slice(1, 6), None);
    let keys: ArrayRef = Arc::new(StringArray::from(vec![
        "a", "b", "c", "d", "e", "f", "g", "h",
    ]));
    let fields = Fields::from(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", views.data_type().clone(), true),
    ]);
    let entries = StructArray::new(fields, vec![keys, Arc::clone(views)], None);
    let entry = Arc::new(Field::new("entries", entries.data_type().clone(), false));
    let offsets = OffsetBuffer::new(vec![1, 3, 3, 6, 8].into());
    vec![
        (
            "fixed list of views",
            Arc::new(FixedSizeListArray::new(
                item(views.data_type()),
                2,
                Arc::clone(views),
                nulls(),
            )),
        ),
        (
            "fixed list of nothing",
            Arc::new(FixedSizeListArray::new(
                item(views.data_type()),
                0,
                views.slice(0, 0),
                nulls(),
            )),
        ),
        (
            "fixed list of dense, sliced",
            (Arc::new(of_dense) as ArrayRef).slice(1, 2),
        ),
        (
            "sorted map",
            Arc::new(MapArray::new(entry, offsets, entries, nulls(), true)),
        ),
    ]
}

/// Dictionaries of nested values, with null and repeated keys, and under a list view.
fn dictionaries(views: &ArrayRef) -> Vec<(&'static str, ArrayRef)> {
    let keys = Int8Array::from(vec![Some(6), None, Some(0), Some(6), Some(2), Some(1)]);
    let parent = masked_struct(vec![("v", Arc::clone(views)), ("n", ints(8))]);
    let inner = Int8Array::from(vec![
        Some(2),
        None,
        Some(0),
        Some(2),
        Some(1),
        Some(1),
        None,
        Some(0),
    ]);
    let tags = Arc::new(StringArray::from(vec!["p", "q", "r"]));
    vec![
        (
            "dictionary of struct",
            Arc::new(DictionaryArray::try_new(keys.clone(), parent).unwrap()),
        ),
        (
            "dictionary of list views",
            Arc::new(DictionaryArray::try_new(keys, odd_list_views(Arc::clone(views))).unwrap()),
        ),
        (
            "list view of dictionary",
            odd_list_views(Arc::new(DictionaryArray::try_new(inner, tags).unwrap())),
        ),
    ]
}

/// Every odd column, with its name.
pub(crate) fn columns() -> Vec<(&'static str, ArrayRef)> {
    let views = long_views();
    let mut columns = list_views(&views);
    columns.extend(unions(&views));
    columns.extend(runs(&views));
    columns.extend(fixed_and_maps(&views));
    columns.extend(dictionaries(&views));
    columns
}
