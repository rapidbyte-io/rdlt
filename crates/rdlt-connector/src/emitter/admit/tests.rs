use std::sync::Arc;

use arrow_array::builder::BinaryViewBuilder;
use arrow_array::types::{Int8Type, Int16Type, Int32Type, Int64Type};
use arrow_array::{
    ArrayRef, DictionaryArray, Int8Array, Int32Array, Int64Array, ListArray, ListViewArray,
    NullArray, RecordBatch, RunArray, StringArray, StructArray, UInt8Array, UnionArray,
};
use arrow_buffer::{Buffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Fields, UnionFields};

use rdlt_wire::Limits;

use super::admit;
use crate::cost::Allocations;
use crate::cost::tests::{batch, every_type, item, nested};

fn refused(batch: &RecordBatch) -> (&'static str, u64) {
    let refusal = admit(batch, true, &Limits::default()).unwrap_err();
    assert!(refusal.actual > refusal.limit);
    (refusal.name, refusal.limit)
}

#[test]
fn a_batch_nested_beyond_the_limit_is_refused_however_deep() {
    admit(&nested(64), true, &Limits::default()).unwrap();
    assert_eq!(refused(&nested(65)), ("nesting depth", 64));
    // Deep enough to overflow a stack that recursed a level at a time.
    assert_eq!(refused(&nested(3_000)), ("nesting depth", 64));
}

#[test]
fn a_batch_of_more_values_than_the_limit_is_refused() {
    const ROWS: usize = 10_000;
    // One row holding two billion nulls takes no bytes.
    let items = 2_000_000_000_usize;
    let nulls = ListArray::new(
        item(DataType::Null),
        OffsetBuffer::new(vec![0_i32, i32::try_from(items).unwrap()].into()),
        Arc::new(NullArray::new(items)),
        None,
    );
    assert_eq!(refused(&batch(Arc::new(nulls))), ("batch values", 64 << 20));
    // A list view's sizes count, since its rows may name the same items.
    let views = ListViewArray::new(
        item(DataType::Int8),
        ScalarBuffer::from(vec![0_i32; ROWS]),
        ScalarBuffer::from(vec![i32::try_from(ROWS).unwrap(); ROWS]),
        Arc::new(Int8Array::from(vec![0; ROWS])),
        None,
    );
    assert_eq!(refused(&batch(Arc::new(views))), ("batch values", 64 << 20));
}

#[test]
fn a_batch_whose_views_name_more_than_the_limit_is_refused() {
    let mut views = BinaryViewBuilder::new();
    let block = views.append_block(vec![7_u8; 1 << 20].into());
    for _ in 0..65 {
        views.try_append_view(block, 0, 1 << 20).unwrap();
    }
    assert_eq!(
        refused(&batch(Arc::new(views.finish()))),
        ("view bytes", 64 << 20)
    );
    // Within a dictionary's values too.
    let mut views = BinaryViewBuilder::new();
    let block = views.append_block(vec![7_u8; 1 << 20].into());
    for _ in 0..65 {
        views.try_append_view(block, 0, 1 << 20).unwrap();
    }
    let keyed =
        DictionaryArray::<Int64Type>::try_new(Int64Array::from(vec![0]), Arc::new(views.finish()))
            .unwrap();
    assert_eq!(refused(&batch(Arc::new(keyed))), ("view bytes", 64 << 20));
}

#[test]
fn a_batch_keeping_more_alive_than_the_limit_is_refused() {
    let values = Buffer::from_vec(vec![0_u8; (64 << 20) + 1]);
    let whole = UInt8Array::new(ScalarBuffer::new(values, 0, (64 << 20) + 1), None);
    // Three rows of it keep all of it alive.
    let slice: ArrayRef = Arc::new(whole.slice(0, 3));
    assert_eq!(refused(&batch(slice)), ("batch bytes", 64 << 20));
}

#[test]
fn a_batch_of_more_rows_or_columns_than_the_limit_is_refused() {
    let rows: ArrayRef = Arc::new(NullArray::new((1 << 20) + 1));
    assert_eq!(refused(&batch(rows)), ("batch rows", 1 << 20));
    // Nested fields count as columns.
    let fields: Fields = (0..10_000)
        .map(|index| Field::new(format!("f{index}"), DataType::Null, true))
        .collect();
    let columns = fields
        .iter()
        .map(|_| Arc::new(NullArray::new(1)) as ArrayRef)
        .collect();
    let wide: ArrayRef = Arc::new(StructArray::new(fields, columns, None));
    assert_eq!(refused(&batch(wide)), ("batch columns", 10_000));
}

#[test]
fn every_type_within_the_limits_is_admitted() {
    for column in every_type() {
        admit(&batch(column), true, &Limits::default()).unwrap();
    }
}

#[test]
fn an_encoded_column_counts_as_the_columns_its_layout_has() {
    // A dictionary column is one column, its values': ten thousand of them are within the limit.
    let words: ArrayRef = Arc::new(StringArray::from(vec!["a"]));
    let keyed = |_| {
        let keys = Int8Array::from(vec![0]);
        Arc::new(DictionaryArray::<Int8Type>::try_new(keys, Arc::clone(&words)).unwrap())
            as ArrayRef
    };
    let wide = |columns: usize, column: &dyn Fn(usize) -> ArrayRef| {
        RecordBatch::try_from_iter((0..columns).map(|index| (format!("c{index}"), column(index))))
            .unwrap()
    };
    admit(&wide(10_000, &keyed), true, &Limits::default()).unwrap();
    // A run-end encoded column is three: itself, its run ends and its values.
    let runs = |_| {
        let ends = arrow_array::Int16Array::from(vec![1]);
        Arc::new(RunArray::<Int16Type>::try_new(&ends, &words).unwrap()) as ArrayRef
    };
    admit(&wide(3_333, &runs), true, &Limits::default()).unwrap();
    assert_eq!(refused(&wide(3_334, &runs)), ("batch columns", 10_000));
    // Its values count toward the batch's, beside its rows and its runs.
    let long = RunArray::<Int32Type>::try_new(&Int32Array::from(vec![1 << 20]), &NullArray::new(1))
        .unwrap();
    admit(&batch(Arc::new(long)), true, &Limits::default()).unwrap();
}

#[test]
fn rows_naming_one_value_are_weighed_each_time_they_name_it() {
    const ROWS: usize = 100;
    // A hundred rows each naming the same mebibyte: a frame of them holds a hundred.
    let long = StringArray::from(vec!["x".repeat(1 << 20)]);
    let views = ListViewArray::new(
        item(DataType::Utf8),
        ScalarBuffer::from(vec![0_i32; ROWS]),
        ScalarBuffer::from(vec![1_i32; ROWS]),
        Arc::new(long.clone()),
        None,
    );
    let views = batch(Arc::new(views));
    assert!(Allocations::of(&views).bytes() < 2 << 20);
    assert_eq!(refused(&views), ("batch bytes", 64 << 20));
    let fields = UnionFields::try_new([0], [Field::new("s", DataType::Utf8, true)]).unwrap();
    let dense = UnionArray::try_new(
        fields,
        ScalarBuffer::from(vec![0_i8; ROWS]),
        Some(ScalarBuffer::from(vec![0_i32; ROWS])),
        vec![Arc::new(long)],
    );
    assert_eq!(
        refused(&batch(Arc::new(dense.unwrap()))),
        ("batch bytes", 64 << 20)
    );
}

#[test]
fn a_slice_is_weighed_by_what_its_rows_name() {
    // Two rows, the first of two billion nulls: the second alone names none of them.
    let items = 2_000_000_000_usize;
    let nulls = ListArray::new(
        item(DataType::Null),
        OffsetBuffer::<i32>::from_lengths([items, 0]),
        Arc::new(NullArray::new(items)),
        None,
    );
    let whole = batch(Arc::new(nulls));
    assert_eq!(refused(&whole), ("batch values", 64 << 20));
    admit(&whole.slice(1, 1), true, &Limits::default()).unwrap();
}

#[test]
fn a_dictionary_is_weighed_as_the_frame_of_its_own_it_would_go_in() {
    // Seventy million values no key names: beyond a frame's, whatever the rows hold.
    let values = NullArray::new(70_000_000);
    let keyed =
        DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![0]), Arc::new(values)).unwrap();
    assert_eq!(refused(&batch(Arc::new(keyed))), ("batch values", 64 << 20));
}

#[test]
fn a_batch_that_is_cut_as_it_is_sent_is_held_to_its_rows_and_schema_alone() {
    // Two billion nulls in one row: beyond a frame's values, which the cut refuses or divides.
    let items = 2_000_000_000_usize;
    let nulls = ListArray::new(
        item(DataType::Null),
        OffsetBuffer::<i32>::from_lengths([items]),
        Arc::new(NullArray::new(items)),
        None,
    );
    admit(&batch(Arc::new(nulls)), false, &Limits::default()).unwrap();
    let rows: ArrayRef = Arc::new(NullArray::new((1 << 20) + 1));
    let refusal = admit(&batch(rows), false, &Limits::default()).unwrap_err();
    assert_eq!((refusal.name, refusal.limit), ("batch rows", 1 << 20));
    let refusal = admit(&nested(65), false, &Limits::default()).unwrap_err();
    assert_eq!((refusal.name, refusal.limit), ("nesting depth", 64));
}

/// A list of each kind holding `item`, by name.
fn lists(item: &Arc<Field>) -> Vec<(&'static str, DataType)> {
    let entries = Field::new(
        "entries",
        DataType::Struct(Fields::from(vec![Arc::clone(item)])),
        false,
    );
    vec![
        ("list", DataType::List(Arc::clone(item))),
        ("large list", DataType::LargeList(Arc::clone(item))),
        ("list view", DataType::ListView(Arc::clone(item))),
        ("large list view", DataType::LargeListView(Arc::clone(item))),
        (
            "fixed size list",
            DataType::FixedSizeList(Arc::clone(item), 2),
        ),
        ("map", DataType::Map(Arc::new(entries), false)),
    ]
}

/// The limit a schema of one column of `kind` is refused by, within `columns` and `depth`.
fn refused_by(kind: DataType, columns: u64, depth: u64) -> Option<&'static str> {
    let limits = Limits {
        schema_columns: columns,
        nesting_depth: depth,
        ..Limits::default()
    };
    let schema = arrow_schema::Schema::new(vec![Field::new("c", kind, true)]);
    super::columns(&schema, &limits)
        .err()
        .map(|refused| refused.name)
}

#[test]
fn every_kind_of_list_counts_its_columns_and_levels() {
    for (name, kind) in lists(&Arc::new(Field::new("item", DataType::Int8, true))) {
        // The list and its items: two levels and two columns, three of each for a map's entries.
        let (columns, depth) = if name == "map" { (3, 3) } else { (2, 2) };
        assert_eq!(refused_by(kind.clone(), columns, depth), None, "{name}");
        assert_eq!(
            refused_by(kind.clone(), columns, depth - 1),
            Some("nesting depth"),
            "{name}"
        );
        assert_eq!(
            refused_by(kind, columns - 1, depth),
            Some("batch columns"),
            "{name}"
        );
    }
}

#[test]
fn a_union_s_members_count_below_it_and_a_dictionary_counts_as_its_values() {
    let fields = UnionFields::try_new(
        [0, 1],
        [
            Field::new("a", DataType::Int8, true),
            Field::new("b", DataType::Int8, true),
        ],
    )
    .unwrap();
    let union = DataType::Union(fields, arrow_schema::UnionMode::Dense);
    assert_eq!(refused_by(union.clone(), 3, 2), None);
    assert_eq!(refused_by(union.clone(), 2, 2), Some("batch columns"));
    assert_eq!(refused_by(union, 3, 1), Some("nesting depth"));
    let dictionary =
        |kind: DataType| DataType::Dictionary(Box::new(DataType::Int8), Box::new(kind));
    assert_eq!(refused_by(dictionary(DataType::Int8), 1, 1), None);
    let listed = DataType::List(Arc::new(Field::new("item", DataType::Int8, true)));
    assert_eq!(refused_by(dictionary(listed.clone()), 2, 2), None);
    assert_eq!(refused_by(dictionary(listed), 2, 1), Some("nesting depth"));
}
