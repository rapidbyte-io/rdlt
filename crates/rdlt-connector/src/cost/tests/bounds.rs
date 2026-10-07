//! What measuring costs: the rows, items and stretches it looks at and the values it remembers,
//! over every encoding that lets rows name a value more than once.

use std::sync::Arc;

use arrow_array::builder::{BinaryViewBuilder, StringViewBuilder};
use arrow_array::types::{
    ArrowDictionaryKeyType, Int8Type, Int16Type, Int32Type, Int64Type, RunEndIndexType, UInt8Type,
    UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{
    Array, ArrayRef, DictionaryArray, GenericListViewArray, Int64Array, ListArray, NullArray,
    OffsetSizeTrait, PrimitiveArray, RunArray, StringArray, StructArray,
};
use arrow_buffer::{ArrowNativeType, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Fields};
use proptest::prelude::*;

use super::{batch, item, native};

/// Calls `$check` with each dictionary key type.
macro_rules! every_key {
    ($check:ident) => {
        $check::<Int8Type>();
        $check::<Int16Type>();
        $check::<Int32Type>();
        $check::<Int64Type>();
        $check::<UInt8Type>();
        $check::<UInt16Type>();
        $check::<UInt32Type>();
        $check::<UInt64Type>();
    };
}

/// What measuring every row of a column cost.
#[derive(Clone, Copy, Debug)]
struct Work {
    expanded: u64,
    steps: u64,
    remembered: usize,
}

/// Measures every row of `column` up to `limit`, remembering values or, where `forgetful`, none.
fn work(column: &ArrayRef, limit: u64, forgetful: bool) -> Work {
    let (batch, rendering) = (batch(Arc::clone(column)), native());
    let mut measure = rendering.measure(&batch, limit);
    if forgetful {
        measure = measure.forgetful();
    }
    let expanded = measure.expanded(0..column.len());
    let (steps, remembered) = measure.work();
    Work {
        expanded,
        steps,
        remembered,
    }
}

/// Measures `column` whole, checks that remembering changes nothing it measures, and returns
/// what it cost with values remembered and without.
fn measured(column: &ArrayRef) -> (Work, Work) {
    let (kept, forgotten) = (work(column, u64::MAX, false), work(column, u64::MAX, true));
    assert_eq!(kept.expanded, forgotten.expanded, "{}", column.data_type());
    assert_eq!(forgotten.remembered, 0);
    (kept, forgotten)
}

fn steps(count: usize) -> u64 {
    u64::try_from(count).unwrap()
}

/// `values` keyed by `keys`, with keys of type `K`.
fn keyed<K: ArrowDictionaryKeyType>(
    keys: impl IntoIterator<Item = usize>,
    values: ArrayRef,
) -> ArrayRef {
    let keys = keys
        .into_iter()
        .map(|key| K::Native::from_usize(key).unwrap());
    let keys = PrimitiveArray::<K>::from_iter_values(keys);
    Arc::new(DictionaryArray::<K>::try_new(keys, values).unwrap())
}

/// `lists` lists of `items` strings each, which take a scan to measure.
fn lists(lists: usize, items: usize) -> ArrayRef {
    let words = StringArray::from(vec!["a \"quoted\" word"; lists * items]);
    Arc::new(ListArray::new(
        item(DataType::Utf8),
        OffsetBuffer::from_lengths(vec![items; lists]),
        Arc::new(words),
        None,
    ))
}

/// `lists` lists of `items` views each, which take a step a view to measure.
fn view_lists(lists: usize, items: usize) -> ArrayRef {
    let mut views = StringViewBuilder::new();
    let block = views.append_block(vec![b'x'; 64].into());
    for _ in 0..lists * items {
        views.try_append_view(block, 0, 64).unwrap();
    }
    Arc::new(ListArray::new(
        item(DataType::Utf8View),
        OffsetBuffer::from_lengths(vec![items; lists]),
        Arc::new(views.finish()),
        None,
    ))
}

#[test]
fn a_view_in_a_nested_value_is_scanned_a_step_for_each_64_bytes_of_it() {
    let listed = |bytes: usize| -> ArrayRef {
        let mut views = StringViewBuilder::new();
        views.append_value("x".repeat(bytes));
        Arc::new(ListArray::new(
            item(DataType::Utf8View),
            OffsetBuffer::from_lengths([1]),
            Arc::new(views.finish()),
            None,
        ))
    };
    assert_eq!(alone(&listed(64 * 80)) - alone(&listed(64 * 40)), 40);
}

/// The steps one row naming the first of `values` takes to measure.
fn alone(values: &ArrayRef) -> u64 {
    let one = keyed::<Int32Type>([0], Arc::clone(values));
    work(&one, u64::MAX, true).steps
}

fn one_dear_value_is_measured_once_however_many_keys_name_it<K: ArrowDictionaryKeyType>() {
    const ROWS: usize = 100;
    for values in [lists(1, 2_000), view_lists(1, 300)] {
        let each = alone(&values);
        assert!(each > 100, "{each}");
        let column = keyed::<K>(vec![0; ROWS], values);
        let (kept, forgotten) = measured(&column);
        assert!(kept.steps <= 4 * steps(ROWS) + each, "{kept:?}");
        assert_eq!(kept.remembered, 1);
        // Measured anew for each row, it costs the rows times the value.
        assert!(forgotten.steps >= steps(ROWS) * (each - 4), "{forgotten:?}");
    }
}

fn each_dear_value_named_is_measured_once<K: ArrowDictionaryKeyType>() {
    const ROWS: usize = 2_000;
    const VALUES: usize = 100;
    for values in [lists(VALUES, 200), view_lists(VALUES, 50)] {
        let each = alone(&values);
        // Half the values are named, twenty times each.
        let named = VALUES / 2;
        let column = keyed::<K>((0..ROWS).map(|row| row % named), values);
        // Remembering changes nothing measured (`remembering_changes_nothing_measured`): only
        // what it saves is measured here.
        let kept = work(&column, u64::MAX, false);
        assert!(
            kept.steps <= 4 * steps(ROWS) + steps(named) * each,
            "{kept:?}"
        );
        assert_eq!(kept.remembered, named);
    }
}

fn values_measured_in_a_step_are_not_remembered<K: ArrowDictionaryKeyType>() {
    const ROWS: usize = 2_000;
    let numbers: ArrayRef = Arc::new(Int64Array::from(vec![7; 100]));
    let (kept, _) = measured(&keyed::<K>((0..ROWS).map(|row| row % 100), numbers));
    // Fixed-width values are measured by arithmetic, whatever their keys.
    assert!(kept.steps <= 4, "{kept:?}");
    assert_eq!(kept.remembered, 0);
    let words: ArrayRef = Arc::new(StringArray::from(vec!["word"; 100]));
    let short = lists(100, 2);
    for values in [words, short] {
        let (kept, _) = measured(&keyed::<K>((0..ROWS).map(|row| row % 100), values));
        assert!(kept.steps <= 8 * steps(ROWS), "{kept:?}");
        assert_eq!(kept.remembered, 0);
    }
}

#[test]
fn a_dictionary_of_every_key_type_is_measured_in_its_rows_and_each_value_once() {
    every_key!(one_dear_value_is_measured_once_however_many_keys_name_it);
    every_key!(each_dear_value_named_is_measured_once);
    every_key!(values_measured_in_a_step_are_not_remembered);
}

fn a_null_key_names_no_value<K: ArrowDictionaryKeyType>() {
    const ROWS: usize = 100;
    let values = lists(1, 10_000);
    let keys = PrimitiveArray::<K>::from_iter((0..ROWS).map(|_| None::<K::Native>));
    let nulls: ArrayRef = Arc::new(DictionaryArray::<K>::try_new(keys, values).unwrap());
    let (kept, _) = measured(&nulls);
    assert!(kept.steps <= 2 * steps(ROWS) + 4, "{kept:?}");
    assert_eq!(kept.remembered, 0);
}

#[test]
fn null_keys_of_every_type_measure_no_value() {
    every_key!(a_null_key_names_no_value);
}

#[test]
fn a_value_nested_dictionaries_name_is_measured_once() {
    const ROWS: usize = 1_000;
    const OUTER: usize = 100;
    let dear = lists(1, 1_000);
    let each = alone(&dear);
    // Every value of the outer dictionary names the inner dictionary's one value.
    let inner = keyed::<Int8Type>(vec![0; OUTER], dear);
    let fields = Fields::from(vec![Field::new("inner", inner.data_type().clone(), true)]);
    let inside: ArrayRef = Arc::new(StructArray::new(fields, vec![Arc::clone(&inner)], None));
    for values in [inner, inside] {
        let column = keyed::<Int32Type>((0..ROWS).map(|row| row % OUTER), values);
        let (kept, forgotten) = measured(&column);
        assert!(kept.steps <= 16 * steps(ROWS) + each, "{kept:?}");
        // The list, and the first value that named it, which took the list's steps.
        assert_eq!(kept.remembered, 2, "{}", column.data_type());
        assert!(forgotten.steps >= steps(ROWS) * (each - 4), "{forgotten:?}");
    }
}

fn runs_of_keys_of_one_list<R: RunEndIndexType>() {
    const RUNS: usize = 100;
    const SPAN: usize = 50;
    let dear = lists(1, 10_000);
    let each = alone(&dear);
    let values = keyed::<Int8Type>(vec![0; RUNS], dear);
    let ends = (1..=RUNS).map(|run| R::Native::from_usize(run * SPAN).unwrap());
    let ends = PrimitiveArray::<R>::from_iter_values(ends);
    let column: ArrayRef = Arc::new(RunArray::<R>::try_new(&ends, values.as_ref()).unwrap());
    let (kept, forgotten) = measured(&column);
    // A step or so a run, whatever its rows, and the list once.
    assert!(kept.steps <= 8 * steps(RUNS) + each, "{kept:?}");
    // The list, and the first key that named it, which took the list's steps.
    assert_eq!(kept.remembered, 2);
    assert!(forgotten.steps >= steps(RUNS) * (each - 4), "{forgotten:?}");
    // Measured a row at a time, as a cut measures, the list is still measured once.
    let (batch, rendering) = (batch(column), native());
    let mut measure = rendering.measure(&batch, u64::MAX);
    for row in 0..RUNS * SPAN {
        measure.expanded(row..row + 1);
    }
    let (rows, remembered) = measure.work();
    assert!(rows <= 8 * steps(RUNS * SPAN) + each, "{rows}");
    assert_eq!(remembered, 2);
}

#[test]
fn a_run_end_column_of_a_dictionary_of_lists_is_measured_in_its_runs() {
    runs_of_keys_of_one_list::<Int16Type>();
    runs_of_keys_of_one_list::<Int32Type>();
    runs_of_keys_of_one_list::<Int64Type>();
}

#[test]
fn keys_into_one_run_measure_its_value_once() {
    const ROWS: usize = 600;
    let dear = lists(1, 1_000);
    let each = alone(&dear);
    // One run of six hundred over the list, and a key for each place in it.
    let ends = PrimitiveArray::<Int32Type>::from_iter_values([i32::try_from(ROWS).unwrap()]);
    let run: ArrayRef = Arc::new(RunArray::<Int32Type>::try_new(&ends, dear.as_ref()).unwrap());
    let column = keyed::<Int32Type>(0..ROWS, run);
    let (kept, forgotten) = measured(&column);
    assert!(kept.steps <= 16 * steps(ROWS) + each, "{kept:?}");
    // The list, and the first place in the run, which took the list's steps.
    assert_eq!(kept.remembered, 2);
    assert!(forgotten.steps >= steps(ROWS) * (each - 4), "{forgotten:?}");
}

#[test]
fn views_sharing_a_buffer_are_measured_a_step_each() {
    const ROWS: usize = 2_000;
    const BYTES: u32 = 100_000;
    let mut strings = StringViewBuilder::new();
    let block = strings.append_block(vec![b'x'; BYTES as usize].into());
    let mut bytes = BinaryViewBuilder::new();
    let binary = bytes.append_block(vec![7_u8; BYTES as usize].into());
    for _ in 0..ROWS {
        strings.try_append_view(block, 0, BYTES).unwrap();
        bytes.try_append_view(binary, 0, BYTES).unwrap();
    }
    for views in [
        Arc::new(strings.finish()) as ArrayRef,
        Arc::new(bytes.finish()),
    ] {
        let (kept, _) = measured(&views);
        assert!(kept.expanded >= steps(ROWS) * u64::from(BYTES));
        assert!(kept.steps <= steps(ROWS) + 4, "{kept:?}");
        assert_eq!(kept.remembered, 0);
    }
}

#[test]
fn views_read_for_their_escapes_are_read_no_further_than_the_limit() {
    const LIMIT: u64 = 1 << 20;
    // A list of two thousand views of one buffer: inside a list, each is read for its escapes.
    let column = view_lists(1, 2_000);
    let whole = work(&column, u64::MAX, false);
    assert!(whole.expanded >= 2_000 * 64);
    assert!(whole.steps <= 2 * 2_000 + 8, "{whole:?}");
    let mut long = StringViewBuilder::new();
    let block = long.append_block(vec![b'x'; 1 << 16].into());
    for _ in 0..100_000 {
        long.try_append_view(block, 0, 1 << 16).unwrap();
    }
    let long: ArrayRef = Arc::new(ListArray::new(
        item(DataType::Utf8View),
        OffsetBuffer::from_lengths([100_000]),
        Arc::new(long.finish()),
        None,
    ));
    let cut = work(&long, LIMIT, false);
    assert!(cut.expanded > LIMIT);
    // Seventeen views pass the limit: a step each, and one for each 64 bytes read.
    assert!(cut.steps <= 2 * LIMIT / 64 + 64, "{cut:?}");
}

fn list_views_of_one_child<O: OffsetSizeTrait>() {
    const ROWS: usize = 3_000;
    const LIMIT: u64 = 1 << 20;
    let view = |items: ArrayRef| -> ArrayRef {
        let size = O::from_usize(items.len()).unwrap();
        Arc::new(GenericListViewArray::<O>::new(
            item(items.data_type().clone()),
            ScalarBuffer::from(vec![O::from_usize(0).unwrap(); ROWS]),
            ScalarBuffer::from(vec![size; ROWS]),
            items,
            None,
        ))
    };
    // Every row names the same three thousand numbers: arithmetic a row.
    let numbers = view(Arc::new(Int64Array::from(vec![7; ROWS])));
    let (kept, _) = measured(&numbers);
    assert!(kept.expanded >= steps(ROWS * ROWS * 8));
    assert!(kept.steps <= 3 * steps(ROWS) + 4, "{kept:?}");
    assert_eq!(kept.remembered, 0);
    // Every row names the same strings, read for their escapes each time they are named, and
    // charged each time: reading stops with the limit.
    let words = view(Arc::new(StringArray::from(vec!["a \"quoted\" word"; ROWS])));
    let cut = work(&words, LIMIT, false);
    assert!(cut.expanded > LIMIT);
    assert!(cut.steps <= 2 * LIMIT / 64 + 64, "{cut:?}");
}

#[test]
fn list_views_naming_the_same_items_are_measured_within_the_limit() {
    list_views_of_one_child::<i32>();
    list_views_of_one_child::<i64>();
}

#[test]
fn a_null_list_spanning_items_is_measured_from_its_offsets() {
    const ITEMS: usize = 1_000_000;
    // One null row whose offsets span a million numbers.
    let spanning: ArrayRef = Arc::new(ListArray::new(
        item(DataType::Int64),
        OffsetBuffer::from_lengths([ITEMS]),
        Arc::new(Int64Array::from(vec![7; ITEMS])),
        Some(NullBuffer::new_null(1)),
    ));
    let (kept, _) = measured(&spanning);
    assert!(kept.steps <= 4, "{kept:?}");
    assert_eq!(kept.remembered, 0);
}

#[test]
fn runs_of_nulls_are_measured_a_step_a_run() {
    const RUNS: usize = 1_000;
    let ends = PrimitiveArray::<Int32Type>::from_iter_values(
        (1..=RUNS).map(|run| i32::try_from(run * 1_000).unwrap()),
    );
    let nulls = RunArray::<Int32Type>::try_new(&ends, &NullArray::new(RUNS)).unwrap();
    let words = StringArray::from(vec![None::<&str>; RUNS]);
    let words = RunArray::<Int32Type>::try_new(&ends, &words).unwrap();
    for column in [Arc::new(nulls) as ArrayRef, Arc::new(words)] {
        let (kept, _) = measured(&column);
        assert!(kept.steps <= 3 * steps(RUNS) + 4, "{kept:?}");
        assert_eq!(kept.remembered, 0);
    }
}

#[test]
fn a_key_is_measured_whatever_the_values_before_it() {
    // One row naming the last of thirty-two million nulls, which hold no bytes.
    const VALUES: usize = 32_000_000;
    let nulls: ArrayRef = Arc::new(NullArray::new(VALUES));
    let (kept, _) = measured(&keyed::<Int32Type>([VALUES - 1], nulls));
    assert!(kept.steps <= 4, "{kept:?}");
    assert_eq!(kept.remembered, 0);
    // One row naming the last place of a run of two billion over a list.
    let last = usize::try_from(i32::MAX).unwrap();
    let ends = PrimitiveArray::<Int32Type>::from_iter_values([i32::MAX]);
    let dear = lists(1, 10_000);
    let each = alone(&dear);
    let run: ArrayRef = Arc::new(RunArray::<Int32Type>::try_new(&ends, dear.as_ref()).unwrap());
    let (kept, _) = measured(&keyed::<Int32Type>([last - 1], run));
    assert!(kept.steps <= each + 16, "{kept:?}");
    assert_eq!(kept.remembered, 2);
}

#[test]
fn a_value_beyond_the_limit_is_measured_once_and_known_beyond() {
    const ROWS: usize = 1_000;
    // A thousand views, each a step: measuring half of them passes the limit below.
    let dear = view_lists(1, 1_000);
    let (each, bytes) = (alone(&dear), work(&dear, u64::MAX, true).expanded);
    let column = keyed::<Int32Type>(vec![0; ROWS], dear);
    let (batch, rendering) = (batch(column), native());
    // A limit the value alone exceeds: every row is beyond it, and is a piece of its own.
    let mut measure = rendering.measure(&batch, bytes / 2);
    let ends: Vec<usize> = measure.cuts().iter().map(|piece| piece.end).collect();
    assert_eq!(ends, (1..=ROWS).collect::<Vec<_>>());
    for row in 0..ROWS {
        assert!(measure.expanded(row..row + 1) > bytes / 2);
    }
    let (rows, remembered) = measure.work();
    assert_eq!(remembered, 1);
    // A few stretches a piece, each ending at the row found beyond.
    assert!(rows <= each + 64 * steps(ROWS), "{rows}");
    // A limit the value is within: what was remembered is its bytes, exactly.
    let mut within = rendering.measure(&batch, u64::MAX);
    let (one, two) = (within.expanded(0..1), within.expanded(0..2));
    assert!(one > bytes && two - one >= bytes, "{one} then {two}");
    assert_eq!(two, rendering.expanded(&batch, 0..2, u64::MAX));
}

proptest! {
    /// Remembering values changes nothing measured: within the limit the same bytes, and
    /// beyond it beyond.
    #[test]
    fn remembering_changes_nothing_measured(
        keys in proptest::collection::vec(0_usize..8, 1..200),
        items in 0_usize..40,
        limit in 1_u64..200_000,
        first in 0_usize..200,
    ) {
        let column = keyed::<Int16Type>(keys.iter().copied(), view_lists(8, items));
        let (batch, rendering) = (batch(column), native());
        let first = first.min(keys.len());
        let mut kept = rendering.measure(&batch, limit);
        let mut forgotten = rendering.measure(&batch, limit).forgetful();
        // Measured twice, so the second finds what the first remembered.
        for _ in 0..2 {
            let (kept, forgotten) = (
                kept.expanded(first..keys.len()),
                forgotten.expanded(first..keys.len()),
            );
            prop_assert_eq!(kept > limit, forgotten > limit);
            if kept <= limit {
                prop_assert_eq!(kept, forgotten);
            }
        }
        let exact = rendering.expanded(&batch, first..keys.len(), u64::MAX);
        let mut whole = rendering.measure(&batch, u64::MAX);
        prop_assert_eq!(whole.expanded(first..keys.len()), exact);
        prop_assert_eq!(whole.expanded(first..keys.len()), exact);
    }
}

/// `rows` list views each naming the same `items` views.
fn view_lists_named(rows: usize, items: usize) -> ArrayRef {
    let mut views = StringViewBuilder::new();
    for _ in 0..items {
        views.append_value("v");
    }
    Arc::new(GenericListViewArray::<i32>::new(
        item(DataType::Utf8View),
        ScalarBuffer::from(vec![0_i32; rows]),
        ScalarBuffer::from(vec![i32::try_from(items).unwrap(); rows]),
        Arc::new(views.finish()),
        None,
    ))
}

#[test]
fn cutting_a_batch_costs_about_one_measuring_of_its_rows_and_half_again() {
    // Rows of list views each naming the same 64 views: every item is a step each time named.
    const ROWS: usize = 20_000;
    let column = view_lists_named(ROWS, 64);
    let (batch, rendering) = (batch(column), native());
    let mut whole = rendering.measure(&batch, u64::MAX);
    let expanded = whole.expanded(0..ROWS);
    let (pass, _) = whole.work();
    assert!(pass >= steps(ROWS * 64));
    for pieces in [16, 105, 1_000] {
        let mut measure = rendering.measure(&batch, expanded / pieces);
        let cuts = measure.cuts();
        assert!(cuts.len() >= usize::try_from(pieces).unwrap());
        let (cutting, _) = measure.work();
        assert!(
            cutting <= 2 * pass,
            "{pieces} pieces took {cutting} steps, a pass {pass}"
        );
    }
}

#[test]
fn no_more_values_are_remembered_than_take_an_eighth_of_the_limit() {
    const VALUES: usize = 20_000;
    // Twenty thousand lists, each dear to measure and each named once.
    let column = keyed::<Int32Type>(0..VALUES, view_lists(VALUES, 20));
    let (batch, rendering) = (batch(column), native());
    let mut unbounded = rendering.measure(&batch, u64::MAX);
    let expanded = unbounded.expanded(0..VALUES);
    assert_eq!(unbounded.work().1, VALUES);
    // A limit of a mebibyte remembers an eighth of it in values of about 64 bytes each, and
    // measures the same.
    let mut small = rendering.measure(&batch, 1 << 20);
    assert!(small.expanded(0..VALUES) > 1 << 20);
    let mut cut = rendering.measure(&batch, 1 << 20);
    let pieces = cut.cuts();
    assert_eq!(pieces.last().map(|piece| piece.end), Some(VALUES));
    assert!(pieces.iter().map(|piece| piece.bytes).sum::<u64>() >= expanded);
    assert_eq!(cut.work().1, 2_048);
}
