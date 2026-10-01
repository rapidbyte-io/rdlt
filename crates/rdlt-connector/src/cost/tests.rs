use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::builder::{BinaryViewBuilder, StringViewBuilder};
use arrow_array::types::{Int8Type, Int16Type, Int32Type, Int64Type};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal32Array, Decimal64Array, Decimal128Array,
    Decimal256Array, DictionaryArray, DurationSecondArray, FixedSizeBinaryArray,
    FixedSizeListArray, Float32Array, Float64Array, Int8Array, Int32Array, Int64Array,
    IntervalYearMonthArray, LargeBinaryArray, LargeListArray, LargeListViewArray, LargeStringArray,
    ListArray, ListViewArray, MapArray, NullArray, RecordBatch, RunArray, StringArray, StructArray,
    Time32SecondArray, Time64NanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array,
    UInt32Array, UInt64Array, UnionArray, new_null_array,
};
use arrow_buffer::{Buffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Fields, IntervalUnit, UnionFields};
use proptest::prelude::*;

use super::{Allocations, Rendering, admit, nulls};
use crate::types::TypeKind;

/// A destination storing every scalar as it is.
fn native() -> Rendering {
    Rendering::new([
        TypeKind::Null,
        TypeKind::Bool,
        TypeKind::Int8,
        TypeKind::Int16,
        TypeKind::Int32,
        TypeKind::Int64,
        TypeKind::Float32,
        TypeKind::Float64,
        TypeKind::Decimal,
        TypeKind::Utf8,
        TypeKind::Binary,
        TypeKind::Date,
        TypeKind::Time,
        TypeKind::Timestamp,
        TypeKind::Duration,
        TypeKind::Uuid,
        TypeKind::Json,
    ])
}

fn batch(column: ArrayRef) -> RecordBatch {
    RecordBatch::try_from_iter([("column", column)]).unwrap()
}

fn expanded(rendering: &Rendering, column: &ArrayRef) -> u64 {
    rendering.expanded_array(column.as_ref(), 0..column.len(), u64::MAX)
}

fn item(data_type: DataType) -> Arc<Field> {
    Arc::new(Field::new("item", data_type, true))
}

#[test]
fn a_plain_column_costs_what_its_rows_take() {
    const ROWS: usize = 10_000;
    let ids: ArrayRef = Arc::new(Int64Array::from(vec![7; ROWS]));
    assert_eq!(expanded(&native(), &ids), u64::try_from(8 * ROWS).unwrap());
    assert_eq!(expanded(&native(), &ids.slice(0, 16)), 8 * 16);
    // A column holding nulls has a bit of validity a row beside.
    let some: ArrayRef = Arc::new(Int64Array::from(vec![Some(7), None, Some(9)]));
    assert_eq!(expanded(&native(), &some), 3 * 8 + 1);
    let words: ArrayRef = Arc::new(StringArray::from(vec!["four"; ROWS]));
    let cost = expanded(&native(), &words);
    assert!(cost >= (4 * ROWS + 4 * ROWS) as u64, "{cost}");
    assert!(cost <= (4 * ROWS + 9 * ROWS + 8) as u64, "{cost}");
}

#[test]
fn a_scalar_a_destination_stores_as_text_costs_its_text() {
    const ROWS: usize = 1_000;
    let ids: ArrayRef = Arc::new(Int64Array::from(vec![7; ROWS]));
    let text = expanded(&Rendering::text(), &ids);
    assert!(text >= 20 * u64::try_from(ROWS).unwrap(), "{text}");
    // A scalar inside a nested value is rendered whatever the destination stores.
    let nested: ArrayRef = Arc::new(ListArray::new(
        item(DataType::Int64),
        OffsetBuffer::from_lengths([ROWS]),
        ids,
        None,
    ));
    assert!(expanded(&native(), &nested) >= 20 * u64::try_from(ROWS).unwrap());
}

#[test]
fn a_nested_string_costs_its_escapes() {
    let plain: ArrayRef = Arc::new(StringArray::from(vec!["abcdefgh"; 100]));
    let controls: ArrayRef = Arc::new(StringArray::from(vec!["\u{1}\u{2}\u{3}\u{4}\"\\\n\t"; 100]));
    let list = |values: ArrayRef| -> ArrayRef {
        Arc::new(ListArray::new(
            item(DataType::Utf8),
            OffsetBuffer::from_lengths([values.len()]),
            values,
            None,
        ))
    };
    let (plain, controls) = (list(plain), list(controls));
    // Four characters of six bytes each and four of two, where the plain ones take one.
    let extra = expanded(&native(), &controls) - expanded(&native(), &plain);
    assert_eq!(extra, 100 * (4 * 5 + 4));
}

/// One array of every Arrow type whose values all take the same bytes, and of each string and
/// bytes type, four rows each.
fn flat() -> Vec<ArrayRef> {
    vec![
        Arc::new(NullArray::new(4)),
        Arc::new(BooleanArray::from(vec![true; 4])),
        Arc::new(Int8Array::from(vec![1; 4])),
        Arc::new(arrow_array::Int16Array::from(vec![1; 4])),
        Arc::new(Int32Array::from(vec![1; 4])),
        Arc::new(Int64Array::from(vec![1; 4])),
        Arc::new(UInt8Array::from(vec![1; 4])),
        Arc::new(UInt16Array::from(vec![1; 4])),
        Arc::new(UInt32Array::from(vec![1; 4])),
        Arc::new(UInt64Array::from(vec![1; 4])),
        new_null_array(&DataType::Float16, 4),
        Arc::new(Float32Array::from(vec![1.0; 4])),
        Arc::new(Float64Array::from(vec![1.0; 4])),
        Arc::new(Decimal32Array::from(vec![1; 4])),
        Arc::new(Decimal64Array::from(vec![1; 4])),
        Arc::new(Decimal128Array::from(vec![1; 4])),
        Arc::new(Decimal256Array::from(vec![arrow_buffer::i256::ONE; 4])),
        Arc::new(Date32Array::from(vec![1; 4])),
        Arc::new(arrow_array::Date64Array::from(vec![1; 4])),
        Arc::new(Time32SecondArray::from(vec![1; 4])),
        Arc::new(Time64NanosecondArray::from(vec![1; 4])),
        Arc::new(TimestampSecondArray::from(vec![1; 4])),
        Arc::new(DurationSecondArray::from(vec![1; 4])),
        Arc::new(IntervalYearMonthArray::from(vec![1; 4])),
        new_null_array(&DataType::Interval(IntervalUnit::DayTime), 4),
        new_null_array(&DataType::Interval(IntervalUnit::MonthDayNano), 4),
        Arc::new(FixedSizeBinaryArray::try_from_iter([[1_u8; 3]; 4].into_iter()).unwrap()),
        Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
        Arc::new(LargeStringArray::from(vec!["a"; 4])),
        Arc::new(arrow_array::StringViewArray::from(vec!["a"; 4])),
        Arc::new(arrow_array::BinaryArray::from(vec![b"a".as_slice(); 4])),
        Arc::new(LargeBinaryArray::from(vec![b"a".as_slice(); 4])),
        Arc::new(arrow_array::BinaryViewArray::from(vec![b"a".as_slice(); 4])),
    ]
}

/// A map of one entry a row: its key from `keys`, its value from `values`.
fn map(keys: &ArrayRef, values: &ArrayRef) -> ArrayRef {
    let entries = StructArray::new(
        Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int32, true),
        ]),
        vec![Arc::clone(keys), Arc::clone(values)],
        None,
    );
    Arc::new(MapArray::new(
        Arc::new(Field::new("entries", entries.data_type().clone(), false)),
        OffsetBuffer::from_lengths([1; 4]),
        entries,
        None,
        false,
    ))
}

/// One array of every nested and encoded Arrow type, four rows each.
fn nested_types() -> Vec<ArrayRef> {
    let ints: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));
    let words: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c", "d"]));
    let fields = Fields::from(vec![Field::new("a", DataType::Int32, true)]);
    let members = UnionFields::try_new(
        [0, 1],
        [
            Field::new("i", DataType::Int32, true),
            Field::new("s", DataType::Utf8, true),
        ],
    )
    .unwrap();
    let union = |offsets: Option<ScalarBuffer<i32>>| -> ArrayRef {
        let ids = ScalarBuffer::from(vec![0_i8, 1, 0, 1]);
        let children = vec![Arc::clone(&ints), Arc::clone(&words)];
        Arc::new(UnionArray::try_new(members.clone(), ids, offsets, children).unwrap())
    };
    let ones = || OffsetBuffer::from_lengths([1; 4]);
    let item = || item(DataType::Int32);
    let (zeros, fours) = (vec![0_i32; 4], vec![4_i32; 4]);
    let (large_zeros, large_fours) = (vec![0_i64; 4], vec![4_i64; 4]);
    let runs = arrow_array::Int16Array::from(vec![2, 4]);
    vec![
        Arc::new(ListArray::new(item(), ones(), Arc::clone(&ints), None)),
        Arc::new(LargeListArray::new(
            item(),
            OffsetBuffer::from_lengths([1; 4]),
            Arc::clone(&ints),
            None,
        )),
        Arc::new(ListViewArray::new(
            item(),
            zeros.into(),
            fours.into(),
            Arc::clone(&ints),
            None,
        )),
        Arc::new(LargeListViewArray::new(
            item(),
            large_zeros.into(),
            large_fours.into(),
            Arc::clone(&ints),
            None,
        )),
        Arc::new(FixedSizeListArray::new(item(), 1, Arc::clone(&ints), None)),
        Arc::new(StructArray::new(fields, vec![Arc::clone(&ints)], None)),
        map(&words, &ints),
        union(None),
        union(Some(ScalarBuffer::from(vec![0_i32, 0, 1, 1]))),
        Arc::new(
            DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![0, 1, 2, 3]),
                Arc::clone(&words),
            )
            .unwrap(),
        ),
        Arc::new(RunArray::<Int16Type>::try_new(&runs, &words.slice(0, 2)).unwrap()),
    ]
}

/// One array of every Arrow type the wire admits, four rows each.
fn every_type() -> Vec<ArrayRef> {
    flat().into_iter().chain(nested_types()).collect()
}

#[test]
fn every_type_costs_at_least_a_byte_a_row_and_grows_with_its_rows() {
    for rendering in [native(), Rendering::text()] {
        for column in every_type() {
            let whole = expanded(&rendering, &column);
            assert!(whole >= 4, "{}", column.data_type());
            let mut running = 0;
            for end in 1..=column.len() {
                let prefix = rendering.expanded_array(column.as_ref(), 0..end, u64::MAX);
                assert!(prefix > running, "{}", column.data_type());
                running = prefix;
            }
            // A range beyond the rows costs what the rows do.
            assert_eq!(
                rendering.expanded_array(column.as_ref(), 0..column.len() + 9, u64::MAX),
                whole
            );
        }
    }
}

#[test]
fn every_type_has_a_null_slot() {
    for column in every_type() {
        let rows = 64;
        let built = new_null_array(column.data_type(), rows);
        let bytes = built.to_data().get_slice_memory_size().unwrap_or(0);
        assert!(
            nulls(column.data_type(), rows) >= u64::try_from(bytes).unwrap(),
            "{}: {bytes}",
            column.data_type()
        );
    }
}

#[test]
fn a_null_key_costs_its_values_slot_whatever_the_dictionary_holds() {
    const WIDTH: i32 = 1_000_000;
    let values = new_null_array(&DataType::FixedSizeBinary(WIDTH), 0);
    let keys = Int8Array::from(vec![None::<i8>; 300]);
    let nulls: ArrayRef = Arc::new(DictionaryArray::<Int8Type>::try_new(keys, values).unwrap());
    assert!(expanded(&native(), &nulls) >= 300 * u64::try_from(WIDTH).unwrap());
}

#[test]
fn views_cost_the_bytes_they_name_however_many_share_them() {
    let mut views = BinaryViewBuilder::new();
    let block = views.append_block(vec![7_u8; 100_000].into());
    for _ in 0..2_000 {
        views.try_append_view(block, 0, 100_000).unwrap();
    }
    let views: ArrayRef = Arc::new(views.finish());
    assert!(expanded(&native(), &views) >= 200_000_000);
    let mut strings = StringViewBuilder::new();
    let block = strings.append_block(vec![b'x'; 100_000].into());
    for _ in 0..2_000 {
        strings.try_append_view(block, 0, 100_000).unwrap();
    }
    let strings: ArrayRef = Arc::new(strings.finish());
    assert!(expanded(&native(), &strings) >= 200_000_000);
}

#[test]
fn list_views_cost_what_each_row_names() {
    const ROWS: usize = 3_000;
    let items: ArrayRef = Arc::new(Int64Array::from_iter_values(
        0..i64::try_from(ROWS).unwrap(),
    ));
    let small: ArrayRef = Arc::new(ListViewArray::new(
        item(DataType::Int64),
        ScalarBuffer::from(vec![0_i32; ROWS]),
        ScalarBuffer::from(vec![i32::try_from(ROWS).unwrap(); ROWS]),
        items.clone(),
        None,
    ));
    let large: ArrayRef = Arc::new(LargeListViewArray::new(
        item(DataType::Int64),
        ScalarBuffer::from(vec![0_i64; ROWS]),
        ScalarBuffer::from(vec![i64::try_from(ROWS).unwrap(); ROWS]),
        items,
        None,
    ));
    for views in [small, large] {
        assert!(expanded(&native(), &views) >= u64::try_from(ROWS * ROWS * 8).unwrap());
    }
}

/// A dictionary of one list of `items` views of `bytes` each, keyed by every one of `rows`.
fn multiplied(rows: usize, items: usize, bytes: usize) -> ArrayRef {
    let mut views = BinaryViewBuilder::new();
    let block = views.append_block(vec![7_u8; bytes].into());
    for _ in 0..items {
        views
            .try_append_view(block, 0, u32::try_from(bytes).unwrap())
            .unwrap();
    }
    let list = ListArray::new(
        item(DataType::BinaryView),
        OffsetBuffer::from_lengths([items]),
        Arc::new(views.finish()),
        None,
    );
    Arc::new(
        DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![0; rows]), Arc::new(list))
            .unwrap(),
    )
}

#[test]
fn measuring_stops_at_its_limit_however_an_encoding_multiplies() {
    // A million rows each naming a list of a hundred thousand views: 10^11 views to visit.
    let column = multiplied(1_000_000, 100_000, 1_000);
    let started = Instant::now();
    let cost = native().expanded_array(column.as_ref(), 0..column.len(), 1 << 20);
    assert!(cost > 1 << 20);
    let cuts = native().cuts(&batch(Arc::clone(&column)), 1 << 20);
    assert_eq!(cuts.len(), 1_000_000);
    assert!(started.elapsed() < Duration::from_secs(60));
}

#[test]
fn a_batch_within_the_maximum_is_one_piece() {
    let ids = batch(Arc::new(Int64Array::from(vec![7; 1_000])));
    assert_eq!(native().cuts(&ids, 1 << 20), [1_000]);
    assert_eq!(native().cuts(&ids.slice(0, 0), 1 << 20), [0]);
}

#[test]
fn cuts_fall_where_the_rows_own_values_say() {
    // Rows 0..100 hold a small value, rows 100..110 one of 10 KB, the rest a small one.
    let large = "x".repeat(10_000);
    let values = StringArray::from(vec!["small", large.as_str()]);
    let keys: Vec<i32> = (0..1_000)
        .map(|row| i32::from((100..110).contains(&row)))
        .collect();
    let skewed: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(Int32Array::from(keys), Arc::new(values)).unwrap(),
    );
    let cuts = native().cuts(&batch(skewed), 12_000);
    // The small rows before fit one piece with the first large value at most; each large value
    // then takes a piece of its own.
    assert!(cuts[0] <= 101, "{cuts:?}");
    assert!(cuts.len() >= 10, "{cuts:?}");
    assert_eq!(cuts.last(), Some(&1_000));
}

proptest! {
    /// Pieces cover the rows in order, and each fits the maximum or is one row.
    #[test]
    fn every_piece_fits_or_is_one_row(
        lengths in proptest::collection::vec(0_usize..200, 1..60),
        max in 1_u64..2_000,
    ) {
        let words: Vec<String> = lengths.iter().map(|length| "w".repeat(*length)).collect();
        let column: ArrayRef = Arc::new(StringArray::from(words));
        let batch = batch(column);
        let rendering = native();
        let cuts = rendering.cuts(&batch, max);
        let mut first = 0;
        for end in &cuts {
            prop_assert!(*end > first || batch.num_rows() == 0);
            let cost = rendering.expanded(&batch, first..*end, u64::MAX);
            prop_assert!(cost <= max || end - first == 1, "{first}..{end} costs {cost}");
            // A piece ends only where one more row would not fit.
            if *end < batch.num_rows() && end - first > 1 {
                prop_assert!(rendering.expanded(&batch, first..end + 1, u64::MAX) > max);
            }
            first = *end;
        }
        prop_assert_eq!(first, batch.num_rows());
    }
}

#[test]
fn a_batch_holds_every_allocation_it_pins_once() {
    let values = Buffer::from_vec(vec![0_i64; 1 << 20]);
    let bytes = values.capacity() as u64;
    let whole = Int64Array::new(ScalarBuffer::new(values, 0, 1 << 20), None);
    let slice: ArrayRef = Arc::new(whole.slice(5, 1));
    // One row keeps the whole buffer alive, and two columns sharing it hold it once.
    assert_eq!(Allocations::of_array(slice.as_ref()).bytes(), bytes);
    let shared = RecordBatch::try_from_iter([("a", Arc::clone(&slice)), ("b", slice)]).unwrap();
    assert_eq!(Allocations::of(&shared).bytes(), bytes);
    assert_eq!(native().cost(&shared, u64::MAX).held, bytes);
    assert!(native().cost(&shared, u64::MAX).charge() >= bytes);
}

#[test]
fn a_dictionary_column_holds_values_no_key_names() {
    let large = "x".repeat(1 << 20);
    let values = StringArray::from(vec![large.as_str(), "y"]);
    let keyed: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![1]), Arc::new(values)).unwrap(),
    );
    let cost = native().cost(&batch(keyed), u64::MAX);
    assert!(cost.expanded < 100, "{cost:?}");
    assert!(cost.held >= 1 << 20, "{cost:?}");
    assert_eq!(cost.charge(), cost.held);
}

#[test]
fn allocations_count_only_what_a_set_did_not_hold() {
    let first: ArrayRef = Arc::new(Int64Array::from(vec![1; 1_000]));
    let second: ArrayRef = Arc::new(Int64Array::from(vec![2; 1_000]));
    let mut held = Allocations::of_array(first.as_ref());
    let before = held.bytes();
    assert_eq!(held.add_array(first.slice(3, 4).as_ref()), 0);
    let added = held.add_array(second.as_ref());
    assert!(added >= 8_000);
    assert_eq!(held.bytes(), before + added);
    assert_eq!(held.add(&batch(second)), 0);
}

#[test]
fn an_allocation_arrow_did_not_make_counts_whole() {
    let owner = bytes::Bytes::from(vec![0_u8; 4_096]);
    let column: ArrayRef = Arc::new(Int8Array::new(
        ScalarBuffer::new(Buffer::from(owner), 4_000, 96),
        None,
    ));
    assert_eq!(Allocations::of_array(column.as_ref()).bytes(), 4_096);
}

/// A batch of one column nested `levels` deep, a top-level column being the first level.
fn nested(levels: usize) -> RecordBatch {
    let mut inner: ArrayRef = Arc::new(Int64Array::from(vec![1_i64]));
    for _ in 1..levels {
        let fields = Fields::from(vec![Field::new("a", inner.data_type().clone(), true)]);
        inner = Arc::new(StructArray::new(fields, vec![inner], None));
    }
    batch(inner)
}

fn refused(batch: &RecordBatch) -> (&'static str, u64) {
    let refusal = admit(batch).unwrap_err();
    assert!(refusal.actual > refusal.limit);
    (refusal.name, refusal.limit)
}

#[test]
fn a_batch_nested_beyond_the_limit_is_refused_however_deep() {
    admit(&nested(64)).unwrap();
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
        admit(&batch(column)).unwrap();
    }
}

#[test]
fn a_batch_nested_beyond_any_the_limits_admit_costs_more_than_any_limit() {
    // Deeper than the meter follows: a batch no limit admits is not measured to its end.
    let deep = nested(300);
    let column = Arc::clone(deep.column(0));
    assert_eq!(expanded(&native(), &column), u64::MAX);
    // One the limits admit is measured whole.
    let admitted = nested(64);
    let column = Arc::clone(admitted.column(0));
    assert!(expanded(&native(), &column) < 10_000);
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
    admit(&wide(10_000, &keyed)).unwrap();
    // A run-end encoded column is three: itself, its run ends and its values.
    let runs = |_| {
        let ends = arrow_array::Int16Array::from(vec![1]);
        Arc::new(RunArray::<Int16Type>::try_new(&ends, &words).unwrap()) as ArrayRef
    };
    admit(&wide(3_333, &runs)).unwrap();
    assert_eq!(refused(&wide(3_334, &runs)), ("batch columns", 10_000));
    // Its values count toward the batch's, beside its rows and its runs.
    let long = RunArray::<Int32Type>::try_new(&Int32Array::from(vec![1 << 20]), &NullArray::new(1))
        .unwrap();
    admit(&batch(Arc::new(long))).unwrap();
}
