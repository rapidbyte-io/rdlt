use std::sync::Arc;

use arrow_array::types::Int32Type;
use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, DictionaryArray, FixedSizeListArray, Int8Array,
    Int32Array, LargeBinaryArray, LargeStringArray, ListArray, ListViewArray, RecordBatch,
    RunArray, StringArray, StringViewArray, StructArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};
use proptest::prelude::*;
use rdlt_testkit::drawn::values;

use super::{Weigher, Weight};
use crate::codec::compact::compacted;
use crate::codec::tests::frames::sent;
use crate::codec::tests::odd;
use crate::codec::tests::samples::{self, batch_of};
use crate::limits::Limits;

/// What rows `start..start + rows` of `weigher`'s batch weigh as one piece.
fn piece(weigher: &mut Weigher, start: usize, rows: usize) -> Weight {
    let mut weight = Weight::default();
    weigher.begin();
    for row in start..start + rows {
        weight += weigher.weigh(row);
    }
    weight
}

/// What the receiver's walk counts in the frame of `batch`, and the frame's bytes.
fn walked(batch: &RecordBatch) -> (u64, u64, u64) {
    let (mut decoder, frames) = sent(batch, Limits::default());
    let mut last = None;
    for frame in &frames {
        last = Some(decoder.shaped(frame).unwrap().1);
    }
    let (shape, frame) = (last.unwrap(), &frames[frames.len() - 1]);
    let bytes = u64::try_from(frame.header.len() + frame.body.len()).unwrap();
    (shape.values, shape.view_bytes, bytes)
}

/// Checks every part of `batch` weighs what its frame holds once it holds only what its rows
/// name: its values and view bytes exactly, and its bytes within the frame's overhead.
fn weighs_as_it_crosses(name: &str, batch: &RecordBatch) -> Result<(), String> {
    let mut weigher = Weigher::new(batch);
    for start in 0..=batch.num_rows() {
        for rows in 1..=batch.num_rows() - start {
            let weight = piece(&mut weigher, start, rows);
            let compact = compacted(&batch.slice(start, rows)).unwrap();
            let (values, view_bytes, bytes) = walked(&compact);
            if (weight.values, weight.view_bytes) != (values, view_bytes) {
                return Err(format!(
                    "{name} {start}+{rows}: weighed {weight:?}, walked {values} values and \
                     {view_bytes} view bytes"
                ));
            }
            let most = weight.frame_bytes() + weigher.overhead();
            if bytes > most {
                return Err(format!(
                    "{name} {start}+{rows}: a frame of {bytes} bytes, weighed at most {most}"
                ));
            }
        }
    }
    Ok(())
}

#[test]
fn every_part_of_every_kind_of_column_weighs_what_its_frame_holds() {
    let mut columns: Vec<(String, ArrayRef)> = Vec::new();
    for column in samples::columns()
        .into_iter()
        .chain(samples::without_runs())
    {
        columns.push((column.data_type().to_string(), column));
    }
    for (name, column) in odd::columns() {
        columns.push((name.to_owned(), column));
    }
    for (name, column) in columns {
        weighs_as_it_crosses(&name, &batch_of(column)).unwrap();
    }
    weighs_as_it_crosses("every sample", &samples::batch()).unwrap();
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(256)))]

    #[test]
    fn every_part_of_a_drawn_batch_weighs_what_its_frame_holds(drawn in values::drawn()) {
        let batch = crate::codec::tests::batch(&drawn);
        let weighed = weighs_as_it_crosses("drawn", &batch);
        prop_assert!(weighed.is_ok(), "{:?}", weighed);
    }
}

#[test]
fn a_run_is_weighed_with_the_first_row_of_its_piece() {
    // Runs of two, one and two rows, of texts of 20, 30 and 40 bytes.
    let texts = ["a".repeat(20), "b".repeat(30), "c".repeat(40)];
    let values = StringViewArray::from_iter_values(texts.iter());
    let runs = RunArray::<Int32Type>::try_new(&Int32Array::from(vec![2, 3, 5]), &values);
    let batch = batch_of(Arc::new(runs.unwrap()));
    let mut weigher = Weigher::new(&batch);
    weigher.begin();
    let rows: Vec<_> = (0..5).map(|row| weigher.weigh(row)).collect();
    // The row, and where it begins a run, the run's end and its value.
    let values: Vec<_> = rows.iter().map(|row| row.values).collect();
    assert_eq!(values, [3, 1, 3, 3, 1]);
    let named: Vec<_> = rows.iter().map(|row| row.view_bytes).collect();
    assert_eq!(named, [20, 0, 30, 40, 0]);
    // A piece begun in the middle of a run holds that run.
    weigher.begin();
    assert_eq!(weigher.weigh(4).values, 3);
    assert_eq!(weigher.weigh(5), Weight::default());
    assert_eq!(weigher.rows(), 5);
}

#[test]
fn a_dictionary_key_is_weighed_in_its_frame_and_its_values_in_one_of_their_own() {
    let tags = StringArray::from(vec!["a tag of eighteen", "b"]);
    let keys = Int8Array::from(vec![Some(0), None, Some(1), Some(0)]);
    let batch = batch_of(Arc::new(
        DictionaryArray::try_new(keys, Arc::new(tags)).unwrap(),
    ));
    let mut weigher = Weigher::new(&batch);
    weigher.begin();
    // A key of a byte and, as the column holds a null, a validity bit, null or not.
    let rows: Vec<_> = (0..4).map(|row| weigher.weigh(row)).collect();
    assert!(
        rows.iter()
            .all(|row| (row.values, row.frame_bits) == (1, 9))
    );
    // Two values: an offset each, and eighteen bytes.
    let values = weigher.dictionaries();
    let values: Vec<_> = values.iter().map(|w| (w.values, w.frame_bits)).collect();
    assert_eq!(values, [(2, 2 * 32 + 8 * 18)]);
}

#[test]
fn a_stretch_of_rows_weighs_what_its_rows_weigh_one_at_a_time() {
    for (name, column) in crate::codec::tests::nested::columns() {
        let batch = batch_of(column);
        let (mut stretches, mut rows) = (Weigher::new(&batch), Weigher::new(&batch));
        for start in 0..=batch.num_rows() {
            for length in 0..=batch.num_rows() - start {
                stretches.begin();
                let stretch = stretches.weigh_rows(start..start + length);
                assert_eq!(
                    stretch,
                    piece(&mut rows, start, length),
                    "{name} {start}+{length}"
                );
                // Two stretches of one piece weigh what the piece does.
                stretches.begin();
                let mut halves = stretches.weigh_rows(start..start + length / 2);
                halves += stretches.weigh_rows(start + length / 2..start + length);
                assert_eq!(halves, stretch, "{name} {start}+{length} in halves");
            }
        }
    }
}

/// What every row of `column` weighs as one piece, the overhead of its frame, and what
/// weighing it looked at.
fn whole(column: ArrayRef) -> ((u64, u64, u64), u64, u64) {
    let batch = batch_of(column);
    let mut weigher = Weigher::new(&batch);
    weigher.begin();
    let weight = weigher.weigh_rows(0..batch.num_rows());
    let weight = (weight.values, weight.view_bytes, weight.frame_bits);
    (weight, weigher.overhead(), weigher.visits())
}

/// The overhead of a frame of `nodes` nodes and `buffers` buffers.
fn overhead(nodes: u64, buffers: u64) -> u64 {
    88 * buffers + 16 * nodes + 512
}

#[test]
fn each_layout_weighs_its_own_bits_and_counts_its_own_nodes_and_buffers() {
    use arrow_array::{NullArray, UnionArray};
    use arrow_schema::UnionFields;
    let item = Arc::new(Field::new("item", DataType::Int32, true));
    let ints = || -> ArrayRef { Arc::new(Int32Array::from(vec![1, 2, 3])) };
    // Three nulls: no bits, a node, no buffer.
    assert_eq!(
        whole(Arc::new(NullArray::new(3))),
        ((3, 0, 0), overhead(1, 0), 1)
    );
    // Three flags: a bit each, two buffers; three fixed bytes, three bytes.
    let flags = BooleanArray::from(vec![true, false, true]);
    assert_eq!(whole(Arc::new(flags)), ((3, 0, 3), overhead(1, 2), 1));
    let fixed = arrow_array::FixedSizeBinaryArray::try_from_iter([[7_u8; 3]].into_iter());
    let weighed = whole(Arc::new(fixed.unwrap()));
    assert_eq!(weighed, ((1, 0, 3 * 8), overhead(1, 2), 1));
    // A view of twelve bytes holds them: they are in no data buffer.
    let views = StringViewArray::from(vec!["t".repeat(12)]);
    assert_eq!(whole(Arc::new(views)), ((1, 0, 128), overhead(1, 3), 2));
    // A view of twenty bytes: sixteen bytes and its bytes; three buffers.
    let views = StringViewArray::from(vec!["t".repeat(20)]);
    let weighed = whole(Arc::new(views));
    assert_eq!(weighed, ((1, 20, 128 + 160), overhead(1, 3), 2));
    // Two lists of three integers between them: an offset each, and the integers.
    let offsets = OffsetBuffer::from_lengths([1, 2]);
    let lists = ListArray::new(Arc::clone(&item), offsets, ints(), None);
    let weighed = whole(Arc::new(lists));
    assert_eq!(weighed, ((2 + 3, 0, 2 * 32 + 3 * 32), overhead(2, 4), 2));
    // One list view naming two of them: an offset and a size.
    let (offsets, sizes) = (vec![1], vec![2]);
    let lists = ListViewArray::new(
        Arc::clone(&item),
        offsets.into(),
        sizes.into(),
        ints(),
        None,
    );
    let weighed = whole(Arc::new(lists));
    assert_eq!(weighed, ((1 + 2 + 2, 0, 64 + 2 * 32), overhead(2, 5), 3));
    // A fixed-size list and a struct: nothing of their own where no row is null, one buffer.
    let lists = FixedSizeListArray::new(item, 3, ints(), None);
    let weighed = whole(Arc::new(lists));
    assert_eq!(weighed, ((1 + 3, 0, 3 * 32), overhead(2, 3), 2));
    let field = Arc::new(Field::new("a", DataType::Int32, true));
    let parent = StructArray::from(vec![(field, ints())]);
    let weighed = whole(Arc::new(parent));
    assert_eq!(weighed, ((3 + 3, 0, 3 * 32), overhead(2, 3), 2));
    // A sparse union: a type id a row, one buffer.
    let fields = [Field::new("a", DataType::Int32, true)];
    let fields = UnionFields::try_new(vec![0], fields).unwrap();
    let union = UnionArray::try_new(fields, vec![0, 0, 0].into(), None, vec![ints()]).unwrap();
    let weighed = whole(Arc::new(union));
    assert_eq!(weighed, ((3 + 3, 0, 3 * 8 + 3 * 32), overhead(2, 3), 2));
    // A dense union: a type id and an offset a row, two buffers, and the item each names.
    let fields = [Field::new("a", DataType::Int32, true)];
    let fields = UnionFields::try_new(vec![0], fields).unwrap();
    let offsets = Some(vec![2, 0, 1].into());
    let union = UnionArray::try_new(fields, vec![0, 0, 0].into(), offsets, vec![ints()]).unwrap();
    let weighed = whole(Arc::new(union));
    assert_eq!(weighed, ((3 + 3, 0, 3 * 40 + 3 * 32), overhead(2, 4), 4));
    // Two runs of integers: each row, and for each run its end and its value; the ends
    // are a node of two buffers, the column itself none.
    let ends = Int32Array::from(vec![2, 5]);
    let runs = RunArray::<Int32Type>::try_new(&ends, &Int32Array::from(vec![7, 8])).unwrap();
    let weighed = whole(Arc::new(runs));
    assert_eq!(
        weighed,
        ((5 + 2 + 2, 0, 2 * 32 + 2 * 32), overhead(3, 4), 5)
    );
}

#[test]
fn the_values_of_a_dictionary_drop_what_a_null_list_spans_only_where_they_are_rebuilt() {
    // Two lists, the first null and spanning two items.
    let lists = |items: ArrayRef| -> ArrayRef {
        let field = Arc::new(Field::new("item", items.data_type().clone(), true));
        let offsets = OffsetBuffer::from_lengths([2, 1]);
        let nulls = NullBuffer::from(vec![false, true]);
        Arc::new(ListArray::new(field, offsets, items, Some(nulls)))
    };
    let values = |items: ArrayRef| {
        let keyed = DictionaryArray::try_new(Int8Array::from(vec![1]), lists(items)).unwrap();
        let batch = batch_of(Arc::new(keyed));
        let weights = Weigher::new(&batch).dictionaries();
        weights
            .iter()
            .map(|weight| weight.values)
            .collect::<Vec<_>>()
    };
    // Integers go as they are, the span with them; views are rebuilt, without it.
    assert_eq!(values(Arc::new(Int32Array::from(vec![1, 2, 3]))), [2 + 3]);
    assert_eq!(
        values(Arc::new(StringViewArray::from(vec!["a", "b", "c"]))),
        [2 + 1]
    );
}

#[test]
fn a_stretch_is_weighed_no_further_once_it_holds_more_values_than_asked() {
    let views: ArrayRef = Arc::new(StringViewArray::from(vec!["v"; 10]));
    let batch = batch_of(views);
    let mut weigher = Weigher::within(&batch, 4);
    weigher.begin();
    // The view that takes the stretch beyond four values is the last weighed.
    assert_eq!(weigher.weigh_rows(0..10).values, 5);
}

#[test]
fn rows_weighed_since_a_mark_are_forgotten_and_those_before_it_kept() {
    let ends = Int32Array::from(vec![2, 4]);
    let runs = RunArray::<Int32Type>::try_new(&ends, &Int32Array::from(vec![7, 8])).unwrap();
    let batch = batch_of(Arc::new(runs));
    let mut weigher = Weigher::new(&batch);
    weigher.begin();
    assert_eq!(weigher.weigh(0).values, 3);
    weigher.mark();
    assert_eq!(weigher.weigh_rows(1..4).values, 3 + 2);
    weigher.rewind();
    // The first run was begun before the mark; the second is begun again.
    assert_eq!(weigher.weigh(1).values, 1);
    assert_eq!(weigher.weigh(2).values, 3);
}

#[test]
fn runs_are_weighed_no_further_once_a_stretch_holds_more_values_than_asked() {
    // Ten runs of one row: each run is its row, its end and its value.
    let ends = Int32Array::from_iter_values(1..=10);
    let runs = RunArray::<Int32Type>::try_new(&ends, &Int32Array::from(vec![7; 10])).unwrap();
    let batch = batch_of(Arc::new(runs));
    let mut weigher = Weigher::within(&batch, 4);
    weigher.begin();
    // The run that takes the stretch beyond four values is the last weighed.
    assert_eq!(weigher.weigh_rows(0..10).values, 2 * 3);
}

#[test]
fn lists_with_no_null_are_weighed_as_one_stretch_whatever_their_validity_buffer() {
    use arrow_array::ListArray;
    use arrow_buffer::{NullBuffer, OffsetBuffer};
    use arrow_schema::Field;
    // Two lists of views, rebuilt where they go, each valid by a buffer saying so.
    let views: ArrayRef = Arc::new(StringViewArray::from(vec!["a", "b", "c"]));
    let field = Arc::new(Field::new("item", views.data_type().clone(), true));
    let offsets = OffsetBuffer::from_lengths([1, 2]);
    let valid = NullBuffer::new_valid(2);
    let lists = ListArray::new(field, offsets, views, Some(valid));
    // The lists, then their items in one stretch, each view looked at.
    let (weight, _, visits) = whole(Arc::new(lists));
    assert_eq!((weight.0, visits), (2 + 3, 1 + 1 + 3));
}

#[test]
fn a_weigher_shows_the_layout_of_each_column_it_weighs() {
    let views: ArrayRef = Arc::new(StringViewArray::from(vec!["a"]));
    let tags: ArrayRef = Arc::new(StringArray::from(vec!["t"]));
    let keyed: ArrayRef =
        Arc::new(DictionaryArray::try_new(Int8Array::from(vec![0]), tags).unwrap());
    let batch = RecordBatch::try_from_iter([("v", views), ("k", keyed)]).unwrap();
    let shown = format!("{:?}", Weigher::new(&batch));
    assert!(
        shown.contains("columns: [views, dictionary keys]"),
        "{shown}"
    );
}

/// What every row of a batch of `array` weighs in bits.
fn bits(array: ArrayRef) -> u64 {
    let batch = batch_of(array);
    piece(&mut Weigher::new(&batch), 0, batch.num_rows()).frame_bits
}

/// Two rows, the second null.
fn second_null() -> NullBuffer {
    NullBuffer::from(vec![true, false])
}

#[test]
fn a_leaf_column_weighs_a_validity_bit_a_value_only_where_it_holds_a_null() {
    let ints = |values: &[Option<i32>]| Arc::new(Int32Array::from(values.to_vec())) as ArrayRef;
    assert_eq!(bits(ints(&[Some(1), Some(2)])), 2 * 32);
    assert_eq!(bits(ints(&[Some(1), None])), 2 * 33);
    let flags = |values: &[Option<bool>]| Arc::new(BooleanArray::from(values.to_vec())) as ArrayRef;
    assert_eq!(bits(flags(&[Some(true), Some(true)])), 2);
    assert_eq!(bits(flags(&[Some(true), None])), 2 * 2);
    let strings = |values: Vec<Option<&str>>| Arc::new(StringArray::from(values)) as ArrayRef;
    assert_eq!(bits(strings(vec![Some("a"), Some("b")])), 2 * 32 + 2 * 8);
    assert_eq!(bits(strings(vec![Some("a"), None])), 2 * 33 + 8);
    let large = |values: Vec<Option<&str>>| Arc::new(LargeStringArray::from(values)) as ArrayRef;
    assert_eq!(bits(large(vec![Some("a"), Some("b")])), 2 * 64 + 2 * 8);
    assert_eq!(bits(large(vec![Some("a"), None])), 2 * 65 + 8);
    let binary = |values: Vec<Option<&[u8]>>| Arc::new(BinaryArray::from(values)) as ArrayRef;
    assert_eq!(bits(binary(vec![Some(b"a"), Some(b"b")])), 2 * 32 + 2 * 8);
    assert_eq!(bits(binary(vec![Some(b"a"), None])), 2 * 33 + 8);
    let large_binary =
        |values: Vec<Option<&[u8]>>| Arc::new(LargeBinaryArray::from(values)) as ArrayRef;
    assert_eq!(
        bits(large_binary(vec![Some(b"a"), Some(b"b")])),
        2 * 64 + 2 * 8
    );
    assert_eq!(bits(large_binary(vec![Some(b"a"), None])), 2 * 65 + 8);
    let views = |values: Vec<Option<&str>>| Arc::new(StringViewArray::from(values)) as ArrayRef;
    assert_eq!(bits(views(vec![Some("a"), Some("b")])), 2 * 128);
    assert_eq!(bits(views(vec![Some("a"), None])), 2 * 129);
}

#[test]
fn a_nested_column_weighs_a_validity_bit_a_row_only_where_it_holds_a_null() {
    let items = || Arc::new(Field::new("item", DataType::Int32, false));
    let two = || Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef;
    let sized = |nulls| Arc::new(FixedSizeListArray::new(items(), 1, two(), nulls)) as ArrayRef;
    assert_eq!(bits(sized(None)), 2 * 32);
    assert_eq!(bits(sized(Some(second_null()))), 2 * 33);
    let lists = |nulls| {
        let offsets = OffsetBuffer::new(vec![0, 1, 2].into());
        Arc::new(ListArray::new(items(), offsets, two(), nulls)) as ArrayRef
    };
    assert_eq!(bits(lists(None)), 2 * 32 + 2 * 32);
    // A null list still spans its item, as a list's items are not rebuilt.
    assert_eq!(bits(lists(Some(second_null()))), 2 * 33 + 2 * 32);
    let list_views = |nulls| {
        let (offsets, lengths) = (vec![0, 1].into(), vec![1, 1].into());
        Arc::new(ListViewArray::new(items(), offsets, lengths, two(), nulls)) as ArrayRef
    };
    assert_eq!(bits(list_views(None)), 2 * 64 + 2 * 32);
    // A null list view names nothing.
    assert_eq!(bits(list_views(Some(second_null()))), 2 * 65 + 32);
    let structs = |nulls| {
        let field = Arc::new(Field::new("a", DataType::Int32, true));
        Arc::new(StructArray::new(vec![field].into(), vec![two()], nulls)) as ArrayRef
    };
    assert_eq!(bits(structs(None)), 2 * 32);
    assert_eq!(bits(structs(Some(second_null()))), 2 * 33);
}

#[test]
fn a_keyed_column_weighs_a_validity_bit_by_its_keys_and_a_run_by_its_values_alone() {
    let keyed = |keys: Vec<Option<i32>>| {
        let values = Arc::new(Int8Array::from(vec![7])) as ArrayRef;
        Arc::new(DictionaryArray::new(Int32Array::from(keys), values)) as ArrayRef
    };
    assert_eq!(bits(keyed(vec![Some(0), Some(0)])), 2 * 32);
    assert_eq!(bits(keyed(vec![Some(0), None])), 2 * 33);
    // A run's end is never null: its ends weigh their width whether its values hold a null or not.
    let runs = |value: Option<i32>| {
        let (ends, values) = (Int32Array::from(vec![4]), Int32Array::from(vec![value]));
        Arc::new(RunArray::try_new(&ends, &values).unwrap()) as ArrayRef
    };
    assert_eq!(bits(runs(Some(1))), 32 + 32);
    assert_eq!(bits(runs(None)), 32 + 33);
}
