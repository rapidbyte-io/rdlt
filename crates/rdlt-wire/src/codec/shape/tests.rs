use std::sync::Arc;

use arrow_array::types::Int32Type;
use arrow_array::{
    Array as _, ArrayRef, BinaryViewArray, Int32Array, ListArray, ListViewArray, NullArray,
    RecordBatch, RecordBatchOptions, RunArray, StructArray, new_null_array,
};
use arrow_buffer::{Buffer, OffsetBuffer, ScalarBuffer};
use arrow_ipc::MetadataVersion;
use arrow_schema::{DataType, Field, Fields, Schema};
use bytes::Bytes;

use super::views::{listed, named};
use crate::codec::tests::frames::{Parts, changed, decoder, problem, refusal, refused, sent};
use crate::codec::tests::samples::{self, ROWS, batch_of};
use crate::codec::{Decoder, Encoder, IpcFrame};
use crate::error::{Frame, Part, Problem, WireError};
use crate::limits::{BATCH_ROWS, BATCH_VALUES, FRAME_BYTES, Limits};

/// A one-row frame of `columns` `Int64` columns whose data buffers all lie at `offsets`, in
/// turn, each reaching the end of one body of `body` bytes.
fn sharing(columns: usize, offsets: [i64; 2], body: i64) -> IpcFrame {
    let mut parts = Parts::batch(1, body);
    for column in 0..columns {
        let offset = offsets[column % 2];
        parts.nodes.push((1, 0));
        parts.buffers.extend([(0, 0), (offset, body - offset)]);
    }
    IpcFrame {
        header: parts.header(),
        body: Bytes::from(vec![0; usize::try_from(body).unwrap()]),
    }
}

#[test]
fn columns_sharing_one_buffer_are_refused() {
    let columns = 64;
    let fields: Vec<_> = (0..columns)
        .map(|column| Field::new(format!("c{column}"), DataType::Int64, false))
        .collect();
    let mut decoder = decoder(&Schema::new(fields), Limits::default());
    // Misaligned for the column's type wherever the transport put the body.
    assert_eq!(
        problem(decoder.frame(&sharing(columns, [1, 2], 1 << 20))),
        Problem::BufferUnaligned {
            index: 1,
            offset: 1
        }
    );
    assert_eq!(
        problem(decoder.frame(&sharing(columns, [8, 8], 1 << 20))),
        Problem::BufferOverlaps {
            index: 2,
            offset: 0,
            end: 1 << 20
        }
    );
}

#[test]
fn one_row_of_lists_of_nulls_beyond_the_value_limit_is_refused() {
    let items = 1_000_000;
    let item = Arc::new(Field::new("item", DataType::Null, true));
    let list: ArrayRef = Arc::new(ListArray::new(
        item,
        OffsetBuffer::from_lengths([items]),
        Arc::new(NullArray::new(items)),
        None,
    ));
    let columns = (0..4_990).map(|column| (format!("c{column}"), Arc::clone(&list)));
    let (mut decoder, frames) = sent(
        &RecordBatch::try_from_iter(columns).unwrap(),
        Limits::default(),
    );
    assert!(frames[0].header.len() + frames[0].body.len() < 2 << 20);
    let refusal = refusal(decoder.frame(&frames[0]));
    assert_eq!(
        (refusal.field, refusal.limit),
        ("batch values", BATCH_VALUES)
    );
    // Refused at the first node beyond the limit, not after all the frame's values, about five billion.
    let items = u64::try_from(items).unwrap();
    assert!((BATCH_VALUES + 1..=BATCH_VALUES + items).contains(&refusal.actual));
}

#[test]
fn null_columns_beyond_the_value_limit_are_refused() {
    let rows = usize::try_from(BATCH_ROWS).unwrap();
    let fields: Vec<_> = (0..10_000)
        .map(|column| Field::new(format!("c{column}"), DataType::Null, true))
        .collect();
    let nulls: ArrayRef = Arc::new(NullArray::new(rows));
    let batch = RecordBatch::try_new_with_options(
        Arc::new(Schema::new(fields)),
        vec![nulls; 10_000],
        &RecordBatchOptions::new().with_row_count(Some(rows)),
    )
    .unwrap();
    let (mut decoder, frames) = sent(&batch, Limits::default());
    assert!(frames[0].body.is_empty());
    let refusal = refusal(decoder.frame(&frames[0]));
    assert_eq!(
        (refusal.field, refusal.limit, refusal.actual),
        ("batch values", BATCH_VALUES, BATCH_VALUES + BATCH_ROWS)
    );
}

/// `rows` views that each name the whole of one data buffer of `bytes` bytes.
fn aliasing(rows: usize, bytes: usize) -> BinaryViewArray {
    let view = u128::try_from(bytes).unwrap() | u128::from(u32::from_le_bytes(*b"aaaa")) << 32;
    BinaryViewArray::try_new(
        ScalarBuffer::from(vec![view; rows]),
        vec![Buffer::from(vec![b'a'; bytes])],
        None,
    )
    .unwrap()
}

/// The frame of `views`, and a decoder that received its column as text.
fn as_text(views: BinaryViewArray) -> (Decoder, IpcFrame) {
    let binary = Schema::new(vec![Field::new("c", DataType::BinaryView, false)]);
    let batch = RecordBatch::try_new(Arc::new(binary), vec![Arc::new(views)]).unwrap();
    let mut encoder = Encoder::default();
    encoder.schema(&batch.schema());
    let frame = encoder.batch(&batch).unwrap().remove(0);
    let text = Schema::new(vec![Field::new("c", DataType::Utf8View, false)]);
    (decoder(&text, Limits::default()), frame)
}

#[test]
fn views_aliasing_one_buffer_beyond_the_frame_limit_are_refused() {
    // Four gibibytes for Arrow to validate as text, in a frame of little more than a mebibyte.
    let (rows, bytes) = (4_096, 1 << 20);
    let (mut decoder, frame) = as_text(aliasing(rows, bytes));
    assert!(frame.header.len() + frame.body.len() < 2 << 20);
    let refusal = refusal(decoder.frame(&frame));
    assert_eq!(
        (refusal.field, refusal.limit, refusal.actual),
        ("view bytes", FRAME_BYTES, 4_096 << 20)
    );
}

#[test]
fn views_aliasing_one_buffer_within_the_frame_limit_decode_and_count_what_they_name() {
    let views = aliasing(3, 1_000);
    let text: ArrayRef = Arc::new(views.clone().to_string_view().unwrap());
    let (_, frame) = as_text(views);
    let within = |frame_bytes| {
        let schema = Schema::new(vec![Field::new("c", DataType::Utf8View, false)]);
        let limits = Limits {
            frame_bytes,
            ..Limits::default()
        };
        decoder(&schema, limits).shaped(&frame)
    };
    let (batch, shape) = within(3_000).unwrap();
    assert_eq!(batch.unwrap().column(0), &text);
    assert_eq!((shape.values, shape.view_bytes), (3, 3_000));
    let refusal = refusal(within(2_999));
    assert_eq!(
        (refusal.field, refusal.limit, refusal.actual),
        ("view bytes", 2_999, 3_000)
    );
}

#[test]
fn the_views_of_every_column_count_toward_the_frame_limit() {
    let views: ArrayRef = Arc::new(aliasing(3, 1_000));
    let batch = RecordBatch::try_from_iter([("a", Arc::clone(&views)), ("b", views)]).unwrap();
    let limits = |frame_bytes| Limits {
        frame_bytes,
        ..Limits::default()
    };
    let (mut decoder, frames) = sent(&batch, limits(6_000));
    let (got, shape) = decoder.shaped(&frames[0]).unwrap();
    assert_eq!(got, Some(batch.clone()));
    assert_eq!(shape.view_bytes, 6_000);
    let (mut decoder, frames) = sent(&batch, limits(5_999));
    let refusal = refusal(decoder.frame(&frames[0]));
    assert_eq!((refusal.field, refusal.actual), ("view bytes", 6_000));
}

/// Views, the sizes of their column's data buffers, and the bytes the views name or the first
/// view that names none.
type Named = (Vec<[u8; 16]>, Vec<u64>, Result<u64, usize>);

/// List views' offsets and sizes, their child's length, and the items they name or the first
/// that names none.
type Listed<'a> = (&'a [i32], &'a [i32], u64, Result<u64, usize>);

/// A change to a message's parts, given its body's length.
type Change = fn(&mut Parts, i64);

/// A view of `length` bytes at `offset` of data buffer `buffer`.
fn view(length: u32, buffer: u32, offset: u32) -> [u8; 16] {
    let mut view = [0xAA; 16];
    view[..4].copy_from_slice(&length.to_le_bytes());
    view[8..12].copy_from_slice(&buffer.to_le_bytes());
    view[12..].copy_from_slice(&offset.to_le_bytes());
    view
}

#[test]
fn views_name_the_bytes_beyond_those_they_hold_themselves() {
    let cases: Vec<Named> = vec![
        (vec![], vec![], Ok(0)),
        // A view of twelve bytes or fewer holds them itself, whatever else it says.
        (vec![view(12, 9, 9), view(0, 0, 0)], vec![], Ok(0)),
        (vec![view(13, 0, 0)], vec![13], Ok(13)),
        (vec![view(13, 0, 0)], vec![12], Err(0)),
        (vec![view(13, 0, 1)], vec![13], Err(0)),
        (vec![view(13, 0, 1)], vec![14], Ok(13)),
        (vec![view(13, 1, 0)], vec![13], Err(0)),
        (vec![view(13, 1, 0)], vec![0, 13], Ok(13)),
        (vec![view(13, 0, 0), view(20, 1, 5)], vec![13, 25], Ok(33)),
        (vec![view(13, 0, 0), view(20, 1, 6)], vec![13, 25], Err(1)),
        // Views of one buffer count each time they name it.
        (vec![view(100, 0, 0); 7], vec![100], Ok(700)),
        (
            vec![view(u32::MAX, 0, u32::MAX)],
            vec![2 * u64::from(u32::MAX)],
            Ok(u64::from(u32::MAX)),
        ),
        (
            vec![view(u32::MAX, 0, u32::MAX)],
            vec![2 * u64::from(u32::MAX) - 1],
            Err(0),
        ),
    ];
    for (views, data, expected) in cases {
        let mut bytes = views.concat();
        assert_eq!(named(&bytes, &data), expected, "{views:?} {data:?}");
        // Bytes short of a whole view are no view.
        bytes.extend([0xFF; 15]);
        assert_eq!(named(&bytes, &data), expected);
    }
}

#[test]
fn list_views_name_items_of_their_child() {
    fn narrow(values: &[i32]) -> Vec<u8> {
        values
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect()
    }
    fn wide(values: &[i32]) -> Vec<u8> {
        let values = values.iter().map(|value| i64::from(*value));
        values.flat_map(i64::to_le_bytes).collect()
    }
    let cases: Vec<Listed<'_>> = vec![
        (&[], &[], 0, Ok(0)),
        (&[0, 1], &[2, 2], 3, Ok(4)),
        (&[0, 1], &[2, 2], 2, Err(1)),
        (&[0, 0, 0], &[3, 3, 3], 3, Ok(9)),
        (&[3], &[0], 3, Ok(0)),
        (&[4], &[0], 3, Err(0)),
        (&[-1], &[1], 3, Err(0)),
        (&[0], &[-1], 3, Err(0)),
        (&[0, i32::MAX], &[1, i32::MAX], 3, Err(1)),
    ];
    for (offsets, sizes, child, expected) in cases {
        let read = |bytes| i64::from(i32::from_le_bytes(bytes));
        assert_eq!(
            listed(&narrow(offsets), &narrow(sizes), child, read),
            expected,
            "{offsets:?} {sizes:?} {child}"
        );
        assert_eq!(
            listed(&wide(offsets), &wide(sizes), child, i64::from_le_bytes),
            expected,
            "{offsets:?} {sizes:?} {child}"
        );
    }
    let most = i64::MAX.to_le_bytes();
    assert_eq!(
        listed(&most, &most, u64::MAX - 1, i64::from_le_bytes),
        Ok(u64::MAX >> 1)
    );
    assert_eq!(
        listed(&most, &most, u64::MAX - 2, i64::from_le_bytes),
        Err(0)
    );
}

/// A decoder that received every frame of `batch` but its last, and that last frame.
fn received(batch: &RecordBatch) -> (Decoder, Vec<IpcFrame>) {
    let (mut decoder, frames) = sent(batch, Limits::default());
    for frame in &frames[..frames.len() - 1] {
        assert_eq!(decoder.frame(frame).unwrap(), None);
    }
    (decoder, frames)
}

/// `frame` with `bytes` written over its body at `at`.
fn overwritten(frame: &IpcFrame, at: i64, bytes: &[u8]) -> IpcFrame {
    let at = usize::try_from(at).unwrap();
    let mut body = frame.body.to_vec();
    body[at..at + bytes.len()].copy_from_slice(bytes);
    IpcFrame {
        header: frame.header.clone(),
        body: Bytes::from(body),
    }
}

#[test]
fn a_view_outside_its_columns_data_buffers_is_refused() {
    let batch = batch_of(samples::text_views());
    let (mut decoder, frames) = received(&batch);
    let parts = Parts::of(&frames[0].header);
    assert_eq!(parts.variadic, [2]);
    // The first view lies after the validity buffer; its last four bytes are its offset.
    let (views, _) = parts.buffers[1];
    let far = overwritten(&frames[0], views + 12, &u32::MAX.to_le_bytes());
    assert_eq!(
        problem(decoder.frame(&far)),
        Problem::View { node: 0, index: 0 }
    );
    let missing = overwritten(&frames[0], views + 3 * 16 + 8, &2_u32.to_le_bytes());
    assert_eq!(
        problem(decoder.frame(&missing)),
        Problem::View { node: 0, index: 3 }
    );
    assert_eq!(decoder.frame(&frames[0]).unwrap(), Some(batch));
}

#[test]
fn a_list_view_outside_its_child_is_refused() {
    let lists = samples::columns().into_iter().filter(|column| {
        matches!(
            column.data_type(),
            DataType::ListView(_) | DataType::LargeListView(_)
        )
    });
    let mut widths = Vec::new();
    for column in lists {
        let batch = batch_of(column);
        let (mut decoder, frames) = received(&batch);
        let parts = Parts::of(&frames[0].header);
        // The sizes follow the validity and the offsets; the child holds five items.
        let (sizes, bytes) = parts.buffers[2];
        let width = usize::try_from(bytes).unwrap() / ROWS;
        widths.push(width);
        let mut size = vec![0; width];
        size[0] = 4;
        let beyond = overwritten(&frames[0], sizes + i64::try_from(2 * width).unwrap(), &size);
        assert_eq!(
            problem(decoder.frame(&beyond)),
            Problem::ListView { node: 0, index: 2 }
        );
        size[0] = 3;
        let within = overwritten(&frames[0], sizes + i64::try_from(2 * width).unwrap(), &size);
        let (got, shape) = decoder.shaped(&within).unwrap();
        assert_eq!(got.unwrap().num_rows(), ROWS);
        // Five lists, their child's five items, and the 2 + 0 + 3 + 0 + 2 items they name.
        assert_eq!(shape.values, 5 + 5 + 7);
    }
    assert_eq!(widths, [4, 8]);
}

/// Columns, each with the values its frame declares: every node's length, and every list
/// view's size.
fn counted() -> Vec<(ArrayRef, u64)> {
    let item = |data_type| Arc::new(Field::new("item", data_type, true));
    let nulls: ArrayRef = Arc::new(NullArray::new(6));
    let ints: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let fields = Fields::from(vec![
        Field::new("n", DataType::Null, true),
        Field::new("i", DataType::Int32, true),
    ]);
    let children = vec![Arc::new(NullArray::new(3)) as ArrayRef, Arc::clone(&ints)];
    let runs = RunArray::<Int32Type>::try_new(&vec![2, 6].into(), &Int32Array::from(vec![7, 8]));
    vec![
        (Arc::clone(&nulls), 6),
        (
            Arc::new(StructArray::new(fields, children, None)),
            3 + 3 + 3,
        ),
        (
            Arc::new(
                arrow_array::FixedSizeListArray::try_new(
                    item(DataType::Null),
                    3,
                    Arc::clone(&nulls),
                    None,
                )
                .unwrap(),
            ),
            2 + 6,
        ),
        (Arc::new(runs.unwrap()), 6 + 2 + 2),
        (
            Arc::new(ListArray::new(
                item(DataType::Null),
                OffsetBuffer::from_lengths([4, 2]),
                Arc::clone(&nulls),
                None,
            )),
            2 + 6,
        ),
        (
            // Two lists of the same three items.
            Arc::new(ListViewArray::new(
                item(DataType::Int32),
                vec![0, 0].into(),
                vec![3, 3].into(),
                ints,
                None,
            )),
            2 + 3 + 6,
        ),
        (
            new_null_array(
                &DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                4,
            ),
            4,
        ),
        (samples::text_views(), 5),
    ]
}

#[test]
fn values_of_every_kind_count_toward_the_value_limit_whether_or_not_they_take_bytes() {
    for (column, values) in counted() {
        let batch = batch_of(column);
        let limits = |batch_values| Limits {
            batch_values,
            ..Limits::default()
        };
        let (mut decoder, frames) = sent(&batch, limits(values));
        let mut last = None;
        for frame in &frames {
            last = Some(decoder.shaped(frame).unwrap());
        }
        let (got, shape) = last.unwrap();
        assert_eq!(got, Some(batch.clone()));
        assert_eq!(shape.values, values, "{}", batch.schema());
        let (mut decoder, frames) = sent(&batch, limits(values - 1));
        let refusal = refusal(
            frames
                .iter()
                .try_fold(None, |_, frame| decoder.frame(frame)),
        );
        assert_eq!(
            (refusal.field, refusal.limit),
            ("batch values", values - 1),
            "{}",
            batch.schema()
        );
    }
}

#[test]
fn the_values_of_a_dictionary_count_in_its_own_frame() {
    let batch = batch_of(samples::columns().pop().unwrap());
    let (mut decoder, frames) = sent(&batch, Limits::default());
    assert_eq!(frames.len(), 2);
    // The dictionary's five lists and their five items; then the batch's five keys.
    let (none, dictionary) = decoder.shaped(&frames[0]).unwrap();
    assert_eq!((none, dictionary.values), (None, 5 + 5));
    let (got, keys) = decoder.shaped(&frames[1]).unwrap();
    assert_eq!((got, keys.values), (Some(batch), 5));
}

#[test]
fn every_kind_of_column_decodes_unchanged() {
    for column in samples::columns() {
        let batch = batch_of(column);
        let (mut decoder, frames) = received(&batch);
        let got = decoder.frame(frames.last().unwrap());
        assert_eq!(got.unwrap(), Some(batch.clone()), "{}", batch.schema());
        let none = batch_of(arrow_array::new_empty_array(batch.column(0).data_type()));
        let (mut decoder, frames) = received(&none);
        let got = decoder.frame(frames.last().unwrap());
        assert_eq!(got.unwrap(), Some(none), "{}", batch.schema());
    }
    let batch = samples::batch();
    let (mut decoder, frames) = received(&batch);
    assert_eq!(decoder.frame(frames.last().unwrap()).unwrap(), Some(batch));
}

/// For one column of each layout, the bytes its node needs of some of its frame's buffers.
fn needs() -> Vec<(ArrayRef, Vec<(usize, u64)>)> {
    let sample = |wanted: fn(&DataType) -> bool| {
        let mut columns = samples::columns().into_iter();
        columns.find(|column| wanted(column.data_type())).unwrap()
    };
    let null = |data_type: &DataType| new_null_array(data_type, ROWS);
    let mut needs = vec![
        (null(&DataType::Boolean), vec![(0, 1), (1, 1)]),
        (null(&DataType::FixedSizeBinary(3)), vec![(0, 1), (1, 15)]),
        (null(&DataType::Utf8), vec![(0, 1), (1, 24)]),
        (null(&DataType::Binary), vec![(0, 1), (1, 24)]),
        (null(&DataType::LargeUtf8), vec![(0, 1), (1, 48)]),
        (null(&DataType::LargeBinary), vec![(0, 1), (1, 48)]),
        (samples::text_views(), vec![(0, 1), (1, 80)]),
        (
            sample(|data_type| matches!(data_type, DataType::BinaryView)),
            vec![(0, 1), (1, 80)],
        ),
        // A list's validity and offsets, then its item's validity and values.
        (
            sample(|data_type| matches!(data_type, DataType::List(_))),
            vec![(0, 1), (1, 24), (2, 1), (3, 20)],
        ),
        (
            sample(|data_type| matches!(data_type, DataType::LargeList(_))),
            vec![(0, 1), (1, 48)],
        ),
        (
            sample(|data_type| matches!(data_type, DataType::Map(..))),
            vec![(0, 1), (1, 24)],
        ),
        (
            sample(|data_type| matches!(data_type, DataType::ListView(_))),
            vec![(0, 1), (1, 20), (2, 20)],
        ),
        (
            sample(|data_type| matches!(data_type, DataType::LargeListView(_))),
            vec![(0, 1), (1, 40), (2, 40)],
        ),
        (
            sample(|data_type| matches!(data_type, DataType::FixedSizeList(..))),
            vec![(0, 1)],
        ),
    ];
    needs.extend(parents());
    needs.extend(widths());
    needs
}

/// As [`needs`], for unions and structs.
fn parents() -> Vec<(ArrayRef, Vec<(usize, u64)>)> {
    let sample = |wanted: fn(&DataType) -> bool| {
        let mut columns = samples::columns().into_iter();
        columns.find(|column| wanted(column.data_type())).unwrap()
    };
    vec![
        // A sparse union's type ids; a dense one's type ids and offsets.
        (
            sample(|data_type| {
                matches!(
                    data_type,
                    DataType::Union(_, arrow_schema::UnionMode::Sparse)
                )
            }),
            vec![(0, 5)],
        ),
        (
            sample(|data_type| {
                matches!(
                    data_type,
                    DataType::Union(_, arrow_schema::UnionMode::Dense)
                )
            }),
            vec![(0, 5), (1, 20)],
        ),
        // A struct's validity.
        (
            Arc::new(StructArray::new_null(
                Fields::from(vec![Field::new("n", DataType::Null, true)]),
                ROWS,
            )),
            vec![(0, 1)],
        ),
    ]
}

/// As [`needs`], for the columns whose values' width follows their type: fixed-width values,
/// dictionary keys and run ends.
fn widths() -> Vec<(ArrayRef, Vec<(usize, u64)>)> {
    let rows = u64::try_from(ROWS).unwrap();
    let null = |data_type: &DataType| new_null_array(data_type, ROWS);
    let mut needs = Vec::new();
    for data_type in samples::fixed_width() {
        let width = u64::try_from(data_type.primitive_width().unwrap()).unwrap();
        needs.push((null(&data_type), vec![(0, 1), (1, rows * width)]));
    }
    for key in samples::KEYS {
        let width = u64::try_from(key.primitive_width().unwrap()).unwrap();
        let keyed = DataType::Dictionary(Box::new(key), Box::new(DataType::Utf8));
        needs.push((null(&keyed), vec![(0, 1), (1, rows * width)]));
    }
    // A run-end column has no buffer of its own: its run ends' values, then its values'.
    for runs in samples::columns() {
        if let DataType::RunEndEncoded(ends, _) = runs.data_type() {
            let width = u64::try_from(ends.data_type().primitive_width().unwrap()).unwrap();
            needs.push((runs, vec![(1, 3 * width), (2, 1), (3, 16)]));
        }
    }
    needs
}

#[test]
fn a_buffer_shorter_than_its_node_needs_is_refused_for_every_layout() {
    for (column, needs) in needs() {
        let batch = batch_of(column);
        let (mut decoder, frames) = received(&batch);
        for (index, needed) in needs {
            let length = i64::try_from(needed).unwrap();
            let exact = changed(&frames, |parts| {
                assert!(parts.buffers[index].1 >= length, "{}", batch.schema());
                parts.buffers[index].1 = length;
            });
            let got = decoder.frame(&exact);
            assert_eq!(got.unwrap(), Some(batch.clone()), "{}", batch.schema());
            let short = changed(&frames, |parts| parts.buffers[index].1 = length - 1);
            assert_eq!(
                problem(decoder.frame(&short)),
                Problem::BufferTooShort {
                    index,
                    length: needed - 1,
                    needed
                },
                "{}",
                batch.schema()
            );
        }
    }
}

#[test]
fn a_column_without_nulls_or_values_needs_no_validity_or_offsets() {
    for data_type in [DataType::Utf8, DataType::LargeBinary, DataType::Int32] {
        let batch = batch_of(new_null_array(&data_type, 0));
        let (mut decoder, frames) = received(&batch);
        let empty = changed(&frames, |parts| {
            for buffer in &mut parts.buffers {
                buffer.1 = 0;
            }
        });
        assert_eq!(decoder.frame(&empty).unwrap(), Some(batch));
    }
    let batch = batch_of(Arc::new(Int32Array::from(vec![1, 2, 3])));
    let (mut decoder, frames) = received(&batch);
    let bare = changed(&frames, |parts| parts.buffers[0].1 = 0);
    assert_eq!(decoder.frame(&bare).unwrap(), Some(batch));
}

#[test]
fn a_frame_whose_counts_are_not_those_its_schema_needs_is_refused() {
    let batch = samples::batch();
    let (mut decoder, frames) = received(&batch);
    let end = frames.last().unwrap().body.len();
    let end = i64::try_from(end).unwrap();
    let cases: [(Change, Problem); 6] = [
        (
            |parts, _| parts.nodes.truncate(parts.nodes.len() - 1),
            Problem::Missing { part: Part::Node },
        ),
        (
            |parts, _| parts.nodes.push((0, 0)),
            Problem::Unused { part: Part::Node },
        ),
        (
            |parts, _| parts.buffers.truncate(parts.buffers.len() - 1),
            Problem::Missing { part: Part::Buffer },
        ),
        (
            |parts, end| parts.buffers.push((end, 0)),
            Problem::Unused { part: Part::Buffer },
        ),
        (
            |parts, _| parts.variadic.truncate(parts.variadic.len() - 1),
            Problem::Missing {
                part: Part::VariadicCount,
            },
        ),
        (
            |parts, _| parts.variadic.push(0),
            Problem::Unused {
                part: Part::VariadicCount,
            },
        ),
    ];
    for (change, expected) in cases {
        let frame = changed(&frames, |parts| change(parts, end));
        assert_eq!(problem(decoder.frame(&frame)), expected);
    }
    assert_eq!(decoder.frame(frames.last().unwrap()).unwrap(), Some(batch));
}

#[test]
fn a_node_with_impossible_counts_is_refused() {
    let batch = samples::batch();
    let (mut decoder, frames) = received(&batch);
    for (length, nulls) in [(-1, 0), (5, -1), (5, 6), (i64::MIN, i64::MIN)] {
        let frame = changed(&frames, |parts| parts.nodes[2] = (length, nulls));
        assert_eq!(
            problem(decoder.frame(&frame)),
            Problem::Node {
                index: 2,
                length,
                nulls
            }
        );
    }
    let frame = changed(&frames, |parts| parts.variadic[0] = -1);
    assert_eq!(
        problem(decoder.frame(&frame)),
        Problem::VariadicCount { count: -1 }
    );
}

#[test]
fn a_buffer_out_of_order_unpadded_or_outside_the_body_is_refused() {
    let batch = batch_of(Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])));
    let (mut decoder, frames) = received(&batch);
    let body = frames[0].body.len();
    assert_eq!(Parts::of(&frames[0].header).buffers, [(0, 1), (64, 12)]);
    let body = u64::try_from(body).unwrap();
    let outside = |offset, length| Problem::BufferOutOfBounds {
        index: 1,
        offset,
        length,
        body,
    };
    let cases = [
        ((128, 1), outside(128, 1)),
        ((64, 65), outside(64, 65)),
        ((-8, 16), outside(-8, 16)),
        ((64, -1), outside(64, -1)),
        ((i64::MAX, 1), outside(i64::MAX, 1)),
        ((i64::MAX, i64::MAX), outside(i64::MAX, i64::MAX)),
        (
            (0, 12),
            Problem::BufferOverlaps {
                index: 1,
                offset: 0,
                end: 1,
            },
        ),
        (
            (4, 12),
            Problem::BufferUnaligned {
                index: 1,
                offset: 4,
            },
        ),
        (
            (65, 12),
            Problem::BufferUnaligned {
                index: 1,
                offset: 65,
            },
        ),
    ];
    for (buffer, expected) in cases {
        let frame = changed(&frames, |parts| parts.buffers[1] = buffer);
        assert_eq!(problem(decoder.frame(&frame)), expected, "{buffer:?}");
    }
    // A buffer may start where the buffer before it ends, at any multiple of eight, and reach
    // the body's end.
    for buffer in [(8, 12), (16, 12), (64, 64)] {
        let frame = changed(&frames, |parts| parts.buffers[1] = buffer);
        assert!(decoder.frame(&frame).is_ok(), "{buffer:?}");
    }
    let frame = changed(&frames, |parts| parts.buffers = vec![(0, 8), (8, 12)]);
    assert_eq!(decoder.frame(&frame).unwrap().unwrap().num_rows(), 3);
}

#[test]
fn a_malformed_dictionary_frame_is_refused_as_a_dictionary() {
    let tags = arrow_array::StringArray::from(vec!["a", "bb"]);
    let keys = arrow_array::Int8Array::from(vec![0, 1, 0]);
    let keyed = arrow_array::DictionaryArray::try_new(keys, Arc::new(tags)).unwrap();
    let batch = batch_of(Arc::new(keyed));
    let (mut decoder, frames) = sent(&batch, Limits::default());
    let short = changed(&frames[..1], |parts| parts.buffers[1].1 -= 1);
    let error = refused(decoder.frame(&short));
    assert!(
        matches!(
            error,
            WireError::Malformed {
                frame: Frame::Dictionary,
                problem: Problem::BufferTooShort { index: 1, .. }
            }
        ),
        "{error}"
    );
    let unknown = changed(&frames[..1], |parts| parts.dictionary = Some((77, false)));
    assert_eq!(
        problem(decoder.frame(&unknown)),
        Problem::UnknownDictionary { id: 77 }
    );
}

#[test]
fn a_message_of_another_metadata_version_is_refused() {
    let batch = samples::batch();
    let (mut decoder, frames) = received(&batch);
    for version in [MetadataVersion::V1, MetadataVersion::V4] {
        let frame = changed(&frames, |parts| parts.version = version);
        assert_eq!(
            problem(decoder.frame(&frame)),
            Problem::Version { found: version.0 }
        );
    }
}

#[test]
fn a_compressed_body_is_refused() {
    let batch = samples::batch();
    let (mut decoder, frames) = sent(&batch, Limits::default());
    for frame in 0..frames.len() {
        let compressed = changed(&frames[..=frame], |parts| parts.compressed = true);
        assert_eq!(problem(decoder.frame(&compressed)), Problem::Compressed);
        assert!(decoder.frame(&frames[frame]).is_ok());
    }
}

#[test]
fn views_beyond_a_columns_length_are_not_its_views() {
    let batch = batch_of(samples::text_views());
    let (mut decoder, frames) = received(&batch);
    let (views, _) = Parts::of(&frames[0].header).buffers[1];
    // The fifth view names bytes no data buffer holds, in a column of four rows.
    let frame = overwritten(&frames[0], views + 4 * 16, &view(100, 9, 0));
    let shorter = changed(std::slice::from_ref(&frame), |parts| {
        parts.length = 4;
        parts.nodes[0].0 = 4;
    });
    assert_eq!(decoder.frame(&shorter).unwrap(), Some(batch.slice(0, 4)));
    assert_eq!(
        problem(decoder.frame(&frame)),
        Problem::View { node: 0, index: 4 }
    );
}

#[test]
fn a_run_end_column_of_no_values_decodes_as_arrows_writer_sends_it() {
    for column in samples::without_runs() {
        let batch = batch_of(column);
        let (mut decoder, frames) = received(&batch);
        // Arrow's writer describes one run ending at zero there, which its reader refuses.
        let nodes = Parts::of(&frames[0].header).nodes;
        assert!(nodes.windows(2).any(|pair| pair == [(0, 0), (1, 0)]));
        let got = decoder.frame(&frames[0]);
        assert_eq!(got.unwrap(), Some(batch.clone()), "{}", batch.schema());
    }
}

#[test]
fn list_views_beyond_a_columns_length_are_not_its_list_views() {
    let lists = samples::columns().into_iter().filter(|column| {
        matches!(
            column.data_type(),
            DataType::ListView(_) | DataType::LargeListView(_)
        )
    });
    for column in lists {
        let batch = batch_of(column);
        let (mut decoder, frames) = received(&batch);
        let parts = Parts::of(&frames[0].header);
        let ((offsets, bytes), (sizes, _)) = (parts.buffers[1], parts.buffers[2]);
        let width = bytes / i64::try_from(ROWS).unwrap();
        // A sixth offset and size, naming items no child holds, in buffers a view longer than
        // the column.
        let beyond = overwritten(&frames[0], offsets + bytes, &[9]);
        let beyond = overwritten(&beyond, sizes + bytes, &[9]);
        let longer = changed(std::slice::from_ref(&beyond), |parts| {
            parts.buffers[1].1 = bytes + width;
            parts.buffers[2].1 = bytes + width;
        });
        let (got, shape) = decoder.shaped(&longer).unwrap();
        assert_eq!(got, Some(batch.clone()), "{}", batch.schema());
        assert_eq!(shape.values, 5 + 5 + 5);
    }
}
