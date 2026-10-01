use std::sync::Arc;

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, DictionaryArray, Int8Array, Int32Array, ListArray,
    NullArray, RecordBatch, StringArray,
};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field};
use proptest::prelude::*;
use rdlt_testkit::drawn::values;

use crate::codec::compact::compacted;
use crate::codec::tests::frames::refusal;
use crate::codec::tests::samples::batch_of;
use crate::codec::{Cut, Decoder, Encoder, IpcFrame};
use crate::error::WireError;
use crate::limits::{BATCH_ROWS, BATCH_VALUES, Limits};

/// The frames a sender within `limits` cuts `batch` into, or its refusal.
fn cut(batch: &RecordBatch, limits: &Limits) -> Result<Vec<IpcFrame>, WireError> {
    let mut encoder = Encoder::default();
    encoder.schema(&batch.schema()).unwrap();
    stepped(&mut encoder, batch, limits)
}

/// Every frame `encoder` cuts `batch` into within `limits`, a piece at a time.
fn stepped(
    encoder: &mut Encoder,
    batch: &RecordBatch,
    limits: &Limits,
) -> Result<Vec<IpcFrame>, WireError> {
    let mut cut = Cut::new(batch.clone(), *limits);
    let mut all = Vec::new();
    while let Some(frames) = encoder.piece(&mut cut)? {
        all.extend(frames);
    }
    assert!(cut.is_done());
    Ok(all)
}

/// The batches a receiver within `limits` decodes from `frames` of `batch`'s schema.
fn received(batch: &RecordBatch, frames: &[IpcFrame], limits: Limits) -> Vec<RecordBatch> {
    let mut decoder = Decoder::new(limits);
    let schema = Encoder::default().schema(&batch.schema()).unwrap();
    decoder.schema(&schema).unwrap();
    let pieces = frames
        .iter()
        .filter_map(|frame| match decoder.frame(frame) {
            Ok(piece) => piece,
            Err(error) => panic!("the receiver refused a frame its sender cut: {error}"),
        });
    pieces.collect()
}

/// Checks `pieces` are `batch`'s rows, once each and in order; how many rows each holds.
fn in_order(batch: &RecordBatch, pieces: &[RecordBatch]) -> Vec<usize> {
    let mut start = 0;
    for piece in pieces {
        assert_eq!(piece, &batch.slice(start, piece.num_rows()), "at {start}");
        start += piece.num_rows();
    }
    assert_eq!(start, batch.num_rows());
    pieces.iter().map(RecordBatch::num_rows).collect()
}

/// `columns` columns of `rows` booleans each.
fn flags(columns: usize, rows: usize) -> RecordBatch {
    let column: ArrayRef = Arc::new(BooleanArray::from(vec![true; rows]));
    let named = (0..columns).map(|at| (format!("c{at}"), Arc::clone(&column)));
    RecordBatch::try_from_iter(named).unwrap()
}

#[test]
fn a_batch_of_more_values_than_a_frame_may_hold_crosses_in_two() {
    let rows = usize::try_from(BATCH_ROWS).unwrap();
    let limits = Limits::default();
    let whole = flags(64, rows);
    let frames = cut(&whole, &limits).unwrap();
    assert_eq!(in_order(&whole, &received(&whole, &frames, limits)), [rows]);
    let wide = flags(65, rows);
    let frames = cut(&wide, &limits).unwrap();
    let first = usize::try_from(BATCH_VALUES).unwrap() / 65;
    assert_eq!(
        in_order(&wide, &received(&wide, &frames, limits)),
        [first, rows - first]
    );
}

/// `rows` rows of an integer and a short text.
fn rows(rows: i32) -> RecordBatch {
    let ids: ArrayRef = Arc::new(Int32Array::from_iter_values(0..rows));
    let names = StringArray::from_iter_values((0..rows).map(|row| format!("row {row}")));
    RecordBatch::try_from_iter([("id", ids), ("name", Arc::new(names) as ArrayRef)]).unwrap()
}

#[test]
fn a_batch_at_each_limit_goes_whole_and_one_beyond_is_cut_where_the_limit_falls() {
    let batch = rows(10);
    let whole = cut(&batch, &Limits::default()).unwrap();
    let bytes = u64::try_from(whole[0].header.len() + whole[0].body.len()).unwrap();
    let of_rows = |batch_rows| Limits {
        batch_rows,
        ..Limits::default()
    };
    let of_values = |batch_values| Limits {
        batch_values,
        ..Limits::default()
    };
    let of_bytes = |frame_bytes| Limits {
        frame_bytes,
        ..Limits::default()
    };
    // Two values a row.
    let cases = [
        (of_rows(10), vec![10]),
        (of_rows(9), vec![9, 1]),
        (of_rows(3), vec![3, 3, 3, 1]),
        (of_values(20), vec![10]),
        (of_values(19), vec![9, 1]),
        (of_values(2), vec![1; 10]),
        (of_bytes(bytes), vec![10]),
    ];
    for (limits, expected) in cases {
        let frames = cut(&batch, &limits).unwrap();
        assert_eq!(frames.len(), expected.len(), "{limits:?}");
        assert_eq!(
            in_order(&batch, &received(&batch, &frames, limits)),
            expected,
            "{limits:?}"
        );
    }
    // A frame of many rows a byte over the limit is cut in two.
    let batch = rows(2_000);
    let whole = cut(&batch, &Limits::default()).unwrap();
    let bytes = u64::try_from(whole[0].header.len() + whole[0].body.len()).unwrap();
    let limits = of_bytes(bytes - 1);
    let frames = cut(&batch, &limits).unwrap();
    let pieces = in_order(&batch, &received(&batch, &frames, limits));
    assert_eq!(pieces.len(), 2, "{pieces:?}");
}

/// Two lists, of one and of three integers.
fn nested() -> ListArray {
    let item = Arc::new(Field::new("item", DataType::Int32, true));
    ListArray::new(
        item,
        OffsetBuffer::from_lengths([1, 3]),
        Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
        None,
    )
}

/// One row holding a list of `items` nulls.
fn nulls(items: usize) -> RecordBatch {
    let item = Arc::new(Field::new("item", DataType::Null, true));
    batch_of(Arc::new(ListArray::new(
        item,
        OffsetBuffer::from_lengths([items]),
        Arc::new(NullArray::new(items)),
        None,
    )))
}

/// `rows` views that each name the whole of one data buffer of `bytes` bytes.
fn aliasing(rows: usize, bytes: usize) -> ArrayRef {
    let mut views = arrow_array::builder::BinaryViewBuilder::new();
    let block = views.append_block(vec![b'a'; bytes].into());
    for _ in 0..rows {
        let bytes = u32::try_from(bytes).unwrap();
        views.try_append_view(block, 0, bytes).unwrap();
    }
    Arc::new(views.finish())
}

#[test]
fn views_naming_more_than_a_frame_may_hold_are_cut_between_rows() {
    let batch = batch_of(aliasing(5, 1_000));
    let limits = Limits {
        frame_bytes: 3_000,
        ..Limits::default()
    };
    let frames = cut(&batch, &limits).unwrap();
    // A piece holds a copy of the bytes each of its views names: two views' worth fit.
    assert_eq!(
        in_order(&batch, &received(&batch, &frames, limits)),
        [2, 2, 1]
    );
}

#[test]
fn one_row_beyond_a_limit_is_refused_by_its_sender_naming_the_limit() {
    let item = Arc::new(Field::new("item", DataType::BinaryView, true));
    let views = ListArray::new(
        item,
        OffsetBuffer::from_lengths([3]),
        aliasing(3, 1_000),
        None,
    );
    let blob = BinaryArray::from_iter_values([vec![7_u8; 4_096]]);
    let cases: [(RecordBatch, Limits, &str); 4] = [
        (
            nulls(100),
            Limits {
                batch_values: 100,
                ..Limits::default()
            },
            "batch values",
        ),
        (
            batch_of(Arc::new(blob)),
            Limits {
                frame_bytes: 4_096,
                ..Limits::default()
            },
            "frame bytes",
        ),
        (
            batch_of(Arc::new(views)),
            Limits {
                frame_bytes: 2_999,
                ..Limits::default()
            },
            "view bytes",
        ),
        (
            rows(1),
            Limits {
                batch_rows: 0,
                ..Limits::default()
            },
            "batch rows",
        ),
    ];
    for (batch, limits, field) in cases {
        let refusal = refusal(cut(&batch, &limits));
        assert_eq!((refusal.field, refusal.code), (field, "limit_exceeded"));
    }
    // One value more in the limit, and the row goes.
    let within = Limits {
        batch_values: 101,
        ..Limits::default()
    };
    assert_eq!(cut(&nulls(100), &within).unwrap().len(), 1);
}

#[test]
fn a_dictionary_goes_once_ahead_of_the_batches_cut_from_its_batch() {
    let tags = StringArray::from(vec!["a", "bb", "ccc"]);
    let keys = Int8Array::from(vec![0, 1, 2, 2, 1, 0, 1]);
    let batch = batch_of(Arc::new(
        DictionaryArray::try_new(keys, Arc::new(tags)).unwrap(),
    ));
    let limits = Limits {
        batch_rows: 3,
        ..Limits::default()
    };
    let mut encoder = Encoder::default();
    encoder.schema(&batch.schema()).unwrap();
    let frames = stepped(&mut encoder, &batch, &limits).unwrap();
    assert_eq!(frames.len(), 1 + 3);
    assert_eq!(
        in_order(&batch, &received(&batch, &frames, limits)),
        [3, 3, 1]
    );
    // The same dictionary is not sent again with the next batch.
    assert_eq!(stepped(&mut encoder, &batch, &limits).unwrap().len(), 3);
    // A dictionary cannot be cut: one beyond a limit is refused.
    let small = Limits {
        batch_values: 2,
        ..Limits::default()
    };
    assert_eq!(refusal(cut(&batch, &small)).field, "batch values");
    // Its dictionary fits and is queued; its one row does not.
    let tag = DictionaryArray::try_new(Int8Array::from(vec![0]), Arc::new(nested())).unwrap();
    let wide = nulls(100).column(0).clone();
    let both = RecordBatch::try_from_iter([("t", Arc::new(tag) as ArrayRef), ("n", wide)]);
    let both = both.unwrap();
    let mut encoder = Encoder::default();
    encoder.schema(&both.schema()).unwrap();
    let few = Limits {
        batch_values: 50,
        ..Limits::default()
    };
    assert_eq!(
        refusal(stepped(&mut encoder, &both, &few)).field,
        "batch values"
    );
    // The schema epoch ended with the refusal: the next begins with its dictionary.
    let unsent = stepped(&mut encoder, &both, &Limits::default());
    assert_eq!(
        crate::codec::tests::frames::problem(unsent),
        crate::error::Problem::NoSchema
    );
    encoder.schema(&both.schema()).unwrap();
    let frames = stepped(&mut encoder, &both, &Limits::default()).unwrap();
    assert_eq!(frames.len(), 1 + 1);
    assert_eq!(received(&both, &frames, Limits::default()), [both]);
}

#[test]
fn a_batch_of_no_rows_is_one_frame() {
    let batch = rows(0);
    let limits = Limits::default();
    let frames = cut(&batch, &limits).unwrap();
    assert_eq!(frames.len(), 1);
    assert_eq!(in_order(&batch, &received(&batch, &frames, limits)), [0]);
}

#[test]
fn a_batch_before_any_schema_is_refused_by_its_sender() {
    let error = stepped(&mut Encoder::default(), &rows(1), &Limits::default()).unwrap_err();
    assert!(
        matches!(
            error,
            WireError::Malformed {
                problem: crate::error::Problem::NoSchema,
                ..
            }
        ),
        "{error}"
    );
}

/// Whether a receiver within `limits` decodes rows `start..start + rows` of `batch` sent as one
/// frame: as they are when they are the whole batch, else holding only what they name.
fn part_fits(batch: &RecordBatch, start: usize, rows: usize, limits: Limits) -> bool {
    let part = batch.slice(start, rows);
    let whole = start == 0 && rows == batch.num_rows();
    (whole && fits(&part, limits)) || fits(&compacted(&part).unwrap(), limits)
}

/// Whether a receiver within `limits` decodes `batch` sent whole.
fn fits(batch: &RecordBatch, limits: Limits) -> bool {
    let mut encoder = Encoder::default();
    let mut decoder = Decoder::new(limits);
    decoder
        .schema(&encoder.schema(&batch.schema()).unwrap())
        .unwrap();
    let frames = encoder.batch(batch).unwrap();
    frames.iter().all(|frame| decoder.frame(frame).is_ok())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(256)))]

    #[test]
    fn every_drawn_batch_is_cut_into_full_frames_its_receiver_admits(
        drawn in values::drawn(),
        (most_rows, most_values, most_bytes) in (1_u64..8, 1_u64..64, 0_u64..4),
    ) {
        let batch = crate::codec::tests::batch(&drawn);
        let limits = Limits {
            batch_rows: most_rows,
            batch_values: most_values * 4,
            frame_bytes: if most_bytes == 0 { 1 << 26 } else { most_bytes * 1024 },
            ..Limits::default()
        };
        let rows = batch.num_rows();
        let each_fits = (0..rows).all(|row| part_fits(&batch, row, 1, limits));
        match cut(&batch, &limits) {
            Err(error) => {
                prop_assert!(matches!(error, WireError::Refused(_)), "{}", error);
                prop_assert!(!each_fits || !part_fits(&batch, 0, 0, limits));
            }
            Ok(frames) => {
                prop_assert!(each_fits);
                let received = received(&batch, &frames, limits);
                let pieces = in_order(&batch, &received);
                // No piece but the last could have taken the row after it, unless the frame's
                // bytes bind: then a row more of its average size would not have fitted.
                let batches = frames.iter().filter(|frame| {
                    let message = arrow_ipc::root_as_message(&frame.header).unwrap();
                    message.header_type() == arrow_ipc::MessageHeader::RecordBatch
                });
                let sizes: Vec<usize> =
                    batches.map(|frame| bytes(std::slice::from_ref(frame))).collect();
                prop_assert_eq!(sizes.len(), pieces.len());
                let mut start = 0;
                for (at, piece) in pieces[..pieces.len() - 1].iter().enumerate() {
                    let full = !part_fits(&batch, start, piece + 1, limits);
                    let size = u64::try_from(sizes[at]).unwrap();
                    let average = size / u64::try_from(*piece).unwrap();
                    prop_assert!(full || size + average > limits.frame_bytes);
                    start += piece;
                }
            }
        }
    }
}

/// Columns whose pieces Arrow's writer would send with buffers their rows do not name: views of
/// shared data, list views and dense unions of shared children, alone and nested.
fn sharing() -> Vec<ArrayRef> {
    use arrow_array::types::Int32Type;
    use arrow_array::{
        Int64Array, ListViewArray, RunArray, StringViewArray, StructArray, UnionArray,
    };
    let rows = 2_000;
    let texts = (0..rows).map(|row| format!("{row:0100}"));
    let views: ArrayRef = Arc::new(StringViewArray::from_iter_values(texts));
    let item = |data_type: &DataType| Arc::new(Field::new("item", data_type.clone(), true));
    let numbers: ArrayRef = Arc::new(Int64Array::from_iter_values(0..10 * 2_000));
    let tens = || (0..2_000).map(|row| row * 10).collect::<Vec<i32>>();
    let list_views = ListViewArray::new(
        item(&DataType::Int64),
        tens().into(),
        vec![10; rows].into(),
        Arc::clone(&numbers),
        None,
    );
    let ones = OffsetBuffer::<i32>::from_lengths(vec![1; rows]);
    let listed = ListArray::new(item(views.data_type()), ones, Arc::clone(&views), None);
    let field = Field::new("v", views.data_type().clone(), true);
    let fields = arrow_schema::UnionFields::try_new(vec![0], vec![field.clone()]).unwrap();
    let ids = vec![0_i8; rows];
    let offsets: Vec<i32> = (0..2_000).collect();
    let dense = UnionArray::try_new(
        fields.clone(),
        ids.clone().into(),
        Some(offsets.into()),
        vec![Arc::clone(&views)],
    );
    let sparse = UnionArray::try_new(fields, ids.into(), None, vec![Arc::clone(&views)]);
    let ends = Int32Array::from_iter_values(1..=2_000);
    let runs = RunArray::<Int32Type>::try_new(&ends, &views).unwrap();
    let parent = StructArray::from(vec![(Arc::new(field), Arc::clone(&views))]);
    vec![
        Arc::clone(&views),
        Arc::new(list_views),
        Arc::new(listed),
        Arc::new(dense.unwrap()),
        Arc::new(sparse.unwrap()),
        Arc::new(runs),
        Arc::new(parent),
    ]
}

fn bytes(frames: &[IpcFrame]) -> usize {
    let sizes = frames
        .iter()
        .map(|frame| frame.header.len() + frame.body.len());
    sizes.sum()
}

#[test]
fn the_pieces_of_a_batch_carry_only_what_their_rows_name() {
    for column in sharing() {
        let batch = batch_of(column);
        let whole = bytes(&cut(&batch, &Limits::default()).unwrap());
        let limits = Limits {
            batch_rows: 100,
            ..Limits::default()
        };
        let frames = cut(&batch, &limits).unwrap();
        assert_eq!(frames.len(), 20, "{}", batch.schema());
        in_order(&batch, &received(&batch, &frames, limits));
        // A frame's buffers are each padded, so twenty frames may take twice one's bytes.
        let pieces = bytes(&frames);
        assert!(
            pieces <= 2 * whole,
            "{pieces} of {whole}: {}",
            batch.schema()
        );
    }
}

#[test]
fn rows_naming_shared_buffers_larger_than_a_frame_are_cut_and_cross() {
    for column in sharing() {
        let batch = batch_of(column);
        let whole = bytes(&cut(&batch, &Limits::default()).unwrap());
        let limits = Limits {
            frame_bytes: u64::try_from(whole).unwrap() / 2,
            ..Limits::default()
        };
        let frames = cut(&batch, &limits).unwrap();
        let pieces = in_order(&batch, &received(&batch, &frames, limits));
        assert!(
            (2..=4).contains(&pieces.len()),
            "{pieces:?}: {}",
            batch.schema()
        );
    }
}

#[test]
fn each_step_hands_over_one_piece_no_larger_than_a_frame() {
    // Twenty thousand rows of 65 flags, a frame for each 1024 rows: what the sender holds
    // beyond the batch is the piece it is sending.
    let batch = flags(65, 20_000);
    let limits = Limits {
        batch_rows: 1_024,
        frame_bytes: 64 * 1_024,
        ..Limits::default()
    };
    let mut encoder = Encoder::default();
    encoder.schema(&batch.schema()).unwrap();
    let whole = bytes(&encoder.batch(&batch).unwrap());
    let mut cut = Cut::new(batch.clone(), limits);
    let (mut steps, mut sent) = (0, 0);
    while let Some(frames) = encoder.piece(&mut cut).unwrap() {
        assert_eq!(frames.len(), 1);
        assert!(bytes(&frames) <= 64 * 1_024, "{}", bytes(&frames));
        (steps, sent) = (steps + 1, sent + bytes(&frames));
    }
    assert_eq!(steps, 20_000_usize.div_ceil(1_024));
    assert!(sent <= 2 * whole, "{sent} of {whole}");
    assert_eq!(encoder.piece(&mut cut).unwrap(), None);
}

/// How many batches `encoder` encodes to cut `batch` within `limits`, and into how many pieces.
fn cost(batch: &RecordBatch, limits: &Limits) -> (usize, usize) {
    let mut encoder = Encoder::default();
    encoder.schema(&batch.schema()).unwrap();
    let frames = stepped(&mut encoder, batch, limits).unwrap();
    (encoder.encodes, frames.len())
}

#[test]
fn a_cut_costs_about_an_encode_a_piece() {
    // Values bind: the cut is found by counting, and each piece is encoded once.
    let rows = usize::try_from(BATCH_ROWS).unwrap();
    assert_eq!(cost(&flags(65, rows), &Limits::default()), (2, 2));
    assert_eq!(cost(&flags(64, rows), &Limits::default()), (1, 1));
    let of_values = Limits {
        batch_values: 6_500,
        ..Limits::default()
    };
    assert_eq!(cost(&flags(65, 20_000), &of_values), (200, 200));
    // Bytes bind: the batch is encoded whole to learn its size, then each piece once or twice.
    let batch = self::rows(20_000);
    let whole = cut(&batch, &Limits::default()).unwrap();
    let of_bytes = Limits {
        frame_bytes: u64::try_from(bytes(&whole)).unwrap() / 10,
        ..Limits::default()
    };
    let (encodes, pieces) = cost(&batch, &of_bytes);
    assert!((10..=12).contains(&pieces), "{pieces}");
    assert!(encodes <= 2 * pieces + 1, "{encodes} for {pieces}");
    // Rows of very different sizes: a thousand of a byte, then one of sixty thousand, fifty
    // times over. No piece costs more than a few encodes.
    let mut texts = Vec::new();
    for _ in 0..50 {
        texts.extend(std::iter::repeat_n("x".to_owned(), 1_000));
        texts.push("y".repeat(60_000));
    }
    let skewed = batch_of(Arc::new(StringArray::from(texts)));
    let of_bytes = Limits {
        frame_bytes: 64 * 1_024,
        ..Limits::default()
    };
    let (encodes, pieces) = cost(&skewed, &of_bytes);
    assert!(encodes <= 6 * pieces, "{encodes} for {pieces}");
}

#[test]
fn a_refused_row_ends_its_batchs_cut_after_the_pieces_before_it() {
    let small = rows(10).column(1).clone();
    let blob: ArrayRef = Arc::new(StringArray::from(vec!["b".repeat(8_192)]));
    let texts = arrow_select::concat::concat(&[small.as_ref(), blob.as_ref()]).unwrap();
    let batch = batch_of(texts);
    let limits = Limits {
        frame_bytes: 4_096,
        ..Limits::default()
    };
    let mut encoder = Encoder::default();
    encoder.schema(&batch.schema()).unwrap();
    let mut cut = Cut::new(batch.clone(), limits);
    let first = encoder.piece(&mut cut).unwrap().unwrap();
    assert_eq!(
        in_order(&batch.slice(0, 10), &received(&batch, &first, limits)),
        [10]
    );
    assert!(!cut.is_done());
    assert_eq!(refusal(encoder.piece(&mut cut)).field, "frame bytes");
    assert!(cut.is_done());
    assert_eq!(encoder.piece(&mut cut).unwrap(), None);
}

#[test]
fn a_row_of_the_widest_schema_fits_the_smallest_frame_a_peer_may_ask_for() {
    use crate::limits::{MIN_BATCH_ROWS, MIN_BATCH_VALUES, MIN_FRAME_BYTES, SCHEMA_COLUMNS};
    let columns = usize::try_from(SCHEMA_COLUMNS).unwrap();
    let text: ArrayRef = Arc::new(arrow_array::StringViewArray::from(vec![
        Some("a value");
        200
    ]));
    let named = (0..columns).map(|at| (format!("c{at}"), Arc::clone(&text)));
    let batch = RecordBatch::try_from_iter(named).unwrap();
    let least = Limits {
        frame_bytes: MIN_FRAME_BYTES,
        batch_rows: MIN_BATCH_ROWS,
        batch_values: MIN_BATCH_VALUES,
        ..Limits::default()
    };
    let one = cut(&batch.slice(0, 1), &least).unwrap();
    assert!(bytes(&one) <= usize::try_from(MIN_FRAME_BYTES).unwrap() / 2);
    // At the minimums a frame takes a hundred rows of a schema that wide, by its values.
    let batch = flags(columns, 200);
    let frames = cut(&batch, &least).unwrap();
    let pieces = in_order(&batch, &received(&batch, &frames, least));
    assert_eq!(pieces, [104, 96]);
}

#[test]
fn a_dictionary_is_bounded_by_the_values_of_a_frame_not_the_rows_of_a_batch() {
    // A hundred tags, in batches of seven rows: the tags are values of their frame, not rows.
    let tags = StringArray::from_iter_values((0..100).map(|tag| format!("tag {tag}")));
    let keys = Int8Array::from_iter_values((0..50).map(|row| row % 100));
    let batch = batch_of(Arc::new(
        DictionaryArray::try_new(keys, Arc::new(tags)).unwrap(),
    ));
    let limits = Limits {
        batch_rows: 7,
        batch_values: 100,
        ..Limits::default()
    };
    let frames = cut(&batch, &limits).unwrap();
    assert_eq!(frames.len(), 1 + 8);
    assert_eq!(
        in_order(&batch, &received(&batch, &frames, limits))[..2],
        [7, 7]
    );
    let fewer = Limits {
        batch_values: 99,
        ..limits
    };
    let refusal = refusal(cut(&batch, &fewer));
    assert_eq!(
        (refusal.field, refusal.limit, refusal.actual),
        ("batch values", 99, 100)
    );
}
