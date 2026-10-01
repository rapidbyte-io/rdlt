use std::sync::Arc;

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, DictionaryArray, Int8Array, Int32Array, ListArray,
    NullArray, RecordBatch, StringArray,
};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field};
use proptest::prelude::*;
use rdlt_testkit::drawn::values;

use crate::codec::tests::frames::refusal;
use crate::codec::tests::samples::batch_of;
use crate::codec::{Decoder, Encoder, IpcFrame};
use crate::error::WireError;
use crate::limits::{BATCH_ROWS, BATCH_VALUES, Limits};

/// The frames a sender within `limits` cuts `batch` into, or its refusal.
fn cut(batch: &RecordBatch, limits: &Limits) -> Result<Vec<IpcFrame>, WireError> {
    let mut encoder = Encoder::default();
    encoder.schema(&batch.schema());
    encoder.batch_within(batch, limits)
}

/// The batches a receiver within `limits` decodes from `frames` of `batch`'s schema.
fn received(batch: &RecordBatch, frames: &[IpcFrame], limits: Limits) -> Vec<RecordBatch> {
    let mut decoder = Decoder::new(limits);
    let schema = Encoder::default().schema(&batch.schema());
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
    assert_eq!(in_order(&batch, &received(&batch, &frames, limits)), [3, 2]);
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
    encoder.schema(&batch.schema());
    let frames = encoder.batch_within(&batch, &limits).unwrap();
    assert_eq!(frames.len(), 1 + 3);
    assert_eq!(
        in_order(&batch, &received(&batch, &frames, limits)),
        [3, 3, 1]
    );
    // The same dictionary is not sent again with the next batch.
    assert_eq!(encoder.batch_within(&batch, &limits).unwrap().len(), 3);
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
    encoder.schema(&both.schema());
    let few = Limits {
        batch_values: 50,
        ..Limits::default()
    };
    assert_eq!(
        refusal(encoder.batch_within(&both, &few)).field,
        "batch values"
    );
    // The schema epoch ended with the refusal: the next begins with its dictionary.
    let unsent = encoder.batch_within(&both, &Limits::default());
    assert_eq!(
        crate::codec::tests::frames::problem(unsent),
        crate::error::Problem::NoSchema
    );
    encoder.schema(&both.schema());
    let frames = encoder.batch_within(&both, &Limits::default()).unwrap();
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
    let error = Encoder::default()
        .batch_within(&rows(1), &Limits::default())
        .unwrap_err();
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

/// Whether a receiver within `limits` decodes `batch` sent whole.
fn fits(batch: &RecordBatch, limits: Limits) -> bool {
    let mut encoder = Encoder::default();
    let mut decoder = Decoder::new(limits);
    decoder.schema(&encoder.schema(&batch.schema())).unwrap();
    let frames = encoder.batch(batch).unwrap();
    frames.iter().all(|frame| decoder.frame(frame).is_ok())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(256)))]

    #[test]
    fn every_drawn_batch_is_cut_into_the_fewest_frames_its_receiver_admits(
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
        let each_fits = (0..rows).all(|row| fits(&batch.slice(row, 1), limits));
        match cut(&batch, &limits) {
            Err(error) => {
                prop_assert!(matches!(error, WireError::Refused(_)), "{}", error);
                prop_assert!(!each_fits || !fits(&batch.slice(0, 0), limits));
            }
            Ok(frames) => {
                prop_assert!(each_fits);
                let pieces = in_order(&batch, &received(&batch, &frames, limits));
                // No piece could have taken the row after it.
                let mut start = 0;
                for piece in &pieces[..pieces.len() - 1] {
                    prop_assert!(!fits(&batch.slice(start, piece + 1), limits));
                    start += piece;
                }
            }
        }
    }
}
