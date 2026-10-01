use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int8Array, Int64Array, RecordBatch, make_array};
use arrow_ipc::MetadataVersion;
use arrow_ipc::writer::IpcWriteOptions;
use bytes::Bytes;

use crate::codec::tests::frames::{Parts, sent};
use crate::codec::tests::samples::{self, batch_of};
use crate::codec::{Decoder, Encoder, IpcFrame};
use crate::limits::Limits;

/// `frame` with its body `skew` bytes into an allocation, as a transport may hand it over.
fn skewed(frame: &IpcFrame, skew: usize) -> IpcFrame {
    let mut bytes = vec![0; skew];
    bytes.extend_from_slice(&frame.body);
    IpcFrame {
        header: frame.header.clone(),
        body: Bytes::from(bytes).slice(skew..),
    }
}

#[test]
fn a_body_at_any_address_decodes_unchanged() {
    let batch = samples::batch();
    let (mut decoder, frames) = sent(&batch, Limits::default());
    for skew in 0..=16 {
        let mut got = None;
        for frame in &frames {
            got = decoder.frame(&skewed(frame, skew)).unwrap();
        }
        assert_eq!(got.as_ref(), Some(&batch), "{skew}");
    }
}

/// An encoder padding buffers to `alignment` bytes.
fn padding(alignment: usize) -> Encoder {
    Encoder {
        options: IpcWriteOptions::try_new(alignment, false, MetadataVersion::V5).unwrap(),
        ..Encoder::default()
    }
}

#[test]
fn a_batch_padded_to_the_formats_eight_bytes_decodes_unchanged() {
    // Three bytes ahead of each column leave it at eight bytes past a multiple of sixteen, where
    // a column of sixteen-byte values is not aligned.
    let odd: ArrayRef = Arc::new(Int8Array::from(vec![1; samples::ROWS]));
    let mut columns = Vec::new();
    for column in samples::columns() {
        columns.extend([Arc::clone(&odd), column]);
    }
    let named = columns.into_iter().enumerate();
    let batch = RecordBatch::try_from_iter(named.map(|(at, column)| (format!("c{at}"), column)));
    let batch = batch.unwrap();
    let mut encoder = padding(8);
    let mut decoder = Decoder::new(Limits::default());
    decoder
        .schema(&encoder.schema(&batch.schema()).unwrap())
        .unwrap();
    let frames = encoder.batch(&batch).unwrap();
    let offsets = Parts::of(&frames.last().unwrap().header).buffers;
    assert!(offsets.iter().any(|(offset, _)| offset % 16 == 8));
    let mut got = None;
    for frame in &frames {
        got = decoder.frame(frame).unwrap();
    }
    assert_eq!(got, Some(batch));
}

/// Where each buffer of `array` and of the columns nested in it lies: its first byte's address
/// and its length.
fn spans(array: &dyn Array, spans: &mut Vec<(usize, usize)>) {
    let data = array.to_data();
    for buffer in data.buffers() {
        spans.push((buffer.as_ptr().addr(), buffer.len()));
    }
    if let Some(nulls) = data.nulls() {
        spans.push((nulls.buffer().as_ptr().addr(), nulls.buffer().len()));
    }
    for child in data.child_data() {
        self::spans(&make_array(child.clone()), spans);
    }
}

#[test]
fn a_decoded_batch_holds_one_allocation_no_larger_than_its_frame() {
    let rows = 100_000;
    let wide: ArrayRef = Arc::new(Int64Array::from_iter_values(0..rows));
    let text: ArrayRef = Arc::new(arrow_array::StringArray::from_iter_values(
        (0..rows).map(|row| row.to_string()),
    ));
    let batch = RecordBatch::try_from_iter([("a", wide), ("b", text)]).unwrap();
    for skew in [0, 1, 8] {
        let (mut decoder, frames) = sent(&batch, Limits::default());
        let frame = skewed(&frames[0], skew);
        let (got, shape) = decoder.shaped(&frame).unwrap();
        let got = got.unwrap();
        assert_eq!(got, batch);
        let mut held = Vec::new();
        for column in got.columns() {
            spans(column, &mut held);
        }
        let start = held.iter().map(|(start, _)| *start).min().unwrap();
        let end = held.iter().map(|(start, len)| start + len).max().unwrap();
        // Every buffer lies in one allocation of the frame's size, and none in the body.
        let body = frame.body.len();
        let allocated = usize::try_from(shape.held_bytes).unwrap();
        assert!(end - start <= allocated, "{skew}");
        let described = Parts::of(&frame.header).buffers;
        let described: i64 = described.iter().map(|(_, length)| length).sum();
        let described = usize::try_from(described).unwrap();
        assert!(
            (described..=body).contains(&allocated),
            "{allocated} of {body}"
        );
        let received = frame.body.as_ptr().addr();
        assert!(end <= received || start >= received + body, "{skew}");
    }
}

#[test]
fn the_allocation_holds_each_buffer_at_its_alignment_and_nothing_else() {
    // Validity of one byte, three one-byte values, then sixteen-byte values: 1, 3 padded to
    // 16, and 48 bytes; the padding the sender put between them is not kept.
    let small: ArrayRef = Arc::new(Int8Array::from(vec![Some(1), None, Some(3)]));
    let large = arrow_array::Decimal128Array::from(vec![Some(1), None, Some(3)]);
    let batch = RecordBatch::try_from_iter([("a", small), ("b", Arc::new(large) as ArrayRef)]);
    let batch = batch.unwrap();
    let (mut decoder, frames) = sent(&batch, Limits::default());
    assert_eq!(frames[0].body.len(), 4 * 64);
    let (got, shape) = decoder.shaped(&frames[0]).unwrap();
    assert_eq!(got, Some(batch));
    assert_eq!(shape.held_bytes, 64);
    // A validity bit and a byte a value: 7 and 56 bytes fill an allocation's unit of 64, and
    // 8 and 57 take the next.
    for (rows, held) in [(56, 64), (57, 128)] {
        let batch = batch_of(Arc::new(Int8Array::from(vec![7; rows])));
        let (mut decoder, frames) = sent(&batch, Limits::default());
        let (_, shape) = decoder.shaped(&frames[0]).unwrap();
        assert_eq!(shape.held_bytes, held);
    }
}
