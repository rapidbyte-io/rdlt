use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int8Array, Int64Array, RecordBatch, make_array};
use bytes::Bytes;

use crate::aligned::{ALIGNMENT, Aligned};
use crate::codec::measure::{Columns, measured};
use crate::codec::tests::frames::{padded_to_eight, sent};
use crate::codec::tests::samples::{self, batch_of};
use crate::codec::{Decoder, IpcFrame};
use crate::error::{Problem, WireError};
use crate::limits::Limits;

/// `frame` with its body `skew` bytes past a 64-byte address of an allocation of its own.
fn skewed(frame: &IpcFrame, skew: usize) -> IpcFrame {
    let mut aligned = Aligned::with_room(skew + frame.body.len());
    aligned.extend(&vec![0; skew]);
    aligned.extend(&frame.body);
    let frozen = aligned.freeze();
    IpcFrame {
        header: frame.header.clone(),
        body: frozen.bytes.slice(skew..),
        allocation: frozen.allocation,
    }
}

#[test]
fn a_body_off_its_alignment_is_refused_before_arrow_reads_it() {
    let batch = samples::batch();
    let (_, frames) = sent(&batch, Limits::default());
    let last = frames.last().unwrap();
    for skew in 0..ALIGNMENT {
        let (mut decoder, _) = sent(&batch, Limits::default());
        for frame in &frames[..frames.len() - 1] {
            decoder.frame(&skewed(frame, 0)).unwrap();
        }
        let read = decoder.frame(&skewed(last, skew));
        if skew == 0 {
            assert_eq!(read.unwrap().as_ref(), Some(&batch));
        } else if !skew.is_multiple_of(8) {
            // Off the format's eight bytes: refused, whatever its types.
            assert!(
                matches!(
                    read,
                    Err(WireError::Malformed {
                        problem: Problem::BufferUnaligned { .. },
                        ..
                    })
                ),
                "{skew}: {read:?}"
            );
        } else {
            // On eight bytes: it decodes where the skew meets every type's alignment, and is
            // refused for one whose alignment it does not meet.
            let met = 1 << skew.trailing_zeros();
            let fits = matches!(&read, Ok(Some(got)) if *got == batch);
            let refused = matches!(
                &read,
                Err(WireError::Malformed {
                    problem: Problem::BufferUnaligned { alignment, .. },
                    ..
                }) if *alignment > met
            );
            assert!(fits || refused, "{skew}: {read:?}");
        }
    }
}

#[test]
fn a_buffer_padded_to_eight_where_its_type_needs_sixteen_is_refused() {
    let (mut decoder, frames) = padded_to_eight(&samples::odd_decimals());
    let refused = decoder.frame(frames.last().unwrap());
    assert!(
        matches!(
            refused,
            Err(WireError::Malformed {
                problem: Problem::BufferUnaligned { alignment: 16, .. },
                ..
            })
        ),
        "{refused:?}"
    );
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
fn a_decoded_batch_lies_in_the_allocation_its_frame_arrived_in() {
    let rows = 100_000;
    let wide: ArrayRef = Arc::new(Int64Array::from_iter_values(0..rows));
    let text: ArrayRef = Arc::new(arrow_array::StringArray::from_iter_values(
        (0..rows).map(|row| row.to_string()),
    ));
    let batch = RecordBatch::try_from_iter([("a", wide), ("b", text)]).unwrap();
    let (mut decoder, frames) = sent(&batch, Limits::default());
    let frame = &frames[0];
    let (read, shape) = decoder.shaped(frame).unwrap();
    assert_eq!(read.as_ref(), Some(&batch));
    let (start, len) = (frame.allocation.as_ptr().addr(), frame.allocation.len());
    let mut held = Vec::new();
    for column in read.unwrap().columns() {
        spans(column, &mut held);
    }
    // Nothing was copied: every buffer is a slice of the allocation, which is what it holds.
    assert!(
        held.iter()
            .all(|(at, n)| *at >= start && at + n <= start + len)
    );
    assert_eq!(shape.held_bytes, u64::try_from(len).unwrap());
}

#[test]
fn an_empty_body_decodes_wherever_it_lies() {
    let batch = batch_of(Arc::new(Int8Array::from(Vec::<i8>::new())));
    let (mut decoder, frames) = sent(&batch, Limits::default());
    // An empty body at an odd address, as prost leaves an empty field.
    let odd = Bytes::from(vec![0_u8; 3]).slice(1..1);
    let frame = IpcFrame {
        header: frames[0].header.clone(),
        body: odd.clone(),
        allocation: odd,
    };
    let (read, shape) = decoder.shaped(&frame).unwrap();
    assert_eq!(read, Some(batch));
    assert_eq!(shape.held_bytes, 0);
}

#[test]
fn a_body_outside_its_allocation_is_read_and_counted_as_its_own() {
    let batch = samples::batch();
    let (mut decoder, frames) = sent(&batch, Limits::default());
    let elsewhere = Aligned::copied(&[0; 8]).bytes;
    let mut got = None;
    for frame in &frames {
        let apart = IpcFrame {
            allocation: elsewhere.clone(),
            ..frame.clone()
        };
        let (read, shape) = decoder.shaped(&apart).unwrap();
        let body = u64::try_from(frame.body.len()).unwrap();
        assert_eq!(shape.held_bytes, if body == 0 { 0 } else { body });
        got = read.or(got);
    }
    assert_eq!(got, Some(batch));
}

#[test]
fn a_run_end_column_of_no_values_decodes_in_place() {
    // Arrow's writer describes a run ending at zero there; the decoder hands Arrow the nodes it
    // reads, and still copies no buffer.
    let batch = samples::empty_runs();
    let (mut decoder, frames) = sent(&batch, Limits::default());
    let mut got = None;
    for frame in &frames {
        got = decoder.frame(frame).unwrap().or(got);
    }
    assert_eq!(got, Some(batch));
}

#[test]
fn only_a_message_whose_run_end_column_the_walk_emptied_is_read_as_the_walk_reads_it() {
    let walked = |batch: &RecordBatch| {
        let (_, frames) = sent(batch, Limits::default());
        let columns = Columns::new(batch.schema());
        let frame = frames.last().unwrap();
        measured(Some(&columns), &Limits::default(), frame)
            .unwrap()
            .walked
            .emptied
    };
    assert!(walked(&samples::empty_runs()));
    assert!(!walked(&samples::batch()));
}

#[test]
fn a_decoder_sharing_its_frames_counts_a_dictionary_at_its_frame_alone() {
    use arrow_array::DictionaryArray;
    use arrow_array::types::Int32Type;
    let keys = arrow_array::Int32Array::from(vec![0, 1, 0]);
    let values = arrow_array::StringArray::from(vec!["a", "b"]);
    let keyed = DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap();
    let batch = batch_of(Arc::new(keyed));
    let (mut alone, frames) = sent(&batch, Limits::default());
    let mut sharing = Decoder::sharing(Limits::default());
    sharing
        .schema(
            &crate::codec::Encoder::default()
                .schema(&batch.schema())
                .unwrap(),
        )
        .unwrap();
    for frame in &frames {
        alone.frame(frame).unwrap();
        sharing.frame(frame).unwrap();
    }
    let dictionary = &frames[0];
    let own = u64::try_from(dictionary.header.len() + dictionary.body.len()).unwrap();
    assert_eq!(sharing.dictionary_bytes(), own);
    assert_eq!(alone.dictionary_bytes(), own + crate::limits::MESSAGE_ROOM);
}
