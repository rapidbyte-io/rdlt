use std::sync::Arc;

use arrow_array::types::Int8Type;
use arrow_array::{ArrayRef, Decimal128Array, DictionaryArray, Int8Array, RecordBatch};
use arrow_ipc::MetadataVersion;
use arrow_ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteContext, IpcWriteOptions};
use bytes::Bytes;
use rdlt_wire::aligned::{ALIGNMENT, is_aligned};
use rdlt_wire::{Decoder, IpcFrame, Limits};

use super::{rebuilt, relaid};

/// `batch` as a writer padding to eight bytes writes it: its schema message and its frames, each
/// body held from a 64-byte address, as the reader holds a block.
fn padded_to_eight(batch: &RecordBatch) -> (Bytes, Vec<IpcFrame>) {
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5).expect("options");
    let generator = IpcDataGenerator::default();
    let mut tracker = DictionaryTracker::new(false);
    let schema =
        generator.schema_to_bytes_with_dictionary_tracker(&batch.schema(), &mut tracker, &options);
    let (dictionaries, encoded) = generator
        .encode(
            batch,
            &mut tracker,
            &options,
            &mut IpcWriteContext::default(),
        )
        .expect("the batch encodes");
    let frame = |encoded: arrow_ipc::writer::EncodedData| {
        IpcFrame::new(
            Bytes::from(encoded.ipc_message),
            Bytes::from(encoded.arrow_data),
        )
        .aligned()
    };
    let mut frames: Vec<IpcFrame> = dictionaries.into_iter().map(frame).collect();
    frames.push(frame(encoded));
    (Bytes::from(schema.ipc_message), frames)
}

/// Three one-byte values ahead of sixteen-byte ones leave the latter at eight past a multiple of
/// sixteen, where a writer padding to eight puts them.
fn odd() -> RecordBatch {
    let small: ArrayRef = Arc::new(Int8Array::from(vec![Some(1), None, Some(3)]));
    let large: ArrayRef = Arc::new(Decimal128Array::from(vec![Some(1), None, Some(3)]));
    RecordBatch::try_from_iter([("a", small), ("b", large)]).expect("a batch")
}

#[test]
fn a_block_padded_to_eight_is_relaid_with_every_buffer_at_a_multiple_of_64_and_decodes() {
    let batch = odd();
    let (schema, frames) = padded_to_eight(&batch);
    let frame = frames.last().expect("a frame");
    let offsets = arrow_ipc::root_as_message(&frame.header)
        .expect("a message")
        .header_as_record_batch()
        .expect("a batch")
        .buffers()
        .expect("buffers")
        .iter()
        .map(arrow_ipc::Buffer::offset)
        .collect::<Vec<_>>();
    assert!(offsets.iter().any(|offset| offset % 16 == 8), "{offsets:?}");
    let mut decoder = Decoder::new(Limits::default());
    decoder.schema(&schema).expect("the schema");
    assert!(
        decoder.frame(frame).is_err(),
        "the decoder refuses it in place"
    );
    let relaid = relaid(frame, &Limits::default()).expect("relaid");
    assert!(is_aligned(&relaid.body));
    let message = arrow_ipc::root_as_message(&relaid.header).expect("a message");
    for buffer in message
        .header_as_record_batch()
        .expect("a batch")
        .buffers()
        .expect("buffers")
    {
        assert_eq!(
            usize::try_from(buffer.offset()).expect("an offset") % ALIGNMENT,
            0
        );
    }
    assert_eq!(decoder.frame(&relaid).expect("it decodes"), Some(batch));
}

#[test]
fn a_relaid_block_beyond_the_frame_limit_is_refused_before_it_is_copied() {
    let (_, frames) = padded_to_eight(&odd());
    let limits = Limits {
        frame_bytes: 200,
        ..Limits::default()
    };
    let refused = relaid(frames.last().expect("a frame"), &limits).expect_err("beyond the limit");
    assert_eq!(
        refused.limit().map(|limit| limit.name),
        Some("frame bytes"),
        "{refused}"
    );
}

#[test]
fn a_block_whose_buffer_lies_off_eight_bytes_or_outside_its_body_is_refused() {
    let (_, frames) = padded_to_eight(&odd());
    let frame = frames.last().expect("a frame");
    // Its body cut short: the last buffer lies past its end.
    let short = IpcFrame::new(
        frame.header.clone(),
        frame.body.slice(..frame.body.len() - 8),
    );
    assert!(relaid(&short, &Limits::default()).is_err());
    // A header that is no message.
    let garbage = IpcFrame::new(Bytes::from_static(&[0; 16]), frame.body.clone());
    assert!(relaid(&garbage, &Limits::default()).is_err());
}

#[test]
fn a_dictionary_block_padded_to_eight_is_relaid_as_a_dictionary_and_decodes() {
    // The dictionary's sixteen-byte values follow its validity, padded to eight.
    let values = Decimal128Array::from(vec![Some(1), None, Some(3)]);
    let keys = Int8Array::from(vec![Some(2), None, Some(0)]);
    let column: ArrayRef =
        Arc::new(DictionaryArray::<Int8Type>::try_new(keys, Arc::new(values)).expect("keys"));
    let batch = RecordBatch::try_from_iter([("d", column)]).expect("a batch");
    let (schema, frames) = padded_to_eight(&batch);
    let (dictionary, frame) = (&frames[0], &frames[1]);
    let mut decoder = Decoder::new(Limits::default());
    decoder.schema(&schema).expect("the schema");
    assert!(decoder.frame(dictionary).is_err(), "refused in place");
    let relaid = relaid(dictionary, &Limits::default()).expect("relaid");
    let message = arrow_ipc::root_as_message(&relaid.header).expect("a message");
    assert!(message.header_as_dictionary_batch().is_some());
    assert_eq!(decoder.frame(&relaid).expect("it decodes"), None);
    assert_eq!(decoder.frame(frame).expect("it decodes"), Some(batch));
}

#[test]
fn a_block_whose_buffer_starts_before_the_buffer_before_it_ends_is_refused() {
    let (_, frames) = padded_to_eight(&odd());
    let frame = frames.last().expect("a frame");
    let message = arrow_ipc::root_as_message(&frame.header).expect("a message");
    let batch = message.header_as_record_batch().expect("a batch");
    let lengths: Vec<usize> = batch
        .buffers()
        .expect("buffers")
        .iter()
        .map(|buffer| usize::try_from(buffer.length()).expect("a length"))
        .collect();
    assert!(lengths[0] > 0, "{lengths:?}");
    // Every buffer at the body's first byte, each over the buffer before it.
    let spans: Vec<_> = lengths.iter().map(|&length| (0, 0, length)).collect();
    let end = lengths.iter().copied().max().expect("a buffer");
    let overlapping = IpcFrame::new(
        rebuilt(&message, batch, None, &spans, end),
        frame.body.clone(),
    );
    assert!(relaid(&overlapping, &Limits::default()).is_err());
}
