//! Decoding corrupted Arrow frames never panics past the decoder: Arrow's own panics are
//! contained, quietly, and refused, and nothing allocates beyond the limits.
//!
//! The frames are real encodings of a few batches, corrupted by the fuzzer's bytes, so the
//! corruption reaches the framing checks and Arrow's readers rather than stopping at the header.
//! They are placed as a receiver holds them, and at times off their alignment, which is refused
//! before Arrow reads them.

#![forbid(unsafe_code)]
#![no_main]

use std::sync::{Arc, OnceLock};

use arrow_array::types::Int8Type;
use arrow_array::{
    ArrayRef, DictionaryArray, Int32Array, ListArray, RecordBatch, StringArray, StructArray,
};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use rdlt_wire::aligned::Aligned;
use rdlt_wire::{Decoder, Encoder, IpcFrame, Limits};

static FIXTURES: OnceLock<Vec<(Bytes, Vec<IpcFrame>)>> = OnceLock::new();

/// A few batches, each encoded as its schema message and frames.
fn fixtures() -> Vec<(Bytes, Vec<IpcFrame>)> {
    let ints: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]));
    let tags: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            vec![0, 1, 0].into(),
            Arc::new(StringArray::from(vec!["a", "bb"])),
        )
        .expect("the keys index the values"),
    );
    let items: ArrayRef = Arc::new(ListArray::from_iter_primitive::<arrow_array::types::Int32Type, _, _>(vec![
        Some(vec![Some(1), Some(2)]),
        None,
        Some(vec![]),
    ]));
    let nested: ArrayRef = Arc::new(StructArray::from(vec![
        (Arc::new(Field::new("n", DataType::Int32, true)), Arc::clone(&ints)),
        (Arc::new(Field::new("items", items.data_type().clone(), true)), items),
    ]));
    [("ints", ints), ("tags", tags), ("nested", nested)]
        .into_iter()
        .map(|(name, array)| {
            let schema = Schema::new(vec![Field::new(name, array.data_type().clone(), true)]);
            let batch = RecordBatch::try_new(Arc::new(schema), vec![array]).expect("valid");
            let mut encoder = Encoder::default();
            let schema = encoder.schema(&batch.schema()).expect("the schema encodes");
            (schema, encoder.batch(&batch).expect("the batch encodes"))
        })
        .collect()
}

/// `bytes` with `mask` XORed over them from the start.
fn corrupted(bytes: &Bytes, mask: &[u8]) -> Bytes {
    let mut bytes = bytes.to_vec();
    for (byte, flip) in bytes.iter_mut().zip(mask) {
        *byte ^= flip;
    }
    Bytes::from(bytes)
}

/// Whether `array`, or an array nested in it, holds a buffer.
fn buffered(array: &dyn arrow_array::Array) -> bool {
    let data = array.to_data();
    !data.buffers().is_empty()
        || data.nulls().is_some()
        || data
            .child_data()
            .iter()
            .any(|child| buffered(&arrow_array::make_array(child.clone())))
}

/// `body` laid `skew` bytes past a 64-byte address of an allocation of its own.
fn placed(header: Bytes, body: &[u8], skew: usize) -> IpcFrame {
    let mut aligned = Aligned::with_room(skew + body.len());
    aligned.extend(&vec![0; skew]);
    aligned.extend(body);
    let frozen = aligned.freeze();
    IpcFrame {
        header,
        body: frozen.bytes.slice(skew..),
        allocation: frozen.allocation,
    }
}

fuzz_target!(|input: (u8, u8, Vec<u8>, Vec<u8>, Vec<u8>)| {
    // libfuzzer aborts in its panic hook; a panic the decoder contains skips the hook, so only one
    // that escapes it fails the target.
    let (which, skew, schema_mask, header_mask, body_mask) = input;
    // Most inputs keep the body where a receiver holds it, so corruption reaches Arrow; the rest
    // move it off its alignment.
    let skew = if skew < 192 { 0 } else { usize::from(skew % 64) };
    let fixtures = FIXTURES.get_or_init(fixtures);
    let (schema, frames) = &fixtures[usize::from(which) % fixtures.len()];
    let limits = Limits {
        frame_bytes: 1 << 20,
        batch_rows: 1 << 12,
        ..Limits::default()
    };
    let mut decoder = Decoder::new(limits);
    let _ = decoder.schema(&corrupted(schema, &schema_mask));
    for frame in frames {
        let body = corrupted(&frame.body, &body_mask);
        let decoded = decoder.frame(&placed(
            corrupted(&frame.header, &header_mask),
            &body,
            skew,
        ));
        // A body off eight bytes is refused, but where Arrow reads no buffer of it.
        let read = matches!(
            &decoded,
            Ok(Some(batch)) if batch.columns().iter().any(|column| buffered(column.as_ref()))
        );
        assert!(
            skew % 8 == 0 || body.is_empty() || !read,
            "a body {skew} bytes off its alignment decoded"
        );
    }
});
