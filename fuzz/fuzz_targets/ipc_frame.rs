//! Decoding corrupted Arrow frames never panics past the decoder: Arrow's own panics are
//! contained and refused, and nothing allocates beyond the limits.
//!
//! The frames are real encodings of a few batches, corrupted by the fuzzer's bytes, so the
//! corruption reaches the framing checks and Arrow's readers rather than stopping at the header.

#![no_main]

use std::sync::{Arc, Once, OnceLock};

use arrow_array::types::Int8Type;
use arrow_array::{
    ArrayRef, DictionaryArray, Int32Array, ListArray, RecordBatch, StringArray, StructArray,
};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use rdlt_wire::{Decoder, Encoder, IpcFrame, Limits};

static QUIET: Once = Once::new();
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
            let schema = encoder.schema(&batch.schema());
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

fuzz_target!(|input: (u8, Vec<u8>, Vec<u8>, Vec<u8>)| {
    // libfuzzer aborts on any panic, even one the decoder contains; the decoder's containment is
    // what this target checks, so panics unwind quietly here and only an escaping one fails.
    QUIET.call_once(|| std::panic::set_hook(Box::new(|_| {})));
    let (which, schema_mask, header_mask, body_mask) = input;
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
        let _ = decoder.frame(&IpcFrame {
            header: corrupted(&frame.header, &header_mask),
            body: corrupted(&frame.body, &body_mask),
        });
    }
});
