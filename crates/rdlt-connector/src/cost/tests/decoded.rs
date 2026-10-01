//! A batch decoded from the wire is held as the decoder says it is: the two counts of what a
//! batch keeps alive agree.

use std::sync::Arc;

use arrow_array::types::Int32Type;
use arrow_array::{ArrayRef, DictionaryArray, Int32Array, Int64Array, RecordBatch, StringArray};
use rdlt_wire::{Decoder, Encoder, Limits};

use crate::cost::Allocations;

/// What `batch` holds once decoded, as the decoder measures it and as its allocations count:
/// the batch's own allocation, the dictionaries the decoder holds, and the allocations.
fn decoded(batch: &RecordBatch) -> (u64, u64, u64) {
    let mut encoder = Encoder::default();
    let mut decoder = Decoder::new(Limits::default());
    let schema = encoder.schema(&batch.schema()).unwrap();
    decoder.schema(&schema).unwrap();
    let mut held = None;
    for frame in encoder.batch(batch).unwrap() {
        let (read, shape) = decoder.shaped(&frame).unwrap();
        if let Some(read) = read {
            assert_eq!(&read, batch);
            held = Some((shape.held_bytes, Allocations::of(&read).bytes()));
        }
    }
    let (own, allocations) = held.unwrap();
    (own, decoder.dictionary_bytes(), allocations)
}

#[test]
fn a_decoded_batch_holds_the_one_allocation_its_frame_was_copied_into() {
    // Ten columns of one frame share its allocation, which counts once.
    let columns = (0..10).map(|column| {
        let ids: ArrayRef = Arc::new(Int64Array::from(vec![column; 1_000]));
        (format!("c{column}"), ids)
    });
    let plain = RecordBatch::try_from_iter(columns).unwrap();
    let (own, dictionaries, allocations) = decoded(&plain);
    assert_eq!((dictionaries, allocations), (0, own));
    assert!(own >= 80_000);
}

#[test]
fn a_decoded_batch_holds_the_dictionaries_its_keys_name() {
    let words = StringArray::from(vec!["x".repeat(10_000)]);
    let keyed =
        DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![0; 100]), Arc::new(words));
    let column: ArrayRef = Arc::new(keyed.unwrap());
    let batch = RecordBatch::try_from_iter([("tag", column)]).unwrap();
    let (own, dictionaries, allocations) = decoded(&batch);
    assert!(dictionaries >= 10_000);
    // The decoder's count of a batch leaves its dictionaries out; a batch keeps them alive.
    assert_eq!(allocations, own + dictionaries);
}
