//! A batch decoded from the wire is held as the decoder says it is: the two counts of what a
//! batch keeps alive agree.

use std::sync::Arc;

use arrow_array::types::Int32Type;
use arrow_array::{ArrayRef, DictionaryArray, Int32Array, Int64Array, RecordBatch, StringArray};
use rdlt_wire::{Decoder, Encoder, Limits};

use crate::cost::{Allocations, schema_bytes};

/// What `batch` holds once decoded from frames held as a receiver holds them, as the decoder
/// measures it and as its allocations count: the batch's own allocation, what its dictionaries
/// held, the decoder's count of them, and the allocations.
fn decoded(batch: &RecordBatch) -> (u64, u64, u64, u64) {
    let mut encoder = Encoder::default();
    let mut decoder = Decoder::new(Limits::default());
    let schema = encoder.schema(&batch.schema()).unwrap();
    decoder.schema(&schema).unwrap();
    let (mut held, mut dictionaries) = (None, 0);
    for frame in encoder.batch(batch).unwrap() {
        let (read, shape) = decoder.shaped(&frame.aligned()).unwrap();
        match read {
            Some(read) => {
                assert_eq!(&read, batch);
                // Beside its buffers a batch holds its schema, which the decoder's count leaves
                // out.
                let buffers = Allocations::of(&read).bytes() - schema_bytes(&read.schema());
                held = Some((shape.held_bytes, buffers));
            }
            None => dictionaries += shape.held_bytes,
        }
    }
    let (own, allocations) = held.unwrap();
    (own, dictionaries, decoder.dictionary_bytes(), allocations)
}

#[test]
fn a_decoded_batch_holds_the_allocation_its_frame_arrived_in() {
    // Ten columns of one frame share its allocation, which counts once.
    let columns = (0..10).map(|column| {
        let ids: ArrayRef = Arc::new(Int64Array::from(vec![column; 1_000]));
        (format!("c{column}"), ids)
    });
    let plain = RecordBatch::try_from_iter(columns).unwrap();
    let (own, dictionaries, counted, allocations) = decoded(&plain);
    assert_eq!((dictionaries, counted, allocations), (0, 0, own));
    assert!(own >= 80_000);
}

#[test]
fn a_decoded_batch_holds_the_dictionaries_its_keys_name() {
    let words = StringArray::from(vec!["x".repeat(10_000)]);
    let keyed =
        DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![0; 100]), Arc::new(words));
    let column: ArrayRef = Arc::new(keyed.unwrap());
    let batch = RecordBatch::try_from_iter([("tag", column)]).unwrap();
    let (own, dictionaries, counted, allocations) = decoded(&batch);
    assert!(dictionaries >= 10_000);
    // A batch keeps alive its own allocation and those its dictionaries arrived in, which the
    // decoder counts at least as large.
    assert_eq!(allocations, own + dictionaries);
    assert!(counted >= dictionaries, "{counted} {dictionaries}");
}

#[test]
fn a_dictionary_is_counted_at_least_as_the_allocation_it_keeps_alive() {
    let words = StringArray::from(vec!["y".repeat(5_000), "z".repeat(3)]);
    let keyed =
        DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![0, 1, 0]), Arc::new(words));
    let column: ArrayRef = Arc::new(keyed.unwrap());
    let batch = RecordBatch::try_from_iter([("tag", column)]).unwrap();
    let (_, dictionaries, counted, _) = decoded(&batch);
    assert!(counted >= dictionaries, "{counted} {dictionaries}");
}
