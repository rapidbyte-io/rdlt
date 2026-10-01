//! A batch decoded from the wire is held as the decoder says it is: the two counts of what a
//! batch keeps alive agree.

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use rdlt_wire::{Decoder, Encoder, Limits};

use crate::cost::Allocations;

/// What `batch` holds once decoded, as the decoder measures it and as its allocations count.
fn decoded(batch: &RecordBatch) -> (u64, u64) {
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
    held.unwrap()
}

#[test]
fn a_decoded_batch_holds_the_one_allocation_its_frame_was_copied_into() {
    // Ten columns of one frame share its allocation, which counts once.
    let columns = (0..10).map(|column| {
        let ids: ArrayRef = Arc::new(Int64Array::from(vec![column; 1_000]));
        (format!("c{column}"), ids)
    });
    let plain = RecordBatch::try_from_iter(columns).unwrap();
    let (own, allocations) = decoded(&plain);
    assert_eq!(allocations, own);
    assert!(own >= 80_000);
}
