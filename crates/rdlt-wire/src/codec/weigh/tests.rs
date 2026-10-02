use std::sync::Arc;

use arrow_array::types::Int32Type;
use arrow_array::{
    ArrayRef, DictionaryArray, Int8Array, Int32Array, RecordBatch, RunArray, StringArray,
    StringViewArray,
};
use proptest::prelude::*;
use rdlt_testkit::drawn::values;

use super::{Weigher, Weight};
use crate::codec::compact::compacted;
use crate::codec::tests::frames::sent;
use crate::codec::tests::odd;
use crate::codec::tests::samples::{self, batch_of};
use crate::limits::Limits;

/// What rows `start..start + rows` of `weigher`'s batch weigh as one piece.
fn piece(weigher: &mut Weigher, start: usize, rows: usize) -> Weight {
    let mut weight = Weight::default();
    weigher.begin();
    for row in start..start + rows {
        weight += weigher.weigh(row);
    }
    weight
}

/// What the receiver's walk counts in the frame of `batch`, and the frame's bytes.
fn walked(batch: &RecordBatch) -> (u64, u64, u64) {
    let (mut decoder, frames) = sent(batch, Limits::default());
    let mut last = None;
    for frame in &frames {
        last = Some(decoder.shaped(frame).unwrap().1);
    }
    let (shape, frame) = (last.unwrap(), &frames[frames.len() - 1]);
    let bytes = u64::try_from(frame.header.len() + frame.body.len()).unwrap();
    (shape.values, shape.view_bytes, bytes)
}

/// Checks every part of `batch` weighs what its frame holds once it holds only what its rows
/// name: its values and view bytes exactly, and its bytes within the frame's overhead.
fn weighs_as_it_crosses(name: &str, batch: &RecordBatch) -> Result<(), String> {
    let mut weigher = Weigher::new(batch);
    for start in 0..=batch.num_rows() {
        for rows in 1..=batch.num_rows() - start {
            let weight = piece(&mut weigher, start, rows);
            let compact = compacted(&batch.slice(start, rows)).unwrap();
            let (values, view_bytes, bytes) = walked(&compact);
            if (weight.values, weight.view_bytes) != (values, view_bytes) {
                return Err(format!(
                    "{name} {start}+{rows}: weighed {weight:?}, walked {values} values and \
                     {view_bytes} view bytes"
                ));
            }
            let most = weight.frame_bytes() + weigher.overhead();
            if bytes > most {
                return Err(format!(
                    "{name} {start}+{rows}: a frame of {bytes} bytes, weighed at most {most}"
                ));
            }
        }
    }
    Ok(())
}

#[test]
fn every_part_of_every_kind_of_column_weighs_what_its_frame_holds() {
    let mut columns: Vec<(String, ArrayRef)> = Vec::new();
    for column in samples::columns()
        .into_iter()
        .chain(samples::without_runs())
    {
        columns.push((column.data_type().to_string(), column));
    }
    for (name, column) in odd::columns() {
        columns.push((name.to_owned(), column));
    }
    for (name, column) in columns {
        weighs_as_it_crosses(&name, &batch_of(column)).unwrap();
    }
    weighs_as_it_crosses("every sample", &samples::batch()).unwrap();
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(256)))]

    #[test]
    fn every_part_of_a_drawn_batch_weighs_what_its_frame_holds(drawn in values::drawn()) {
        let batch = crate::codec::tests::batch(&drawn);
        let weighed = weighs_as_it_crosses("drawn", &batch);
        prop_assert!(weighed.is_ok(), "{:?}", weighed);
    }
}

#[test]
fn a_run_is_weighed_with_the_first_row_of_its_piece() {
    // Runs of two, one and two rows, of texts of 20, 30 and 40 bytes.
    let texts = ["a".repeat(20), "b".repeat(30), "c".repeat(40)];
    let values = StringViewArray::from_iter_values(texts.iter());
    let runs = RunArray::<Int32Type>::try_new(&Int32Array::from(vec![2, 3, 5]), &values);
    let batch = batch_of(Arc::new(runs.unwrap()));
    let mut weigher = Weigher::new(&batch);
    weigher.begin();
    let rows: Vec<_> = (0..5).map(|row| weigher.weigh(row)).collect();
    // The row, and where it begins a run, the run's end and its value.
    let values: Vec<_> = rows.iter().map(|row| row.values).collect();
    assert_eq!(values, [3, 1, 3, 3, 1]);
    let named: Vec<_> = rows.iter().map(|row| row.view_bytes).collect();
    assert_eq!(named, [20, 0, 30, 40, 0]);
    // A piece begun in the middle of a run holds that run.
    weigher.begin();
    assert_eq!(weigher.weigh(4).values, 3);
    assert_eq!(weigher.weigh(5), Weight::default());
    assert_eq!(weigher.rows(), 5);
}

#[test]
fn a_dictionary_key_is_weighed_in_its_frame_and_its_values_in_one_of_their_own() {
    let tags = StringArray::from(vec!["a tag of eighteen", "b"]);
    let keys = Int8Array::from(vec![Some(0), None, Some(1), Some(0)]);
    let batch = batch_of(Arc::new(
        DictionaryArray::try_new(keys, Arc::new(tags)).unwrap(),
    ));
    let mut weigher = Weigher::new(&batch);
    weigher.begin();
    // A key of a byte and a validity bit, null or not.
    let rows: Vec<_> = (0..4).map(|row| weigher.weigh(row)).collect();
    assert!(
        rows.iter()
            .all(|row| (row.values, row.frame_bits) == (1, 9))
    );
    // Two values: an offset and a validity bit each, and eighteen bytes.
    let values = weigher.dictionaries();
    let values: Vec<_> = values.iter().map(|w| (w.values, w.frame_bits)).collect();
    assert_eq!(values, [(2, 2 * 33 + 8 * 18)]);
}

#[test]
fn a_stretch_of_rows_weighs_what_its_rows_weigh_one_at_a_time() {
    for (name, column) in crate::codec::tests::nested::columns() {
        let batch = batch_of(column);
        let (mut stretches, mut rows) = (Weigher::new(&batch), Weigher::new(&batch));
        for start in 0..=batch.num_rows() {
            for length in 0..=batch.num_rows() - start {
                stretches.begin();
                let stretch = stretches.weigh_rows(start..start + length);
                assert_eq!(
                    stretch,
                    piece(&mut rows, start, length),
                    "{name} {start}+{length}"
                );
                // Two stretches of one piece weigh what the piece does.
                stretches.begin();
                let mut halves = stretches.weigh_rows(start..start + length / 2);
                halves += stretches.weigh_rows(start + length / 2..start + length);
                assert_eq!(halves, stretch, "{name} {start}+{length} in halves");
            }
        }
    }
}
