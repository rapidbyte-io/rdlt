use proptest::prelude::*;
use rdlt_testkit::drawn::values;

use arrow_array::RecordBatch;

use super::compacted;
use crate::codec::count::{Counted, counted};
use crate::codec::tests::frames::sent;
use crate::codec::tests::odd;
use crate::codec::tests::samples::{self, ROWS, batch_of};
use crate::limits::Limits;

/// What the receiver's walk counts in the frame of `batch`.
fn walked(batch: &RecordBatch) -> Counted {
    let (mut decoder, frames) = sent(batch, Limits::default());
    let mut last = None;
    for frame in &frames {
        last = Some(decoder.shaped(frame).unwrap().1);
    }
    let shape = last.unwrap();
    Counted {
        values: shape.values,
        view_bytes: shape.view_bytes,
    }
}

#[test]
fn every_part_of_every_kind_of_column_compacts_to_the_same_rows() {
    let mut columns = samples::columns();
    columns.extend(samples::without_runs());
    for column in columns {
        let batch = batch_of(column);
        for start in 0..=batch.num_rows() {
            for rows in 0..=batch.num_rows() - start {
                let part = batch.slice(start, rows);
                let compact = compacted(&part).unwrap();
                assert_eq!(compact, part, "{start}+{rows} of {}", batch.schema());
                assert_eq!(counted(&compact), walked(&compact), "{}", batch.schema());
            }
        }
    }
    assert_eq!(compacted(&samples::batch()).unwrap().num_rows(), ROWS);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(256)))]

    #[test]
    fn every_part_of_a_drawn_batch_compacts_to_the_same_rows(
        drawn in values::drawn(),
        (start, rows) in (0_usize..8, 0_usize..8),
    ) {
        let batch = crate::codec::tests::batch(&drawn);
        let start = start.min(batch.num_rows());
        let rows = rows.min(batch.num_rows() - start);
        let part = batch.slice(start, rows);
        let compact = compacted(&part).unwrap();
        prop_assert_eq!(counted(&compact), walked(&compact));
        prop_assert_eq!(compact, part);
    }
}

#[test]
fn every_part_of_every_odd_column_compacts_to_the_same_rows_of_the_same_type() {
    for (name, column) in odd::columns() {
        let batch = batch_of(column);
        let rows = odd::rendered(&batch);
        for start in 0..=batch.num_rows() {
            for length in 0..=batch.num_rows() - start {
                let part = batch.slice(start, length);
                let compact = match compacted(&part) {
                    Ok(compact) => compact,
                    Err(error) => panic!("{name} {start}+{length}: {error}"),
                };
                assert_eq!(compact.schema(), part.schema(), "{name} {start}+{length}");
                assert_eq!(
                    odd::rendered(&compact),
                    rows[start..start + length],
                    "{name} {start}+{length}"
                );
                // The rows cross the wire as they were.
                let (mut decoder, frames) = sent(&compact, Limits::default());
                let mut crossed = None;
                for frame in &frames {
                    crossed = decoder.frame(frame).unwrap();
                }
                assert_eq!(
                    odd::rendered(&crossed.unwrap()),
                    rows[start..start + length],
                    "{name} {start}+{length}"
                );
            }
        }
    }
}
