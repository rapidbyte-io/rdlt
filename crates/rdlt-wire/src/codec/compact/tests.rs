use proptest::prelude::*;
use rdlt_testkit::drawn::values;

use super::compacted;
use crate::codec::tests::frames::sent;
use crate::codec::tests::odd;
use crate::codec::tests::samples::{self, ROWS, batch_of};
use crate::limits::Limits;

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

#[test]
fn copying_what_rows_name_holds_a_bounded_run_of_indices_whatever_they_name() {
    // Three rows naming the same three hundred thousand items: each row a range of its own.
    let items = arrow_array::Int8Array::from(vec![7; 300_000]);
    let field = arrow_schema::Field::new("item", arrow_schema::DataType::Int8, true);
    let field = std::sync::Arc::new(field);
    let (offsets, sizes) = (vec![0; 3], vec![300_000; 3]);
    let lists = arrow_array::ListViewArray::new(
        field,
        offsets.into(),
        sizes.into(),
        std::sync::Arc::new(items),
        None,
    );
    let batch = batch_of(std::sync::Arc::new(lists));
    let held = || super::leaf::HELD.replace(0);
    assert_eq!(compacted(&batch).unwrap(), batch);
    assert_eq!(held(), super::leaf::INDICES);
    // Rows naming one stretch of items between them take no indices at all.
    let apart = batch.slice(0, 1);
    assert_eq!(compacted(&apart).unwrap(), apart);
    assert_eq!(held(), 0);
}

#[test]
fn ranges_that_follow_one_another_are_named_as_one() {
    let mut ranges = Vec::new();
    for (start, end) in [(0, 5), (5, 9), (9, 9), (12, 11), (3, 4), (4, 6), (0, 1)] {
        super::name(&mut ranges, start, end);
    }
    assert_eq!(ranges, [(0, 9), (3, 6), (0, 1)]);
}

#[test]
fn a_part_of_views_of_bytes_keeps_only_the_bytes_its_rows_name() {
    let blobs = (0..500).map(|at| format!("bytes too long for a view to hold, {at:04}"));
    let blobs = arrow_array::BinaryViewArray::from_iter_values(blobs.map(String::into_bytes));
    let part = batch_of(std::sync::Arc::new(blobs.slice(250, 3)));
    let narrowed = compacted(&part).unwrap();
    assert_eq!(narrowed, part);
    let held = arrow_array::cast::AsArray::as_binary_view(narrowed.column(0)).data_buffers();
    let held: usize = held.iter().map(arrow_buffer::Buffer::len).sum();
    assert_eq!(held, 3 * 39);
}

#[test]
fn rows_in_no_range_begin_no_run() {
    // A run-end column rebuilt from ranges with an empty one between them.
    let values = arrow_array::Int32Array::from(vec![1, 2]);
    let ends = arrow_array::Int32Array::from(vec![2, 4]);
    let runs = arrow_array::RunArray::try_new(&ends, &values).unwrap();
    let runs: arrow_array::ArrayRef = std::sync::Arc::new(runs);
    let mut narrower = super::Narrower::default();
    let rebuilt = narrower.gathered(&runs, &[(0, 1), (3, 3), (1, 2)]).unwrap();
    let rebuilt = arrow_array::cast::AsArray::as_run::<arrow_array::types::Int32Type>(&rebuilt);
    assert_eq!(rebuilt.run_ends().values(), [2]);
}
