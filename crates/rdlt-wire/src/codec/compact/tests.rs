use proptest::prelude::*;
use rdlt_testkit::drawn::values;

use super::compacted;
use crate::codec::tests::samples::{self, ROWS, batch_of};

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
        prop_assert_eq!(compacted(&part).unwrap(), part);
    }
}
