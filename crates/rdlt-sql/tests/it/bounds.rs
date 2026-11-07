//! Every listed function, and every predicate, within the bound its query declares, on a batch
//! and on the parts the engine cuts it into.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use proptest::prelude::*;
use rdlt_engine::{Transform, allocated_beyond};
use rdlt_sql::Sql;

use crate::queries::{EXPRESSIONS, PREDICATES, batches, large, query};

/// `batch` whole, in halves, its first row, and all but its first row: slices at offsets.
fn cuts(batch: &RecordBatch) -> Vec<RecordBatch> {
    let rows = batch.num_rows();
    let half = rows / 2;
    vec![
        batch.clone(),
        batch.slice(0, half),
        batch.slice(half, rows - half),
        batch.slice(0, 1),
        batch.slice(1, rows - 1),
    ]
}

/// `batch` cut into `parts` slices, as the stage cuts a batch in halves and halves again.
fn parts(batch: &RecordBatch, parts: usize) -> Vec<RecordBatch> {
    let rows = batch.num_rows();
    let starts: Vec<usize> = (0..=parts).map(|part| part * rows / parts).collect();
    let pairs = starts.windows(2);
    pairs
        .map(|pair| batch.slice(pair[0], pair[1] - pair[0]))
        .collect()
}

/// Asserts that what `text` allocates on `part`, its output and its filter's answer, stays
/// within the bound it declares.
fn within(text: &str, sql: &Sql, part: &RecordBatch) -> Result<(), TestCaseError> {
    let Some(bound) = sql.bound(part.schema_ref()) else {
        let layout = part.schema_ref().field(0).data_type().clone();
        let refused = sql.output(part.schema_ref()).err();
        panic!("{text} on {layout}: {refused:?}");
    };
    let keep = sql.keep(part).expect("the filter runs");
    let kept = RecordBatch::try_from_iter([("k", Arc::new(keep) as ArrayRef)]);
    let kept = kept.expect("an answer is a column");
    let output = sql.apply(part.clone()).expect("the query runs");
    let allocated = allocated_beyond(&output, part) + allocated_beyond(&kept, part);
    let limit = bound.of(part);
    let rows = part.num_rows();
    prop_assert!(
        allocated <= limit,
        "{text} on {rows} rows: {allocated} > {limit}"
    );
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(48)))]

    #[test]
    fn what_a_query_allocates_stays_within_its_bound(batches in batches()) {
        let predicates = PREDICATES.iter().copied().map(Some).chain([None]);
        for (expression, predicate) in EXPRESSIONS.iter().zip(predicates.cycle()) {
            let text = query(expression, predicate);
            let sql = Sql::parse(&text).unwrap_or_else(|error| panic!("{text}: {error}"));
            for batch in batches.iter().flat_map(cuts) {
                within(&text, &sql, &batch)?;
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(6)))]

    #[test]
    fn what_a_query_allocates_on_slices_of_a_megabyte_of_text_stays_within_its_bound(
        batch in large(),
    ) {
        let predicates = PREDICATES.iter().copied().map(Some).chain([None]);
        for (expression, predicate) in EXPRESSIONS.iter().zip(predicates.cycle()) {
            let text = query(expression, predicate);
            let sql = Sql::parse(&text).unwrap_or_else(|error| panic!("{text}: {error}"));
            for cut in [1, 2, 4, 8] {
                for part in parts(&batch, cut) {
                    within(&text, &sql, &part)?;
                }
            }
        }
    }
}
