//! JSON pushes whose records become far more than their text: refused before anything is built
//! where they would pass a limit or the budget, and built within the budget where they fit.

use std::sync::Arc;

use rdlt_engine::{ErrorKind, RunOutcome, RunStatus};

use crate::HEAP;
use crate::support::destinations::null;
use crate::support::making::{Step, Steps, making};
use crate::support::{commit_every, engine, pipeline, stream};

/// About the least memory an engine runs on.
const LEAST: u64 = 34 << 20;

/// The heap a run may reach under `budget`, as the engine documents it.
fn bound(budget: u64) -> usize {
    usize::try_from(budget * 12 / 10 + (32 << 20)).expect("a bound in memory")
}

/// Loads the JSON push `text` under `budget`: the most the heap held beyond what it held before,
/// and how the run ended.
async fn run(name: &str, budget: u64, text: String) -> (usize, RunOutcome) {
    let pushed = bytes::Bytes::from(text);
    let steps: Steps = Arc::new(move |step| (step < 1).then(|| Step::Json(pushed.clone())));
    let source = making(name, steps).await;
    let config = commit_every(1_000_000_000).memory(budget).lanes(1);
    HEAP.reset_peak_usage();
    let before = HEAP.current_usage();
    let outcome = engine(config)
        .run(pipeline(name, [stream("events")]), source, null().await)
        .await;
    (HEAP.peak_usage().saturating_sub(before), outcome)
}

/// Asserts the run failed with `code`, a source's limit, without the heap passing what a run
/// on the least memory may reach.
fn refused(outcome: &RunOutcome, peak: usize, code: &str) {
    let error = outcome.error.as_ref().expect("the run fails");
    assert_eq!(
        (error.kind(), error.code()),
        (ErrorKind::Source, Some(code)),
        "{error:?}"
    );
    assert!(peak <= bound(LEAST), "the heap held {peak} bytes");
}

/// An object of `count` distinct keys from `first` on, each holding `value`.
fn wide_object(first: usize, count: usize, value: &str) -> String {
    let fields: Vec<String> = (first..first + count)
        .map(|index| format!("\"c{index}\":{value}"))
        .collect();
    format!("{{{}}}", fields.join(","))
}

#[tokio::test(start_paused = true)]
async fn empty_records_before_a_wide_one_are_refused_for_their_cells_unbuilt() {
    // Five thousand rows under seven thousand columns: thirty-five million cells, from 85 KB.
    let text = format!("{}{}", "{}\n".repeat(5_000), wide_object(0, 7_000, "1"));
    let (peak, outcome) = run("sparse_cells", 1 << 30, text).await;
    refused(&outcome, peak, "limit_exceeded");
}

#[tokio::test(start_paused = true)]
async fn a_list_of_empty_objects_before_a_wide_one_counts_its_items_as_rows() {
    // One record: its list's five thousand items under seven thousand columns.
    let text = format!(
        "{{\"a\":[{}{}]}}",
        "{},".repeat(5_000),
        wide_object(0, 7_000, "1")
    );
    let (peak, outcome) = run("listed_cells", 1 << 30, text).await;
    refused(&outcome, peak, "limit_exceeded");
    // Within the cells, five thousand items under nine hundred columns take more than a request
    // may of the least budget.
    let text = format!(
        "{{\"a\":[{}{}]}}",
        "{},".repeat(5_000),
        wide_object(0, 900, "1")
    );
    let (peak, outcome) = run("listed_bytes", LEAST, text).await;
    refused(&outcome, peak, "json_exceeds_budget");
}

#[tokio::test(start_paused = true)]
async fn the_column_limit_holds_for_every_column_a_record_holds_together() {
    // Ten objects of a thousand fields each: no object is wide, the record is.
    let objects: Vec<String> = (0..10)
        .map(|object| format!("\"o{object}\":{}", wide_object(0, 1_000, "1")))
        .collect();
    let text = format!("{{{}}}", objects.join(","));
    let (peak, outcome) = run("total_columns", 1 << 30, text).await;
    refused(&outcome, peak, "limit_exceeded");
}

#[tokio::test(start_paused = true)]
async fn records_within_the_cells_that_take_more_than_a_request_are_refused_unbuilt() {
    // Three thousand three hundred rows under nine hundred columns of 39-digit integers: under
    // the cells' limit, and a hundred megabytes of decimals.
    let wide = "123456789012345678901234567890123456789";
    let text = format!("{}{}", "{}\n".repeat(3_300), wide_object(0, 900, wide));
    let (peak, outcome) = run("wide_decimals", LEAST, text).await;
    refused(&outcome, peak, "json_exceeds_budget");
}

#[tokio::test(start_paused = true)]
async fn a_megabyte_of_records_of_one_key_among_hundreds_is_refused_unbuilt() {
    // Each record names one key of three hundred: every row takes a cell in each.
    let records: Vec<String> = (0..95_000)
        .map(|record| format!("{{\"k{}\":1}}", record % 300))
        .collect();
    let (peak, outcome) = run("one_key_each", LEAST, records.join("\n")).await;
    refused(&outcome, peak, "json_exceeds_budget");
}

#[tokio::test(start_paused = true)]
async fn sparse_records_whose_batches_fit_a_request_load_within_the_budget() {
    const BUDGET: u64 = 64 << 20;
    // Three thousand rows under three hundred columns take a few megabytes beyond their text,
    // reserved before they are built.
    let records: Vec<String> = (0..3_000)
        .map(|record| format!("{{\"k{}\":{record}}}", record % 300))
        .collect();
    let (peak, outcome) = run("sparse_fits", BUDGET, records.join("\n")).await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert_eq!(outcome.report.rows, 3_000);
    assert!(peak <= bound(BUDGET), "the heap held {peak} bytes");
    assert!(outcome.report.peak_memory <= BUDGET);
}
