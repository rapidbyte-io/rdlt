//! JSON pushes whose records become far more than their text: refused before anything is built
//! where they would pass a limit or the budget, and built within the budget where they fit.

use std::sync::Arc;

use std::time::Duration;

use rdlt_engine::{BatchPolicy, ErrorKind, RunOutcome, RunStatus};

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
    run_chunked(name, budget, 1 << 20, text).await
}

/// Loads the JSON push `text` under `budget`, shredded in chunks of `chunk_bytes`: the most the
/// heap held beyond what it held before, and how the run ended.
async fn run_chunked(
    name: &str,
    budget: u64,
    chunk_bytes: usize,
    text: String,
) -> (usize, RunOutcome) {
    let pushed = bytes::Bytes::from(text);
    let steps: Steps = Arc::new(move |step| (step < 1).then(|| Step::Json(pushed.clone())));
    let source = making(name, steps).await;
    let batch = BatchPolicy::new(8 << 20, 1 << 20, Duration::from_secs(1), chunk_bytes)
        .expect("a valid policy");
    let config = commit_every(1_000_000_000)
        .memory(budget)
        .lanes(1)
        .batch(batch);
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

#[tokio::test(start_paused = true)]
async fn narrow_records_after_a_wide_one_fill_its_table_within_the_budget() {
    // One record gives the table nine hundred columns; twenty thousand records of one key then
    // take a null in each, 160 MB from 160 KB pushed.
    let wide = bytes::Bytes::from(wide_object(0, 900, "1"));
    let narrow = bytes::Bytes::from(vec!["{\"a\":1}"; 20_000].join("\n"));
    let steps: Steps = Arc::new(move |step| match step {
        0 => Some(Step::Json(wide.clone())),
        1 => Some(Step::Checkpoint(8)),
        2 => Some(Step::Json(narrow.clone())),
        _ => None,
    });
    let source = making("narrow_after_wide", steps).await;
    let config = commit_every(1_000_000_000).memory(LEAST).lanes(1);
    HEAP.reset_peak_usage();
    let before = HEAP.current_usage();
    let outcome = engine(config)
        .run(
            pipeline("narrow_after_wide", [stream("events")]),
            source,
            null().await,
        )
        .await;
    let peak = HEAP.peak_usage().saturating_sub(before);
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert_eq!(outcome.report.rows, 20_001);
    assert!(peak <= bound(LEAST), "the heap held {peak} bytes");
}

/// A push of `chunks` records of 7,480 keys, each followed by a record of filler that fills its
/// chunk of `chunk_bytes`.
fn wide_chunks(chunks: usize, chunk_bytes: usize) -> String {
    let wide = wide_object(0, 7_480, "1");
    let fill = chunk_bytes.saturating_sub(wide.len()).max(1);
    let filler = format!("{{\"pad\":\"{}\"}}", "x".repeat(fill));
    let mut text = String::new();
    for _ in 0..chunks {
        text.push_str(&wide);
        text.push('\n');
        text.push_str(&filler);
        text.push('\n');
    }
    text
}

#[tokio::test(start_paused = true)]
async fn wide_records_in_small_chunks_are_held_within_the_bound() {
    const BUDGET: u64 = 256 << 20;
    // Three hundred and eighty chunks of 64 KiB, each with all seven thousand columns.
    let text = wide_chunks(380, 64 << 10);
    let (peak, outcome) = run_chunked("wide_small_chunks", BUDGET, 64 << 10, text).await;
    if let Some(error) = &outcome.error {
        assert_eq!(error.kind(), ErrorKind::Source, "{error:?}");
    }
    assert!(peak <= bound(BUDGET), "the heap held {peak} bytes");
}

#[tokio::test(start_paused = true)]
async fn wide_records_in_full_chunks_are_charged_what_they_hold() {
    const BUDGET: u64 = 256 << 20;
    // Chunks of a megabyte, each with all seven thousand columns: their columns' parts a chunk
    // are charged, so the heap stays within what the budget held, a fifth and 32 MiB, and what
    // takes more than a request is refused before it is built.
    for (chunks, loads) in [(8, true), (26, false)] {
        let name = format!("wide_full_chunks_{chunks}");
        let (peak, outcome) =
            run_chunked(&name, BUDGET, 1 << 20, wide_chunks(chunks, 1 << 20)).await;
        match &outcome.error {
            None => assert!(loads, "{chunks} chunks loaded"),
            Some(error) => {
                assert!(!loads, "{error:?}");
                assert_eq!(error.code(), Some("json_exceeds_budget"), "{error:?}");
            }
        }
        let held = outcome.report.peak_memory;
        let within = usize::try_from(held + held / 5 + (32 << 20)).expect("a bound in memory");
        assert!(
            peak <= within,
            "{chunks} chunks: the heap held {peak} bytes, the budget {held}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn observing_a_push_reserves_what_its_columns_may_hold_beyond_its_text() {
    // However small, a push's chunks may hold a shape of every column the records may hold
    // beyond what the push was admitted for: 576 bytes a column, of the 7,489 a schema may hold
    // under the default budget, reserved while the push is observed.
    let (_, outcome) = run("observing_reserved", 256 << 20, "{\"a\":1}".to_owned()).await;
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    assert!(
        outcome.report.peak_memory >= 7_489 * 576,
        "the budget held at most {} bytes",
        outcome.report.peak_memory
    );
}
