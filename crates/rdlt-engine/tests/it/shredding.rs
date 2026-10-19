//! JSON pushes whose records become far more than their text: refused before anything is built
//! where they would pass a limit or the budget, and built within the budget where they fit.

use std::sync::Arc;

use std::time::Duration;

use rdlt_engine::{
    BatchPolicy, EngineConfig, ErrorKind, Nested, RetryPolicy, RunOutcome, RunStatus,
    SchemaSettings, ShredCounts,
};

use crate::HEAP;
use crate::support::destinations::null;
use crate::support::making::{Step, Steps, making, making_parts};
use crate::support::{commit_every, engine, pipeline, stream, yielding_engine};

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
async fn a_stream_reports_the_passes_shredding_its_committed_json_took() {
    // A hundred records of an integer, then a hundred of an object, which spoils the column: the
    // chunk is parsed once and built again as JSON text. The note makes the chunk's text hold
    // what its builders take.
    let note = "x".repeat(64);
    let records = |value: &str| vec![format!("{{\"a\":{value},\"n\":\"{note}\"}}"); 100].join("\n");
    let text = format!("{}\n{}", records("1"), records(r#"{"b":1}"#));
    let (_, outcome) = run("shred_counts", 1 << 30, text).await;
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    let shred = ShredCounts {
        parsed: 1,
        rebuilt: 1,
        ..ShredCounts::default()
    };
    assert_eq!(outcome.report.streams["events"].shred, shred);
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

/// Loads `chunks` chunks of two megabytes, each with all seven thousand columns, under the default
/// budget: their columns' parts a chunk are charged, so the heap stays within what the budget
/// held, a fifth and 32 MiB, and what takes more than a request is refused before it is built.
async fn wide_full_chunks(chunks: usize) -> RunOutcome {
    const BUDGET: u64 = 256 << 20;
    let name = format!("wide_full_chunks_{chunks}");
    let (peak, outcome) = run_chunked(&name, BUDGET, 2 << 20, wide_chunks(chunks, 2 << 20)).await;
    let held = outcome.report.peak_memory;
    let within = usize::try_from(held + held / 5 + (32 << 20)).expect("a bound in memory");
    assert!(
        peak <= within,
        "{chunks} chunks: the heap held {peak} bytes, the budget {held}"
    );
    outcome
}

#[tokio::test(start_paused = true)]
async fn wide_records_in_a_few_full_chunks_load_charged_what_they_hold() {
    let outcome = wide_full_chunks(4).await;
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
}

#[tokio::test(start_paused = true)]
async fn wide_records_in_many_full_chunks_are_refused_charged_what_they_hold() {
    let outcome = wide_full_chunks(13).await;
    let error = outcome
        .error
        .expect("their batches take more than a request");
    assert_eq!(error.code(), Some("json_exceeds_budget"), "{error:?}");
}

#[tokio::test(start_paused = true)]
async fn observing_a_push_reserves_what_its_columns_may_hold_beyond_its_text() {
    // However small, a push's chunks may hold a shape of every column the records may hold
    // beyond what the push was admitted for: 752 bytes a column, of the 7,489 a schema may hold
    // under the default budget, reserved while the push is observed.
    let (_, outcome) = run("observing_reserved", 256 << 20, "{\"a\":1}".to_owned()).await;
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    assert!(
        outcome.report.peak_memory >= 7_489 * 752,
        "the budget held at most {} bytes",
        outcome.report.peak_memory
    );
}

/// Loads, under `memory`, `parts` partitions each pushing 600 records of a key each, whose nulls
/// take building more than the push was admitted for, on a compute pool whose jobs let every
/// other task run first; `normalized` normalizes the stream.
async fn sparse_partitions(name: &str, parts: usize, memory: u64, normalized: bool) -> RunOutcome {
    const RECORDS: usize = 600;
    let records: Vec<String> = (0..RECORDS)
        .map(|record| format!("{{\"k{record}\":{record}}}"))
        .collect();
    let pushed = bytes::Bytes::from(records.join("\n"));
    let steps: Steps = Arc::new(move |step| (step < 1).then(|| Step::Json(pushed.clone())));
    let source = making_parts(name, steps, parts).await;
    let config = commit_every(1_000_000_000)
        .memory(memory)
        .partitions(parts)
        .memory_wait(Duration::from_secs(60))
        .retry(RetryPolicy::default().max_attempts(1));
    let plan = if normalized {
        stream("events").schema(SchemaSettings::new().nested(Nested::normalize()))
    } else {
        stream("events")
    };
    let outcome = yielding_engine(config)
        .run(pipeline(name, [plan]), source, null().await)
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{parts} partitions under {memory} bytes: {:?}",
        outcome.error
    );
    assert_eq!(outcome.report.rows, (parts * RECORDS) as u64);
    assert!(outcome.report.peak_memory <= memory);
    outcome
}

#[tokio::test(start_paused = true)]
async fn partitions_holding_what_building_their_json_took_lower_it_without_waiting_on_each_other() {
    // Pressed: partitions held what their JSON's batches took beyond their pushes while they
    // waited for the budget to lower them, until those holds left no piece room.
    sparse_partitions("built_held", 32, 64 << 20, false).await;
}

#[tokio::test(start_paused = true)]
async fn normalizing_partitions_holding_what_building_their_json_took_never_wait_on_each_other() {
    sparse_partitions("built_held_normalized", 32, 64 << 20, true).await;
}

#[tokio::test(start_paused = true)]
async fn sparse_json_of_any_number_of_partitions_loads_under_any_budget() {
    for parts in [1, 4, 16, 32] {
        let least = EngineConfig::least_memory(parts).expect("a memory admits these partitions");
        for memory in [least, least * 3 / 2, least * 2, least * 4] {
            for normalized in [false, true] {
                sparse_partitions("built_held_swept", parts, memory, normalized).await;
            }
        }
    }
}

/// What building 1,500 records of a key each takes beyond twice their text.
const BUILT_BEYOND: u64 = 20_318_222;

/// Loads one partition's 1,500 records of a key each under a budget whose request is `beside`
/// bytes more than building them takes beyond their text.
async fn beside_a_build(name: &str, beside: u64) -> RunOutcome {
    let records: Vec<String> = (0..1_500)
        .map(|record| format!("{{\"k{record}\":{record}}}"))
        .collect();
    let pushed = bytes::Bytes::from(records.join("\n"));
    let steps: Steps = Arc::new(move |step| (step < 1).then(|| Step::Json(pushed.clone())));
    let source = making(name, steps).await;
    let config = commit_every(1_000_000_000)
        .memory((BUILT_BEYOND + beside) * 4)
        .memory_wait(Duration::from_secs(60))
        .retry(RetryPolicy::default().max_attempts(1));
    engine(config)
        .run(pipeline(name, [stream("events")]), source, null().await)
        .await
}

#[tokio::test(start_paused = true)]
async fn a_row_a_request_leaves_no_room_for_beside_its_jsons_build_is_refused_naming_both() {
    let outcome = beside_a_build("beside_build_refused", 8_000).await;
    let error = outcome.error.expect("the row is refused");
    assert_eq!(error.code(), Some("row_exceeds_budget"), "{error:?}");
    let shown = error.to_string();
    assert!(shown.contains(&BUILT_BEYOND.to_string()), "{shown}");
    assert!(
        shown.contains(&(BUILT_BEYOND + 8_000).to_string()),
        "{shown}"
    );
    let outcome = beside_a_build("beside_build_loaded", 200_000).await;
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    assert_eq!(outcome.report.rows, 1_500);
}

/// Loads 16 partitions under 256 MiB, each pushing forty times the same six hundred records of
/// ten keys of `keys`, in flushes of 256 KiB, into a destination taking 5 ms a write; normalized
/// where `normalized` says: how long the run took on the paused clock.
async fn optional_fields(name: &str, keys: usize, normalized: bool) -> Duration {
    const PARTS: usize = 16;
    let records: Vec<String> = (0..600)
        .map(|record| {
            let fields: Vec<String> = (0..10)
                .map(|key| format!("\"k{}\":{record}", (record * 10 + key) % keys))
                .collect();
            format!("{{{}}}", fields.join(","))
        })
        .collect();
    let pushed = bytes::Bytes::from(records.join("\n"));
    let steps: Steps = Arc::new(move |step| (step < 40).then(|| Step::Json(pushed.clone())));
    let source = making_parts(name, steps, PARTS).await;
    let batch = BatchPolicy::new(256 << 10, 1 << 20, Duration::from_secs(1), 1 << 20)
        .expect("a valid policy");
    let config = commit_every(1_000_000_000)
        .memory(256 << 20)
        .partitions(PARTS)
        .batch(batch)
        .retry(RetryPolicy::default().max_attempts(1));
    let plan = if normalized {
        stream("events").schema(SchemaSettings::new().nested(Nested::normalize()))
    } else {
        stream("events")
    };
    let began = tokio::time::Instant::now();
    let outcome = yielding_engine(config)
        .run(pipeline(name, [plan]), source, null().await)
        .await;
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    assert_eq!(outcome.report.rows, 600 * 40 * PARTS as u64);
    began.elapsed()
}

#[tokio::test(start_paused = true)]
async fn records_of_optional_fields_load_about_as_fast_as_records_of_all_of_them() {
    // Ten keys of sixty, or of six hundred: each record's batch holds the other keys' nulls,
    // more than its text was admitted for, which its partition holds while it lowers them.
    let dense = optional_fields("fields_all", 10, false).await;
    for (name, keys, normalized) in [
        ("fields_of_sixty", 60, false),
        ("fields_of_six_hundred", 600, false),
        ("fields_of_sixty_normalized", 60, true),
    ] {
        let sparse = optional_fields(name, keys, normalized).await;
        assert!(
            sparse <= dense * 3 / 2,
            "{name}: {sparse:?} against {dense:?}"
        );
    }
}
