//! Arrow pushes whose columns of JSON name far more values than they hold: checked within the
//! budget, each value once, nothing held a value its rows name.

use std::sync::Arc;

use arrow_array::types::{Int8Type, Int32Type};
use arrow_array::{
    Array, ArrayRef, DictionaryArray, Int8Array, Int32Array, ListArray, RecordBatch, RunArray,
    StringArray,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{Field, Schema};
use rdlt_engine::RunOutcome;

use crate::HEAP;
use crate::support::destinations::null;
use crate::support::making::{Step, Steps, making};
use crate::support::{commit_every, engine, pipeline, stream};

/// The heap a run may reach under `budget`, as the engine documents it.
fn bound(budget: u64) -> usize {
    usize::try_from(budget * 12 / 10 + (32 << 20)).expect("a bound in memory")
}

/// A batch of one row, a list of `items` items of JSON that `values` holds.
fn one_long_list(items: i32, values: ArrayRef) -> RecordBatch {
    let extension = [("ARROW:extension:name".to_owned(), "arrow.json".to_owned())];
    let item = Field::new("item", values.data_type().clone(), true).with_metadata(extension.into());
    let list = ListArray::new(
        Arc::new(item),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, items])),
        values,
        None,
    );
    let field = Field::new("c", list.data_type().clone(), true);
    RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![Arc::new(list)])
        .expect("a list of one row")
}

/// Loads `batch` under `budget`: the most the heap held beyond what it held before, and how the
/// run ended.
async fn run(name: &str, budget: u64, batch: RecordBatch) -> (usize, RunOutcome) {
    let steps: Steps = Arc::new(move |step| (step < 1).then(|| Step::Batch(batch.clone())));
    let source = making(name, steps).await;
    let config = commit_every(1_000_000_000).memory(budget).lanes(1);
    HEAP.reset_peak_usage();
    let before = HEAP.current_usage();
    let outcome = engine(config)
        .run(pipeline(name, [stream("events")]), source, null().await)
        .await;
    (HEAP.peak_usage().saturating_sub(before), outcome)
}

#[tokio::test(start_paused = true)]
async fn a_run_naming_sixty_million_items_is_checked_within_the_least_memory() {
    const BUDGET: u64 = 34 << 20;
    const ITEMS: i32 = 60 << 20;
    // 452 bytes: one row whose list names one run of sixty million items of JSON.
    let one: ArrayRef = Arc::new(StringArray::from(vec!["1"]));
    let runs =
        RunArray::<Int32Type>::try_new(&Int32Array::from(vec![ITEMS]), &one).expect("one run");
    let (peak, outcome) = run("checked_runs", BUDGET, one_long_list(ITEMS, Arc::new(runs))).await;
    let error = outcome.error.expect("no budget lowers the row");
    assert_eq!(error.code(), Some("row_exceeds_budget"), "{error:?}");
    assert!(peak <= bound(BUDGET), "the heap held {peak} bytes");
}

#[tokio::test(start_paused = true)]
async fn keys_of_a_byte_naming_one_value_are_checked_within_the_budget() {
    const BUDGET: u64 = 256 << 20;
    const KEYS: i32 = 8 << 20;
    // Eight million keys of a byte, all naming one value of JSON.
    let one: ArrayRef = Arc::new(StringArray::from(vec!["1"]));
    let keys = Int8Array::from(vec![0_i8; usize::try_from(KEYS).expect("a count")]);
    let keyed = DictionaryArray::<Int8Type>::try_new(keys, one).expect("keys name the value");
    let (peak, outcome) = run("checked_keys", BUDGET, one_long_list(KEYS, Arc::new(keyed))).await;
    let error = outcome.error.expect("no request lowers the row");
    assert_eq!(error.code(), Some("row_exceeds_budget"), "{error:?}");
    assert!(peak <= bound(BUDGET), "the heap held {peak} bytes");
}

#[tokio::test(start_paused = true)]
async fn what_checking_holds_beside_a_push_is_reserved_before_it_is_held() {
    const ROWS: usize = 1 << 18;
    // Null rows whose views name items out of order, and one valid row naming text that is not
    // JSON: the check gathers a span a row before it reads that text and fails the write.
    let extension = [("ARROW:extension:name".to_owned(), "arrow.json".to_owned())];
    let item =
        Field::new("item", arrow_schema::DataType::Utf8, true).with_metadata(extension.into());
    let rows = i32::try_from(ROWS).expect("a count");
    let views = arrow_array::ListViewArray::new(
        Arc::new(item),
        ScalarBuffer::from((0..rows).rev().collect::<Vec<_>>()),
        ScalarBuffer::from(vec![1; ROWS]),
        Arc::new(StringArray::from_iter_values((0..rows).map(|row| {
            if row == 0 {
                "{".to_owned()
            } else {
                row.to_string()
            }
        }))),
        Some(arrow_buffer::NullBuffer::from_iter(
            (0..ROWS).map(|row| row == ROWS - 1),
        )),
    );
    let field = Field::new("c", views.data_type().clone(), true);
    let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![Arc::new(views)])
        .expect("a list view");
    let pushed = u64::try_from(batch.get_array_memory_size()).expect("a size");
    let (_, outcome) = run("checked_views", 256 << 20, batch).await;
    let error = outcome.error.expect("the text is not JSON");
    assert_eq!(error.code(), Some("json_invalid"), "{error:?}");
    // The push and the spans gathered beside it, both reserved at once.
    let gathered = 16 * u64::try_from(ROWS).expect("a count");
    assert!(
        outcome.report.peak_memory >= pushed + gathered,
        "the budget held at most {} bytes",
        outcome.report.peak_memory
    );
}
