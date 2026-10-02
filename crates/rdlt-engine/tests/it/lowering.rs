//! What a run's heap reaches against its budget where lowering makes far more than was pushed:
//! columns stored wider than they arrive, rows of most of the budget, schemas of megabytes, tables
//! far wider than a batch, and a budget only reads hold.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::types::Int32Type;
use arrow_array::{
    ArrayRef, Decimal256Array, DictionaryArray, Int8Array, Int32Array, Int64Array, ListViewArray,
    NullArray, RecordBatch, StringArray,
};
use arrow_buffer::{ScalarBuffer, i256};
use arrow_schema::{DataType, Field, Schema};
use rdlt_connector::{
    BoxFuture, Catalog, Cursor, PartitionId, PartitionPlan, PartitionSink, ReadRequest, Source,
    StreamName, StreamState,
};
use rdlt_engine::{ErrorKind, RetryPolicy, RunOutcome, RunStatus};

use crate::HEAP;
use crate::support::destinations::null;
use crate::support::making::{Step, Steps, making};
use crate::support::{commit_every, engine, pipeline, stream};

const BUDGET: u64 = 16 << 20;

fn batch(column: ArrayRef) -> RecordBatch {
    RecordBatch::try_from_iter([("column", column)]).expect("one column makes a batch")
}

/// The heap a run may reach under the budget, as the engine documents it.
fn bound() -> usize {
    usize::try_from(BUDGET * 12 / 10 + (32 << 20)).expect("a bound in memory")
}

/// Runs a load of what `steps` makes under the budget: the most the heap held beyond what it
/// held before, and how the run ended.
async fn run(name: &str, source: Arc<dyn Source>) -> (usize, RunOutcome) {
    let config = commit_every(1_000_000_000).memory(BUDGET).lanes(1);
    HEAP.reset_peak_usage();
    let before = HEAP.current_usage();
    let outcome = engine(config)
        .run(pipeline(name, [stream("events")]), source, null().await)
        .await;
    (HEAP.peak_usage().saturating_sub(before), outcome)
}

/// Loads what `steps` makes and asserts every row loaded within the heap's bound.
async fn loaded(name: &str, steps: Steps, rows: u64) {
    let (peak, outcome) = run(name, making(name, steps).await).await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert_eq!(outcome.report.rows, rows);
    assert!(peak <= bound(), "the heap held {peak} bytes");
}

#[tokio::test(start_paused = true)]
async fn small_integers_into_a_column_of_256_bit_decimals_load_within_the_budget() {
    // One row makes the column a 256-bit decimal; sixteen million bytes then become 512 MiB.
    let steps: Steps = Arc::new(|step| match step {
        0 => {
            let wide = Decimal256Array::from(vec![i256::from_i128(1)])
                .with_precision_and_scale(76, 0)
                .expect("a decimal");
            Some(Step::Batch(batch(Arc::new(wide))))
        }
        1 => Some(Step::Checkpoint(8)),
        2..=17 => {
            let small = Int8Array::from(vec![1_i8; 1_000_000]);
            Some(Step::Batch(batch(Arc::new(small))))
        }
        _ => None,
    });
    loaded("lowering_widened", steps, 16_000_001).await;
}

#[tokio::test(start_paused = true)]
async fn control_characters_into_a_column_of_json_load_within_the_budget() {
    // One integer makes the column JSON; each control character then takes six bytes of text.
    let steps: Steps = Arc::new(|step| match step {
        0 => Some(Step::Batch(batch(Arc::new(Int64Array::from(vec![1_i64]))))),
        1 => Some(Step::Checkpoint(8)),
        2..=17 => {
            let text = "\u{1}".repeat(1_000);
            let strings = StringArray::from(vec![text.as_str(); 1_000]);
            Some(Step::Batch(batch(Arc::new(strings))))
        }
        _ => None,
    });
    loaded("lowering_json", steps, 16_001).await;
}

#[tokio::test(start_paused = true)]
async fn rows_each_of_most_of_the_budget_are_lowered_one_at_a_time() {
    // Sixty-four keys of one string of twelve mebibytes: each row is a piece of its own.
    let steps: Steps = Arc::new(|step| {
        let long = "x".repeat(12 << 20);
        let keyed = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![0; 64]),
            Arc::new(StringArray::from(vec![long.as_str()])),
        );
        (step < 1).then(|| Step::Batch(batch(Arc::new(keyed.expect("a dictionary")))))
    });
    loaded("lowering_rows", steps, 64).await;
}

#[tokio::test(start_paused = true)]
async fn a_row_that_takes_more_than_the_budget_to_lower_fails_the_run_and_is_never_lowered() {
    // One string of twenty mebibytes, more than the budget of sixteen.
    let steps: Steps = Arc::new(|step| {
        (step < 1).then(|| {
            let long = "x".repeat(20 << 20);
            let keyed = DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![0; 4]),
                Arc::new(StringArray::from(vec![long.as_str()])),
            );
            Step::Batch(batch(Arc::new(keyed.expect("a dictionary"))))
        })
    });
    let (peak, outcome) = run("lowering_refused", making("lowering_refused", steps).await).await;
    let error = outcome.error.expect("the run fails");
    assert_eq!(
        (error.kind(), error.code()),
        (ErrorKind::Source, Some("row_exceeds_budget"))
    );
    assert!(error.to_string().contains("16777216"), "{error}");
    // The source's own string and the batch's: nothing was lowered beside them.
    assert!(peak < (2 * 20 + 4) << 20, "the heap held {peak} bytes");
}

#[tokio::test(start_paused = true)]
async fn batches_each_holding_a_schema_of_their_own_load_within_the_budget() {
    // Sixteen columns named in 64 KiB each: a megabyte of schema, built anew each push.
    let steps: Steps = Arc::new(|step| {
        let fields: Vec<Field> = (0..16)
            .map(|index| {
                let name = format!("{index:02}{}", "n".repeat((64 << 10) - 2));
                Field::new(name, DataType::Int8, true)
            })
            .collect();
        let columns = (0..16).map(|_| Arc::new(Int8Array::from(vec![1_i8])) as ArrayRef);
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns.collect());
        (step < 200).then(|| Step::Batch(batch.expect("a batch")))
    });
    loaded("lowering_schemas", steps, 200).await;
}

#[tokio::test(start_paused = true)]
async fn rows_of_a_table_far_wider_than_their_batch_load_within_the_budget() {
    // One row gives the table two hundred columns of 256-bit decimals; rows of one small column
    // then take 6.6 kilobytes of nulls each, 660 MB a push.
    let steps: Steps = Arc::new(|step| match step {
        0 => {
            let wide = Decimal256Array::from(vec![i256::from_i128(1)])
                .with_precision_and_scale(76, 0)
                .expect("a decimal");
            let wide: ArrayRef = Arc::new(wide);
            let columns = (0..200).map(|index| (format!("wide{index:03}"), Arc::clone(&wide)));
            let batch = RecordBatch::try_from_iter(columns).expect("a batch");
            Some(Step::Batch(batch))
        }
        1 => Some(Step::Checkpoint(8)),
        2..=4 => {
            let small: ArrayRef = Arc::new(Int8Array::from(vec![1_i8; 100_000]));
            let batch = RecordBatch::try_from_iter([("narrow", small)]).expect("a batch");
            Some(Step::Batch(batch))
        }
        _ => None,
    });
    loaded("lowering_fill", steps, 300_001).await;
}

#[tokio::test(start_paused = true)]
async fn a_list_view_over_a_long_child_of_no_bytes_loads_within_the_budget() {
    // Four rows each naming one of sixty million nulls, which hold no bytes.
    let steps: Steps = Arc::new(|step| {
        let views = ListViewArray::new(
            Arc::new(Field::new("item", DataType::Null, true)),
            ScalarBuffer::from(vec![7_i32; 4]),
            ScalarBuffer::from(vec![1_i32; 4]),
            Arc::new(NullArray::new(60_000_000)),
            None,
        );
        (step < 1).then(|| Step::Batch(batch(Arc::new(views))))
    });
    loaded("lowering_views", steps, 4).await;
}

/// A source whose every read keeps `bytes` beside its events for as long as it lasts, as a
/// remote read keeps its decoder's dictionaries.
struct Keeping {
    source: Arc<dyn Source>,
    bytes: u64,
}

impl Source for Keeping {
    fn check(&self) -> BoxFuture<'_, rdlt_connector::Result<()>> {
        self.source.check()
    }

    fn discover(&self) -> BoxFuture<'_, rdlt_connector::Result<Catalog>> {
        self.source.discover()
    }

    fn plan<'a>(
        &'a self,
        stream: &'a StreamName,
        state: &'a StreamState,
    ) -> BoxFuture<'a, rdlt_connector::Result<PartitionPlan>> {
        self.source.plan(stream, state)
    }

    fn committed<'a>(
        &'a self,
        stream: &'a StreamName,
        cursors: &'a [(PartitionId, Cursor)],
    ) -> BoxFuture<'a, rdlt_connector::Result<()>> {
        self.source.committed(stream, cursors)
    }

    fn read(
        &self,
        request: ReadRequest,
        sink: PartitionSink,
    ) -> BoxFuture<'_, rdlt_connector::Result<()>> {
        Box::pin(async move {
            let _kept = sink.reserve(self.bytes);
            self.source.read(request, sink).await
        })
    }
}

#[tokio::test(start_paused = true)]
async fn a_budget_only_reads_hold_fails_the_attempt_with_what_held_it() {
    const WAIT: Duration = Duration::from_secs(120);
    // The read keeps the whole budget, then sends a row and a checkpoint: the checkpoint's
    // cursor finds no room, and nothing in flight or waiting for a commit could make any.
    let steps: Steps = Arc::new(|step| match step {
        0 => Some(Step::Batch(batch(Arc::new(Int64Array::from(vec![1_i64]))))),
        1 => Some(Step::Checkpoint(64)),
        _ => None,
    });
    let source = Arc::new(Keeping {
        source: making("lowering_kept", steps).await,
        bytes: BUDGET,
    });
    let config = commit_every(1_000_000_000)
        .memory(BUDGET)
        .memory_wait(WAIT)
        .retry(RetryPolicy::default().max_attempts(1))
        .lanes(1);
    let started = tokio::time::Instant::now();
    let outcome = engine(config)
        .run(
            pipeline("lowering_kept", [stream("events")]),
            source,
            null().await,
        )
        .await;
    let error = outcome.error.expect("the run fails");
    assert_eq!(
        (error.kind(), error.code()),
        (ErrorKind::Memory, Some("memory_budget_wait_exceeded"))
    );
    assert!(error.is_retryable());
    let said = format!("{:?}", error.report());
    assert!(said.contains("16777216 are kept by reads"), "{said}");
    assert!(
        started.elapsed() < WAIT + Duration::from_secs(30),
        "{:?}",
        started.elapsed()
    );
}
