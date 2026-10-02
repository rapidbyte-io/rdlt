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
use crate::support::destinations::{Gate, gated, null, stalling};
use crate::support::making::{Step, Steps, making};
use crate::support::{commit_every, engine, pipeline, stream};

const BUDGET: u64 = 34 << 20;

/// About the least budget an engine reading one partition at once takes.
const ONE_READ: u64 = 54 << 20;

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
    // One row makes the column a 256-bit decimal; four million bytes then become 128 MiB, past
    // the heap's bound.
    let steps: Steps = Arc::new(|step| match step {
        0 => {
            let wide = Decimal256Array::from(vec![i256::from_i128(1)])
                .with_precision_and_scale(76, 0)
                .expect("a decimal");
            Some(Step::Batch(batch(Arc::new(wide))))
        }
        1 => Some(Step::Checkpoint(8)),
        2..=5 => {
            let small = Int8Array::from(vec![1_i8; 1_000_000]);
            Some(Step::Batch(batch(Arc::new(small))))
        }
        _ => None,
    });
    loaded("lowering_widened", steps, 4_000_001).await;
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
async fn rows_each_of_most_of_what_a_request_may_take_are_lowered_one_at_a_time() {
    // Sixty-four keys of one string of three mebibytes, where a frame may hold four and a request
    // for lowering take eight: each row is a piece of its own, and together they are six times
    // the budget.
    let steps: Steps = Arc::new(|step| {
        let long = "x".repeat(3 << 20);
        let keyed = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![0; 64]),
            Arc::new(StringArray::from(vec![long.as_str()])),
        );
        (step < 1).then(|| Step::Batch(batch(Arc::new(keyed.expect("a dictionary")))))
    });
    loaded("lowering_rows", steps, 64).await;
}

#[tokio::test(start_paused = true)]
async fn a_row_its_table_widens_beyond_what_a_request_may_take_fails_the_run_unlowered() {
    // One row makes the column a list of 256-bit decimals; a row of four million small integers
    // in a list, within a frame, then takes thirty-three times its bytes lowered: more than a
    // request for lowering may take, which a frame bounds only for a row lowered as it arrives.
    let lists = |values: ArrayRef, rows: usize| -> ArrayRef {
        let item = Arc::new(Field::new("item", values.data_type().clone(), true));
        let offsets = arrow_buffer::OffsetBuffer::from_lengths([values.len()]);
        assert_eq!(rows, 1);
        Arc::new(arrow_array::ListArray::new(item, offsets, values, None))
    };
    let steps: Steps = Arc::new(move |step| match step {
        0 => {
            let wide = Decimal256Array::from(vec![i256::from_i128(1)])
                .with_precision_and_scale(76, 0)
                .expect("a decimal");
            Some(Step::Batch(batch(lists(Arc::new(wide), 1))))
        }
        1 => Some(Step::Checkpoint(8)),
        2 => {
            let small = Int8Array::from(vec![1_i8; 4_000_000]);
            Some(Step::Batch(batch(lists(Arc::new(small), 1))))
        }
        _ => None,
    });
    let (peak, outcome) = run("lowering_refused", making("lowering_refused", steps).await).await;
    let error = outcome.error.expect("the run fails");
    assert_eq!(
        (error.kind(), error.code()),
        (ErrorKind::Source, Some("row_exceeds_budget"))
    );
    let request = (BUDGET / 4).to_string();
    assert!(error.to_string().contains(&request), "{error}");
    // The source's own integers and what admitted them: nothing was lowered beside them.
    assert!(peak < 32 << 20, "the heap held {peak} bytes");
    assert!(outcome.report.peak_memory < 6 << 20);
}

#[tokio::test(start_paused = true)]
async fn batches_each_holding_a_schema_of_their_own_load_within_the_budget() {
    // Sixteen columns named in three kilobytes each, as long a schema as a source is told it may
    // send: forty-eight kilobytes of schema, built anew each push.
    let steps: Steps = Arc::new(|step| {
        let fields: Vec<Field> = (0..16)
            .map(|index| {
                let name = format!("{index:02}{}", "n".repeat(3_000 - 2));
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
pub(crate) struct Keeping {
    pub(crate) source: Arc<dyn Source>,
    pub(crate) bytes: u64,
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
            let _kept = sink.reserve(self.bytes)?;
            self.source.read(request, sink).await
        })
    }
}

/// A row and a checkpoint.
fn row_and_checkpoint() -> Steps {
    Arc::new(|step| match step {
        0 => Some(Step::Batch(batch(Arc::new(Int64Array::from(vec![1_i64]))))),
        1 => Some(Step::Checkpoint(64)),
        _ => None,
    })
}

#[tokio::test(start_paused = true)]
async fn a_read_keeping_more_than_its_part_of_the_budget_fails_at_once_naming_the_limit() {
    // Reads may keep a quarter of the budget between the sixteen read at once by default.
    const SHARE: u64 = BUDGET / 4 / 16;
    let keeping = |name: &'static str, bytes: u64| async move {
        let source = Arc::new(Keeping {
            source: making(name, row_and_checkpoint()).await,
            bytes,
        });
        let config = commit_every(1_000_000_000)
            .memory(BUDGET)
            .retry(RetryPolicy::default().max_attempts(1))
            .lanes(1);
        let started = tokio::time::Instant::now();
        let outcome = engine(config)
            .run(pipeline(name, [stream("events")]), source, null().await)
            .await;
        (outcome, started.elapsed())
    };
    let (within, _) = keeping("lowering_kept", SHARE).await;
    assert_eq!(
        within.report.status,
        RunStatus::Succeeded,
        "{:?}",
        within.error
    );
    assert_eq!(within.report.rows, 1);
    assert!(within.report.peak_memory >= SHARE);
    let (beyond, elapsed) = keeping("lowering_kept_beyond", SHARE + 1).await;
    let error = beyond.error.expect("the run fails");
    assert_eq!(
        (error.kind(), error.code()),
        (ErrorKind::Source, Some("limit_exceeded"))
    );
    assert!(!error.is_retryable());
    let said = format!("{:?}", error.report());
    let expected = format!(
        "read kept bytes is {}, over the limit of {SHARE}",
        SHARE + 1
    );
    assert!(said.contains(&expected), "{said}");
    assert!(elapsed < Duration::from_secs(1), "{elapsed:?}");
}

#[tokio::test(start_paused = true)]
async fn cursors_no_commit_releases_fail_the_attempt_at_the_deadline_with_what_held_the_budget() {
    const WAIT: Duration = Duration::from_secs(120);
    // The destination never answers the first commit, while the source of one partition
    // checkpoints on: cursors of a quarter of what cursors may take, as large as it is told it
    // may send, wait for a commit until the fifth finds no room.
    let gate = Gate::closed();
    let steps: Steps = Arc::new(|step| match step {
        step if step < 12 && step.is_multiple_of(2) => {
            Some(Step::Batch(batch(Arc::new(Int64Array::from(vec![1_i64])))))
        }
        step if step < 12 => Some(Step::Checkpoint(
            usize::try_from(ONE_READ / 64 / 4).expect("a size"),
        )),
        _ => None,
    });
    let config = commit_every(1)
        .memory(ONE_READ)
        .partitions(1)
        .memory_wait(WAIT)
        .retry(RetryPolicy::default().max_attempts(1))
        .lanes(1);
    let started = tokio::time::Instant::now();
    // The commit lands once the attempt has failed, which waits for it.
    let late = Arc::clone(&gate);
    let opened = tokio::spawn(async move {
        tokio::time::sleep(WAIT + Duration::from_secs(10)).await;
        late.open();
    });
    let outcome = engine(config)
        .run(
            pipeline("lowering_cursors", [stream("events")]),
            making("lowering_cursors", steps).await,
            gated(null().await, Arc::clone(&gate)),
        )
        .await;
    opened.await.expect("the gate opens");
    let error = outcome.error.expect("the run fails");
    assert_eq!(
        (error.kind(), error.code()),
        (ErrorKind::Memory, Some("memory_budget_wait_exceeded"))
    );
    assert!(error.is_retryable());
    assert_eq!(
        error.stream().map(ToString::to_string),
        Some("events".to_owned())
    );
    let said = format!("{:?}", error.report());
    assert!(said.contains("for a cursor waited 120s"), "{said}");
    let full = format!("cursors waiting for a commit {}", ONE_READ / 64);
    assert!(said.contains(&full), "{said}");
    let elapsed = started.elapsed();
    assert!(
        elapsed >= WAIT && elapsed < WAIT + Duration::from_secs(30),
        "{elapsed:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_destination_that_stops_writing_fails_the_attempt_at_the_deadline_within_the_budget() {
    const WAIT: Duration = Duration::from_secs(120);
    // No write returns, so nothing lowered is ever released: pieces queue on the lane, deep
    // enough to take them, until the budget has no room for another, and the wait for room ends
    // at the deadline, the stuck write with the attempt.
    let steps: Steps = Arc::new(|step| {
        let small = Int8Array::from(vec![1_i8; 1_000_000]);
        (step < 64).then(|| Step::Batch(batch(Arc::new(small))))
    });
    let config = commit_every(1_000_000_000)
        .memory(BUDGET)
        .memory_wait(WAIT)
        .retry(RetryPolicy::default().max_attempts(1))
        .lanes(1)
        .lane_window(10_000);
    let started = tokio::time::Instant::now();
    let outcome = engine(config)
        .run(
            pipeline("lowering_stalled", [stream("events")]),
            making("lowering_stalled", steps).await,
            stalling().await,
        )
        .await;
    let error = outcome.error.expect("the run fails");
    assert_eq!(
        (error.kind(), error.code()),
        (ErrorKind::Memory, Some("memory_budget_wait_exceeded"))
    );
    assert!(error.is_retryable());
    let elapsed = started.elapsed();
    assert!(
        elapsed >= WAIT && elapsed < WAIT + Duration::from_secs(30),
        "{elapsed:?}"
    );
    assert!(outcome.report.peak_memory <= BUDGET);
}

#[tokio::test(start_paused = true)]
async fn a_json_push_is_charged_for_what_it_becomes_before_it_is_parsed() {
    // A megabyte of records whose text is mostly their one long key: the batch they become
    // takes far less than their text.
    let key = "k".repeat(1_000);
    let record = format!("{{\"{key}\":1}}\n");
    let text = record.repeat(1_000);
    let bytes = u64::try_from(text.len()).expect("a length");
    let pushed = bytes::Bytes::from(text);
    let steps: Steps = Arc::new(move |step| (step < 1).then(|| Step::Json(pushed.clone())));
    let (_, outcome) = run("lowering_chunks", making("lowering_chunks", steps).await).await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert_eq!(outcome.report.rows, 1_000);
    // The push's text, and twice it for its chunks before any was parsed, but for the line
    // ends between chunks.
    let peak = outcome.report.peak_memory;
    assert!(
        (3 * bytes - 1_024..4 * bytes).contains(&peak),
        "{peak} bytes reserved of {bytes} pushed"
    );
}
