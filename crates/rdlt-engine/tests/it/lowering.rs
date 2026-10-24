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
use rdlt_engine::{ErrorKind, Origin, RetryPolicy, RunOutcome, RunStatus, codes};
use rdlt_wire::limits::LIMIT_EXCEEDED;

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
        (ErrorKind::Source, Some(codes::ROW_EXCEEDS_BUDGET))
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
        (error.kind(), error.origin(), error.connector_code()),
        (ErrorKind::Source, Origin::Source, Some(LIMIT_EXCEEDED))
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
async fn a_run_reports_the_most_its_reads_kept_at_once() {
    // A quarter of the budget between the sixteen reads at once by default.
    const SHARE: u64 = BUDGET / 4 / 16;
    let source = Arc::new(Keeping {
        source: making("lowering_kept_peak", row_and_checkpoint()).await,
        bytes: SHARE,
    });
    let config = commit_every(1_000_000_000).memory(BUDGET).lanes(1);
    let outcome = engine(config)
        .run(
            pipeline("lowering_kept_peak", [stream("events")]),
            source,
            null().await,
        )
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert_eq!(outcome.report.counters.kept_peak, SHARE);
}

/// The deadline of a connector's call where it is `wait`, and the default's elsewhere.
fn deadline(wait: Option<Duration>) -> Duration {
    wait.unwrap_or(Duration::from_mins(30))
}

/// Loads one partition's six rows through a destination whose commits wait at `gate`, in one
/// attempt whose connector calls each wait `wait` where one is given: how the run ended, and how
/// long it took.
///
/// Each row is checkpointed with a cursor of a quarter of what cursors may take, as large as the
/// source is told it may send, so behind a commit that has not landed the fifth finds no room
/// and waits.
async fn behind_a_commit(
    name: &str,
    gate: &Arc<Gate>,
    wait: Option<Duration>,
) -> (RunOutcome, Duration) {
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
        .retry(RetryPolicy::default().max_attempts(1))
        .lanes(1);
    let config = match wait {
        Some(wait) => config.connector_wait(wait),
        None => config,
    };
    let started = tokio::time::Instant::now();
    let outcome = engine(config)
        .with_limit(Duration::from_hours(3))
        .run(
            pipeline(name, [stream("events")]),
            making(name, steps).await,
            gated(null().await, Arc::clone(gate)),
        )
        .await;
    (outcome, started.elapsed())
}

#[tokio::test(start_paused = true)]
async fn a_commit_that_never_answers_fails_at_its_own_deadline_before_a_cursor_blames_memory() {
    for wait in [None, Some(Duration::from_hours(2))] {
        let name = format!("lowering_hung_commit_{}", deadline(wait).as_secs());
        let (outcome, elapsed) = behind_a_commit(&name, &Gate::closed(), wait).await;
        let error = outcome.error.expect("the run fails");
        // The engine's wait for the call ends it, before the budget's.
        assert_eq!(
            (error.origin(), error.code()),
            (Origin::Engine, Some(codes::DEADLINE_EXCEEDED)),
            "{wait:?}: {error:?}"
        );
        assert!(error.is_retryable());
        assert_eq!(outcome.report.counters.waits.cursors.count, 1, "{wait:?}");
        assert!(
            elapsed >= deadline(wait) && elapsed < deadline(wait) + Duration::from_secs(30),
            "{wait:?}: {elapsed:?}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_cursor_waits_behind_a_commit_for_as_long_as_twice_the_connector_deadline() {
    let gate = Gate::closed();
    let late = Arc::clone(&gate);
    let landed = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_mins(90)).await;
        late.open();
    });
    let wait = Some(Duration::from_hours(2));
    let (outcome, _) = behind_a_commit("lowering_slow_commit", &gate, wait).await;
    landed.await.expect("the commit lands");
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert_eq!(outcome.report.rows, 6);
    let cursors = outcome.report.counters.waits.cursors;
    assert!(cursors.time > Duration::from_hours(1), "{cursors:?}");
}

#[tokio::test(start_paused = true)]
async fn a_destination_that_stops_writing_fails_at_its_write_deadline_within_the_budget() {
    // No write returns, so nothing lowered is ever released: pieces queue on the lane, deep
    // enough to take them, until the budget has no room for another, and the stuck write fails
    // at its own deadline with the attempt, before the wait for room reaches the budget's.
    let steps: Steps = Arc::new(|step| {
        let small = Int8Array::from(vec![1_i8; 1_000_000]);
        (step < 64).then(|| Step::Batch(batch(Arc::new(small))))
    });
    for wait in [None, Some(Duration::from_hours(2))] {
        let name = format!("lowering_stalled_{}", deadline(wait).as_secs());
        let config = commit_every(1_000_000_000)
            .memory(BUDGET)
            .retry(RetryPolicy::default().max_attempts(1))
            .lanes(1)
            .lane_window(10_000);
        let config = match wait {
            Some(wait) => config.connector_wait(wait),
            None => config,
        };
        let started = tokio::time::Instant::now();
        let outcome = engine(config)
            .with_limit(Duration::from_hours(3))
            .run(
                pipeline(&name, [stream("events")]),
                making(&name, Arc::clone(&steps)).await,
                stalling().await,
            )
            .await;
        let error = outcome.error.expect("the run fails");
        // The engine's wait for the call ends it, before the budget's.
        assert_eq!(
            (error.origin(), error.code()),
            (Origin::Engine, Some(codes::DEADLINE_EXCEEDED)),
            "{wait:?}: {error:?}"
        );
        assert!(error.is_retryable());
        let waits = outcome.report.counters.waits;
        assert!(
            waits.work.count + waits.intake.count > 0,
            "{wait:?}: {waits:?}"
        );
        let elapsed = started.elapsed();
        assert!(
            elapsed >= deadline(wait) && elapsed < deadline(wait) + Duration::from_secs(30),
            "{wait:?}: {elapsed:?}"
        );
        assert!(outcome.report.peak_memory <= BUDGET);
    }
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
