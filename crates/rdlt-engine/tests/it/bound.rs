//! The budget's bound as the system runs: what a run reserves never passes its budget, and what
//! its heap holds stays inside the stated bound, through plans that normalize, on a compute pool
//! of several threads that takes turns between pieces, with sixteen partitions read at once and
//! a commit replayed from the log.

use std::sync::Arc;

use arrow_array::{ArrayRef, Decimal256Array, Int8Array, ListArray, RecordBatch};
use arrow_buffer::{OffsetBuffer, i256};
use arrow_schema::{DataType, Field};
use rdlt_connector::ReadMode;
use rdlt_engine::{LocalWal, Nested, RunOutcome, RunStatus, SchemaSettings, StreamPlan, WalStore};

use crate::HEAP;
use crate::support::destinations::{Step as Fails, buffering, failing, null};
use crate::support::making::{Step, Steps, making, making_parts};
use crate::support::{
    commit_every, engine, pipeline, pooled_engine, pooled_logging_engine, retrying, stream,
};

const BUDGET: u64 = 34 << 20;

/// The heap a run may reach under the budget beside the pushes its sources hold before the
/// engine admits them, as the engine documents it.
fn bound() -> usize {
    usize::try_from(BUDGET * 12 / 10 + (32 << 20)).expect("a bound in memory")
}

fn normalized(name: &str) -> StreamPlan {
    stream(name).schema(SchemaSettings::new().nested(Nested::normalize()))
}

fn batch(column: ArrayRef) -> RecordBatch {
    RecordBatch::try_from_iter([("column", column)]).expect("one column makes a batch")
}

/// One 256-bit decimal: the widest a column of numbers is stored.
fn wide() -> ArrayRef {
    let wide = Decimal256Array::from(vec![i256::from_i128(1)]);
    Arc::new(wide.with_precision_and_scale(76, 0).expect("a decimal"))
}

/// What `run` makes the heap hold at its peak beyond what it held before, and how it ended.
async fn measured(run: impl Future<Output = RunOutcome>) -> (usize, RunOutcome) {
    HEAP.reset_peak_usage();
    let before = HEAP.current_usage();
    let outcome = run.await;
    (HEAP.peak_usage().saturating_sub(before), outcome)
}

/// Asserts `outcome` loaded `rows` rows, and that the heap's `peak` is within `bound`.
fn within(outcome: &RunOutcome, rows: u64, peak: usize, bound: usize) {
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert_eq!(outcome.report.rows, rows);
    assert!(peak <= bound, "the heap held {peak} bytes of {bound}");
    // Something was lowered under the budget, and the test engine has checked it never passed.
    assert!(outcome.report.peak_memory > 0);
}

/// Loads what `steps` makes through a plan that normalizes, on the inline pool.
async fn normalizing(name: &str, steps: Steps) -> (usize, RunOutcome) {
    let config = commit_every(1_000_000_000).memory(BUDGET).lanes(1);
    let source = making(name, steps).await;
    let run = engine(config).run(pipeline(name, [normalized("events")]), source, null().await);
    let (peak, outcome) = measured(run).await;
    // A piece asks once for all a request may take, before it is split, while the parts of
    // the piece before it still hold what their split made and their allowance.
    let reserved = outcome.report.peak_memory;
    assert!(
        reserved >= BUDGET / 4 + BUDGET / 16,
        "{reserved} bytes reserved"
    );
    (peak, outcome)
}

#[tokio::test(start_paused = true)]
async fn rows_of_a_table_far_wider_than_their_batch_load_within_the_budget_normalized() {
    // One row gives the table two hundred columns of 256-bit decimals; rows of one small column
    // then take 6.6 kilobytes of nulls each, 660 MB a push, from 100 KB pushed.
    let steps: Steps = Arc::new(|step| match step {
        0 => {
            let wide = wide();
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
    let (peak, outcome) = normalizing("bound_fill", steps).await;
    within(&outcome, 300_001, peak, bound());
}

#[tokio::test(start_paused = true)]
async fn small_integers_into_a_column_of_256_bit_decimals_load_within_the_budget_normalized() {
    const ROWS: usize = 500_000;
    // One row makes the column a 256-bit decimal; a million and a half bytes then become 48 MiB,
    // more than the budget, lowered a piece at a time.
    let steps: Steps = Arc::new(|step| match step {
        0 => Some(Step::Batch(batch(wide()))),
        1 => Some(Step::Checkpoint(8)),
        2..=4 => {
            let small = Int8Array::from(vec![1_i8; ROWS]);
            Some(Step::Batch(batch(Arc::new(small))))
        }
        _ => None,
    });
    let (peak, outcome) = normalizing("bound_widened", steps).await;
    within(&outcome, 3 * ROWS as u64 + 1, peak, bound());
    // The heap held less than half of what lowering made: its pieces were let go as they were
    // written, where a lowering that kept them would hold it all.
    let lowered = 3 * ROWS * 32;
    assert!(
        peak < lowered / 2,
        "the heap held {peak} bytes of {lowered} lowered"
    );
}

/// Rows each push of a nested read holds, and the pushes of each read.
const NESTED_ROWS: usize = 5_000;
const PUSHES: usize = 2;

/// A read of a wide row, then pushes of small integers beside lists of two each, which a plan
/// that normalizes sends to a table of their own; each push with its checkpoint.
fn nested() -> Steps {
    let lists = |rows: usize, column: ArrayRef| {
        let items: ArrayRef = Arc::new(Int8Array::from(vec![1_i8; 2 * rows]));
        let lists = ListArray::new(
            Arc::new(Field::new("item", DataType::Int8, true)),
            OffsetBuffer::from_lengths(vec![2; rows]),
            items,
            None,
        );
        let columns = [("column", column), ("items", Arc::new(lists) as ArrayRef)];
        RecordBatch::try_from_iter(columns).expect("a batch")
    };
    Arc::new(move |step| match step {
        0 => Some(Step::Batch(lists(1, wide()))),
        step if step > 2 * PUSHES + 1 => None,
        step if !step.is_multiple_of(2) => Some(Step::Checkpoint(8)),
        _ => {
            let small: ArrayRef = Arc::new(Int8Array::from(vec![1_i8; NESTED_ROWS]));
            Some(Step::Batch(lists(NESTED_ROWS, small)))
        }
    })
}

/// Bytes: what one push of a nested read keeps alive, about: its integers, its items and their
/// offsets.
const NESTED_PUSH: usize = 8 * NESTED_ROWS;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sixteen_partitions_normalizing_into_two_tables_on_a_pool_stay_within_the_budget() {
    const PARTS: usize = 16;
    let config = commit_every(1_000_000_000)
        .memory(BUDGET)
        .partitions(PARTS)
        .lanes(2);
    let source = making_parts("bound_pooled", nested(), PARTS).await;
    let plan = pipeline("bound_pooled", [normalized("events")]);
    let run = pooled_engine(config, 4).run(plan, source, null().await);
    let (peak, outcome) = measured(run).await;
    // Each row and each of its two items is a row of a table.
    let rows = PARTS * (3 + PUSHES * 3 * NESTED_ROWS);
    // What each partition splits and lowers is reserved, so the heap stays within the budget
    // itself beside the pushes the sources hold: sixteen partitions holding a split or an
    // allowance the budget did not count would pass it.
    let held = PARTS * NESTED_PUSH;
    let budget = usize::try_from(BUDGET).expect("a budget in memory");
    within(&outcome, rows as u64, peak, budget + held);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sixteen_partitions_widening_on_a_pool_stay_within_the_budget() {
    const PARTS: usize = 16;
    const ROWS: usize = 500_000;
    // Each partition sends half a megabyte of small integers into a column of 256-bit
    // decimals, sixteen megabytes once lowered, more than a request may take: in pieces every
    // partition cuts at the same time, 256 MiB together.
    let steps: Steps = Arc::new(|step| match step {
        0 => Some(Step::Batch(batch(wide()))),
        1 => Some(Step::Checkpoint(8)),
        2 => Some(Step::Batch(batch(Arc::new(Int8Array::from(vec![
            1_i8;
            ROWS
        ]))))),
        _ => None,
    });
    let config = commit_every(1_000_000_000)
        .memory(BUDGET)
        .partitions(PARTS)
        .lanes(1);
    let source = making_parts("bound_widening", steps, PARTS).await;
    let plan = pipeline("bound_widening", [stream("events")]);
    let run = pooled_engine(config, 4).run(plan, source, null().await);
    let (peak, outcome) = measured(run).await;
    let rows = PARTS * (1 + ROWS);
    within(&outcome, rows as u64, peak, bound() + PARTS * ROWS);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_logged_load_replayed_and_read_by_sixteen_partitions_stays_within_the_budget() {
    const PARTS: usize = 16;
    let base = tempfile::tempdir().expect("a temporary directory");
    let store: Arc<dyn WalStore> = Arc::new(LocalWal::new(base.path()));
    // The first commit fails before it lands: the next attempt replays it from the log, then
    // reads every partition again, each batch logged before it is written.
    let config = retrying(3)
        .commit(rdlt_engine::CommitPolicy::new(None, Some(100_000), None).expect("a policy"))
        .memory(BUDGET)
        .partitions(PARTS)
        .lanes(2);
    let source = making_parts("bound_logged", nested(), PARTS).await;
    let plan = pipeline(
        "bound-logged",
        [normalized("events").read(ReadMode::Incremental)],
    )
    .with_wal(true);
    let destination = failing(null().await, Fails::CommitOnce);
    let run = pooled_logging_engine(config, 4, store).run(plan, source, destination);
    let (peak, outcome) = measured(run).await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert_eq!(outcome.report.attempts.len(), 2);
    let held = PARTS * NESTED_PUSH;
    assert!(
        peak <= bound() + held,
        "the heap held {peak} bytes of {}",
        bound() + held
    );
    assert!(outcome.report.peak_memory > 0);
}

#[tokio::test(start_paused = true)]
async fn a_row_whose_items_normalize_past_a_request_fails_unsplit_within_the_bound() {
    // One row of eight million flags: a megabyte pushed, eight million child rows each with its
    // lineage, far more than a request may take.
    let steps: Steps = Arc::new(|step| {
        (step < 1).then(|| {
            let items = 8 << 20;
            let flags =
                arrow_array::BooleanArray::new(arrow_buffer::BooleanBuffer::new_unset(items), None);
            let list = ListArray::new(
                Arc::new(Field::new("item", DataType::Boolean, true)),
                OffsetBuffer::from_lengths([items]),
                Arc::new(flags),
                None,
            );
            Step::Batch(batch(Arc::new(list)))
        })
    });
    let config = commit_every(1_000_000_000).memory(BUDGET).lanes(1);
    let source = making("bound_flags", steps).await;
    let run = engine(config).run(
        pipeline("bound_flags", [normalized("events")]),
        source,
        null().await,
    );
    let (peak, outcome) = measured(run).await;
    let error = outcome.error.expect("the row takes more than a request");
    assert_eq!(error.code(), Some("row_exceeds_budget"), "{error:?}");
    assert!(peak <= bound(), "the heap held {peak} bytes");
}

#[tokio::test(start_paused = true)]
async fn list_views_naming_shared_items_normalize_within_the_bound() {
    // Five hundred rows each naming the same four thousand flags: two million child rows from a
    // few kilobytes pushed.
    let steps: Steps = Arc::new(|step| {
        (step < 1).then(|| {
            let views = arrow_array::ListViewArray::new(
                Arc::new(Field::new("item", DataType::Boolean, true)),
                arrow_buffer::ScalarBuffer::from(vec![0_i32; 500]),
                arrow_buffer::ScalarBuffer::from(vec![4_000_i32; 500]),
                Arc::new(arrow_array::BooleanArray::from(vec![true; 4_000])),
                None,
            );
            Step::Batch(batch(Arc::new(views)))
        })
    });
    let (peak, outcome) = normalizing("bound_views", steps).await;
    within(&outcome, 500 + 500 * 4_000, peak, bound());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_logged_commit_replayed_into_writers_that_buffer_stays_within_the_budget() {
    const BATCHES: usize = 120;
    let base = tempfile::tempdir().expect("a temporary directory");
    let store: Arc<dyn WalStore> = Arc::new(LocalWal::new(base.path()));
    // One commit takes every batch, about four times the budget; it fails before it lands, and
    // the next attempt stages it again from the log through writers that hold every batch until
    // they flush.
    let blob = |_: usize| -> ArrayRef {
        Arc::new(arrow_array::BinaryArray::from_iter_values([vec![
            7_u8;
            1 << 20
        ]]))
    };
    let steps: Steps = Arc::new(move |step| match step {
        step if step < BATCHES => Some(Step::Batch(batch(blob(step)))),
        step if step == BATCHES => Some(Step::Checkpoint(8)),
        _ => None,
    });
    let config = retrying(3)
        .commit(rdlt_engine::CommitPolicy::new(None, Some(1_000_000), None).expect("a policy"))
        .memory(BUDGET)
        .lanes(1);
    let source = making("bound_buffered", steps).await;
    let plan = pipeline(
        "bound-buffered",
        [stream("events").read(ReadMode::Incremental)],
    )
    .with_wal(true);
    let destination = failing(buffering().await, Fails::CommitOnce);
    let run = pooled_logging_engine(config, 4, store).run(plan, source, destination);
    let (peak, outcome) = measured(run).await;
    assert_eq!(outcome.report.attempts.len(), 2, "{:?}", outcome.error);
    within(&outcome, BATCHES as u64, peak, bound());
}
