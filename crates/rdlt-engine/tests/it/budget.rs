//! The memory budget holds whatever a source sends: batches that keep far more alive than their
//! rows take, encodings that expand far beyond their bytes, and rows no budget fits.

use std::sync::Arc;

use arrow_array::builder::BinaryViewBuilder;
use arrow_array::types::{Int8Type, Int32Type};
use arrow_array::{
    ArrayRef, BooleanArray, DictionaryArray, Int8Array, Int32Array, Int64Array, ListArray,
    ListViewArray, RecordBatch, new_null_array,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field};
use rdlt_connector::cost::Rendering;
use rdlt_engine::RunStatus;

use crate::HEAP;
use crate::support::destinations::{buffering, null};
use crate::support::making::{Step, making};
use crate::support::{commit_every, engine, pipeline, stream};

fn batch(column: ArrayRef) -> RecordBatch {
    RecordBatch::try_from_iter([("column", column)]).expect("one column makes a batch")
}

/// The heap a run may reach under `budget`, as the engine documents it.
fn bound(budget: u64) -> usize {
    usize::try_from(budget * 12 / 10 + (32 << 20)).expect("a bound in memory")
}

#[test]
fn costing_a_batch_allocates_nothing_a_value() {
    const ITEMS: usize = 20_000_000;
    // One row whose dictionary value is a list of twenty million booleans.
    let list = ListArray::new(
        Arc::new(Field::new("item", DataType::Boolean, true)),
        OffsetBuffer::from_lengths([ITEMS]),
        Arc::new(BooleanArray::from(vec![true; ITEMS])),
        None,
    );
    let keyed = DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![0]), Arc::new(list))
        .expect("a valid dictionary");
    let batch = batch(Arc::new(keyed));
    let rendering = Rendering::text();
    HEAP.reset_peak_usage();
    let before = HEAP.current_usage();
    let cost = rendering.cost(&batch, u64::MAX);
    let cuts = rendering.cuts(&batch, 1 << 20);
    let peak = HEAP.peak_usage().saturating_sub(before);
    assert!(cost.expanded >= u64::try_from(ITEMS).expect("a count"));
    assert_eq!(cuts, [1]);
    assert!(peak < 64 << 10, "costing allocated {peak} bytes");
}

#[tokio::test(start_paused = true)]
async fn list_views_naming_one_child_load_within_the_budget() {
    const BUDGET: u64 = 8 << 20;
    const ROWS: usize = 2_000;
    // Each batch is 16 KB of views and 16 KB of items, and 32 MB once every row holds its items.
    let steps = Arc::new(|step: usize| {
        let rows = i32::try_from(ROWS).expect("a few rows");
        let views = ListViewArray::new(
            Arc::new(Field::new("item", DataType::Int64, true)),
            ScalarBuffer::from(vec![0_i32; ROWS]),
            ScalarBuffer::from(vec![rows; ROWS]),
            Arc::new(Int64Array::from_iter_values(0..i64::from(rows))),
            None,
        );
        (step < 4).then(|| Step::Batch(batch(Arc::new(views))))
    });
    let source = making("budget_views", steps).await;
    let config = commit_every(1_000_000).memory(BUDGET).lanes(1);
    HEAP.reset_peak_usage();
    let before = HEAP.current_usage();
    let outcome = engine(config)
        .run(pipeline("views", [stream("events")]), source, null().await)
        .await;
    let peak = HEAP.peak_usage().saturating_sub(before);
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert_eq!(outcome.report.rows, 4 * 2_000);
    assert!(peak <= bound(BUDGET), "peak {peak} bytes");
}

#[tokio::test(start_paused = true)]
async fn views_naming_one_buffer_load_within_the_budget() {
    const BUDGET: u64 = 8 << 20;
    // Nine hundred views of one 64 KB value: 14 KB of views, 58 MB once each row holds its own.
    let steps = Arc::new(|step: usize| {
        let mut views = BinaryViewBuilder::new();
        let block = views.append_block(vec![7_u8; 64 << 10].into());
        for _ in 0..900 {
            views
                .try_append_view(block, 0, 64 << 10)
                .expect("a view within its block");
        }
        (step < 2).then(|| Step::Batch(batch(Arc::new(views.finish()))))
    });
    let source = making("budget_aliased", steps).await;
    let config = commit_every(1_000_000).memory(BUDGET).lanes(1);
    HEAP.reset_peak_usage();
    let before = HEAP.current_usage();
    let outcome = engine(config)
        .run(
            pipeline("aliased", [stream("events")]),
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
    assert_eq!(outcome.report.rows, 1_800);
    assert!(peak <= bound(BUDGET), "peak {peak} bytes");
}

#[tokio::test(start_paused = true)]
async fn batches_keeping_large_buffers_alive_load_within_the_budget() {
    const BUDGET: u64 = 16 << 20;
    const PUSHES: usize = 48;
    // Each push is three rows of a buffer of 4 MiB, checkpointed so it is written alone, into a
    // destination that keeps what it is written until it is flushed.
    let steps = Arc::new(|step: usize| {
        if step >= 2 * PUSHES {
            return None;
        }
        if step % 2 == 1 {
            return Some(Step::Checkpoint(8));
        }
        // A type the destination stores as it is, so lowering passes the buffer through.
        let whole = Int8Array::from(vec![1_i8; 4 << 20]);
        Some(Step::Batch(batch(Arc::new(whole.slice(0, 3)))))
    });
    let source = making("budget_kept", steps).await;
    let config = commit_every(1_000_000).memory(BUDGET).lanes(1);
    HEAP.reset_peak_usage();
    let before = HEAP.current_usage();
    let outcome = engine(config)
        .run(
            pipeline("kept", [stream("events")]),
            source,
            buffering().await,
        )
        .await;
    let peak = HEAP.peak_usage().saturating_sub(before);
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert_eq!(outcome.report.rows, 3 * 48);
    assert!(peak <= bound(BUDGET), "peak {peak} bytes");
}

#[tokio::test(start_paused = true)]
async fn a_row_expanding_beyond_the_budget_fails_the_run_before_it_is_built() {
    const BUDGET: u64 = 4 << 20;
    // Three hundred null keys over values 16 MiB wide: a few hundred bytes pushed.
    let steps = Arc::new(|step: usize| {
        let values = new_null_array(&DataType::FixedSizeBinary(16 << 20), 0);
        let keys = Int8Array::from(vec![None::<i8>; 300]);
        let nulls = DictionaryArray::<Int8Type>::try_new(keys, values).expect("null keys");
        (step < 1).then(|| Step::Batch(batch(Arc::new(nulls))))
    });
    let source = making("budget_row", steps).await;
    let config = commit_every(1_000_000).memory(BUDGET).lanes(1);
    HEAP.reset_peak_usage();
    let before = HEAP.current_usage();
    let outcome = engine(config)
        .run(pipeline("row", [stream("events")]), source, null().await)
        .await;
    let peak = HEAP.peak_usage().saturating_sub(before);
    assert_eq!(outcome.report.status, RunStatus::Failed);
    let error = outcome.error.expect("a failed run has its error");
    assert_eq!(error.code(), Some("row_exceeds_budget"));
    assert!(peak <= bound(BUDGET), "peak {peak} bytes");
}
