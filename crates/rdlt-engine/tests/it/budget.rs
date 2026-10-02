//! The memory budget holds whatever a source sends: batches that keep far more alive than their
//! rows take, encodings that expand far beyond their bytes, and rows no budget fits.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use arrow_array::builder::BinaryViewBuilder;
use arrow_array::types::{Int8Type, Int32Type};
use arrow_array::{
    ArrayRef, BooleanArray, DictionaryArray, Int8Array, Int32Array, Int64Array, ListArray,
    ListViewArray, RecordBatch, new_null_array,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Fields};
use rdlt_connector::cost::Rendering;
use rdlt_engine::RunStatus;

use crate::HEAP;
use crate::support::destinations::{Gate, buffering, gated, null};
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
    let cuts = rendering.measure(&batch, 1 << 20).cuts();
    let peak = HEAP.peak_usage().saturating_sub(before);
    assert!(cost.expanded >= u64::try_from(ITEMS).expect("a count"));
    assert_eq!(cuts.len(), 1);
    assert!(peak < 64 << 10, "costing allocated {peak} bytes");
}

/// What costing and cutting `batch` allocates at its peak, and what the batch is charged.
fn costing(batch: &RecordBatch, max: u64) -> (usize, u64) {
    let rendering = Rendering::text();
    HEAP.reset_peak_usage();
    let before = HEAP.current_usage();
    let cost = rendering.cost(batch, u64::MAX);
    let cuts = rendering.measure(batch, max).cuts();
    let peak = HEAP.peak_usage().saturating_sub(before);
    assert_eq!(cuts.last().map(|piece| piece.end), Some(batch.num_rows()));
    // The cuts themselves are a word a piece.
    let cuts = cuts.capacity() * size_of::<rdlt_connector::cost::Piece>();
    (peak.saturating_sub(cuts), cost.charge())
}

/// A dictionary of `values` lists of twenty views each, keyed by `keys`.
fn keyed_lists(values: usize, keys: Vec<i32>) -> RecordBatch {
    let mut views = BinaryViewBuilder::new();
    let block = views.append_block(vec![7_u8; 64].into());
    for _ in 0..values * 20 {
        views.try_append_view(block, 0, 64).expect("a view");
    }
    let lists = ListArray::new(
        Arc::new(Field::new("item", DataType::BinaryView, true)),
        OffsetBuffer::from_lengths(vec![20; values]),
        Arc::new(views.finish()),
        None,
    );
    let keyed = DictionaryArray::<Int32Type>::try_new(Int32Array::from(keys), Arc::new(lists));
    batch(Arc::new(keyed.expect("a valid dictionary")))
}

#[test]
fn costing_holds_nothing_for_the_values_no_row_names() {
    // One row naming the last of thirty-two million nulls, which hold no bytes.
    const VALUES: usize = 32_000_000;
    let nulls = arrow_array::NullArray::new(VALUES);
    let key = i32::try_from(VALUES - 1).expect("a key");
    let keyed = DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![key]), Arc::new(nulls));
    let (peak, _) = costing(
        &batch(Arc::new(keyed.expect("a valid dictionary"))),
        1 << 20,
    );
    assert!(peak < 16 << 10, "costing allocated {peak} bytes");
    // A hundred thousand lists, of which two hundred thousand rows name one.
    let one = keyed_lists(100_000, vec![99_999; 200_000]);
    let (peak, _) = costing(&one, 1 << 20);
    assert!(peak < 16 << 10, "costing allocated {peak} bytes");
}

#[test]
fn costing_remembers_less_than_it_charges() {
    // Every one of a hundred thousand lists is named twice: each is remembered, in a few
    // words, and charged for its twenty views twice over.
    const VALUES: usize = 100_000;
    let keys = (0..2 * VALUES).map(|row| i32::try_from(row % VALUES).expect("a key"));
    let (peak, charged) = costing(&keyed_lists(VALUES, keys.collect()), 1 << 20);
    assert!(peak <= 16 << 20, "costing allocated {peak} bytes");
    let charged = usize::try_from(charged).expect("a charge in memory");
    assert!(peak <= charged / 16, "{peak} bytes to charge {charged}");
}

#[tokio::test(start_paused = true)]
async fn list_views_naming_one_child_load_within_the_budget() {
    const BUDGET: u64 = 34 << 20;
    const ROWS: usize = 2_000;
    // Each batch is 16 KB of views and 16 KB of items, and 32 MB once every row holds its items:
    // 64 MB together, twice the budget.
    let steps = Arc::new(|step: usize| {
        let rows = i32::try_from(ROWS).expect("a few rows");
        let views = ListViewArray::new(
            Arc::new(Field::new("item", DataType::Int64, true)),
            ScalarBuffer::from(vec![0_i32; ROWS]),
            ScalarBuffer::from(vec![rows; ROWS]),
            Arc::new(Int64Array::from_iter_values(0..i64::from(rows))),
            None,
        );
        (step < 2).then(|| Step::Batch(batch(Arc::new(views))))
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
    assert_eq!(outcome.report.rows, 2 * 2_000);
    assert!(peak <= bound(BUDGET), "peak {peak} bytes");
    // The heap held less than half of what the rows became: they were lowered a piece at a
    // time and let go, where rows kept as they became would hold it all.
    let became = 2 * ROWS * ROWS * 8;
    assert!(peak < became / 2, "the heap held {peak} bytes of {became}");
}

#[tokio::test(start_paused = true)]
async fn views_naming_one_buffer_load_within_the_budget() {
    const BUDGET: u64 = 34 << 20;
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
    const BUDGET: u64 = 34 << 20;
    const PUSHES: usize = 48;
    // Each push is three rows of a buffer of 4 MiB, checkpointed so it is written alone, into a
    // destination that keeps what it is written until it is flushed.
    let steps = Arc::new(|step: usize| {
        if step >= 2 * PUSHES {
            return None;
        }
        if !step.is_multiple_of(2) {
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
    const BUDGET: u64 = 34 << 20;
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

/// The budget of the cursor tests: about the least an engine of one partition takes.
const CURSORS_BUDGET: u64 = 54 << 20;

/// Loads `count` cursors as large as a read of one partition is told it may send, each after a
/// row where `rows`, under a policy that commits by rows no cursor test reaches: the heap's
/// peak above where it started, and the outcome.
async fn cursors_loaded(name: &str, count: usize, rows: bool) -> (usize, rdlt_engine::RunOutcome) {
    let config = || {
        commit_every(1_000_000)
            .memory(CURSORS_BUDGET)
            .partitions(1)
            .lanes(1)
    };
    let limit = config().build().expect("valid").limits().cursor_bytes;
    let cursor = usize::try_from(limit).expect("a size");
    let steps = Arc::new(move |step: usize| match (rows, step) {
        (false, step) => (step < count).then_some(Step::Checkpoint(cursor)),
        (true, step) if step >= 2 * count => None,
        (true, step) if step.is_multiple_of(2) => {
            Some(Step::Batch(batch(Arc::new(Int8Array::from(vec![1])))))
        }
        (true, _) => Some(Step::Checkpoint(cursor)),
    });
    let source = making(name, steps).await;
    HEAP.reset_peak_usage();
    let before = HEAP.current_usage();
    let outcome = engine(config())
        .run(pipeline(name, [stream("events")]), source, null().await)
        .await;
    let peak = HEAP.peak_usage().saturating_sub(before);
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert!(peak <= bound(CURSORS_BUDGET), "peak {peak} bytes");
    (peak, outcome)
}

/// Bytes: how much more heap ten times the cursors may take, where what they hold at once is
/// bounded: a fraction of what the extra cursors take together, about 7 MB.
const CURSORS_GROWTH: usize = 2 << 20;

#[tokio::test(start_paused = true)]
async fn checkpoints_with_large_cursors_load_within_the_budget() {
    // Cursors and not one row: each replaces the cursor waiting, so ten times as many, together
    // far more than the cursors' share, hold no more of the heap.
    let (few, _) = cursors_loaded("budget_cursors_few", 6, false).await;
    let (many, outcome) = cursors_loaded("budget_cursors", 60, false).await;
    assert_eq!(outcome.report.rows, 0);
    assert!(many <= few + CURSORS_GROWTH, "{few} bytes, then {many}");
}

#[tokio::test(start_paused = true)]
async fn rows_sealed_under_large_cursors_load_within_the_budget() {
    // A row then a cursor: every seal has a row, so none replaces the seal before it, and the
    // cursors waiting fill their share and make commits due instead of piling up.
    let (few, _) = cursors_loaded("budget_sealed_few", 6, true).await;
    let (many, outcome) = cursors_loaded("budget_sealed", 60, true).await;
    assert_eq!(outcome.report.rows, 60);
    // The cursors' share holds four cursors of the limit, and a commit is due at half of it.
    assert!(
        outcome.report.commits >= 15,
        "{} commits",
        outcome.report.commits
    );
    assert!(many <= few + CURSORS_GROWTH, "{few} bytes, then {many}");
}

/// A future that completes once `ready` says so, looking again every millisecond.
fn once(ready: impl Fn() -> bool + Send + 'static) -> Step {
    Step::Wait(Box::pin(async move {
        while !ready() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }))
}

#[tokio::test(start_paused = true)]
async fn signals_sent_while_a_commit_is_in_flight_do_not_pile_up() {
    const SIGNALS: usize = 250_000;
    let gate = Gate::closed();
    let held = Arc::new(AtomicUsize::new(0));
    let (commits, heap) = (Arc::clone(&gate), Arc::clone(&held));
    // A row and a checkpoint make a commit due; while the destination holds it, the source says
    // a quarter of a million times how far behind it is and that its partitions changed: queued,
    // they would take megabytes.
    let steps = Arc::new(move |step: usize| match step {
        0 => Some(Step::Batch(batch(Arc::new(Int8Array::from(vec![1]))))),
        1 => Some(Step::Checkpoint(8)),
        2 => {
            let commits = Arc::clone(&commits);
            Some(once(move || commits.started.load(Ordering::SeqCst) > 0))
        }
        3 => {
            HEAP.reset_peak_usage();
            heap.store(HEAP.current_usage(), Ordering::SeqCst);
            Some(Step::Replan)
        }
        step if step < SIGNALS => Some(if step.is_multiple_of(2) {
            Step::Behind(u64::try_from(step).expect("a count"))
        } else {
            Step::Replan
        }),
        step if step == SIGNALS => {
            let grown = HEAP
                .peak_usage()
                .saturating_sub(heap.load(Ordering::SeqCst));
            heap.store(grown, Ordering::SeqCst);
            commits.open();
            Some(Step::Behind(0))
        }
        _ => None,
    });
    let source = making("budget_signals", steps).await;
    let outcome = engine(commit_every(1).lanes(1))
        .run(
            pipeline("signals", [stream("events")]),
            source,
            gated(null().await, Arc::clone(&gate)),
        )
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert!(gate.started.load(Ordering::SeqCst) >= 1);
    let grown = held.load(Ordering::SeqCst);
    assert!(grown < 1 << 20, "the signals held {grown} bytes");
}

#[tokio::test(start_paused = true)]
async fn a_push_of_very_many_small_records_loads_within_the_budget() {
    const BUDGET: u64 = 34 << 20;
    const RECORDS: usize = 250_000;
    // A quarter of a million records of eight bytes each, as much text as pushes may take of
    // the budget once it is charged for what it becomes: a range a record would take twice it.
    let steps = Arc::new(|step: usize| {
        (step < 1).then(|| Step::Json("{\"a\":1}\n".repeat(RECORDS).into()))
    });
    let source = making("budget_records", steps).await;
    let config = commit_every(10_000_000).memory(BUDGET).lanes(1);
    HEAP.reset_peak_usage();
    let before = HEAP.current_usage();
    let outcome = engine(config)
        .run(
            pipeline("records", [stream("events")]),
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
    assert_eq!(outcome.report.rows, 250_000);
    assert!(peak <= bound(BUDGET), "peak {peak} bytes");
}

#[tokio::test(start_paused = true)]
async fn a_null_typed_column_loads_within_the_budget_however_wide_its_table_column() {
    const BUDGET: u64 = 34 << 20;
    const ROWS: usize = 100_000;
    // A row gives the column a struct of two hundred decimals, sixteen bytes each; a hundred
    // thousand rows then hold nothing in it, in a column typed null.
    let steps = Arc::new(|step: usize| {
        let fields: Fields = (0..200)
            .map(|index| Field::new(format!("d{index}"), DataType::Decimal128(38, 0), true))
            .collect();
        let wide = DataType::Struct(fields);
        match step {
            0 => Some(Step::Batch(batch(new_null_array(&wide, 1)))),
            1 => Some(Step::Checkpoint(8)),
            2 => Some(Step::Batch(batch(new_null_array(&DataType::Null, ROWS)))),
            _ => None,
        }
    });
    let source = making("budget_nulls", steps).await;
    let config = commit_every(1_000_000).memory(BUDGET).lanes(1);
    HEAP.reset_peak_usage();
    let before = HEAP.current_usage();
    let outcome = engine(config)
        .run(pipeline("nulls", [stream("events")]), source, null().await)
        .await;
    let peak = HEAP.peak_usage().saturating_sub(before);
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert_eq!(outcome.report.rows, 100_001);
    assert!(peak <= bound(BUDGET), "peak {peak} bytes");
}

/// A struct of two hundred decimals, sixteen bytes each.
fn wide_struct() -> DataType {
    let fields: Fields = (0..200)
        .map(|index| Field::new(format!("d{index}"), DataType::Decimal128(38, 0), true))
        .collect();
    DataType::Struct(fields)
}

/// Loads a batch of one row of `wide`, then one of `rows` rows of `narrow`, whose values typed
/// null are converted to `wide`'s: what the heap held at its peak, and how the run ended.
async fn widened_nulls(
    name: &str,
    wide: ArrayRef,
    narrow: ArrayRef,
) -> (usize, rdlt_engine::RunOutcome) {
    const BUDGET: u64 = 34 << 20;
    let steps = Arc::new(move |step: usize| match step {
        0 => Some(Step::Batch(batch(Arc::clone(&wide)))),
        1 => Some(Step::Checkpoint(8)),
        2 => Some(Step::Batch(batch(Arc::clone(&narrow)))),
        _ => None,
    });
    let source = making(name, steps).await;
    let config = commit_every(1_000_000).memory(BUDGET).lanes(1);
    HEAP.reset_peak_usage();
    let before = HEAP.current_usage();
    let outcome = engine(config)
        .run(pipeline(name, [stream("events")]), source, null().await)
        .await;
    let peak = HEAP.peak_usage().saturating_sub(before);
    assert!(peak <= bound(BUDGET), "peak {peak} bytes");
    (peak, outcome)
}

#[tokio::test(start_paused = true)]
async fn nulls_nested_in_structs_and_lists_load_within_the_budget_however_wide_their_type() {
    const ROWS: usize = 100_000;
    // A struct whose field is typed null, into a table whose field is the wide struct.
    let field = |data_type: DataType| Fields::from(vec![Field::new("a", data_type, true)]);
    let wide = arrow_array::StructArray::new_null(field(wide_struct()), 1);
    let narrow = arrow_array::StructArray::new(
        field(DataType::Null),
        vec![Arc::new(arrow_array::NullArray::new(ROWS))],
        None,
    );
    let (_, outcome) = widened_nulls("nested_nulls", Arc::new(wide), Arc::new(narrow)).await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert_eq!(outcome.report.rows, ROWS as u64 + 1);
    // A list whose items are typed null, into a table whose items are the wide struct.
    let item = |data_type: DataType| Arc::new(Field::new("item", data_type, true));
    let wide = ListArray::new_null(item(wide_struct()), 1);
    let narrow = ListArray::new(
        item(DataType::Null),
        OffsetBuffer::from_lengths(vec![1; ROWS]),
        Arc::new(arrow_array::NullArray::new(ROWS)),
        None,
    );
    let (_, outcome) = widened_nulls("listed_nulls", Arc::new(wide), Arc::new(narrow)).await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert_eq!(outcome.report.rows, ROWS as u64 + 1);
}
