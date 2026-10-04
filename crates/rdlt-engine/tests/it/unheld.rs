//! Values their column cannot hold — a date beyond the days a date holds, a change time no
//! version can begin at — follow the column's schema policy row by row: refused, the row
//! dropped, or the value nulled, and counted.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{
    Array, ArrayRef, Date64Array, Int64Array, ListArray, RecordBatch, StructArray,
    TimestampSecondArray,
};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field};
use rdlt_engine::{
    ErrorKind, Nested, RunOutcome, RunStatus, SchemaPolicy, SchemaSettings, StreamPlan, WriteMode,
};

use crate::schema::{batch, ints, text};
use crate::support::batches::{BatchStream, batches};
use crate::support::{commit_every, engine, memory, pipeline, stream};

/// A day of milliseconds.
const DAY: i64 = 86_400_000;

/// Runs `events` as `plan` into the memory destination at `store`.
async fn ran(store: &str, plan: StreamPlan, events: BatchStream) -> RunOutcome {
    engine(commit_every(10))
        .run(
            pipeline(store, [plan]),
            batches(store, vec![events]).await,
            memory(store).await,
        )
        .await
}

/// The column `name` of the table `table` at `store`, as integers, by row id.
fn published(store: &str, table: &str, name: &str) -> Vec<(i64, Option<i64>)> {
    let mut rows = Vec::new();
    for batch in rdlt_connector_reference::published(store, table) {
        let ids = arrow_cast::cast(batch.column_by_name("id").expect("ids"), &DataType::Int64)
            .expect("integer ids");
        let values = batch.column_by_name(name).expect("the column");
        let values = arrow_cast::cast(values, &DataType::Int64).expect("integers");
        let (ids, values) = (
            ids.as_primitive::<Int64Type>(),
            values.as_primitive::<Int64Type>(),
        );
        for row in 0..batch.num_rows() {
            let value = (!values.is_null(row)).then(|| values.value(row));
            rows.push((ids.value(row), value));
        }
    }
    rows.sort_unstable();
    rows
}

/// A batch of ids 1 to 3 whose `d` holds a day, a date no date holds, and another day.
fn far_dates() -> RecordBatch {
    let dates: ArrayRef = Arc::new(Date64Array::from(vec![0, i64::MAX, DAY]));
    batch(vec![("id", ints(&[1, 2, 3])), ("d", dates)])
}

/// The plan of stream `events` whose column `column` follows `policy`.
fn following(column: &str, policy: SchemaPolicy) -> StreamPlan {
    stream("events").column(column, SchemaSettings::new().policy(policy))
}

/// Checks that `outcome` failed on `code`, a Schema error.
fn refused(outcome: &RunOutcome, code: &str) {
    assert_eq!(outcome.report.status, RunStatus::Failed);
    let error = outcome.error.as_ref().expect("the run fails");
    assert_eq!(
        (error.kind(), error.code()),
        (ErrorKind::Schema, Some(code))
    );
}

/// Checks that `outcome` succeeded, loading `rows` rows and discarding `discarded` rows and
/// values.
fn loaded(outcome: &RunOutcome, rows: u64, (rows_out, values_out): (u64, u64)) {
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert_eq!(outcome.report.rows, rows);
    let report = &outcome.report.streams["events"];
    assert_eq!(
        (report.discarded_rows, report.discarded_values),
        (rows_out, values_out)
    );
}

#[tokio::test(start_paused = true)]
async fn a_value_its_column_cannot_hold_follows_the_columns_policy_row_by_row() {
    let events = || BatchStream::new("events", vec![far_dates()]);
    for (store, policy) in [
        ("unheld_evolve", SchemaPolicy::Evolve),
        ("unheld_freeze", SchemaPolicy::Freeze),
    ] {
        refused(
            &ran(store, following("d", policy), events()).await,
            "value_unrepresentable",
        );
    }
    let dropped = ran(
        "unheld_row",
        following("d", SchemaPolicy::DiscardRow),
        events(),
    )
    .await;
    loaded(&dropped, 2, (1, 0));
    assert_eq!(
        published("unheld_row", "events", "d"),
        [(1, Some(0)), (3, Some(1))]
    );
    let nulled = ran(
        "unheld_value",
        following("d", SchemaPolicy::DiscardValue),
        events(),
    )
    .await;
    loaded(&nulled, 3, (0, 1));
    assert_eq!(
        published("unheld_value", "events", "d"),
        [(1, Some(0)), (2, None), (3, Some(1))]
    );
}

#[tokio::test(start_paused = true)]
async fn a_normalized_row_dropped_for_a_value_takes_its_child_rows_with_it() {
    let items: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Int64, true)),
        OffsetBuffer::from_lengths([1, 2, 1]),
        Arc::new(Int64Array::from(vec![10, 20, 21, 30])),
        None,
    ));
    let mut columns = far_dates().columns().to_vec();
    columns.push(items);
    let rows =
        RecordBatch::try_from_iter(["id", "d", "items"].into_iter().zip(columns)).expect("a batch");
    let events = BatchStream::new("events", vec![rows]).primary_key(&["id"]);
    let plan = following("d", SchemaPolicy::DiscardRow)
        .schema(SchemaSettings::new().nested(Nested::normalize()));
    let outcome = ran("unheld_normalized", plan, events).await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    let items = rdlt_connector_reference::published("unheld_normalized", "events__items");
    let values: Vec<i64> = items
        .iter()
        .flat_map(|batch| {
            let values = batch.column_by_name("value").expect("items");
            let values = arrow_cast::cast(values, &DataType::Int64).expect("integers");
            values.as_primitive::<Int64Type>().values().to_vec()
        })
        .collect();
    let mut values = values;
    values.sort_unstable();
    assert_eq!(values, [10, 30], "row 2's items go with it");
}

/// A batch of ids 1 to 3 changing at 10 seconds, at a second microseconds cannot hold, and at
/// 30, a null change time among them where `null` says.
fn changing(null: bool) -> RecordBatch {
    let middle = if null { None } else { Some(i64::MAX) };
    let at: ArrayRef = Arc::new(TimestampSecondArray::from(vec![Some(10), middle, Some(30)]));
    batch(vec![
        ("id", ints(&[1, 2, 3])),
        ("v", text(&["a", "b", "c"])),
        ("at", at),
    ])
}

/// Runs a history stream of `changing(null)` changes into `store`, its change time following
/// `policy`.
async fn kept(store: &str, policy: SchemaPolicy, null: bool) -> RunOutcome {
    let events = BatchStream::new("events", vec![changing(null)])
        .primary_key(&["id"])
        .change_time("at");
    let plan = following("at", policy).write(WriteMode::History);
    ran(store, plan, events).await
}

#[tokio::test(start_paused = true)]
async fn a_change_time_no_version_can_begin_at_follows_its_columns_policy() {
    for null in [false, true] {
        let code = if null {
            "change_time_null"
        } else {
            "change_time_invalid"
        };
        let evolve = format!("change_time_evolve_{null}");
        refused(&kept(&evolve, SchemaPolicy::Evolve, null).await, code);
        let freeze = format!("change_time_freeze_{null}");
        refused(&kept(&freeze, SchemaPolicy::Freeze, null).await, code);
        let dropped = format!("change_time_row_{null}");
        loaded(
            &kept(&dropped, SchemaPolicy::DiscardRow, null).await,
            2,
            (1, 0),
        );
        let store = format!("change_time_value_{null}");
        loaded(
            &kept(&store, SchemaPolicy::DiscardValue, null).await,
            3,
            (0, 1),
        );
        let begun = published(&store, "events", "_rdlt_valid_from");
        assert_eq!(begun.len(), 3);
        // The version whose change time was discarded begins when its batch arrived.
        assert_eq!(
            (begun[0].1, begun[2].1),
            (Some(10_000_000), Some(30_000_000))
        );
        assert!(
            begun[1].1.is_some_and(|from| from > 30_000_000),
            "{begun:?}"
        );
        assert_eq!(published(&store, "events", "at")[1], (2, None));
    }
}

#[tokio::test(start_paused = true)]
async fn a_change_time_holding_no_time_is_refused_whatever_its_policy() {
    for policy in [SchemaPolicy::DiscardRow, SchemaPolicy::DiscardValue] {
        let rows = batch(vec![
            ("id", ints(&[1])),
            ("v", text(&["a"])),
            ("at", text(&["yesterday"])),
        ]);
        let events = BatchStream::new("events", vec![rows])
            .primary_key(&["id"])
            .change_time("at");
        let plan = following("at", policy).write(WriteMode::History);
        let store = format!("timeless_{policy:?}");
        refused(&ran(&store, plan, events).await, "change_time_invalid");
    }
}

#[tokio::test(start_paused = true)]
async fn a_change_time_refused_on_two_counts_is_one_value_discarded() {
    // A far `Date64` no date holds is also a time no version can begin at.
    let at: ArrayRef = Arc::new(Date64Array::from(vec![10 * DAY, i64::MAX, 30 * DAY]));
    let rows = batch(vec![
        ("id", ints(&[1, 2, 3])),
        ("v", text(&["a", "b", "c"])),
        ("at", at),
    ]);
    let events = BatchStream::new("events", vec![rows])
        .primary_key(&["id"])
        .change_time("at");
    let plan = following("at", SchemaPolicy::DiscardValue).write(WriteMode::History);
    loaded(&ran("far_change_time", plan, events).await, 3, (0, 1));
}

/// A struct column `s` of rows 1 to 3 whose field `d` holds a far date only under its null row 2,
/// and a list column `l` whose null row 2 spans an item holding one.
fn hidden() -> RecordBatch {
    let dates: ArrayRef = Arc::new(Date64Array::from(vec![0, i64::MAX, DAY]));
    let fields = arrow_schema::Fields::from(vec![Field::new("d", DataType::Date64, true)]);
    let nulls = Some(vec![true, false, true].into());
    let rows: ArrayRef =
        Arc::new(StructArray::try_new(fields, vec![Arc::clone(&dates)], nulls).expect("a struct"));
    let lists: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Date64, true)),
        OffsetBuffer::from_lengths([1, 1, 1]),
        dates,
        Some(vec![true, false, true].into()),
    ));
    batch(vec![("id", ints(&[1, 2, 3])), ("s", rows), ("l", lists)])
}

#[tokio::test(start_paused = true)]
async fn a_value_under_a_null_row_is_no_value_and_refuses_nothing() {
    for policy in [
        SchemaPolicy::Evolve,
        SchemaPolicy::Freeze,
        SchemaPolicy::DiscardRow,
        SchemaPolicy::DiscardValue,
    ] {
        let store = format!("hidden_{policy:?}");
        let plan = following("s", policy).column("l", SchemaSettings::new().policy(policy));
        let outcome = ran(&store, plan, BatchStream::new("events", vec![hidden()])).await;
        loaded(&outcome, 3, (0, 0));
    }
}

#[tokio::test(start_paused = true)]
async fn a_change_time_refused_on_either_count_is_discarded() {
    // Row 2's time no date holds; row 3 holds no time at all.
    let at: ArrayRef = Arc::new(Date64Array::from(vec![
        Some(10 * DAY),
        Some(i64::MAX),
        None,
    ]));
    let rows = batch(vec![
        ("id", ints(&[1, 2, 3])),
        ("v", text(&["a", "b", "c"])),
        ("at", at),
    ]);
    let events = BatchStream::new("events", vec![rows])
        .primary_key(&["id"])
        .change_time("at");
    let plan = following("at", SchemaPolicy::DiscardValue).write(WriteMode::History);
    loaded(&ran("either_change_time", plan, events).await, 3, (0, 2));
}
