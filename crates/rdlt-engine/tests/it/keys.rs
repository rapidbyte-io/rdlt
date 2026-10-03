//! Keys no row can be matched or identified by — missing, null, NaN, or flagged unchanged by a
//! change — refused by the engine before any destination sees them, for every kind of table.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type};
use arrow_array::{
    ArrayRef, BinaryArray, FixedSizeBinaryArray, Float64Array, Int8Array, Int64Array, ListArray,
    RecordBatch, StringArray,
};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field};
use rdlt_connector::{ChangeOp, OP_COLUMN, Push, ReadMode, SEQ_COLUMN, UNCHANGED_COLUMN};
use rdlt_engine::{ErrorKind, Nested, RunOutcome, RunStatus, SchemaSettings, WriteMode};

use crate::change_limits::Pushing;
use crate::changes::{changes, orders};
use crate::support::batches::{BatchStream, batches};
use crate::support::targets::Target;
use crate::support::{commit_every, each, engine, memory, pipeline, stream};

/// Checks that `outcome` failed on the engine's refusal `code`, a Schema error.
fn refused(outcome: &RunOutcome, code: &str) {
    assert_eq!(outcome.report.status, RunStatus::Failed);
    let error = outcome.error.as_ref().expect("the run fails");
    assert_eq!(
        (error.kind(), error.code(), error.is_retryable()),
        (ErrorKind::Schema, Some(code), false),
        "{error}"
    );
}

/// A batch keyed by floats `ids`.
fn floats(ids: Vec<f64>) -> RecordBatch {
    let values: Vec<&str> = ids.iter().map(|_| "x").collect();
    RecordBatch::try_from_iter([
        ("id", Arc::new(Float64Array::from(ids)) as ArrayRef),
        ("v", Arc::new(StringArray::from(values)) as ArrayRef),
    ])
    .expect("a batch")
}

#[tokio::test(start_paused = true)]
async fn a_nan_key_is_refused_by_the_engine_for_every_destination_and_keyed_mode() {
    each(Target::IN_PROCESS, |target| async move {
        let modes = [("merge", WriteMode::Merge), ("history", WriteMode::History)];
        for (mode, write) in modes {
            let store = format!("nan_key_{mode}");
            let source = batches(
                &store,
                vec![BatchStream::new(
                    "events",
                    vec![floats(vec![f64::NAN, 1.5])],
                )],
            )
            .await;
            let plan = stream("events").write(write).key(["id"]);
            let outcome = engine(commit_every(10))
                .run(
                    pipeline(&store, [plan]),
                    source,
                    target.destination(&store).await,
                )
                .await;
            refused(&outcome, "merge_key_nan");
        }
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn a_normalized_streams_rows_without_a_whole_key_are_refused() {
    let normalized = || stream("events").schema(SchemaSettings::new().nested(Nested::normalize()));
    let cases = [
        ("keyless", r#"{"x":1,"items":[1]}"#, "merge_key_missing"),
        ("null_keyed", r#"{"id":null,"items":[1]}"#, "merge_key_null"),
    ];
    for (store, push, code) in cases {
        let rows =
            BatchStream::json("events", &[push, r#"{"id":2,"items":[2]}"#]).primary_key(&["id"]);
        let outcome = engine(commit_every(10))
            .run(
                pipeline(store, [normalized()]),
                batches(store, vec![rows]).await,
                memory(store).await,
            )
            .await;
        refused(&outcome, code);
    }
    let store = "nan_keyed";
    let rows = BatchStream::new("events", vec![floats(vec![f64::NAN])]).primary_key(&["id"]);
    let outcome = engine(commit_every(10))
        .run(
            pipeline(store, [normalized()]),
            batches(store, vec![rows]).await,
            memory(store).await,
        )
        .await;
    refused(&outcome, "merge_key_nan");
}

/// A change inserting key `id`, flagging its key column unchanged.
fn flagging_its_key(id: i64) -> RecordBatch {
    let seq = [0xff_u8; 16];
    let seqs = FixedSizeBinaryArray::try_from_iter([seq].into_iter()).expect("a sequence");
    RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![id])) as ArrayRef),
        ("value", Arc::new(StringArray::from(vec!["x"])) as ArrayRef),
        ("n", Arc::new(Int64Array::from(vec![-1])) as ArrayRef),
        (
            OP_COLUMN,
            Arc::new(Int8Array::from(vec![ChangeOp::Insert.code()])) as ArrayRef,
        ),
        (SEQ_COLUMN, Arc::new(seqs) as ArrayRef),
        (
            UNCHANGED_COLUMN,
            Arc::new(BinaryArray::from(vec![Some(&[0b001_u8][..])])) as ArrayRef,
        ),
    ])
    .expect("a valid change batch")
}

#[tokio::test(start_paused = true)]
async fn a_change_flagging_its_key_unchanged_is_refused_before_the_destination_sees_it() {
    each(Target::IN_PROCESS, |target| async move {
        let store = "flagged_key";
        let flagging = Arc::new(Pushing {
            inner: changes(21, &orders(&[])).await,
            push: Some(Push::Changes(flagging_its_key(9_999))),
            phased: false,
        });
        let plan = stream("orders").read(ReadMode::Cdc).write(WriteMode::Merge);
        let outcome = engine(commit_every(16))
            .run(
                pipeline(store, [plan]),
                flagging,
                target.destination(store).await,
            )
            .await;
        refused(&outcome, "merge_key_unchanged");
    })
    .await;
}

/// A row keyed by the float `id`, holding `items` and the text `v`.
fn keyed_items(id: f64, items: &[i64], v: &str) -> RecordBatch {
    let list: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Int64, true)),
        OffsetBuffer::from_lengths([items.len()]),
        Arc::new(Int64Array::from(items.to_vec())),
        None,
    ));
    RecordBatch::try_from_iter([
        ("id", Arc::new(Float64Array::from(vec![id])) as ArrayRef),
        ("v", Arc::new(StringArray::from(vec![v])) as ArrayRef),
        ("items", list),
    ])
    .expect("a batch")
}

/// The column `name` of `batches`, as `to`.
fn column_of(batches: &[RecordBatch], name: &str, to: &DataType) -> Vec<ArrayRef> {
    batches
        .iter()
        .map(|batch| arrow_cast::cast(batch.column_by_name(name).expect("the column"), to))
        .collect::<Result<_, _>>()
        .expect("castable")
}

#[tokio::test(start_paused = true)]
async fn a_negative_zero_key_is_the_key_zero_for_every_destination_and_keyed_mode() {
    each(Target::IN_PROCESS, |target| async move {
        let modes = [
            ("merge", WriteMode::Merge, false),
            ("normalized", WriteMode::Merge, true),
            ("history", WriteMode::History, false),
        ];
        for (mode, write, normalized) in modes {
            let store = format!("negative_zero_{mode}");
            let runs = [keyed_items(0.0, &[1, 2], "a"), keyed_items(-0.0, &[3], "b")];
            for rows in runs {
                let source = batches(&store, vec![BatchStream::new("events", vec![rows])]).await;
                let mut plan = stream("events").write(write).key(["id"]);
                if normalized {
                    plan = plan.schema(SchemaSettings::new().nested(Nested::normalize()));
                }
                let outcome = engine(commit_every(10))
                    .run(
                        pipeline(&store, [plan]),
                        source,
                        target.destination(&store).await,
                    )
                    .await;
                assert_eq!(
                    outcome.report.status,
                    RunStatus::Succeeded,
                    "{target:?} {mode}: {:?}",
                    outcome.error
                );
            }
            let roots = target.published(&store, "events");
            let ids = column_of(&roots, "id", &DataType::Float64);
            let signs: Vec<bool> = ids
                .iter()
                .flat_map(|ids| {
                    let ids = ids.as_primitive::<Float64Type>().clone();
                    ids.values()
                        .iter()
                        .map(|id| id.is_sign_negative())
                        .collect::<Vec<_>>()
                })
                .collect();
            let versions = if mode == "history" { 2 } else { 1 };
            assert_eq!(signs, vec![false; versions], "{target:?} {mode}");
            if normalized {
                let items = target.published(&store, "events__items");
                let mut values: Vec<i64> = column_of(&items, "value", &DataType::Int64)
                    .iter()
                    .flat_map(|values| values.as_primitive::<Int64Type>().values().to_vec())
                    .collect();
                values.sort_unstable();
                assert_eq!(
                    values,
                    [3],
                    "{target:?}: the key's children are its latest row's"
                );
            }
        }
    })
    .await;
}
