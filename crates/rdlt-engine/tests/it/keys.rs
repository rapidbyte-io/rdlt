//! Keys no row can be matched or identified by — missing, null, NaN, or flagged unchanged by a
//! change — refused by the engine before any destination sees them, for every kind of table.

use std::sync::Arc;

use arrow_array::{
    ArrayRef, BinaryArray, FixedSizeBinaryArray, Float64Array, Int8Array, Int64Array, RecordBatch,
    StringArray,
};
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
