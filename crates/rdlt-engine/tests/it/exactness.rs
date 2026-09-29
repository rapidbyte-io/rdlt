//! Floats arriving at a column of integers, in a later run: beside integers every one of which a
//! float holds exactly they land as floats, and beside any other as JSON text.

use std::sync::Arc;

use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch};
use bytes::Bytes;
use rdlt_connector::{Push, ReadMode};
use rdlt_engine::RunStatus;
use serde_json::{Value, json};

use crate::support::batches::{BatchStream, batches};
use crate::support::{commit_every, engine, memory, pipeline, published_json, stream};

/// Runs `store`'s pipeline over `pushes`, reading those earlier runs did not.
async fn run(store: &str, pushes: Vec<Push>) {
    let stream_ = BatchStream {
        pushes,
        ..BatchStream::new("events", Vec::new())
    };
    let outcome = engine(commit_every(1000))
        .run(
            pipeline(store, [stream("events").read(ReadMode::Incremental)]),
            batches(store, vec![stream_]).await,
            memory(store).await,
        )
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
}

fn json(text: &'static str) -> Push {
    Push::Json(Bytes::from_static(text.as_bytes()))
}

/// The rows `store` published, after runs pushing `integers` then `floats`.
async fn after(store: &str, integers: Push, floats: Push) -> Vec<Value> {
    run(store, vec![integers.clone()]).await;
    run(store, vec![integers, floats]).await;
    published_json(store, "events")
}

#[tokio::test(start_paused = true)]
async fn floats_after_json_integers_every_one_exact_land_beside_them_as_floats() {
    let rows = after(
        "exact_json",
        json("{\"n\":1}\n{\"n\":-9007199254740992}"),
        json("{\"n\":2.5}"),
    )
    .await;
    assert_eq!(
        rows,
        [
            json!({"n": -9_007_199_254_740_992_i64}),
            json!({"n": 1}),
            json!({"n__float64": 2.5}),
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn floats_after_a_json_integer_a_float_would_round_land_as_json_text() {
    let rows = after(
        "rounded_json",
        json("{\"n\":9007199254740993}"),
        json("{\"n\":2.5}"),
    )
    .await;
    assert_eq!(
        rows,
        [
            json!({"n": 9_007_199_254_740_993_i64}),
            json!({"n__json": "2.5"}),
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn arrow_integers_are_judged_by_their_values_as_json_ones_are() {
    let batch = |column: ArrayRef| RecordBatch::try_from_iter([("n", column)]).unwrap();
    let exact = batch(Arc::new(Int64Array::from(vec![1_i64 << 53])));
    let rounded = batch(Arc::new(Int64Array::from(vec![(1_i64 << 53) + 1])));
    let floats = batch(Arc::new(Float64Array::from(vec![0.5])));
    let exact = after(
        "exact_arrow",
        Push::Arrow(exact),
        Push::Arrow(floats.clone()),
    )
    .await;
    assert_eq!(
        exact,
        [
            json!({"n": 9_007_199_254_740_992_i64}),
            json!({"n__float64": 0.5}),
        ]
    );
    let rounded = after("rounded_arrow", Push::Arrow(rounded), Push::Arrow(floats)).await;
    assert_eq!(
        rounded,
        [
            json!({"n": 9_007_199_254_740_993_i64}),
            json!({"n__json": "0.5"}),
        ]
    );
}
