//! Floats and 64-bit integers meeting across runs: floats arriving at a column of integers land
//! beside it as floats, whatever integers it holds, and integers arriving at a column of floats
//! land in it only where a float holds every integer pushed with them exactly.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch};
use bytes::Bytes;
use rdlt_connector::{Push, ReadMode};
use rdlt_engine::{
    BatchPolicy, EngineConfigBuilder, Nested, RunStatus, SchemaSettings, StreamPlan,
};
use serde_json::{Value, json};

use crate::support::batches::{BatchStream, batches};
use crate::support::{commit_every, engine, memory, pipeline, published_json, stream};

/// Runs `store`'s pipeline over `pushes`, reading those earlier runs did not.
async fn run(store: &str, pushes: Vec<Push>) {
    let plan = stream("events").read(ReadMode::Incremental);
    run_as(store, commit_every(1000), plan, pushes).await;
}

/// Runs `store`'s pipeline of `plan` over `pushes` with `config`, reading those earlier runs did
/// not.
async fn run_as(store: &str, config: EngineConfigBuilder, plan: StreamPlan, pushes: Vec<Push>) {
    let stream_ = BatchStream {
        pushes,
        ..BatchStream::new("events", Vec::new())
    };
    let outcome = engine(config)
        .run(
            pipeline(store, [plan]),
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
async fn floats_after_integers_land_beside_them_as_floats_whatever_the_integers() {
    let arrow =
        |column: ArrayRef| Push::Arrow(RecordBatch::try_from_iter([("n", column)]).unwrap());
    let integers = |values: Vec<i64>| arrow(Arc::new(Int64Array::from(values)));
    let floats = arrow(Arc::new(Float64Array::from(vec![0.5])));
    let cases = [
        (
            "exact_json",
            json("{\"n\":1}\n{\"n\":-9007199254740992}"),
            json("{\"n\":2.5}"),
            vec![
                json!({"n": -9_007_199_254_740_992_i64}),
                json!({"n": 1}),
                json!({"n__float64": 2.5}),
            ],
        ),
        (
            "rounded_json",
            json("{\"n\":9007199254740993}"),
            json("{\"n\":2.5}"),
            vec![
                json!({"n": 9_007_199_254_740_993_i64}),
                json!({"n__float64": 2.5}),
            ],
        ),
        (
            "exact_arrow",
            integers(vec![1_i64 << 53]),
            floats.clone(),
            vec![
                json!({"n": 9_007_199_254_740_992_i64}),
                json!({"n__float64": 0.5}),
            ],
        ),
        (
            "rounded_arrow",
            integers(vec![(1_i64 << 53) + 1]),
            floats,
            vec![
                json!({"n": 9_007_199_254_740_993_i64}),
                json!({"n__float64": 0.5}),
            ],
        ),
    ];
    for (store, integers, floats, expected) in cases {
        assert_eq!(after(store, integers, floats).await, expected, "{store}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_push_s_integers_are_judged_together_however_its_records_are_shredded() {
    // Chunks of 16 bytes shred each record apart; the push's integers still land alike.
    let policy =
        BatchPolicy::new(8 << 20, 1 << 20, Duration::from_secs(1), 16).expect("a valid policy");
    let config = || commit_every(1000).batch(policy);
    let plan = || stream("events").read(ReadMode::Incremental);
    let floats = json("{\"n\":0.5}");
    run_as("chunked", config(), plan(), vec![floats.clone()]).await;
    let integers = json("{\"n\":9007199254740993}\n{\"n\":1}");
    run_as("chunked", config(), plan(), vec![floats, integers]).await;
    assert_eq!(
        published_json("chunked", "events"),
        [
            json!({"n": 0.5}),
            json!({"n__json": "1"}),
            json!({"n__json": "9007199254740993"}),
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_normalized_push_s_integers_are_judged_together_however_its_records_are_shredded() {
    let policy =
        BatchPolicy::new(8 << 20, 1 << 20, Duration::from_secs(1), 16).expect("a valid policy");
    let config = || commit_every(1000).batch(policy);
    let plan = || {
        stream("events")
            .read(ReadMode::Incremental)
            .schema(SchemaSettings::new().nested(Nested::Normalize { max_depth: 2 }))
    };
    let floats = json("{\"n\":0.5,\"items\":[{\"m\":0.5}]}");
    run_as("normalized_chunks", config(), plan(), vec![floats.clone()]).await;
    let integers = json(
        "{\"n\":9007199254740993,\"items\":[{\"m\":9007199254740993}]}\n\
         {\"n\":1,\"items\":[{\"m\":1}]}",
    );
    run_as(
        "normalized_chunks",
        config(),
        plan(),
        vec![floats, integers],
    )
    .await;
    let rows = published_json("normalized_chunks", "events");
    assert_eq!(
        rows,
        [
            json!({"n": 0.5}),
            json!({"n__json": "1"}),
            json!({"n__json": "9007199254740993"}),
        ]
    );
    let items = published_json("normalized_chunks", "events__items");
    assert_eq!(
        items,
        [
            json!({"m": 0.5}),
            json!({"m__json": "1"}),
            json!({"m__json": "9007199254740993"}),
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_normalized_push_of_one_unit_has_its_integers_judged_as_they_lie() {
    let plan = || {
        stream("events")
            .read(ReadMode::Incremental)
            .schema(SchemaSettings::new().nested(Nested::Normalize { max_depth: 2 }))
    };
    let floats = json("{\"n\":0.5,\"items\":[{\"m\":0.5}]}");
    run_as(
        "normalized_unit",
        commit_every(1000),
        plan(),
        vec![floats.clone()],
    )
    .await;
    let integers = json(
        "{\"n\":9007199254740993,\"items\":[{\"m\":9007199254740993}]}\n\
         {\"n\":1,\"items\":[{\"m\":1}]}",
    );
    run_as(
        "normalized_unit",
        commit_every(1000),
        plan(),
        vec![floats, integers],
    )
    .await;
    let rows = published_json("normalized_unit", "events");
    assert_eq!(
        rows,
        [
            json!({"n": 0.5}),
            json!({"n__json": "1"}),
            json!({"n__json": "9007199254740993"})
        ]
    );
    let items = published_json("normalized_unit", "events__items");
    assert_eq!(
        items,
        [
            json!({"m": 0.5}),
            json!({"m__json": "1"}),
            json!({"m__json": "9007199254740993"})
        ]
    );
}
