//! JSON pushes end to end, and pushes coalesced into batches: shredding, nesting and coalescing.

use std::sync::Arc;

use arrow_array::StringArray;
use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use bytes::Bytes;
use rdlt_connector::Push;
use rdlt_connector_reference::published;
use std::time::Duration;

use rdlt_engine::{BatchPolicy, ErrorKind, Nested, RunStatus, SchemaSettings, WriteMode};
use serde_json::{Value, json};

use crate::support::batches::{BatchStream, batches};
use crate::support::{
    commit_every, engine, memory, pipeline, published_json, published_rows, stream,
};

/// Loads `stream_` from `store` and checks the run succeeded.
async fn load(store: &str, stream_: BatchStream, nested: Nested) {
    let source = batches(store, vec![stream_]).await;
    let plan = pipeline(
        store,
        [stream("events").schema(SchemaSettings::default().nested(nested))],
    );
    let outcome = engine(commit_every(1000))
        .run(plan, source, memory(store).await)
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
}

/// `value` with every JSON text column parsed, so nested values compare as values.
fn parsed(mut value: Value) -> Value {
    if let Value::Object(columns) = &mut value {
        for column in columns.values_mut() {
            if let Some(text) = column.as_str()
                && let Ok(inner) = serde_json::from_str::<Value>(text)
                && inner.is_object()
            {
                *column = inner;
            }
        }
    }
    value
}

#[tokio::test(start_paused = true)]
async fn nested_lists_inside_objects_are_never_dropped() {
    let rows = [
        r#"{"id":1,"profile":{"tags":["a","b"],"geo":{"points":[[1,2],[3]]}}}"#,
        r#"{"id":2,"profile":{"tags":[],"geo":{"points":[[4]]}}}"#,
    ];
    for (store, nested) in [("d1_native", Nested::Native), ("d1_json", Nested::Json)] {
        load(
            store,
            BatchStream::json("events", &[&rows.join("\n")]),
            nested,
        )
        .await;
        let loaded: Vec<Value> = published_json(store, "events")
            .into_iter()
            .map(parsed)
            .collect();
        assert_eq!(
            loaded,
            [
                json!({"id": 1, "profile": {"tags": ["a", "b"], "geo": {"points": [[1, 2], [3]]}}}),
                json!({"id": 2, "profile": {"tags": [], "geo": {"points": [[4]]}}}),
            ],
            "{nested:?}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn pushes_between_checkpoints_are_coalesced_into_one_batch() {
    let pushes: Vec<String> = (0..10).map(|id| format!(r#"{{"id":{id}}}"#)).collect();
    let pushes: Vec<&str> = pushes.iter().map(String::as_str).collect();
    load(
        "coalesced",
        BatchStream::json("events", &pushes).one_segment(),
        Nested::Native,
    )
    .await;
    load(
        "checkpointed",
        BatchStream::json("events", &pushes),
        Nested::Native,
    )
    .await;
    assert_eq!(published("coalesced", "events").len(), 1);
    assert_eq!(published("checkpointed", "events").len(), 10);
    assert_eq!(
        published_json("coalesced", "events"),
        published_json("checkpointed", "events")
    );
}

#[tokio::test(start_paused = true)]
async fn arrow_batches_between_checkpoints_are_coalesced_into_one() {
    let batches: Vec<RecordBatch> = (0..10)
        .map(|id| {
            let ids = Arc::new(Int64Array::from(vec![id * 2, id * 2 + 1]));
            RecordBatch::try_from_iter([("id", ids as _)]).expect("one column makes a batch")
        })
        .collect();
    load(
        "arrow_coalesced",
        BatchStream::new("events", batches).one_segment(),
        Nested::Native,
    )
    .await;
    assert_eq!(published("arrow_coalesced", "events").len(), 1);
    let ids: Vec<Value> = (0..20).map(|id| json!({ "id": id })).collect();
    let mut loaded = published_json("arrow_coalesced", "events");
    loaded.sort_by_key(|row| row["id"].as_i64());
    assert_eq!(loaded, ids);
}

#[tokio::test(start_paused = true)]
async fn a_push_the_shredder_refuses_fails_the_run_with_its_code() {
    for (store, push, code) in [
        ("truncated", r#"{"id":"#, "json_invalid"),
        ("not_objects", "[1,2]", "json_not_object"),
        ("repeated_key", r#"{"id":1,"id":2}"#, "json_duplicate_key"),
    ] {
        let source = batches(store, vec![BatchStream::json("events", &[push])]).await;
        let outcome = engine(commit_every(10))
            .run(
                pipeline(store, [stream("events")]),
                source,
                memory(store).await,
            )
            .await;
        let error = outcome.error.expect("the run fails");
        assert_eq!(
            (error.kind(), error.code()),
            (ErrorKind::Source, Some(code)),
            "{store}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn json_pushes_larger_than_the_memory_budget_still_load() {
    let rows: Vec<String> = (0..2000)
        .map(|id| format!(r#"{{"id":{id},"pad":"{}"}}"#, "x".repeat(40)))
        .collect();
    let push = rows.join("\n");
    let source = batches(
        "over_budget",
        vec![BatchStream::json("events", &[&push, &push])],
    )
    .await;
    let outcome = engine(commit_every(1000).memory(4096))
        .run(
            pipeline("over_budget", [stream("events")]),
            source,
            memory("over_budget").await,
        )
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert_eq!(published_rows("over_budget", "events"), 4000);
}

#[tokio::test(start_paused = true)]
async fn a_stream_mixing_arrow_and_json_whose_types_disagree_keeps_every_value() {
    let texts: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
    let arrow = RecordBatch::try_from_iter([("v", texts)]).expect("one column makes a batch");
    let stream_ = BatchStream {
        pushes: vec![
            Push::Arrow(arrow),
            Push::Json(Bytes::from_static(b"{\"v\":1}\n{\"v\":2.5}")),
        ],
        ..BatchStream::new("events", Vec::new())
    }
    .one_segment();
    load("mixed", stream_, Nested::Native).await;
    // The JSON floats conflict with the text column, so they land in its variant column.
    assert_eq!(
        published_json("mixed", "events"),
        [
            json!({"v": "a"}),
            json!({"v": "b"}),
            json!({"v__json": "1.0"}),
            json!({"v__json": "2.5"}),
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn records_that_are_empty_objects_load_as_rows() {
    load(
        "empty_objects",
        BatchStream::json("events", &["{}\n{}\n{}"]),
        Nested::Native,
    )
    .await;
    assert_eq!(published_rows("empty_objects", "events"), 3);
}

#[tokio::test(start_paused = true)]
async fn rows_shredded_into_several_batches_keep_their_order_for_merges() {
    // Chunks of 16 bytes put the first two records in one batch and the third in another.
    let push = "{\"id\":1,\"v\":1}\n{\"id\":2,\"v\":1}\n{\"id\":1,\"v\":2}";
    let events = BatchStream::json("events", &[push]).primary_key(&["id"]);
    let source = batches("merge_order", vec![events]).await;
    let policy =
        BatchPolicy::new(8 << 20, 1 << 20, Duration::from_secs(1), 16).expect("a valid policy");
    let plan = pipeline("merge_order", [stream("events").write(WriteMode::Merge)]);
    let outcome = engine(commit_every(1000).batch(policy))
        .run(plan, source, memory("merge_order").await)
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert_eq!(
        published_json("merge_order", "events"),
        [json!({"id": 1, "v": 2}), json!({"id": 2, "v": 1})]
    );
}
