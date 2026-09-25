//! Normalized streams against every reference destination.

use rdlt_engine::{Nested, RunStatus, SchemaPolicy, SchemaSettings, StreamPlan, WriteMode};
use serde_json::json;

use crate::support::batches::{BatchStream, batches};
use crate::support::targets::Target;
use crate::support::{commit_every, engine, pipeline, stream};

fn normalized(name: &str) -> StreamPlan {
    stream(name).schema(SchemaSettings::new().nested(Nested::normalize()))
}

/// Loads `streams` into `target`'s `store` as `plans` say, and checks the run succeeded.
async fn load(target: Target, store: &str, streams: Vec<BatchStream>, plans: Vec<StreamPlan>) {
    let source = batches(&target.name(store), streams).await;
    let outcome = engine(commit_every(1))
        .run(
            pipeline(store, plans),
            source,
            target.destination(store).await,
        )
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{target:?}: {:?}",
        outcome.error
    );
}

#[tokio::test(start_paused = true)]
async fn every_destination_takes_tables_of_arrays_that_hold_only_arrays() {
    for target in Target::ALL {
        let events = r#"{"id":1,"m":[[1,2],[3]]}"#;
        let only = r#"{"items":[{"sku":"x"}]}"#;
        load(
            target,
            "arrays_of_arrays",
            vec![
                BatchStream::json("events", &[events]),
                BatchStream::json("only", &[only]),
            ],
            vec![normalized("events"), normalized("only")],
        )
        .await;
        let store = "arrays_of_arrays";
        assert_eq!(target.rows(store, "events__m"), 2, "{target:?}");
        assert_eq!(
            target.json(store, "events__m__value"),
            [
                json!({"value": 1}),
                json!({"value": 2}),
                json!({"value": 3})
            ],
            "{target:?}"
        );
        assert_eq!(target.rows(store, "only"), 1, "{target:?}");
        assert_eq!(
            target.json(store, "only__items"),
            [json!({"sku": "x"})],
            "{target:?}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn every_destination_takes_normalize_turned_on_for_an_existing_stream() {
    for target in Target::ALL {
        let store = "turned_on";
        load(
            target,
            store,
            vec![BatchStream::json("events", &[r#"{"id":1}"#])],
            vec![stream("events")],
        )
        .await;
        load(
            target,
            store,
            vec![BatchStream::json(
                "events",
                &[r#"{"id":2,"items":[{"sku":"x"}]}"#],
            )],
            vec![normalized("events")],
        )
        .await;
        assert_eq!(target.rows(store, "events"), 2, "{target:?}");
        assert_eq!(
            target.json(store, "events__items"),
            [json!({"sku": "x"})],
            "{target:?}"
        );
    }
}

fn merged(name: &str) -> StreamPlan {
    normalized(name).write(WriteMode::Merge).key(["id"])
}

#[tokio::test(start_paused = true)]
async fn every_destination_replaces_a_roots_children_when_it_merges() {
    for target in Target::ALL {
        let store = "merged_children";
        let first = [
            r#"{"id":1,"items":[{"sku":"a"},{"sku":"b","tags":["t"]}]}"#,
            r#"{"id":2,"items":[{"sku":"c"}]}"#,
            r#"{"id":3,"items":[{"sku":"d"}]}"#,
        ]
        .join("\n");
        load(
            target,
            store,
            vec![BatchStream::json("events", &[&first])],
            vec![merged("events")],
        )
        .await;
        let second = [
            r#"{"id":1,"items":[{"sku":"e"}]}"#,
            r#"{"id":2,"items":[]}"#,
        ]
        .join("\n");
        load(
            target,
            store,
            vec![BatchStream::json("events", &[&second])],
            vec![merged("events")],
        )
        .await;
        assert_eq!(target.rows(store, "events"), 3, "{target:?}");
        assert_eq!(
            target.json(store, "events__items"),
            [json!({"sku": "d"}), json!({"sku": "e"})],
            "{target:?}: merging 1 and 2 replaced their items; 3 kept its own"
        );
        assert_eq!(
            target.rows(store, "events__items__tags"),
            0,
            "{target:?}: grandchildren follow their root"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn every_destination_keeps_the_children_of_a_keys_last_row_in_a_commit() {
    for target in Target::ALL {
        let store = "last_row";
        let push = [
            r#"{"id":1,"items":[{"sku":"old"}]}"#,
            r#"{"id":1,"items":[]}"#,
            r#"{"id":2,"items":[{"sku":"x"}]}"#,
            r#"{"id":2,"items":[{"sku":"y"}]}"#,
        ]
        .join("\n");
        load(
            target,
            store,
            vec![BatchStream::json("events", &[&push])],
            vec![merged("events")],
        )
        .await;
        assert_eq!(target.rows(store, "events"), 2, "{target:?}");
        assert_eq!(
            target.json(store, "events__items"),
            [json!({"sku": "y"})],
            "{target:?}"
        );
    }
}

fn dropping_merge() -> StreamPlan {
    stream("events")
        .schema(
            SchemaSettings::new()
                .nested(Nested::normalize())
                .policy(SchemaPolicy::DiscardRow),
        )
        .write(WriteMode::Merge)
        .key(["id"])
}

#[tokio::test(start_paused = true)]
async fn every_destination_keeps_the_children_of_rows_after_a_dropped_one() {
    let plan = dropping_merge;
    for target in Target::ALL {
        let store = "after_dropped";
        let first = r#"{"id":1,"items":[{"sku":"z"}]}"#;
        load(
            target,
            store,
            vec![BatchStream::json("events", &[first])],
            vec![plan()],
        )
        .await;
        let second = [
            r#"{"id":1,"items":[{"sku":"a"}]}"#,
            r#"{"id":2,"extra":1,"items":[{"sku":"b"}]}"#,
            r#"{"id":3,"items":[{"sku":"c"}]}"#,
        ]
        .join("\n");
        load(
            target,
            store,
            vec![BatchStream::json("events", &[&second])],
            vec![plan()],
        )
        .await;
        assert_eq!(target.rows(store, "events"), 2, "{target:?}");
        assert_eq!(
            target.json(store, "events__items"),
            [json!({"sku": "a"}), json!({"sku": "c"})],
            "{target:?}: row 2 went with its item; row 3 kept its own"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn every_destination_drops_only_the_rows_carrying_a_change_among_rows_sharing_a_key() {
    for target in Target::ALL {
        let store = "shared_key";
        let first = r#"{"id":1,"v":0,"items":[{"sku":"z"}]}"#;
        let plan = || vec![dropping_merge()];
        load(
            target,
            store,
            vec![BatchStream::json("events", &[first])],
            plan(),
        )
        .await;
        let second = [
            r#"{"id":1,"v":1,"extra":1,"items":[{"sku":"a"}]}"#,
            r#"{"id":1,"v":2,"tags":["t"],"items":[{"sku":"b"}]}"#,
            r#"{"id":1,"v":3,"items":[{"sku":"c"}]}"#,
        ]
        .join("\n");
        load(
            target,
            store,
            vec![BatchStream::json("events", &[&second])],
            plan(),
        )
        .await;
        assert_eq!(
            target.json(store, "events"),
            [json!({"id": 1, "v": 3})],
            "{target:?}: rows 1 and 2 carried a new column and a new array"
        );
        assert_eq!(
            target.json(store, "events__items"),
            [json!({"sku": "c"})],
            "{target:?}: the kept row keeps its children"
        );
    }
}
