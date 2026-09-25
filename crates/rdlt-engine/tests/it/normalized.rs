//! Normalized streams against every reference destination.

use rdlt_engine::{Nested, RunStatus, SchemaSettings, StreamPlan};
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
