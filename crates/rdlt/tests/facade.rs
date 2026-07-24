//! T027: the US1 flow through the public facade, plus build-time validation (B1–B3).

use rdlt::prelude::*;
use rdlt_testkit::{MemoryDestination, MemorySource};
use serde_json::json;

#[tokio::test]
async fn full_sync_through_the_facade() {
    let source = MemorySource::single_stream(
        rdlt_connector::StreamSpec::new("users"),
        vec![
            json!({"id": 1, "name": "ada", "emails": [{"addr": "a@x"}]}),
            json!({"id": 2, "name": "grace", "emails": []}),
        ],
    );
    let dest = MemoryDestination::new();

    let pipeline = Pipeline::builder("facade-demo")
        .source(source)
        .destination(dest.clone())
        .write_mode(WriteMode::Append)
        .build()
        .expect("valid config");

    // A pipeline is one-shot: `run` consumes it, so a second run cannot compile
    // (proven by the `compile_fail` doctest on `Pipeline::run`). Resuming means
    // building a fresh pipeline, which continues from committed state.
    let report = pipeline.run().await.expect("run succeeds");
    assert_eq!(report.total_rows(), 3, "2 users + 1 email child row");
    assert_eq!(dest.committed_rows("users").len(), 2);
    assert_eq!(dest.committed_rows("users__emails").len(), 1);
}

#[test]
fn build_rejects_merge_against_non_merge_destination() {
    let dest =
        MemoryDestination::new().with_capabilities(rdlt_connector::DestinationCapabilities {
            merge: false,
            ..rdlt_connector::DestinationCapabilities::default()
        });
    let err = Pipeline::builder("bad")
        .source(MemorySource::default())
        .destination(dest)
        .write_mode(WriteMode::Merge {
            key: vec!["id".into()],
        })
        .build()
        .expect_err("must fail fast at build time, pre-I/O");
    assert!(matches!(err, RdltError::Config { .. }));
    assert!(err.to_string().contains("Merge"));
}

#[test]
fn build_rejects_empty_merge_key() {
    let err = Pipeline::builder("bad")
        .source(MemorySource::default())
        .destination(MemoryDestination::new())
        .write_mode(WriteMode::Merge { key: vec![] })
        .build()
        .expect_err("empty key is a config error");
    assert!(err.to_string().contains("key"));
}
