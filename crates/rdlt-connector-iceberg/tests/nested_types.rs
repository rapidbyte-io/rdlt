//! The destination advertises `structs: true` and `scalar_lists: true`. This is
//! the cell that exercises them end to end against a live catalog.
//!
//! It is CONFIRMATION, not evidence. A container-gated test skips when no
//! runtime is present, and a skipping test is green — so it can never
//! demonstrate that a fix was needed. The red pin for the drift defect is the
//! container-free one in `dest::schema::comparison_tests`.

mod common;

use common::CatalogFixture;
use rdlt_connector::StreamSpec;
use rdlt_connector_iceberg::IcebergDest;
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::{MemoryBatch, MemorySource, MemoryStream};
use serde_json::json;

/// Loading the same nested stream TWICE must settle. The second load re-ensures
/// the table, which is where a schema comparison that includes catalog-assigned
/// field ids reports drift for a stream that has not changed.
#[tokio::test(flavor = "multi_thread")]
async fn a_nested_stream_loads_twice_without_reporting_drift() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let dest = IcebergDest::from_config(fixture.config("nested")).expect("dest");

    let rows = || {
        vec![
            json!({"id": 1, "profile": {"city": "eu", "zip": 10}, "tags": ["a", "b"]}),
            json!({"id": 2, "profile": {"city": "us", "zip": 20}, "tags": []}),
        ]
    };
    for attempt in 1..=2 {
        let source = MemorySource::new(vec![MemoryStream::new(
            StreamSpec::new("events"),
            vec![MemoryBatch::new(rows()).with_checkpoint(attempt)],
        )]);
        Engine::new(EngineConfig::new("ice-nested"), source, dest.clone())
            .run()
            .await
            .unwrap_or_else(|e| panic!("load {attempt} must settle, got {e:?}"));
    }
}
