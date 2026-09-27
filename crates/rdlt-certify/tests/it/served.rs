//! The reference connectors, served in this process and certified through the protocol.

use rdlt_certify::{Outcome, Probe, Target, certify_destination, certify_source};
use rdlt_connector::serve::Served;
use rdlt_connector::{BoxFuture, TableRef, destination_factory, source_factory};
use rdlt_connector_reference::{GeneratorSource, MemoryDestination, MemorySource, published};
use serde_json::json;

struct MemoryProbe(&'static str);

impl Probe for MemoryProbe {
    fn published<'a>(
        &'a self,
        table: &'a TableRef,
    ) -> BoxFuture<'a, rdlt_connector::Result<Vec<arrow_array::RecordBatch>>> {
        let batches = published(self.0, &table.name);
        Box::pin(async move { Ok(batches) })
    }
}

#[tokio::test]
async fn the_generator_served_in_process_is_certified_through_the_protocol() {
    let target = Target::served(Served::new().with_source(source_factory::<GeneratorSource>()));
    let config = json!({
        "seed": 7,
        "streams": [{ "name": "events", "rows": 57, "partitions": 3, "batch_rows": 5 }],
    });
    let report = certify_source(&target, config).await;
    report.assert_passed();
    for id in [
        "P-HANDSHAKE",
        "P-ORDER",
        "P-ROLE",
        "P-LIMITS",
        "P-HEARTBEAT",
        "P-MALFORMED",
        "P-CREDIT",
        "S-RESUME",
        "S-BARRIER",
    ] {
        assert_eq!(report.outcome(id), Some(&Outcome::Passed), "{id}: {report}");
    }
}

#[tokio::test]
async fn the_memory_destination_served_in_process_is_certified_through_the_protocol() {
    let target =
        Target::served(Served::new().with_destination(destination_factory::<MemoryDestination>()));
    let report = certify_destination(
        &target,
        json!({ "store": "certify_served" }),
        &MemoryProbe("certify_served"),
    )
    .await;
    report.assert_passed();
    assert_eq!(
        report
            .outcome("P-CREDIT")
            .map(|outcome| matches!(outcome, Outcome::Skipped(_))),
        Some(true),
        "{report}"
    );
    assert_eq!(
        report.outcome("D-FENCE"),
        Some(&Outcome::Passed),
        "{report}"
    );
}

#[tokio::test]
async fn the_memory_source_served_in_process_is_certified_through_the_protocol() {
    let target = Target::served(Served::new().with_source(source_factory::<MemorySource>()));
    let config =
        json!({ "streams": { "users": [{"id": 1}, {"id": 2}, {"id": 3}] }, "page_size": 1 });
    certify_source(&target, config).await.assert_passed();
}
