//! The reference connectors, served in this process and certified through the protocol.

use rdlt_certify::{Outcome, Probe, Target, certify_destination, certify_source};
use rdlt_connector::serve::Served;
use rdlt_connector::{BoxFuture, TableRef, destination_factory, source_factory};
use rdlt_connector_reference::{GeneratorSource, MemoryDestination, MemorySource, published};
use serde_json::json;

use crate::killed::SETTLED_LATE;

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
    for id in ["D-FENCE", "K-DESTINATION"] {
        assert_eq!(report.outcome(id), Some(&Outcome::Passed), "{id}: {report}");
    }
}

#[tokio::test]
async fn a_source_served_in_process_killed_as_it_loads_resumes_where_it_was() {
    let target = Target::served(Served::new().with_source(source_factory::<GeneratorSource>()))
        .kill_seed(SETTLED_LATE);
    let config = json!({
        "seed": 5,
        "streams": [{ "name": "events", "rows": 20000, "partitions": 2, "batch_rows": 50 }],
    });
    let report = certify_source(&target, config).await;
    assert_eq!(
        report.outcome("K-SOURCE"),
        Some(&Outcome::Passed),
        "{report}"
    );
}

#[tokio::test]
async fn a_source_read_before_any_kill_lands_proves_nothing_of_kills() {
    let target = Target::served(Served::new().with_source(source_factory::<GeneratorSource>()));
    let config = json!({
        "seed": 5,
        "streams": [{ "name": "events", "rows": 300, "partitions": 2, "batch_rows": 5 }],
    });
    let report = certify_source(&target, config).await;
    assert!(
        matches!(report.outcome("K-SOURCE"), Some(Outcome::Skipped(_))),
        "{report}"
    );
}

#[tokio::test]
async fn a_destination_certified_again_in_its_store_is_killed_into_tables_of_its_own() {
    let target =
        Target::served(Served::new().with_destination(destination_factory::<MemoryDestination>()));
    for _ in 0..2 {
        let report = certify_destination(
            &target,
            json!({ "store": "certify_again" }),
            &MemoryProbe("certify_again"),
        )
        .await;
        assert_eq!(
            report.outcome("K-DESTINATION"),
            Some(&Outcome::Passed),
            "{report}"
        );
    }
}

#[tokio::test]
async fn the_memory_source_served_in_process_is_certified_through_the_protocol() {
    let target = Target::served(Served::new().with_source(source_factory::<MemorySource>()));
    let config =
        json!({ "streams": { "users": [{"id": 1}, {"id": 2}, {"id": 3}] }, "page_size": 1 });
    certify_source(&target, config).await.assert_passed();
}

#[tokio::test]
async fn a_kill_timeout_bounds_the_kill_clauses_alone() {
    let target =
        Target::served(Served::new().with_destination(destination_factory::<MemoryDestination>()))
            .kill_timeout(std::time::Duration::ZERO);
    let report = certify_destination(
        &target,
        json!({ "store": "certify_hurried" }),
        &MemoryProbe("certify_hurried"),
    )
    .await;
    assert!(
        matches!(report.outcome("K-DESTINATION"), Some(Outcome::Failed(_))),
        "{report}"
    );
    assert_eq!(
        report.outcome("D-COMMIT"),
        Some(&Outcome::Passed),
        "{report}"
    );
}
