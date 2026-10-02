//! The reference connectors, served in this process and certified through the protocol.

use rdlt_certify::{Outcome, Probe, Target, certify_destination, certify_source};
use rdlt_connector::serve::Served;
use rdlt_connector::{
    BoxFuture, TableRef, acknowledging_source_factory, destination_factory, source_factory,
};
use rdlt_connector_reference::{
    ChangesSource, GeneratorSource, MemoryDestination, MemorySource, published,
};
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
    let target = Target::served(Served::new().with_source(source_factory::<GeneratorSource>()))
        .credit_watch(crate::BRIEF);
    let config = json!({
        "seed": 7,
        "streams": [{ "name": "events", "rows": 57, "partitions": 3, "batch_rows": 5 }],
    });
    let report = certify_source(&target, config).await;
    // A source this small ends before a kill lands: its kill clause is not observed.
    assert_eq!(crate::unobserved(&report), ["K-SOURCE"], "{report}");
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
            .map(|outcome| matches!(outcome, Outcome::Inapplicable(_))),
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
        .credit_watch(crate::BRIEF)
        .kill_seed(SETTLED_LATE);
    let config = json!({
        "seed": 5,
        "streams": [{ "name": "events", "rows": 20000, "partitions": 2, "batch_rows": 50 }],
    });
    let report = certify_source(&target, config).await;
    // Every clause that applies was seen to be met, the kill clause among them.
    report.assert_passed();
    assert_eq!(
        report.outcome("K-SOURCE"),
        Some(&Outcome::Passed),
        "{report}"
    );
    // Served in this process, the source is cut off, not killed, and the report says so.
    assert!(
        report
            .note("K-SOURCE")
            .is_some_and(|note| note.starts_with("cut: "))
    );
}

#[tokio::test]
async fn a_source_holding_more_than_a_kill_clause_loads_leaves_it_unobserved() {
    let target = Target::served(Served::new().with_source(source_factory::<GeneratorSource>()))
        .credit_watch(crate::BRIEF)
        .kill_seed(SETTLED_LATE);
    // A hundred rows more than a kill clause loads.
    let config = json!({
        "seed": 5,
        "streams": [{ "name": "events", "rows": 100_100, "partitions": 1, "batch_rows": 500 }],
    });
    let report = certify_source(&target, config).await;
    let outcome = report.outcome("K-SOURCE");
    assert!(matches!(outcome, Some(Outcome::Unobserved(_))), "{report}");
    assert_eq!(report.failures().count(), 0, "{report}");
}

#[tokio::test]
async fn a_source_holding_all_a_kill_clause_loads_is_beyond_it_once_a_kill_repeats_a_row() {
    let target = Target::served(Served::new().with_source(source_factory::<GeneratorSource>()))
        .credit_watch(crate::BRIEF)
        .kill_seed(SETTLED_LATE);
    // Exactly what a kill clause loads: a load never killed writes each row once, and one
    // killed writes again what its kills left uncommitted.
    let config = json!({
        "seed": 5,
        "streams": [{ "name": "events", "rows": 100_000, "partitions": 1, "batch_rows": 500 }],
    });
    let report = certify_source(&target, config).await;
    let outcome = report.outcome("K-SOURCE");
    assert!(matches!(outcome, Some(Outcome::Unobserved(_))), "{report}");
    assert_eq!(report.failures().count(), 0, "{report}");
}

#[tokio::test]
async fn a_source_read_before_any_kill_lands_proves_nothing_of_kills() {
    let target = Target::served(Served::new().with_source(source_factory::<GeneratorSource>()))
        .credit_watch(crate::BRIEF);
    let config = json!({
        "seed": 5,
        "streams": [{ "name": "events", "rows": 300, "partitions": 2, "batch_rows": 5 }],
    });
    let report = certify_source(&target, config).await;
    assert!(
        matches!(report.outcome("K-SOURCE"), Some(Outcome::Unobserved(_))),
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
    let target = Target::served(Served::new().with_source(source_factory::<MemorySource>()))
        .credit_watch(crate::BRIEF);
    let config =
        json!({ "streams": { "users": [{"id": 1}, {"id": 2}, {"id": 3}] }, "page_size": 1 });
    let report = certify_source(&target, config).await;
    assert_eq!(crate::unobserved(&report), ["K-SOURCE"], "{report}");
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

#[tokio::test]
async fn a_change_source_served_in_process_tells_where_it_stands_through_the_protocol() {
    let served = Served::new().with_source(acknowledging_source_factory::<ChangesSource>());
    let target = Target::served(served).credit_watch(crate::BRIEF);
    let config = json!({
        "seed": 5,
        "streams": [{ "name": "accounts", "keys": 9, "changes": 6, "batch_rows": 2 }],
        "slot": "certified_over_the_wire",
    });
    let report = certify_source(&target, config).await;
    // Its changes are few: they end before a kill lands.
    assert_eq!(crate::unobserved(&report), ["K-SOURCE"], "{report}");
    assert_eq!(report.outcome("S-ACK"), Some(&Outcome::Passed), "{report}");
}

#[tokio::test]
async fn s_ack_does_not_apply_through_the_protocol_to_a_source_that_tells_nothing() {
    let target = Target::served(Served::new().with_source(source_factory::<GeneratorSource>()))
        .credit_watch(crate::BRIEF);
    let config = json!({
        "seed": 7,
        "streams": [{ "name": "events", "rows": 5, "partitions": 1, "batch_rows": 5 }],
    });
    let report = certify_source(&target, config).await;
    assert!(
        matches!(report.outcome("S-ACK"), Some(Outcome::Inapplicable(_))),
        "{report}"
    );
}

#[tokio::test]
async fn a_read_back_probe_shows_nothing_of_its_configuration_or_of_what_its_connector_said() {
    use rdlt_connector::readable_destination_factory;
    let served =
        Served::new().with_destination(readable_destination_factory::<MemoryDestination>());
    let target = Target::served(served);
    // A field the destination does not know: its handshake fails, saying so.
    let config = serde_json::json!({ "store": "probe_debug", "password": "hunter2-SECRET" });
    let probe = rdlt_certify::read_back(&target, &config)
        .await
        .expect("a probe whose read-backs fail");
    let shown = format!("{probe:?} {probe:#?}");
    assert!(shown.contains("ReadBackProbe"), "{shown}");
    for hidden in ["hunter2", "password", "probe_debug", "failed"] {
        assert!(!shown.contains(hidden), "{hidden}: {shown}");
    }
}
