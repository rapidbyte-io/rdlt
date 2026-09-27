//! Reading back what a destination published, over the wire: every destination clause runs, and
//! a read-back that fails, never ends or never finishes fails what needs it.

use rdlt_certify::{Outcome, Probe, Target, certify_destination, read_back};
use rdlt_connector::serve::Served;
use rdlt_connector::{
    ConnectorErrorKind, SchemaVersion, TablePath, TableRef, destination_factory,
    readable_destination_factory,
};
use rdlt_connector_reference::MemoryDestination;
use serde_json::json;

use crate::faults::fake::{Fault, served};

fn table() -> TableRef {
    TableRef {
        path: TablePath::new(["events"]).expect("a valid path"),
        name: "events".into(),
        version: SchemaVersion(1),
        generation: None,
        merge: None,
    }
}

#[tokio::test]
async fn a_destination_that_reads_back_is_certified_in_every_clause_that_reads_it() {
    let served =
        Served::new().with_destination(readable_destination_factory::<MemoryDestination>());
    let target = Target::served(served);
    let config = json!({ "store": "certify_read_back" });
    let probe = read_back(&target, &config)
        .await
        .expect("the destination reads back");
    let report = certify_destination(&target, config, &probe).await;
    report.assert_passed();
    for id in [
        "D-STAGING",
        "D-COMMIT",
        "D-IDEMPOTENT",
        "D-DISCARD",
        "D-MERGE",
        "D-FENCE",
    ] {
        assert_eq!(report.outcome(id), Some(&Outcome::Passed), "{id}: {report}");
    }
}

#[tokio::test]
async fn a_destination_that_cannot_read_back_is_not_read_back() {
    let served = Served::new().with_destination(destination_factory::<MemoryDestination>());
    let target = Target::served(served);
    assert!(
        read_back(&target, &json!({ "store": "certify_unread" }))
            .await
            .is_none()
    );
}

#[tokio::test]
async fn a_read_back_that_fails_ends_or_never_finishes_is_an_error() {
    for (fault, kind) in [
        (Fault::ReadBackFails, ConnectorErrorKind::Transient),
        (Fault::ReadBackEndless, ConnectorErrorKind::Data),
        (Fault::ReadBackUnfinished, ConnectorErrorKind::Data),
    ] {
        let target = Target::connected(move || Box::pin(async move { served(fault) }));
        let probe = read_back(&target, &json!({}))
            .await
            .expect("the fake reads back");
        let error = probe
            .published(&table())
            .await
            .expect_err("the read-back fails");
        assert_eq!(error.kind(), kind, "{fault:?}: {error}");
    }
}
