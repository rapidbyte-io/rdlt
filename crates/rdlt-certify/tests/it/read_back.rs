//! Reading back what a destination published, over the wire: every destination clause runs, and
//! a read-back that fails, never ends or never finishes fails what needs it.

use rdlt_certify::{Outcome, Probe, Target, certify_destination, read_back};
use rdlt_connector::serve::Served;
use std::sync::Arc;

use arrow_array::RecordBatch;
use rdlt_connector::{
    BoxFuture, ConnectContext, ConnectorErrorKind, ConnectorSpec, DestinationFactory,
    PublishedReader, Reading, SchemaVersion, TablePath, TableRef, destination_factory,
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
        "D-DELETE",
        "D-PARTIAL",
        "D-TRUNCATE",
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

/// A memory destination that accepts the read-back, and never answers one; or, `refusing`, fails
/// to connect for one.
struct Hanging(Box<dyn DestinationFactory>, bool);

struct Never;

impl PublishedReader for Never {
    fn published<'a>(
        &'a self,
        _: &'a TableRef,
    ) -> BoxFuture<'a, rdlt_connector::Result<Vec<RecordBatch>>> {
        Box::pin(std::future::pending())
    }
}

impl DestinationFactory for Hanging {
    fn spec(&self) -> &ConnectorSpec {
        self.0.spec()
    }

    fn connect(
        &self,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> BoxFuture<'_, rdlt_connector::Result<Box<dyn rdlt_connector::Destination>>> {
        self.0.connect(config, context)
    }

    fn reads_back(&self) -> bool {
        true
    }

    fn connect_reading(
        &self,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> BoxFuture<'_, rdlt_connector::Result<Reading>> {
        Box::pin(async move {
            if self.1 {
                return Err(rdlt_connector::ConnectorError::config("no reader"));
            }
            let destination = self.0.connect(config, context).await?;
            Ok((
                Arc::from(destination),
                Arc::new(Never) as Arc<dyn PublishedReader>,
            ))
        })
    }
}

#[tokio::test(start_paused = true)]
async fn a_read_back_that_never_answers_fails_what_needs_it_in_bounded_time() {
    let factory = Hanging(destination_factory::<MemoryDestination>(), false);
    let target = Target::served(Served::new().with_destination(Box::new(factory)));
    let config = json!({ "store": "certify_hanging" });
    let probe = read_back(&target, &config)
        .await
        .expect("the destination accepts the read-back");
    let error = probe
        .published(&table())
        .await
        .expect_err("the read-back ends");
    assert_eq!(error.code(), Some("published_time"), "{error}");
    let started = tokio::time::Instant::now();
    let report = certify_destination(&target, config, &probe).await;
    assert!(
        matches!(report.outcome("D-COMMIT"), Some(Outcome::Failed(_))),
        "{report}"
    );
    let took = started.elapsed();
    assert!(took.as_secs() < 3600, "certification took {took:?}");
}

#[tokio::test]
async fn a_read_back_whose_handshake_fails_fails_what_needs_it() {
    let factory = Hanging(destination_factory::<MemoryDestination>(), true);
    let target = Target::served(Served::new().with_destination(Box::new(factory)));
    let config = json!({ "store": "certify_refusing" });
    let probe = read_back(&target, &config)
        .await
        .expect("a read-back that cannot be opened is still one to fail");
    let report = certify_destination(&target, config, &probe).await;
    for id in ["D-COMMIT", "D-NAMES"] {
        assert!(
            matches!(report.outcome(id), Some(Outcome::Failed(_))),
            "{id}: {report}"
        );
    }
}
