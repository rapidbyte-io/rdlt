//! Reading back what a destination published, over the wire: every destination clause runs, and
//! a read-back that fails, never ends or never finishes fails what needs it.

use rdlt_certify::{Outcome, Probe, Target, certify_destination, read_back};
use rdlt_connector::serve::Served;
use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, NullArray, RecordBatch};
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

/// A memory destination whose read-backs are whatever its reader answers.
struct Reads<R>(Box<dyn DestinationFactory>, R);

impl<R: PublishedReader + Clone + 'static> DestinationFactory for Reads<R> {
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
            let destination = self.0.connect(config, context).await?;
            let reader = Arc::new(self.1.clone()) as Arc<dyn PublishedReader>;
            Ok((Arc::from(destination), reader))
        })
    }
}

/// Reads back batches of as many rows each, of a bit and of nothing: rows that cost no bytes.
#[derive(Clone)]
struct Bits(Vec<usize>);

impl PublishedReader for Bits {
    fn published<'a>(
        &'a self,
        _: &'a TableRef,
    ) -> BoxFuture<'a, rdlt_connector::Result<Vec<RecordBatch>>> {
        let batch = |rows: &usize| {
            let ids: ArrayRef = Arc::new(BooleanArray::from(vec![true; *rows]));
            let names: ArrayRef = Arc::new(NullArray::new(*rows));
            RecordBatch::try_from_iter([("id", ids), ("name", names)]).expect("a valid batch")
        };
        let batches = self.0.iter().map(batch).collect();
        Box::pin(async move { Ok(batches) })
    }
}

#[tokio::test]
async fn a_read_back_is_decoded_up_to_the_rows_certification_reads_and_no_further() {
    // The most rows certification reads of a table.
    let most = 100_000;
    let cases = [
        (vec![most], Some(most)),
        (vec![most / 2, most / 2], Some(most)),
        (vec![most + 1], None),
        (vec![most, 1], None),
        (vec![1, most], None),
        // A megabyte of frames, read back within every limit of the wire.
        (vec![1 << 20; 8], None),
    ];
    for (batches, read) in cases {
        let factory = Reads(
            destination_factory::<MemoryDestination>(),
            Bits(batches.clone()),
        );
        let target = Target::served(Served::new().with_destination(Box::new(factory)));
        let probe = read_back(&target, &json!({ "store": "certify_bits" }))
            .await
            .expect("the destination accepts the read-back");
        let published = probe.published(&table()).await;
        if let Some(rows) = read {
            let batches = published.expect("a read-back within its rows is read");
            let decoded: usize = batches.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(decoded, rows, "{batches:?}");
        } else {
            let error = published.expect_err("a read-back beyond its rows is refused");
            assert_eq!(error.code(), Some("published_rows"), "{batches:?}: {error}");
        }
    }
}

#[tokio::test]
async fn a_read_back_of_rows_that_cost_no_bytes_fails_every_clause_that_reads_it() {
    let factory = Reads(
        destination_factory::<MemoryDestination>(),
        // Two batches, each of more rows than a read-back decodes of a table.
        Bits(vec![1 << 17; 2]),
    );
    let target = Target::served(Served::new().with_destination(Box::new(factory)));
    let config = json!({ "store": "certify_flood" });
    let probe = read_back(&target, &config)
        .await
        .expect("the destination accepts the read-back");
    let report = certify_destination(&target, config, &probe).await;
    for id in [
        "D-STAGING",
        "D-COMMIT",
        "D-MERGE",
        "D-HIST",
        "D-NAMES",
        "K-DESTINATION",
    ] {
        let Some(Outcome::Failed(reason)) = report.outcome(id) else {
            panic!("{id} did not fail: {report}");
        };
        assert!(reason.len() <= rdlt_certify::REASON_BYTES, "{id}");
    }
    // The whole report is no larger than its clauses' statements and bounded reasons.
    assert!(rdlt_certify::plain(&report).len() < 100_000);
}

/// A memory destination whose configuration, its `connect`, never answers, as one dialing a
/// store that never answers does.
struct ConfigureNever(Box<dyn DestinationFactory>);

impl DestinationFactory for ConfigureNever {
    fn spec(&self) -> &ConnectorSpec {
        self.0.spec()
    }

    fn connect(
        &self,
        _: serde_json::Value,
        _: ConnectContext,
    ) -> BoxFuture<'_, rdlt_connector::Result<Box<dyn rdlt_connector::Destination>>> {
        Box::pin(std::future::pending())
    }
}

#[tokio::test(start_paused = true)]
async fn a_read_back_probe_never_answered_ends_at_its_deadline_and_fails_what_needs_it() {
    let unconfigured = {
        let factory = ConfigureNever(destination_factory::<MemoryDestination>());
        Target::served(Served::new().with_destination(Box::new(factory)))
    };
    let deaf = Target::connected(|| Box::pin(async { served(Fault::Deaf) }));
    for (call, target) in [("configure", unconfigured), ("handshake", deaf)] {
        let config = json!({ "store": "certify_never" });
        // A week of paused time passes at once: a call without a deadline fails this bound.
        let week = std::time::Duration::from_hours(7 * 24);
        let probe = tokio::time::timeout(week, read_back(&target, &config))
            .await
            .unwrap_or_else(|_| panic!("the probe's {call} holds the certification"))
            .expect("a probe that could not ask still fails what needs it");
        assert!(probe.published(&table()).await.is_err(), "{call}");
    }
}
