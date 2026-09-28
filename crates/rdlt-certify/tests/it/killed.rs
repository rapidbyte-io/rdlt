//! Destinations that lose or repeat rows once killed as an engine loads through them: the kill
//! clause fails them.

use rdlt_certify::{Outcome, Probe, Target, certify_destination};
use rdlt_connector::serve::Served;
use rdlt_connector::{
    BoxFuture, Capabilities, CommitMeta, ConnectContext, ConnectorSpec, Destination,
    DestinationFactory, DestinationSession, DestinationWriter, OpenContext, OpenedSession, Receipt,
    SegmentSet, TableChange, TableRef, destination_factory,
};
use rdlt_connector_reference::{MemoryDestination, published};
use serde_json::json;

/// A seed whose kills land after a commit that published rows.
///
/// They land at the first write after the second commit, before the second, and after the first,
/// losing its answer. A connector that breaks exactly-once only across a commit's rows is caught
/// there, whatever the timing.
pub(crate) const SETTLED_LATE: u64 = 1;

/// A destination that records each commit's state at once but publishes its rows only with the
/// next commit, or as the session closes: a kill between them loses rows its state says it has.
struct Deferring(Box<dyn DestinationFactory>);

struct DeferringDestination(Box<dyn Destination>);

struct DeferringSession {
    session: Box<dyn DestinationSession>,
    held: SegmentSet,
    last: Option<CommitMeta>,
}

impl DestinationFactory for Deferring {
    fn spec(&self) -> &ConnectorSpec {
        self.0.spec()
    }

    fn connect(
        &self,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> BoxFuture<'_, rdlt_connector::Result<Box<dyn Destination>>> {
        Box::pin(async move {
            let destination = self.0.connect(config, context).await?;
            Ok(Box::new(DeferringDestination(destination)) as Box<dyn Destination>)
        })
    }
}

impl Destination for DeferringDestination {
    fn capabilities(&self) -> &Capabilities {
        self.0.capabilities()
    }

    fn check(&self) -> BoxFuture<'_, rdlt_connector::Result<()>> {
        self.0.check()
    }

    fn open<'a>(
        &'a self,
        context: &'a OpenContext,
    ) -> BoxFuture<'a, rdlt_connector::Result<OpenedSession>> {
        Box::pin(async move {
            let opened = self.0.open(context).await?;
            let session = DeferringSession {
                session: opened.session,
                held: SegmentSet::new(),
                last: None,
            };
            Ok(OpenedSession {
                session: Box::new(session),
                ..opened
            })
        })
    }
}

impl DestinationSession for DeferringSession {
    fn apply_schema<'a>(
        &'a mut self,
        change: &'a TableChange,
    ) -> BoxFuture<'a, rdlt_connector::Result<()>> {
        self.session.apply_schema(change)
    }

    fn writer<'a>(
        &'a mut self,
        table: &'a TableRef,
    ) -> BoxFuture<'a, rdlt_connector::Result<Box<dyn DestinationWriter>>> {
        self.session.writer(table)
    }

    fn commit<'a>(
        &'a mut self,
        meta: &'a CommitMeta,
    ) -> BoxFuture<'a, rdlt_connector::Result<Receipt>> {
        Box::pin(async move {
            let mut deferred = meta.clone();
            deferred.segments = std::mem::replace(&mut self.held, meta.segments.clone());
            self.last = Some(meta.clone());
            self.session.commit(&deferred).await
        })
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, rdlt_connector::Result<()>> {
        let Self {
            mut session,
            held,
            last,
        } = *self;
        Box::pin(async move {
            if let Some(last) = last.filter(|_| !held.is_empty()) {
                let rest = CommitMeta {
                    commit_seq: last.commit_seq.next(),
                    segments: held,
                    state_delta: Vec::new(),
                    finish_generations: Vec::new(),
                    ..last
                };
                session.commit(&rest).await?;
            }
            session.close().await
        })
    }
}

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

fn failed(outcome: Option<&Outcome>) -> bool {
    matches!(outcome, Some(Outcome::Failed(_)))
}

#[tokio::test]
async fn a_destination_that_records_state_before_rows_fails_k_destination() {
    let deferring = Deferring(destination_factory::<MemoryDestination>());
    let target =
        Target::served(Served::new().with_destination(Box::new(deferring))).kill_seed(SETTLED_LATE);
    let report = certify_destination(
        &target,
        json!({ "store": "certify_deferring" }),
        &MemoryProbe("certify_deferring"),
    )
    .await;
    let outcome = report.outcome("K-DESTINATION");
    assert!(failed(outcome), "{report}");
}

#[tokio::test]
async fn a_chosen_kill_seed_is_the_one_a_failure_reports() {
    let deferring = Deferring(destination_factory::<MemoryDestination>());
    let target =
        Target::served(Served::new().with_destination(Box::new(deferring))).kill_seed(4242);
    let report = certify_destination(
        &target,
        json!({ "store": "certify_deferring_seeded" }),
        &MemoryProbe("certify_deferring_seeded"),
    )
    .await;
    let Some(Outcome::Failed(reason)) = report.outcome("K-DESTINATION") else {
        panic!("K-DESTINATION did not fail: {report}");
    };
    assert!(reason.contains("kill seed 4242"), "{reason}");
}
