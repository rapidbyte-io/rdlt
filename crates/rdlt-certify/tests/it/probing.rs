//! How certification asks a source served through the protocol where it stands.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use rdlt_certify::{Outcome, Target, certify_source};
use rdlt_connector::serve::Served;
use rdlt_connector::{
    Acknowledging, BoxFuture, ConnectContext, ConnectorError, ConnectorErrorKind, ConnectorSpec,
    Source, SourceFactory, source_factory,
};
use rdlt_connector_reference::ChangesSource;
use serde_json::json;

/// The change source's factory, counting the connections that ask where it stands, and failing
/// them where it is told to.
struct Counted {
    inner: Box<dyn SourceFactory>,
    asking: Arc<AtomicUsize>,
    failing: bool,
}

impl SourceFactory for Counted {
    fn spec(&self) -> &ConnectorSpec {
        self.inner.spec()
    }

    fn connect(
        &self,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> BoxFuture<'_, rdlt_connector::Result<Box<dyn Source>>> {
        self.inner.connect(config, context)
    }

    fn acknowledges(&self) -> bool {
        self.inner.acknowledges()
    }

    fn connect_acknowledging(
        &self,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> BoxFuture<'_, rdlt_connector::Result<Acknowledging>> {
        self.asking.fetch_add(1, Ordering::SeqCst);
        if self.failing {
            let error = ConnectorError::new(ConnectorErrorKind::Transient, "the slot is busy");
            return Box::pin(async move { Err(error) });
        }
        self.inner.connect_acknowledging(config, context)
    }
}

fn served(asking: &Arc<AtomicUsize>, failing: bool) -> Target {
    let counted = Counted {
        inner: source_factory::<ChangesSource>(),
        asking: Arc::clone(asking),
        failing,
    };
    Target::served(Served::new().with_source(Box::new(counted))).credit_watch(crate::BRIEF)
}

fn config(slot: &str) -> serde_json::Value {
    json!({
        "seed": 5,
        "streams": [{ "name": "accounts", "keys": 9, "changes": 6, "batch_rows": 2 }],
        "slot": slot,
    })
}

#[tokio::test]
async fn every_question_goes_over_one_connection() {
    let asking = Arc::new(AtomicUsize::new(0));
    let report = certify_source(&served(&asking, false), config("asked_over_one")).await;
    assert_eq!(report.outcome("S-ACK"), Some(&Outcome::Passed), "{report}");
    assert_eq!(asking.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_source_that_cannot_open_its_probe_fails_s_ack_alone() {
    let asking = Arc::new(AtomicUsize::new(0));
    let report = certify_source(&served(&asking, true), config("probe_fails")).await;
    assert!(
        matches!(report.outcome("S-ACK"), Some(Outcome::Failed(_))),
        "{report}"
    );
    assert_eq!(report.failures().count(), 1, "{report}");
}
