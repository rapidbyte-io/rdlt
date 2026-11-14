use std::num::NonZeroUsize;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use rdlt_connector::{
    BoxFuture, Capabilities, CommitMeta, ConnectContext, ConnectorError, Destination,
    DestinationSession, DestinationWriter, OpenContext, OpenedSession, Receipt, Result, Source,
    TableChange, TableRef, destination_factory,
};
use rdlt_connector_reference::{MemoryDestination, published};
use rdlt_engine::WriteMode;
use rdlt_host::Kills;
use tokio::sync::watch;

use super::{every_row_once, generator, planned};
use crate::kill::converged;
use crate::kill::killing::{Killing, Schedule};
use crate::kill::paced::Pace;

/// Points that come late in their ranges: the first write once the third commit has started,
/// before the fifth, and after the fourth, losing its answer.
const LATE: Schedule = Schedule {
    settled: 3,
    commit: 5,
    answer: Some(4),
};

/// The longest a commit waits for the load's source to stall: far more than it needs, and short
/// enough that a wait for each of the loads a clause runs ends within [`BOUND`].
const STALL: Duration = Duration::from_secs(20);

/// The longest the load takes: far more than it needs.
const BOUND: Duration = Duration::from_secs(120);

/// A destination each commit of which waits until the load's source can send nothing more,
/// which the pace it is given tells: the slowest a destination's commits can be.
struct Stalling {
    destination: Arc<dyn Destination>,
    pace: Arc<OnceLock<watch::Receiver<Pace>>>,
}

struct StallingSession {
    session: Box<dyn DestinationSession>,
    pace: watch::Receiver<Pace>,
}

impl Destination for Stalling {
    fn capabilities(&self) -> &Capabilities {
        self.destination.capabilities()
    }

    fn check(&self) -> BoxFuture<'_, Result<()>> {
        self.destination.check()
    }

    fn open<'a>(&'a self, context: &'a OpenContext) -> BoxFuture<'a, Result<OpenedSession>> {
        Box::pin(async move {
            let opened = self.destination.open(context).await?;
            let pace = self.pace.get().expect("the pace is set before the load");
            Ok(OpenedSession {
                session: Box::new(StallingSession {
                    session: opened.session,
                    pace: pace.clone(),
                }),
                ..opened
            })
        })
    }

    fn max_calls(&self) -> Option<NonZeroUsize> {
        self.destination.max_calls()
    }
}

impl DestinationSession for StallingSession {
    fn apply_schema<'a>(&'a mut self, change: &'a TableChange) -> BoxFuture<'a, Result<()>> {
        self.session.apply_schema(change)
    }

    fn writer<'a>(
        &'a mut self,
        table: &'a TableRef,
    ) -> BoxFuture<'a, Result<Box<dyn DestinationWriter>>> {
        self.session.writer(table)
    }

    fn commit<'a>(&'a mut self, meta: &'a CommitMeta) -> BoxFuture<'a, Result<Receipt>> {
        Box::pin(async move {
            let waiting = tokio::time::timeout(STALL, self.pace.wait_for(Pace::stalled));
            if !waiting.await.is_ok_and(|stalled| stalled.is_ok()) {
                return Err(ConnectorError::internal("the load's source never stalled"));
            }
            self.session.commit(meta).await
        })
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
        self.session.close()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_destination_committing_once_its_source_stalls_is_killed_at_every_point() {
    let name = "certify_stalled_k";
    let memory = destination_factory::<MemoryDestination>()
        .connect(serde_json::json!({ "store": name }), ConnectContext::new())
        .await
        .expect("the memory destination connects");
    let pace = Arc::new(OnceLock::new());
    let stalling = Stalling {
        destination: Arc::from(memory),
        pace: Arc::clone(&pace),
    };
    let kills = Kills::new();
    let killing = Arc::new(Killing::new(Arc::new(stalling), &kills, LATE));
    pace.set(killing.pace()).expect("the pace is set once");
    let generated = generator(name, 1).await.expect("the generator connects");
    let source: Arc<dyn Source> = killing.paced(generated);
    let destination: Arc<dyn Destination> = Arc::clone(&killing) as Arc<dyn Destination>;
    let plan = planned(name, WriteMode::Append).expect("a valid plan");
    let interrupted = tokio::time::timeout(BOUND, converged(&plan, &source, &destination))
        .await
        .expect("the load ends")
        .expect("the load converges");
    // In process a kill cuts nothing, so each point counts once it is reached.
    assert_eq!(kills.count(), 3, "every point is reached");
    assert!(interrupted, "the lost answer fails an attempt");
    let tables = killing.tables();
    let [table] = tables.as_slice() else {
        panic!("the load wrote {tables:?}");
    };
    every_row_once(&published(name, &table.name)).expect("each row is published once");
}
