//! A destination counting the rows its commits land, and a wait for a count of them.

#[cfg(test)]
mod tests;

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use rdlt_connector::{
    BoxFuture, Capabilities, CommitMeta, Destination, DestinationSession, DestinationWriter,
    OpenContext, OpenedSession, Receipt, Result, TableChange, TableRef,
};

use crate::env::{Env, InlineEnv};

/// The wait between looks at the rows landed.
const LOOKING: Duration = Duration::from_millis(10);

/// A destination adding the rows each of its commits lands to a count.
pub(super) struct Landing {
    destination: Arc<dyn Destination>,
    landed: Arc<AtomicU64>,
}

impl Landing {
    /// `destination`, adding the rows each commit of it lands to `landed`.
    pub(super) fn new(destination: Arc<dyn Destination>, landed: Arc<AtomicU64>) -> Self {
        Self {
            destination,
            landed,
        }
    }
}

impl Destination for Landing {
    fn capabilities(&self) -> &Capabilities {
        self.destination.capabilities()
    }

    fn check(&self) -> BoxFuture<'_, Result<()>> {
        self.destination.check()
    }

    fn open<'a>(&'a self, context: &'a OpenContext) -> BoxFuture<'a, Result<OpenedSession>> {
        Box::pin(async move {
            let opened = self.destination.open(context).await?;
            let session = LandingSession {
                session: opened.session,
                landed: Arc::clone(&self.landed),
            };
            Ok(OpenedSession {
                session: Box::new(session),
                ..opened
            })
        })
    }

    fn max_calls(&self) -> Option<NonZeroUsize> {
        self.destination.max_calls()
    }
}

/// A session of a [`Landing`] destination.
struct LandingSession {
    session: Box<dyn DestinationSession>,
    landed: Arc<AtomicU64>,
}

impl DestinationSession for LandingSession {
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
            let receipt = self.session.commit(meta).await?;
            self.landed.fetch_add(receipt.rows, Ordering::Relaxed);
            Ok(receipt)
        })
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
        self.session.close()
    }
}

/// Waits on the bench's clock until `landed` counts `rows`, or for `limit` at most.
pub(super) async fn until_landed(landed: &AtomicU64, rows: u64, limit: Duration) {
    let clock = InlineEnv;
    let landing = async {
        while landed.load(Ordering::Relaxed) < rows {
            clock.sleep(LOOKING).await;
        }
    };
    tokio::select! {
        biased;
        // Every row landed wins: the wait ends at once.
        () = landing => {}
        () = clock.sleep(limit) => {}
    }
}
