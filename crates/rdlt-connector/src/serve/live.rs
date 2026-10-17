//! What a test's serving tasks still hold, counted, so a test sees them end.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use crate::destination::{DestinationSession, DestinationWriter, TableChange, TableRef};
use crate::error::Result;
use crate::spec::BoxFuture;
use crate::{CommitMeta, Receipt};

/// One in its count while it lives.
pub(super) struct Live(Arc<AtomicUsize>);

impl Live {
    /// One more in `count`, until it is dropped.
    pub(super) fn new(count: &Arc<AtomicUsize>) -> Self {
        count.fetch_add(1, Ordering::SeqCst);
        Self(Arc::clone(count))
    }
}

impl Drop for Live {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Resolves once nothing `count` counts lives; it sleeps between looks, so a paused clock moves
/// on and a bound around it fires.
pub(super) async fn gone(count: &AtomicUsize) {
    while count.load(Ordering::SeqCst) > 0 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// A destination session that holds nothing but its count, and whose close takes `closing`.
pub(super) struct CountedSession {
    live: Live,
    closing: Duration,
}

impl CountedSession {
    /// A session counted in `count` until it is dropped or its close ends.
    #[expect(
        clippy::new_ret_no_self,
        reason = "tests hold the session as the trait object the service holds"
    )]
    pub(super) fn new(count: &Arc<AtomicUsize>, closing: Duration) -> Box<dyn DestinationSession> {
        Box::new(Self {
            live: Live::new(count),
            closing,
        })
    }
}

impl DestinationSession for CountedSession {
    fn apply_schema<'a>(&'a mut self, _: &'a TableChange) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn writer<'a>(
        &'a mut self,
        _: &'a TableRef,
    ) -> BoxFuture<'a, Result<Box<dyn DestinationWriter>>> {
        unreachable!("no test writes")
    }

    fn commit<'a>(&'a mut self, _: &'a CommitMeta) -> BoxFuture<'a, Result<Receipt>> {
        unreachable!("no test commits")
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            tokio::time::sleep(self.closing).await;
            drop(self.live);
            Ok(())
        })
    }
}
