//! What a test's serving tasks still hold, counted, so a test sees them end.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

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
