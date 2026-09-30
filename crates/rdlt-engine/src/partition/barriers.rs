//! The barriers a commit coordinator raises, as one partition's read sees them.

use rdlt_connector::PartitionFeed;

use crate::watch;

/// The barriers the coordinator raises, forwarded to an on-demand partition's read.
pub(super) struct Barriers {
    receiver: watch::Receiver<u64>,
    /// Whether barriers may still come: the partition checkpoints on demand and the
    /// coordinator has not gone.
    pub(super) open: bool,
}

impl Barriers {
    /// Barriers for a partition, forwarding one already raised to `feed` at once.
    pub(super) fn new(
        mut receiver: watch::Receiver<u64>,
        on_demand: bool,
        feed: &PartitionFeed,
    ) -> Self {
        if on_demand {
            let raised = receiver.borrow_and_update();
            if raised > 0 {
                feed.request_checkpoint(raised);
            }
        }
        Self {
            receiver,
            open: on_demand,
        }
    }

    /// The next barrier raised; `None` once the coordinator has gone.
    pub(super) async fn next(&mut self) -> Option<u64> {
        if self.receiver.changed().await.is_ok() {
            Some(self.receiver.borrow_and_update())
        } else {
            self.open = false;
            None
        }
    }
}
