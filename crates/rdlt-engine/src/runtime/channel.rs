//! Byte-bounded stage channels.
//!
//! Runtime rule (plan.md constraints): channels are bounded by BYTES, not batch count —
//! peak memory stays capped regardless of row width; a batch-count bound would silently
//! scale RSS with schema size. A slow consumer exhausts the byte budget and the
//! producer parks on it: that *is* the backpressure.

use std::sync::Arc;

use tokio::sync::{Semaphore, mpsc};

/// Secondary message-count bound on a stage channel. The byte budget is the primary
/// backpressure; this hard cap keeps zero-byte items (markers) from queueing without
/// limit when the budget alone would never park them.
const STAGE_MSG_CAPACITY: usize = 256;

/// Items know their in-memory footprint.
pub(crate) trait ByteSized {
    fn byte_size(&self) -> usize;
}

#[derive(Debug)]
pub(crate) struct ByteTx<T> {
    tx: mpsc::Sender<Item<T>>,
    budget: Arc<Semaphore>,
    budget_max: usize,
}

// Manual impl: `T` need not be Clone for the sender to be.
impl<T> Clone for ByteTx<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            budget: Arc::clone(&self.budget),
            budget_max: self.budget_max,
        }
    }
}

#[derive(Debug)]
pub(crate) struct ByteRx<T> {
    rx: mpsc::Receiver<Item<T>>,
}

#[derive(Debug)]
struct Item<T> {
    value: T,
    _permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

/// The channel closed (consumer dropped or shutdown began).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("stage channel closed")]
pub(crate) struct StageClosed;

impl<T: ByteSized> ByteTx<T> {
    pub(crate) async fn send(&self, value: T) -> Result<(), StageClosed> {
        // An item larger than the whole budget degrades to "budget fully drained"
        // rather than deadlocking. The `.min` caps the request at the budget, so
        // the `u32` conversion can only overflow when the budget ITSELF exceeds
        // `u32::MAX` (semaphore permits are `u32`); there it saturates to
        // `u32::MAX` permits — still a valid "drain the budget" request.
        let permits = value
            .byte_size()
            .min(self.budget_max)
            .try_into()
            .unwrap_or(u32::MAX);
        // Mutation note: `> 0` versus `>= 0` is EQUIVALENT here for the same
        // reason as the SPI channel — acquiring zero permits always succeeds.
        // Kept as the cheaper path and as a statement that zero-byte markers
        // are not budgeted.
        let permit = if permits > 0 {
            Some(
                Arc::clone(&self.budget)
                    .acquire_many_owned(permits)
                    .await
                    .map_err(|_| StageClosed)?,
            )
        } else {
            None
        };
        self.tx
            .send(Item {
                value,
                _permit: permit,
            })
            .await
            .map_err(|_| StageClosed)
    }
}

impl<T> ByteRx<T> {
    /// `None` once all senders are dropped and the channel is drained.
    pub(crate) async fn recv(&mut self) -> Option<T> {
        self.rx.recv().await.map(|item| item.value)
    }
}

pub(crate) fn byte_channel<T>(byte_budget: usize) -> (ByteTx<T>, ByteRx<T>) {
    let budget = Arc::new(Semaphore::new(byte_budget));
    let (tx, rx) = mpsc::channel(STAGE_MSG_CAPACITY);
    (
        ByteTx {
            tx,
            budget,
            budget_max: byte_budget,
        },
        ByteRx { rx },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Blob(usize);
    impl ByteSized for Blob {
        fn byte_size(&self) -> usize {
            self.0
        }
    }

    #[tokio::test]
    async fn capacity_is_bytes_not_items() {
        let (tx, mut rx) = byte_channel::<Blob>(100);
        // Many small items fit even though they exceed any small item count…
        for _ in 0..50 {
            tx.send(Blob(2)).await.unwrap();
        }
        for _ in 0..50 {
            rx.recv().await.unwrap();
        }

        // …but a second large item blocks until the first is received. (The budget
        // covers *queued* bytes; once a stage receives an item, accounting for the
        // derived data belongs to that stage's own downstream channel.)
        tx.send(Blob(80)).await.unwrap();
        let pending = tx.send(Blob(80));
        tokio::pin!(pending);
        tokio::select! {
            _ = &mut pending => panic!("byte budget not enforced"),
            _ = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let _first = rx.recv().await.unwrap();
        pending.await.unwrap();
    }

    #[tokio::test]
    async fn oversized_item_still_passes() {
        let (tx, mut rx) = byte_channel::<Blob>(16);
        tx.send(Blob(1_000_000)).await.unwrap();
        assert_eq!(rx.recv().await.unwrap().0, 1_000_000);
    }
}

#[cfg(test)]
mod budget_tests {
    // Mutation-report closure: the byte-budget boundary — a send of exactly
    // the budget must pass; holding it, the NEXT send must wait.
    use super::*;

    #[derive(Debug)]
    struct Sized(usize);
    impl ByteSized for Sized {
        fn byte_size(&self) -> usize {
            self.0
        }
    }

    #[tokio::test]
    async fn exact_budget_passes_and_next_send_waits() {
        let (tx, mut rx) = byte_channel::<Sized>(100);
        tx.send(Sized(100)).await.expect("exactly the budget");
        let pending =
            tokio::time::timeout(std::time::Duration::from_millis(50), tx.send(Sized(1))).await;
        assert!(
            pending.is_err(),
            "budget exhausted: the next send must wait"
        );
        // Receiving (and letting the item fall out of scope) releases its permit.
        let _item = rx.recv().await.expect("item");
        tx.send(Sized(1)).await.expect("freed budget");
    }
}
