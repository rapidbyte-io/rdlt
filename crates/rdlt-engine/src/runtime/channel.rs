//! Byte-bounded stage channels.
//!
//! Runtime rule (plan.md constraints): channels are bounded by BYTES, not batch count —
//! peak memory stays capped regardless of row width; a batch-count bound would silently
//! scale RSS with schema size. A slow consumer exhausts the byte budget and the
//! producer parks on it: that *is* the backpressure.

// Wired into the task graph by US1 (T024); constructed only there.
#![allow(dead_code)]

use std::sync::Arc;

use tokio::sync::{Semaphore, mpsc};

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
        // rather than deadlocking.
        let permits = value
            .byte_size()
            .min(self.budget_max)
            .try_into()
            .unwrap_or(u32::MAX);
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
    // Message-count capacity is a secondary bound so zero-byte items (markers) can't
    // queue unboundedly.
    let (tx, rx) = mpsc::channel(256);
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
