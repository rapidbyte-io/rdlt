//! The memory budget: every in-flight batch holds a reservation of its bytes.

#[cfg(test)]
mod tests;

use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;

use parking_lot::Mutex;
use rdlt_connector::{Admission, BoxFuture, Permit};
use tokio::sync::oneshot;

/// Bytes the engine may hold in flight, shared by everything that reserves them.
///
/// A request is admitted when it fits beside the bytes already reserved, or when nothing is
/// reserved at all, so a single request larger than the whole budget still makes progress.
/// Requests are admitted in arrival order. Acquiring is the only operation that waits, and
/// reservations are released by dropping them, so every wait ends once earlier reservations drop.
#[derive(Clone)]
pub(crate) struct MemoryBudget {
    shared: Arc<Mutex<Ledger>>,
}

struct Ledger {
    capacity: u64,
    reserved: u64,
    peak: u64,
    waiting: VecDeque<(u64, oneshot::Sender<Reservation>)>,
}

impl Ledger {
    fn admits(&self, bytes: u64) -> bool {
        self.reserved == 0 || self.reserved.saturating_add(bytes) <= self.capacity
    }

    fn reserve(&mut self, bytes: u64) {
        self.reserved = self.reserved.saturating_add(bytes);
        self.peak = self.peak.max(self.reserved);
    }
}

impl MemoryBudget {
    /// A budget of `capacity` bytes.
    pub(crate) fn new(capacity: u64) -> Self {
        Self {
            shared: Arc::new(Mutex::new(Ledger {
                capacity,
                reserved: 0,
                peak: 0,
                waiting: VecDeque::new(),
            })),
        }
    }

    /// Reserves `bytes`, waiting until earlier requests are admitted and the bytes fit.
    pub(crate) async fn acquire(&self, bytes: u64) -> Reservation {
        let receiver = {
            let mut ledger = self.shared.lock();
            if ledger.waiting.is_empty() && ledger.admits(bytes) {
                ledger.reserve(bytes);
                return self.reservation(bytes);
            }
            let (sender, receiver) = oneshot::channel();
            ledger.waiting.push_back((bytes, sender));
            receiver
        };
        receiver
            .await
            .expect("the ledger answers every waiter it keeps")
    }

    /// Bytes reserved now.
    #[cfg(test)]
    pub(crate) fn reserved(&self) -> u64 {
        self.shared.lock().reserved
    }

    /// The most bytes ever reserved at once.
    pub(crate) fn peak(&self) -> u64 {
        self.shared.lock().peak
    }

    fn reservation(&self, bytes: u64) -> Reservation {
        Reservation {
            budget: Some(Arc::clone(&self.shared)),
            bytes,
        }
    }
}

impl fmt::Debug for MemoryBudget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ledger = self.shared.lock();
        f.debug_struct("MemoryBudget")
            .field("capacity", &ledger.capacity)
            .field("reserved", &ledger.reserved)
            .finish_non_exhaustive()
    }
}

impl Admission for MemoryBudget {
    fn admit(&self, bytes: u64) -> BoxFuture<'_, Permit> {
        Box::pin(async move { Box::new(self.acquire(bytes).await) as Permit })
    }
}

/// Reserved bytes, released when dropped.
#[must_use = "dropping a reservation releases its bytes"]
pub(crate) struct Reservation {
    budget: Option<Arc<Mutex<Ledger>>>,
    bytes: u64,
}

impl Reservation {
    /// The bytes held.
    #[cfg(test)]
    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }
}

impl fmt::Debug for Reservation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Reservation").field(&self.bytes).finish()
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        let Some(shared) = self.budget.take() else {
            return;
        };
        let mut ledger = shared.lock();
        ledger.reserved = ledger.reserved.saturating_sub(self.bytes);
        admit_waiting(&shared, &mut ledger);
    }
}

/// Admits waiting requests in order while the front one fits.
fn admit_waiting(shared: &Arc<Mutex<Ledger>>, ledger: &mut Ledger) {
    while let Some(&(bytes, _)) = ledger.waiting.front() {
        if !ledger.admits(bytes) {
            break;
        }
        let (bytes, sender) = ledger
            .waiting
            .pop_front()
            .expect("the front request was just read");
        ledger.reserve(bytes);
        let reservation = Reservation {
            budget: Some(Arc::clone(shared)),
            bytes,
        };
        if let Err(mut abandoned) = sender.send(reservation) {
            // The waiter stopped waiting; release its bytes here, under the lock already held.
            abandoned.budget = None;
            ledger.reserved = ledger.reserved.saturating_sub(bytes);
        }
    }
}
