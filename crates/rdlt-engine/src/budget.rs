//! The memory budget: every in-flight batch holds a reservation of its bytes.

#[cfg(test)]
mod tests;

use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::oneshot;

use crate::watch;

/// Bytes the engine may hold in flight, shared by everything that reserves them.
///
/// A request is admitted when it fits beside the bytes already reserved, or when nothing is
/// reserved at all, so a single request larger than the whole budget still makes progress: it
/// takes the whole budget, not more, since the engine works through it a slice at a time.
/// Requests are admitted in arrival order. Acquiring is the only operation that waits: growth of a
/// batch already admitted is charged at once, beyond the budget if need be, and later requests
/// wait until it is released. Reservations are released by dropping them, so every wait ends once
/// earlier reservations drop.
///
/// Bytes only a commit releases, as the cursors of sealed segments, are kept apart: writing what
/// is in flight cannot free them, so they neither keep a request larger than the budget out nor
/// press writers to flush. They wait for room like any request, so they never exceed the budget.
#[derive(Clone)]
pub(crate) struct MemoryBudget {
    shared: Arc<Mutex<Ledger>>,
}

struct Ledger {
    capacity: u64,
    reserved: u64,
    /// Of the reserved bytes, those only a commit releases.
    kept: u64,
    peak: u64,
    waiting: VecDeque<(Request, oneshot::Sender<Reservation>)>,
    /// Whether any request is waiting.
    pressed: watch::Sender<bool>,
}

/// Bytes asked of the budget, and whether only a commit releases them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Request {
    bytes: u64,
    kept: bool,
}

impl Ledger {
    /// The reserved bytes that writing what is in flight releases.
    fn in_flight(&self) -> u64 {
        self.reserved.saturating_sub(self.kept)
    }

    /// Whether `request` fits beside what is reserved, or nothing it could wait for is: bytes in
    /// flight for an ordinary request, any bytes for one a commit releases.
    fn admits(&self, request: Request) -> bool {
        let nothing = if request.kept {
            self.reserved == 0
        } else {
            self.in_flight() == 0
        };
        nothing || self.reserved.saturating_add(request.bytes) <= self.capacity
    }

    fn reserve(&mut self, request: Request) {
        self.reserved = self.reserved.saturating_add(request.bytes);
        if request.kept {
            self.kept = self.kept.saturating_add(request.bytes);
        }
        self.peak = self.peak.max(self.reserved);
        self.press();
    }

    fn release(&mut self, request: Request) {
        self.reserved = self.reserved.saturating_sub(request.bytes);
        if request.kept {
            self.kept = self.kept.saturating_sub(request.bytes);
        }
    }

    /// Signals pressure while a request waits or the bytes in flight exceed the budget: either
    /// way, whoever holds bytes it could release early should.
    fn press(&self) {
        let pressed = !self.waiting.is_empty() || self.in_flight() > self.capacity;
        self.pressed.send_replace(pressed);
    }
}

impl MemoryBudget {
    /// A budget of `capacity` bytes.
    pub(crate) fn new(capacity: u64) -> Self {
        Self {
            shared: Arc::new(Mutex::new(Ledger {
                capacity,
                reserved: 0,
                kept: 0,
                peak: 0,
                waiting: VecDeque::new(),
                pressed: watch::Sender::new(false),
            })),
        }
    }

    /// Reserves `bytes`, waiting until earlier requests are admitted and the bytes fit.
    pub(crate) async fn acquire(&self, bytes: u64) -> Reservation {
        self.request(bytes, false).await
    }

    /// Reserves `bytes` only a commit releases, waiting until earlier requests are admitted and
    /// the bytes fit.
    pub(crate) async fn acquire_kept(&self, bytes: u64) -> Reservation {
        self.request(bytes, true).await
    }

    async fn request(&self, bytes: u64, kept: bool) -> Reservation {
        let receiver = {
            let mut ledger = self.shared.lock();
            let request = Request {
                bytes: bytes.min(ledger.capacity),
                kept,
            };
            if ledger.waiting.is_empty() && ledger.admits(request) {
                ledger.reserve(request);
                return self.reservation(request);
            }
            let (sender, receiver) = oneshot::channel();
            ledger.waiting.push_back((request, sender));
            ledger.press();
            receiver
        };
        receiver
            .await
            .expect("the ledger answers every waiter it keeps")
    }

    /// Charges `bytes` at once, without waiting and beyond the budget if need be: the growth of a
    /// batch already admitted, which later requests pay back by waiting.
    pub(crate) fn charge(&self, bytes: u64) -> Reservation {
        let request = Request { bytes, kept: false };
        self.shared.lock().reserve(request);
        self.reservation(request)
    }

    /// Charges `bytes` no write releases at once, without waiting: what a read keeps beside its
    /// events, as its decoder's dictionaries, bounded by a limit of its own.
    pub(crate) fn keep(&self, bytes: u64) -> Reservation {
        let request = Request { bytes, kept: true };
        self.shared.lock().reserve(request);
        self.reservation(request)
    }

    /// Completes once a request is waiting for bytes or charges exceed the budget: whoever holds
    /// bytes it could release early should.
    pub(crate) fn pressed(&self) -> impl Future<Output = ()> + Send + 'static {
        let mut pressed = self.shared.lock().pressed.subscribe();
        async move {
            // The ledger keeps the sender as long as the budget lives.
            if pressed.wait_for(|pressed| *pressed).await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }

    /// The bytes the budget holds.
    pub(crate) fn capacity(&self) -> u64 {
        self.shared.lock().capacity
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

    fn reservation(&self, request: Request) -> Reservation {
        Reservation {
            budget: Some(Arc::clone(&self.shared)),
            request,
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

/// Reserved bytes, released when dropped.
#[must_use = "dropping a reservation releases its bytes"]
pub(crate) struct Reservation {
    budget: Option<Arc<Mutex<Ledger>>>,
    request: Request,
}

impl Reservation {
    /// The bytes held.
    #[cfg(test)]
    pub(crate) fn bytes(&self) -> u64 {
        self.request.bytes
    }
}

impl fmt::Debug for Reservation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Reservation")
            .field(&self.request.bytes)
            .finish()
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        let Some(shared) = self.budget.take() else {
            return;
        };
        let mut ledger = shared.lock();
        ledger.release(self.request);
        admit_waiting(&shared, &mut ledger);
    }
}

/// Admits waiting requests in order while the front one fits.
fn admit_waiting(shared: &Arc<Mutex<Ledger>>, ledger: &mut Ledger) {
    while let Some(&(request, _)) = ledger.waiting.front() {
        if !ledger.admits(request) {
            break;
        }
        let (request, sender) = ledger
            .waiting
            .pop_front()
            .expect("the front request was just read");
        let peak = ledger.peak;
        ledger.reserve(request);
        let reservation = Reservation {
            budget: Some(Arc::clone(shared)),
            request,
        };
        if let Err(mut abandoned) = sender.send(reservation) {
            // The waiter stopped waiting; release its bytes here, under the lock already held.
            // Only bytes a waiter receives count toward the peak.
            abandoned.budget = None;
            ledger.release(request);
            ledger.peak = peak;
        }
    }
    ledger.press();
}
