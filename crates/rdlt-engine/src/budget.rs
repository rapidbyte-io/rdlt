//! The memory budget: every in-flight batch holds a reservation of its bytes.

mod ledger;
#[cfg(test)]
mod tests;

use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

pub(crate) use self::ledger::Exhausted;
use self::ledger::{Asker, Class, Ledger, Request, admit_waiting};
use crate::env::Env;

/// Bytes the engine may hold in flight, shared by everything that reserves them.
///
/// A request is admitted when it fits beside the bytes already reserved, or when nothing it could
/// wait for is reserved, so a single request larger than the whole budget still makes progress:
/// it takes the whole budget, not more, since the engine works through it a piece at a time.
/// Requests are admitted in arrival order, those of work already begun first. Reservations are
/// released by dropping them.
///
/// - Bytes only a commit releases, as the cursors of sealed segments, and bytes a read keeps, as
///   its decoder's dictionaries, are kept apart: writing what is in flight cannot free them, so
///   they neither keep a request larger than the budget out nor press writers to flush.
/// - No request waits for ever: a wait ends at the budget's deadline with what held the budget,
///   and a request nobody waits for any more leaves the queue at once.
#[derive(Clone)]
pub(crate) struct MemoryBudget {
    shared: Arc<Mutex<Ledger>>,
    deadline: Option<Deadline>,
}

/// How long a request waits, and the clock that says so.
#[derive(Clone)]
struct Deadline {
    env: Arc<dyn Env>,
    wait: Duration,
}

/// A request's place among those waiting, given up when dropped.
struct Queued<'a> {
    shared: &'a Arc<Mutex<Ledger>>,
    id: u64,
}

impl Drop for Queued<'_> {
    fn drop(&mut self) {
        let mut ledger = self.shared.lock();
        ledger.forget(self.id);
        // Whoever waited behind it may fit now.
        admit_waiting(self.shared, &mut ledger);
    }
}

impl MemoryBudget {
    /// A budget of `capacity` bytes whose requests wait until they are admitted.
    pub(crate) fn new(capacity: u64) -> Self {
        Self {
            shared: Arc::new(Mutex::new(Ledger::new(capacity))),
            deadline: None,
        }
    }

    /// The budget, each of its requests waiting `wait` at most on `env`'s clock.
    pub(crate) fn within(mut self, env: Arc<dyn Env>, wait: Duration) -> Self {
        self.deadline = Some(Deadline { env, wait });
        self
    }

    /// Reserves `bytes`, waiting until earlier requests are admitted and the bytes fit.
    ///
    /// # Errors
    ///
    /// What held the budget, once the request has waited the budget's deadline.
    pub(crate) async fn acquire(&self, bytes: u64) -> Result<Reservation, Exhausted> {
        self.request(bytes, Class::Flight, false, Asker::New).await
    }

    /// Reserves `bytes` for work already begun, whose requester holds bytes in flight until the
    /// work is done.
    ///
    /// The request waits only for bytes a write releases, and goes before requests of work not
    /// begun. One row that alone takes more than a piece is `large`: such rows are reserved
    /// beyond the budget one at a time.
    ///
    /// # Errors
    ///
    /// As [`MemoryBudget::acquire`].
    pub(crate) async fn acquire_working(
        &self,
        bytes: u64,
        large: bool,
    ) -> Result<Reservation, Exhausted> {
        self.request(bytes, Class::Flight, large, Asker::Begun)
            .await
    }

    /// As [`MemoryBudget::acquire_working`], where that needs no wait; nothing otherwise.
    pub(crate) fn try_acquire_working(&self, bytes: u64, large: bool) -> Option<Reservation> {
        let mut ledger = self.shared.lock();
        let request = Request {
            bytes: bytes.min(ledger.capacity),
            class: Class::Flight,
            large,
        };
        ledger.open(request, Asker::Begun).then(|| {
            ledger.reserve(request);
            Reservation::of(&self.shared, request)
        })
    }

    /// Reserves `bytes` only a commit releases, waiting until earlier requests are admitted and
    /// the bytes fit.
    ///
    /// # Errors
    ///
    /// As [`MemoryBudget::acquire`].
    pub(crate) async fn acquire_kept(&self, bytes: u64) -> Result<Reservation, Exhausted> {
        self.request(bytes, Class::Commit, false, Asker::New).await
    }

    async fn request(
        &self,
        bytes: u64,
        class: Class,
        large: bool,
        asker: Asker,
    ) -> Result<Reservation, Exhausted> {
        let (request, queued, receiver) = {
            let mut ledger = self.shared.lock();
            let request = Request {
                bytes: bytes.min(ledger.capacity),
                class,
                large,
            };
            if ledger.open(request, asker) {
                ledger.reserve(request);
                return Ok(Reservation::of(&self.shared, request));
            }
            let (id, receiver) = ledger.wait(request, asker);
            let queued = Queued {
                shared: &self.shared,
                id,
            };
            (request, queued, receiver)
        };
        let answered = |reservation: Result<Reservation, _>| {
            reservation.expect("the ledger answers every waiter it keeps")
        };
        let Some(deadline) = &self.deadline else {
            return Ok(answered(receiver.await));
        };
        tokio::select! {
            biased;
            reservation = receiver => Ok(answered(reservation)),
            () = deadline.env.sleep(deadline.wait) => {
                let exhausted = self.shared.lock().exhausted(request.bytes, deadline.wait);
                drop(queued);
                Err(exhausted)
            }
        }
    }

    /// Charges `bytes` at once, without waiting and beyond the budget if need be: the growth of a
    /// batch already admitted, which later requests pay back by waiting.
    pub(crate) fn charge(&self, bytes: u64) -> Reservation {
        self.charged(bytes, Class::Flight)
    }

    /// Charges `bytes` no write releases at once, without waiting: what a read keeps beside its
    /// events, as its decoder's dictionaries, bounded by a limit of its own.
    pub(crate) fn keep(&self, bytes: u64) -> Reservation {
        self.charged(bytes, Class::Read)
    }

    fn charged(&self, bytes: u64, class: Class) -> Reservation {
        let request = Request {
            bytes,
            class,
            large: false,
        };
        self.shared.lock().reserve(request);
        Reservation::of(&self.shared, request)
    }

    /// Completes once a request is waiting for bytes in flight or charges exceed the budget:
    /// whoever holds bytes it could release early should.
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
    fn of(shared: &Arc<Mutex<Ledger>>, request: Request) -> Self {
        Self {
            budget: Some(Arc::clone(shared)),
            request,
        }
    }

    /// Lets go of the ledger without releasing: for a reservation whose bytes its holder
    /// releases itself.
    fn forget(mut self) {
        self.budget = None;
    }

    /// The bytes held.
    pub(crate) fn bytes(&self) -> u64 {
        self.request.bytes
    }

    /// Says the bytes are queued for a write, which releases them whatever else waits: work
    /// begun waits for them from now, where it waits for no bytes work holds.
    pub(crate) fn stage(&mut self) {
        let Some(shared) = &self.budget else {
            return;
        };
        if self.request.class != Class::Flight {
            return;
        }
        let mut ledger = shared.lock();
        ledger.release(self.request);
        self.request.class = Class::Staged;
        ledger.reserve(self.request);
        ledger.press();
    }

    /// Holds `bytes` from now, at once: fewer release the rest to whoever waits, and more are
    /// charged beyond the budget if need be.
    pub(crate) fn resize(&mut self, bytes: u64) {
        let Some(shared) = &self.budget else {
            return;
        };
        let mut ledger = shared.lock();
        ledger.release(self.request);
        self.request.bytes = bytes;
        ledger.reserve(self.request);
        admit_waiting(shared, &mut ledger);
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
