//! The memory budget: everything the engine holds of what connectors send has a reservation of
//! its bytes, and the reservations never pass the budget.

mod admits;
mod decoding;
mod ledger;
#[cfg(test)]
mod tests;

use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

pub(crate) use self::admits::{admitted, kept, least};
pub(crate) use self::decoding::Decoding;
pub(crate) use self::ledger::{Class, Denied, Exhausted, Shares, TooLarge};
use self::ledger::{Ledger, admit_waiting};
use crate::env::Env;

/// Bytes the engine may hold, divided into shares, each its holders' alone.
///
/// - **Never exceeded.** A request is reserved only when it fits its share. One that asks for
///   more than a request of its share may take is refused, since no wait could admit it; none
///   is cut down to fit, and nothing is reserved without asking.
/// - **Shares.** The cursors of seals waiting for a commit, the log's frames and what reads keep
///   each have a share no push can use, so a checkpoint never waits behind data. The rest holds
///   pushes waiting to be lowered and what lowering makes of them; pushes never take all of it,
///   so a request for lowering fits once the pieces before it are written.
/// - **No hold and wait.** Whoever holds what lowering reserved, a piece on the compute pool, on
///   its lane or in the log's writer, needs no budget to release it. A partition waits for
///   lowering while it holds pushes, and nothing waits for pushes while it holds anything.
/// - **No wait is for ever.** A wait ends at the budget's deadline with what held the budget, and
///   a request nobody waits for any more leaves the queue at once.
///
/// Requests of one share are admitted in arrival order. Reservations are released by dropping
/// them.
#[derive(Clone)]
pub(crate) struct MemoryBudget {
    shared: Arc<Mutex<Ledger>>,
    deadline: Option<Deadline>,
    /// How many reads share what reads may keep.
    readers: usize,
    /// The limits on what a read sends, where they are fewer than what the budget admits.
    limits: Option<rdlt_wire::Limits>,
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
            readers: 1,
            limits: None,
        }
    }

    /// The budget, each of its requests waiting `wait` at most on `env`'s clock.
    pub(crate) fn within(mut self, env: Arc<dyn Env>, wait: Duration) -> Self {
        self.deadline = Some(Deadline { env, wait });
        self
    }

    /// The budget, what its reads may keep divided among `readers` reads at once at most.
    pub(crate) fn read_by(mut self, readers: usize) -> Self {
        self.readers = readers.max(1);
        self
    }

    /// How many reads share what reads may keep.
    pub(crate) fn readers(&self) -> usize {
        self.readers
    }

    /// The budget, its reads held to `limits` on what they send.
    pub(crate) fn limited(mut self, limits: rdlt_wire::Limits) -> Self {
        self.limits = Some(limits);
        self
    }

    /// The limits on what a read sends: those set, or what the budget's shares admit.
    pub(crate) fn limits(&self) -> rdlt_wire::Limits {
        let admitted = admitted(self.shares(), self.readers);
        self.limits
            .map_or(admitted, |limits| limits.lesser(&admitted))
    }

    /// Reserves `bytes` a push keeps alive, waiting until earlier pushes are admitted and the
    /// bytes fit what pushes may take.
    ///
    /// # Errors
    ///
    /// [`Denied::TooLarge`] for more than pushes may ever take, and [`Denied::Exhausted`] with
    /// what held the budget once the request has waited the budget's deadline.
    pub(crate) async fn acquire(&self, bytes: u64) -> Result<Reservation, Denied> {
        self.request(Class::Intake, bytes).await
    }

    /// Reserves `bytes` for lowering, all its next step holds, in one request: it waits only for
    /// what other lowerings hold, which their writes release.
    ///
    /// # Errors
    ///
    /// As [`MemoryBudget::acquire`], for more than one request for lowering may take.
    pub(crate) async fn acquire_working(&self, bytes: u64) -> Result<Reservation, Denied> {
        self.request(Class::Work, bytes).await
    }

    /// As [`MemoryBudget::acquire_working`], where that needs no wait; nothing otherwise.
    pub(crate) fn try_acquire_working(&self, bytes: u64) -> Option<Reservation> {
        let mut ledger = self.shared.lock();
        let admitted =
            ledger.too_large(Class::Work, bytes).is_none() && ledger.open(Class::Work, bytes);
        admitted.then(|| {
            ledger.reserve(Class::Work, bytes);
            Reservation::of(&self.shared, Class::Work, bytes)
        })
    }

    /// Reserves the `bytes` of a cursor only a commit releases, from the cursors' own share.
    ///
    /// # Errors
    ///
    /// As [`MemoryBudget::acquire`], for a cursor larger than the share.
    pub(crate) async fn acquire_cursor(&self, bytes: u64) -> Result<Reservation, Denied> {
        self.request(Class::Cursor, bytes).await
    }

    /// Reserves the `bytes` of a frame on its way into the log, from the log's own share.
    ///
    /// # Errors
    ///
    /// As [`MemoryBudget::acquire`], for a frame larger than the share.
    pub(crate) async fn acquire_log(&self, bytes: u64) -> Result<Reservation, Denied> {
        self.request(Class::Log, bytes).await
    }

    /// As [`MemoryBudget::acquire_log`], where that needs no wait; nothing otherwise.
    pub(crate) fn try_acquire_log(&self, bytes: u64) -> Option<Reservation> {
        let mut ledger = self.shared.lock();
        let admitted =
            ledger.too_large(Class::Log, bytes).is_none() && ledger.open(Class::Log, bytes);
        admitted.then(|| {
            ledger.reserve(Class::Log, bytes);
            Reservation::of(&self.shared, Class::Log, bytes)
        })
    }

    /// Reserves the `bytes` a commit will record of a table, from the tables' share: only a
    /// commit releases them.
    ///
    /// # Errors
    ///
    /// As [`MemoryBudget::acquire`], for records larger than the share.
    pub(crate) async fn acquire_tables(&self, bytes: u64) -> Result<Reservation, Denied> {
        self.request(Class::Tables, bytes).await
    }

    /// Reserves the `bytes` decoding a connector's answer holds, from the share of answers being
    /// decoded, until it is decoded.
    ///
    /// # Errors
    ///
    /// As [`MemoryBudget::acquire`], for an answer that holds more than the share.
    pub(crate) async fn acquire_control(&self, bytes: u64) -> Result<Reservation, Denied> {
        self.request(Class::Control, bytes).await
    }

    /// Reserves `bytes` a read keeps beside its events, at once.
    ///
    /// # Errors
    ///
    /// A [`TooLarge`] where the reads' share has no room for them: a read keeps no more than its
    /// part of the share, so the share is never passed and no read waits.
    pub(crate) fn keep(&self, bytes: u64) -> Result<Reservation, TooLarge> {
        let mut ledger = self.shared.lock();
        if !ledger.open(Class::Read, bytes) {
            return Err(TooLarge {
                what: "what reads keep",
                asked: bytes,
                limit: ledger.shares.reads,
            });
        }
        ledger.reserve(Class::Read, bytes);
        Ok(Reservation::of(&self.shared, Class::Read, bytes))
    }

    async fn request(&self, class: Class, bytes: u64) -> Result<Reservation, Denied> {
        let (queued, receiver) = {
            let mut ledger = self.shared.lock();
            if let Some(refused) = ledger.too_large(class, bytes) {
                return Err(refused.into());
            }
            if ledger.open(class, bytes) {
                ledger.reserve(class, bytes);
                return Ok(Reservation::of(&self.shared, class, bytes));
            }
            let Some((id, receiver)) = ledger.wait(class, bytes) else {
                return Err(ledger.exhausted(class, bytes, Duration::ZERO).into());
            };
            let queued = Queued {
                shared: &self.shared,
                id,
            };
            (queued, receiver)
        };
        let answered = |reservation: Result<Reservation, _>| {
            reservation.expect("the ledger answers every waiter it keeps")
        };
        let Some(deadline) = &self.deadline else {
            return Ok(answered(receiver.await));
        };
        let began = deadline.env.instant();
        tokio::select! {
            biased;
            reservation = receiver => Ok(answered(reservation)),
            () = deadline.env.sleep(deadline.wait) => {
                let waited = deadline.env.instant().saturating_duration_since(began);
                let exhausted = self.shared.lock().exhausted(class, bytes, waited);
                drop(queued);
                Err(exhausted.into())
            }
        }
    }

    /// Waits for `slot`, one of the places among which what reads keep is divided, for no longer
    /// than a request waits for bytes.
    ///
    /// # Errors
    ///
    /// [`Exhausted`] for what reads keep, with the part a slot holds, once the wait reaches the
    /// budget's deadline.
    pub(crate) async fn read_slot<T>(&self, slot: impl Future<Output = T>) -> Result<T, Exhausted> {
        let Some(deadline) = &self.deadline else {
            return Ok(slot.await);
        };
        let began = deadline.env.instant();
        tokio::select! {
            biased;
            taken = slot => Ok(taken),
            () = deadline.env.sleep(deadline.wait) => {
                let waited = deadline.env.instant().saturating_duration_since(began);
                let readers = u64::try_from(self.readers).unwrap_or(u64::MAX);
                let part = self.shares().reads / readers.max(1);
                Err(self.shared.lock().exhausted(Class::Read, part, waited))
            }
        }
    }

    /// Completes once a request waits for bytes of pushes or of lowering, or a holder of queued
    /// pieces waits for their writes: whoever holds bytes a write releases should write them.
    pub(crate) fn pressed(&self) -> impl Future<Output = ()> + Send + 'static {
        let mut pressed = self.shared.lock().pressed.subscribe();
        async move {
            // The ledger keeps the sender as long as the budget lives.
            if pressed.wait_for(|pressed| *pressed).await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }

    /// Completes once a cursor waits for room among the cursors of seals waiting for a commit:
    /// a commit is then due, whatever its policy says.
    pub(crate) fn cursor_waits(&self) -> impl Future<Output = ()> + Send + 'static {
        let mut waits = self.shared.lock().cursor_waits.subscribe();
        async move {
            if waits.wait_for(|waits| *waits).await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }

    /// Presses whoever holds queued pieces to write them, for as long as what is returned lives:
    /// for a holder of an allowance waiting for its own pieces to be written.
    pub(crate) fn pressing(&self) -> Pressing {
        self.shared.lock().pressing(true);
        Pressing(Arc::clone(&self.shared))
    }

    /// The bytes the budget holds.
    #[cfg(test)]
    pub(crate) fn capacity(&self) -> u64 {
        self.shared.lock().capacity
    }

    /// What each share of the budget holds at most.
    pub(crate) fn shares(&self) -> Shares {
        self.shared.lock().shares
    }

    /// Bytes reserved now.
    #[cfg(test)]
    pub(crate) fn reserved(&self) -> u64 {
        self.shared.lock().reserved()
    }

    /// How many requests for pushes or lowering have waited for room, and how many cursors.
    pub(crate) fn waits(&self) -> (u64, u64) {
        self.shared.lock().waited
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
            .field("reserved", &ledger.reserved())
            .finish_non_exhaustive()
    }
}

/// Pressure on the holders of queued pieces, lifted when dropped.
pub(crate) struct Pressing(Arc<Mutex<Ledger>>);

impl Drop for Pressing {
    fn drop(&mut self) {
        self.0.lock().pressing(false);
    }
}

/// Reserved bytes, released when dropped.
#[must_use = "dropping a reservation releases its bytes"]
pub(crate) struct Reservation {
    budget: Option<Arc<Mutex<Ledger>>>,
    class: Class,
    bytes: u64,
}

impl Reservation {
    fn of(shared: &Arc<Mutex<Ledger>>, class: Class, bytes: u64) -> Self {
        Self {
            budget: Some(Arc::clone(shared)),
            class,
            bytes,
        }
    }

    /// Lets go of the ledger without releasing: for a reservation whose bytes its holder
    /// releases itself.
    fn forget(mut self) {
        self.budget = None;
    }

    /// The bytes held.
    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Holds `bytes` from now where they are fewer, and releases the rest to whoever waits; a
    /// reservation never grows.
    pub(crate) fn shrink(&mut self, bytes: u64) {
        let Some(shared) = &self.budget else {
            return;
        };
        if bytes >= self.bytes {
            return;
        }
        let mut ledger = shared.lock();
        ledger.release(self.class, self.bytes - bytes);
        self.bytes = bytes;
        admit_waiting(shared, &mut ledger);
    }

    /// Takes `bytes` of what is held, or all of it where it holds fewer, as a reservation of
    /// their own: for a part of what was reserved that another holder releases.
    pub(crate) fn split(&mut self, bytes: u64) -> Self {
        let bytes = bytes.min(self.bytes);
        self.bytes -= bytes;
        Self {
            budget: self.budget.clone(),
            class: self.class,
            bytes,
        }
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
        ledger.release(self.class, self.bytes);
        admit_waiting(&shared, &mut ledger);
    }
}
