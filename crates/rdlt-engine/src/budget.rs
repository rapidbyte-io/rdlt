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
use std::time::{Duration, Instant};

use parking_lot::Mutex;

pub(crate) use self::admits::{admitted, kept, least};
pub(crate) use self::decoding::Decoding;
pub(crate) use self::ledger::{Class, Denied, Exhausted, Shares, TooLarge};
use self::ledger::{Ledger, admit_waiting};
use crate::env::Env;
use crate::report::Waits;
use crate::watch;

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
///   lowering while it holds pushes, and nothing waits for pushes while it holds anything; while
///   it holds lowering's bytes it lowers inside what it reserved with them, a [`Lent`] it waits
///   on for its own pieces.
/// - **No wait is for ever.** A wait ends at the budget's deadline with what held the budget, and
///   a request nobody waits for any more leaves the queue at once.
///
/// Requests of one share are admitted in arrival order. Reservations are released by dropping
/// them.
#[derive(Clone)]
pub(crate) struct MemoryBudget {
    shared: Arc<Mutex<Ledger>>,
    /// What each share holds at most, fixed with the budget's capacity.
    shares: Shares,
    /// The clock a wait is measured on.
    env: Arc<dyn Env>,
    /// How long a request waits at most.
    wait: Duration,
    /// How many reads share what reads may keep.
    readers: usize,
    /// The limits on what a read sends, where they are fewer than what the budget admits.
    limits: Option<rdlt_wire::Limits>,
}

/// A value of the budget a task watches across its loop, through one receiver it keeps.
pub(crate) struct Signal(watch::Receiver<bool>);

impl Signal {
    /// Completes once the value is raised: at once where it is raised now.
    pub(crate) async fn raised(&mut self) {
        // The ledger keeps the sender as long as the budget lives.
        if self.0.wait_for(|raised| *raised).await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

/// A request's place among those waiting, given up when dropped.
struct Queued<'a> {
    shared: &'a Arc<Mutex<Ledger>>,
    id: u64,
}

/// A wait of a request of a class, counted with its time once it ends, however it ends.
struct Waiting<'a> {
    budget: &'a MemoryBudget,
    class: Class,
    began: Instant,
}

impl Waiting<'_> {
    /// How long the request has waited.
    fn waited(&self) -> Duration {
        self.budget
            .env
            .instant()
            .saturating_duration_since(self.began)
    }
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        let waited = self.waited();
        self.budget.shared.lock().count_wait(self.class, waited);
    }
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
    /// A budget of `capacity` bytes, each of its requests waiting `wait` at most on `env`'s
    /// clock.
    pub(crate) fn new(capacity: u64, env: Arc<dyn Env>, wait: Duration) -> Self {
        Self {
            shared: Arc::new(Mutex::new(Ledger::new(capacity))),
            shares: Shares::of(capacity),
            env,
            wait,
            readers: 1,
            limits: None,
        }
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

    /// Reserves the `bytes` of a frame on its way into the log, or of what the log's store stages
    /// and a carry reads at once, from the log's own share.
    ///
    /// # Errors
    ///
    /// As [`MemoryBudget::acquire`], for a request larger than the share.
    pub(crate) async fn acquire_log(&self, bytes: u64) -> Result<Reservation, Denied> {
        self.request(Class::Log, bytes).await
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
        let waiting = self.waiting(class);
        tokio::select! {
            biased;
            reservation = receiver => {
                Ok(reservation.expect("the ledger answers every waiter it keeps"))
            }
            () = self.env.sleep(self.wait) => {
                let exhausted = self.shared.lock().exhausted(class, bytes, waiting.waited());
                drop(queued);
                Err(exhausted.into())
            }
        }
    }

    /// A wait of a request of `class` that begins now.
    fn waiting(&self, class: Class) -> Waiting<'_> {
        Waiting {
            budget: self,
            class,
            began: self.env.instant(),
        }
    }

    /// Waits for `slot`, one of the places among which what reads keep is divided, which was not
    /// free, for no longer than a request waits for bytes.
    ///
    /// # Errors
    ///
    /// [`Exhausted`] for what reads keep, with the part a slot holds, once the wait reaches the
    /// budget's deadline.
    pub(crate) async fn read_slot<T>(&self, slot: impl Future<Output = T>) -> Result<T, Exhausted> {
        let waiting = self.waiting(Class::Read);
        tokio::select! {
            biased;
            taken = slot => Ok(taken),
            () = self.env.sleep(self.wait) => {
                let readers = u64::try_from(self.readers).unwrap_or(u64::MAX);
                let part = self.shares().reads / readers.max(1);
                Err(self.shared.lock().exhausted(Class::Read, part, waiting.waited()))
            }
        }
    }

    /// Raised while a request waits for bytes of pushes or of lowering, or a holder of queued
    /// pieces waits for their writes: whoever holds bytes a write releases should write them.
    pub(crate) fn pressure(&self) -> Signal {
        Signal(self.shared.lock().pressed.subscribe())
    }

    /// Raised while a cursor or a table's records wait for room only a commit frees: a commit
    /// is then due, whatever its policy says.
    pub(crate) fn cursor_waits(&self) -> Signal {
        Signal(self.shared.lock().cursor_waits.subscribe())
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
        self.shares
    }

    /// Bytes reserved now.
    #[cfg(test)]
    pub(crate) fn reserved(&self) -> u64 {
        self.shared.lock().reserved()
    }

    /// The requests that have waited for room, and for how long, by share.
    pub(crate) fn waited(&self) -> Waits {
        self.shared.lock().waited
    }

    /// Changes the budget has sent to the tasks watching for pressure and for cursors' waits.
    pub(crate) fn signals(&self) -> u64 {
        self.shared.lock().signals
    }

    /// The most bytes ever reserved at once.
    pub(crate) fn peak(&self) -> u64 {
        self.shared.lock().peak
    }
}

/// A budget of `capacity` bytes whose requests wait a day at most, on a test's clock.
#[cfg(test)]
pub(crate) fn budget(capacity: u64) -> MemoryBudget {
    MemoryBudget::new(
        capacity,
        Arc::new(crate::env::InlineEnv),
        Duration::from_hours(24),
    )
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
    home: Option<Home>,
    class: Class,
    bytes: u64,
}

/// Where a reservation's bytes go back to: the budget, or the reservation they were lent from.
#[derive(Clone)]
enum Home {
    Budget(Arc<Mutex<Ledger>>),
    Lent(Arc<Lending>),
}

impl Home {
    fn release(&self, class: Class, bytes: u64) {
        match self {
            Self::Budget(shared) => {
                let mut ledger = shared.lock();
                ledger.release(class, bytes);
                admit_waiting(shared, &mut ledger);
            }
            Self::Lent(lending) => lending.give_back(bytes),
        }
    }
}

impl Reservation {
    fn of(shared: &Arc<Mutex<Ledger>>, class: Class, bytes: u64) -> Self {
        Self {
            home: Some(Home::Budget(Arc::clone(shared))),
            class,
            bytes,
        }
    }

    /// Lets go of the ledger without releasing: for a reservation whose bytes its holder
    /// releases itself.
    fn forget(mut self) {
        self.home = None;
    }

    /// The bytes held.
    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Holds `bytes` from now where they are fewer, and releases the rest to whoever waits; a
    /// reservation never grows.
    pub(crate) fn shrink(&mut self, bytes: u64) {
        let Some(home) = &self.home else {
            return;
        };
        if bytes >= self.bytes {
            return;
        }
        home.release(self.class, self.bytes - bytes);
        self.bytes = bytes;
    }

    /// Takes `bytes` of what is held, or all of it where it holds fewer, as a reservation of
    /// their own: for a part of what was reserved that another holder releases.
    pub(crate) fn split(&mut self, bytes: u64) -> Self {
        let bytes = bytes.min(self.bytes);
        self.bytes -= bytes;
        Self {
            home: self.home.clone(),
            class: self.class,
            bytes,
        }
    }

    /// The reservation lent, a part at a time, to its holder's own requests: a part goes back to
    /// it, not to the budget, so a holder that needs more waits for its own parts to be released.
    pub(crate) fn lend(self) -> Lent {
        let free = self.bytes;
        Lent(Arc::new(Lending {
            held: self,
            free: Mutex::new(free),
            returned: tokio::sync::Notify::new(),
        }))
    }
}

/// A reservation lent to its holder's requests, a part at a time; the budget has its bytes back
/// once it and every part are gone.
#[derive(Clone)]
pub(crate) struct Lent(Arc<Lending>);

struct Lending {
    held: Reservation,
    /// Bytes no part holds.
    free: Mutex<u64>,
    /// Notified as a part goes back.
    returned: tokio::sync::Notify,
}

impl Lending {
    fn give_back(&self, bytes: u64) {
        *self.free.lock() += bytes;
        self.returned.notify_one();
    }
}

impl Lent {
    /// The bytes lent, the most one request of them may take.
    pub(crate) fn bytes(&self) -> u64 {
        self.0.held.bytes
    }

    /// Takes `bytes` of what is lent where no part holds them; nothing otherwise.
    pub(crate) fn try_take(&self, bytes: u64) -> Option<Reservation> {
        let mut free = self.0.free.lock();
        (bytes <= *free).then(|| {
            *free -= bytes;
            Reservation {
                home: Some(Home::Lent(Arc::clone(&self.0))),
                class: self.0.held.class,
                bytes,
            }
        })
    }

    /// Takes `bytes` of what is lent, waiting for parts to go back and pressing their holders to
    /// write them meanwhile.
    ///
    /// # Errors
    ///
    /// A [`TooLarge`] for more than is lent, which no wait could take.
    pub(crate) async fn take(&self, bytes: u64) -> Result<Reservation, TooLarge> {
        if bytes > self.bytes() {
            return Err(TooLarge {
                what: self.0.held.class.what(),
                asked: bytes,
                limit: self.bytes(),
            });
        }
        if let Some(taken) = self.try_take(bytes) {
            return Ok(taken);
        }
        let _pressing = match &self.0.held.home {
            Some(Home::Budget(shared)) => {
                shared.lock().pressing(true);
                Some(Pressing(Arc::clone(shared)))
            }
            _ => None,
        };
        loop {
            // A part that went back since the last try left its notice.
            self.0.returned.notified().await;
            if let Some(taken) = self.try_take(bytes) {
                return Ok(taken);
            }
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
        if let Some(home) = self.home.take() {
            home.release(self.class, self.bytes);
        }
    }
}
