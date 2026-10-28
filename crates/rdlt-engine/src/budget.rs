//! The memory budget: everything the engine holds of what connectors send has a reservation of
//! its bytes, and the reservations never pass the budget.

mod admits;
mod decoding;
mod ledger;
#[cfg(test)]
mod tests;

use std::fmt;
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

pub(crate) use self::admits::{admitted, least};
pub(crate) use self::decoding::Decoding;
pub(crate) use self::ledger::{Class, Denied, Exhausted, Shares, TooLarge};
use self::ledger::{Ledger, Wait, admit_waiting};
use crate::env::Env;
use crate::report::Waits;
use crate::watch;

/// Bytes the engine may hold, divided into shares, each its holders' alone.
///
/// - **Never exceeded.** A request is reserved only when it fits its share. One that asks for
///   more than a request of its share may take is refused, since no wait could admit it; none
///   is cut down to fit, and nothing is reserved without asking.
/// - **Shares.** The cursors of seals waiting for a commit, tables' records, answers being decoded
///   and what reads keep each have a share no push can use, so a checkpoint never waits behind
///   data. The rest holds pushes waiting to be lowered and what lowering makes of them; pushes
///   never take all of it, so a request for lowering fits once the pieces before it are written.
/// - **No hold and wait.** Whoever holds what lowering reserved, a piece on the compute pool or on
///   its lane, needs no budget to release it. A partition waits for lowering while it holds
///   pushes, and nothing waits for pushes while it holds anything; while it holds lowering's bytes
///   it lowers inside what it reserved with them, a [`Lent`] it waits on for its own pieces.
/// - **No wait for bytes is for ever.** A request's wait ends at the budget's deadline with what
///   held the budget, and a request nobody waits for any more leaves the queue at once.
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
    /// The reads at once that what the budget admits is derived for.
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

/// A wait of a request, counted with its time once it ends, however it ends.
struct Waiting<'a> {
    budget: &'a MemoryBudget,
    wait: Wait,
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
        self.budget.shared.lock().count_wait(self.wait, waited);
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
        let ledger = Ledger::new(capacity);
        #[cfg(feature = "sim")]
        let ledger = ledger.observed(Arc::clone(&env));
        Self {
            shared: Arc::new(Mutex::new(ledger)),
            shares: Shares::of(capacity),
            env,
            wait,
            readers: 1,
            limits: None,
        }
    }

    /// The budget, read by `readers` reads at once at most, among whom what its reads keep is
    /// divided into parts.
    pub(crate) fn read_by(mut self, readers: usize) -> Self {
        self.readers = readers.max(1);
        self
    }

    /// The parts what reads keep is divided into.
    pub(crate) fn parts(&self) -> NonZeroUsize {
        admits::parts(self.shares, self.readers)
    }

    /// Bytes: a part of what reads keep, the most one read keeps beside its events.
    pub(crate) fn part(&self) -> u64 {
        admits::kept(self.shares, self.readers)
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
        self.request(Wait::Of(Class::Intake), bytes).await
    }

    /// Reserves `bytes` a frame of a served read holds while it is decoded, as
    /// [`MemoryBudget::acquire`] reserves a push's: its wait is counted apart from pushes'.
    ///
    /// # Errors
    ///
    /// As [`MemoryBudget::acquire`].
    pub(crate) async fn acquire_decoding(&self, bytes: u64) -> Result<Reservation, Denied> {
        self.request(Wait::Decode, bytes).await
    }

    /// Reserves `bytes` for lowering, all its next step holds, in one request: it waits only for
    /// what other lowerings hold, which their writes release.
    ///
    /// # Errors
    ///
    /// As [`MemoryBudget::acquire`], for more than one request for lowering may take.
    pub(crate) async fn acquire_working(&self, bytes: u64) -> Result<Reservation, Denied> {
        self.request(Wait::Of(Class::Work), bytes).await
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
        self.request(Wait::Of(Class::Cursor), bytes).await
    }

    /// Reserves the `bytes` a commit will record of a table, from the tables' share: only a
    /// commit releases them.
    ///
    /// # Errors
    ///
    /// As [`MemoryBudget::acquire`], for records larger than the share.
    pub(crate) async fn acquire_tables(&self, bytes: u64) -> Result<Reservation, Denied> {
        self.request(Wait::Of(Class::Tables), bytes).await
    }

    /// Reserves the `bytes` decoding a connector's answer holds, from the share of answers being
    /// decoded, until it is decoded.
    ///
    /// # Errors
    ///
    /// As [`MemoryBudget::acquire`], for an answer that holds more than the share.
    pub(crate) async fn acquire_control(&self, bytes: u64) -> Result<Reservation, Denied> {
        self.request(Wait::Of(Class::Control), bytes).await
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
        ledger.kept_peak = ledger.kept_peak.max(ledger.kept());
        Ok(Reservation::of(&self.shared, Class::Read, bytes))
    }

    async fn request(&self, wait: Wait, bytes: u64) -> Result<Reservation, Denied> {
        let class = wait.class();
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
        let waiting = self.waiting(wait);
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

    /// A wait counted as `wait` that begins now.
    fn waiting(&self, wait: Wait) -> Waiting<'_> {
        Waiting {
            budget: self,
            wait,
            began: self.env.instant(),
        }
    }

    /// Waits for `slot`, one of the places among which what reads keep is divided, which was not
    /// free, for as long as it takes, counting the wait with what reads keep.
    pub(crate) async fn read_slot<T>(&self, slot: impl Future<Output = T>) -> T {
        let _waiting = self.waiting(Wait::Of(Class::Read));
        slot.await
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

    /// The most bytes reads ever kept at once.
    pub(crate) fn kept_peak(&self) -> u64 {
        self.shared.lock().kept_peak
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
        let (bytes, class, free) = (self.bytes, self.class, self.bytes);
        // A part of a lent reservation, lent again, presses the budget the first was lent from.
        let ledger = match &self.home {
            Some(Home::Budget(shared)) => Some(Arc::clone(shared)),
            Some(Home::Lent(lending)) => lending.ledger.as_ref().map(Arc::clone),
            None => None,
        };
        Lent(Arc::new(Lending {
            bytes,
            class,
            ledger,
            state: Mutex::new(Holding {
                held: self,
                free,
                closed: false,
            }),
            returned: tokio::sync::Notify::new(),
        }))
    }
}

/// A reservation lent to its holder's requests, a part at a time; the budget has its bytes back
/// once it and every part are gone.
#[derive(Clone)]
pub(crate) struct Lent(Arc<Lending>);

struct Lending {
    /// Bytes: what was lent, the most one request of it may take.
    bytes: u64,
    class: Class,
    /// The ledger of the budget it was reserved of, pressed while a request of it waits.
    ledger: Option<Arc<Mutex<Ledger>>>,
    state: Mutex<Holding>,
    /// Notified as a part goes back.
    returned: tokio::sync::Notify,
}

/// What a lent reservation holds of the budget, and of that what no part holds.
struct Holding {
    held: Reservation,
    /// Bytes no part holds.
    free: u64,
    /// Whether it lends no more: a part then goes back to the budget.
    closed: bool,
}

impl Lending {
    fn give_back(&self, bytes: u64) {
        let mut lend = self.state.lock();
        if lend.closed {
            let held = lend.held.bytes().saturating_sub(bytes);
            lend.held.shrink(held);
        } else {
            lend.free += bytes;
        }
        self.returned.notify_one();
    }
}

impl Lent {
    /// The bytes lent, the most one request of them may take.
    pub(crate) fn bytes(&self) -> u64 {
        self.0.bytes
    }

    /// Lends no more: what no part holds goes back to the budget now, and each part as it is
    /// released.
    pub(crate) fn close(&self) {
        let mut lend = self.0.state.lock();
        lend.closed = true;
        let held = lend.held.bytes().saturating_sub(lend.free);
        lend.held.shrink(held);
        lend.free = 0;
    }

    /// Takes `bytes` of what is lent where no part holds them; nothing otherwise, or once it is
    /// closed.
    pub(crate) fn try_take(&self, bytes: u64) -> Option<Reservation> {
        let mut lend = self.0.state.lock();
        (bytes <= lend.free).then(|| {
            lend.free -= bytes;
            Reservation {
                home: Some(Home::Lent(Arc::clone(&self.0))),
                class: self.0.class,
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
                what: self.0.class.what(),
                asked: bytes,
                limit: self.bytes(),
            });
        }
        if let Some(taken) = self.try_take(bytes) {
            return Ok(taken);
        }
        let _pressing = self.0.ledger.as_ref().map(|shared| {
            shared.lock().pressing(true);
            Pressing(Arc::clone(shared))
        });
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
