//! The budget's ledger: what each share holds, and who waits for it.

#[cfg(kani)]
mod proofs;
#[cfg(test)]
mod tests;

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::oneshot;

use super::Reservation;
use crate::limits::{
    CONTROL_SHARE, CURSOR_SHARE, MIN_PIECE, PIECE_SHARE, READ_SHARE, REQUEST_SHARE, TABLE_SHARE,
};
use crate::report::Waits;
use crate::watch;

/// Whose bytes a reservation holds: which share they are of.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Class {
    /// A push admitted and not yet written: what it keeps alive.
    Intake,
    /// What lowering holds: a piece being lowered or queued for its write.
    Work,
    /// The cursor of a seal waiting for its commit.
    Cursor,
    /// What a commit records of a table's schema and names, from the schema change that makes
    /// it until the commit recording it lands.
    Tables,
    /// What a read keeps beside its events.
    Read,
    /// What decoding a connector's answer holds, until it is decoded.
    Control,
}

impl Class {
    /// What a request of the class is for, as its refusal says it.
    pub(super) fn what(self) -> &'static str {
        match self {
            Self::Intake => "a push",
            Self::Work => "lowering",
            Self::Cursor => "a cursor",
            Self::Tables => "a table's records",
            Self::Read => "what reads keep",
            Self::Control => "decoding an answer",
        }
    }
}

/// What a wait for room is counted as: its share's, or, for a frame of a served read waiting to
/// be decoded, which waits in the pushes' share, a decode's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Wait {
    Of(Class),
    Decode,
}

impl Wait {
    /// The share the wait is for.
    pub(super) fn class(self) -> Class {
        match self {
            Self::Of(class) => class,
            Self::Decode => Class::Intake,
        }
    }
}

/// Bytes: what each share of a budget holds at most.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Shares {
    /// The cursors of seals waiting for a commit.
    pub(crate) cursors: u64,
    /// What commits record of tables' schemas and names.
    pub(crate) tables: u64,
    /// What all reads keep together.
    pub(crate) reads: u64,
    /// What decoding connectors' answers holds together.
    pub(crate) control: u64,
    /// Pushes and what lowering makes of them: the rest of the budget.
    pub(crate) data: u64,
    /// The most one request for lowering may take.
    pub(crate) request: u64,
    /// The most pushes waiting to be lowered take together: the data less one request, so a
    /// request for lowering always fits once the pieces before it are written.
    pub(crate) intake: u64,
    /// The most lowering one piece holds, but for a row that alone takes more.
    pub(crate) piece: u64,
}

impl Shares {
    /// The shares of a budget of `capacity` bytes.
    pub(crate) fn of(capacity: u64) -> Self {
        let (cursors, tables, reads, control) = (
            capacity / CURSOR_SHARE,
            capacity / TABLE_SHARE,
            capacity / READ_SHARE,
            capacity / CONTROL_SHARE,
        );
        let data = capacity - cursors - tables - reads - control;
        let request = (capacity / REQUEST_SHARE).min(data);
        Self {
            cursors,
            tables,
            reads,
            control,
            data,
            request,
            intake: data - request,
            piece: (capacity / PIECE_SHARE).max(MIN_PIECE).min(request),
        }
    }
}

/// A request waiting for room.
struct Waiter {
    id: u64,
    bytes: u64,
    sender: oneshot::Sender<Reservation>,
}

/// A wait on the budget that ended at its deadline: what was asked, and what held the budget.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error(
    "{asked} bytes for {what} waited {waited:?} for room in a memory budget of {capacity}: \
     pushes hold {intake} bytes, lowering {work}, cursors waiting for a commit {cursors}, \
     tables' records waiting for a commit {tables}, answers being decoded {control}, and \
     {reads} are kept by reads"
)]
pub(crate) struct Exhausted {
    /// What the request was for.
    pub(crate) what: &'static str,
    /// Bytes: what the request asked for.
    pub(crate) asked: u64,
    /// Bytes: the budget.
    pub(crate) capacity: u64,
    /// Bytes pushes waiting to be lowered hold.
    pub(crate) intake: u64,
    /// Bytes lowering holds.
    pub(crate) work: u64,
    /// Bytes the cursors of seals waiting for a commit hold.
    pub(crate) cursors: u64,
    /// Bytes tables' records waiting for a commit hold.
    pub(crate) tables: u64,
    /// Bytes reads keep.
    pub(crate) reads: u64,
    /// Bytes answers being decoded hold.
    pub(crate) control: u64,
    /// How long the request waited.
    pub(crate) waited: Duration,
}

/// A request for more than one request of its share may take: it could never be admitted.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{asked} bytes for {what} are more than the {limit} it may take of a memory budget")]
pub(crate) struct TooLarge {
    /// What the request was for.
    pub(crate) what: &'static str,
    /// Bytes: what the request asked for.
    pub(crate) asked: u64,
    /// Bytes: the most a request of its share may take.
    pub(crate) limit: u64,
}

/// Why a request was not admitted.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum Denied {
    /// It waited until its deadline.
    #[error(transparent)]
    Exhausted(#[from] Exhausted),
    /// It asked for more than a request may take.
    #[error(transparent)]
    TooLarge(#[from] TooLarge),
}

pub(super) struct Ledger {
    pub(super) capacity: u64,
    pub(super) shares: Shares,
    intake: u64,
    work: u64,
    cursors: u64,
    tables: u64,
    reads: u64,
    control: u64,
    pub(super) peak: u64,
    /// The most what reads keep has held at once.
    pub(super) kept_peak: u64,
    /// The requests waiting for each share, in arrival order; reads never wait.
    waiting: [VecDeque<Waiter>; 5],
    next: u64,
    /// How many holders of pieces queued for a write wait for them to be written.
    pressing: usize,
    /// Whether whoever holds bytes a write releases should write them now.
    pub(super) pressed: watch::Sender<bool>,
    /// Whether a cursor or a table's records wait for what a commit releases.
    pub(super) cursor_waits: watch::Sender<bool>,
    /// The requests that waited, and for how long, by share.
    pub(super) waited: Waits,
    /// Changes sent to the tasks watching for pressure and for cursors' waits, each waking every
    /// task waiting on that value.
    pub(super) signals: u64,
    /// The waits under way, each with its place, what it is counted as and when it began, the
    /// earliest first: waits mostly end in the order they began, so each is found near the front.
    under_way: VecDeque<(u64, Wait, Instant)>,
    /// The place the next wait under way takes among them.
    opened: u64,
    /// Where each change of what is reserved is told.
    #[cfg(feature = "sim")]
    observer: Option<Arc<dyn crate::env::Env>>,
}

/// The classes that wait, in the order their waiters are admitted: cursors, tables' records and
/// answers being decoded first, which no push holds up, then lowering, which releases bytes, then
/// pushes.
const WAITING: [Class; 5] = [
    Class::Cursor,
    Class::Tables,
    Class::Control,
    Class::Work,
    Class::Intake,
];

impl Ledger {
    pub(super) fn new(capacity: u64) -> Self {
        Self {
            capacity,
            shares: Shares::of(capacity),
            intake: 0,
            work: 0,
            cursors: 0,
            tables: 0,
            reads: 0,
            control: 0,
            peak: 0,
            kept_peak: 0,
            waiting: Default::default(),
            next: 0,
            pressing: 0,
            pressed: watch::Sender::new(false),
            cursor_waits: watch::Sender::new(false),
            waited: Waits::default(),
            signals: 0,
            under_way: VecDeque::new(),
            opened: 0,
            #[cfg(feature = "sim")]
            observer: None,
        }
    }

    /// The ledger, each change of what it holds reserved told to `observer`.
    #[cfg(feature = "sim")]
    pub(super) fn observed(self, observer: Arc<dyn crate::env::Env>) -> Self {
        Self {
            observer: Some(observer),
            ..self
        }
    }

    /// Bytes what reads keep holds.
    pub(super) fn kept(&self) -> u64 {
        self.reads
    }

    /// Bytes reserved, every share together.
    pub(super) fn reserved(&self) -> u64 {
        self.intake + self.work + self.cursors + self.tables + self.reads + self.control
    }

    fn queue(&mut self, class: Class) -> Option<&mut VecDeque<Waiter>> {
        let place = WAITING.iter().position(|waiting| *waiting == class)?;
        self.waiting.get_mut(place)
    }

    fn held(&mut self, class: Class) -> &mut u64 {
        match class {
            Class::Intake => &mut self.intake,
            Class::Work => &mut self.work,
            Class::Cursor => &mut self.cursors,
            Class::Tables => &mut self.tables,
            Class::Read => &mut self.reads,
            Class::Control => &mut self.control,
        }
    }

    /// Bytes: the most one request of `class` may take.
    fn limit(&self, class: Class) -> u64 {
        match class {
            Class::Intake => self.shares.intake,
            Class::Work => self.shares.request,
            Class::Cursor => self.shares.cursors,
            Class::Tables => self.shares.tables,
            Class::Read => self.shares.reads,
            Class::Control => self.shares.control,
        }
    }

    /// The refusal of a request of `bytes` of `class` that no wait could admit.
    pub(super) fn too_large(&self, class: Class, bytes: u64) -> Option<TooLarge> {
        let limit = self.limit(class);
        (bytes > limit).then(|| TooLarge {
            what: class.what(),
            asked: bytes,
            limit,
        })
    }

    /// Whether `bytes` of `class` fit their share beside what it holds.
    ///
    /// The data beside a push is added saturating: of a budget past 15.5 EiB the two pass what a
    /// `u64` holds, and the data share is less than that.
    fn fits(&self, class: Class, bytes: u64) -> bool {
        let data = self.intake + self.work;
        match class {
            Class::Intake => {
                self.intake + bytes <= self.shares.intake
                    && data.saturating_add(bytes) <= self.shares.data
            }
            Class::Work => data + bytes <= self.shares.data,
            Class::Cursor => self.cursors + bytes <= self.shares.cursors,
            Class::Tables => self.tables + bytes <= self.shares.tables,
            Class::Read => self.reads + bytes <= self.shares.reads,
            Class::Control => self.control + bytes <= self.shares.control,
        }
    }

    /// Whether `bytes` of `class` may be reserved now: they fit, and no request that goes before
    /// them waits: one of their own share, or, for a push, one for lowering.
    pub(super) fn open(&self, class: Class, bytes: u64) -> bool {
        let waits = |class: Class| {
            let place = WAITING.iter().position(|waiting| *waiting == class);
            place.is_some_and(|place| !self.waiting[place].is_empty())
        };
        let behind = waits(class) || (class == Class::Intake && waits(Class::Work));
        !behind && self.fits(class, bytes)
    }

    /// Reserves `bytes` of `class`, which fit: no share ever holds more than it may.
    pub(super) fn reserve(&mut self, class: Class, bytes: u64) {
        debug_assert!(self.fits(class, bytes), "{bytes} bytes pass their share");
        *self.held(class) += bytes;
        self.peak = self.peak.max(self.reserved());
        #[cfg(feature = "sim")]
        self.tell(bytes, true);
    }

    /// Releases `bytes` of `class`, which holds them: releasing more is a bug, which debug builds
    /// and the simulation trip on, and a release build releases what is held.
    pub(super) fn release(&mut self, class: Class, bytes: u64) {
        let held = self.held(class);
        debug_assert!(
            bytes <= *held,
            "a release of {bytes} bytes passes the {held} bytes {class:?} holds"
        );
        let released = bytes.min(*held);
        *held -= released;
        #[cfg(feature = "sim")]
        self.tell(released, false);
    }

    /// Tells the observer that `bytes` more are reserved, or fewer where not `more`, where that
    /// changes anything.
    #[cfg(feature = "sim")]
    fn tell(&self, bytes: u64, more: bool) {
        if bytes == 0 {
            return;
        }
        if let Some(observer) = &self.observer {
            let bytes = i64::try_from(bytes).unwrap_or(i64::MAX);
            observer.charged(if more { bytes } else { -bytes });
        }
    }

    /// Queues a request of `bytes` of `class` and returns its place and where its reservation
    /// will arrive; nothing for a class that never waits.
    pub(super) fn wait(
        &mut self,
        class: Class,
        bytes: u64,
    ) -> Option<(u64, oneshot::Receiver<Reservation>)> {
        let (sender, receiver) = oneshot::channel();
        let id = self.next;
        self.next = self.next.wrapping_add(1);
        self.queue(class)?.push_back(Waiter { id, bytes, sender });
        self.press();
        Some((id, receiver))
    }

    /// Notes that a wait counted as `wait` began at `began`: the place it takes among the waits
    /// under way.
    pub(super) fn open_wait(&mut self, wait: Wait, began: Instant) -> u64 {
        let id = self.opened;
        self.opened = self.opened.wrapping_add(1);
        self.under_way.push_back((id, wait, began));
        id
    }

    /// Notes that the wait counted as `wait` at place `id` ended, having waited `time`.
    pub(super) fn close_wait(&mut self, wait: Wait, id: u64, time: Duration) {
        if let Some(place) = self.under_way.iter().position(|(opened, ..)| *opened == id) {
            self.under_way.remove(place);
        }
        self.count_wait(wait, time);
    }

    /// When the earliest wait under way counted as `wait` began.
    pub(super) fn earliest(&self, wait: Wait) -> Option<Instant> {
        let mut under_way = self.under_way.iter();
        under_way
            .find(|(_, counted, _)| *counted == wait)
            .map(|(.., began)| *began)
    }

    /// Notes that a request counted as `wait` waited `time`.
    fn count_wait(&mut self, wait: Wait, time: Duration) {
        let waited = match wait {
            Wait::Decode => &mut self.waited.decode,
            Wait::Of(Class::Intake) => &mut self.waited.intake,
            Wait::Of(Class::Work) => &mut self.waited.work,
            Wait::Of(Class::Cursor) => &mut self.waited.cursors,
            Wait::Of(Class::Tables) => &mut self.waited.tables,
            Wait::Of(Class::Read) => &mut self.waited.reads,
            Wait::Of(Class::Control) => &mut self.waited.control,
        };
        waited.add(time);
    }

    /// Forgets the request waiting at place `id`, where it still waits.
    pub(super) fn forget(&mut self, id: u64) {
        for queue in &mut self.waiting {
            queue.retain(|waiter| waiter.id != id);
        }
    }

    /// What holds the budget, for a request of `asked` bytes of `class` that waited `waited`.
    pub(super) fn exhausted(&self, class: Class, asked: u64, waited: Duration) -> Exhausted {
        Exhausted {
            what: class.what(),
            asked,
            capacity: self.capacity,
            intake: self.intake,
            work: self.work,
            cursors: self.cursors,
            tables: self.tables,
            reads: self.reads,
            control: self.control,
            waited,
        }
    }

    /// One more holder of pieces queued for a write waits for them, or one fewer.
    pub(super) fn pressing(&mut self, more: bool) {
        self.pressing = if more {
            self.pressing + 1
        } else {
            self.pressing.saturating_sub(1)
        };
        self.press();
    }

    /// Signals pressure while a request waits for bytes of pushes or of lowering, or a holder of
    /// queued pieces waits for their writes: whoever holds bytes a write releases should write.
    ///
    /// A cursor that waits is signalled apart: only a commit releases what it waits for. A value
    /// is sent only where it changes, so a release that frees no waiter wakes no one.
    pub(super) fn press(&mut self) {
        let waits = |class: Class| {
            let place = WAITING.iter().position(|waiting| *waiting == class);
            place.is_some_and(|place| !self.waiting[place].is_empty())
        };
        let pressed = self.pressing > 0 || waits(Class::Work) || waits(Class::Intake);
        let cursor_waits = waits(Class::Cursor) || waits(Class::Tables);
        let sent =
            u64::from(self.pressed.send(pressed)) + u64::from(self.cursor_waits.send(cursor_waits));
        self.signals = self.signals.saturating_add(sent);
    }

    /// The next waiting request that is admitted, and its class: the first of each share in
    /// turn, a push only while no request for lowering waits.
    fn next(&mut self) -> Option<(Class, Waiter)> {
        for (place, class) in WAITING.into_iter().enumerate() {
            let fits = match self.waiting[place].front() {
                Some(front) => self.fits(class, front.bytes),
                None => false,
            };
            if fits {
                return self.waiting[place]
                    .pop_front()
                    .map(|waiter| (class, waiter));
            }
            if class == Class::Work && !self.waiting[place].is_empty() {
                // A push never takes the room a request for lowering waits for.
                return None;
            }
        }
        None
    }
}

/// Admits waiting requests while the first of a share fits.
pub(super) fn admit_waiting(shared: &Arc<Mutex<Ledger>>, ledger: &mut Ledger) {
    while let Some((class, waiter)) = ledger.next() {
        let peak = ledger.peak;
        ledger.reserve(class, waiter.bytes);
        let reservation = Reservation::of(shared, class, waiter.bytes);
        if let Err(abandoned) = waiter.sender.send(reservation) {
            // The waiter stopped waiting; release its bytes here, under the lock already held.
            // Only bytes a waiter receives count toward the peak.
            abandoned.forget();
            ledger.release(class, waiter.bytes);
            ledger.peak = peak;
        }
    }
    ledger.press();
}
