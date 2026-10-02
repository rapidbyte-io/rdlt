//! The budget's ledger: what is reserved, by whom it can be released, and who waits.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::oneshot;

use super::Reservation;
use crate::watch;

/// What releases reserved bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Class {
    /// Whoever holds them, once its work is done: a push on its way, a unit being lowered.
    Flight,
    /// A write: a piece queued on its lane, which a flush releases whatever else waits.
    Staged,
    /// A commit: the cursors of sealed segments.
    Commit,
    /// The end of a read, or its decoder letting go: what a read keeps beside its events.
    Read,
}

/// Bytes asked of the budget, and what releases them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Request {
    pub(super) bytes: u64,
    pub(super) class: Class,
    /// Whether the bytes are one row's that alone takes more than a piece: one such row is
    /// held beyond the budget at a time.
    pub(super) large: bool,
}

/// Who asks: work not begun, or work begun, which holds bytes in flight itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Asker {
    New,
    Begun,
}

/// A request waiting for room.
struct Waiter {
    id: u64,
    request: Request,
    asker: Asker,
    sender: oneshot::Sender<Reservation>,
}

/// A wait on the budget that ended at its deadline: what was asked, and what held the budget.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error(
    "{asked} bytes waited {waited:?} for room in a memory budget of {capacity}: {in_flight} bytes \
     are in flight, {commit} wait for a commit and {read} are kept by reads"
)]
pub(crate) struct Exhausted {
    /// Bytes: what the request asked for.
    pub(crate) asked: u64,
    /// Bytes: the budget.
    pub(crate) capacity: u64,
    /// Bytes reserved that writing what is in flight releases.
    pub(crate) in_flight: u64,
    /// Bytes reserved that only a commit releases.
    pub(crate) commit: u64,
    /// Bytes reserved that reads keep until they end.
    pub(crate) read: u64,
    /// How long the request waited.
    pub(crate) waited: Duration,
}

pub(super) struct Ledger {
    pub(super) capacity: u64,
    pub(super) reserved: u64,
    /// Of the reserved bytes, those a write releases.
    staged: u64,
    /// Of the reserved bytes, those only a commit releases.
    commit: u64,
    /// Of the reserved bytes, those reads keep.
    read: u64,
    /// How many rows that each take more than a piece are reserved.
    large: usize,
    pub(super) peak: u64,
    /// Requests of work already begun, which go before any other: finishing it releases bytes.
    working: VecDeque<Waiter>,
    waiting: VecDeque<Waiter>,
    next: u64,
    /// Whether a request waits for bytes that writing what is in flight would release.
    pub(super) pressed: watch::Sender<bool>,
}

impl Ledger {
    pub(super) fn new(capacity: u64) -> Self {
        Self {
            capacity,
            reserved: 0,
            staged: 0,
            commit: 0,
            read: 0,
            large: 0,
            peak: 0,
            working: VecDeque::new(),
            waiting: VecDeque::new(),
            next: 0,
            pressed: watch::Sender::new(false),
        }
    }

    /// The reserved bytes that writing what is in flight releases, sooner or later.
    fn in_flight(&self) -> u64 {
        self.reserved
            .saturating_sub(self.commit)
            .saturating_sub(self.read)
    }

    /// Whether `request` fits beside what is reserved, or nothing it could wait for is.
    ///
    /// - Work not begun waits for every byte in flight.
    /// - Work begun holds bytes in flight itself, as other work begun does, and none of them
    ///   is released before the work that holds it is done: it waits only for bytes a write
    ///   releases, and a row that takes more than a piece for the other such row reserved.
    /// - Bytes a commit releases wait for any byte at all.
    fn admits(&self, request: Request, asker: Asker) -> bool {
        let nothing = match (request.class, asker) {
            (Class::Commit | Class::Read, _) => self.reserved == 0,
            (_, Asker::New) => self.in_flight() == 0,
            (_, Asker::Begun) => self.staged == 0 && !(request.large && self.large > 0),
        };
        nothing || self.reserved.saturating_add(request.bytes) <= self.capacity
    }

    /// Whether `request` may be reserved now: it is admitted, and no request that goes before it
    /// waits.
    pub(super) fn open(&self, request: Request, asker: Asker) -> bool {
        let queued = match asker {
            Asker::Begun => !self.working.is_empty(),
            Asker::New => !self.working.is_empty() || !self.waiting.is_empty(),
        };
        !queued && self.admits(request, asker)
    }

    pub(super) fn reserve(&mut self, request: Request) {
        self.reserved = self.reserved.saturating_add(request.bytes);
        if let Some(class) = self.class(request.class) {
            *class = class.saturating_add(request.bytes);
        }
        self.large += usize::from(request.large);
        self.peak = self.peak.max(self.reserved);
        self.press();
    }

    pub(super) fn release(&mut self, request: Request) {
        self.reserved = self.reserved.saturating_sub(request.bytes);
        if let Some(class) = self.class(request.class) {
            *class = class.saturating_sub(request.bytes);
        }
        self.large = self.large.saturating_sub(usize::from(request.large));
    }

    /// The bytes reserved of `class`, where they are counted apart.
    fn class(&mut self, class: Class) -> Option<&mut u64> {
        match class {
            Class::Flight => None,
            Class::Staged => Some(&mut self.staged),
            Class::Commit => Some(&mut self.commit),
            Class::Read => Some(&mut self.read),
        }
    }

    /// Queues `request` and returns its place and where its reservation will arrive.
    pub(super) fn wait(
        &mut self,
        request: Request,
        asker: Asker,
    ) -> (u64, oneshot::Receiver<Reservation>) {
        let (sender, receiver) = oneshot::channel();
        let id = self.next;
        self.next = self.next.wrapping_add(1);
        let waiter = Waiter {
            id,
            request,
            asker,
            sender,
        };
        match asker {
            Asker::Begun => self.working.push_back(waiter),
            Asker::New => self.waiting.push_back(waiter),
        }
        self.press();
        (id, receiver)
    }

    /// Forgets the request waiting at place `id`, where it still waits.
    pub(super) fn forget(&mut self, id: u64) {
        self.working.retain(|waiter| waiter.id != id);
        self.waiting.retain(|waiter| waiter.id != id);
    }

    /// What holds the budget, for a request of `asked` bytes that waited `waited`.
    pub(super) fn exhausted(&self, asked: u64, waited: Duration) -> Exhausted {
        Exhausted {
            asked,
            capacity: self.capacity,
            in_flight: self.in_flight(),
            commit: self.commit,
            read: self.read,
            waited,
        }
    }

    /// Signals pressure while a request waits for bytes in flight, or the bytes in flight exceed
    /// the budget: either way, whoever holds bytes it could release early should.
    pub(super) fn press(&self) {
        let waits = !self.working.is_empty() || !self.waiting.is_empty();
        let pressed = (waits && self.in_flight() > 0) || self.in_flight() > self.capacity;
        self.pressed.send_replace(pressed);
    }

    /// The next waiting request that is admitted: the first of work begun, or, where no work
    /// begun waits, the first of the others.
    fn next(&mut self) -> Option<Waiter> {
        let admitted = |ledger: &Self, waiter: &Waiter| ledger.admits(waiter.request, waiter.asker);
        match self.working.front() {
            Some(front) if admitted(self, front) => self.working.pop_front(),
            Some(_) => None,
            None => match self.waiting.front() {
                Some(front) if admitted(self, front) => self.waiting.pop_front(),
                _ => None,
            },
        }
    }
}

/// Admits waiting requests while the front one fits: those of work begun first, then the others
/// in arrival order.
pub(super) fn admit_waiting(shared: &Arc<Mutex<Ledger>>, ledger: &mut Ledger) {
    while let Some(waiter) = ledger.next() {
        let peak = ledger.peak;
        ledger.reserve(waiter.request);
        let reservation = Reservation::of(shared, waiter.request);
        if let Err(abandoned) = waiter.sender.send(reservation) {
            // The waiter stopped waiting; release its bytes here, under the lock already held.
            // Only bytes a waiter receives count toward the peak.
            abandoned.forget();
            ledger.release(waiter.request);
            ledger.peak = peak;
        }
    }
    ledger.press();
}
