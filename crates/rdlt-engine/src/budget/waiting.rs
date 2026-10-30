//! The waits of a budget's requests: each under way until it ends, however it ends, then
//! counted with its time, and the earliest of each share for a run's status.

use std::future::Future;
use std::time::{Duration, Instant, SystemTime};

use super::MemoryBudget;
use super::ledger::{Class, Ledger, Wait};
use crate::status::OpenWaits;

/// A wait of a request, under way until it ends, however it ends, and then counted with its
/// time.
pub(super) struct Waiting<'a> {
    budget: &'a MemoryBudget,
    wait: Wait,
    began: Instant,
    /// Its place among the waits under way.
    id: u64,
}

impl Waiting<'_> {
    /// How long the request has waited.
    pub(super) fn waited(&self) -> Duration {
        self.budget
            .env
            .instant()
            .saturating_duration_since(self.began)
    }
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        let waited = self.waited();
        let mut ledger = self.budget.shared.lock();
        ledger.close_wait(self.wait, self.id, waited);
    }
}

impl MemoryBudget {
    /// A wait counted as `wait` that begins now, under way in `ledger`, this budget's.
    pub(super) fn waiting(&self, ledger: &mut Ledger, wait: Wait) -> Waiting<'_> {
        let began = self.env.instant();
        Waiting {
            budget: self,
            wait,
            began,
            id: ledger.open_wait(wait, began),
        }
    }

    /// Waits for `slot`, one of the places among which what reads keep is divided, which was not
    /// free, for as long as it takes, counting the wait with what reads keep.
    pub(crate) async fn read_slot<T>(&self, slot: impl Future<Output = T>) -> T {
        let _waiting = self.waiting(&mut self.shared.lock(), Wait::Of(Class::Read));
        slot.await
    }

    /// When the earliest wait under way began in each share, as `wall` reads an instant.
    pub(crate) fn open_waits(&self, wall: impl Fn(Instant) -> SystemTime) -> OpenWaits {
        let ledger = self.shared.lock();
        let earliest = |wait: Wait| ledger.earliest(wait).map(&wall);
        OpenWaits {
            intake: earliest(Wait::Of(Class::Intake)),
            decode: earliest(Wait::Decode),
            work: earliest(Wait::Of(Class::Work)),
            cursors: earliest(Wait::Of(Class::Cursor)),
            tables: earliest(Wait::Of(Class::Tables)),
            control: earliest(Wait::Of(Class::Control)),
            reads: earliest(Wait::Of(Class::Read)),
        }
    }
}
