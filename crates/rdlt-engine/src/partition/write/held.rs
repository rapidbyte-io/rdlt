//! What holds a unit's memory while it is lowered: the permits charged for it, and the
//! allocations they cover.

use std::sync::Arc;

use arrow_array::RecordBatch;
use parking_lot::Mutex;
use rdlt_connector::Permit;
use rdlt_connector::cost::Allocations;

use crate::budget::{MemoryBudget, Reservation};
use crate::cost::Admitted;

/// What holds a unit's memory: the permits charged for it and the allocations they cover.
pub(super) struct Held {
    pub(super) permits: Vec<Permit>,
    /// Bytes the permits hold.
    bytes: u64,
    /// Bytes the permits hold beyond what the allocations take: what the unit may still grow by
    /// before more is charged.
    spare: u64,
    /// The allocations charged so far, the unit's own first; the pieces of a unit share them,
    /// so each is charged once however many pieces keep it alive.
    pub(super) allocations: Arc<Mutex<Allocations>>,
}

impl Held {
    /// What holds `batches`, a unit `permits` hold `bytes` for.
    pub(super) fn of(permits: Vec<Permit>, bytes: u64, batches: &[RecordBatch]) -> Self {
        let mut allocations = Allocations::default();
        for batch in batches {
            allocations.add(batch);
        }
        Self {
            permits,
            bytes,
            spare: bytes.saturating_sub(allocations.bytes()),
            allocations: Arc::new(Mutex::new(allocations)),
        }
    }

    /// What holds another piece of the same unit: no permits of its own, the unit's allocations.
    pub(super) fn piece(&self) -> Self {
        Self {
            permits: Vec::new(),
            bytes: 0,
            spare: 0,
            allocations: Arc::clone(&self.allocations),
        }
    }

    /// Bytes the unit's permits hold.
    #[cfg(test)]
    pub(super) fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Lets go of what the unit was charged beyond the allocations it keeps alive: each of its
    /// pieces reserves what lowering it takes, so the unit holds its source alone from now.
    ///
    /// Permits the engine's own admission did not issue are kept as they are.
    pub(super) fn settle(&mut self) {
        let known = |permit: &Permit| permit.is::<Admitted>() || permit.is::<Reservation>();
        if !self.permits.iter().all(known) {
            return;
        }
        let mut kept = self.bytes.saturating_sub(self.spare);
        let mut held = 0_u64;
        for permit in &mut self.permits {
            let bytes = if let Some(admitted) = permit.downcast_mut::<Admitted>() {
                admitted.keep(kept.min(admitted.bytes));
                admitted.bytes
            } else if let Some(reservation) = permit.downcast_mut::<Reservation>() {
                reservation.resize(kept.min(reservation.bytes()));
                reservation.bytes()
            } else {
                0
            };
            kept = kept.saturating_sub(bytes);
            held = held.saturating_add(bytes);
        }
        (self.bytes, self.spare) = (held, 0);
    }

    /// The unit's permits, each the engine issued said to be queued for a write: the unit goes
    /// to its lane with its last piece.
    pub(super) fn staged(mut self) -> Vec<Permit> {
        for permit in &mut self.permits {
            if let Some(admitted) = permit.downcast_mut::<Admitted>() {
                admitted.stage();
            } else if let Some(reservation) = permit.downcast_mut::<Reservation>() {
                reservation.stage();
            }
        }
        self.permits
    }

    /// Holds `reserved` for the unit too: bytes it is about to grow by, which its permits then
    /// spare for the growth that follows.
    pub(super) fn reserved(&mut self, reserved: Reservation) {
        self.bytes = self.bytes.saturating_add(reserved.bytes());
        self.spare = self.spare.saturating_add(reserved.bytes());
        self.permits.push(Box::new(reserved));
    }

    /// Bytes the unit's permits spare for growth.
    pub(super) fn spare(&self) -> u64 {
        self.spare
    }

    /// Charges `budget` the `fresh` bytes the unit grew by, beyond what its permits spare.
    pub(super) fn grow(&mut self, budget: &MemoryBudget, fresh: u64) {
        let beyond = fresh.saturating_sub(self.spare);
        self.permits.push(Box::new(budget.charge(beyond)));
        self.bytes = self.bytes.saturating_add(beyond);
        self.spare = self.spare.saturating_sub(fresh);
    }
}
