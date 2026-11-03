//! The spare a budget keeps: the allocation a served read's frame arrived in, once nothing holds
//! it, reserved of what pushes may take for a later frame while no request for the data needs its
//! room.

use rdlt_wire::aligned::Spare;
use rdlt_wire::limits::count;

use super::{Class, MemoryBudget, Reservation};

/// A spare and its charge, declared after it: dropped, the allocation is freed before its charge
/// is released.
pub(super) struct Kept {
    spare: Spare,
    charge: Reservation,
}

impl MemoryBudget {
    /// Keeps `spare`, the allocation of a served read's frame nothing holds any more, reserved of
    /// what pushes may take for a later frame, in place of a smaller one, where it fits and no
    /// request for the data waits; frees it otherwise.
    ///
    /// A request for the data that would wait has its room first: a spare never makes anything
    /// wait.
    pub(crate) fn keep_spare(&self, spare: Spare) {
        let bytes = count(spare.bytes());
        let mut slot = self.spare.lock();
        if slot
            .as_ref()
            .is_some_and(|kept| kept.charge.bytes() >= bytes)
        {
            return;
        }
        // Freed and released before the larger is charged.
        drop(slot.take());
        let charge = {
            let mut ledger = self.shared.lock();
            ledger.open(Class::Intake, bytes).then(|| {
                ledger.reserve(Class::Intake, bytes);
                Reservation::of(&self.shared, Class::Intake, bytes)
            })
        };
        if let Some(charge) = charge {
            *slot = Some(Kept { spare, charge });
        }
    }

    /// The spare kept for a frame needing `room` bytes, where one [fits](Spare::fits) it: taken,
    /// it is reserved no more, and the frame arriving in it is held as any frame arriving is.
    pub(crate) fn take_spare(&self, room: usize) -> Option<Spare> {
        let kept = self.spare.lock().take_if(|kept| kept.spare.fits(room))?;
        Some(kept.spare)
    }

    /// Gives up the spare for a request of `class` where that is a request for the data, freed
    /// and then its charge released to whoever waits, and says whether one was kept.
    pub(super) fn yields(&self, class: Class) -> bool {
        if !matches!(class, Class::Intake | Class::Work) {
            return false;
        }
        let kept = self.spare.lock().take();
        kept.is_some()
    }
}
