//! What holds a unit's memory while it is lowered: its batches, the permits its pushes were
//! admitted with, and the allocations they cover.

use std::sync::Arc;

use arrow_array::RecordBatch;
use parking_lot::Mutex;
use rdlt_connector::Permit;
use rdlt_connector::cost::Allocations;

use crate::budget::Reservation;
use crate::cost::Admitted;

/// A unit's batches and what was charged for them, held together however the unit ends.
///
/// The fields are dropped in the order they are declared: the batches are freed before the
/// permits release what was charged for them, so nothing else may take those bytes while the
/// batches still hold them.
#[derive(Default)]
pub(super) struct Held {
    pub(super) batches: Vec<RecordBatch>,
    pub(super) permits: Vec<Permit>,
    /// The allocations the batches keep alive, made once and never added to, so every address
    /// in it is alive while it is: what a piece is lowered to counts only what it keeps alive
    /// beyond them.
    pub(super) allocations: Arc<Allocations>,
}

/// What holds a unit, shared by each of its pieces, each declaring its share after its rows:
/// the unit's batches, then its permits, go once the last piece is written or let go, whichever
/// piece that is and wherever it is.
pub(super) type Share = Arc<Mutex<Held>>;

impl Held {
    /// What holds `batches`, a unit `permits` were admitted for.
    pub(super) fn of(batches: Vec<RecordBatch>, permits: Vec<Permit>) -> Self {
        let mut allocations = Allocations::default();
        for batch in &batches {
            allocations.add(batch);
        }
        Self {
            batches,
            permits,
            allocations: Arc::new(allocations),
        }
    }

    /// What holds `batches`, another piece of the same unit: no permits of its own, the unit's
    /// allocations.
    pub(super) fn piece(&self, batches: Vec<RecordBatch>) -> Self {
        Self {
            batches,
            permits: Vec::new(),
            allocations: Arc::clone(&self.allocations),
        }
    }
}

/// What holds each of `batches`, shredded from the JSON pushes `permits` were admitted for, with
/// `beyond` reserved for what building them took beyond that, where it took more.
///
/// A JSON push is admitted for its text and for the batches it becomes, and what the batches take
/// beyond was reserved before they were built: the permits and the reservation give back what
/// the batches do not keep alive, now that the text is gone, and the batches share them until
/// the last is written.
pub(super) fn shredded(
    mut permits: Vec<Permit>,
    beyond: Option<Reservation>,
    batches: Vec<RecordBatch>,
) -> Vec<Held> {
    let mut alive = batches
        .iter()
        .map(|batch| Allocations::of(batch).bytes())
        .fold(0, u64::saturating_add);
    for permit in &mut permits {
        if let Some(admitted) = permit.downcast_mut::<Admitted>() {
            admitted.shrink(alive);
            alive = alive.saturating_sub(admitted.bytes);
        }
    }
    if let Some(mut beyond) = beyond {
        beyond.shrink(alive);
        permits.push(Box::new(beyond));
    }
    let shared = Arc::new(Mutex::new(permits));
    batches
        .into_iter()
        .map(|batch| {
            let permit: Permit = Box::new(Arc::clone(&shared));
            Held::of(vec![batch], vec![permit])
        })
        .collect()
}
