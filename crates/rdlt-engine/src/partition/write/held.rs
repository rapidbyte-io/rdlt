//! What holds a unit's memory while it is lowered: its batches and the permits its pushes were
//! admitted with.

use std::sync::Arc;

use arrow_array::RecordBatch;
use parking_lot::Mutex;
use rdlt_connector::Permit;
use rdlt_connector::cost::batch_charge;

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
}

/// What holds a unit, shared by each of its pieces, each declaring its share after its rows:
/// the unit's batches, then its permits, go once the last piece is written or let go, whichever
/// piece that is and wherever it is.
pub(super) type Share = Arc<Mutex<Held>>;

impl Held {
    /// What holds `batches`, a unit `permits` were admitted for.
    pub(super) fn of(batches: Vec<RecordBatch>, permits: Vec<Permit>) -> Self {
        Self { batches, permits }
    }
}

/// What holds each of `batches`, shredded from the JSON pushes `permits` were admitted for, with
/// `beyond` reserved for what building them took beyond that, where it took more.
///
/// A JSON push is admitted for its text and for the batches it becomes, and what the batches take
/// beyond was reserved before they were built: the permits and the reservation give back what
/// the batches do not hold, now that the text is gone, keeping what a push of them would be
/// charged, and the batches share them until the last is written.
pub(super) fn shredded(
    mut permits: Vec<Permit>,
    beyond: Option<Reservation>,
    batches: Vec<RecordBatch>,
) -> Vec<Held> {
    let mut alive = batches
        .iter()
        .map(batch_charge)
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
