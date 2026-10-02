//! What holds a unit's memory while it is lowered: the permits its pushes were admitted with,
//! and the allocations they cover.

use std::sync::Arc;

use arrow_array::RecordBatch;
use parking_lot::Mutex;
use rdlt_connector::Permit;
use rdlt_connector::cost::Allocations;

use crate::cost::Admitted;

/// What holds a unit's memory: the permits charged for it and the allocations they cover.
pub(super) struct Held {
    pub(super) permits: Vec<Permit>,
    /// The allocations the unit keeps alive; the pieces of a unit share them, so what a piece
    /// is lowered to counts only what it keeps alive beside them.
    pub(super) allocations: Arc<Mutex<Allocations>>,
}

impl Held {
    /// What holds `batches`, a unit `permits` were admitted for.
    pub(super) fn of(permits: Vec<Permit>, batches: &[RecordBatch]) -> Self {
        let mut allocations = Allocations::default();
        for batch in batches {
            allocations.add(batch);
        }
        Self {
            permits,
            allocations: Arc::new(Mutex::new(allocations)),
        }
    }

    /// What holds another piece of the same unit: no permits of its own, the unit's allocations.
    pub(super) fn piece(&self) -> Self {
        Self {
            permits: Vec::new(),
            allocations: Arc::clone(&self.allocations),
        }
    }
}

/// What holds each of `batches`, shredded from the JSON pushes `permits` were admitted for.
///
/// A JSON push is admitted for its text and for the batches it becomes, so nothing more is
/// asked of the budget here: the permits give back what the batches do not keep alive, now that
/// the text is gone, and the batches share them until the last is written. Batches that keep
/// more alive than was admitted for them hold no more of the budget than that.
pub(super) fn shredded(mut permits: Vec<Permit>, batches: &[RecordBatch]) -> Vec<Held> {
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
    let shared = Arc::new(Mutex::new(permits));
    batches
        .iter()
        .map(|batch| {
            let permit: Permit = Box::new(Arc::clone(&shared));
            Held::of(vec![permit], std::slice::from_ref(batch))
        })
        .collect()
}
