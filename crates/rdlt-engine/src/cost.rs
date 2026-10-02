//! What the engine charges its budget as a partition reads: each push by what it keeps alive, a
//! checkpoint by its cursor's bytes, and what the read keeps beside its events.

#[cfg(test)]
mod tests;

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use rdlt_connector::cost::{Allocations, Rendering};
use rdlt_connector::{
    Admission, BoxFuture, Capabilities, ConnectorError, ConnectorErrorKind, LimitExceeded, Permit,
    Push, SourceEvent,
};

use crate::budget::{Denied, Exhausted, MemoryBudget, Reservation, TooLarge};
use crate::limits::PUSH_EXCEEDS_BUDGET;

/// How `capabilities`' destination renders values: the kinds it stores as they are, and every
/// other as text.
pub(crate) fn rendering(capabilities: &Capabilities) -> Rendering {
    Rendering::new(capabilities.types.iter().copied())
}

/// Bytes a JSON push is charged for each byte of its text: the text, and twice it for the
/// batches it is shredded into, which are paid for before they are built.
///
/// What sparse records become beyond that, a null in every column for every row, the shredder's
/// limit on cells bounds, not the budget.
pub(crate) const JSON_CHARGE: u64 = 3;

/// Bytes: what normalizing makes of each of a batch's rows beside its values: its id and its
/// place.
pub(crate) const LINEAGE_ROW: u64 = 32;

/// Bytes: what normalizing makes of each item an array holds beside the item, which it copies:
/// the item's id, its parent's and its root's, and its places among them.
pub(crate) const LINEAGE_ITEM: u64 = 96;

/// Times what a batch's rows expand to that normalizing them may hold: the items it copies out of
/// their arrays, and an array of another layout cast to a list first.
pub(crate) const SPLIT_COPIES: u64 = 2;

/// Admits one partition's events into its channel: a push by what it keeps alive, a checkpoint
/// by its cursor's bytes, which wait with its seal for a commit, and what the read keeps beside
/// its events up to a read's part of the share reads have.
pub(crate) struct Charging {
    budget: MemoryBudget,
    /// Bytes: the most the read may keep beside its events.
    read_share: u64,
    /// Bytes the read keeps now.
    kept: Arc<AtomicU64>,
    /// The wait on the budget that failed an admission, where one did.
    exhausted: Mutex<Option<Exhausted>>,
}

impl Charging {
    /// An admission charging `budget` for the events of one of the reads that share what its
    /// reads may keep.
    pub(crate) fn new(budget: MemoryBudget) -> Self {
        let partitions = u64::try_from(budget.readers()).unwrap_or(u64::MAX).max(1);
        Self {
            read_share: budget.shares().reads / partitions,
            budget,
            kept: Arc::new(AtomicU64::new(0)),
            exhausted: Mutex::new(None),
        }
    }

    /// The wait on the budget that failed one of the read's events, where one did: the read
    /// then failed for the budget, whatever error its source ended with.
    pub(crate) fn exhausted(&self) -> Option<Exhausted> {
        *self.exhausted.lock()
    }

    /// The error an event is refused with, as the read's source sees it; a wait that reached
    /// its deadline is remembered, since the source may answer it with any error of its own.
    fn refused(&self, name: &'static str, denied: Denied) -> ConnectorError {
        match denied {
            Denied::Exhausted(exhausted) => {
                *self.exhausted.lock() = Some(exhausted);
                ConnectorError::new(ConnectorErrorKind::Transient, exhausted.to_string())
            }
            Denied::TooLarge(large) => {
                let refused = too_large(name, large);
                match name {
                    PUSH => refused.with_code(PUSH_EXCEEDS_BUDGET),
                    _ => refused,
                }
            }
        }
    }
}

/// The name of the limit a push beyond what pushes may take of the budget passes.
const PUSH: &str = "push bytes";

/// The error for `name` beyond what one request may take of the budget.
fn too_large(name: &'static str, large: TooLarge) -> ConnectorError {
    ConnectorError::exceeds(LimitExceeded {
        name,
        limit: large.limit,
        actual: large.asked,
    })
}

impl Admission for Charging {
    fn admit<'a>(
        &'a self,
        event: &'a SourceEvent,
    ) -> BoxFuture<'a, rdlt_connector::Result<Option<Permit>>> {
        Box::pin(async move {
            let count = |bytes: usize| u64::try_from(bytes).unwrap_or(u64::MAX);
            let (bytes, admitted) = match event {
                SourceEvent::Push(Push::Arrow(batch) | Push::Changes(batch)) => {
                    let bytes = Allocations::of(batch).bytes();
                    (bytes, self.budget.acquire(bytes).await)
                }
                SourceEvent::Push(Push::Json(json)) => {
                    let bytes = count(json.len()).saturating_mul(JSON_CHARGE);
                    (bytes, self.budget.acquire(bytes).await)
                }
                // A cursor waits with its seal for a commit, which alone releases it.
                SourceEvent::Checkpoint { cursor, .. } => {
                    let bytes = count(cursor.bytes().len());
                    let admitted = self.budget.acquire_cursor(bytes).await;
                    let admitted = admitted.map_err(|denied| self.refused("cursor bytes", denied));
                    return Ok(Some(Box::new(Admitted::new(bytes, admitted?)) as Permit));
                }
                SourceEvent::Log { .. }
                | SourceEvent::Metric { .. }
                | SourceEvent::Replan
                | SourceEvent::Behind { .. } => return Ok(None),
            };
            let reservation = admitted.map_err(|denied| self.refused(PUSH, denied))?;
            Ok(Some(Box::new(Admitted::new(bytes, reservation)) as Permit))
        })
    }

    fn charge(&self, bytes: u64) -> rdlt_connector::Result<Permit> {
        // What a read keeps beside its events no write releases: it has a part of a share of its
        // own, and a read that would keep more is refused where it would.
        let kept = self
            .kept
            .fetch_add(bytes, Ordering::SeqCst)
            .saturating_add(bytes);
        let within = (kept <= self.read_share).then(|| self.budget.keep(bytes));
        match within {
            Some(Ok(reservation)) => Ok(Box::new(Kept {
                bytes,
                total: Arc::clone(&self.kept),
                _reservation: reservation,
            })),
            beyond => {
                self.kept.fetch_sub(bytes, Ordering::SeqCst);
                let limit = match beyond {
                    Some(Err(large)) => large.limit.min(self.read_share),
                    _ => self.read_share,
                };
                Err(ConnectorError::exceeds(LimitExceeded {
                    name: "read kept bytes",
                    limit,
                    actual: kept,
                }))
            }
        }
    }
}

/// What a read keeps beside its events, released when dropped.
struct Kept {
    bytes: u64,
    total: Arc<AtomicU64>,
    _reservation: Reservation,
}

impl Drop for Kept {
    fn drop(&mut self) {
        self.total.fetch_sub(self.bytes, Ordering::SeqCst);
    }
}

/// What admitted an event: the bytes charged for it, held until this is dropped.
pub(crate) struct Admitted {
    /// The bytes the event was charged.
    pub(crate) bytes: u64,
    reservation: Reservation,
}

impl Admitted {
    fn new(bytes: u64, reservation: Reservation) -> Self {
        Self { bytes, reservation }
    }

    /// Holds `bytes` from now where they are fewer than were admitted: the rest is released.
    pub(crate) fn shrink(&mut self, bytes: u64) {
        self.reservation.shrink(bytes);
        self.bytes = self.reservation.bytes();
    }

    /// What `permit` admitted, where an engine's admission issued it.
    pub(crate) fn of(permit: Permit) -> Option<Box<Self>> {
        permit.downcast().ok()
    }
}

impl fmt::Debug for Admitted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Admitted").field(&self.bytes).finish()
    }
}
