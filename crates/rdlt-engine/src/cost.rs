//! What the engine charges its budget as a partition reads: each push by what it keeps alive, a
//! checkpoint by its cursor's bytes, and what the read keeps beside its events.

#[cfg(test)]
pub(crate) mod tests;

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use rdlt_connector::cost::{Rendering, push_charge};
use rdlt_connector::{
    Admission, BoxFuture, Capabilities, ConnectorError, ConnectorErrorKind, LimitExceeded, Permit,
    SourceEvent,
};
use rdlt_wire::limits::count;

use crate::budget::{Denied, Exhausted, MemoryBudget, Reservation, TooLarge};
use crate::normalize::Shape;

/// How `capabilities`' destination renders values: the kinds it stores as they are, and every
/// other as text.
pub(crate) fn rendering(capabilities: &Capabilities) -> Rendering {
    Rendering::new(capabilities.types.iter().copied())
}

/// Bytes: what normalizing makes of each of a batch's rows beside its values: its position.
const LINEAGE_ROW: u64 = 4;

/// Bytes: what normalizing makes of each of a merge stream's root rows beside its position: its
/// key's hash and an offset.
const KEY_ID: u64 = 36;

/// Bytes: what normalizing makes of each of a batch's rows beside its values, as a stream
/// normalized as `shape`: its position, and a merge root's key hash.
pub(crate) fn lineage_row(shape: &Shape) -> u64 {
    if shape.key.is_empty() {
        LINEAGE_ROW
    } else {
        LINEAGE_ROW + KEY_ID
    }
}

/// Bytes: what normalizing makes of each item an array holds beside the item, which it may copy:
/// its position, its parent's, its root's, its index in its array and where it sat among the
/// array's values.
pub(crate) const LINEAGE_ITEM: u64 = 20;

/// Bytes: what splitting a change row holds beside its data: its op, its position as bytes and
/// its flags, each with its offset.
pub(crate) const CHANGE_ROW: u64 = 64;

/// Bytes: what a row of a column of bytes or of text takes beside its value: its offset and its
/// validity.
const OFFSET_AND_VALIDITY: u64 = 5;

/// Bytes: what a change row's unchanged flags take once remapped over `columns` columns of its
/// table, a bit a column: as bytes, and where `text`, their hex text beside them.
pub(crate) const fn flags_bytes(columns: u64, text: bool) -> u64 {
    let width = columns.div_ceil(8);
    let bytes = width.saturating_add(OFFSET_AND_VALIDITY);
    if !text {
        return bytes;
    }
    bytes
        .saturating_add(width.saturating_mul(2))
        .saturating_add(OFFSET_AND_VALIDITY)
}

/// Times what a batch's rows expand to that normalizing them may hold: the items it copies out of
/// arrays that a null array spans between, and an array of another layout cast to a list first.
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
        Self {
            read_share: crate::budget::kept(budget.shares(), budget.readers()),
            budget,
            kept: Arc::new(AtomicU64::new(0)),
            exhausted: Mutex::new(None),
        }
    }

    /// The wait on the budget that failed the read's last event the budget was asked for.
    ///
    /// The read then failed for the budget, whatever error its source ended with. A source that
    /// carried on and had a later event admitted fails for its own reasons.
    pub(crate) fn exhausted(&self) -> Option<Exhausted> {
        *self.exhausted.lock()
    }

    /// `reservation`, an event's: a wait on the budget that failed before it no longer explains
    /// how the read ends.
    fn admitted(&self, reservation: Reservation) -> Reservation {
        *self.exhausted.lock() = None;
        reservation
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

/// The code of the refusal a source is answered with for a push beyond what pushes may take of
/// the budget, which its read then fails with as the source's own.
const PUSH_EXCEEDS_BUDGET: &str = "push_exceeds_budget";

/// The error for `name` beyond what one request may take of the budget.
fn too_large(name: &'static str, large: TooLarge) -> ConnectorError {
    ConnectorError::exceeds(LimitExceeded {
        name,
        limit: large.limit,
        actual: large.asked,
    })
}

impl Admission for Charging {
    fn admit(&self, event: &SourceEvent) -> BoxFuture<'_, rdlt_connector::Result<Option<Permit>>> {
        let (bytes, cursor) = match event {
            SourceEvent::Push(push) => (push_charge(push), false),
            // A cursor waits with its seal for a commit, which alone releases it.
            SourceEvent::Checkpoint { cursor, .. } => (count(cursor.bytes().len()), true),
            SourceEvent::Log { .. }
            | SourceEvent::Metric { .. }
            | SourceEvent::Replan
            | SourceEvent::Behind { .. } => return Box::pin(async { Ok(None) }),
        };
        Box::pin(async move {
            if cursor {
                let admitted = self.budget.acquire_cursor(bytes).await;
                let admitted = admitted.map_err(|denied| self.refused("cursor bytes", denied));
                let admitted = self.admitted(admitted?);
                return Ok(Some(Box::new(Admitted::new(bytes, admitted)) as Permit));
            }
            let admitted = self.budget.acquire(bytes).await;
            let reservation = admitted.map_err(|denied| self.refused(PUSH, denied))?;
            let reservation = self.admitted(reservation);
            Ok(Some(Box::new(Admitted::new(bytes, reservation)) as Permit))
        })
    }

    fn limits(&self) -> rdlt_wire::Limits {
        self.budget.limits()
    }

    fn reserve_frame(&self, bytes: u64) -> BoxFuture<'_, rdlt_connector::Result<Permit>> {
        Box::pin(async move {
            // A frame's wait is counted apart from pushes', which it waits among.
            let reserved = self.budget.acquire_decoding(bytes).await;
            let reservation = reserved.map_err(|denied| self.refused(PUSH, denied))?;
            let reservation = self.admitted(reservation);
            Ok(Box::new(Admitted::new(bytes, reservation)) as Permit)
        })
    }

    fn admit_reserved(
        &self,
        event: &SourceEvent,
        reserved: Permit,
    ) -> BoxFuture<'_, rdlt_connector::Result<Option<Permit>>> {
        let SourceEvent::Push(push) = event else {
            // A cursor waits with its seal in a share of its own: the frame's reservation goes.
            drop(reserved);
            return self.admit(event);
        };
        let bytes = push_charge(push);
        let admitted = match Admitted::of(reserved) {
            Some(mut admitted) if bytes <= admitted.bytes => {
                // One operation on the budget: what the frame held beyond the push goes.
                admitted.shrink(bytes);
                Ok(Some(admitted as Permit))
            }
            _ => Err(ConnectorError::new(
                ConnectorErrorKind::Internal,
                format!("a push of {bytes} bytes beyond what its frame reserved"),
            )
            .with_code(rdlt_connector::wire::MALFORMED_FRAME)),
        };
        Box::pin(async move { admitted })
    }

    fn charge(&self, bytes: u64) -> rdlt_connector::Result<Permit> {
        // What a read keeps beside its events no write releases: it has a part of a share of its
        // own, and a read that would keep more is refused where it would.
        let kept = self
            .kept
            .fetch_add(bytes, Ordering::SeqCst)
            .saturating_add(bytes);
        // Within its part, a read always finds room: the parts together are the share.
        if kept <= self.read_share
            && let Ok(reservation) = self.budget.keep(bytes)
        {
            return Ok(Box::new(Kept {
                bytes,
                total: Arc::clone(&self.kept),
                _reservation: reservation,
            }));
        }
        self.kept.fetch_sub(bytes, Ordering::SeqCst);
        Err(ConnectorError::exceeds(LimitExceeded {
            name: "read kept bytes",
            limit: self.read_share,
            actual: kept,
        }))
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
    pub(crate) fn new(bytes: u64, reservation: Reservation) -> Self {
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
