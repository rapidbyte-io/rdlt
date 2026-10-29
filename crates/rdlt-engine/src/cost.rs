//! What the engine charges its budget as a partition reads: each push by what it keeps alive, a
//! checkpoint by its cursor's bytes, and what the read keeps beside its events.

#[cfg(test)]
pub(crate) mod tests;

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::RecordBatch;
use parking_lot::Mutex;
use rdlt_connector::cost::{Rendering, ViewBuffers, listed_bytes, push_charge};
use rdlt_connector::{
    Admission, BoxFuture, Capabilities, ConnectorError, ConnectorErrorKind, LimitExceeded, Permit,
    SourceEvent,
};
use rdlt_wire::limits::count;

use crate::budget::{Denied, Exhausted, MemoryBudget, Reservation, TooLarge};
use crate::normalize::Shape;
use crate::partition::Parts;

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

/// Copies of a view array's list of its data buffers that lowering a part of it holds at once:
/// the batch its parts are joined into, and Arrow's conversion of that, which copies the list into
/// a vector it grows to twice its length and then into a list of its own.
const VIEW_LISTS: u64 = 4;

/// Bytes: what lowering a part of `batch` holds of its views' lists of their data buffers, for
/// each buffer beyond the first of each view array: the part's arrays hold those of the first.
pub(crate) fn view_lists(batch: &RecordBatch) -> u64 {
    let beyond = ViewBuffers::of(batch).beyond_first;
    listed_bytes(beyond.saturating_mul(VIEW_LISTS))
}

/// Times what a batch's rows expand to that normalizing them may hold: the items it copies out of
/// arrays that a null array spans between, and an array of another layout cast to a list first.
pub(crate) const SPLIT_COPIES: u64 = 2;

/// Where a read keeps what it keeps beside its events.
pub(crate) enum Keeping {
    /// In the part of what reads keep it holds while it reads: a read that is not followed.
    Part,
    /// In the parts no other read holds, which followed unbounded reads draw on together.
    Pooled(Arc<Parts>),
}

/// Admits one partition's events into its channel: a push by what it keeps alive, a checkpoint
/// by its cursor's bytes, which wait with its seal for a commit, and what the read keeps beside
/// its events up to a part of the share reads have.
pub(crate) struct Charging {
    budget: MemoryBudget,
    /// Bytes: the most the read may keep beside its events.
    read_share: u64,
    /// Bytes the read keeps now.
    kept: Arc<AtomicU64>,
    keeping: Keeping,
    /// The wait on the budget that failed an admission, where one did.
    exhausted: Mutex<Option<Exhausted>>,
}

impl Charging {
    /// An admission charging `budget` for the events of one of its reads, which keeps what it
    /// keeps beside them as `keeping` says.
    pub(crate) fn new(budget: MemoryBudget, keeping: Keeping) -> Self {
        Self {
            read_share: budget.part(),
            budget,
            kept: Arc::new(AtomicU64::new(0)),
            keeping,
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

    fn charge(&self, bytes: u64) -> rdlt_connector::Result<Permit> {
        let refused = |name, limit, actual| {
            ConnectorError::exceeds(LimitExceeded {
                name,
                limit,
                actual,
            })
        };
        // What a read keeps beside its events no write releases: a read that is not followed
        // holds a part of a share of its own, and a read that would keep more is refused where it
        // would.
        let kept = self
            .kept
            .fetch_add(bytes, Ordering::SeqCst)
            .saturating_add(bytes);
        if kept > self.read_share {
            self.kept.fetch_sub(bytes, Ordering::SeqCst);
            return Err(refused(READ_KEPT, self.read_share, kept));
        }
        let pooled = match &self.keeping {
            Keeping::Part => None,
            Keeping::Pooled(parts) => Some(parts),
        };
        if let Some(parts) = pooled
            && let Err(full) = parts.draw(bytes)
        {
            self.kept.fetch_sub(bytes, Ordering::SeqCst);
            return Err(refused("followed reads kept bytes", full.room, full.kept));
        }
        // The parts together are the share, so a read within its part, or within the parts
        // followed reads drew, always finds room.
        let Ok(reservation) = self.budget.keep(bytes) else {
            self.kept.fetch_sub(bytes, Ordering::SeqCst);
            if let Some(parts) = pooled {
                parts.give_back(bytes);
            }
            return Err(refused(READ_KEPT, self.read_share, kept));
        };
        Ok(Box::new(Kept {
            bytes,
            total: Arc::clone(&self.kept),
            pooled: pooled.map(Arc::clone),
            reservation,
        }))
    }
}

/// The name of the limit what one read keeps beyond its part passes.
const READ_KEPT: &str = "read kept bytes";

/// What a read keeps beside its events, released when dropped.
struct Kept {
    bytes: u64,
    total: Arc<AtomicU64>,
    /// The parts followed reads draw on, where the read is one of them.
    pooled: Option<Arc<Parts>>,
    reservation: Reservation,
}

impl Drop for Kept {
    fn drop(&mut self) {
        self.total.fetch_sub(self.bytes, Ordering::SeqCst);
        // The bytes go back before the part does, so a read that draws the part finds them.
        self.reservation.shrink(0);
        if let Some(parts) = &self.pooled {
            parts.give_back(self.bytes);
        }
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
