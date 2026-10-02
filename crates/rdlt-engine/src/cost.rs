//! What the engine charges its budget: each event a partition reads by what it holds and what
//! it becomes, as the cost model measures it for the attempt's destination.

#[cfg(test)]
mod tests;

use std::fmt;
use std::sync::Arc;

use rdlt_connector::cost::Rendering;
use rdlt_connector::{
    Admission, BoxFuture, Capabilities, ConnectorError, ConnectorErrorKind, Permit, SourceEvent,
};

use crate::budget::{Exhausted, MemoryBudget, Reservation};
use crate::limits::BUDGET_WAIT_EXCEEDED;

/// How `capabilities`' destination renders values: the kinds it stores as they are, and every
/// other as text.
pub(crate) fn rendering(capabilities: &Capabilities) -> Rendering {
    Rendering::new(capabilities.types.iter().copied())
}

/// Admits a partition's events into its channel: a push by what it costs, a checkpoint by its
/// cursor's bytes, which wait with its seal for a commit.
pub(crate) struct Charging {
    budget: MemoryBudget,
    rendering: Arc<Rendering>,
}

impl Charging {
    /// An admission charging `budget` for events as `rendering` costs them.
    pub(crate) fn new(budget: MemoryBudget, rendering: Arc<Rendering>) -> Self {
        Self { budget, rendering }
    }

    /// The bytes `event` is charged; `None` for an event that holds nothing.
    fn cost(&self, event: &SourceEvent) -> Option<u64> {
        let count = |bytes: usize| u64::try_from(bytes).unwrap_or(u64::MAX);
        match event {
            SourceEvent::Push(push) => Some(self.rendering.charge(push, self.budget.capacity())),
            SourceEvent::Checkpoint { cursor, .. } => Some(count(cursor.bytes().len())),
            SourceEvent::Log { .. }
            | SourceEvent::Metric { .. }
            | SourceEvent::Replan
            | SourceEvent::Behind { .. } => None,
        }
    }
}

impl Admission for Charging {
    fn admit<'a>(
        &'a self,
        event: &'a SourceEvent,
    ) -> BoxFuture<'a, rdlt_connector::Result<Option<Permit>>> {
        Box::pin(async move {
            let Some(cost) = self.cost(event) else {
                return Ok(None);
            };
            // What the budget reserves: a request beyond it takes the whole of it.
            let bytes = cost.min(self.budget.capacity());
            // A cursor waits with its seal for a commit, which alone releases it.
            let reservation = match event {
                SourceEvent::Checkpoint { .. } => self.budget.acquire_kept(bytes).await,
                _ => self.budget.acquire(bytes).await,
            };
            let reservation = reservation.map_err(|exhausted| refused(&exhausted))?;
            Ok(Some(Box::new(Admitted { bytes, reservation }) as Permit))
        })
    }

    fn charge(&self, bytes: u64) -> Permit {
        // What a read keeps beside its events, no write releases.
        Box::new(self.budget.keep(bytes))
    }
}

/// The error an event is refused with once it waited on the budget until its deadline: transient,
/// and coded so the read's failure is known for the budget's.
fn refused(exhausted: &Exhausted) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Transient, exhausted.to_string())
        .with_code(BUDGET_WAIT_EXCEEDED)
}

/// What admitted an event: the bytes charged for it, held until this is dropped.
pub(crate) struct Admitted {
    /// The bytes the event was charged.
    pub(crate) bytes: u64,
    reservation: Reservation,
}

impl Admitted {
    /// What `permit` admitted, where an engine's admission issued it.
    pub(crate) fn of(permit: Permit) -> Option<Box<Self>> {
        permit.downcast().ok()
    }

    /// Says the bytes are queued for a write, which releases them.
    pub(crate) fn stage(&mut self) {
        self.reservation.stage();
    }

    /// Holds `bytes` from now, fewer than were admitted: the rest is released.
    pub(crate) fn keep(&mut self, bytes: u64) {
        let bytes = bytes.min(self.bytes);
        self.reservation.resize(bytes);
        self.bytes = bytes;
    }
}

impl fmt::Debug for Admitted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Admitted").field(&self.bytes).finish()
    }
}
