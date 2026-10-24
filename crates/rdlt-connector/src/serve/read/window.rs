//! What a served read's channel holds: the pushes its source sends ahead keep alive no more than
//! the host's frame limit together, so a host slow to take them holds the read to a frame's worth.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use rdlt_wire::Limits;
use tokio::sync::Semaphore;

use crate::cost::push_charge;
use crate::error::Result;
use crate::sink::{Admission, Permit, SourceEvent};
use crate::spec::BoxFuture;

/// Admits a served read's pushes into its channel by what each keeps alive, within a window of
/// the host's frame limit; every other event passes free.
///
/// A push takes the lesser of its charge and the window, so the pushes waiting for the host keep
/// alive a frame's worth together, or one larger push alone: it waits until no push holds any of
/// the window. A checkpoint's cursor is bounded by the host's cursor limit, and the host charges
/// it as it arrives.
pub(super) struct Window {
    room: Arc<Semaphore>,
    /// Bytes: the window, the host's frame limit or the most a semaphore's request takes.
    bytes: u32,
    /// The host's limits, which what the read sends keeps within.
    host: Limits,
}

impl Window {
    /// The window of a read whose host's limits are `host`.
    pub(super) fn new(host: Limits) -> Self {
        let bytes = u32::try_from(host.frame_bytes).unwrap_or(u32::MAX);
        Self {
            room: Arc::new(Semaphore::new(bytes as usize)),
            bytes,
            host,
        }
    }
}

impl Admission for Window {
    fn admit(&self, event: &SourceEvent) -> BoxFuture<'_, Result<Option<Permit>>> {
        let SourceEvent::Push(push) = event else {
            return Box::pin(async { Ok(None) });
        };
        let wanted =
            u32::try_from(push_charge(push)).map_or(self.bytes, |charge| charge.min(self.bytes));
        let room = Arc::clone(&self.room);
        Box::pin(async move {
            let taken = room
                .acquire_many_owned(wanted)
                .await
                .expect("a window's semaphore is never closed");
            Ok(Some(Box::new(taken) as Permit))
        })
    }

    /// Charges nothing: what a read keeps beside its events is its connector's process's to bound.
    fn charge(&self, _: u64) -> Result<Permit> {
        Ok(Box::new(()))
    }

    fn limits(&self) -> Limits {
        self.host
    }
}
