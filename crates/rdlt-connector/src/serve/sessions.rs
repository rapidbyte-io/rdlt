//! The destination sessions one connection holds open: a few, the newest.
//!
//! A session holds what its destination opened for it, files among them, and a connection's
//! share of the process's file descriptors is fixed. A host that opens a session beyond those a
//! connection holds closes its oldest, once the calls in flight on it end: a host that leaves
//! sessions of failed attempts open keeps loading, and one that opens them without end takes no
//! more than its share. A session counts from before its destination opens it until its close
//! ends, so Opens and Closes in flight count too.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::sync::Arc;

use tokio::sync::{Mutex, OwnedMutexGuard};

use crate::destination::DestinationSession;
use crate::error::{ConnectorError, ConnectorErrorKind};

/// A destination session, until its close takes it; empty once closed, or where its open failed.
pub(super) type SessionSlot = Arc<Mutex<Option<Box<dyn DestinationSession>>>>;

/// The sessions a connection holds open, by their ids, `limit` at most.
pub(super) struct Sessions {
    open: Mutex<(u64, BTreeMap<u64, SessionSlot>)>,
    limit: usize,
}

/// Room for a session its destination is opening, held until the session is, or dropped.
pub(super) struct Opening {
    id: u64,
    slot: OwnedMutexGuard<Option<Box<dyn DestinationSession>>>,
}

impl Opening {
    /// Holds `session` open in this room, and answers its id.
    pub(super) fn hold(mut self, session: Box<dyn DestinationSession>) -> u64 {
        *self.slot = Some(session);
        self.id
    }
}

impl Sessions {
    /// No session, and room for `limit`, one at least.
    pub(super) fn holding(limit: usize) -> Self {
        Self {
            open: Mutex::new((0, BTreeMap::new())),
            limit: limit.max(1),
        }
    }

    /// Room for one more session; where the connection holds as many as it may, its oldest is
    /// closed first, once the calls in flight on it end.
    pub(super) async fn open(&self) -> Opening {
        loop {
            let (id, oldest) = {
                let mut held = self.open.lock().await;
                let (last, open) = &mut *held;
                // A slot left empty, by a close or by an open that failed, holds no room; a
                // locked one is in use.
                open.retain(|_, slot| !slot.try_lock().is_ok_and(|session| session.is_none()));
                if open.len() < self.limit {
                    *last += 1;
                    let slot = SessionSlot::default();
                    let room = Arc::clone(&slot)
                        .try_lock_owned()
                        .expect("a slot no one else holds is free");
                    open.insert(*last, slot);
                    return Opening {
                        id: *last,
                        slot: room,
                    };
                }
                let (id, oldest) = open
                    .iter()
                    .next()
                    .expect("a connection holding its limit holds a session");
                (*id, Arc::clone(oldest))
            };
            // Waits for the calls in flight on the oldest; dropping its session closes what it
            // held. The slot leaves the map under its own lock: waiters passing the lock to one
            // another would otherwise each find it locked, and never see it empty.
            let mut slot = oldest.lock().await;
            drop(slot.take());
            self.open.lock().await.1.remove(&id);
            drop(slot);
        }
    }

    /// The open session `id`.
    pub(super) async fn session(&self, id: u64) -> Result<SessionSlot, ConnectorError> {
        let held = self.open.lock().await;
        held.1.get(&id).cloned().ok_or_else(|| closed(id))
    }

    /// Closes the session `id`, which holds its room until its close ends.
    pub(super) async fn close(&self, id: u64) -> Result<(), ConnectorError> {
        let slot = self.session(id).await?;
        // The slot stays locked until the close ends, so the session still counts meanwhile.
        let mut held = slot.lock().await;
        let session = held.take().ok_or_else(|| closed(id))?;
        session.close().await
    }
}

/// The error of a call on a session that is not open: closed, or never opened.
pub(super) fn closed(id: u64) -> ConnectorError {
    let message = format!("session {id} is not open");
    ConnectorError::new(ConnectorErrorKind::Internal, message).with_code("no_session")
}
