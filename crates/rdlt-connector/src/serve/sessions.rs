//! The destination sessions one connection holds open: a few, the newest.
//!
//! A session holds what its destination opened for it, files among them, and a connection's
//! share of the process's file descriptors is fixed. A host that opens a session beyond those a
//! connection holds closes its oldest: a host that leaves sessions of failed attempts open keeps
//! loading, and one that opens them without end takes no more than its share.

use std::collections::BTreeMap;
use std::sync::Arc;

use rdlt_wire::tonic::Status;
use tokio::sync::Mutex;

use crate::destination::DestinationSession;
use crate::error::{ConnectorError, ConnectorErrorKind};
use crate::wire::status;

/// A destination session, until its close takes it.
pub(super) type SessionSlot = Arc<Mutex<Option<Box<dyn DestinationSession>>>>;

/// The sessions a connection holds open, by their ids, `limit` at most.
pub(super) struct Sessions {
    open: Mutex<(u64, BTreeMap<u64, SessionSlot>)>,
    limit: usize,
}

impl Sessions {
    /// No session, and room for `limit`, one at least.
    pub(super) fn holding(limit: usize) -> Self {
        Self {
            open: Mutex::new((0, BTreeMap::new())),
            limit: limit.max(1),
        }
    }

    /// Holds `session` open and answers its id; where the connection held as many as it may,
    /// its oldest is dropped first, which closes what it held.
    pub(super) async fn open(&self, session: Box<dyn DestinationSession>) -> u64 {
        let mut held = self.open.lock().await;
        let (last, open) = &mut *held;
        while open.len() >= self.limit {
            open.pop_first();
        }
        *last += 1;
        open.insert(*last, Arc::new(Mutex::new(Some(session))));
        *last
    }

    /// The open session `id`.
    pub(super) async fn session(&self, id: u64) -> Result<SessionSlot, Status> {
        let held = self.open.lock().await;
        held.1.get(&id).cloned().ok_or_else(|| closed(id))
    }

    /// Takes the session `id`, which is open no more.
    pub(super) async fn take(&self, id: u64) -> Result<Box<dyn DestinationSession>, Status> {
        let slot = self.open.lock().await.1.remove(&id);
        let slot = slot.ok_or_else(|| closed(id))?;
        let session = slot.lock().await.take();
        session.ok_or_else(|| closed(id))
    }
}

/// The status of a call on a session that is not open: closed, or never opened.
pub(super) fn closed(id: u64) -> Status {
    let message = format!("session {id} is not open");
    status(&ConnectorError::new(ConnectorErrorKind::Internal, message).with_code("no_session"))
}
