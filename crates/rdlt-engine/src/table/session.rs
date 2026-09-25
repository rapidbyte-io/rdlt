//! The destination session an attempt shares: the coordinator commits through it, and partitions
//! change tables and open writers through it.

use std::sync::Arc;

use rdlt_connector::{
    CommitMeta, DestinationSession, DestinationWriter, Receipt, TableChange, TableRef,
};

use crate::error::{Error, Side};

/// The destination session, shared by the coordinator's commits and the partitions' schema
/// changes until the coordinator closes it.
pub(crate) struct SharedSession(tokio::sync::Mutex<Option<Box<dyn DestinationSession>>>);

impl SharedSession {
    pub(crate) fn new(session: Box<dyn DestinationSession>) -> Arc<Self> {
        Arc::new(Self(tokio::sync::Mutex::new(Some(session))))
    }

    /// Applies `changes` in order, stopping at the first the destination refuses.
    ///
    /// Each call returns the destination's own result inside the error for a closed session.
    pub(crate) async fn apply_schema(
        &self,
        changes: &[TableChange],
    ) -> Result<rdlt_connector::Result<()>, Error> {
        let mut session = self.0.lock().await;
        let session = session.as_mut().ok_or_else(closed)?;
        for change in changes {
            if let Err(error) = session.apply_schema(change).await {
                return Ok(Err(error));
            }
        }
        Ok(Ok(()))
    }

    /// A writer for `table`.
    pub(crate) async fn writer(
        &self,
        table: &TableRef,
    ) -> Result<rdlt_connector::Result<Box<dyn DestinationWriter>>, Error> {
        let mut session = self.0.lock().await;
        Ok(session.as_mut().ok_or_else(closed)?.writer(table).await)
    }

    /// Commits `meta`.
    pub(crate) async fn commit(
        &self,
        meta: &CommitMeta,
    ) -> Result<rdlt_connector::Result<Receipt>, Error> {
        let mut session = self.0.lock().await;
        Ok(session.as_mut().ok_or_else(closed)?.commit(meta).await)
    }

    /// Closes the session; later calls find it closed.
    pub(crate) async fn close(&self) -> Result<(), Error> {
        let Some(session) = self.0.lock().await.take() else {
            return Ok(());
        };
        session
            .close()
            .await
            .map_err(|error| Error::connector(Side::Destination, "closing the session", error))
    }
}

fn closed() -> Error {
    Error::internal("the destination session is already closed")
}

impl std::fmt::Debug for SharedSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedSession").finish_non_exhaustive()
    }
}
