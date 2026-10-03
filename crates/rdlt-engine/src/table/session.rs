//! The destination session an attempt shares: the coordinator commits through it, and partitions
//! change tables and open writers through it.

use std::sync::Arc;

use rdlt_connector::{
    CommitMeta, ConnectorError, DestinationSession, DestinationWriter, Receipt, TableChange,
    TableRef,
};

use crate::error::{Error, ErrorKind, Side};

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
        Ok(self.apply_each(changes).await?.map_err(|(_, error)| error))
    }

    /// Applies `changes` in order, one call each, stopping at the first the destination refuses,
    /// which the refusal names by its index.
    pub(crate) async fn apply_each(
        &self,
        changes: &[TableChange],
    ) -> Result<Result<(), (usize, ConnectorError)>, Error> {
        let mut session = self.0.lock().await;
        let session = session.as_mut().ok_or_else(closed)?;
        for (index, change) in changes.iter().enumerate() {
            if let Err(error) = session.apply_schema(change).await {
                return Ok(Err((index, error)));
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

    /// Commits `meta`; a receipt the destination answers with is the commit's own, as
    /// [`answered`] checks.
    pub(crate) async fn commit(
        &self,
        meta: &CommitMeta,
    ) -> Result<rdlt_connector::Result<Receipt>, Error> {
        let mut session = self.0.lock().await;
        match session.as_mut().ok_or_else(closed)?.commit(meta).await {
            Ok(receipt) => answered(meta, receipt).map(Ok),
            Err(error) => Ok(Err(error)),
        }
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

/// `receipt`, where it is the receipt of the commit `meta` describes.
///
/// # Errors
///
/// A receipt of another load or sequence is `receipt_mismatch`, a Destination error that no
/// retry mends: the write-ahead log would settle a commit the destination never made, and
/// leave pending the commit it made.
pub(crate) fn answered(meta: &CommitMeta, receipt: Receipt) -> Result<Receipt, Error> {
    if receipt.answers(meta) {
        return Ok(receipt);
    }
    Err(Error::new(
        ErrorKind::Destination,
        format!(
            "the destination answered commit {} of load {} with the receipt of commit {} of \
             load {}",
            meta.commit_seq.get(),
            meta.load_id,
            receipt.commit_seq.get(),
            receipt.load_id
        ),
    )
    .with_code("receipt_mismatch"))
}

fn closed() -> Error {
    Error::internal("the destination session is already closed")
}

impl std::fmt::Debug for SharedSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedSession").finish_non_exhaustive()
    }
}
