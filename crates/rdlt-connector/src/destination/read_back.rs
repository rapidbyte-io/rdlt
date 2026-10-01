//! Reading back what a destination published, which certification compares with what was
//! committed.
//!
//! The engine never reads it back, and a connector's own binary cannot: the trait, the factory
//! and the protocol's call exist only with the `certify` feature, and are served only by a
//! binary whose `main` builds [`readable_destination_factory`](super::readable_destination_factory).

#[cfg(test)]
mod tests;

use arrow_array::RecordBatch;
use std::future::Future;
use std::sync::Arc;

use tokio::sync::mpsc;

use super::{Destination, DestinationConnector, TableRef};
use crate::error::{ConnectorError, ConnectorErrorKind, Result};
use crate::spec::BoxFuture;

/// A destination connector that can read back what it published: every row of a table that a
/// commit published, and nothing staged.
///
/// Implementing it, and serving the connector through
/// [`readable_destination_factory`](super::readable_destination_factory), lets certification
/// check the clauses that compare what was published with what was committed.
pub trait ReadBack: DestinationConnector {
    /// Sends every published row of `table` to `rows`, as batches, and returns once all are sent.
    ///
    /// Each send waits until its reader has taken the batch before, so a table is read a batch
    /// at a time, however large it is.
    fn published(
        &self,
        table: &TableRef,
        rows: PublishedRows,
    ) -> impl Future<Output = Result<()>> + Send;
}

/// The object-safe form of [`ReadBack`].
pub trait PublishedReader: Send + Sync {
    /// See [`ReadBack::published`].
    fn published<'a>(
        &'a self,
        table: &'a TableRef,
        rows: PublishedRows,
    ) -> BoxFuture<'a, Result<()>>;
}

/// Where a destination sends the rows it reads back, a batch at a time.
#[derive(Clone, Debug)]
pub struct PublishedRows(mpsc::Sender<RecordBatch>);

impl PublishedRows {
    /// The sending end, and what receives its batches, holding one batch at most between them.
    pub fn channel() -> (Self, mpsc::Receiver<RecordBatch>) {
        let (rows, batches) = mpsc::channel(1);
        (Self(rows), batches)
    }

    /// Sends `batch`, once its reader has taken the batch before.
    ///
    /// # Errors
    ///
    /// A stopped error once the reader has gone: the read-back ends.
    pub async fn send(&self, batch: RecordBatch) -> Result<()> {
        self.0.send(batch).await.map_err(|_| gone())
    }

    /// Sends `batch` as [`send`](Self::send) does, from a thread that may block.
    ///
    /// # Errors
    ///
    /// A stopped error once the reader has gone: the read-back ends.
    ///
    /// # Panics
    ///
    /// Called from a task of the runtime, which it would block.
    pub fn blocking_send(&self, batch: RecordBatch) -> Result<()> {
        self.0.blocking_send(batch).map_err(|_| gone())
    }

    /// Every published row of `table` that `reader` reads back, gathered: for a table known to be
    /// small, as those certification writes are.
    ///
    /// # Errors
    ///
    /// The error the read-back failed with.
    pub async fn gather(
        reader: &dyn PublishedReader,
        table: &TableRef,
    ) -> Result<Vec<RecordBatch>> {
        let (rows, mut batches) = Self::channel();
        let reading = reader.published(table, rows);
        let gathering = async {
            let mut gathered = Vec::new();
            while let Some(batch) = batches.recv().await {
                gathered.push(batch);
            }
            gathered
        };
        let (read, gathered) = tokio::join!(reading, gathering);
        read.map(|()| gathered)
    }
}

fn gone() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Stopped,
        "what read the table back has gone",
    )
}

/// A connected destination, as the engine drives it, and a reader of what it published.
pub type Reading = (Arc<dyn Destination>, Arc<dyn PublishedReader>);
