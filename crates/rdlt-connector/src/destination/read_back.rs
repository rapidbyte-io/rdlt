//! Reading back what a destination published, which certification compares with what was
//! committed.
//!
//! The engine never reads it back: only certification does, through
//! [`DestinationFactory::connect_reading`](super::DestinationFactory::connect_reading).

use arrow_array::RecordBatch;
use std::future::Future;
use std::sync::Arc;

use super::{Destination, DestinationConnector, TableRef};
use crate::error::Result;
use crate::spec::BoxFuture;

/// A destination connector that can read back what it published: every row of a table that a
/// commit published, and nothing staged.
///
/// Implementing it, and serving the connector through
/// [`readable_destination_factory`](super::readable_destination_factory), lets certification
/// check the clauses that compare what was published with what was committed.
pub trait ReadBack: DestinationConnector {
    /// Every published row of `table`, as batches.
    fn published(&self, table: &TableRef) -> impl Future<Output = Result<Vec<RecordBatch>>> + Send;
}

/// The object-safe form of [`ReadBack`].
pub trait PublishedReader: Send + Sync {
    /// See [`ReadBack::published`].
    fn published<'a>(&'a self, table: &'a TableRef) -> BoxFuture<'a, Result<Vec<RecordBatch>>>;
}

/// A connected destination, as the engine drives it, and a reader of what it published.
pub type Reading = (Arc<dyn Destination>, Arc<dyn PublishedReader>);
