//! Where a source stands outside the engine, which certification checks moves only once the
//! engine tells the source its cursors are committed.
//!
//! The engine never asks: only certification does, through
//! [`SourceFactory::connect_acknowledging`](super::SourceFactory::connect_acknowledging).

use std::sync::Arc;

use super::Source;
use crate::cursor::Cursor;
use crate::error::Result;
use crate::id::{PartitionId, StreamName};
use crate::spec::BoxFuture;

/// What tells where a source stands for each partition outside the engine.
pub trait AcknowledgedReader: Send + Sync {
    /// See [`ReadStream::acknowledged`](super::ReadStream::acknowledged): the cursor encoded in
    /// the stream's format.
    fn acknowledged<'a>(
        &'a self,
        stream: &'a StreamName,
        partition: &'a PartitionId,
    ) -> BoxFuture<'a, Result<Option<Cursor>>>;
}

/// A connected source, as the engine drives it, and a reader of where it stands.
pub type Acknowledging = (Arc<dyn Source>, Arc<dyn AcknowledgedReader>);
