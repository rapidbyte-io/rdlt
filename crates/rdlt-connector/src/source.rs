//! Source connectors: the traits authors implement and the engine-facing form the SDK builds.

mod adapter;
#[cfg(test)]
mod tests;

use std::future::Future;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::catalog::{Catalog, DuplicateStream, StreamSpec};
use crate::cursor::Cursor;
use crate::emitter::Emitter;
use crate::error::{ConnectorError, ConnectorErrorKind, Result};
use crate::id::{PartitionId, StreamName};
use crate::sink::PartitionSink;
use crate::spec::{BoxFuture, ConnectContext};
use crate::state::StreamState;

pub use adapter::source_factory;

/// A source connector, as its author writes it.
///
/// The `#[source(id = "...")]` attribute fills in `ID` and `VERSION`.
pub trait SourceConnector: Sized + Send + Sync + 'static {
    /// The connector's id, such as `io.example.tickets`.
    const ID: &'static str;
    /// The connector's version.
    const VERSION: &'static str;
    /// The configuration the connector accepts; its JSON Schema is published.
    type Config: DeserializeOwned + schemars::JsonSchema + Send;

    /// Builds the connector's clients from its configuration.
    fn connect(
        config: Self::Config,
        context: &ConnectContext,
    ) -> impl Future<Output = Result<Self>> + Send;

    /// Verifies connectivity and permissions; must succeed exactly when reading can.
    fn check(&self) -> impl Future<Output = Result<()>> + Send;

    /// The streams the connector reads.
    fn streams(&self) -> Streams<Self>;

    /// The catalog; by default, the specs of [`SourceConnector::streams`].
    fn discover(&self) -> impl Future<Output = Result<Catalog>> + Send {
        let catalog = self
            .streams()
            .catalog()
            .map_err(|error| duplicate_stream(&error));
        async move { catalog }
    }
}

/// One stream of a source, with its own cursor type.
pub trait ReadStream<S: SourceConnector>: Send + Sync + 'static {
    /// The stream's resume position; `Default` is the start.
    type Cursor: Serialize + DeserializeOwned + Default + Send + Sync + 'static;

    /// The cursor's format version; bump it when the cursor's shape changes incompatibly.
    const CURSOR_VERSION: u16 = 1;

    /// The stream's name and read properties.
    fn spec(&self) -> StreamSpec;

    /// The partitions to read this run; by default one partition.
    fn partitions(
        &self,
        _source: &S,
        _state: &StreamState,
    ) -> impl Future<Output = Result<Vec<Partition>>> + Send {
        async { Ok(vec![Partition::single()]) }
    }

    /// Reads one partition from `cursor`, pushing data and checkpoints to `out`.
    fn read(
        &self,
        source: &S,
        partition: &Partition,
        cursor: Self::Cursor,
        out: &mut Emitter<Self::Cursor>,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Called once `cursors` are committed; sources that acknowledge upstream (CDC) do it here.
    fn committed(
        &self,
        _source: &S,
        _cursors: &[(PartitionId, Self::Cursor)],
    ) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }
}

/// A slice of a stream that is read, and checkpointed, on its own.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Partition {
    id: PartitionId,
}

impl Partition {
    /// A partition with `id`; the source gives the id its meaning.
    pub fn new(id: PartitionId) -> Self {
        Self { id }
    }

    /// The partition of a stream that is not split, with id `whole`.
    pub fn single() -> Self {
        Self::new(PartitionId::whole())
    }

    /// The partition's id.
    pub fn id(&self) -> &PartitionId {
        &self.id
    }
}

/// The streams of a source; built by [`SourceConnector::streams`].
pub struct Streams<S> {
    streams: Vec<Box<dyn adapter::ErasedStream<S>>>,
}

impl<S: SourceConnector> Streams<S> {
    /// No streams.
    pub fn new() -> Self {
        Self {
            streams: Vec::new(),
        }
    }

    /// Adds `stream`.
    #[must_use]
    pub fn with<R: ReadStream<S>>(mut self, stream: R) -> Self {
        self.streams.push(Box::new(stream));
        self
    }

    /// The catalog of the streams' specs.
    pub fn catalog(&self) -> Result<Catalog, DuplicateStream> {
        Catalog::new(self.streams.iter().map(|stream| stream.spec()).collect())
    }
}

impl<S: SourceConnector> Default for Streams<S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> std::fmt::Debug for Streams<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Streams")
            .field("count", &self.streams.len())
            .finish()
    }
}

/// What the engine asks one partition read for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadRequest {
    /// The stream.
    pub stream: StreamName,
    /// The partition.
    pub partition: Partition,
    /// Where to resume; `None` reads from the start.
    pub cursor: Option<Cursor>,
}

/// A connected source, as the engine drives it; built by [`source_factory`].
pub trait Source: Send + Sync {
    /// Verifies connectivity and permissions.
    fn check(&self) -> BoxFuture<'_, Result<()>>;

    /// The catalog.
    fn discover(&self) -> BoxFuture<'_, Result<Catalog>>;

    /// The partitions of `stream` to read, given its committed state.
    fn plan<'a>(
        &'a self,
        stream: &'a StreamName,
        state: &'a StreamState,
    ) -> BoxFuture<'a, Result<Vec<Partition>>>;

    /// Reads one partition into `sink` until it is exhausted or stopped.
    fn read(&self, request: ReadRequest, sink: PartitionSink) -> BoxFuture<'_, Result<()>>;

    /// Reports that `cursors` of `stream` are committed.
    fn committed<'a>(
        &'a self,
        stream: &'a StreamName,
        cursors: &'a [(PartitionId, Cursor)],
    ) -> BoxFuture<'a, Result<()>>;
}

/// Creates connected sources from JSON configuration.
pub trait SourceFactory: Send + Sync {
    /// The connector's identity and configuration schema.
    fn spec(&self) -> &crate::spec::ConnectorSpec;

    /// Validates `config` and connects.
    fn connect(
        &self,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> BoxFuture<'_, Result<Box<dyn Source>>>;
}

fn duplicate_stream(error: &DuplicateStream) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Internal, error.to_string())
}
