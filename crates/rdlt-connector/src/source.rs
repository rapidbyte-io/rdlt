//! Source connectors: the traits authors implement and the engine-facing form the SDK builds.

mod acknowledged;
mod adapter;
#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
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

pub use acknowledged::{AcknowledgedReader, Acknowledging};
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

    /// Whether the connector's streams tell where they stand outside the engine
    /// ([`ReadStream::acknowledged`]), which certification checks moves only in
    /// [`ReadStream::committed`]; `#[source(..., acknowledged)]` sets it.
    ///
    /// Certification tells such a source that a few checkpoints are committed, so it runs against
    /// a slot or consumer group of its own.
    const ACKNOWLEDGES: bool = false;

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

    /// The phase and partitions to read this run: by default the stream's current phase, and
    /// [`ReadStream::partitions`].
    ///
    /// A stream read in phases, as a CDC snapshot and then its changes, overrides this: once
    /// every partition of a phase is done, the next plan names the next phase and its partitions.
    /// Where a phase's partitions start comes from `state` alone, never from what the source
    /// acknowledged: a load that begins the phase again begins it where a failed one did, whose
    /// logged rows must still land.
    fn plan(
        &self,
        source: &S,
        state: &StreamState,
    ) -> impl Future<Output = Result<PartitionPlan>> + Send {
        async move {
            self.partitions(source, state)
                .await
                .map(PartitionPlan::from)
        }
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

    /// Where the stream stands for `partition` outside the engine: the cursor it was last told
    /// is committed, as it keeps it beyond a connection (a replication slot's confirmed position,
    /// a consumer group's committed offset); none where it keeps none, or was never told.
    ///
    /// Only certification asks, where the connector says it tells
    /// ([`SourceConnector::ACKNOWLEDGES`]), to check that the position moves only in
    /// [`committed`](Self::committed), and asks from a connection of its own. A stream that tells
    /// answers the cursor `committed` was told as soon as that call returns: one that
    /// acknowledges in the background waits there until the position has moved.
    fn acknowledged(
        &self,
        _source: &S,
        _partition: &PartitionId,
    ) -> impl Future<Output = Result<Option<Self::Cursor>>> + Send {
        async { Ok(None) }
    }
}

/// A slice of a stream that is read, and checkpointed, on its own.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Partition {
    id: PartitionId,
    unbounded: bool,
}

impl Partition {
    /// A partition with `id`; the source gives the id its meaning.
    pub fn new(id: PartitionId) -> Self {
        Self {
            id,
            unbounded: false,
        }
    }

    /// The partition, as one that never ends, as a change stream's changes or a log's records
    /// do: a read of it that ends only pauses it.
    ///
    /// Such a partition is never done: its next read resumes from its last checkpoint, so rows a
    /// read pushed after that checkpoint are read again, not committed without a position.
    #[must_use]
    pub fn unbounded(mut self) -> Self {
        self.unbounded = true;
        self
    }

    /// Whether the partition never ends.
    pub fn is_unbounded(&self) -> bool {
        self.unbounded
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

/// The partitions a source plans for a stream, and the phase they belong to.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PartitionPlan {
    /// The stream's phase the partitions belong to; `None` keeps the phase state records.
    ///
    /// A phase other than the recorded one begins the phase: the commit that first records it
    /// forgets the previous phase's partitions.
    pub phase: Option<u16>,
    /// The partitions, with distinct ids.
    pub partitions: Vec<Partition>,
    /// Where partitions of a new phase start, as a CDC stream's changes start from the position
    /// its snapshot captured.
    ///
    /// A partition without a start starts from the beginning. A plan in the recorded phase
    /// resumes each partition from its committed position instead.
    pub starts: BTreeMap<PartitionId, Cursor>,
}

impl PartitionPlan {
    /// `partitions`, in the phase state records.
    pub fn new(partitions: Vec<Partition>) -> Self {
        Self {
            phase: None,
            partitions,
            starts: BTreeMap::new(),
        }
    }

    /// The plan with its partitions in `phase`.
    #[must_use]
    pub fn phase(mut self, phase: u16) -> Self {
        self.phase = Some(phase);
        self
    }

    /// The plan with `partition`, of a new phase, starting at `cursor`.
    #[must_use]
    pub fn start(mut self, partition: PartitionId, cursor: Cursor) -> Self {
        self.starts.insert(partition, cursor);
        self
    }
}

impl From<Vec<Partition>> for PartitionPlan {
    fn from(partitions: Vec<Partition>) -> Self {
        Self::new(partitions)
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
#[non_exhaustive]
pub struct ReadRequest {
    /// The stream.
    pub stream: StreamName,
    /// The partition.
    pub partition: Partition,
    /// Where to resume; `None` reads from the start.
    pub cursor: Option<Cursor>,
    /// Whether a read of an unbounded partition follows it once caught up, waiting for more
    /// until asked to stop; otherwise it returns once caught up to where the source stood when
    /// the read started.
    ///
    /// A bounded partition's read ignores it.
    pub follow: bool,
}

impl ReadRequest {
    /// A read of `partition` of `stream` from `cursor`, which returns once caught up.
    pub fn new(stream: StreamName, partition: Partition, cursor: Option<Cursor>) -> Self {
        Self {
            stream,
            partition,
            cursor,
            follow: false,
        }
    }

    /// This read, following an unbounded partition where `follow` says so.
    #[must_use]
    pub fn following(mut self, follow: bool) -> Self {
        self.follow = follow;
        self
    }
}

/// A connected source, as the engine drives it; built by [`source_factory`].
pub trait Source: Send + Sync {
    /// Verifies connectivity and permissions.
    fn check(&self) -> BoxFuture<'_, Result<()>>;

    /// The catalog.
    fn discover(&self) -> BoxFuture<'_, Result<Catalog>>;

    /// The phase and partitions of `stream` to read, given its committed state.
    fn plan<'a>(
        &'a self,
        stream: &'a StreamName,
        state: &'a StreamState,
    ) -> BoxFuture<'a, Result<PartitionPlan>>;

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

    /// Whether the source tells where it stands outside the engine, for certification.
    fn acknowledges(&self) -> bool {
        false
    }

    /// Validates `config` and connects, with a reader of where the source stands outside the
    /// engine.
    ///
    /// # Errors
    ///
    /// An unsupported error when the source does not tell where it stands.
    fn connect_acknowledging(
        &self,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> BoxFuture<'_, Result<Acknowledging>> {
        drop((config, context));
        Box::pin(async {
            Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "this source does not tell where it stands outside the engine",
            )
            .with_code(ACKNOWLEDGED_CODE))
        })
    }
}

/// The code of the error a source refuses to tell where it stands with, where it does not.
pub const ACKNOWLEDGED_CODE: &str = "acknowledged";

fn duplicate_stream(error: &DuplicateStream) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Internal, error.to_string())
}
