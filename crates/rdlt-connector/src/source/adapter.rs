//! Turns an author's [`SourceConnector`] into the engine-facing [`Source`].

use std::collections::BTreeMap;
use std::marker::PhantomData;

use super::{PartitionPlan, ReadRequest, ReadStream, Source, SourceConnector, SourceFactory};
use crate::catalog::{Catalog, InvalidCatalog, StreamSpec};
use crate::config;
use crate::cursor::Cursor;
use crate::emitter::Emitter;
use crate::error::{ConnectorError, ConnectorErrorKind, Result};
use crate::id::{ConnectorId, PartitionId, StreamName};
use crate::sink::PartitionSink;
use crate::spec::{BoxFuture, ConnectContext, ConnectorSpec};
use crate::state::StreamState;

/// A [`ReadStream`] with its cursor type erased, so streams of one source fit in one map by name.
///
/// Its cursors are read and written in the format `version` its source's adapter gives it.
pub(crate) trait ErasedStream<S>: Send + Sync {
    fn spec(&self) -> StreamSpec;

    /// The format of its cursors its type says, [`ReadStream::CURSOR_VERSION`].
    fn cursor_version(&self) -> u16;

    fn plan<'a>(
        &'a self,
        source: &'a S,
        state: &'a StreamState,
    ) -> BoxFuture<'a, Result<PartitionPlan>>;

    /// Reads `request`'s partition, from its cursor, into `sink`.
    fn read<'a>(
        &'a self,
        source: &'a S,
        request: ReadRequest,
        sink: PartitionSink,
        version: u16,
    ) -> BoxFuture<'a, Result<()>>;

    fn committed<'a>(
        &'a self,
        source: &'a S,
        cursors: &'a [(PartitionId, Cursor)],
        version: u16,
    ) -> BoxFuture<'a, Result<()>>;

    #[cfg(feature = "certify")]
    fn acknowledged<'a>(
        &'a self,
        source: &'a S,
        partition: &'a PartitionId,
        version: u16,
    ) -> BoxFuture<'a, Result<Option<Cursor>>>;
}

impl<S: SourceConnector, R: ReadStream<S>> ErasedStream<S> for R {
    fn spec(&self) -> StreamSpec {
        ReadStream::spec(self)
    }

    fn cursor_version(&self) -> u16 {
        R::CURSOR_VERSION
    }

    fn plan<'a>(
        &'a self,
        source: &'a S,
        state: &'a StreamState,
    ) -> BoxFuture<'a, Result<PartitionPlan>> {
        Box::pin(ReadStream::plan(self, source, state))
    }

    fn read<'a>(
        &'a self,
        source: &'a S,
        request: ReadRequest,
        sink: PartitionSink,
        version: u16,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let ReadRequest {
                partition,
                cursor,
                follow,
                ..
            } = request;
            let resumes = cursor.is_some();
            let cursor = match cursor {
                Some(cursor) => decode::<S, R>(self, &cursor, version)?,
                None => R::Cursor::default(),
            };
            let mut out = Emitter::new(sink, version, follow).resuming(resumes);
            match ReadStream::read(self, source, &partition, cursor, &mut out).await {
                Err(error) if error.kind() == ConnectorErrorKind::Stopped => Ok(()),
                other => other,
            }
        })
    }

    fn committed<'a>(
        &'a self,
        source: &'a S,
        cursors: &'a [(PartitionId, Cursor)],
        version: u16,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let decoded = cursors
                .iter()
                .map(|(id, cursor)| Ok((id.clone(), decode::<S, R>(self, cursor, version)?)))
                .collect::<Result<Vec<_>>>()?;
            ReadStream::committed(self, source, &decoded).await
        })
    }

    #[cfg(feature = "certify")]
    fn acknowledged<'a>(
        &'a self,
        source: &'a S,
        partition: &'a PartitionId,
        version: u16,
    ) -> BoxFuture<'a, Result<Option<Cursor>>> {
        Box::pin(async move {
            ReadStream::acknowledged(self, source, partition)
                .await?
                .map(|cursor| Cursor::encode(version, &cursor))
                .transpose()
        })
    }
}

/// Decodes `cursor` in the stream's format `version`; a mismatch names the stream.
fn decode<S: SourceConnector, R: ReadStream<S>>(
    stream: &R,
    cursor: &Cursor,
    version: u16,
) -> Result<R::Cursor> {
    cursor
        .decode(version)
        .map_err(|error| error.in_stream(ReadStream::spec(stream).name()))
}

/// One stream of a source, and the format of its cursors.
pub(super) struct Versioned<C> {
    pub(super) stream: Box<dyn ErasedStream<C>>,
    pub(super) version: u16,
}

pub(super) struct SourceAdapter<C: SourceConnector> {
    pub(super) connector: C,
    streams: BTreeMap<StreamName, Versioned<C>>,
}

impl<C: SourceConnector> SourceAdapter<C> {
    pub(super) fn stream(&self, name: &StreamName) -> Result<&Versioned<C>> {
        self.streams.get(name).ok_or_else(|| {
            ConnectorError::config(format!("stream {name} is not in this source's catalog"))
                .with_code("unknown_stream")
        })
    }
}

impl<C: SourceConnector> Source for SourceAdapter<C> {
    fn check(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(self.connector.check())
    }

    fn discover(&self) -> BoxFuture<'_, Result<Catalog>> {
        Box::pin(self.connector.discover())
    }

    fn plan<'a>(
        &'a self,
        stream: &'a StreamName,
        state: &'a StreamState,
    ) -> BoxFuture<'a, Result<PartitionPlan>> {
        match self.stream(stream) {
            Ok(found) => found.stream.plan(&self.connector, state),
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }

    fn read(&self, request: ReadRequest, sink: PartitionSink) -> BoxFuture<'_, Result<()>> {
        match self.stream(&request.stream) {
            Ok(found) => found
                .stream
                .read(&self.connector, request, sink, found.version),
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }

    fn committed<'a>(
        &'a self,
        stream: &'a StreamName,
        cursors: &'a [(PartitionId, Cursor)],
    ) -> BoxFuture<'a, Result<()>> {
        match self.stream(stream) {
            Ok(found) => found
                .stream
                .committed(&self.connector, cursors, found.version),
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }
}

/// Connects `C` with `config`, as the engine drives it.
pub(super) async fn adapted<C: SourceConnector>(
    config: serde_json::Value,
    context: &ConnectContext,
) -> Result<SourceAdapter<C>> {
    let config = config::parse::<C::Config>(&config)?;
    assembled(C::connect(config, context).await?, |_, stream| {
        stream.cursor_version()
    })
}

/// `connector`, with its streams by name, each cursor in the format `version` says of the stream;
/// two of one name are refused.
fn assembled<C: SourceConnector>(
    connector: C,
    version: impl Fn(&C, &dyn ErasedStream<C>) -> u16,
) -> Result<SourceAdapter<C>> {
    let mut streams = BTreeMap::new();
    for stream in connector.streams().streams {
        let name = stream.spec().name().clone();
        if streams.contains_key(&name) {
            return Err(super::invalid_catalog(&InvalidCatalog::Duplicate(name)));
        }
        let version = version(&connector, stream.as_ref());
        streams.insert(name, Versioned { stream, version });
    }
    Ok(SourceAdapter { connector, streams })
}

pub(super) struct Factory<C> {
    spec: ConnectorSpec,
    connector: PhantomData<fn() -> C>,
}

impl<C: SourceConnector> SourceFactory for Factory<C> {
    fn spec(&self) -> &ConnectorSpec {
        &self.spec
    }

    fn connect(
        &self,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> BoxFuture<'_, Result<Box<dyn Source>>> {
        Box::pin(async move {
            let adapter = adapted::<C>(config, &context).await?;
            Ok(Box::new(adapter) as Box<dyn Source>)
        })
    }
}

/// The engine-facing factory for source connector `C`.
///
/// It never tells where the source stands outside the engine: certification asks that of
/// `acknowledging_source_factory`, which the `certify` feature adds.
///
/// # Panics
///
/// Panics if `C::ID` is not a valid [`ConnectorId`]; the `#[source]` attribute checks it at
/// compile time.
pub fn source_factory<C: SourceConnector>() -> Box<dyn SourceFactory> {
    Box::new(factory::<C>())
}

/// The factory of a source described at run time: its spec, what connects it, and what says the
/// format of a connected source's streams' cursors.
pub(super) struct Described<C, F, V> {
    spec: ConnectorSpec,
    connect: F,
    cursor_version: V,
    connector: PhantomData<fn() -> C>,
}

impl<C, F, V> Described<C, F, V>
where
    C: SourceConnector,
    F: Fn(serde_json::Value, ConnectContext) -> BoxFuture<'static, Result<C>>
        + Send
        + Sync
        + 'static,
    V: Fn(&C, &StreamName) -> Option<u16> + Send + Sync + 'static,
{
    /// The source `spec` names, which `connect` makes from its whole configuration, its streams'
    /// cursors in the format `cursor_version` says, where it says one.
    pub(super) fn new(spec: ConnectorSpec, connect: F, cursor_version: V) -> Self {
        Self {
            spec,
            connect,
            cursor_version,
            connector: PhantomData,
        }
    }

    /// Connects `C` with `config` through the function that describes it.
    pub(super) fn adapted(
        &self,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> impl Future<Output = Result<SourceAdapter<C>>> + Send + '_ {
        let connecting = (self.connect)(config, context);
        async move {
            assembled(connecting.await?, |connector, stream| {
                (self.cursor_version)(connector, stream.spec().name())
                    .unwrap_or_else(|| stream.cursor_version())
            })
        }
    }
}

impl<C, F, V> SourceFactory for Described<C, F, V>
where
    C: SourceConnector,
    F: Fn(serde_json::Value, ConnectContext) -> BoxFuture<'static, Result<C>>
        + Send
        + Sync
        + 'static,
    V: Fn(&C, &StreamName) -> Option<u16> + Send + Sync + 'static,
{
    fn spec(&self) -> &ConnectorSpec {
        &self.spec
    }

    fn connect(
        &self,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> BoxFuture<'_, Result<Box<dyn Source>>> {
        let adapting = self.adapted(config, context);
        Box::pin(async move { Ok(Box::new(adapting.await?) as Box<dyn Source>) })
    }
}

/// The engine-facing factory for a source known only at run time, as one written in another
/// language is: `spec` names it, `connect` makes it from its whole configuration, and
/// `cursor_version` says, of the source connected and a stream's name, the format of that stream's
/// cursors, where it says one.
///
/// `C`'s `ID`, `VERSION`, `Config` and `connect` are not read; `spec` and `connect` stand for
/// them, and `cursor_version` for each stream's [`ReadStream::CURSOR_VERSION`] where it answers
/// one. Every other part of the source is `C`'s, its streams', and the SDK's choreography. It
/// never tells where the source stands outside the engine: certification asks that of
/// `acknowledging_source_factory_from`, which the `certify` feature adds.
pub fn source_factory_from<C, F, V>(
    spec: ConnectorSpec,
    connect: F,
    cursor_version: V,
) -> Box<dyn SourceFactory>
where
    C: SourceConnector,
    F: Fn(serde_json::Value, ConnectContext) -> BoxFuture<'static, Result<C>>
        + Send
        + Sync
        + 'static,
    V: Fn(&C, &StreamName) -> Option<u16> + Send + Sync + 'static,
{
    Box::new(Described::new(spec, connect, cursor_version))
}

pub(super) fn factory<C: SourceConnector>() -> Factory<C> {
    let spec = ConnectorSpec {
        id: ConnectorId::parse(C::ID).expect("the connector's ID is a valid connector id"),
        version: C::VERSION.to_owned(),
        config_schema: config::schema::<C::Config>(),
    };
    Factory::<C> {
        spec,
        connector: PhantomData,
    }
}
