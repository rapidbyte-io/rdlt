//! Turns an author's [`SourceConnector`] into the engine-facing [`Source`].

use std::marker::PhantomData;

use super::{PartitionPlan, ReadRequest, ReadStream, Source, SourceConnector, SourceFactory};
use crate::catalog::{Catalog, StreamSpec};
use crate::config;
use crate::cursor::Cursor;
use crate::emitter::Emitter;
use crate::error::{ConnectorError, ConnectorErrorKind, Result};
use crate::id::{ConnectorId, PartitionId, StreamName};
use crate::sink::PartitionSink;
use crate::spec::{BoxFuture, ConnectContext, ConnectorSpec, Role};
use crate::state::StreamState;

/// A [`ReadStream`] with its cursor type erased, so streams of one source fit in one list.
pub(crate) trait ErasedStream<S>: Send + Sync {
    fn spec(&self) -> StreamSpec;

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
    ) -> BoxFuture<'a, Result<()>>;

    fn committed<'a>(
        &'a self,
        source: &'a S,
        cursors: &'a [(PartitionId, Cursor)],
    ) -> BoxFuture<'a, Result<()>>;

    #[cfg(feature = "certify")]
    fn acknowledged<'a>(
        &'a self,
        source: &'a S,
        partition: &'a PartitionId,
    ) -> BoxFuture<'a, Result<Option<Cursor>>>;
}

impl<S: SourceConnector, R: ReadStream<S>> ErasedStream<S> for R {
    fn spec(&self) -> StreamSpec {
        ReadStream::spec(self)
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
                Some(cursor) => decode::<S, R>(self, &cursor)?,
                None => R::Cursor::default(),
            };
            let mut out = Emitter::new(sink, R::CURSOR_VERSION, follow).resuming(resumes);
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
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let decoded = cursors
                .iter()
                .map(|(id, cursor)| Ok((id.clone(), decode::<S, R>(self, cursor)?)))
                .collect::<Result<Vec<_>>>()?;
            ReadStream::committed(self, source, &decoded).await
        })
    }

    #[cfg(feature = "certify")]
    fn acknowledged<'a>(
        &'a self,
        source: &'a S,
        partition: &'a PartitionId,
    ) -> BoxFuture<'a, Result<Option<Cursor>>> {
        Box::pin(async move {
            ReadStream::acknowledged(self, source, partition)
                .await?
                .map(|cursor| Cursor::encode(R::CURSOR_VERSION, &cursor))
                .transpose()
        })
    }
}

/// Decodes `cursor` in the stream's format; a mismatch names the stream.
fn decode<S: SourceConnector, R: ReadStream<S>>(stream: &R, cursor: &Cursor) -> Result<R::Cursor> {
    cursor
        .decode(R::CURSOR_VERSION)
        .map_err(|error| error.in_stream(ReadStream::spec(stream).name()))
}

pub(super) struct SourceAdapter<C: SourceConnector> {
    pub(super) connector: C,
    streams: Vec<(StreamName, Box<dyn ErasedStream<C>>)>,
}

impl<C: SourceConnector> SourceAdapter<C> {
    pub(super) fn stream(&self, name: &StreamName) -> Result<&dyn ErasedStream<C>> {
        self.streams
            .iter()
            .find(|(candidate, _)| candidate == name)
            .map(|(_, stream)| stream.as_ref())
            .ok_or_else(|| {
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
            Ok(erased) => erased.plan(&self.connector, state),
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }

    fn read(&self, request: ReadRequest, sink: PartitionSink) -> BoxFuture<'_, Result<()>> {
        match self.stream(&request.stream) {
            Ok(erased) => erased.read(&self.connector, request, sink),
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }

    fn committed<'a>(
        &'a self,
        stream: &'a StreamName,
        cursors: &'a [(PartitionId, Cursor)],
    ) -> BoxFuture<'a, Result<()>> {
        match self.stream(stream) {
            Ok(erased) => erased.committed(&self.connector, cursors),
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
    let connector = C::connect(config, context).await?;
    let streams = connector.streams().streams;
    let streams = streams
        .into_iter()
        .map(|stream| (stream.spec().name().clone(), stream))
        .collect();
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

pub(super) fn factory<C: SourceConnector>() -> Factory<C> {
    let spec = ConnectorSpec {
        id: ConnectorId::parse(C::ID).expect("the connector's ID is a valid connector id"),
        version: C::VERSION.to_owned(),
        role: Role::Source,
        config_schema: config::schema::<C::Config>(),
    };
    Factory::<C> {
        spec,
        connector: PhantomData,
    }
}
