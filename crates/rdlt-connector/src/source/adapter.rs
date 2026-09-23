//! Turns an author's [`SourceConnector`] into the engine-facing [`Source`].

use std::marker::PhantomData;

use super::{Partition, ReadRequest, ReadStream, Source, SourceConnector, SourceFactory};
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

    fn partitions<'a>(
        &'a self,
        source: &'a S,
        state: &'a StreamState,
    ) -> BoxFuture<'a, Result<Vec<Partition>>>;

    fn read<'a>(
        &'a self,
        source: &'a S,
        partition: Partition,
        cursor: Option<Cursor>,
        sink: PartitionSink,
    ) -> BoxFuture<'a, Result<()>>;

    fn committed<'a>(
        &'a self,
        source: &'a S,
        cursors: &'a [(PartitionId, Cursor)],
    ) -> BoxFuture<'a, Result<()>>;
}

impl<S: SourceConnector, R: ReadStream<S>> ErasedStream<S> for R {
    fn spec(&self) -> StreamSpec {
        ReadStream::spec(self)
    }

    fn partitions<'a>(
        &'a self,
        source: &'a S,
        state: &'a StreamState,
    ) -> BoxFuture<'a, Result<Vec<Partition>>> {
        Box::pin(ReadStream::partitions(self, source, state))
    }

    fn read<'a>(
        &'a self,
        source: &'a S,
        partition: Partition,
        cursor: Option<Cursor>,
        sink: PartitionSink,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let cursor = match cursor {
                Some(cursor) => cursor.decode(R::CURSOR_VERSION)?,
                None => R::Cursor::default(),
            };
            let mut out = Emitter::new(sink, R::CURSOR_VERSION);
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
                .map(|(id, cursor)| Ok((id.clone(), cursor.decode(R::CURSOR_VERSION)?)))
                .collect::<Result<Vec<_>>>()?;
            ReadStream::committed(self, source, &decoded).await
        })
    }
}

struct SourceAdapter<C: SourceConnector> {
    connector: C,
    streams: Vec<(StreamName, Box<dyn ErasedStream<C>>)>,
}

impl<C: SourceConnector> SourceAdapter<C> {
    fn stream(&self, name: &StreamName) -> Result<&dyn ErasedStream<C>> {
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
    ) -> BoxFuture<'a, Result<Vec<Partition>>> {
        match self.stream(stream) {
            Ok(erased) => erased.partitions(&self.connector, state),
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }

    fn read(&self, request: ReadRequest, sink: PartitionSink) -> BoxFuture<'_, Result<()>> {
        match self.stream(&request.stream) {
            Ok(erased) => erased.read(&self.connector, request.partition, request.cursor, sink),
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

struct Factory<C> {
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
            let config = config::parse::<C::Config>(config)?;
            let connector = C::connect(config, &context).await?;
            let streams = connector.streams().streams;
            let streams = streams
                .into_iter()
                .map(|stream| (stream.spec().name().clone(), stream))
                .collect();
            Ok(Box::new(SourceAdapter { connector, streams }) as Box<dyn Source>)
        })
    }
}

/// The engine-facing factory for source connector `C`.
///
/// # Panics
///
/// Panics if `C::ID` is not a valid [`ConnectorId`]; the `#[source]` attribute checks it at
/// compile time.
pub fn source_factory<C: SourceConnector>() -> Box<dyn SourceFactory> {
    let spec = ConnectorSpec {
        id: ConnectorId::parse(C::ID).expect("the connector's ID is a valid connector id"),
        version: C::VERSION.to_owned(),
        role: Role::Source,
        config_schema: config::schema::<C::Config>(),
    };
    Box::new(Factory::<C> {
        spec,
        connector: PhantomData,
    })
}
