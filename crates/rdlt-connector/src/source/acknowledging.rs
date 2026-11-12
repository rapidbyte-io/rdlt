//! The factory of a source that tells where it stands outside the engine, for certification.

use std::sync::Arc;

use super::adapter::{Described, Factory, SourceAdapter, adapted, factory};
use super::{
    ACKNOWLEDGED_CODE, AcknowledgedReader, Acknowledging, Source, SourceConnector, SourceFactory,
};
use crate::cursor::Cursor;
use crate::error::{ConnectorError, ConnectorErrorKind, Result};
use crate::id::{PartitionId, StreamName};
use crate::spec::{BoxFuture, ConnectContext, ConnectorSpec};

impl<C: SourceConnector> AcknowledgedReader for SourceAdapter<C> {
    fn acknowledged<'a>(
        &'a self,
        stream: &'a StreamName,
        partition: &'a PartitionId,
    ) -> BoxFuture<'a, Result<Option<Cursor>>> {
        match self.stream(stream) {
            Ok(found) => found
                .stream
                .acknowledged(&self.connector, partition, found.version),
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }
}

/// [`Factory`], which also tells where a connector that says it does stands.
struct AcknowledgingFactory<C>(Factory<C>);

impl<C: SourceConnector> SourceFactory for AcknowledgingFactory<C> {
    fn spec(&self) -> &ConnectorSpec {
        self.0.spec()
    }

    fn connect(
        &self,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> BoxFuture<'_, Result<Box<dyn Source>>> {
        self.0.connect(config, context)
    }

    fn acknowledges(&self) -> bool {
        C::ACKNOWLEDGES
    }

    fn connect_acknowledging(
        &self,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> BoxFuture<'_, Result<Acknowledging>> {
        Box::pin(async move {
            if !C::ACKNOWLEDGES {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "this source does not tell where it stands outside the engine",
                )
                .with_code(ACKNOWLEDGED_CODE));
            }
            // The reader is connected apart from the source it tells of, so it answers what the
            // source keeps beyond a connection, not what one connection was told.
            let source = adapted::<C>(config.clone(), &context).await?;
            let reader = adapted::<C>(config, &context).await?;
            Ok((
                Arc::new(source) as Arc<dyn Source>,
                Arc::new(reader) as Arc<dyn AcknowledgedReader>,
            ))
        })
    }
}

/// The engine-facing factory for source connector `C`, which also tells where `C` stands outside
/// the engine where `C` says it does ([`SourceConnector::ACKNOWLEDGES`]), for certification.
///
/// A binary serves the protocol's question only where its `main` serves this factory: one that
/// serves [`source_factory`](super::source_factory), or the connector by its type, refuses it as
/// unsupported whatever its host offers.
///
/// # Panics
///
/// Panics if `C::ID` is not a valid [`ConnectorId`](crate::ConnectorId); the `#[source]`
/// attribute checks it at compile time.
pub fn acknowledging_source_factory<C: SourceConnector>() -> Box<dyn SourceFactory> {
    Box::new(AcknowledgingFactory(factory::<C>()))
}

/// [`Described`], which also tells where its connector stands.
struct AcknowledgingDescribed<C, F, V>(Described<C, F, V>);

impl<C, F, V> SourceFactory for AcknowledgingDescribed<C, F, V>
where
    C: SourceConnector,
    F: Fn(serde_json::Value, ConnectContext) -> BoxFuture<'static, Result<C>>
        + Send
        + Sync
        + 'static,
    V: Fn(&C, &StreamName) -> Option<u16> + Send + Sync + 'static,
{
    fn spec(&self) -> &ConnectorSpec {
        self.0.spec()
    }

    fn connect(
        &self,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> BoxFuture<'_, Result<Box<dyn Source>>> {
        self.0.connect(config, context)
    }

    fn acknowledges(&self) -> bool {
        true
    }

    fn connect_acknowledging(
        &self,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> BoxFuture<'_, Result<Acknowledging>> {
        // The reader is connected apart from the source it tells of, so it answers what the
        // source keeps beyond a connection, not what one connection was told.
        let source = self.0.adapted(config.clone(), context.clone());
        let reader = self.0.adapted(config, context);
        Box::pin(async move {
            Ok((
                Arc::new(source.await?) as Arc<dyn Source>,
                Arc::new(reader.await?) as Arc<dyn AcknowledgedReader>,
            ))
        })
    }
}

/// The engine-facing factory for a source known only at run time, as
/// [`source_factory_from`](super::source_factory_from) makes, which also tells where it stands
/// outside the engine, for certification.
///
/// It stands for [`SourceConnector::ACKNOWLEDGES`], which is not read, as `spec` and `connect`
/// stand for the rest of what `C` would say of itself: a source described as one that tells is
/// served by this factory, and one that does not by `source_factory_from`.
pub fn acknowledging_source_factory_from<C, F, V>(
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
    Box::new(AcknowledgingDescribed(Described::new(
        spec,
        connect,
        cursor_version,
    )))
}
