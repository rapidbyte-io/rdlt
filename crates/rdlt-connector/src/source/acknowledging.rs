//! The factory of a source that tells where it stands outside the engine, for certification.

use std::sync::Arc;

use super::adapter::{Factory, SourceAdapter, adapted, factory};
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
            Ok(erased) => erased.acknowledged(&self.connector, partition),
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
