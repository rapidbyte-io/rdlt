//! The factory of a destination that reads back what it published, for certification.

use std::sync::Arc;

use super::adapter::{Factory, adapted, factory};
use super::{
    Destination, DestinationFactory, PublishedReader, PublishedRows, ReadBack, Reading, TableRef,
};
use crate::error::Result;
use crate::spec::{BoxFuture, ConnectContext, ConnectorSpec};

/// What reads back what connector `C` published.
struct ReaderAdapter<C>(Arc<C>);

impl<C: ReadBack> PublishedReader for ReaderAdapter<C> {
    fn published<'a>(
        &'a self,
        table: &'a TableRef,
        rows: PublishedRows,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(self.0.published(table, rows))
    }
}

/// [`Factory`], for a connector that can read back what it published.
struct ReadableFactory<C>(Factory<C>);

impl<C: ReadBack> DestinationFactory for ReadableFactory<C> {
    fn spec(&self) -> &ConnectorSpec {
        self.0.spec()
    }

    fn connect(
        &self,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> BoxFuture<'_, Result<Box<dyn Destination>>> {
        self.0.connect(config, context)
    }

    fn reads_back(&self) -> bool {
        true
    }

    fn connect_reading(
        &self,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> BoxFuture<'_, Result<Reading>> {
        Box::pin(async move {
            let adapter = adapted::<C>(config, &context).await?;
            let reader = ReaderAdapter(Arc::clone(&adapter.connector));
            Ok((
                Arc::new(adapter) as Arc<dyn Destination>,
                Arc::new(reader) as Arc<dyn PublishedReader>,
            ))
        })
    }
}

/// The engine-facing factory for destination connector `C`, which also reads back what `C`
/// published, for certification.
///
/// A binary serves the protocol's read-back only where its `main` serves this factory: one that
/// serves [`destination_factory`](super::destination_factory), or the connector by its type,
/// refuses it as unsupported whatever its host offers.
///
/// # Panics
///
/// Panics if `C::ID` is not a valid [`ConnectorId`](crate::ConnectorId); the `#[destination]`
/// attribute checks it at compile time.
pub fn readable_destination_factory<C: ReadBack>() -> Box<dyn DestinationFactory> {
    Box::new(ReadableFactory(factory::<C>()))
}
