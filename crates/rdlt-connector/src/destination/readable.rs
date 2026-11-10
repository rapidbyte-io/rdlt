//! The factory of a destination that reads back what it published, for certification.

use std::sync::Arc;

use super::adapter::{Described, DestinationAdapter, Factory, adapted, factory};
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
        Box::pin(async move { Ok(reading(adapted::<C>(config, &context).await?)) })
    }
}

/// `adapter`, and what reads back what its connector published.
fn reading<C: ReadBack>(adapter: DestinationAdapter<C>) -> Reading {
    let reader = ReaderAdapter(Arc::clone(&adapter.connector));
    (
        Arc::new(adapter) as Arc<dyn Destination>,
        Arc::new(reader) as Arc<dyn PublishedReader>,
    )
}

/// [`Described`], for a connector that can read back what it published.
struct ReadableDescribed<C, F>(Described<C, F>);

impl<C, F> DestinationFactory for ReadableDescribed<C, F>
where
    C: ReadBack,
    F: Fn(serde_json::Value, ConnectContext) -> BoxFuture<'static, Result<C>>
        + Send
        + Sync
        + 'static,
{
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
        let adapting = self.0.adapted(config, context);
        Box::pin(async move { Ok(reading(adapting.await?)) })
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

/// The engine-facing factory for a destination known only at run time, as
/// [`destination_factory_from`](super::destination_factory_from) makes, which also reads back
/// what `C` published, for certification.
pub fn readable_destination_factory_from<C, F>(
    spec: ConnectorSpec,
    connect: F,
) -> Box<dyn DestinationFactory>
where
    C: ReadBack,
    F: Fn(serde_json::Value, ConnectContext) -> BoxFuture<'static, Result<C>>
        + Send
        + Sync
        + 'static,
{
    Box::new(ReadableDescribed(Described::new(spec, connect)))
}
