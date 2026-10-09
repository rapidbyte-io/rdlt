//! Turns an author's [`DestinationConnector`] into the engine-facing [`Destination`].

use std::marker::PhantomData;
use std::sync::Arc;

use arrow_array::RecordBatch;

use super::{
    Destination, DestinationConnector, DestinationFactory, DestinationSession, DestinationWriter,
    OpenContext, Opened, OpenedSession, Session, TableChange, TableRef, TableWriter, WriteStats,
};
use crate::capabilities::Capabilities;
use crate::commit::{CommitMeta, Receipt};
use crate::config;
use crate::error::{ConnectorError, Result};
use crate::id::{ConnectorId, SegmentId};
use crate::spec::{BoxFuture, ConnectContext, ConnectorSpec};

pub(super) struct DestinationAdapter<C> {
    pub(super) connector: Arc<C>,
    capabilities: Capabilities,
}

impl<C: DestinationConnector> Destination for DestinationAdapter<C> {
    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    fn check(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(self.connector.check())
    }

    fn open<'a>(&'a self, context: &'a OpenContext) -> BoxFuture<'a, Result<OpenedSession>> {
        Box::pin(async move {
            let Opened {
                mut session,
                epoch,
                state,
            } = self.connector.open(context).await?;
            // Unpublished staging from any earlier load must never reach a later commit.
            if let Err(error) = session.discard_staged().await {
                return Err(close_after(session, error).await);
            }
            Ok(OpenedSession {
                session: Box::new(SessionAdapter(session)),
                epoch,
                state,
            })
        })
    }
}

/// Closes a session whose open failed, so it releases what it holds, and returns the failure:
/// it matters more than any error from closing.
async fn close_after<S: Session>(session: S, error: ConnectorError) -> ConnectorError {
    drop(session.close().await);
    error
}

struct SessionAdapter<S>(S);

impl<S: Session> DestinationSession for SessionAdapter<S> {
    fn apply_schema<'a>(&'a mut self, change: &'a TableChange) -> BoxFuture<'a, Result<()>> {
        Box::pin(self.0.apply_schema(change))
    }

    fn writer<'a>(
        &'a mut self,
        table: &'a TableRef,
    ) -> BoxFuture<'a, Result<Box<dyn DestinationWriter>>> {
        Box::pin(async move {
            let writer = self.0.writer(table).await?;
            Ok(Box::new(WriterAdapter(writer)) as Box<dyn DestinationWriter>)
        })
    }

    fn commit<'a>(&'a mut self, meta: &'a CommitMeta) -> BoxFuture<'a, Result<Receipt>> {
        Box::pin(self.0.commit(meta))
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
        Box::pin(self.0.close())
    }
}

struct WriterAdapter<W>(W);

impl<W: TableWriter> DestinationWriter for WriterAdapter<W> {
    fn write(&mut self, segment: SegmentId, batch: RecordBatch) -> BoxFuture<'_, Result<()>> {
        Box::pin(self.0.write(segment, batch))
    }

    fn flush(&mut self) -> BoxFuture<'_, Result<WriteStats>> {
        Box::pin(self.0.flush())
    }
}

pub(super) struct Factory<C> {
    spec: ConnectorSpec,
    connector: PhantomData<fn() -> C>,
}

impl<C: DestinationConnector> DestinationFactory for Factory<C> {
    fn spec(&self) -> &ConnectorSpec {
        &self.spec
    }

    fn connect(
        &self,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> BoxFuture<'_, Result<Box<dyn Destination>>> {
        Box::pin(async move {
            let adapter = adapted::<C>(config, &context).await?;
            Ok(Box::new(adapter) as Box<dyn Destination>)
        })
    }
}

/// Connects `C` with `config`, as the engine drives it.
pub(super) async fn adapted<C: DestinationConnector>(
    config: serde_json::Value,
    context: &ConnectContext,
) -> Result<DestinationAdapter<C>> {
    let config = config::parse::<C::Config>(&config)?;
    let connector = C::connect(config, context).await?;
    let capabilities = connector.capabilities();
    Ok(DestinationAdapter {
        connector: Arc::new(connector),
        capabilities,
    })
}

/// The engine-facing factory for destination connector `C`.
///
/// # Panics
///
/// Panics if `C::ID` is not a valid [`ConnectorId`]; the `#[destination]` attribute checks it at
/// compile time.
pub fn destination_factory<C: DestinationConnector>() -> Box<dyn DestinationFactory> {
    Box::new(factory::<C>())
}

pub(super) fn factory<C: DestinationConnector>() -> Factory<C> {
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
