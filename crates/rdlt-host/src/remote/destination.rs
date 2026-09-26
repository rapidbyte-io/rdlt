//! The engine's destination for a connector served on the other end of a connection: its
//! sessions, and their schema changes, writers and commits.

use std::sync::Arc;

use rdlt_connector::wire::v1;
use rdlt_connector::{
    BoxFuture, Capabilities, CommitMeta, ConnectorError, ConnectorErrorKind, Destination,
    DestinationSession, DestinationWriter, Epoch, OpenContext, OpenedSession, Receipt, StateRecord,
    TableChange, TableRef,
};

use super::source::invalid;
use super::{Connection, write};

/// A destination served on the other end of a connection.
#[derive(Debug)]
pub struct RemoteDestination {
    connection: Arc<Connection>,
    capabilities: Capabilities,
}

impl RemoteDestination {
    /// The destination `connection` handshook for, with the capabilities its handshake declared.
    ///
    /// # Errors
    ///
    /// An internal error when the handshake declared no capabilities, or invalid ones.
    pub fn new(connection: Arc<Connection>) -> Result<Self, ConnectorError> {
        let declared = connection
            .spec()
            .destination_capabilities
            .clone()
            .ok_or_else(|| {
                invalid(&rdlt_connector::wire::Invalid::Missing(
                    "destination capabilities",
                ))
            })?;
        let capabilities = Capabilities::try_from(declared).map_err(|error| invalid(&error))?;
        Ok(Self {
            connection,
            capabilities,
        })
    }
}

impl Destination for RemoteDestination {
    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    fn check(&self) -> BoxFuture<'_, rdlt_connector::Result<()>> {
        Box::pin(async move {
            let (connection, deadline) =
                (&self.connection, self.connection.options.deadlines.check);
            let mut client = connection.client.clone();
            connection
                .call(deadline, "the check", client.check(v1::CheckRequest {}))
                .await?;
            Ok(())
        })
    }

    fn open<'a>(
        &'a self,
        context: &'a OpenContext,
    ) -> BoxFuture<'a, rdlt_connector::Result<OpenedSession>> {
        Box::pin(async move {
            let (connection, deadline) = (&self.connection, self.connection.options.deadlines.open);
            let mut client = connection.client.clone();
            let request = v1::OpenRequest {
                pipeline: context.pipeline.as_str().to_owned(),
                load_id: context.load_id.as_bytes().to_vec().into(),
            };
            let opened = connection
                .call(deadline, "the open", client.open(request))
                .await?;
            Ok(OpenedSession {
                session: Box::new(RemoteSession {
                    connection: Arc::clone(&self.connection),
                    id: opened.session,
                }),
                epoch: Epoch(opened.epoch),
                state: opened.state.into_iter().map(StateRecord::from).collect(),
            })
        })
    }
}

/// A session on a destination served on the other end of a connection.
#[derive(Debug)]
struct RemoteSession {
    connection: Arc<Connection>,
    id: u64,
}

impl DestinationSession for RemoteSession {
    fn apply_schema<'a>(
        &'a mut self,
        change: &'a TableChange,
    ) -> BoxFuture<'a, rdlt_connector::Result<()>> {
        Box::pin(async move {
            let connection = &self.connection;
            let mut client = connection.client.clone();
            let request = v1::ApplySchemaRequest {
                session: self.id,
                change: Some(v1::TableChange::from(change)),
            };
            let deadline = connection.options.deadlines.apply_schema;
            connection
                .call(deadline, "the schema change", client.apply_schema(request))
                .await?;
            Ok(())
        })
    }

    fn writer<'a>(
        &'a mut self,
        table: &'a TableRef,
    ) -> BoxFuture<'a, rdlt_connector::Result<Box<dyn DestinationWriter>>> {
        Box::pin(async move {
            let writer =
                write::RemoteWriter::open(Arc::clone(&self.connection), self.id, table).await?;
            Ok(Box::new(writer) as Box<dyn DestinationWriter>)
        })
    }

    fn commit<'a>(
        &'a mut self,
        meta: &'a CommitMeta,
    ) -> BoxFuture<'a, rdlt_connector::Result<Receipt>> {
        Box::pin(async move {
            let connection = &self.connection;
            let mut client = connection.client.clone();
            let request = v1::CommitRequest {
                session: self.id,
                meta: Some(v1::CommitMeta::from(meta)),
            };
            let deadline = connection.options.deadlines.commit;
            let receipt = connection
                .call(deadline, "the commit", client.commit(request))
                .await?;
            Receipt::try_from(receipt).map_err(|error| invalid(&error))
        })
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, rdlt_connector::Result<()>> {
        Box::pin(async move {
            let connection = &self.connection;
            let mut client = connection.client.clone();
            let deadline = connection.options.deadlines.close;
            connection
                .call(
                    deadline,
                    "the close",
                    client.close(v1::CloseRequest { session: self.id }),
                )
                .await?;
            Ok(())
        })
    }
}

/// An error for a write the connector answered out of turn.
pub(super) fn out_of_turn(what: &str) -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Internal,
        format!("the connector answered a write with {what}"),
    )
    .with_code("invalid_message")
}
