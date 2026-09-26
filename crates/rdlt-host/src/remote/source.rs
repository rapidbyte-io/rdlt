//! The engine's source for a connector served on the other end of a connection.

use std::sync::Arc;

use rdlt_connector::wire::{Invalid, v1};
use rdlt_connector::{
    BoxFuture, Catalog, ConnectorError, ConnectorErrorKind, Cursor, Partition, PartitionId,
    PartitionSink, ReadRequest, Source, StreamName, StreamState,
};

use super::{Connection, read};

/// A source served on the other end of a connection.
#[derive(Debug)]
pub struct RemoteSource {
    connection: Arc<Connection>,
}

impl RemoteSource {
    /// The source `connection` handshook for.
    pub fn new(connection: Arc<Connection>) -> Self {
        Self { connection }
    }
}

/// `invalid`, a message the connector sent that does not decode, as the error it is.
pub(super) fn invalid(invalid: &Invalid) -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Internal,
        format!("the connector sent an invalid message: {invalid}"),
    )
    .with_code("invalid_message")
}

impl Source for RemoteSource {
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

    fn discover(&self) -> BoxFuture<'_, rdlt_connector::Result<Catalog>> {
        Box::pin(async move {
            let (connection, deadline) =
                (&self.connection, self.connection.options.deadlines.discover);
            let mut client = connection.client.clone();
            let catalog = connection
                .call(
                    deadline,
                    "the discovery",
                    client.discover(v1::DiscoverRequest {}),
                )
                .await?;
            Catalog::try_from(catalog).map_err(|error| invalid(&error))
        })
    }

    fn plan<'a>(
        &'a self,
        stream: &'a StreamName,
        state: &'a StreamState,
    ) -> BoxFuture<'a, rdlt_connector::Result<Vec<Partition>>> {
        Box::pin(async move {
            let (connection, deadline) = (&self.connection, self.connection.options.deadlines.plan);
            let mut client = connection.client.clone();
            let request = v1::PlanRequest {
                stream: Some(v1::StreamName::from(stream)),
                state: Some(v1::StreamState::from(state)),
            };
            let planned = connection
                .call(deadline, "the plan", client.plan(request))
                .await?;
            planned
                .partitions
                .into_iter()
                .map(|id| {
                    PartitionId::parse(id)
                        .map(Partition::new)
                        .map_err(|error| invalid(&Invalid::rejected("partition id", error)))
                })
                .collect()
        })
    }

    fn read(
        &self,
        request: ReadRequest,
        sink: PartitionSink,
    ) -> BoxFuture<'_, rdlt_connector::Result<()>> {
        Box::pin(read::run(&self.connection, request, sink))
    }

    fn committed<'a>(
        &'a self,
        stream: &'a StreamName,
        cursors: &'a [(PartitionId, Cursor)],
    ) -> BoxFuture<'a, rdlt_connector::Result<()>> {
        Box::pin(async move {
            // §12.6 names no deadline for reporting committed cursors; a commit's is the closest.
            let (connection, deadline) =
                (&self.connection, self.connection.options.deadlines.commit);
            let mut client = connection.client.clone();
            let request = v1::CommittedRequest {
                stream: Some(v1::StreamName::from(stream)),
                cursors: cursors
                    .iter()
                    .map(|(partition, cursor)| v1::CommittedCursor {
                        partition: partition.as_str().to_owned(),
                        cursor: Some(v1::Cursor::from(cursor)),
                    })
                    .collect(),
            };
            connection
                .call(deadline, "the committed report", client.committed(request))
                .await?;
            Ok(())
        })
    }
}
