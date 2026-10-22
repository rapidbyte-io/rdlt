//! A spawned source, whose transport errors carry the connector's last words.

use std::sync::Arc;

use rdlt_connector::{
    BoxFuture, Catalog, ConnectorError, Cursor, PartitionId, PartitionPlan, PartitionSink,
    ReadRequest, Source, StreamName, StreamState,
};

use super::{Supervisor, Words};
use crate::remote::RemoteSource;

/// A spawned source, respawned when lost.
pub(crate) struct SupervisedSource(pub(crate) Arc<Supervisor>);

impl SupervisedSource {
    async fn source(&self) -> Result<(RemoteSource, Words), ConnectorError> {
        let (connection, words) = self.0.connection().await?;
        Ok((RemoteSource::new(connection), words))
    }
}

impl Source for SupervisedSource {
    fn check(&self) -> BoxFuture<'_, rdlt_connector::Result<()>> {
        Box::pin(async move {
            let (source, words) = self.source().await?;
            words.explain(source.check().await).await
        })
    }

    fn discover(&self) -> BoxFuture<'_, rdlt_connector::Result<Catalog>> {
        Box::pin(async move {
            let (source, words) = self.source().await?;
            words.explain(source.discover().await).await
        })
    }

    fn plan<'a>(
        &'a self,
        stream: &'a StreamName,
        state: &'a StreamState,
    ) -> BoxFuture<'a, rdlt_connector::Result<PartitionPlan>> {
        Box::pin(async move {
            let (source, words) = self.source().await?;
            words.explain(source.plan(stream, state).await).await
        })
    }

    fn read(
        &self,
        request: ReadRequest,
        sink: PartitionSink,
    ) -> BoxFuture<'_, rdlt_connector::Result<()>> {
        Box::pin(async move {
            let (source, words) = self.source().await?;
            words.explain(source.read(request, sink).await).await
        })
    }

    fn committed<'a>(
        &'a self,
        stream: &'a StreamName,
        cursors: &'a [(PartitionId, Cursor)],
    ) -> BoxFuture<'a, rdlt_connector::Result<()>> {
        Box::pin(async move {
            let (source, words) = self.source().await?;
            words.explain(source.committed(stream, cursors).await).await
        })
    }
}
