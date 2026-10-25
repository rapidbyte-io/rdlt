//! A spawned destination, whose transport errors carry the connector's last words.

use std::num::NonZeroUsize;
use std::sync::Arc;

use rdlt_connector::{
    BoxFuture, Capabilities, ConnectorError, ConnectorErrorKind, Destination, OpenContext,
    OpenedSession,
};

use super::session::SupervisedSession;
use super::{Supervisor, Words};
use crate::remote::{CALLS, RemoteDestination};

/// A spawned destination, respawned when lost; its capabilities are those it first declared.
pub(crate) struct SupervisedDestination {
    pub(crate) supervisor: Arc<Supervisor>,
    pub(crate) capabilities: Capabilities,
}

impl SupervisedDestination {
    async fn destination(&self) -> Result<(RemoteDestination, Words), ConnectorError> {
        let (connection, words) = self.supervisor.connection().await?;
        let destination = RemoteDestination::new(connection)?;
        if *destination.capabilities() != self.capabilities {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Internal,
                "the respawned connector declares other capabilities than it first did",
            ));
        }
        Ok((destination, words))
    }
}

impl Destination for SupervisedDestination {
    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    fn check(&self) -> BoxFuture<'_, rdlt_connector::Result<()>> {
        Box::pin(async move {
            let (destination, words) = self.destination().await?;
            words.explain(destination.check().await).await
        })
    }

    fn open<'a>(
        &'a self,
        context: &'a OpenContext,
    ) -> BoxFuture<'a, rdlt_connector::Result<OpenedSession>> {
        Box::pin(async move {
            let (destination, words) = self.destination().await?;
            let opened = words.explain(destination.open(context).await).await?;
            Ok(OpenedSession {
                session: Box::new(SupervisedSession {
                    inner: opened.session,
                    words,
                }),
                ..opened
            })
        })
    }

    fn max_calls(&self) -> Option<NonZeroUsize> {
        Some(CALLS)
    }
}
