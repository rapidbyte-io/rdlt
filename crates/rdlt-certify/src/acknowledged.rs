//! Asking, over the wire, where a source stands outside the engine: the reader `S-ACK` checks with,
//! when the source accepts the handshake's `acknowledged` feature.
//!
//! The questions go over a connection of their own, which never reads or commits, as a
//! replication slot's position is read apart from the connection that moved it: what it answers is
//! what the source keeps beyond a connection.

use rdlt_connector::wire::{error, v1};
use rdlt_connector::{
    AcknowledgedReader, BoxFuture, ConnectorError, ConnectorErrorKind, Cursor, PartitionId, Result,
    Role, StreamName,
};
use rdlt_host::remote::Client;
use rdlt_wire::ACKNOWLEDGED;
use tokio::sync::Mutex;

use crate::protocol::{configure_request, request};
use crate::target::Target;

/// The longest a source takes to tell where it stands: one that takes longer fails the clause,
/// rather than hold the certification.
const TELLING_TIME: std::time::Duration = std::time::Duration::from_secs(30);

/// What asks a source where it stands, over a connection of its own that never reads.
pub(crate) struct AckProbe {
    client: Mutex<Client>,
}

impl AckProbe {
    async fn ask(&self, stream: &StreamName, partition: &PartitionId) -> Result<Option<Cursor>> {
        let asked = v1::ReadAcknowledgedRequest {
            stream: Some(v1::StreamName::from(stream)),
            partition: partition.to_string(),
        };
        let answer = self
            .client
            .lock()
            .await
            .read_acknowledged(asked)
            .await
            .map_err(|status| error(&status))?
            .into_inner();
        answer
            .cursor
            .map(Cursor::try_from)
            .transpose()
            .map_err(|invalid| ConnectorError::data(format!("the source told a cursor: {invalid}")))
    }
}

impl AcknowledgedReader for AckProbe {
    fn acknowledged<'a>(
        &'a self,
        stream: &'a StreamName,
        partition: &'a PartitionId,
    ) -> BoxFuture<'a, Result<Option<Cursor>>> {
        Box::pin(async move {
            tokio::time::timeout(TELLING_TIME, self.ask(stream, partition))
                .await
                .unwrap_or_else(|_| {
                    let message = format!(
                        "the source took longer than {TELLING_TIME:?} to tell where it stands"
                    );
                    Err(ConnectorError::new(ConnectorErrorKind::Transient, message))
                })
        })
    }
}

/// A probe of the source `target` reaches with `config`, handshaken as a source with the
/// `acknowledged` feature offered and configured; none where the handshake did not accept the
/// feature.
pub(crate) async fn probe(target: &Target, config: &str) -> Result<Option<AckProbe>> {
    let mut client = target.client().await?;
    let mut offered = request(Role::Source, rdlt_wire::PROTOCOL_MAJOR);
    offered.features.push(ACKNOWLEDGED.to_owned());
    let answer = client
        .handshake(offered)
        .await
        .map_err(|status| error(&status))?
        .into_inner();
    if !answer
        .accepted_features
        .iter()
        .any(|feature| feature == ACKNOWLEDGED)
    {
        return Ok(None);
    }
    client
        .configure(configure_request(config))
        .await
        .map_err(|status| error(&status))?;
    Ok(Some(AckProbe {
        client: Mutex::new(client),
    }))
}
