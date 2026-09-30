//! Asking, over the wire, where a source stands outside the engine: the reader `S-ACK` checks with,
//! when the source accepts the handshake's `acknowledged` feature.
//!
//! Each question goes over a connection of its own, as a replication slot's position is read
//! apart from the connection that moved it: what it answers is what the source keeps beyond a
//! connection.

use rdlt_connector::wire::{error, v1};
use rdlt_connector::{
    AcknowledgedReader, BoxFuture, ConnectorError, ConnectorErrorKind, Cursor, PartitionId, Result,
    Role, StreamName,
};
use rdlt_host::remote::Client;
use rdlt_wire::ACKNOWLEDGED;

use crate::protocol::{configure_request, request};
use crate::target::Target;

/// The longest a source takes to tell where it stands: one that takes longer fails the clause,
/// rather than hold the certification.
const TELLING_TIME: std::time::Duration = std::time::Duration::from_secs(30);

/// What asks the source `target` reaches, with `config`, where it stands.
pub(crate) struct AckProbe {
    target: Target,
    config: String,
}

impl AckProbe {
    pub(crate) fn new(target: Target, config: &serde_json::Value) -> Self {
        Self {
            target,
            config: config.to_string(),
        }
    }

    async fn ask(&self, stream: &StreamName, partition: &PartitionId) -> Result<Option<Cursor>> {
        let (mut client, accepted) = handshaken(&self.target, &self.config).await?;
        if !accepted {
            let message = "the source no longer tells where it stands";
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                message,
            ));
        }
        let asked = v1::ReadAcknowledgedRequest {
            stream: Some(v1::StreamName::from(stream)),
            partition: partition.to_string(),
        };
        let answer = client
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

/// A client of `target`, handshaken as a source with `config` and the `acknowledged` feature
/// offered, and whether it accepted the feature.
pub(crate) async fn handshaken(target: &Target, config: &str) -> Result<(Client, bool)> {
    let mut client = target.client().await?;
    let mut offered = request(Role::Source, rdlt_wire::PROTOCOL_MAJOR);
    offered.features.push(ACKNOWLEDGED.to_owned());
    let answer = client
        .handshake(offered)
        .await
        .map_err(|status| error(&status))?
        .into_inner();
    client
        .configure(configure_request(config))
        .await
        .map_err(|status| error(&status))?;
    let accepted = answer
        .accepted_features
        .iter()
        .any(|feature| feature == ACKNOWLEDGED);
    Ok((client, accepted))
}
