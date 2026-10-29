//! Reading back, over the wire, what a destination published: the probe certification compares
//! with what was committed, when the destination accepts the handshake's `published` feature.

#[cfg(test)]
mod tests;

use arrow_array::RecordBatch;
use rdlt_connector::testing::Probe;
use rdlt_connector::wire::{frame_error, v1};
use rdlt_connector::{BoxFuture, ConnectorError, ConnectorErrorKind, Result, Role, TableRef};
use rdlt_host::remote::Client;
use rdlt_wire::stream::{Call, Kind};
use rdlt_wire::{Decoder, IpcFrame, PUBLISHED};

use crate::limits::{PUBLISHED_BYTES, PUBLISHED_ROWS, READ_BACK_TIME};
use crate::protocol::{configure_request, request};
use crate::target::Target;
use rdlt_host::remote::RawCall;

/// What reads back what the destination `target` reaches published, with `config`.
pub struct ReadBackProbe<'a> {
    target: &'a Target,
    config: String,
    /// Why the handshake offering the read-back failed, when it did: each read-back fails so.
    failed: Option<String>,
}

/// Shows the target alone: the configuration may hold credentials, and what failed is the
/// connector's own words.
impl std::fmt::Debug for ReadBackProbe<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReadBackProbe")
            .field("target", self.target)
            .finish_non_exhaustive()
    }
}

/// A probe reading back what the destination `target` reaches published, with `config`; `None`
/// when a handshake offering the `published` feature succeeds without accepting it.
///
/// A handshake that fails gives a probe whose every read-back fails, so the clauses that read
/// published data fail rather than go unobserved.
pub async fn read_back<'a>(
    target: &'a Target,
    config: &serde_json::Value,
) -> Option<ReadBackProbe<'a>> {
    let config = config.to_string();
    let failed = match handshaken(target, &config).await {
        Ok((_, false)) => return None,
        Ok((_, true)) => None,
        Err(error) => Some(error.to_string()),
    };
    Some(ReadBackProbe {
        target,
        config,
        failed,
    })
}

impl Probe for ReadBackProbe<'_> {
    fn published<'a>(&'a self, table: &'a TableRef) -> BoxFuture<'a, Result<Vec<RecordBatch>>> {
        Box::pin(async move {
            tokio::time::timeout(READ_BACK_TIME, self.read(table))
                .await
                .unwrap_or_else(|_| {
                    let message =
                        format!("the destination's read-back took longer than {READ_BACK_TIME:?}");
                    Err(ConnectorError::new(ConnectorErrorKind::Transient, message)
                        .with_code("published_time"))
                })
        })
    }
}

impl ReadBackProbe<'_> {
    /// What the destination published to `table`, read back.
    async fn read(&self, table: &TableRef) -> Result<Vec<RecordBatch>> {
        if let Some(failed) = &self.failed {
            let message = format!("the handshake offering the read-back failed: {failed}");
            return Err(ConnectorError::new(ConnectorErrorKind::Transient, message));
        }
        {
            let (client, accepted) = handshaken(self.target, &self.config).await?;
            if !accepted {
                let message = "the destination no longer reads back what it published";
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    message,
                ));
            }
            let request = v1::ReadPublishedRequest {
                table: Some(v1::TableRef::from(table)),
            };
            let limits = self.target.limits();
            let streamed = RawCall::open(&client, Call::ReadPublished, &request, &limits).await?;
            decoded(streamed?).await
        }
    }
}

/// A client of `target`, handshaken as a destination with `config` and the `published` feature
/// offered, and whether it accepted the feature; each call within the deadline a connection's
/// handshake has.
async fn handshaken(target: &Target, config: &str) -> Result<(Client, bool)> {
    let mut client = target.client().await?;
    let mut offered = request(Role::Destination, rdlt_wire::PROTOCOL_MAJOR);
    offered.features.push(PUBLISHED.to_owned());
    let answer = target
        .opened("the read-back's handshake", client.rpc.handshake(offered))
        .await?;
    target
        .opened(
            "the read-back's configuration",
            client.rpc.configure(configure_request(config)),
        )
        .await?;
    let accepted = answer
        .accepted_features
        .iter()
        .any(|feature| feature == PUBLISHED);
    Ok((client, accepted))
}

/// The batches `streamed` carries, decoded within its limits, [`PUBLISHED_BYTES`] and
/// [`PUBLISHED_ROWS`], once the done frame ends them.
async fn decoded(mut streamed: RawCall) -> Result<Vec<RecordBatch>> {
    use v1::read_frame::Frame;
    let mut decoder = Decoder::new(*streamed.limits());
    let (mut batches, mut bytes, mut rows) = (Vec::new(), 0_usize, 0_usize);
    while let Some(received) = streamed.next().await? {
        if let Kind::Batch { .. } = received.head.kind {
            let Some(read) = within(bytes, received.body.bytes.len(), PUBLISHED_BYTES) else {
                let message = format!(
                    "the destination read back more than the {PUBLISHED_BYTES} bytes \
                     certification reads of a table"
                );
                return Err(ConnectorError::data(message).with_code("published_bytes"));
            };
            bytes = read;
            let frame = IpcFrame::framed(received.body).map_err(|error| frame_error(&error))?;
            if let Some(batch) = decoder.frame(&frame).map_err(|error| frame_error(&error))? {
                let Some(read) = within(rows, batch.num_rows(), PUBLISHED_ROWS) else {
                    let message = format!(
                        "the destination read back more than the {PUBLISHED_ROWS} rows \
                         certification reads of a table"
                    );
                    return Err(ConnectorError::data(message).with_code("published_rows"));
                };
                rows = read;
                batches.push(batch);
            }
            continue;
        }
        match streamed.read_frame(&received)?.frame {
            Some(Frame::Schema(schema)) => {
                decoder
                    .schema(&schema.ipc_schema)
                    .map_err(|error| frame_error(&error))?;
            }
            Some(Frame::Done(_)) => return Ok(batches),
            Some(Frame::Error(error)) => {
                let failed = ConnectorError::try_from(error);
                return Err(failed.unwrap_or_else(|invalid| {
                    ConnectorError::data(format!("the read-back failed: {invalid}"))
                }));
            }
            _ => {
                let message = "the read-back sent a frame no read-back sends";
                return Err(ConnectorError::data(message));
            }
        }
    }
    Err(ConnectorError::data(
        "the read-back ended without its done frame",
    ))
}

/// What is read of a table once `more` is added to `read`, when that is within `limit`.
fn within(read: usize, more: usize, limit: usize) -> Option<usize> {
    read.checked_add(more).filter(|read| *read <= limit)
}
