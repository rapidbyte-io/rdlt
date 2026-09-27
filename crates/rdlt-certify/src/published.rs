//! Reading back, over the wire, what a destination published: the probe certification compares
//! with what was committed, when the destination accepts the handshake's `published` feature.

#[cfg(test)]
mod tests;

use arrow_array::RecordBatch;
use rdlt_connector::testing::Probe;
use rdlt_connector::wire::{error, frame_error, v1};
use rdlt_connector::{BoxFuture, ConnectorError, ConnectorErrorKind, Result, Role, TableRef};
use rdlt_host::remote::Client;
use rdlt_wire::tonic::Streaming;
use rdlt_wire::{Decoder, IpcFrame, Limits, PUBLISHED};

use crate::protocol::request;
use crate::target::Target;

/// The most a read-back decodes of one table, in bytes: a destination that sends more fails the
/// clause, rather than size this process's memory.
const PUBLISHED_BYTES: usize = 64 << 20;

/// What reads back what the destination `target` reaches published, with `config`.
#[derive(Debug)]
pub struct ReadBack<'a> {
    target: &'a Target,
    config: String,
}

/// A probe reading back what the destination `target` reaches published, with `config`, when it
/// accepts the handshake's `published` feature; `None` when it does not, or cannot be reached.
pub async fn read_back<'a>(target: &'a Target, config: &serde_json::Value) -> Option<ReadBack<'a>> {
    let config = config.to_string();
    let (_, accepted) = handshaken(target, &config).await.ok()?;
    accepted.then_some(ReadBack { target, config })
}

impl Probe for ReadBack<'_> {
    fn published<'a>(&'a self, table: &'a TableRef) -> BoxFuture<'a, Result<Vec<RecordBatch>>> {
        Box::pin(async move {
            let (mut client, accepted) = handshaken(self.target, &self.config).await?;
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
            let frames = client
                .read_published(request)
                .await
                .map_err(|status| error(&status))?
                .into_inner();
            decoded(frames, self.target.limits()).await
        })
    }
}

/// A client of `target`, handshaken as a destination with `config` and the `published` feature
/// offered, and whether it accepted the feature.
async fn handshaken(target: &Target, config: &str) -> Result<(Client, bool)> {
    let mut client = target.client().await?;
    let mut offered = request(Role::Destination, config, rdlt_wire::PROTOCOL_MAJOR);
    offered.features.push(PUBLISHED.to_owned());
    let answer = client
        .handshake(offered)
        .await
        .map_err(|status| error(&status))?
        .into_inner();
    let accepted = answer
        .accepted_features
        .iter()
        .any(|feature| feature == PUBLISHED);
    Ok((client, accepted))
}

/// The batches `frames` carry, decoded within `limits` and [`PUBLISHED_BYTES`], once the done
/// frame ends them.
async fn decoded(mut frames: Streaming<v1::ReadFrame>, limits: Limits) -> Result<Vec<RecordBatch>> {
    use v1::read_frame::Frame;
    let mut decoder = Decoder::new(limits);
    let (mut batches, mut bytes) = (Vec::new(), 0_usize);
    while let Some(frame) = frames.message().await.map_err(|status| error(&status))? {
        match frame.frame {
            Some(Frame::Schema(schema)) => {
                decoder
                    .schema(&schema.ipc_schema)
                    .map_err(|error| frame_error(&error))?;
            }
            Some(Frame::Batch(batch)) => {
                let frame = batch
                    .data_header
                    .len()
                    .saturating_add(batch.data_body.len());
                let Some(read) = within_cap(bytes, frame) else {
                    let message = format!(
                        "the destination read back more than the {PUBLISHED_BYTES} bytes \
                         certification reads of a table"
                    );
                    return Err(ConnectorError::data(message).with_code("published_bytes"));
                };
                bytes = read;
                let frame = IpcFrame {
                    header: batch.data_header,
                    body: batch.data_body,
                };
                if let Some(batch) = decoder.frame(&frame).map_err(|error| frame_error(&error))? {
                    batches.push(batch);
                }
            }
            Some(Frame::Done(_)) => return Ok(batches),
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

/// The bytes read of a table once a `frame` of bytes more is, when they are within
/// [`PUBLISHED_BYTES`].
fn within_cap(read: usize, frame: usize) -> Option<usize> {
    read.checked_add(frame)
        .filter(|read| *read <= PUBLISHED_BYTES)
}
