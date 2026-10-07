//! A served write: the engine's frames for one table become batches the destination's writer
//! stages, and each frame's bytes return to the engine as credit once staged.

use rdlt_wire::flow::Granting;
use rdlt_wire::limits::CREDIT_FLOOR;
use rdlt_wire::prost::Message as _;
use rdlt_wire::tonic::{Status, Streaming};
use rdlt_wire::{Decoder, IpcFrame, Limits};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use super::service::{Answer, Service, invalid};
use crate::destination::{DestinationWriter, TableRef};
use crate::error::ConnectorError;
use crate::id::SegmentId;
use crate::wire::{Invalid, frame_error, status, v1};

/// Starts serving the write the engine's first frame asks for.
pub(super) async fn serve(
    service: &Service,
    limits: Limits,
    mut frames: Streaming<v1::WriteFrame>,
) -> Result<Answer<v1::WriteAck>, Status> {
    let Some(v1::WriteFrame {
        frame: Some(v1::write_frame::Frame::Start(start)),
    }) = frames.message().await?
    else {
        return Err(invalid(&Invalid::Missing("write start")));
    };
    let table = start
        .table
        .ok_or(Invalid::Missing("table"))
        .and_then(TableRef::try_from)
        .map_err(|error| invalid(&error))?;
    let writer = service.writer(start.session, &table).await?;
    let (acks, answer) = mpsc::channel(16);
    let pumping = tokio::spawn(pump(writer, frames, acks.clone(), limits));
    tokio::spawn(async move {
        // A writer that panics fails the write with the panic, as a read's panic fails the read,
        // rather than ending the write as though it were done.
        if let Err(join) = pumping.await {
            let error = ConnectorError::new(
                crate::error::ConnectorErrorKind::Internal,
                format!("the write failed: {join}"),
            );
            acks.send(Err(status(&error))).await.ok();
        }
    });
    Ok(Box::pin(ReceiverStream::new(answer)))
}

/// Stages the engine's frames until it finishes the write, answering each with credit, a flush
/// with its credit and then its stats, and a failure with the error, after which the write ends.
async fn pump(
    mut writer: Box<dyn DestinationWriter>,
    mut frames: Streaming<v1::WriteFrame>,
    acks: mpsc::Sender<Result<v1::WriteAck, Status>>,
    limits: Limits,
) {
    use v1::write_ack::Ack;
    let answer = |ack| acks.send(Ok(v1::WriteAck { ack: Some(ack) }));
    let mut granting = Granting::new(CREDIT_FLOOR, &limits);
    let mut staging = Staging {
        decoder: Decoder::new(limits),
        limits,
        staged: 0,
    };
    let opening = v1::Credit {
        bytes: granting.opening(),
    };
    if answer(Ack::Credit(opening)).await.is_err() {
        return;
    }
    while let Ok(Some(frame)) = frames.message().await {
        let size = u64::try_from(frame.encoded_len()).unwrap_or(u64::MAX);
        let flushed = match staging.stage(frame, writer.as_mut()).await {
            Ok(flushed) => flushed,
            Err(error) => {
                answer(Ack::Error(v1::Error::from(&error))).await.ok();
                return;
            }
        };
        let credit = Ack::Credit(v1::Credit {
            bytes: granting.taken(size),
        });
        let answers = std::iter::once(credit).chain(flushed.map(Ack::Flushed));
        for ack in answers {
            if answer(ack).await.is_err() {
                return;
            }
        }
    }
}

/// What a write has staged since its last flush, and the decoder of its frames.
///
/// A writer may keep what it stages until it flushes, so the frames a host sends between two
/// flushes are bounded together.
struct Staging {
    decoder: Decoder,
    limits: Limits,
    /// Bytes: the batch frames staged since the last flush, header and body.
    staged: u64,
}

impl Staging {
    /// Applies one frame: a schema, a batch staged, or a flush and its stats.
    async fn stage(
        &mut self,
        frame: v1::WriteFrame,
        writer: &mut dyn DestinationWriter,
    ) -> Result<Option<v1::WriteStats>, ConnectorError> {
        use v1::write_frame::Frame;
        match frame.frame {
            Some(Frame::Schema(schema)) => {
                self.decoder
                    .schema(&schema.ipc_schema)
                    .map_err(|error| frame_error(&error))?;
                Ok(None)
            }
            Some(Frame::Batch(batch)) => {
                let frame = IpcFrame {
                    header: batch.data_header,
                    body: batch.data_body,
                };
                let bytes = frame.header.len().saturating_add(frame.body.len());
                let staged = self
                    .staged
                    .saturating_add(u64::try_from(bytes).unwrap_or(u64::MAX));
                // Refused before it is decoded: nothing more is held than the limit allows.
                self.limits
                    .admit_staged(staged)
                    .map_err(|refusal| frame_error(&rdlt_wire::WireError::Refused(refusal)))?;
                let decoded = self.decoder.frame(&frame);
                if let Some(decoded) = decoded.map_err(|error| frame_error(&error))? {
                    writer.write(SegmentId(batch.segment), decoded).await?;
                }
                self.staged = staged;
                Ok(None)
            }
            Some(Frame::Flush(_)) => {
                let stats = writer.flush().await?;
                self.staged = 0;
                Ok(Some(v1::WriteStats::from(stats)))
            }
            Some(Frame::Start(_)) | None => Err(ConnectorError::new(
                crate::error::ConnectorErrorKind::Internal,
                "a write frame other than the first started the write",
            )
            .with_code("invalid_message")),
        }
    }
}
