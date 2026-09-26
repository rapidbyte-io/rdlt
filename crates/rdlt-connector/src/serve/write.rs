//! A served write: the engine's frames for one table become batches the destination's writer
//! stages, and each frame's bytes return to the engine as credit once staged.

use rdlt_wire::prost::Message as _;
use rdlt_wire::tonic::{Status, Streaming};
use rdlt_wire::{Decoder, IpcFrame, Limits};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use super::service::{Answer, Service, invalid};
use crate::destination::{DestinationWriter, TableRef};
use crate::error::ConnectorError;
use crate::id::SegmentId;
use crate::wire::{Invalid, frame_error, v1};

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
    tokio::spawn(pump(writer, frames, acks, limits));
    Ok(Box::pin(ReceiverStream::new(answer)))
}

/// Stages the engine's frames until it finishes the write, answering each with credit, a flush
/// with its stats, and a failure with the error, after which the write ends.
async fn pump(
    mut writer: Box<dyn DestinationWriter>,
    mut frames: Streaming<v1::WriteFrame>,
    acks: mpsc::Sender<Result<v1::WriteAck, Status>>,
    limits: Limits,
) {
    use v1::write_ack::Ack;
    // The window never exceeds a frame, so a small frame limit keeps the engine close behind.
    let window = rdlt_wire::limits::CREDIT_WINDOW.min(limits.frame_bytes);
    let mut decoder = Decoder::new(limits);
    let mut answer = Some(Ack::Credit(v1::Credit { bytes: window }));
    while let Some(ack) = answer.take() {
        let failed = matches!(ack, Ack::Error(_));
        if acks
            .send(Ok(v1::WriteAck { ack: Some(ack) }))
            .await
            .is_err()
            || failed
        {
            return;
        }
        let Ok(Some(frame)) = frames.message().await else {
            return;
        };
        let size = u64::try_from(frame.encoded_len()).unwrap_or(u64::MAX);
        answer = Some(match stage(frame, &mut decoder, writer.as_mut()).await {
            Ok(Some(stats)) => Ack::Flushed(stats),
            Ok(None) => Ack::Credit(v1::Credit { bytes: size }),
            Err(error) => Ack::Error(v1::Error::from(&error)),
        });
    }
}

/// Applies one frame: a schema, a batch staged, or a flush and its stats.
async fn stage(
    frame: v1::WriteFrame,
    decoder: &mut Decoder,
    writer: &mut dyn DestinationWriter,
) -> Result<Option<v1::WriteStats>, ConnectorError> {
    use v1::write_frame::Frame;
    match frame.frame {
        Some(Frame::Schema(schema)) => {
            decoder
                .schema(&schema.ipc_schema)
                .map_err(|error| frame_error(&error))?;
            Ok(None)
        }
        Some(Frame::Batch(batch)) => {
            let frame = IpcFrame {
                header: batch.data_header,
                body: batch.data_body,
            };
            if let Some(decoded) = decoder.frame(&frame).map_err(|error| frame_error(&error))? {
                writer.write(SegmentId(batch.segment), decoded).await?;
            }
            Ok(None)
        }
        Some(Frame::Flush(_)) => Ok(Some(v1::WriteStats::from(writer.flush().await?))),
        Some(Frame::Start(_)) | None => Err(ConnectorError::new(
            crate::error::ConnectorErrorKind::Internal,
            "a write frame other than the first started the write",
        )
        .with_code("invalid_message")),
    }
}
