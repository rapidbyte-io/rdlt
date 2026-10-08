//! The fake's writes, as a destination whose batch frames one fault or another breaks.

use rdlt_connector::wire::{frame_error, v1};
use rdlt_connector::{ConnectorError, ConnectorErrorKind};
use rdlt_wire::plane::Incoming;
use rdlt_wire::{Decoder, IpcFrame, Limits};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use super::{Answer, FRAME_BYTES, Fault};

/// A write that begins with its start, and refuses a batch it cannot decode, or one beyond
/// [`FRAME_BYTES`], as `fault` has it.
pub(super) fn write(fault: Fault, mut frames: Incoming<v1::WriteFrame>) -> Answer<v1::WriteAck> {
    let (acks, answer) = mpsc::channel(4);
    tokio::spawn(async move {
        use v1::write_frame::Frame;
        let ack = |ack| Ok(v1::WriteAck { ack: Some(ack) });
        let limits = Limits {
            frame_bytes: FRAME_BYTES,
            ..Limits::default()
        };
        let mut decoder = Decoder::new(limits);
        let first = frames
            .message()
            .await
            .ok()
            .flatten()
            .and_then(|frame| frame.frame);
        if !matches!(first, Some(Frame::Start(_))) {
            let refused = ConnectorError::new(ConnectorErrorKind::Internal, "no start")
                .with_code("invalid_message");
            acks.send(Err(rdlt_connector::wire::status(&refused)))
                .await
                .ok();
            return;
        }
        while let Ok(Some(frame)) = frames.message().await {
            let answered = match frame.frame {
                Some(Frame::Schema(schema)) => {
                    decoder.schema(&schema.ipc_schema).ok();
                    continue;
                }
                Some(Frame::Batch(batch)) => {
                    let frame = IpcFrame {
                        header: batch.data_header,
                        body: batch.data_body,
                    };
                    match (decoder.frame(&frame), fault) {
                        (Ok(_), _) | (Err(_), Fault::LenientFrames) => {
                            v1::write_ack::Ack::Credit(v1::Credit { bytes: 1 << 20 })
                        }
                        (Err(_), Fault::MiscodedFrames) => {
                            let refused =
                                ConnectorError::data("a bad batch").with_code("bad_batch");
                            v1::write_ack::Ack::Error(v1::Error::from(&refused))
                        }
                        (Err(error), _) => {
                            v1::write_ack::Ack::Error(v1::Error::from(&frame_error(&error)))
                        }
                    }
                }
                Some(Frame::Flush(_)) => v1::write_ack::Ack::Flushed(v1::WriteStats::default()),
                _ => continue,
            };
            let failed = matches!(answered, v1::write_ack::Ack::Error(_));
            if acks.send(ack(answered)).await.is_err() || failed {
                return;
            }
        }
    });
    Box::pin(ReceiverStream::new(answer))
}
