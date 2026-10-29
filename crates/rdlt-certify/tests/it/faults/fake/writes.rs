//! The fake's writes, as a destination whose batch frames one fault or another breaks.

use arrow_array::RecordBatch;
use rdlt_connector::wire::{frame_error, v1};
use rdlt_connector::{ConnectorError, ConnectorErrorKind};
use rdlt_wire::stream::{Call, End, FrameReader, Kind, Received};
use rdlt_wire::{Decoder, IpcFrame, Limits, Problem, WireError};
use tokio::io::{AsyncRead, AsyncWrite};

use super::{FRAME_BYTES, Fault, message, opened, refusal, send};

/// A write `opening` starts, whose frames `frames` brings; it refuses a batch it cannot decode,
/// or one beyond [`FRAME_BYTES`], as `fault` has it, answering on `io`.
pub(super) async fn write<R, W>(
    fault: Fault,
    opening: &Received,
    mut frames: FrameReader<R>,
    io: &mut W,
) where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    use v1::write_frame::Frame;
    if opening
        .message::<v1::WriteStart>(Call::Write.opening(), 1 << 20)
        .is_err()
    {
        let refused = ConnectorError::new(ConnectorErrorKind::Internal, "no start")
            .with_code("invalid_message");
        send(io, refusal(Call::Write, &refused)).await;
        return;
    }
    if !send(io, opened(Call::Write)).await {
        return;
    }
    let limits = Limits {
        frame_bytes: FRAME_BYTES,
        ..Limits::default()
    };
    let mut decoder = Decoder::new(limits);
    let form = Call::Write
        .form(End::Host)
        .expect("a write's host sends messages");
    while let Ok(Some(frame)) = frames.next().await {
        let answered = match frame.head.kind {
            Kind::Message { .. } => {
                let Ok(written) = frame.message::<v1::WriteFrame>(form, 1 << 20) else {
                    continue;
                };
                match written.frame {
                    Some(Frame::Schema(schema)) => {
                        decoder.schema(&schema.ipc_schema).ok();
                        continue;
                    }
                    Some(Frame::Flush(_)) => v1::write_ack::Ack::Flushed(v1::WriteStats::default()),
                    None => continue,
                }
            }
            Kind::Batch { .. } => {
                // Refused by its size before it is read as a message, as the decoder would.
                let held = IpcFrame::held_bytes(frame.body.bytes.len());
                let outcome = limits
                    .admit_frame(held)
                    .map_err(WireError::Refused)
                    .and_then(|()| IpcFrame::framed(frame.body))
                    .and_then(|frame| decoder.frame(&frame));
                match answer_to(outcome, fault) {
                    Some(refused) => refused,
                    None => continue,
                }
            }
            Kind::Json => continue,
        };
        let failed = matches!(answered, v1::write_ack::Ack::Error(_));
        let answer = v1::WriteAck {
            ack: Some(answered),
        };
        if !send(io, message(&answer)).await || failed {
            return;
        }
    }
}

/// How the fake answers a batch its decoder `decoded`, as `fault` has it: nothing for one it
/// takes, an error for one it refuses.
fn answer_to(
    decoded: Result<Option<RecordBatch>, WireError>,
    fault: Fault,
) -> Option<v1::write_ack::Ack> {
    match (decoded, fault) {
        (Ok(_), _)
        | (Err(_), Fault::LenientFrames)
        | (
            Err(WireError::Malformed {
                problem: Problem::BufferUnaligned { .. },
                ..
            }),
            Fault::MisalignedFrames,
        ) => None,
        (Err(_), Fault::MiscodedFrames) => {
            let refused = ConnectorError::data("a bad batch").with_code("bad_batch");
            Some(v1::write_ack::Ack::Error(v1::Error::from(&refused)))
        }
        (Err(error), _) => Some(v1::write_ack::Ack::Error(v1::Error::from(&frame_error(
            &error,
        )))),
    }
}
