//! A served write, in two stages: one receives and decodes the engine's frames for one table,
//! while the other has the destination's writer stage each batch and returns its frame's bytes
//! to the engine as credit as it takes it.
//!
//! A frame decodes only once the writer has taken the frame before it, so a write holds what
//! its writer stages, within the staged bound, and one decoded frame waiting for it.

#[cfg(test)]
mod tests;

use arrow_array::RecordBatch;
use rdlt_wire::flow::Granting;
use rdlt_wire::limits::CREDIT_FLOOR;
use rdlt_wire::plane::{Answers, Incoming};
use rdlt_wire::prost::Message as _;
use rdlt_wire::tonic::Status;
use rdlt_wire::{Decoder, IpcFrame, Limits};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use super::service::{Service, invalid};
use crate::destination::{DestinationWriter, TableRef};
use crate::error::{ConnectorError, ConnectorErrorKind};
use crate::id::SegmentId;
use crate::wire::{Invalid, frame_error, status, v1};

/// Starts serving the write the engine's first frame asks for.
pub(super) async fn serve(
    service: &Service,
    limits: Limits,
    mut frames: Incoming<v1::WriteFrame>,
) -> Result<Answers<v1::WriteAck>, Status> {
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
    Ok(writing(writer, frames, limits))
}

/// The answers of a write of `frames` to `writer`, received in one task and written in another.
fn writing(
    writer: Box<dyn DestinationWriter>,
    frames: Incoming<v1::WriteFrame>,
    limits: Limits,
) -> Answers<v1::WriteAck> {
    let (acks, answers) = mpsc::channel(16);
    let (received, taken) = mpsc::channel(1);
    let receiving = tokio::spawn(receive(frames, received, limits));
    let writing = tokio::spawn(write(writer, taken, acks.clone(), limits));
    tokio::spawn(async move {
        let (receiving, writing) = tokio::join!(receiving, writing);
        // A stage that panics fails the write with the panic once the writer has answered all it
        // will, as a read's panic fails the read, rather than ending the write as though it were
        // done.
        if let Some(join) = writing.err().or(receiving.err()) {
            let error = ConnectorError::new(
                ConnectorErrorKind::Internal,
                format!("the write failed: {join}"),
            );
            acks.send(Err(status(&error))).await.ok();
        }
    });
    Box::pin(ReceiverStream::new(answers))
}

/// A frame received, as its writer takes it.
struct Staged {
    /// Bytes: the frame's, returned as credit once the writer takes it.
    bytes: u64,
    step: Step,
}

/// What a frame asks of the writer.
enum Step {
    /// Nothing: a schema, or a dictionary the decoder keeps for the batches after it.
    Kept,
    /// To stage a batch.
    Write(SegmentId, RecordBatch),
    /// To flush what it staged.
    Flush,
}

/// Receives and decodes the engine's frames, each once the writer has taken the frame before
/// it, until the engine ends the write, a frame fails it, or the writer stops.
async fn receive(
    mut frames: Incoming<v1::WriteFrame>,
    received: mpsc::Sender<Result<Staged, ConnectorError>>,
    limits: Limits,
) {
    let mut receiving = Receiving {
        decoder: Decoder::new(limits),
        limits,
        staged: 0,
    };
    loop {
        let frame = tokio::select! {
            biased;
            () = received.closed() => return,
            frame = frames.message() => frame,
        };
        // A request that ends, or fails, ends the write after the frames that came before it.
        let Ok(Some(frame)) = frame else {
            return;
        };
        let Ok(slot) = received.reserve().await else {
            return;
        };
        let staged = receiving.received(frame);
        let failed = staged.is_err();
        slot.send(staged);
        if failed {
            return;
        }
    }
}

/// What a write has staged since its last flush, and the decoder of its frames.
///
/// A writer may keep what it stages until it flushes, so the frames a host sends between two
/// flushes are bounded together.
struct Receiving {
    decoder: Decoder,
    limits: Limits,
    /// Bytes: the batch frames staged since the last flush, header and body.
    staged: u64,
}

impl Receiving {
    /// Applies one frame: a schema to the decoder, a batch decoded, or a flush.
    fn received(&mut self, frame: v1::WriteFrame) -> Result<Staged, ConnectorError> {
        use v1::write_frame::Frame;
        let bytes = u64::try_from(frame.encoded_len()).unwrap_or(u64::MAX);
        let step = match frame.frame {
            Some(Frame::Schema(schema)) => {
                self.decoder
                    .schema(&schema.ipc_schema)
                    .map_err(|error| frame_error(&error))?;
                Step::Kept
            }
            Some(Frame::Batch(batch)) => {
                let frame = IpcFrame {
                    header: batch.data_header,
                    body: batch.data_body,
                };
                let held = frame.header.len().saturating_add(frame.body.len());
                let staged = self
                    .staged
                    .saturating_add(u64::try_from(held).unwrap_or(u64::MAX));
                // Refused before it is decoded: nothing more is held than the limit allows.
                self.limits
                    .admit_staged(staged)
                    .map_err(|refusal| frame_error(&rdlt_wire::WireError::Refused(refusal)))?;
                let decoded = self
                    .decoder
                    .frame(&frame)
                    .map_err(|error| frame_error(&error))?;
                self.staged = staged;
                match decoded {
                    Some(decoded) => Step::Write(SegmentId(batch.segment), decoded),
                    None => Step::Kept,
                }
            }
            Some(Frame::Flush(_)) => {
                // The host counts afresh once it sends the flush, so the receiver does too.
                self.staged = 0;
                Step::Flush
            }
            Some(Frame::Start(_)) | None => {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    "a write frame other than the first started the write",
                )
                .with_code("invalid_message"));
            }
        };
        Ok(Staged { bytes, step })
    }
}

/// Has the writer take each frame received, in order: answers its credit, then stages its batch
/// or flushes and answers the stats; a failure, the receiver's or the writer's, is answered with
/// its error, after which the write ends.
async fn write(
    mut writer: Box<dyn DestinationWriter>,
    mut taken: mpsc::Receiver<Result<Staged, ConnectorError>>,
    acks: mpsc::Sender<Result<v1::WriteAck, Status>>,
    limits: Limits,
) {
    use v1::write_ack::Ack;
    let answer = |ack| acks.send(Ok(v1::WriteAck { ack: Some(ack) }));
    let mut granting = Granting::new(CREDIT_FLOOR, &limits);
    let opening = v1::Credit {
        bytes: granting.opening(),
    };
    if answer(Ack::Credit(opening)).await.is_err() {
        return;
    }
    while let Some(staged) = taken.recv().await {
        let done = match staged {
            Ok(Staged { bytes, step }) => {
                let credit = v1::Credit {
                    bytes: granting.taken(bytes),
                };
                if answer(Ack::Credit(credit)).await.is_err() {
                    return;
                }
                match step {
                    Step::Kept => Ok(None),
                    Step::Write(segment, batch) => {
                        writer.write(segment, batch).await.map(|()| None)
                    }
                    Step::Flush => writer.flush().await.map(Some),
                }
            }
            Err(error) => Err(error),
        };
        let ack = match done {
            Ok(None) => continue,
            Ok(Some(stats)) => Ack::Flushed(v1::WriteStats::from(stats)),
            Err(error) => Ack::Error(v1::Error::from(&error)),
        };
        let failed = matches!(ack, Ack::Error(_));
        if answer(ack).await.is_err() || failed {
            return;
        }
    }
}
