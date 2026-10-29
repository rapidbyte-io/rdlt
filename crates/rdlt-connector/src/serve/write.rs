//! A served write, in three stages of one frame each: one reads the engine's frames for one
//! table, one decodes each, and the last has the destination's writer stage each batch, or flush
//! what it staged and answer with its stats.
//!
//! A frame decodes only once the writer has taken the frame before it, so a write holds what its
//! writer stages, within the staged bound, one decoded frame waiting for it, and one frame read
//! waiting to be decoded.

#[cfg(test)]
mod tests;

use arrow_array::RecordBatch;
use rdlt_wire::stream::{Call, Kind, Outgoing, Received};
use rdlt_wire::{Decoder, IpcFrame, Limits};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::service::Service;
use super::stream::{Frames, failed, invalid, message, opened, send};
use crate::destination::{DestinationWriter, TableRef};
use crate::error::{ConnectorError, ConnectorErrorKind};
use crate::id::SegmentId;
use crate::wire::{Invalid, frame_error, v1};

/// Serves the write `opening` starts, its frames read from `frames`, its answers written to `io`.
pub(super) async fn serve<R, W>(service: &Service, opening: &Received, frames: Frames<R>, mut io: W)
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send,
{
    // Read before the writer is awaited: nothing of the stream is held across it.
    let start = frames.start::<v1::WriteStart>(Call::Write, opening);
    let writer = async {
        let start = start?;
        let table = start
            .table
            .ok_or(Invalid::Missing("table"))
            .and_then(TableRef::try_from)
            .map_err(|error| super::service::invalid(&error))?;
        service.writer(start.session, &table).await
    };
    let writer = match writer.await {
        Ok(writer) => writer,
        Err(error) => {
            send(&mut io, failed(Call::Write, &error)).await;
            return;
        }
    };
    if send(&mut io, opened(Call::Write)).await {
        writing(writer, frames, io, service.limits).await;
    }
}

/// A frame decoded, as its writer takes it.
#[derive(Debug)]
enum Step {
    /// Nothing: a schema, or a dictionary the decoder keeps for the batches after it.
    Kept,
    /// To stage a batch.
    Write(SegmentId, RecordBatch),
    /// To flush what it staged.
    Flush,
}

/// Writes the frames of `frames` to `writer`, answering on `io`: read in one task, decoded in
/// another, and written in a third, whose answers this one sends.
async fn writing<R, W>(
    writer: Box<dyn DestinationWriter>,
    frames: Frames<R>,
    mut io: W,
    limits: Limits,
) where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send,
{
    let (arrived, received) = mpsc::channel(1);
    let (decoded, taken) = mpsc::channel(1);
    let (answering, mut answers) = mpsc::channel(1);
    let gone = CancellationToken::new();
    let reading = Stage(tokio::spawn(read(frames, arrived, gone.clone())));
    let receiving = Stage(tokio::spawn(receive(received, decoded, limits)));
    let taking = Stage(tokio::spawn(take(writer, taken, answering)));
    loop {
        let answer = tokio::select! {
            biased;
            // A host that goes ends the write once the reader sees it go, at once where nothing
            // it sent waits to be read, whatever the writer waits for; the writer is dropped
            // with what it staged, which no commit published.
            () = gone.cancelled() => return,
            answer = answers.recv() => answer,
        };
        let Some(answer) = answer else {
            break;
        };
        if !send(&mut io, answer).await {
            return;
        }
    }
    // A stage that panicked fails the write once the writer has answered all it will, as a read's
    // does, without the join error's text, which may quote what was written. The stages before
    // the writer ended before it, unless it ended the write itself.
    let panicked = match taking.joined().await {
        Ok(true) => return,
        Ok(false) => match receiving.joined().await {
            Ok(()) => reading.joined().await.err(),
            Err(join) => Some(join),
        },
        Err(join) => Some(join),
    };
    if let Some(join) = panicked {
        let error = ConnectorError::new(
            ConnectorErrorKind::Internal,
            if join.is_panic() {
                "the write panicked"
            } else {
                "the write was cancelled"
            },
        );
        send(&mut io, failed(Call::Write, &error)).await;
    }
}

/// A stage of a write, aborted once dropped.
struct Stage<T>(tokio::task::JoinHandle<T>);

impl<T> Stage<T> {
    /// What the stage ended with, once it has.
    async fn joined(mut self) -> Result<T, tokio::task::JoinError> {
        (&mut self.0).await
    }
}

impl<T> Drop for Stage<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Reads the engine's frames, each once the stage after has room for it, until the engine ends
/// the write, a frame fails it, or the stage after stops; cancels `gone` where the engine went.
async fn read<R: AsyncRead + Unpin>(
    mut frames: Frames<R>,
    arrived: mpsc::Sender<Result<Received, ConnectorError>>,
    gone: CancellationToken,
) {
    loop {
        // Room first: a frame is read only once the decoder has taken the frame before it. The
        // next head is awaited meanwhile, so an engine that ends or leaves the write between
        // frames is seen at once, even while the writer holds the frames before it.
        let (mut headed, mut refused) = (false, None);
        let slot = loop {
            tokio::select! {
                biased;
                slot = arrived.reserve() => match slot {
                    Ok(slot) => break slot,
                    Err(_) => return,
                },
                more = frames.more(), if !headed => match more {
                    Ok(true) => headed = true,
                    Ok(false) => {
                        gone.cancel();
                        return;
                    }
                    Err(error) if error.kind() == ConnectorErrorKind::Transient => {
                        gone.cancel();
                        return;
                    }
                    Err(error) => (headed, refused) = (true, Some(error)),
                },
            }
        };
        if let Some(error) = refused {
            slot.send(Err(error));
            return;
        }
        let frame = match frames.next().await {
            Ok(Some(frame)) => Ok(frame),
            // The engine ended the write, or left it: nothing it sent is to be answered.
            Ok(None) => {
                gone.cancel();
                return;
            }
            Err(error) if error.kind() == ConnectorErrorKind::Transient => {
                gone.cancel();
                return;
            }
            Err(error) => Err(error),
        };
        let failed = frame.is_err();
        slot.send(frame);
        if failed {
            return;
        }
    }
}

/// Decodes each frame read, once the writer has taken the frame before it, until the reading
/// ends, a frame fails the write, or the writer stops.
async fn receive(
    mut received: mpsc::Receiver<Result<Received, ConnectorError>>,
    decoded: mpsc::Sender<Result<Step, ConnectorError>>,
    limits: Limits,
) {
    let mut receiving = Receiving {
        decoder: Decoder::new(limits),
        limits,
        staged: 0,
    };
    loop {
        // Room first: a frame is taken from the reader only once the writer has taken the frame
        // before it, so the write holds one frame waiting to be decoded, not two.
        let Ok(slot) = decoded.reserve().await else {
            return;
        };
        let Some(frame) = received.recv().await else {
            return;
        };
        let step = frame.and_then(|frame| receiving.received(frame));
        let failed = step.is_err();
        slot.send(step);
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
    /// Applies one frame: a schema to the decoder, a batch decoded where it arrived, or a flush.
    fn received(&mut self, frame: Received) -> Result<Step, ConnectorError> {
        use v1::write_frame::Frame;
        let (segment, dictionary) = match frame.head.kind {
            Kind::Batch {
                tag, dictionary, ..
            } => (SegmentId(tag), dictionary),
            Kind::Message { .. } => {
                let message: v1::WriteFrame = message(Call::Write, &self.limits, &frame)?;
                return match message.frame {
                    Some(Frame::Schema(schema)) => {
                        self.decoder
                            .schema(&schema.ipc_schema)
                            .map_err(|error| frame_error(&error))?;
                        Ok(Step::Kept)
                    }
                    Some(Frame::Flush(_)) => {
                        // The host counts afresh once it sends the flush, so the receiver does too.
                        self.staged = 0;
                        Ok(Step::Flush)
                    }
                    None => Err(invalid("an empty write frame")),
                };
            }
            Kind::Json => return Err(invalid("a JSON frame on a write")),
        };
        let held = IpcFrame::held_bytes(frame.body.bytes.len());
        let staged = self
            .staged
            .saturating_add(u64::try_from(held).unwrap_or(u64::MAX));
        // Refused before it is read as a message: nothing more is held than the limit allows.
        self.limits
            .admit_staged(staged)
            .map_err(|refusal| frame_error(&rdlt_wire::WireError::Refused(refusal)))?;
        let frame = IpcFrame::framed(frame.body).map_err(|error| frame_error(&error))?;
        let decoded = self
            .decoder
            .frame(&frame)
            .map_err(|error| frame_error(&error))?;
        self.staged = staged;
        match (decoded, dictionary) {
            (Some(decoded), false) => Ok(Step::Write(segment, decoded)),
            (None, true) => Ok(Step::Kept),
            _ => Err(mislabelled()),
        }
    }
}

/// The error of a batch frame whose message is not what its flags say: a dictionary flagged as
/// rows, or rows as a dictionary.
pub(super) fn mislabelled() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Internal,
        "a batch frame whose message is not what its flags say",
    )
    .with_code(crate::wire::MALFORMED_FRAME)
}

/// Has the writer take each frame decoded, in order: stages its batch, or flushes and answers
/// the stats; and whether the write ended answered, or its host went, rather than its stages
/// having stopped.
///
/// A failure, the stages' or the writer's, is answered with its error, after which the write
/// ends.
async fn take(
    mut writer: Box<dyn DestinationWriter>,
    mut taken: mpsc::Receiver<Result<Step, ConnectorError>>,
    answers: mpsc::Sender<Outgoing>,
) -> bool {
    use v1::write_ack::Ack;
    while let Some(step) = taken.recv().await {
        let done = match step {
            Ok(Step::Kept) => Ok(None),
            Ok(Step::Write(segment, batch)) => writer.write(segment, batch).await.map(|()| None),
            Ok(Step::Flush) => writer.flush().await.map(Some),
            Err(error) => Err(error),
        };
        let frame = match done {
            Ok(None) => continue,
            Ok(Some(stats)) => {
                let ack = v1::WriteAck {
                    ack: Some(Ack::Flushed(v1::WriteStats::from(stats))),
                };
                Outgoing::message(None, &ack).expect("stats fit a head")
            }
            Err(error) => {
                answers.send(failed(Call::Write, &error)).await.ok();
                return true;
            }
        };
        if answers.send(frame).await.is_err() {
            return true;
        }
    }
    false
}
