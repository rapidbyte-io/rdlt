//! A served read: the source reads a partition into a channel, its pushes within a window of the
//! host's frame limit, and each event it sends becomes frames sent to the host within the credit
//! the host grants.

#[cfg(test)]
mod tests;
mod window;

use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use bytes::Bytes;
use rdlt_wire::flow::Spending;
use rdlt_wire::plane::{Answers, Chained as _, Incoming};
use rdlt_wire::tonic::Status;
use rdlt_wire::{Cut, Encoder, Limits};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use super::noted::Read;
use super::service::invalid;
use crate::Cursor;
use crate::error::ConnectorErrorKind;
use crate::id::{PartitionId, StreamName};
use crate::sink::{
    Admission, PartitionFeed, Push, SourceEvent, admitted_partition_channel,
};
use crate::source::{Partition, ReadRequest, Source};
use crate::wire::{Invalid, frame_error, status, v1};
use window::Window;

/// Events the source may send ahead of the frames waiting for credit.
const EVENTS: NonZeroUsize = NonZeroUsize::new(16).expect("sixteen is not zero");

/// Starts serving the read the host's first message asks for, whose cursor keeps within `own`,
/// this end's limits; what it sends keeps within `host`, the lesser of the host's and its own.
///
/// Answers its frames, and which stream and partition it reads.
pub(super) async fn serve(
    source: Arc<dyn Source>,
    own: Limits,
    host: Limits,
    mut controls: Incoming<v1::ReadControl>,
) -> Result<(Answers<v1::ReadFrame>, Read), Status> {
    let Some(v1::ReadControl {
        control: Some(v1::read_control::Control::Start(start)),
    }) = controls.message().await?
    else {
        return Err(invalid(&Invalid::Missing("read start")));
    };
    if let Some(cursor) = &start.cursor {
        own.admit_cursor(cursor.bytes.len()).map_err(refused)?;
    }
    let barrier = start.barrier;
    let request = request(start).map_err(|error| invalid(&error))?;
    let read = Read {
        stream: request.stream.clone(),
        partition: request.partition.id().clone(),
        start: request.cursor.clone(),
    };
    let (frames, answer) = mpsc::channel(EVENTS.get());
    let window = Arc::new(Window::new(host));
    tokio::spawn(pump(source, request, barrier, controls, frames, window));
    Ok((Box::pin(ReceiverStream::new(answer)), read))
}

fn request(start: v1::ReadStart) -> Result<ReadRequest, Invalid> {
    let stream = StreamName::try_from(start.stream.ok_or(Invalid::Missing("stream"))?)?;
    let partition = PartitionId::parse(start.partition)
        .map_err(|error| Invalid::rejected("partition id", error))?;
    let partition = if start.unbounded {
        Partition::new(partition).unbounded()
    } else {
        Partition::new(partition)
    };
    Ok(ReadRequest::new(
        stream,
        partition,
        start.cursor.map(Cursor::try_from).transpose()?,
    )
    .following(start.follow))
}

/// Frames waiting for credit, and what encodes them.
pub(super) struct Outbox {
    frames: VecDeque<v1::ReadFrame>,
    credit: Spending,
    encoder: Encoder,
    schema: Option<SchemaRef>,
    epoch: u64,
    /// The host's limits, which every frame sent keeps within.
    host: Limits,
    /// The batch whose pieces are being sent, and its kind.
    cutting: Option<(Cut, v1::BatchKind)>,
}

/// A frame the host's limits refuse, as the status the read fails with.
fn refused(refusal: rdlt_wire::Refusal) -> Status {
    status(&frame_error(&rdlt_wire::WireError::Refused(refusal)))
}

impl Outbox {
    /// An empty outbox, whose frames keep within `host`'s limits.
    pub(super) fn new(host: Limits) -> Self {
        Self {
            frames: VecDeque::new(),
            credit: Spending::default(),
            encoder: Encoder::default(),
            schema: None,
            epoch: 0,
            host,
            cutting: None,
        }
    }

    /// The frames queued, in order, which leave the outbox.
    #[cfg(feature = "certify")]
    pub(super) fn take_frames(&mut self) -> VecDeque<v1::ReadFrame> {
        std::mem::take(&mut self.frames)
    }

    fn push(&mut self, frame: v1::read_frame::Frame) {
        self.frames.push_back(v1::ReadFrame { frame: Some(frame) });
    }

    /// Queues `event` as the frames that carry it.
    fn event(&mut self, event: SourceEvent) -> Result<(), Status> {
        use v1::read_frame::Frame;
        match event {
            SourceEvent::Push(Push::Arrow(batch)) => self.batch(&batch, v1::BatchKind::Arrow)?,
            SourceEvent::Push(Push::Changes(batch)) => self.batch(&batch, v1::BatchKind::Change)?,
            SourceEvent::Push(Push::Json(data)) => {
                self.host.admit_json(data.len()).map_err(refused)?;
                self.push(Frame::Json(v1::JsonFrame { data }));
            }
            SourceEvent::Checkpoint { cursor, answers } => {
                let cursor = v1::Cursor::from(&cursor);
                self.host
                    .admit_cursor(cursor.bytes.len())
                    .map_err(refused)?;
                self.push(Frame::Checkpoint(v1::CheckpointFrame {
                    cursor: Some(cursor),
                    barrier: answers,
                }));
            }
            SourceEvent::Replan => self.push(Frame::Replan(v1::ReplanFrame {})),
            SourceEvent::Behind { records } => {
                self.push(Frame::Behind(v1::BehindFrame { records }));
            }
        }
        Ok(())
    }

    /// Queues `batch`, after a schema frame opening a new epoch if its schema differs.
    pub(super) fn batch(&mut self, batch: &RecordBatch, kind: v1::BatchKind) -> Result<(), Status> {
        use v1::read_frame::Frame;
        if self.schema.as_ref() != Some(&batch.schema()) {
            self.epoch += 1;
            self.schema = None;
            let ipc_schema = self
                .encoder
                .schema(&batch.schema())
                .map_err(|error| status(&frame_error(&error)))?;
            self.schema = Some(batch.schema());
            self.push(Frame::Schema(v1::SchemaFrame {
                schema_epoch: self.epoch,
                ipc_schema,
            }));
        }
        self.cutting = Some((Cut::new(batch.clone(), self.host), kind));
        self.refill()
    }

    /// Whether a batch is still being cut: its next piece is queued by [`Outbox::refill`], and
    /// nothing else may be queued before its last.
    pub(super) fn cutting(&self) -> bool {
        self.cutting.is_some()
    }

    /// Queues the next piece of the batch being cut.
    ///
    /// A batch beyond the host's limits goes as several, in order, each encoded only once the
    /// piece before it was sent; a row beyond them fails the read.
    pub(super) fn refill(&mut self) -> Result<(), Status> {
        use v1::read_frame::Frame;
        let Some((cut, kind)) = &mut self.cutting else {
            return Ok(());
        };
        let kind = *kind;
        let piece = self.encoder.piece(cut);
        if cut.is_done() {
            self.cutting = None;
        }
        let frames = piece.map_err(|error| status(&frame_error(&error)))?;
        for frame in frames.into_iter().flatten() {
            self.push(Frame::Batch(v1::BatchFrame {
                schema_epoch: self.epoch,
                kind: kind as i32,
                data_header: frame.header,
                padding: Bytes::new(),
                data_body: frame.body,
            }));
        }
        Ok(())
    }
}

/// A spawned read, aborted once dropped: a read the host leaves, which may wait for data that
/// never comes, ends with the pump.
struct Reading(tokio::task::JoinHandle<crate::error::Result<()>>);

impl Drop for Reading {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Reads the partition and sends its frames within credit until the read is done, or the host
/// goes; `admission` admits what the source sends into the channel, within the host's limits.
async fn pump(
    source: Arc<dyn Source>,
    request: ReadRequest,
    barrier: u64,
    mut controls: Incoming<v1::ReadControl>,
    frames: mpsc::Sender<Result<v1::ReadFrame, Status>>,
    admission: Arc<dyn Admission>,
) {
    let mut outbox = Outbox::new(admission.limits());
    let (sink, mut feed) = admitted_partition_channel(EVENTS, admission);
    // What the source pushes is cut to the host's frames as it is sent.
    let sink = sink.cut();
    // A barrier pending when the read started is the source's to answer from its first push.
    if let Some(barrier) = std::num::NonZeroU64::new(barrier) {
        feed.request_checkpoint(barrier.get());
    }
    let mut reading = Reading(tokio::spawn(
        async move { source.read(request, sink).await },
    ));
    let (mut done, mut exhausted) = (false, false);
    loop {
        if !send_queued(&mut outbox, &frames).await {
            feed.stop();
            return;
        }
        // The next piece of a batch being cut is encoded once the last was sent, and before
        // anything else the source sends is taken. The host's controls wait meanwhile, for no
        // longer than the credit lasts or the batch has pieces.
        if outbox.frames.is_empty() && outbox.cutting() {
            if !refill(&mut outbox, &frames).await {
                feed.stop();
                return;
            }
            tokio::task::yield_now().await;
            continue;
        }
        if done && outbox.frames.is_empty() {
            return;
        }
        let queued = tokio::select! {
            biased;
            // A host that dropped its answers takes nothing more: it wins over everything else.
            () = frames.closed() => {
                feed.stop();
                return;
            }
            control = controls.message() => {
                if !control_read(control, &mut outbox, &feed) {
                    feed.stop();
                    return;
                }
                Ok(())
            }
            event = feed.recv(), if outbox.frames.is_empty() && !exhausted => {
                if let Some(event) = event {
                    outbox.event(event)
                } else {
                    exhausted = true;
                    Ok(())
                }
            }
            // A source may clean up after its sink is gone, while the host's controls are read.
            ended = &mut reading.0, if exhausted && !done => {
                done = true;
                finish(&mut outbox, ended)
            }
        };
        if let Err(error) = queued {
            // The host may have gone; the read ends either way.
            frames.send(Err(error)).await.ok();
            feed.stop();
            return;
        }
    }
}

/// Sends the frames queued while credit lasts; whether the host is still there to take them.
async fn send_queued(
    outbox: &mut Outbox,
    frames: &mpsc::Sender<Result<v1::ReadFrame, Status>>,
) -> bool {
    while let Some(frame) = outbox.frames.front() {
        // A frame goes while credit remains, and spends its size, even below zero.
        if !outbox.credit.may_send() {
            break;
        }
        let size = u64::try_from(frame.sent_len()).unwrap_or(u64::MAX);
        outbox.credit.spend(size);
        let frame = outbox.frames.pop_front().expect("a frame is at the front");
        if frames.send(Ok(frame)).await.is_err() {
            return false;
        }
    }
    true
}

/// Encodes the next piece of the batch being cut; whether the read goes on, which it does not
/// where the piece cannot be encoded and the host is told so.
async fn refill(outbox: &mut Outbox, frames: &mpsc::Sender<Result<v1::ReadFrame, Status>>) -> bool {
    if let Err(error) = outbox.refill() {
        frames.send(Err(error)).await.ok();
        return false;
    }
    true
}

/// Applies the host's `control`; whether the host is still there.
fn control_read(
    control: Result<Option<v1::ReadControl>, Status>,
    outbox: &mut Outbox,
    feed: &PartitionFeed,
) -> bool {
    use v1::read_control::Control;
    let Ok(Some(control)) = control else {
        return false;
    };
    match control.control {
        Some(Control::Credit(credit)) => {
            // A credit of no bytes grants nothing, and changes nothing.
            outbox.credit.grant(credit.bytes).ok();
        }
        Some(Control::Checkpoint(request)) => feed.request_checkpoint(request.barrier),
        Some(Control::Stop(_)) => feed.stop(),
        Some(Control::Start(_)) | None => {}
    }
    true
}

/// Queues the frame ending the read, from how the source's read ended.
fn finish(
    outbox: &mut Outbox,
    ended: Result<crate::error::Result<()>, tokio::task::JoinError>,
) -> Result<(), Status> {
    match ended {
        Ok(Ok(())) => {
            outbox.push(v1::read_frame::Frame::Done(v1::Done {}));
            Ok(())
        }
        Ok(Err(error)) => Err(status(&error)),
        // The join error's text carries a process-wide task number and the panic's message,
        // which may quote what was read; neither is this connector's to tell its host.
        Err(join) => Err(status(&crate::error::ConnectorError::new(
            ConnectorErrorKind::Internal,
            if join.is_panic() {
                "the read panicked"
            } else {
                "the read was cancelled"
            },
        ))),
    }
}
