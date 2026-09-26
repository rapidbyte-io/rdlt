//! A served read: the source reads a partition into a channel, and each event it sends becomes
//! frames sent to the host within the credit the host grants.

use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use rdlt_wire::prost::Message as _;
use rdlt_wire::tonic::{Status, Streaming};
use rdlt_wire::{Encoder, Limits};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use super::service::{Answer, invalid};
use crate::Cursor;
use crate::error::ConnectorErrorKind;
use crate::id::{PartitionId, StreamName};
use crate::sink::{LogLevel, PartitionFeed, Push, SourceEvent, partition_channel};
use crate::source::{Partition, ReadRequest, Source};
use crate::wire::{Invalid, frame_error, status, v1};

/// Events the source may send ahead of the frames waiting for credit.
const EVENTS: NonZeroUsize = NonZeroUsize::new(16).expect("sixteen is not zero");

/// Starts serving the read the host's first message asks for.
pub(super) async fn serve(
    source: Arc<dyn Source>,
    host: Limits,
    mut controls: Streaming<v1::ReadControl>,
) -> Result<Answer<v1::ReadFrame>, Status> {
    let Some(v1::ReadControl {
        control: Some(v1::read_control::Control::Start(start)),
    }) = controls.message().await?
    else {
        return Err(invalid(&Invalid::Missing("read start")));
    };
    let request = request(start).map_err(|error| invalid(&error))?;
    let (frames, answer) = mpsc::channel(EVENTS.get());
    tokio::spawn(pump(source, request, controls, frames, host));
    Ok(Box::pin(ReceiverStream::new(answer)))
}

fn request(start: v1::ReadStart) -> Result<ReadRequest, Invalid> {
    let stream = StreamName::try_from(start.stream.ok_or(Invalid::Missing("stream"))?)?;
    let partition = PartitionId::parse(start.partition)
        .map_err(|error| Invalid::rejected("partition id", error))?;
    Ok(ReadRequest {
        stream,
        partition: Partition::new(partition),
        cursor: start.cursor.map(Cursor::try_from).transpose()?,
    })
}

/// Frames waiting for credit, and what encodes them.
struct Outbox {
    frames: VecDeque<v1::ReadFrame>,
    credit: i64,
    encoder: Encoder,
    schema: Option<SchemaRef>,
    epoch: u64,
    /// The host's limits, which every frame sent keeps within.
    host: Limits,
}

/// A frame the host's limits refuse, as the status the read fails with.
fn refused(refusal: rdlt_wire::Refusal) -> Status {
    status(&frame_error(&rdlt_wire::WireError::Refused(refusal)))
}

impl Outbox {
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
            SourceEvent::Log { level, message } => {
                self.host.admit_string(&message).map_err(refused)?;
                self.push(Frame::Log(v1::LogFrame {
                    level: log_level(level) as i32,
                    message,
                }));
            }
            SourceEvent::Metric { name, value } => {
                self.host.admit_string(&name).map_err(refused)?;
                self.push(Frame::Metric(v1::MetricFrame { name, value }));
            }
        }
        Ok(())
    }

    /// Queues `batch`, after a schema frame opening a new epoch if its schema differs.
    fn batch(&mut self, batch: &RecordBatch, kind: v1::BatchKind) -> Result<(), Status> {
        use v1::read_frame::Frame;
        if self.schema.as_ref() != Some(&batch.schema()) {
            self.epoch += 1;
            let ipc_schema = self.encoder.schema(&batch.schema());
            self.schema = Some(batch.schema());
            self.push(Frame::Schema(v1::SchemaFrame {
                schema_epoch: self.epoch,
                ipc_schema,
            }));
        }
        let frames = self
            .encoder
            .batch(batch)
            .map_err(|error| status(&frame_error(&error)))?;
        for frame in frames {
            self.host
                .admit_frame(frame.header.len().saturating_add(frame.body.len()))
                .map_err(refused)?;
            self.push(Frame::Batch(v1::BatchFrame {
                schema_epoch: self.epoch,
                kind: kind as i32,
                data_header: frame.header,
                data_body: frame.body,
            }));
        }
        Ok(())
    }
}

fn log_level(level: LogLevel) -> v1::LogLevel {
    match level {
        LogLevel::Error => v1::LogLevel::Error,
        LogLevel::Warn => v1::LogLevel::Warn,
        LogLevel::Info => v1::LogLevel::Info,
        LogLevel::Debug => v1::LogLevel::Debug,
    }
}

/// Reads the partition and sends its frames within credit until the read is done, or the host
/// goes.
async fn pump(
    source: Arc<dyn Source>,
    request: ReadRequest,
    mut controls: Streaming<v1::ReadControl>,
    frames: mpsc::Sender<Result<v1::ReadFrame, Status>>,
    host: Limits,
) {
    let (sink, mut feed) = partition_channel(EVENTS);
    let mut reading = tokio::spawn(async move { source.read(request, sink).await });
    let mut outbox = Outbox {
        frames: VecDeque::new(),
        credit: 0,
        encoder: Encoder::default(),
        schema: None,
        epoch: 0,
        host,
    };
    let mut done = false;
    loop {
        while let Some(frame) = outbox.frames.front() {
            // A frame goes while credit remains, and spends its size, even below zero.
            if outbox.credit <= 0 {
                break;
            }
            outbox.credit -= i64::try_from(frame.encoded_len()).unwrap_or(i64::MAX);
            let frame = outbox.frames.pop_front().expect("a frame is at the front");
            if frames.send(Ok(frame)).await.is_err() {
                feed.stop();
                return;
            }
        }
        if done && outbox.frames.is_empty() {
            return;
        }
        tokio::select! {
            biased;
            control = controls.message() => {
                if !control_read(control, &mut outbox, &feed) {
                    feed.stop();
                    return;
                }
            }
            event = feed.recv(), if outbox.frames.is_empty() && !done => {
                let queued = if let Some(event) = event {
                    outbox.event(event)
                } else {
                    done = true;
                    finish(&mut outbox, (&mut reading).await)
                };
                if let Err(error) = queued {
                    // The host may have gone; the read ends either way.
                    frames.send(Err(error)).await.ok();
                    feed.stop();
                    return;
                }
            }
        }
    }
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
            let bytes = i64::try_from(credit.bytes).unwrap_or(i64::MAX);
            outbox.credit = outbox.credit.saturating_add(bytes);
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
        Err(join) => Err(status(&crate::error::ConnectorError::new(
            ConnectorErrorKind::Internal,
            format!("the read failed: {join}"),
        ))),
    }
}
