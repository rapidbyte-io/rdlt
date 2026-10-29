//! A remote read: the connector's frames, each reserved from what the engine's pushes may take
//! before its body is read into place on a reader of their own, become the events of the
//! engine's partition sink; and the engine's requests go to the connector.
//!
//! A push's frame is reserved once: decoded, its reservation shrinks to what the push holds and
//! admits it. A batch's reservation counts what the read's decoder keeps, its schema and its
//! dictionaries, which a batch decoded beside them holds too, and the arrays of a batch of the
//! schema; so the reader reserves a batch only once every schema and dictionary before it is
//! decoded. The reader decodes each message
//! itself, to know a schema from the rest.

mod frames;
#[cfg(test)]
mod tests;

use std::sync::Arc;

use rdlt_connector::wire::v1::read_control::Control;
use rdlt_connector::wire::{Invalid, frame_error, v1};
use rdlt_connector::{
    ConnectorError, ConnectorErrorKind, Cursor, LogLevel, PartitionSink, Permit, Push, ReadRequest,
    Requested, SourceEvent,
};
use rdlt_wire::limits::count;
use rdlt_wire::stream::{Call, Kind, Outgoing, Received, Rows};
use rdlt_wire::{Decoder, IpcFrame, Limits};
use tokio::io::AsyncWrite;
use tokio::sync::mpsc;

use super::source::invalid;
use super::{Connection, lost_error};
#[cfg(test)]
use frames::message;
pub(crate) use frames::reservation;
use frames::{Arrived, Frames, Keeping, Payload, Reading, read};

/// Reads `request`'s partition from the connector into `sink`.
pub(super) async fn run(
    connection: &Connection,
    request: ReadRequest,
    mut sink: PartitionSink,
) -> rdlt_connector::Result<()> {
    // A barrier raised before the read starts goes with its start, so a read too short to see a
    // later control answers it, as in the engine's process.
    let pending = sink.pending_barrier().unwrap_or(0);
    let opened = connection
        .open_call(Call::Read, &start(&request, pending))
        .await?;
    let keeping = Arc::new(Keeping::default());
    let (arrived, mut frames) = mpsc::channel(1);
    let reader = Frames {
        reader: opened.reader,
        reserved: None,
        reserving: sink.reserving(),
        keeping: Arc::clone(&keeping),
    };
    let _reading = Reading(tokio::spawn(read(reader, arrived)));
    let mut controls = opened.writer;
    let mut reader = Reader::new(connection.options.limits);
    let (mut forwarded, mut stopping) = (pending, false);
    let mut kept = Kept::default();
    let mut writing: Option<Outgoing> = None;
    loop {
        let idle = writing.is_none();
        tokio::select! {
            biased;
            () = connection.lost.cancelled() => return Err(lost_error()),
            requested = sink.requested(forwarded), if idle && !stopping => {
                let control = match requested {
                    Requested::Checkpoint(barrier) => {
                        forwarded = barrier;
                        Control::Checkpoint(v1::CheckpointRequest { barrier })
                    }
                    Requested::Stop => {
                        stopping = true;
                        Control::Stop(v1::Stop { mode: v1::StopMode::Now as i32 })
                    }
                };
                writing = Outgoing::message(None, &v1::ReadControl { control: Some(control) }).ok();
            }
            // A connector gone fails the frames' reader; nothing more is sent to it.
            written = write(&mut writing, &mut controls), if !idle => stopping |= !written,
            arrived = frames.recv() => {
                let Some(arrived) = arrived else {
                    return Err(unfinished());
                };
                let Arrived { payload, reserved, settles } = arrived?;
                let read = reader.event(payload)?;
                if settles {
                    // What the decoder keeps changed: the reader reserves the next batch by it.
                    keeping.settled(reader.beside());
                }
                match read {
                    Read::Done => return Ok(()),
                    Read::Event(event) => forward(&mut sink, event, reserved, forwarded).await?,
                    // A schema and a dictionary wait in the decoder for the batches that use them;
                    // a dictionary's reservation goes once the decoder's own charge holds it.
                    Read::Nothing => {
                        kept.charge(&sink, reader.kept())?;
                        drop(reserved);
                    }
                }
            }
        }
    }
}

/// The error of a read whose connector ended its stream without saying it was done.
fn unfinished() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Transient,
        "the connector ended the read without saying it was done",
    )
    .with_code(super::CONNECTOR_LOST)
}

/// The start of a read of `request`, answering `barrier` where it is not zero.
fn start(request: &ReadRequest, barrier: u64) -> v1::ReadStart {
    v1::ReadStart {
        stream: Some(v1::StreamName::from(&request.stream)),
        partition: request.partition.id().as_str().to_owned(),
        cursor: request.cursor.as_ref().map(v1::Cursor::from),
        barrier,
        unbounded: request.partition.is_unbounded(),
        follow: request.follow,
    }
}

/// Writes what is left of the frame `writing` holds to `io`; whether the connector took it.
async fn write<W: AsyncWrite + Unpin>(writing: &mut Option<Outgoing>, io: &mut W) -> bool {
    let Some(frame) = writing else {
        return true;
    };
    let written = frame.write(io).await.is_ok();
    *writing = None;
    written
}

/// Sends `event` to the engine within `reserved`, the reservation of the frame it came from,
/// where a checkpoint in it answers no barrier beyond `forwarded`, the newest the host forwarded.
///
/// A send the engine stopped fails as stopped, even one that waited for room: the read's next
/// turn forwards the stop, and the read drains.
///
/// # Errors
///
/// A checkpoint answering a barrier the host did not forward, a push beyond what its frame
/// reserved, and why its admission refused the event, where it did.
async fn forward(
    sink: &mut PartitionSink,
    event: SourceEvent,
    reserved: Option<Permit>,
    forwarded: u64,
) -> rdlt_connector::Result<()> {
    if let SourceEvent::Checkpoint {
        answers: Some(barrier),
        ..
    } = &event
        && *barrier > forwarded
    {
        return Err(invalid(&Invalid::OutOfRange("checkpoint barrier")));
    }
    let sent = match reserved {
        Some(reserved) => sink.send_reserved(event, reserved).await,
        None => sink.send(event).await,
    };
    match sent {
        Err(refused) if refused.kind() != ConnectorErrorKind::Stopped => Err(refused),
        _ => Ok(()),
    }
}

/// What holds the bytes a read's decoder keeps between batches, its schema and its
/// dictionaries, charged to whoever admits the read's events for as long as the decoder keeps
/// them.
#[derive(Default)]
struct Kept {
    bytes: u64,
    held: Option<Permit>,
}

impl Kept {
    /// Charges `sink`'s admission the `bytes` the decoder now keeps, in place of what it kept.
    ///
    /// What it kept is released first, so the two are never charged together; and whatever ends
    /// the read, its return, its failure or its being dropped, releases what is held, once.
    ///
    /// # Errors
    ///
    /// The refusal of whoever admits the read's events, where the read would keep more than a
    /// read may: the read fails at the frame that would pass it.
    fn charge(&mut self, sink: &PartitionSink, bytes: u64) -> rdlt_connector::Result<()> {
        if bytes != self.bytes {
            (self.held, self.bytes) = (None, 0);
            self.held = sink.reserve(bytes)?;
            self.bytes = bytes;
        }
        Ok(())
    }
}

/// What one frame means to the read.
enum Read {
    /// An event for the engine.
    Event(SourceEvent),
    /// The source's read returned.
    Done,
    /// Nothing the engine sees: a schema, or a dictionary.
    Nothing,
}

/// Decodes a read's frames, within this end's limits; schema epochs only grow.
struct Reader {
    decoder: Decoder,
    limits: Limits,
    epoch: Option<u64>,
    /// Bytes: what the schema the decoder holds takes, with the message it came from.
    schema: u64,
    /// Bytes: what the arrays of a batch of that schema take, as its push is charged them.
    arrays: u64,
    /// Frames since the last event of a read's data that carried no event.
    quiet: u64,
    /// Frames since the last event of a read's data that carried an event the engine takes for
    /// nothing: a log line, a metric, a lag or a replan.
    free: u64,
}

/// The frames whose events the engine takes for nothing, log lines, metrics, lags and replans,
/// a read may send between two of its data's: rows or a checkpoint.
pub const MAX_FREE_FRAMES: u64 = 1024;

impl Reader {
    /// A reader of frames within `limits`, which has read none.
    fn new(limits: Limits) -> Self {
        Self {
            decoder: Decoder::new(limits),
            limits,
            epoch: None,
            schema: 0,
            arrays: 0,
            quiet: 0,
            free: 0,
        }
    }

    /// Bytes: what the decoder keeps between batches, its schema and its dictionaries.
    fn kept(&self) -> u64 {
        self.schema.saturating_add(self.decoder.dictionary_bytes())
    }

    /// Bytes: what a batch's push holds beside its frame, and is charged: what the decoder keeps,
    /// which the batch holds too, and the batch's arrays.
    fn beside(&self) -> u64 {
        self.kept().saturating_add(self.arrays)
    }

    /// What `frame` means to the read.
    ///
    /// Frames the engine takes for nothing are not slowed by it, so each kind is bounded between
    /// two events of the read's data: schemas and dictionaries, which carry no event, to a schema
    /// and a dictionary for each column it may have; log lines, metrics, lags and replans to
    /// [`MAX_FREE_FRAMES`].
    fn event(&mut self, payload: Payload) -> rdlt_connector::Result<Read> {
        let read = match payload {
            Payload::Data(frame) => self.decoded(frame)?,
            Payload::Message(message) => self.message(message)?,
        };
        let (count, limit, what) = match &read {
            Read::Nothing => (
                &mut self.quiet,
                self.limits.schema_columns.saturating_add(1),
                "frames between events that carry none",
            ),
            Read::Event(
                SourceEvent::Log { .. }
                | SourceEvent::Metric { .. }
                | SourceEvent::Behind { .. }
                | SourceEvent::Replan,
            ) => (
                &mut self.free,
                MAX_FREE_FRAMES,
                "log, metric, lag and replan frames between events",
            ),
            Read::Event(_) | Read::Done => {
                (self.quiet, self.free) = (0, 0);
                return Ok(read);
            }
        };
        *count += 1;
        if *count > limit {
            return Err(invalid(&Invalid::OutOfRange(what)));
        }
        Ok(read)
    }

    fn decoded(&mut self, frame: Received) -> rdlt_connector::Result<Read> {
        match frame.head.kind {
            Kind::Batch {
                rows,
                dictionary,
                tag,
            } => self.batch(frame, rows, dictionary, tag),
            Kind::Json => {
                let text = frame.body.bytes;
                self.limits
                    .admit_json(text.len())
                    .map_err(|refusal| frame_error(&rdlt_wire::WireError::Refused(refusal)))?;
                Ok(Read::Event(SourceEvent::Push(Push::Json(text))))
            }
            Kind::Message { .. } => Err(invalid(&Invalid::Unknown("read frame"))),
        }
    }

    fn message(&mut self, frame: v1::ReadFrame) -> rdlt_connector::Result<Read> {
        use v1::read_frame::Frame;
        let refused = |refusal| frame_error(&rdlt_wire::WireError::Refused(refusal));
        Ok(
            match frame
                .frame
                .ok_or_else(|| invalid(&Invalid::Missing("read frame")))?
            {
                Frame::Schema(schema) => self.schema(&schema)?,
                Frame::Checkpoint(checkpoint) => {
                    let cursor = checkpoint
                        .cursor
                        .ok_or_else(|| invalid(&Invalid::Missing("cursor")))?;
                    self.limits
                        .admit_cursor(cursor.bytes.len())
                        .map_err(refused)?;
                    let cursor = Cursor::try_from(cursor).map_err(|error| invalid(&error))?;
                    Read::Event(SourceEvent::Checkpoint {
                        cursor,
                        answers: checkpoint.barrier,
                    })
                }
                Frame::Log(log) => {
                    self.limits.admit_string(&log.message).map_err(refused)?;
                    Read::Event(SourceEvent::Log {
                        level: log_level(log.level)?,
                        message: log.message,
                    })
                }
                Frame::Metric(metric) => {
                    self.limits.admit_string(&metric.name).map_err(refused)?;
                    Read::Event(SourceEvent::Metric {
                        name: metric.name,
                        value: metric.value,
                    })
                }
                Frame::Replan(_) => Read::Event(SourceEvent::Replan),
                Frame::Behind(behind) => Read::Event(SourceEvent::Behind {
                    records: behind.records,
                }),
                Frame::Done(_) => Read::Done,
                Frame::Error(error) => {
                    let failed = ConnectorError::try_from(error);
                    return Err(failed.unwrap_or_else(|error| invalid(&error)));
                }
            },
        )
    }

    fn schema(&mut self, schema: &v1::SchemaFrame) -> rdlt_connector::Result<Read> {
        if self.epoch.is_some_and(|epoch| schema.schema_epoch <= epoch) {
            let message = format!(
                "schema epoch {} does not follow {:?}",
                schema.schema_epoch, self.epoch
            );
            return Err(ConnectorError::new(ConnectorErrorKind::Internal, message)
                .with_code(rdlt_connector::wire::MALFORMED_FRAME));
        }
        let held = self
            .decoder
            .schema(&schema.ipc_schema)
            .map_err(|error| frame_error(&error))?;
        // The decoder keeps the schema, and its message to know it by.
        let message = count(schema.ipc_schema.len());
        self.schema = rdlt_connector::cost::schema_bytes(&held).saturating_add(message);
        self.arrays = rdlt_connector::cost::batch_arrays(held.fields());
        self.epoch = Some(schema.schema_epoch);
        Ok(Read::Nothing)
    }

    fn batch(
        &mut self,
        frame: Received,
        rows: Rows,
        dictionary: bool,
        epoch: u64,
    ) -> rdlt_connector::Result<Read> {
        if self.epoch != Some(epoch) {
            let message = format!("a batch of schema epoch {epoch} arrived before its schema");
            return Err(ConnectorError::new(ConnectorErrorKind::Internal, message)
                .with_code(rdlt_connector::wire::MALFORMED_FRAME));
        }
        let frame = IpcFrame::framed(frame.body).map_err(|error| frame_error(&error))?;
        let decoded = self
            .decoder
            .frame(&frame)
            .map_err(|error| frame_error(&error))?;
        match (decoded, dictionary) {
            (Some(decoded), false) => Ok(Read::Event(SourceEvent::Push(match rows {
                Rows::Arrow => Push::Arrow(decoded),
                Rows::Changes => Push::Changes(decoded),
            }))),
            (None, true) => Ok(Read::Nothing),
            _ => Err(ConnectorError::new(
                ConnectorErrorKind::Internal,
                "a batch frame whose message is not what its flags say",
            )
            .with_code(rdlt_connector::wire::MALFORMED_FRAME)),
        }
    }
}

fn log_level(level: i32) -> rdlt_connector::Result<LogLevel> {
    match v1::LogLevel::try_from(level) {
        Ok(v1::LogLevel::Error) => Ok(LogLevel::Error),
        Ok(v1::LogLevel::Warn) => Ok(LogLevel::Warn),
        Ok(v1::LogLevel::Info) => Ok(LogLevel::Info),
        Ok(v1::LogLevel::Debug) => Ok(LogLevel::Debug),
        Ok(v1::LogLevel::Unspecified) | Err(_) => Err(invalid(&Invalid::Unknown("log level"))),
    }
}
