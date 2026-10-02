//! A remote read: the connector's frames, received within the credit this end grants, become the
//! events of the engine's partition sink, and the engine's requests go to the connector.

#[cfg(test)]
mod tests;

use rdlt_connector::wire::{Invalid, frame_error, v1};
use rdlt_connector::{
    ConnectorError, ConnectorErrorKind, Cursor, LogLevel, PartitionSink, Permit, Push, ReadRequest,
    Requested, SourceEvent,
};
use rdlt_wire::prost::Message as _;
use rdlt_wire::prost::bytes::Bytes;
use rdlt_wire::{Decoder, IpcFrame, Limits};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use super::source::invalid;
use super::{Connection, lost_error};

/// Reads `request`'s partition from the connector into `sink`.
pub(super) async fn run(
    connection: &Connection,
    request: ReadRequest,
    mut sink: PartitionSink,
) -> rdlt_connector::Result<()> {
    use v1::read_control::Control;
    let limits = connection.options.limits;
    // A barrier raised before the read starts goes with its start, so a read too short to see a
    // later control answers it, as in the engine's process; and as a control before its credit,
    // for a connector that knows no barrier in the start. One that knows both answers it once.
    let pending = sink.pending_barrier().unwrap_or(0);
    let (controls, mut frames) = start(connection, &request, pending).await?;
    let control = |control| v1::ReadControl {
        control: Some(control),
    };
    let mut reader = Reader {
        decoder: Decoder::new(limits),
        limits,
        epoch: None,
        schema: 0,
    };
    let (mut forwarded, mut stopping) = (pending, false);
    let mut kept = Kept::default();
    loop {
        tokio::select! {
            biased;
            () = connection.lost.cancelled() => return Err(lost_error()),
            requested = sink.requested(forwarded), if !stopping => {
                let sent = match requested {
                    Requested::Checkpoint(barrier) => {
                        forwarded = barrier;
                        control(Control::Checkpoint(v1::CheckpointRequest { barrier }))
                    }
                    Requested::Stop => {
                        stopping = true;
                        control(Control::Stop(v1::Stop { mode: v1::StopMode::Now as i32 }))
                    }
                };
                controls.send(sent).await.ok();
            }
            frame = frames.message() => {
                let frame = match frame {
                    Ok(Some(frame)) => frame,
                    Ok(None) => return Err(ConnectorError::new(
                        ConnectorErrorKind::Transient,
                        "the connector ended the read without saying it was done",
                    ).with_code(super::CONNECTOR_LOST)),
                    Err(status) => return Err(rdlt_connector::wire::error(&status)),
                };
                let size = u64::try_from(frame.encoded_len()).unwrap_or(u64::MAX);
                match reader.event(frame)? {
                    Read::Done => return Ok(()),
                    Read::Event(event) => forward(&mut sink, event, forwarded).await?,
                    // A schema and a dictionary wait in the decoder for the batches that use
                    // them.
                    Read::Nothing => kept.charge(&sink, reader.kept())?,
                }
                controls.send(control(Control::Credit(v1::Credit { bytes: size }))).await.ok();
            }
        }
    }
}

/// Sends `event` to the engine, where a checkpoint in it answers no barrier beyond `forwarded`,
/// the newest the host forwarded.
///
/// A send the engine stopped fails as stopped, even one that waited for room: the read's next
/// turn forwards the stop, and the read drains.
///
/// # Errors
///
/// A checkpoint answering a barrier the host did not forward, and why its admission refused the
/// event, where it did.
async fn forward(
    sink: &mut PartitionSink,
    event: SourceEvent,
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
    match sink.send(event).await {
        Err(refused) if refused.kind() != ConnectorErrorKind::Stopped => Err(refused),
        _ => Ok(()),
    }
}

/// Starts the read: its first message, and the read's window of credit; the sender of what
/// follows, and the connector's frames.
async fn start(
    connection: &Connection,
    request: &ReadRequest,
    barrier: u64,
) -> rdlt_connector::Result<(
    mpsc::Sender<v1::ReadControl>,
    tonic::Streaming<v1::ReadFrame>,
)> {
    use v1::read_control::Control;
    let window = connection.options.read_window;
    let (controls, receiver) = mpsc::channel(8);
    let start = v1::ReadStart {
        stream: Some(v1::StreamName::from(&request.stream)),
        partition: request.partition.id().as_str().to_owned(),
        cursor: request.cursor.as_ref().map(v1::Cursor::from),
        barrier,
        unbounded: request.partition.is_unbounded(),
        follow: request.follow,
    };
    let control = |control| v1::ReadControl {
        control: Some(control),
    };
    // The receiver is open until it is dropped with the call, so these sends succeed.
    controls.send(control(Control::Start(start))).await.ok();
    // Asked again before any credit, for a connector that knows no barrier in the start: one with
    // nothing to read may end at its first credit.
    if barrier > 0 {
        let asked = v1::CheckpointRequest { barrier };
        controls
            .send(control(Control::Checkpoint(asked)))
            .await
            .ok();
    }
    controls
        .send(control(Control::Credit(v1::Credit { bytes: window })))
        .await
        .ok();
    let mut client = connection.client.clone();
    let deadline = connection.options.deadlines.connect;
    let frames = connection
        .call(
            deadline,
            "starting the read",
            client.read(ReceiverStream::new(receiver)),
        )
        .await?;
    Ok((controls, frames))
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
}

impl Reader {
    /// Bytes: what the decoder keeps between batches, its schema and its dictionaries.
    fn kept(&self) -> u64 {
        self.schema.saturating_add(self.decoder.dictionary_bytes())
    }

    fn event(&mut self, frame: v1::ReadFrame) -> rdlt_connector::Result<Read> {
        use v1::read_frame::Frame;
        let refused = |refusal| frame_error(&rdlt_wire::WireError::Refused(refusal));
        Ok(
            match frame
                .frame
                .ok_or_else(|| invalid(&Invalid::Missing("read frame")))?
            {
                Frame::Schema(schema) => self.schema(&schema)?,
                Frame::Batch(batch) => self.batch(batch)?,
                Frame::Json(json) => {
                    self.limits.admit_json(json.data.len()).map_err(refused)?;
                    // The bytes are a slice of the whole message: copied, the push is charged
                    // for all it keeps alive.
                    let json = Bytes::copy_from_slice(&json.data);
                    Read::Event(SourceEvent::Push(Push::Json(json)))
                }
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
        let message = u64::try_from(schema.ipc_schema.len()).unwrap_or(u64::MAX);
        self.schema = rdlt_connector::cost::schema_bytes(&held).saturating_add(message);
        self.epoch = Some(schema.schema_epoch);
        Ok(Read::Nothing)
    }

    fn batch(&mut self, batch: v1::BatchFrame) -> rdlt_connector::Result<Read> {
        if self.epoch != Some(batch.schema_epoch) {
            let message = format!(
                "a batch of schema epoch {} arrived before its schema",
                batch.schema_epoch
            );
            return Err(ConnectorError::new(ConnectorErrorKind::Internal, message)
                .with_code(rdlt_connector::wire::MALFORMED_FRAME));
        }
        let frame = IpcFrame {
            header: batch.data_header,
            body: batch.data_body,
        };
        let Some(decoded) = self
            .decoder
            .frame(&frame)
            .map_err(|error| frame_error(&error))?
        else {
            return Ok(Read::Nothing);
        };
        let push = match v1::BatchKind::try_from(batch.kind) {
            Ok(v1::BatchKind::Arrow) => Push::Arrow(decoded),
            Ok(v1::BatchKind::Change) => Push::Changes(decoded),
            Ok(v1::BatchKind::Unspecified) | Err(_) => {
                return Err(invalid(&Invalid::Unknown("batch kind")));
            }
        };
        Ok(Read::Event(SourceEvent::Push(push)))
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
