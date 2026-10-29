//! A remote writer: batches for one table go to the connector as frames on a stream of their
//! own, each written from the batch's own buffers within the write-ack deadline, and a flush
//! waits for its stats.
//!
//! A write returns once its last frame is in the stream, so no frame outlives the batch it lends.
//! A frame cut part way leaves the stream torn, so the writer then refuses every call; one cut
//! before any of it went is dropped.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use rdlt_connector::wire::{frame_error, stream_error, v1};
use rdlt_connector::{
    BoxFuture, ConnectorError, DestinationWriter, SegmentId, TableRef, WriteStats,
};
use rdlt_wire::limits::Class;
use rdlt_wire::stream::{Call, End, FrameReader, Kind, Outgoing, Rows};
use rdlt_wire::{Cut, Encoder};
use tokio::io::{ReadHalf, WriteHalf};
use tokio::time::Instant;

use super::destination::out_of_turn;
use super::read::reservation;
use super::{Connection, exceeded, lost_because, lost_error};
use crate::network::Stream;

/// A writer for one table of a session on a destination served on the other end of a connection.
pub(super) struct RemoteWriter {
    connection: Arc<Connection>,
    /// The connector's answers, each charged until it is decoded.
    acks: FrameReader<ReadHalf<Box<dyn Stream>>>,
    frames: WriteHalf<Box<dyn Stream>>,
    encoder: Encoder,
    schema: Option<SchemaRef>,
    version: u32,
    /// Bytes: the batch frames sent since the last flush, header and body, as the connector
    /// counts what it has staged.
    staged: u64,
    /// The stats of the flushes the writer made itself since the last one it was asked for.
    flushed: WriteStats,
    /// Whether a flush was sent whose stats have not arrived: they would be taken for a later
    /// flush's, so the writer refuses every call.
    unanswered: bool,
    /// The frame being written, and whether it is a flush, until it is whole.
    pending: Option<(Outgoing, bool)>,
    /// Whether a frame was cut part way: what follows it would be read as its rest, so the writer
    /// refuses every call.
    torn: bool,
}

impl RemoteWriter {
    /// Opens a write of `table` in session `session`.
    pub(super) async fn open(
        connection: Arc<Connection>,
        session: u64,
        table: &TableRef,
    ) -> rdlt_connector::Result<Self> {
        let start = v1::WriteStart {
            session,
            table: Some(v1::TableRef::from(table)),
        };
        let opened = connection.open_call(Call::Write, &start).await?;
        Ok(Self {
            connection,
            acks: opened.reader,
            frames: opened.writer,
            encoder: Encoder::default(),
            schema: None,
            version: table.version.0,
            staged: 0,
            flushed: WriteStats::default(),
            unanswered: false,
            pending: None,
            torn: false,
        })
    }

    /// The connector's next answer, by `due`, charged as an answer is until it is decoded.
    async fn ack(&mut self, due: Instant) -> rdlt_connector::Result<v1::write_ack::Ack> {
        let deadline = self.connection.options.deadlines.write_ack;
        let answer = tokio::select! {
            biased;
            () = self.connection.lost.cancelled() => return Err(lost_error()),
            answer = tokio::time::timeout_at(due, answered(&mut self.acks)) => answer,
        };
        let answer = answer.map_err(|_| exceeded("an answer to a write", deadline))??;
        match answer.ack {
            Some(v1::write_ack::Ack::Error(error)) => Err(ConnectorError::try_from(error)
                .unwrap_or_else(|error| super::source::invalid(&error))),
            Some(ack) => Ok(ack),
            None => Err(out_of_turn("an empty answer")),
        }
    }

    /// Settles what a call cut before its end left, then refuses every call once a flush went
    /// unanswered or a frame was cut part way.
    fn usable(&mut self) -> rdlt_connector::Result<()> {
        if let Some((frame, flush)) = self.pending.take() {
            if frame.is_begun() {
                self.torn = true;
            } else if flush {
                // The flush never went, so nothing waits for its stats.
                self.unanswered = false;
            }
        }
        let refused = if self.torn {
            "a frame of the writer was cut part way, so the writer takes no more"
        } else if self.unanswered {
            "a flush of the writer went unanswered, so the writer takes no more"
        } else {
            return Ok(());
        };
        Err(
            ConnectorError::new(rdlt_connector::ConnectorErrorKind::Transient, refused)
                .with_code(super::DEADLINE_EXCEEDED),
        )
    }

    /// Writes `frame`, a flush where `flush`, whole within the write-ack deadline: a connector
    /// that takes none of it in time fails the write.
    async fn send(&mut self, frame: Outgoing, flush: bool) -> rdlt_connector::Result<()> {
        let deadline = self.connection.options.deadlines.write_ack;
        let (frame, _) = self.pending.insert((frame, flush));
        let written = tokio::select! {
            biased;
            () = self.connection.lost.cancelled() => return Err(lost_error()),
            written = tokio::time::timeout(deadline, frame.write(&mut self.frames)) => written,
        };
        match written {
            Ok(Ok(())) => {
                self.pending = None;
                Ok(())
            }
            // The frame is settled at the next call, by what of it went.
            Err(_) => Err(exceeded(
                "the connector taking a frame of a write",
                deadline,
            )),
            // The connector ended the write: its answers say why.
            Ok(Err(_)) => {
                self.pending = None;
                Err(self.ended().await)
            }
        }
    }

    /// Why the connector ended the write: the error its answers end with.
    async fn ended(&mut self) -> ConnectorError {
        let due = self.due();
        loop {
            if let Err(error) = self.ack(due).await {
                return error;
            }
        }
    }

    /// When an answer awaited from now is due.
    fn due(&self) -> Instant {
        Instant::now() + self.connection.options.deadlines.write_ack
    }

    async fn write_batch(
        &mut self,
        segment: SegmentId,
        batch: RecordBatch,
    ) -> rdlt_connector::Result<()> {
        use v1::write_frame::Frame;
        self.usable()?;
        let schema = batch.schema();
        // Kept once every frame of the batch is written: a write that fails or is dropped part
        // way leaves the next to send its schema, and every dictionary after it, again.
        let kept = self.schema.take();
        if kept.as_ref() != Some(&schema) {
            let ipc_schema = self
                .encoder
                .schema(&schema)
                .map_err(|error| frame_error(&error))?;
            let message = v1::WriteFrame {
                frame: Some(Frame::Schema(v1::WriteSchema {
                    version: self.version,
                    ipc_schema,
                })),
            };
            self.send(Outgoing::message(None, &message).map_err(too_long)?, false)
                .await?;
        }
        // A batch beyond what the connector takes or this end sends goes as several, each of the
        // same segment and encoded only once the piece before it was written; a row beyond them
        // is refused here, typed.
        let limits = self.connection.peer.lesser(&self.connection.options.limits);
        let mut cut = Cut::new(batch, limits);
        loop {
            let pieced = self
                .encoder
                .piece(&mut cut)
                .map_err(|error| frame_error(&error))?;
            let Some(frames) = pieced else {
                self.schema = Some(schema);
                return Ok(());
            };
            for frame in frames {
                // The connector refuses more than it may stage between two flushes, so a frame
                // that would pass that is sent after a flush of what is staged; a frame alone,
                // within the connector's frame limit, never passes it.
                let bytes = u64::try_from(frame.frame_bytes()).unwrap_or(u64::MAX);
                let staged = self.staged.saturating_add(bytes);
                if self.connection.peer.admit_staged(staged).is_err() {
                    let stats = self.flush_staged().await?;
                    self.flushed.rows = self.flushed.rows.saturating_add(stats.rows);
                    self.flushed.bytes = self.flushed.bytes.saturating_add(stats.bytes);
                }
                self.staged = self.staged.saturating_add(bytes);
                let kind = Kind::Batch {
                    rows: Rows::Arrow,
                    dictionary: frame.is_dictionary(),
                    tag: segment.0,
                };
                let frame = Outgoing::new(kind, frame.into_pieces()).map_err(too_long)?;
                self.send(frame, false).await?;
            }
            // Encoding a piece is work of its own: other tasks run before the next.
            tokio::task::yield_now().await;
        }
    }

    /// Flushes what the connector has staged, and returns the stats of everything flushed since
    /// the last flush asked for.
    async fn flush_all(&mut self) -> rdlt_connector::Result<WriteStats> {
        self.usable()?;
        let stats = self.flush_staged().await?;
        let before = std::mem::take(&mut self.flushed);
        Ok(WriteStats {
            rows: before.rows.saturating_add(stats.rows),
            bytes: before.bytes.saturating_add(stats.bytes),
        })
    }

    /// Asks the connector to flush what it has staged, and waits for its stats.
    async fn flush_staged(&mut self) -> rdlt_connector::Result<WriteStats> {
        let flush = v1::WriteFrame {
            frame: Some(v1::write_frame::Frame::Flush(v1::Unit {})),
        };
        // Cleared once its stats arrive: a flush that fails or is dropped before then would have
        // its stats taken for the next, unless none of it went.
        self.unanswered = true;
        self.send(Outgoing::message(None, &flush).map_err(too_long)?, true)
            .await?;
        self.staged = 0;
        let due = self.due();
        match self.ack(due).await? {
            v1::write_ack::Ack::Flushed(stats) => {
                self.unanswered = false;
                Ok(WriteStats::from(stats))
            }
            v1::write_ack::Ack::Error(_) => Err(out_of_turn("an error")),
        }
    }
}

/// The connector's next answer on a write, its message charged to whoever the call is charged
/// to from before its body is read until it is decoded.
async fn answered(
    acks: &mut FrameReader<ReadHalf<Box<dyn Stream>>>,
) -> rdlt_connector::Result<v1::WriteAck> {
    let Some(head) = acks.head().await.map_err(|error| stream_error(&error))? else {
        // As a connector that crashed or lost its connection ends it.
        return Err(lost_because(
            "the connector ended the write unanswered".to_owned(),
        ));
    };
    let bytes = reservation(&head, 0);
    let charged = match rdlt_wire::bounded::current() {
        Some(charge) => {
            let bytes = usize::try_from(bytes).unwrap_or(usize::MAX);
            let charged = charge.charge(Class::Control, bytes).await;
            Some(charged.map_err(|status| rdlt_connector::wire::error(&status))?)
        }
        None => None,
    };
    let frame = acks.body().await.map_err(|error| stream_error(&error))?;
    let Kind::Message { opens: None } = frame.head.kind else {
        return Err(out_of_turn("a frame that is no answer"));
    };
    let form = Call::Write
        .form(End::Connector)
        .expect("a write's connector answers");
    let bound = usize::try_from(bytes).unwrap_or(usize::MAX);
    let answer = frame
        .message(form, bound)
        .map_err(|error| stream_error(&error));
    drop(frame);
    drop(charged);
    answer
}

/// A frame longer than a head can say, which the connector's limits keep any frame short of.
fn too_long(error: rdlt_wire::stream::TooLong) -> ConnectorError {
    ConnectorError::new(
        rdlt_connector::ConnectorErrorKind::Internal,
        error.to_string(),
    )
}

impl DestinationWriter for RemoteWriter {
    fn write(
        &mut self,
        segment: SegmentId,
        batch: RecordBatch,
    ) -> BoxFuture<'_, rdlt_connector::Result<()>> {
        Box::pin(self.write_batch(segment, batch))
    }

    fn flush(&mut self) -> BoxFuture<'_, rdlt_connector::Result<WriteStats>> {
        Box::pin(self.flush_all())
    }
}
