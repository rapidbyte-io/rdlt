//! A remote writer: batches for one table go to the connector as frames, within the credit it
//! grants, and a flush waits for its stats.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use rdlt_connector::wire::{frame_error, v1};
use rdlt_connector::{
    BoxFuture, ConnectorError, DestinationWriter, SegmentId, TableRef, WriteStats,
};
use rdlt_wire::flow::Spending;
use rdlt_wire::plane::{Chained as _, Incoming, WRITE};
use rdlt_wire::prost::bytes::Bytes;
use rdlt_wire::{Cut, Encoder};
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_stream::wrappers::ReceiverStream;

use super::destination::out_of_turn;
use super::{Connection, exceeded, lost_error};

/// A writer for one table of a session on a destination served on the other end of a connection.
pub(super) struct RemoteWriter {
    connection: Arc<Connection>,
    frames: mpsc::Sender<v1::WriteFrame>,
    /// The connector's answers, each charged until it is decoded.
    acks: Incoming<v1::WriteAck>,
    /// The credit the connector has left, which the last frame may have taken below zero.
    credit: Spending,
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
}

impl RemoteWriter {
    /// Opens a write of `table` in session `session`.
    pub(super) async fn open(
        connection: Arc<Connection>,
        session: u64,
        table: &TableRef,
    ) -> rdlt_connector::Result<Self> {
        let (frames, receiver) = mpsc::channel(8);
        let start = v1::WriteStart {
            session,
            table: Some(v1::TableRef::from(table)),
        };
        frames
            .send(v1::WriteFrame {
                frame: Some(v1::write_frame::Frame::Start(start)),
            })
            .await
            .ok();
        let mut channel = connection.client.channel.clone();
        let most = connection.options.limits.largest();
        let deadline = connection.options.deadlines.write_ack;
        let opening = async move {
            let frames = ReceiverStream::new(receiver);
            let opened = super::clients::called(&mut channel, WRITE, frames, most).await;
            opened.map(tonic::Response::new)
        };
        let acks = connection
            .call(deadline, "opening the writer", opening)
            .await?;
        Ok(Self {
            connection,
            frames,
            acks,
            credit: Spending::default(),
            encoder: Encoder::default(),
            schema: None,
            version: table.version.0,
            staged: 0,
            flushed: WriteStats::default(),
            unanswered: false,
        })
    }

    /// When an answer awaited from now is due: the write-ack deadline bounds the whole wait,
    /// however many answers the connector sends meanwhile.
    fn due(&self) -> Instant {
        Instant::now() + self.connection.options.deadlines.write_ack
    }

    /// The connector's next answer, by `due`.
    async fn ack(&mut self, due: Instant) -> rdlt_connector::Result<v1::write_ack::Ack> {
        let answer = tokio::select! {
            biased;
            () = self.connection.lost.cancelled() => return Err(lost_error()),
            answer = tokio::time::timeout_at(due, self.acks.message()) => answer,
        };
        // Decoded: its charge goes.
        self.acks.release();
        let message = match answer {
            Ok(Ok(Some(ack))) => ack.ack,
            Ok(Ok(None)) => return Err(out_of_turn("the end of the write")),
            Ok(Err(status)) => return Err(rdlt_connector::wire::error(&status)),
            Err(_) => {
                return Err(exceeded(
                    "an answer to a write",
                    self.connection.options.deadlines.write_ack,
                ));
            }
        };
        match message {
            Some(v1::write_ack::Ack::Error(error)) => Err(ConnectorError::try_from(error)
                .unwrap_or_else(|error| super::source::invalid(&error))),
            Some(ack) => Ok(ack),
            None => Err(out_of_turn("an empty answer")),
        }
    }

    /// Takes `credit` the connector granted.
    ///
    /// # Errors
    ///
    /// A credit of no bytes, which grants nothing and only keeps the write waiting.
    fn grant(&mut self, credit: v1::Credit) -> rdlt_connector::Result<()> {
        self.credit
            .grant(credit.bytes)
            .map_err(|_| out_of_turn("a credit of no bytes"))
    }

    /// Refuses every call once a flush went unanswered.
    fn usable(&self) -> rdlt_connector::Result<()> {
        if !self.unanswered {
            return Ok(());
        }
        Err(ConnectorError::new(
            rdlt_connector::ConnectorErrorKind::Transient,
            "a flush of the writer went unanswered, so the writer takes no more",
        )
        .with_code(super::DEADLINE_EXCEEDED))
    }

    /// Sends `frame` once the connector has credit left for it and the writer's queue has room,
    /// spending its size as the queue takes it.
    async fn send(&mut self, frame: v1::write_frame::Frame) -> rdlt_connector::Result<()> {
        let frame = v1::WriteFrame { frame: Some(frame) };
        let size = u64::try_from(frame.sent_len()).unwrap_or(u64::MAX);
        let due = self.due();
        while !self.credit.may_send() {
            match self.ack(due).await? {
                v1::write_ack::Ack::Credit(credit) => self.grant(credit)?,
                v1::write_ack::Ack::Flushed(_) | v1::write_ack::Ack::Error(_) => {
                    return Err(out_of_turn("stats no flush asked for"));
                }
            }
        }
        // The transport's windows may fill before the credit is spent: the wait for room has a
        // deadline. A frame spends its credit only as the queue takes it, so a wait that ends
        // early, at its deadline or dropped, spends none.
        let deadline = self.connection.options.deadlines.write_ack;
        tokio::select! {
            biased;
            () = self.connection.lost.cancelled() => return Err(lost_error()),
            reserved = tokio::time::timeout(deadline, self.frames.reserve()) => match reserved {
                Ok(Ok(permit)) => {
                    self.credit.spend(size);
                    permit.send(frame);
                    return Ok(());
                }
                Err(_) => return Err(exceeded("an answer to a write", deadline)),
                Ok(Err(_)) => {}
            },
        }
        // The connector ended the write: its answers say why.
        Err(self.ended().await)
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

    async fn write_batch(
        &mut self,
        segment: SegmentId,
        batch: RecordBatch,
    ) -> rdlt_connector::Result<()> {
        use v1::write_frame::Frame;
        self.usable()?;
        let schema = batch.schema();
        // Kept once every frame of the batch is queued: a write that fails or is dropped part way
        // leaves the next to send its schema, and every dictionary after it, again.
        let kept = self.schema.take();
        if kept.as_ref() != Some(&schema) {
            let ipc_schema = self
                .encoder
                .schema(&schema)
                .map_err(|error| frame_error(&error))?;
            let message = Frame::Schema(v1::WriteSchema {
                version: self.version,
                ipc_schema,
            });
            self.send(message).await?;
        }
        // A batch beyond what the connector takes or this end sends goes as several, each of the
        // same segment and encoded only once the piece before it was sent; a row beyond them is
        // refused here, typed.
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
                let bytes = frame.header.len().saturating_add(frame.body.len());
                let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
                let staged = self.staged.saturating_add(bytes);
                if self.connection.peer.admit_staged(staged).is_err() {
                    let stats = self.flush_staged().await?;
                    self.flushed.rows = self.flushed.rows.saturating_add(stats.rows);
                    self.flushed.bytes = self.flushed.bytes.saturating_add(stats.bytes);
                }
                self.staged = self.staged.saturating_add(bytes);
                let batch = Frame::Batch(v1::WriteBatch {
                    segment: segment.0,
                    data_header: frame.header,
                    padding: Bytes::new(),
                    data_body: frame.body,
                });
                self.send(batch).await?;
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
        self.send(v1::write_frame::Frame::Flush(v1::Unit {}))
            .await?;
        self.staged = 0;
        // Cleared once its stats arrive: a flush that fails or is dropped before then would have
        // its stats taken for the next.
        self.unanswered = true;
        let due = self.due();
        loop {
            match self.ack(due).await? {
                v1::write_ack::Ack::Credit(credit) => self.grant(credit)?,
                v1::write_ack::Ack::Flushed(stats) => {
                    self.unanswered = false;
                    return Ok(WriteStats::from(stats));
                }
                v1::write_ack::Ack::Error(_) => return Err(out_of_turn("an error")),
            }
        }
    }
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
