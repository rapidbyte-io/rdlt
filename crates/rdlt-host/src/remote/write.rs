//! A remote writer: batches for one table go to the connector as frames, within the credit it
//! grants, and a flush waits for its stats.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use rdlt_connector::wire::{frame_error, v1};
use rdlt_connector::{
    BoxFuture, ConnectorError, DestinationWriter, SegmentId, TableRef, WriteStats,
};
use rdlt_wire::Encoder;
use rdlt_wire::prost::Message as _;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Streaming;

use super::destination::out_of_turn;
use super::{Connection, lost_error};

/// A writer for one table of a session on a destination served on the other end of a connection.
pub(super) struct RemoteWriter {
    connection: Arc<Connection>,
    frames: mpsc::Sender<v1::WriteFrame>,
    acks: Streaming<v1::WriteAck>,
    /// The credit the connector has left, which the last frame may have taken below zero.
    credit: i64,
    encoder: Encoder,
    schema: Option<SchemaRef>,
    version: u32,
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
        let mut client = connection.client.clone();
        let deadline = connection.options.deadlines.write_ack;
        let acks = connection
            .call(
                deadline,
                "opening the writer",
                client.write(ReceiverStream::new(receiver)),
            )
            .await?;
        Ok(Self {
            connection,
            frames,
            acks,
            credit: 0,
            encoder: Encoder::default(),
            schema: None,
            version: table.version.0,
        })
    }

    /// The connector's next answer, within the write-ack deadline.
    async fn ack(&mut self) -> rdlt_connector::Result<v1::write_ack::Ack> {
        let deadline = self.connection.options.deadlines.write_ack;
        let answer = tokio::select! {
            biased;
            () = self.connection.lost.cancelled() => return Err(lost_error()),
            answer = tokio::time::timeout(deadline, self.acks.message()) => answer,
        };
        let message = match answer {
            Ok(Ok(Some(ack))) => ack.ack,
            Ok(Ok(None)) => return Err(out_of_turn("the end of the write")),
            Ok(Err(status)) => return Err(rdlt_connector::wire::error(&status)),
            Err(_) => {
                return Err(ConnectorError::new(
                    rdlt_connector::ConnectorErrorKind::Transient,
                    format!("an answer to a write took longer than its deadline of {deadline:?}"),
                )
                .with_code(super::DEADLINE_EXCEEDED));
            }
        };
        match message {
            Some(v1::write_ack::Ack::Error(error)) => Err(ConnectorError::try_from(error)
                .unwrap_or_else(|error| super::source::invalid(&error))),
            Some(ack) => Ok(ack),
            None => Err(out_of_turn("an empty answer")),
        }
    }

    /// Sends `frame` once the connector has credit left for it, spending its size.
    async fn send(&mut self, frame: v1::write_frame::Frame) -> rdlt_connector::Result<()> {
        let frame = v1::WriteFrame { frame: Some(frame) };
        let size = u64::try_from(frame.encoded_len()).unwrap_or(u64::MAX);
        while self.credit <= 0 {
            match self.ack().await? {
                v1::write_ack::Ack::Credit(credit) => {
                    let bytes = i64::try_from(credit.bytes).unwrap_or(i64::MAX);
                    self.credit = self.credit.saturating_add(bytes);
                }
                v1::write_ack::Ack::Flushed(_) | v1::write_ack::Ack::Error(_) => {
                    return Err(out_of_turn("stats no flush asked for"));
                }
            }
        }
        self.credit -= i64::try_from(size).unwrap_or(i64::MAX);
        self.frames.send(frame).await.map_err(|_| lost_error())
    }

    async fn write_batch(
        &mut self,
        segment: SegmentId,
        batch: RecordBatch,
    ) -> rdlt_connector::Result<()> {
        use v1::write_frame::Frame;
        if self.schema.as_ref() != Some(&batch.schema()) {
            let ipc_schema = self.encoder.schema(&batch.schema());
            self.schema = Some(batch.schema());
            self.send(Frame::Schema(v1::WriteSchema {
                version: self.version,
                ipc_schema,
            }))
            .await?;
        }
        for frame in self
            .encoder
            .batch(&batch)
            .map_err(|error| frame_error(&error))?
        {
            self.send(Frame::Batch(v1::WriteBatch {
                segment: segment.0,
                data_header: frame.header,
                data_body: frame.body,
            }))
            .await?;
        }
        Ok(())
    }

    async fn flush_all(&mut self) -> rdlt_connector::Result<WriteStats> {
        self.send(v1::write_frame::Frame::Flush(v1::Unit {}))
            .await?;
        loop {
            match self.ack().await? {
                v1::write_ack::Ack::Credit(credit) => {
                    let bytes = i64::try_from(credit.bytes).unwrap_or(i64::MAX);
                    self.credit = self.credit.saturating_add(bytes);
                }
                v1::write_ack::Ack::Flushed(stats) => return Ok(WriteStats::from(stats)),
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
