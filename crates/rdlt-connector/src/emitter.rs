//! What a source's read handler writes to.

mod admit;
#[cfg(test)]
mod tests;

use std::marker::PhantomData;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use bytes::Bytes;
use serde::Serialize;

use crate::change::validate_change_batch;
use crate::cursor::Cursor;
use crate::error::{ConnectorError, LimitExceeded, Result, ResultExt};
use crate::sink::{LogLevel, PartitionSink, Push, SourceEvent};

/// Sends one partition's data and checkpoints to the engine, with cursors of type `C`.
///
/// Every method fails with a [`Stopped`](crate::ConnectorErrorKind::Stopped) error once the
/// engine asks the read to stop; returning that error with `?` ends the read cleanly.
#[derive(Debug)]
pub struct Emitter<C> {
    sink: PartitionSink,
    cursor_version: u16,
    follow: bool,
    resumes: bool,
    /// The schema of the batch last pushed, which its schema message was found within limits.
    schema: Option<SchemaRef>,
    cursor: PhantomData<fn(&C)>,
}

impl<C: Serialize> Emitter<C> {
    pub(crate) fn new(sink: PartitionSink, cursor_version: u16, follow: bool) -> Self {
        Self {
            sink,
            cursor_version,
            follow,
            resumes: false,
            schema: None,
            cursor: PhantomData,
        }
    }

    /// Says the host gave the read a cursor.
    pub(crate) fn resuming(mut self, resumes: bool) -> Self {
        self.resumes = resumes;
        self
    }

    /// Whether the host gave the read a cursor to start from; where it gave none, the read's
    /// cursor is the stream's default, and its host is heard for no position until the read
    /// sends a checkpoint.
    pub fn resumes(&self) -> bool {
        self.resumes
    }

    /// Whether the engine asks this read to follow its unbounded partition once caught up,
    /// waiting for more until asked to stop, rather than return once caught up to where the
    /// source stood when the read started ([`ReadRequest::follow`](crate::ReadRequest::follow)).
    pub fn follows(&self) -> bool {
        self.follow
    }

    /// Resolves once the engine asks the read to stop, or stops listening: a read waiting for
    /// data selects on it, and returns.
    pub async fn stopped(&self) {
        self.sink.stopped().await;
    }

    /// Pushes `rows` as JSON for the engine to infer their schema; an empty slice pushes nothing.
    pub async fn rows<T: Serialize>(&mut self, rows: &[T]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        // serde_json, not sonic-rs: rows holding serde_json's raw values or arbitrary-precision
        // numbers serialize through its private tokens, which sonic-rs writes out as objects.
        let json = serde_json::to_vec(rows).data("serializing rows")?;
        self.json(Bytes::from(json)).await
    }

    /// Pushes raw JSON: an array of objects or newline-delimited objects.
    pub async fn json(&mut self, json: Bytes) -> Result<()> {
        let limit = self.sink.limits().json_push_bytes;
        check_limit("json push bytes", json.len(), limit)?;
        self.sink.send(SourceEvent::Push(Push::Json(json))).await
    }

    /// Pushes an Arrow batch; an empty batch pushes nothing.
    pub async fn batch(&mut self, batch: RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        check_batch(&batch, &self.sink)?;
        self.check_schema(&batch)?;
        self.sink.send(SourceEvent::Push(Push::Arrow(batch))).await
    }

    /// Pushes a change batch; an empty batch pushes nothing.
    pub async fn changes(&mut self, batch: RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        check_batch(&batch, &self.sink)?;
        self.check_schema(&batch)?;
        validate_change_batch(&batch)?;
        self.sink
            .send(SourceEvent::Push(Push::Changes(batch)))
            .await
    }

    /// Checks that `batch`'s schema, as the message that carries it, is within the sink's limit
    /// on schema bytes; a schema shared with the batch before is checked once.
    fn check_schema(&mut self, batch: &RecordBatch) -> Result<()> {
        let schema = batch.schema();
        if self
            .schema
            .as_ref()
            .is_some_and(|checked| Arc::ptr_eq(checked, &schema))
        {
            return Ok(());
        }
        let message = rdlt_wire::schema_message_bytes(&schema);
        check_limit("schema bytes", message, self.sink.limits().schema_bytes)?;
        self.schema = Some(schema);
        Ok(())
    }

    /// Seals everything pushed since the last checkpoint; a restart resumes from `cursor`.
    pub async fn checkpoint(&mut self, cursor: &C) -> Result<()> {
        let cursor = Cursor::encode(self.cursor_version, cursor)?;
        let limit = self.sink.limits().cursor_bytes;
        check_limit("cursor bytes", cursor.bytes().len(), limit)?;
        let answers = self.sink.pending_barrier();
        self.sink
            .send(SourceEvent::Checkpoint { cursor, answers })
            .await
    }

    /// Whether the engine is waiting for a checkpoint at the next safe point.
    pub fn checkpoint_due(&self) -> bool {
        self.sink.pending_barrier().is_some()
    }

    /// Sends a log line to the engine's log.
    pub async fn log(&mut self, level: LogLevel, message: impl Into<String>) -> Result<()> {
        self.sink
            .send(SourceEvent::Log {
                level,
                message: message.into(),
            })
            .await
    }

    /// Sends a metric sample to the engine's metrics.
    pub async fn metric(&mut self, name: impl Into<String>, value: f64) -> Result<()> {
        self.sink
            .send(SourceEvent::Metric {
                name: name.into(),
                value,
            })
            .await
    }

    /// Tells the engine the stream's partitions changed, as a topic's whose partitions were
    /// increased or a consumer's rebalanced: a run that follows its source plans the stream
    /// again now, rather than at its next interval.
    pub async fn replan(&mut self) -> Result<()> {
        self.sink.send(SourceEvent::Replan).await
    }

    /// Tells the engine how many records this read is behind its source's newest, as the source
    /// measures it: the engine reports the stream's lag from them.
    pub async fn behind(&mut self, records: u64) -> Result<()> {
        self.sink.send(SourceEvent::Behind { records }).await
    }
}

/// Checks `batch` against `sink`'s limits on a batch, before anything else walks it; one the
/// sink holds whole, as it is, against what a frame may hold too.
fn check_batch(batch: &RecordBatch, sink: &PartitionSink) -> Result<()> {
    admit::admit(batch, sink.holds_whole(), &sink.limits()).map_err(ConnectorError::exceeds)
}

fn check_limit(name: &'static str, actual: usize, limit: u64) -> Result<()> {
    let actual = u64::try_from(actual).unwrap_or(u64::MAX);
    if actual > limit {
        return Err(ConnectorError::exceeds(LimitExceeded {
            name,
            limit,
            actual,
        }));
    }
    Ok(())
}
