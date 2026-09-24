//! What a source's read handler writes to.

#[cfg(test)]
mod tests;

use std::marker::PhantomData;

use arrow_array::RecordBatch;
use bytes::Bytes;
use serde::Serialize;

use crate::change::validate_change_batch;
use crate::cursor::Cursor;
use crate::error::{ConnectorError, LimitExceeded, Result, ResultExt};
use crate::limits::{MAX_BATCH_ROWS, MAX_COLUMNS, MAX_JSON_PUSH_BYTES};
use crate::sink::{LogLevel, PartitionSink, Push, SourceEvent};

/// Sends one partition's data and checkpoints to the engine, with cursors of type `C`.
///
/// Every method fails with a [`Stopped`](crate::ConnectorErrorKind::Stopped) error once the
/// engine asks the read to stop; returning that error with `?` ends the read cleanly.
#[derive(Debug)]
pub struct Emitter<C> {
    sink: PartitionSink,
    cursor_version: u16,
    cursor: PhantomData<fn(&C)>,
}

impl<C: Serialize> Emitter<C> {
    pub(crate) fn new(sink: PartitionSink, cursor_version: u16) -> Self {
        Self {
            sink,
            cursor_version,
            cursor: PhantomData,
        }
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
        check_limit("JSON push bytes", json.len(), MAX_JSON_PUSH_BYTES)?;
        self.sink.send(SourceEvent::Push(Push::Json(json))).await
    }

    /// Pushes an Arrow batch; an empty batch pushes nothing.
    pub async fn batch(&mut self, batch: RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        check_batch(&batch)?;
        self.sink.send(SourceEvent::Push(Push::Arrow(batch))).await
    }

    /// Pushes a change batch; an empty batch pushes nothing.
    pub async fn changes(&mut self, batch: RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        check_batch(&batch)?;
        validate_change_batch(&batch)?;
        self.sink
            .send(SourceEvent::Push(Push::Changes(batch)))
            .await
    }

    /// Seals everything pushed since the last checkpoint; a restart resumes from `cursor`.
    pub async fn checkpoint(&mut self, cursor: &C) -> Result<()> {
        let cursor = Cursor::encode(self.cursor_version, cursor)?;
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
}

fn check_batch(batch: &RecordBatch) -> Result<()> {
    check_limit("batch rows", batch.num_rows(), MAX_BATCH_ROWS)?;
    check_limit("batch columns", batch.num_columns(), MAX_COLUMNS)
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
