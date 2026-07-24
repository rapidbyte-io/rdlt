//! The byte-budgeted push channel between a host and one `Source::read` call.
//!
//! Awaiting a push IS the flow control: the budget is bounded in BYTES (not messages),
//! so a slow destination propagates backpressure to the source without letting peak
//! memory scale with row width.

use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{Semaphore, mpsc};

use crate::RecordBatch;
use crate::core::Cursor;

/// Secondary message-count bound on the channel. The byte budget is the primary
/// backpressure; this hard cap keeps zero-byte messages (checkpoints) from queueing
/// without limit when the budget alone would never park them.
const CHANNEL_MSG_CAPACITY: usize = 64;

/// The stream was closed by the host (cancellation or failure downstream). Sources
/// should return promptly without escalating.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("records channel closed by host")]
pub struct ChannelClosed;

/// What a source pushed. Obtained by hosts from [`RecordsIn::recv`]; carries its
/// byte-budget permit, released when the message is dropped.
#[derive(Debug)]
pub struct SourcePush {
    pub payload: PushPayload,
    _permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

#[derive(Debug)]
pub enum PushPayload {
    /// Raw JSON bytes: one document, an array of documents, or NDJSON. The engine's
    /// shredder parses these directly into Arrow builders. `rows()` also lands here
    /// (canonically serialized) so hosts have exactly one JSON ingest path.
    RawJson(Bytes),
    /// A source-native Arrow batch; bypasses the shredder (schema check only).
    Arrow(RecordBatch),
    /// "All rows pushed so far are complete up to this cursor."
    Checkpoint(Cursor),
}

/// Push handle held by sources. Awaiting a push is the flow control: the byte budget
/// (bounded in BYTES, not messages) is what a slow destination propagates back to the
/// source.
#[derive(Debug)]
pub struct RecordsOut {
    tx: mpsc::Sender<SourcePush>,
    budget: Arc<Semaphore>,
    budget_max: usize,
}

impl RecordsOut {
    /// Perf path: raw JSON bytes (one document, an array, or NDJSON). Sources that
    /// already hold bytes (HTTP bodies, files) should always use this.
    pub async fn raw_json(&mut self, bytes: Bytes) -> Result<(), ChannelClosed> {
        let size = bytes.len();
        self.send(PushPayload::RawJson(bytes), size).await
    }

    /// Convenience path for programmatically constructed rows; serialized here to
    /// NDJSON so hosts see a single JSON ingest path.
    pub async fn rows(
        &mut self,
        rows: impl IntoIterator<Item = serde_json::Value>,
    ) -> Result<(), ChannelClosed> {
        let mut buf = Vec::new();
        for row in rows {
            serde_json::to_writer(&mut buf, &row).map_err(|_| ChannelClosed)?;
            buf.push(b'\n');
        }
        if buf.is_empty() {
            return Ok(());
        }
        let size = buf.len();
        self.send(PushPayload::RawJson(buf.into()), size).await
    }

    pub async fn arrow(&mut self, batch: RecordBatch) -> Result<(), ChannelClosed> {
        let size = batch.get_array_memory_size();
        self.send(PushPayload::Arrow(batch), size).await
    }

    /// "All rows pushed so far are complete up to `cursor`."
    pub async fn checkpoint(&mut self, cursor: Cursor) -> Result<(), ChannelClosed> {
        self.send(PushPayload::Checkpoint(cursor), 0).await
    }

    async fn send(&mut self, payload: PushPayload, size: usize) -> Result<(), ChannelClosed> {
        // A single oversized message may not exceed the whole budget or it would
        // deadlock; it degrades to "budget fully drained" instead.
        let permits = size.min(self.budget_max).try_into().unwrap_or(u32::MAX);
        let permit = if permits > 0 {
            Some(
                Arc::clone(&self.budget)
                    .acquire_many_owned(permits)
                    .await
                    .map_err(|_| ChannelClosed)?,
            )
        } else {
            None
        };
        self.tx
            .send(SourcePush {
                payload,
                _permit: permit,
            })
            .await
            .map_err(|_| ChannelClosed)
    }
}

/// Receiving half held by hosts (the engine, or a conformance harness).
#[derive(Debug)]
pub struct RecordsIn {
    rx: mpsc::Receiver<SourcePush>,
    budget: Arc<Semaphore>,
}

impl RecordsIn {
    /// `None` when the source finished (dropped its `RecordsOut`).
    pub async fn recv(&mut self) -> Option<SourcePush> {
        self.rx.recv().await
    }

    /// Closing tells the source to stop at its next push.
    pub fn close(&mut self) {
        self.rx.close();
        // Wake any source blocked on the byte budget so it observes the closure.
        self.budget.close();
    }
}

/// Create the push channel between a host and one `Source::read` call.
/// `byte_budget` bounds in-flight bytes (backpressure); message count is secondary.
pub fn records_channel(byte_budget: usize) -> (RecordsOut, RecordsIn) {
    let budget = Arc::new(Semaphore::new(byte_budget));
    let (tx, rx) = mpsc::channel(CHANNEL_MSG_CAPACITY);
    (
        RecordsOut {
            tx,
            budget: Arc::clone(&budget),
            budget_max: byte_budget,
        },
        RecordsIn { rx, budget },
    )
}

#[cfg(test)]
mod budget_tests {
    // Mutation-report closure: same boundary at the SPI layer.
    use super::*;

    #[tokio::test]
    async fn exact_budget_passes_and_next_push_waits() {
        let (mut out, mut input) = records_channel(100);
        out.raw_json(bytes::Bytes::from(vec![b'1'; 100]))
            .await
            .expect("exactly the budget");
        let pending = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            out.raw_json(bytes::Bytes::from_static(b"2")),
        )
        .await;
        assert!(
            pending.is_err(),
            "budget exhausted: the next push must wait"
        );
        let push = input.recv().await.expect("push");
        drop(push);
        out.raw_json(bytes::Bytes::from_static(b"2"))
            .await
            .expect("freed budget");
    }
}
