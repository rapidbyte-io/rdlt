//! One stream's extraction: read from the source, shred (or pass structured
//! batches through) on the blocking pool, and forward load items downstream.

use std::sync::Arc;

use bytes::Bytes;
use rdlt_connector::{
    DestinationCapabilities, PushPayload, ReadRequest, Source, StreamSpec, channel::ByteSender,
    records_channel,
};
use rdlt_core::{
    Cursor, LoadId, PipelineEvent, RdltError, SchemaPolicy, StreamName, TableName, WriteMode,
};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::{
    load::LoadItem,
    schema::registry::SchemaRegistry,
    shred::{TapeShredder, tape::PushError},
};

use super::classify::classify_source_error;

/// Single-owner shred/passthrough state for one stream. `run_blocking`-style
/// methods consume `self`, move it onto the blocking pool, and hand it back —
/// so the CPU-bound work stays lock-free and single-owner WITHOUT the take/expect
/// dance an `Option` would need (the owner is never absent by construction).
struct ShredOwner {
    shredder: TapeShredder,
    registry: SchemaRegistry,
}

impl ShredOwner {
    /// Shred one raw-JSON slab on the blocking pool. `Err` is a task panic; the
    /// inner `Result` is the shred outcome (invalid JSON classified per stream).
    async fn shred(
        mut self,
        bytes: Bytes,
        load_id: LoadId,
        mode: WriteMode,
        policy: SchemaPolicy,
        stream: StreamName,
    ) -> Result<(Self, Result<Vec<LoadItem>, RdltError>), RdltError> {
        tokio::task::spawn_blocking(move || {
            let span = tracing::info_span!("rdlt.shred");
            let _guard = span.enter();
            let ctx = crate::shred::ShredContext {
                registry: &mut self.registry,
                load_id: &load_id,
                mode: &mode,
                policy: &policy,
            };
            let items = self
                .shredder
                .push_and_drain(&bytes, ctx)
                .map_err(|e| match e {
                    PushError::Json(e) => {
                        RdltError::source(stream, format!("invalid JSON from source: {e}"))
                    }
                    PushError::Engine(e) => e,
                });
            (self, items)
        })
        .await
        .map_err(|e| RdltError::internal(format!("shred task panicked: {e}")))
    }

    /// Pass one already-structured batch through on the blocking pool: the common
    /// path is cheap (schema map + one constant column), but a widened column
    /// casts real data — that work must not sit on the async executor.
    async fn passthrough(
        mut self,
        batch: arrow::record_batch::RecordBatch,
        table: TableName,
        load_id: LoadId,
        mode: WriteMode,
        policy: SchemaPolicy,
        capabilities: DestinationCapabilities,
    ) -> Result<(Self, Result<Vec<LoadItem>, RdltError>), RdltError> {
        tokio::task::spawn_blocking(move || {
            let span = tracing::info_span!("rdlt.passthrough");
            let _guard = span.enter();
            let ctx = crate::shred::ShredContext {
                registry: &mut self.registry,
                load_id: &load_id,
                mode: &mode,
                policy: &policy,
            };
            let items =
                crate::shred::passthrough::passthrough_items(&batch, &table, ctx, capabilities);
            (self, items)
        })
        .await
        .map_err(|e| RdltError::internal(format!("passthrough task panicked: {e}")))
    }
}

/// Everything `run_once` plans for one stream, bundled so the task takes the
/// plan whole instead of eight loose values.
pub(super) struct StreamPlan {
    pub(super) spec: StreamSpec,
    pub(super) capabilities: DestinationCapabilities,
    pub(super) since: Option<Cursor>,
    pub(super) mode: WriteMode,
    pub(super) root_table: TableName,
    pub(super) byte_budget: usize,
    pub(super) load_id: LoadId,
    pub(super) policy: SchemaPolicy,
}

/// One stream's task: read from the source, shred (or pass structured batches
/// through), forward the emitted load items, and classify the source outcome.
/// Classification only — the retry decision lives in `run` (run-level).
pub(super) async fn stream_task(
    plan: StreamPlan,
    source: Arc<dyn Source>,
    tx: ByteSender<LoadItem>,
    cancel: CancellationToken,
    events: broadcast::Sender<PipelineEvent>,
    read_totals: Arc<std::sync::Mutex<std::collections::BTreeMap<StreamName, (u64, u64)>>>,
) -> Result<(), RdltError> {
    let StreamPlan {
        spec,
        capabilities,
        since,
        mode,
        root_table,
        byte_budget,
        load_id,
        policy,
    } = plan;
    let stream_name = spec.name.clone();

    let arrow_table = root_table.clone();
    // Single-owner by construction: each blocking method consumes the owner and
    // returns it, so it is moved out and reassigned in place — never absent.
    let mut owner = ShredOwner {
        shredder: TapeShredder::new(spec.clone(), capabilities, root_table),
        registry: SchemaRegistry::default(),
    };

    let (out, mut input) = records_channel(byte_budget);
    let request = ReadRequest::new(spec.clone(), since, out);
    let read_source = Arc::clone(&source);
    let mut reader = tokio::spawn(async move { read_source.read(request).await });

    let push_result: Result<(), RdltError> = loop {
        let push = tokio::select! {
            push = input.recv() => push,
            _ = cancel.cancelled() => {
                input.close();
                break Err(RdltError::Cancelled);
            }
        };
        let Some(push) = push else { break Ok(()) };
        match push.payload {
            PushPayload::RawJson(bytes) => {
                let payload_bytes = bytes.len() as u64;
                // CPU-bound shred on the blocking pool; the owner keeps the
                // shredder single-owner without locks. The tape path parses the
                // slab into an arena and drains it in one call — no per-row trees.
                let (returned, items) = owner
                    .shred(
                        bytes,
                        load_id.clone(),
                        mode.clone(),
                        policy.clone(),
                        stream_name.clone(),
                    )
                    .await?;
                owner = returned;
                let items = items?;
                // Rows READ: what the source payload decoded to. For a
                // JSON source the row count is only knowable after the
                // shred; the bytes are the raw payload's.
                let rows_read: u64 = items
                    .iter()
                    .map(|item| match item {
                        LoadItem::Batch { batch, .. } => batch.num_rows() as u64,
                        _ => 0,
                    })
                    .sum();
                let _ = events.send(rdlt_core::PipelineEvent::BatchRead {
                    stream: stream_name.clone(),
                    rows: rows_read,
                    bytes: payload_bytes,
                });
                // The REPORT's copy of the same numbers — counted here,
                // not folded from the lossy broadcast, because report
                // numbers must be exact.
                if let Ok(mut totals) = read_totals.lock() {
                    let entry = totals.entry(stream_name.clone()).or_default();
                    entry.0 += rows_read;
                    entry.1 += payload_bytes;
                }
                for item in items {
                    if tx.send(item).await.is_err() {
                        break;
                    }
                }
            }
            PushPayload::Arrow(batch) => {
                // Structured fast path; undeclared streams are a
                // contract violation.
                if !spec.structured {
                    break Err(RdltError::source(
                        stream_name.clone(),
                        "source pushed Arrow batches on a stream not declared \
                         `structured`",
                    ));
                }
                let (rows_read, payload_bytes) = (
                    batch.num_rows() as u64,
                    batch.get_array_memory_size() as u64,
                );
                let _ = events.send(rdlt_core::PipelineEvent::BatchRead {
                    stream: stream_name.clone(),
                    rows: rows_read,
                    bytes: payload_bytes,
                });
                if let Ok(mut totals) = read_totals.lock() {
                    let entry = totals.entry(stream_name.clone()).or_default();
                    entry.0 += rows_read;
                    entry.1 += payload_bytes;
                }
                let (returned, items) = owner
                    .passthrough(
                        batch,
                        arrow_table.clone(),
                        load_id.clone(),
                        mode.clone(),
                        policy.clone(),
                        capabilities,
                    )
                    .await?;
                owner = returned;
                for item in items? {
                    if tx.send(item).await.is_err() {
                        break;
                    }
                }
            }
            PushPayload::Checkpoint(cursor) => {
                if tx
                    .send(LoadItem::Checkpoint {
                        stream: stream_name.clone(),
                        cursor,
                    })
                    .await
                    .is_err()
                {
                    break Ok(());
                }
            }
        }
    };

    // However the push loop ended, release the source BEFORE awaiting it:
    // closing wakes a sender parked on the byte budget so it observes the
    // closure — without this, a break on a contract violation would await
    // a reader that can never finish. (Idempotent; the Cancelled arm
    // already closed early for faster teardown.)
    input.close();
    // Surface source-side failures even when the push loop ended first.
    let read_result = (&mut reader).await;
    match (push_result, read_result) {
        (Err(e), _) => Err(e),
        (Ok(()), Ok(Ok(()))) => {
            let _ = events.send(rdlt_core::PipelineEvent::StreamFinished {
                stream: stream_name,
            });
            Ok(())
        }
        (Ok(()), Ok(Err(e))) => Err(classify_source_error(stream_name, &e)),
        (Ok(()), Err(join_err)) => Err(RdltError::source(
            stream_name,
            format!("source task: {join_err}"),
        )),
    }
}
