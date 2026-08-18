//! One stream's extraction: read from the source, shred (or pass structured
//! batches through) on the blocking pool, and forward load items downstream.

use std::sync::Arc;

use bytes::Bytes;
use rdlt_connector::channel::{ByteSender, PushPayload, SharedBudget, records_shared};
use rdlt_connector::destination::Capabilities;
use rdlt_connector::source::{ReadRequest, Source, StreamSpec};
use rdlt_core::commit::WriteMode;
use rdlt_core::cursor::Cursor;
use rdlt_core::error::Error;
use rdlt_core::event::PipelineEvent;
use rdlt_core::id::{LoadId, StreamName, TableName};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::classify::classify_source_error;
use crate::load::LoadItem;
use crate::policy::SchemaPolicy;
use crate::schema::registry::SchemaRegistry;
use crate::shred::json::{PushError, Shredder};
use crate::shred::resolve::ShredContext;

/// How long an error exit waits for the reader to notice its closed
/// channel before aborting it. Long enough for a well-behaved source
/// to observe the closure at its next push and return on its own;
/// short enough that the advertised `cancellation_token().cancel()`
/// stays prompt for an embedder whose source is parked between frames.
const READER_ABORT_GRACE: std::time::Duration = std::time::Duration::from_millis(250);

/// A source that closes its output has longer to finish its own teardown, but
/// the promise is still enforced rather than trusted. Cancellation remains
/// prompt during this grace.
const READER_FINISH_GRACE: std::time::Duration = std::time::Duration::from_secs(5);

/// Single-owner shred state for one stream, JSON and Arrow. `run_blocking`-style
/// methods consume `self`, move it onto the blocking pool, and hand it back —
/// so the CPU-bound work stays lock-free and single-owner WITHOUT the take/expect
/// dance an `Option` would need (the owner is never absent by construction).
struct ShredOwner {
    shredder: Shredder,
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
        max_batch_cells: usize,
        stream: StreamName,
    ) -> Result<(Self, Result<Vec<LoadItem>, Error>), Error> {
        tokio::task::spawn_blocking(move || {
            let span = tracing::info_span!("rdlt.shred");
            let _guard = span.enter();
            let ctx = ShredContext {
                registry: &mut self.registry,
                load_id: &load_id,
                mode: &mode,
                policy: &policy,
                max_batch_cells,
            };
            let items = self
                .shredder
                .push_and_resolve(&bytes, ctx)
                .map_err(|e| match e {
                    PushError::Json(e) => {
                        Error::source(stream, format!("invalid JSON from source: {e}"))
                    }
                    PushError::Engine(e) => e,
                });
            (self, items)
        })
        .await
        .map_err(|e| Error::internal(format!("shred task panicked: {e}")))
    }

    /// Pass one already-structured batch through on the blocking pool: the common
    /// path is cheap (schema map + one constant column), but a widened column
    /// casts real data — that work must not sit on the async executor.
    #[allow(clippy::too_many_arguments)]
    async fn arrow(
        mut self,
        batch: arrow::record_batch::RecordBatch,
        table: TableName,
        load_id: LoadId,
        mode: WriteMode,
        policy: SchemaPolicy,
        max_batch_cells: usize,
        capabilities: Capabilities,
    ) -> Result<(Self, Result<Vec<LoadItem>, Error>), Error> {
        tokio::task::spawn_blocking(move || {
            // The span keeps its published name (docs/telemetry.md).
            let span = tracing::info_span!("rdlt.passthrough");
            let _guard = span.enter();
            let ctx = ShredContext {
                registry: &mut self.registry,
                load_id: &load_id,
                mode: &mode,
                policy: &policy,
                max_batch_cells,
            };
            let items = crate::shred::arrow::items(&batch, &table, ctx, capabilities);
            (self, items)
        })
        .await
        .map_err(|e| Error::internal(format!("passthrough task panicked: {e}")))
    }
}

/// Everything `run_once` plans for one stream, bundled so the task takes the
/// plan whole instead of eight loose values.
pub(super) struct StreamPlan {
    pub(super) spec: StreamSpec,
    pub(super) capabilities: Capabilities,
    pub(super) since: Option<Cursor>,
    pub(super) mode: WriteMode,
    pub(super) root_table: TableName,
    /// The RUN's one in-flight read budget, shared by every stream's
    /// records channel: per-stream budgets would multiply the peak memory
    /// cap by the stream count — the one axis discovery declares.
    pub(super) records_budget: SharedBudget,
    pub(super) load_id: LoadId,
    pub(super) policy: SchemaPolicy,
    /// The batch-assembly cell budget (`config::Config::with_max_batch_cells`).
    pub(super) max_batch_cells: usize,
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
) -> Result<(), Error> {
    let StreamPlan {
        spec,
        capabilities,
        since,
        mode,
        root_table,
        records_budget,
        load_id,
        policy,
        max_batch_cells,
    } = plan;
    let stream_name = spec.name.clone();

    let arrow_table = root_table.clone();
    // Single-owner by construction: each blocking method consumes the owner and
    // returns it, so it is moved out and reassigned in place — never absent.
    let mut owner = ShredOwner {
        shredder: Shredder::new(spec.clone(), capabilities, root_table)?,
        registry: SchemaRegistry::default(),
    };

    let (out, mut input) = records_shared(&records_budget);
    let request = ReadRequest::new(spec.clone(), since, out);
    let read_source = Arc::clone(&source);
    let mut reader = tokio::spawn(async move { read_source.read(request).await });

    let push_result: Result<LoopExit, Error> = loop {
        let push = tokio::select! {
            push = input.recv() => push,
            _ = cancel.cancelled() => {
                input.close();
                break Err(Error::Cancelled);
            }
        };
        let Some(push) = push else {
            break Ok(LoopExit::SourceFinished);
        };
        let push_bytes = push.bytes;
        match push.payload {
            PushPayload::RawJson(bytes) => {
                let payload_bytes = bytes.len() as u64;
                // CPU-bound shred on the blocking pool; the owner keeps the
                // shredder single-owner without locks. The JSON path parses the
                // slab into an arena and resolves it in one call — no per-row trees.
                //
                // Errors BREAK, never `?`-return: an early return would skip
                // the cleanup below — the channel would close by drop (which
                // does not wake a sender parked on the byte budget) and the
                // spawned reader would never be reaped, leaking one parked
                // task per refused run.
                let (returned, items) = match owner
                    .shred(
                        bytes,
                        load_id.clone(),
                        mode.clone(),
                        policy.clone(),
                        max_batch_cells,
                        stream_name.clone(),
                    )
                    .await
                {
                    Ok(returned) => returned,
                    Err(e) => break Err(e),
                };
                owner = returned;
                let items = match items {
                    Ok(items) => items,
                    Err(e) => break Err(e),
                };
                // Rows READ: what the source payload DECODED to — batch
                // rows plus whole rows a Discard policy dropped, so the
                // read-vs-loaded divergence the event doc promises is
                // real for discards on this path too (the structured
                // path counts at arrival, before its policies, and the
                // two must agree on what "read" means). For a JSON
                // source the count is only knowable after the shred;
                // the bytes are the raw payload's.
                let rows_read: u64 = items
                    .iter()
                    .map(|item| match item {
                        LoadItem::Batch { batch, .. } => batch.num_rows() as u64,
                        LoadItem::Discarded { rows, .. } => *rows,
                        _ => 0,
                    })
                    .sum();
                let _ = events.send(rdlt_core::event::PipelineEvent::BatchRead {
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
                // A send failure means the loader is gone — exit the push
                // loop, like the Checkpoint arm does. Breaking only the
                // item loop would keep this task SHREDDING every remaining
                // push of a run whose outcome was already decided.
                let mut loader_gone = false;
                for item in items {
                    if tx.send(item).await.is_err() {
                        loader_gone = true;
                        break;
                    }
                }
                if loader_gone {
                    break Ok(LoopExit::LoaderGone);
                }
            }
            PushPayload::Arrow(batch) => {
                // Structured fast path; undeclared streams are a
                // contract violation.
                if !spec.structured {
                    break Err(Error::source(
                        stream_name.clone(),
                        "source pushed Arrow batches on a stream not declared \
                         `structured`",
                    ));
                }
                // The number the channel already metered at push: no
                // re-walk, and BatchRead / the report totals charge the
                // identical figure the budget did.
                let (rows_read, payload_bytes) = (batch.num_rows() as u64, push_bytes as u64);
                let _ = events.send(rdlt_core::event::PipelineEvent::BatchRead {
                    stream: stream_name.clone(),
                    rows: rows_read,
                    bytes: payload_bytes,
                });
                if let Ok(mut totals) = read_totals.lock() {
                    let entry = totals.entry(stream_name.clone()).or_default();
                    entry.0 += rows_read;
                    entry.1 += payload_bytes;
                }
                // Same break-not-return rule as the shred arm: the cleanup
                // below must run for every exit.
                let (returned, items) = match owner
                    .arrow(
                        batch,
                        arrow_table.clone(),
                        load_id.clone(),
                        mode.clone(),
                        policy.clone(),
                        max_batch_cells,
                        capabilities,
                    )
                    .await
                {
                    Ok(returned) => returned,
                    Err(e) => break Err(e),
                };
                owner = returned;
                let items = match items {
                    Ok(items) => items,
                    Err(e) => break Err(e),
                };
                // Same loader-gone exit as the RawJson arm: stop
                // shredding pushes whose items can never be loaded.
                let mut loader_gone = false;
                for item in items {
                    if tx.send(item).await.is_err() {
                        loader_gone = true;
                        break;
                    }
                }
                if loader_gone {
                    break Ok(LoopExit::LoaderGone);
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
                    break Ok(LoopExit::LoaderGone);
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
    // Every exit bounds the join. A source that closed its own channel gets a
    // longer grace, but that close is only a promise that `read` will return,
    // not a mechanism that can be awaited forever. Every OTHER exit
    // (cancellation, a contract violation, the loader gone mid-run) uses the
    // short cleanup grace, because
    // closing the channel only wakes a source parked ON A PUSH — a
    // source idle between frames (a wire adapter waiting on its
    // connector process) observes cancellation at its next push, which
    // may never come, and one parked reader would hang the whole
    // teardown. A bounded grace lets a well-behaved source notice the
    // closure and finish on its own; then the reader is aborted, and
    // the cancelled JoinError is this exit's EXPECTED outcome, never a
    // defect to surface.
    let read_result = match &push_result {
        Ok(LoopExit::SourceFinished) => {
            tokio::select! {
                joined = &mut reader => joined,
                _ = cancel.cancelled() => {
                    reader.abort();
                    let _ = (&mut reader).await;
                    return Err(Error::Cancelled);
                }
                _ = tokio::time::sleep(READER_FINISH_GRACE) => {
                    reader.abort();
                    let _ = (&mut reader).await;
                    return Err(Error::source(
                        stream_name,
                        "source closed its output channel but did not return from read() \
                         within the bounded teardown grace",
                    ));
                }
            }
        }
        _ => match tokio::time::timeout(READER_ABORT_GRACE, &mut reader).await {
            Ok(join) => join,
            Err(_elapsed) => {
                reader.abort();
                (&mut reader).await
            }
        },
    };
    match (push_result, read_result) {
        (Err(e), _) => Err(e),
        (Ok(_), Ok(Ok(()))) => {
            let _ = events.send(rdlt_core::event::PipelineEvent::StreamFinished {
                stream: stream_name,
            });
            Ok(())
        }
        (Ok(_), Ok(Err(e))) => Err(classify_source_error(stream_name, &e)),
        // The abort above is the only cancellation this join can see:
        // the loader-gone exit resolved the run's outcome elsewhere
        // (the loader's own error), so the reaped reader is cleanup,
        // not a stream failure.
        (Ok(_), Err(join_err)) if join_err.is_cancelled() => Ok(()),
        (Ok(_), Err(join_err)) => Err(Error::source(
            stream_name,
            format!("source task: {join_err}"),
        )),
    }
}

/// How the push loop ended without an error — what decides whether the
/// reader's join may be waited on unconditionally.
enum LoopExit {
    /// The source closed the records channel itself: its `read` is
    /// returning now.
    SourceFinished,
    /// The loader hung up mid-run: the source may be parked between
    /// frames and never observe it.
    LoaderGone,
}
